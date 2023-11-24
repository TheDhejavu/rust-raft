use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::vec;
use config::Config;
use configuration::{Configuration, ConfigCommand, MembershipConfigurations, ConfigStore};
use error::RaftError;
use fsm::FSM;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt; 
use ::log::*;
use raft::TimeoutNowResponse;
use rand::Rng;
use node::{Node, ReplicaNode, ReplicaNodeTxn};
use stable::StableStore;
use storage::LogStore;
use tokio::sync::RwLock;
use utils::ThreadSafeBool;
use crate::executor::FSMExecutor;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll}
};
use futures::Stream;
use tokio::sync::mpsc::UnboundedReceiver;
use crate::signals::{Signals, ServerEventCommand};
use crate::node::NodeType;
use crate::state::{NodeState, RaftState};
use std::collections::{HashMap, HashSet};
use crate::raft::{
    AppendEntriesRequest, AppendEntriesResponse,
    RequestVoteRequest, RequestVoteResponse,
};
use tokio::time::{Duration, sleep};
use std::collections::VecDeque;
use crate::grpc_transport::{RaftTransportResponse,RaftTransportRequest};

pub mod configuration;
pub mod storage;
mod state;
mod election;
pub mod error;
mod utils;
mod signals;
mod mocks;
mod executor;
pub mod datastore;
pub mod config;
pub mod log;
pub mod fsm;
pub mod grpc_transport;
pub mod node;
pub mod stable;

mod raft {
    tonic::include_proto!("raft"); 
}

pub enum RaftNodeServerMessage {
   Apply(Vec<u8>),
   ApplyConfChange(ConfigCommand),
   TransferLeadership,
}


pub struct RaftCore {
    /// Unique identifier for the node in the Raft cluster.
    id: Arc<String>,

    /// Leader's unique identifier. If `None`, the cluster doesn't currently have a leader.
    leader_id: Option<Arc<String>>,
}

impl RaftCore {
    /// Return true if current node is the leader
    pub fn is_leader(&self) -> bool {
        let leader_id = &self.leader_id;
        leader_id.is_some() && leader_id.clone().unwrap() == self.id
    }
}

/// `RaftNodeServer` represents a node in a Raft cluster. 
///
/// It encapsulates the state and behavior associated with the Raft consensus algorithm.
pub struct RaftNodeServer {
    pub core: Arc<Mutex<RaftCore>>,

    /// Current state of the node (e.g., follower, candidate, or leader).
    state:  Arc<Mutex<NodeState>>,

    /// The state of the Raft node.
    raft_state: Arc<RaftState>,

    /// Configuration details for the Raft node.
    conf: Config,

    // cluster configurations containing C_new & C_old for membership change
    membership_configurations: MembershipConfigurations,

    /// Replicas of other nodes in the Raft cluster.
    replica_nodes: Option<HashMap<String, Arc<tokio::sync::Mutex<ReplicaNode>>>>,

    /// Communication channel between raft server and it's replica. we
    /// notify nodes of new logs via this in a non-blocking way.
    replica_nodes_txn: Option<HashMap<String, ReplicaNodeTxn>>,

    /// Log entries storage mechanism.
    logs: Arc<RwLock<Box<dyn LogStore>>>,

    /// Channels to signal stopping server background task.
    shutdown_tx_rx: (tokio::sync::mpsc::Sender<()>, tokio::sync::mpsc::Receiver<()>),

    /// A flag indicating whether a heartbeat has been received.
    received_heartbeat: Arc<AtomicBool>,

    /// 3.10 Leadership transfer extension
    /// A flag indicating whether that there is a leadrship transfer in progress.
    #[allow(dead_code)]
    leadership_transfer_progress: Arc<AtomicBool>,
    
    start_index: Mutex<u64>,

    /// Channel to trigger step down for the current leader.
    step_down: (tokio::sync::mpsc::Sender<()>, tokio::sync::mpsc::Receiver<()>),

    /// Channel to receive incoming Raft RPCs.
    rpc_rx: tokio::sync::mpsc::Receiver<(RaftTransportRequest, tokio::sync::mpsc::Sender<RaftTransportResponse>)>,

    // Leader specific channel for processing commit index
    process_commit_tx: tokio::sync::mpsc::UnboundedSender<u64>, 

    process_commit_rx: tokio::sync::mpsc::UnboundedReceiver<u64>,

    // Leader specific channel for advancing commit index 
    advance_commit_tx: tokio::sync::mpsc::UnboundedSender<((String, u64), tokio::sync::mpsc::Sender<Result<(), RaftError>>)>,

    advance_commit_rx: tokio::sync::mpsc::UnboundedReceiver<((String, u64), tokio::sync::mpsc::Sender<Result<(), RaftError>>)>,

    fsm_apply_tx: tokio::sync::mpsc::UnboundedSender<Vec<log::LogEntry>>,

    /// FSM shutdown Channel to signal stopping of the FSM executor.
    fsm_shutdown_tx: tokio::sync::mpsc::Sender<()>,

    notify: (tokio::sync::mpsc::UnboundedSender<((), tokio::sync::mpsc::Sender<Result<(), RaftError>>)>, tokio::sync::mpsc::UnboundedReceiver<((), tokio::sync::mpsc::Sender<Result<(), RaftError>>)>),
    
    running: ThreadSafeBool,

    config_store: Arc<tokio::sync::Mutex<ConfigStore>>,

    // Leader specific queue for applying log entry based on FIFO
    inflight_entry_queue: VecDeque<log::LogEntry>,

    /// Channel interacting with the Raft Node server, this is a temporary solution. 
    /// There should be a way to run the Raft Node server in a way that's shareable for multiple threads to call
    /// Maybe introduce a shareable non-blocking state ?.
    api_message_rx: tokio::sync::mpsc::UnboundedReceiver<(RaftNodeServerMessage, tokio::sync::mpsc::Sender<Result<(), RaftError>>)>,

    event_tx: tokio::sync::mpsc::UnboundedSender<ServerEventCommand>,
}


/// A multiplexer for Raft node server events, combining an unbounded channel (`event_rx`)
/// and an optional signal future (`signal_fut`).
///
/// Implements the `Stream` trait for asynchronous event handling.
///
struct RaftNodeServerEventMultiplexer {
    event_rx: UnboundedReceiver<ServerEventCommand>,
    signal_fut: Option<Signals>,
}

impl Stream for RaftNodeServerEventMultiplexer {
    type Item = ServerEventCommand;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = Pin::into_inner(self);

        if let Some(signal_fut) = &mut this.signal_fut {
            if let Poll::Ready(signal) = Pin::new(signal_fut).poll(cx) {
                this.signal_fut = None;
                return Poll::Ready(Some(Signals::map_signal(signal)));
            }
        }

        this.event_rx.poll_recv(cx)
    }
}

impl RaftNodeServer {
    pub async fn new(
        id: String,
        conf: Config,
        nodes: Vec<Arc<Node>>,
        fsm: Box<dyn FSM>,
        stable: Box<dyn StableStore>,
        logs: Box<dyn LogStore>,
        rpc_rx: tokio::sync::mpsc::Receiver<(RaftTransportRequest, tokio::sync::mpsc::Sender<RaftTransportResponse>)>,
        api_message_rx: tokio::sync::mpsc::UnboundedReceiver<(RaftNodeServerMessage, tokio::sync::mpsc::Sender<Result<(), RaftError>>)>,
    ) -> Self {
        let shutdown_tx_rx = tokio::sync::mpsc::channel::<()>(1);
        let (advance_commit_tx, advance_commit_rx, ) = tokio::sync::mpsc::unbounded_channel::<((String, u64), tokio::sync::mpsc::Sender<Result<(), RaftError>>)>();
        let notify= tokio::sync::mpsc::unbounded_channel::<((), tokio::sync::mpsc::Sender<Result<(), RaftError>>)>();
        let ( process_commit_tx,  process_commit_rx, ) = tokio::sync::mpsc::unbounded_channel::<u64>();
        let ( fsm_apply_tx,  fsm_apply_rx, ) = tokio::sync::mpsc::unbounded_channel::<Vec<log::LogEntry>>();
        let ( fsm_shutdown_tx,  fsm_shutdown_rx, ) =tokio::sync::mpsc::channel::<()>(1);
        let step_down = tokio::sync::mpsc::channel::<()>(1);
       
        env_logger::Builder::new()
            .filter_level(::log::LevelFilter::Info) 
            .filter_module("sled", ::log::LevelFilter::Warn) 
            .init();

        let raft_state =  Arc::new(RaftState::new(stable));
        let mut membership_configurations = MembershipConfigurations::new(
            Configuration::new(nodes.clone()),
            0,
            0,
            Configuration::new(nodes.clone())
        );

        let config_store = Arc::new(tokio::sync::Mutex::new(ConfigStore::new(format!("./conf/{}/conf.bin", id))));
        
        // Load configuration from disk
        match config_store.lock().await.load() {
            Ok(config) => {
                membership_configurations.set_comitted_configuration(config.config.clone(), config.index);
                membership_configurations.set_latest_configuration(config.config.clone(), config.index);
            },
            Err(e) => error!("Unable to load persisted membership configuration: {}", e),
        }

        info!("raft.nodes: {:?}", membership_configurations.latest.nodes);

        let clone_config_store =  config_store.clone();
        let _handle = tokio::spawn(async move {
            let mut fsm_executor = FSMExecutor::new(fsm_apply_rx, fsm, clone_config_store, fsm_shutdown_rx).await;
            fsm_executor.fsm_loop().await;
        });

        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel::<ServerEventCommand>();

        let server = Self {
            state: Arc::new(Mutex::new(NodeState::Follower)),
            conf,
            step_down,
            notify,
            core: Arc::new(Mutex::new(RaftCore { 
                id: Arc::new(id),
                leader_id: None,
            })),
            inflight_entry_queue: VecDeque::new(),
            advance_commit_tx,
            advance_commit_rx,
            fsm_shutdown_tx,
            fsm_apply_tx,
            process_commit_rx,
            process_commit_tx,
            replica_nodes_txn: None,
            running: ThreadSafeBool::new(false),
            config_store,
            shutdown_tx_rx,
            api_message_rx,
            membership_configurations,
            start_index: Mutex::new(0),
            replica_nodes: None,
            received_heartbeat: Arc::new(AtomicBool::new(false)),
            leadership_transfer_progress: Arc::new(AtomicBool::new(false)),
            logs: Arc::new(RwLock::new(logs)),
            rpc_rx,
            event_tx,
            raft_state,
        };

        server.setup_event_listener(event_rx);

        info!("Log Index: {:?}", server.get_last_log_index().await);
        server
    }

    /// Sets the current leaderid
    pub fn server_id(&self)-> Arc<String> {
        self.core.lock().unwrap().id.clone()
    }

    /// Sets the current leader id
    pub fn set_leader_id(&mut self, id: Arc<String>) {
        let mut core = self.core.lock().unwrap();
        core.leader_id = Some(id);
    }
    

    /// Returns the start index for current node if it is a leader.
    pub fn get_start_index(&self) -> u64 {
        let start_index = self.start_index.lock().unwrap();
        *start_index
    }

    pub fn set_start_index(&self, value: u64) {
        let mut start_index = self.start_index.lock().unwrap();
        *start_index = value;
    }

    /// Retrieves the current term of the node.
    fn get_current_term(&self) -> u64 {
        self.raft_state.get_current_term()
    }

    /// Sets the current term of the node to the specified value.
    fn set_current_term(&self, value: u64) {
        self.raft_state.set_current_term(value);
    }

    /// Returns the current commit index.
    fn get_commit_index(&self) -> u64 {
        self.raft_state.get_commit_index()
    }

    /// Sets the commit index to the given value.
    fn set_commit_index(&self, value: u64) {
        self.raft_state.set_commit_index(value);
    }

    /// Returns the last applied index.
    fn get_last_applied(&self) -> u64 {
        self.raft_state.get_last_applied()
    }

    /// Sets the last applied index to the given value.
    fn set_last_applied(&self, value: u64) {
        self.raft_state.set_last_applied(value);
    }

    /// Retrieves the number of votes the node has received.
    #[allow(dead_code)]
    fn get_votes(&self) -> u64 {
        self.raft_state.get_votes()
    }

    /// Sets the number of votes the node has received.
    #[allow(dead_code)]
    fn set_votes(&self, value: u64) {
        self.raft_state.set_votes(value);
    }

    /// Retrieves the identifier of the node that this node has voted for in the current term.
    fn get_voted_for(&self) -> Option<String> {
        self.raft_state.get_voted_for()
    }

    /// Sets the identifier of the node that this node votes for.
    #[allow(dead_code)]
    fn set_voted_for(&self, node_id: Option<String>) {
        self.raft_state.set_voted_for(node_id);
    }

    /// Retrieves the term of the last log entry.
    async fn get_last_log_term(&self) -> u64 {
        match self.logs.read().await.get_log(self.logs.read().await.last_index()) {
            Ok(Some(log)) => log.term,
            Ok(None) => 0,
            Err(_) => 0,
        }
    }

    /// Retrieves the index of the last log entry.
    async fn get_last_log_index(&self) -> u64  {
        self.logs.read().await.last_index()
    }
    
    /// Transitions the node's state to `Leader`.
    ///
    /// When a node becomes a leader, this function will update 
    /// the internal state accordingly.
    fn become_leader(&mut self) {
        self.set_state(NodeState::Leader);
        self.set_leader_id(self.server_id().clone());
        info!("Node {} became the leader", self.server_id());
    }

    /// Transitions the node's state to `Candidate`.
    ///
    /// When a node becomes a candidate, it will vote for itself,
    /// update the `voted_for` attribute, change its internal state
    fn become_candidate(&mut self) {
        self.set_state(NodeState::Candidate);
        info!("Node {} became the candidate", self.server_id());
    }

    /// Transitions the node's state to `Follower`.
    ///
    /// When a node becomes a follower, this function will 
    /// update the internal state accordingly.
    fn become_follower(&mut self) {
        self.set_state(NodeState::Follower);
        info!("Node {} became the follower", self.server_id());
    }

    /// Handles the appending of log entries.
    ///
    /// Steps:
    /// 1. Reply false if `term` is less than `currentTerm`.
    /// 2. Reply false if the log doesn’t contain an entry at `prevLogIndex`
    ///    whose term matches `prevLogTerm`.
    /// 3. If an existing entry conflicts with a new one (same index
    ///    but different terms), delete the existing entry and all that
    ///    follow it.
    /// 4. Append any new entries not already in the log.
    /// 5. If `leaderCommit` is greater than `commitIndex`, set `commitIndex` 
    ///    to the minimum of `leaderCommit` and the index of the last new entry.
    async fn append_entries(&mut self, args: AppendEntriesRequest) -> AppendEntriesResponse {
        // Acquire the current term of this node.
        let current_term = self.get_current_term();
    
        // STEP 1: Verifying Term Consistency
        // In Raft, all nodes synchronize based on terms. If the incoming term is outdated (i.e., less than the current term),
        // then we should reject the request to ensure consistency. This is vital because an outdated leader shouldn't 
        // overwrite the logs of a more up-to-date node.
        if args.term < current_term {
            error!("[LOG] The incoming term is outdated. Current local term: {}. Incoming term: {}", current_term, args.term);
            return AppendEntriesResponse {
                term: self.get_current_term(),
                success: false,
            };
        }

        // STEP 2: Confirm Log Consistency
        // Here we're ensuring that the logs of the follower are consistent with what the leader believes.
        // If they're not consistent, the follower should reject the request to ensure that its logs don't 
        // diverge from the leader's logs.
        if args.prev_log_index > 0 {
            // Fetch the server's most recent log state to determine if it's consistent with the leader's perspective.
            let last_index = self.get_last_log_index().await;
            let last_entry = self.logs.read().await.get_log(last_index);
            let mut last_term: u64 = 0;
            if let Ok(Some(log_entry)) = last_entry {
                last_term = log_entry.term;
            }
    
            let mut prev_log_term: u64 = 0;
            if args.prev_log_index == last_index {
                prev_log_term = last_term;
            } else {
                let last_log_entry = self.logs.read().await.get_log(args.prev_log_index);
                if let Ok(Some(entry)) = last_log_entry {
                    prev_log_term = entry.term;
                };
            }
    
            if args.prev_log_term != prev_log_term {
                return AppendEntriesResponse {
                    term: self.get_current_term(),
                    success: false,
                };
            }
        }
    
        if args.entries.len() > 0 {
            info!("[LOG] New entries count: {}", args.entries.len());
            // STEP 3: Resolving Log Conflicts
            // It's possible that the follower has some log entries that conflict with what the leader is sending.
            // In Raft, logs flow from leaders to follower and they are considered to be upto-date. Thus, conflicting entries in the follower's 
            // log should be deleted and replaced with the entries from the leader.
          
            let last_log_index = self.get_last_log_index().await;
            let mut entries: Vec<raft::LogEntry> = vec![];
            for (idx , entry ) in args.entries.iter().enumerate() {
                if entry.index > last_log_index {
                    entries.extend_from_slice(&args.entries[idx..]);
                    // We expect entries to be store in an increasing order and we need to break out of the loop 
                    // immediately when we discover an index that's greater than the last log index, because at this point,
                    // we expect the set of log starting from this current index to be correct/valid enough to be stored.
                    break
                }

                let log_result = self.logs.read().await.get_log(entry.index);
                if let Err(e) = log_result {
                    error!("[LOG] unable to get log {:?}", e);
                    return AppendEntriesResponse {
                        term: self.get_current_term(),
                        success: false,
                    };
                }

                // Check if the term of this log is different. if its different then it means that this log was created
                // in a different term that was replicated by a leader that probably died prematurely.
                if let Ok(Some(log)) = log_result {
                    if entry.term != log.term {
                        // Remove any conflicting logs starting from the current entry index up to the last log index. 
                        // This is necessary because, by default, any logs subsequent to this point are considered 
                        // conflicting due to the monotonic increase in term and log index.
                        let delete_conflicting_log = self.logs.write().await.delete_range(entry.index, last_log_index);
                        if let Err(e) = delete_conflicting_log {
                            error!("[LOG] unable to delete conflicting logs {:?}", e);
                            return AppendEntriesResponse {
                                term: self.get_current_term(),
                                success: false,
                            };
                        }

                        // Check if configuration index is within the range of the conflicting logs. If YES
                        // Revert the log back to committed configuration 
                        if self.membership_configurations.index >= entry.index && self.membership_configurations.index <= last_log_index {
                            // Revert changes to committed.
                            self.membership_configurations.set_latest_configuration(
                                self.membership_configurations.comitted.clone(), 
                                self.membership_configurations.commit_index,
                            )
                        }
                        entries.extend_from_slice(&args.entries[idx..]);
                        break;
                    }
                }                

            }

            // STEP 4: Appending New Entries
            // Once any conflicts have been resolved, we can proceed to append new entries to the log. 
            // These are the entries that the leader has but the follower does not.
            if entries.len() > 0 {
                let converted_entries: Vec<log::LogEntry> = entries.iter()
                    .filter_map(|entry: &raft::LogEntry| log::LogEntry::from_rpc_raft_log(entry.clone()))
                    .collect();
                _ = self.logs.write().await.store_logs(&converted_entries[..]);
                
                for entry in &converted_entries {
                    info!("[LOG] log.entry: {}", entry.index);
                    match entry.log_entry_type {
                        log::LogEntryType::LogConfCommand => {
                            self.process_membership_configuration_entry(entry).await;
                        },
                        _ => {},
                    }
                }

                info!("[LOG] stored.entries.count {}......", converted_entries.len());
            }
        }
    
        // STEP 5: Update Commit Index
        // The leader might have committed some entries that the follower hasn't. This step ensures that 
        // the follower updates its commit index to reflect these newly committed entries, but without 
        // going beyond the last entry it has.
        if args.commit_index > self.get_commit_index() {
            let index = args.commit_index.min(self.get_last_log_index().await as u64);
            self.set_commit_index(index);
            debug!("[LOG] leader.commit_index: {}, node.commit_index: {}", args.commit_index,  self.get_commit_index());
            if index >= self.membership_configurations.commit_index {
                let latest_configuration = self.membership_configurations.latest.clone();
                self.membership_configurations.set_comitted_configuration(latest_configuration.clone(), self.membership_configurations.index);
                let server_id = self.server_id().to_string();
                let mut shutdown = true;
                for node in  self.membership_configurations.comitted.clone().nodes {
                    if node.id == server_id { shutdown = false }
                }
                if shutdown {
                    // Shut down current server if node is no longer part of the configuration.
                    // This shut down will only affect followers. Leader shutdown is special.
                    _ = self.shutdown();
                }
            }
            self.process_committed_logs(index, HashMap::new()).await;
        }
    
        self.set_current_term(args.term);
        self.set_leader_id(Arc::new(args.leader_id));
    
        return AppendEntriesResponse {
            term: self.get_current_term(),
            success: true,
        };
    }    

    /// Handles the request for votes.
    ///
    /// Receiver Implementation:
    /// - Reply false if `term` is less than `currentTerm`.
    /// - If `votedFor` is null or `candidateId`, and the candidate’s log 
    ///   is at least as up-to-date as the receiver’s log, grant the vote.
    async fn request_vote(&mut self, args: RequestVoteRequest) -> RequestVoteResponse {
        // STEP 1: Term Verification
        // Ensure that the term of the incoming request is not outdated. In Raft, a node should reject vote requests
        // from candidates with an outdated term. This ensures that only candidates with up-to-date information
        // can be elected as leader.
        if args.term < self.get_current_term() {
            return RequestVoteResponse {
                term: self.get_current_term(),
                vote_granted: false,
            };
        }
    
        // If the candidate's term is greater than the current term, then our node's information is outdated.
        // We need to update our term and also revert to follower state.
        if args.term > self.get_current_term() {
            // RULE:
            // If RPC request or response contains term T > currentTerm:
            // set currentTerm = T, convert to follower (§5.1)
            self.set_current_term(args.term);
            self.become_follower();
        }
    
        let last_index = self.logs.read().await.last_index() as u64;
        let last_entry = self.logs.read().await.get_log(last_index);
        let mut last_term= 0;
        if let Ok(Some(entry)) = last_entry {
            last_term = entry.term;
        }

        let last_vote_term = self.raft_state.last_vote_term();
        let last_voted_for = self.raft_state.last_voted_for();

        // STEP 2: Vote Uniqueness Check
        // Each node can vote for only one candidate per term. If our node has already voted in this term,
        // it must reject any subsequent requests for votes.
        info!("{:?} - {:?} - {:?}", self.get_voted_for().is_some() , self.get_current_term(), args.term);
        if last_voted_for.is_some() && last_vote_term == args.term {
            error!("vote.denied: request denied because this node has already voted in the current term. Candidate's last log index: {}, Local last log index: {}. Candidate's term: {}, Local Vote term: {}.", 
                args.last_log_index, 
                last_index,
                args.term,
                last_vote_term
            );
            return RequestVoteResponse {
                term: self.get_current_term(),
                vote_granted: false,
            };
        }

        // STEP 3: Log Term Consistency Check
        // Ensure our log's term is not more recent than the candidate's.
        if args.last_log_term < last_term {
            error!("vote.rejected: rejected due to invalid last log term from the candidate. Candidate's last log term: {}, Local last log term: {}.", 
                args.last_log_term, 
                last_term
            );
            return RequestVoteResponse {
                term: self.get_current_term(),
                vote_granted: false,
            };
        }
        
        // STEP 4: Log Consistency Check
        // The candidate's log must be at least as up-to-date as the log of the node it's asking a vote from.
        // This ensures that only candidates with up-to-date logs are eligible to become leaders.
        if last_term == args.last_log_term && last_index > args.last_log_index {
            error!("vote.rejected: rejected due to outdated log index from the candidate. Candidate's last log index: {}, Local last log index: {}.", 
                args.last_log_index, 
                last_index
            );
            return RequestVoteResponse {
                term: self.get_current_term(),
                vote_granted: false,
            };
        }

        // STEP 5: Granting Vote
        // If the above conditions are satisfied, the node can grant its vote to the candidate. 
        // We then update our voted_for field to the candidate's ID and also update the vote term .
        self.raft_state.set_voted_for(Some(args.candidate_id.into()));
        self.raft_state.set_vote_term(args.term);

        return RequestVoteResponse {
            term: self.get_current_term(),
            vote_granted: true,
        };
    }

    /// According to 3.10 Leadership transfer extension , the author discuss that the prior leader sends a `TimeoutNow` request to the target server. 
    /// This request has the same effect as the target server’s election timer firing: the target server starts a new election (incrementing its term and becoming a candidate).
    async fn timeout_now(&mut self)-> TimeoutNowResponse {
        self.reset_leader();
        self.become_candidate();
        TimeoutNowResponse{term: self.get_current_term(), success: true}
    }    
    /// Retrieves the current state of the node.
    ///
    /// This function attempts to lock and access the node's state.
    /// If successful, it returns the current state. 
    fn get_state(&self) -> Result<Option<NodeState>, RaftError> {
        let locked_state = self.state.lock();
        match locked_state {
            Ok(result) => {
                let state = match *result {
                    NodeState::Stopped =>  NodeState::Stopped,
                    NodeState::Follower => NodeState::Follower,
                    NodeState::Candidate => NodeState::Candidate,
                    NodeState::Leader => NodeState::Leader,
                };
                Ok(Some(state))
            },
            Err(e) =>  {
                error!("unable to unlock node state: {}", e);
                Err(RaftError::UnableToUnlockNodeState)
            }
        }
    }

    //=================\
    // RAFT SERVER LOOP 
    //=================
    //
    //                                   Election Timeout
    //  Initial State                 Initiates New Election
    //     |                                .----.
    //     |                                |    |
    //     v        Election Timeout        |    v      Gains Majority
    // +-----------+   Initiates Vote  +------------+  of Votes   +---------+
    // |  Follower |------------------>| Candidate |------------>|  Leader |
    // +-----------+                   +------------+             +---------+
    //     ^ ^                              |                           |
    //     | |    Detects Leader or         |                           |
    //     | |    Higher Term               |                           |
    //     | '------------------------------'                           |
    //     |                                                             |
    //     |                       Higher Term Detected                  |
    //     '-------------------------------------------------------------'
    //                                   Stopped
    //                                               (Step down, Removed, Sigterm, etc.)
    //
    async fn start(&mut self) {
        info!("server.loop.start");
        self.running.set(true);
       {
            _ = self.config_store.lock().await.persist(
                self.membership_configurations.comitted.clone(), 
                self.membership_configurations.commit_index,
            );
       }

        while self.running.get() {
            let state = self.get_state();
            match state {
                Ok(current_state) => {
            
                    info!("server.loop.run");
                    match current_state {
                        Some(NodeState::Follower) => self.run_follower_loop().await,
                        Some(NodeState::Candidate) => self.run_candidate_loop().await,
                        Some(NodeState::Leader) => self.run_leader_loop().await,
                        Some(NodeState::Stopped) => self.stop().await,
                        _ => {},
                    }
                }
                Err(err) => {
                    error!("server.loop.end: {:?}", err);
                    self.running.set(false);
                }
            };
        }
    }
    pub async fn run(&mut self) {   
        if let Ok(Some(state)) = self.get_state() {
            self.become_follower();
            info!("server.start.state: {:?}", state);

           self.start().await;
        } else {
            error!("Failed to start raft server");
        };
    }
    // Gracefully shutdown running background processes
    async fn stop(&mut self) {
        // shutdown FSM process using channel
        _ = self.fsm_shutdown_tx.send(()).await;
        self.running.set(false);
        info!("server.loop.stop");
    }

    fn setup_event_listener(&self, event_rx: UnboundedReceiver<ServerEventCommand>) {
        let mut mux = RaftNodeServerEventMultiplexer{
            event_rx,
            signal_fut: Some(Signals::new()),
        };

        let shutdown = self.shutdown_tx_rx.0.clone();
        tokio::spawn(async move {
            while let Some(cmd) = mux.next().await {
                match cmd {
                    ServerEventCommand::Shutdown => {
                        _ = shutdown.send(()).await;
                        sleep(Duration::from_secs(2)).await;
                        break;
                    }
                    #[allow(unreachable_patterns)]
                    _ => {
                        warn!("[Unknown] command received.");
                    }
                }
            }
        });
    }

    fn shutdown(&mut self) {
        _ = self.event_tx.send(ServerEventCommand::Shutdown);
    }
    async fn run_follower_loop(&mut self) {
        info!("server.follower.loop");
        let mut election_timeout = self.random_election_timeout();
    
        loop {
            let state = self.get_state();
            match state {
                Ok(current_state) => {
                    if current_state != Some(NodeState::Follower) {
                        break;
                    }
    
                    tokio::select! {
                        api_message_cmd = self.api_message_rx.recv() => { 
                            match api_message_cmd {
                                Some((request, response_sender))  => {
                                    let result = match request {
                                        RaftNodeServerMessage::Apply(_) => {
                                            response_sender.send(Err(RaftError::NotALeader)).await
                                        }
                                        RaftNodeServerMessage::ApplyConfChange(_) => {
                                            response_sender.send(Err(RaftError::NotALeader)).await
                                        }
                                        RaftNodeServerMessage::TransferLeadership => {
                                            response_sender.send(Err(RaftError::NotALeader)).await
                                        }
                                    };
                                    if let Err(e) = result {
                                        error!("[ERR] error handling request: {}", e);
                                    }
                                },
                                None => (),
                            }
                        },
                        notify_result = self.notify.1.recv() => {
                            match notify_result {
                                Some((_, response_sender))  => {
                                    _ = response_sender.send(Err(RaftError::NotALeader)).await;
                                },
                                None => (),
                            }
                        },
                        advance_commit_result = self.advance_commit_rx.recv() => { 
                            match advance_commit_result {
                                Some((_, response_sender))  => {
                                    _ = response_sender.send(Err(RaftError::NotALeader)).await;
                                },
                                None => (),
                            }
                        },
                        rpc_result = self.rpc_rx.recv() => match rpc_result {
                            Some((request, response_sender)) => {
                                let result = match request {
                                    RaftTransportRequest::AppendEntries(args) => {
                                        self.set_receive_heartbeat();
                                        let response = self.append_entries(args).await;
                                        response_sender.send(RaftTransportResponse::AppendEntries(response)).await
                                    }, 
                                    RaftTransportRequest::RequestVote(args) => {
                                        self.set_receive_heartbeat();
                                        let response = self.request_vote(args).await;
                                        response_sender.send(RaftTransportResponse::RequestVote(response)).await
                                    },
                                    RaftTransportRequest::TimeoutNow(_) => {
                                        let response = self.timeout_now().await;
                                        response_sender.send(RaftTransportResponse::TimeoutNow(response)).await
                                    },
                                };
                                election_timeout = self.random_election_timeout(); 
                                if let Err(e) = result {
                                    error!("error handling request: {}", e);
                                }
                            },
                            None => {
                                error!("channel was closed, restarting listener");
                            }
                        },
                        _ = tokio::time::sleep(election_timeout) => {
                            info!("Election timed-out. do stuff");
                            let nodes = &self.membership_configurations.latest.nodes;
                            let server_id = self.server_id().to_string();
                            let mut candidateship_allowed = false;

                            warn!("Nodes: {:?}", nodes);
                            for node in nodes {
                                // Only a Voter Node type can become a candidate
                                warn!("Node ID: {} Type: {:?}", node.id, node.node_type);
                                if node.id == server_id && node.node_type == NodeType::Voter {
                                    self.become_candidate();
                                    candidateship_allowed = true;
                                    break
                                }
                            }

                            if !candidateship_allowed {
                                warn!("node is unable to become candidate, {}", server_id)
                            }
                        },
                        _ = self.shutdown_tx_rx.1.recv() => { 
                            info!("Stopping the follower loop");
                            self.set_state(NodeState::Stopped);
                            return;
                        },
                    }
                },
                Err(e) => error!("unable to unlock node state: {}", e)
            }
        }
    }    
    
    /// Candidates (§5.2):
    /// 
    /// - On conversion to candidate, start election:
    ///   - Increment `currentTerm`.
    ///   - Vote for self.
    ///   - Reset election timer.
    ///   - Send `RequestVote` RPCs to all other servers.
    /// 
    /// - If votes are received from a majority of servers: become leader.
    /// 
    /// - If `AppendEntries` RPC received from new leader: convert to follower.
    /// 
    /// - If election timeout elapses: start a new election.
    async fn run_candidate_loop(&mut self) {
        info!("server.candidate.loop");

        let election_timeout = self.random_election_timeout();
        let new_term = self.get_current_term() + 1;
        info!("total votes ({:?}) needed for election in this term {:?}", self.quorum_size(), new_term);
        
        self.reset_receive_heartbeat();
        self.set_current_term(new_term);
        let nodes = &self.membership_configurations.latest.nodes.clone();
        loop { 
            
            let state = self.get_state();
            match state {
                Ok(current_state) => {
                    if current_state != Some(NodeState::Candidate) {
                        break;
                    }

                    let req = RequestVoteRequest { 
                        term: new_term, 
                        candidate_id: self.server_id().clone().to_string(), 
                        last_log_index: self.get_last_log_index().await, 
                        last_log_term: self.get_last_log_term().await,
                        commit_index: self.get_commit_index(), 
                    };
                    let begin_election = election::campaign(nodes,self.server_id(),req).await;
                  
                    // info!("Running as {:?}...", current_state.clone().unwrap());  
                    if self.received_heartbeat.swap(false, Ordering::SeqCst) {
                        info!("Looks like there is a leader, stepping down now. {:?}", self.received_heartbeat);
                        self.become_follower();
                    }

                    tokio::select! {
                        election_result = begin_election => {
                            let step_down = self.handle_vote_result(election_result, new_term).await;
                            if step_down {
                                return;
                            }
                        },
                        _ = self.step_down.1.recv() => {
                            self.become_follower();
                        }
                        api_message_cmd = self.api_message_rx.recv() => { 
                            match api_message_cmd {
                                Some((request, response_sender))  => {
                                    _ = match request {
                                        RaftNodeServerMessage::Apply(_) => {
                                            response_sender.send(Err(RaftError::NotALeader)).await
                                        }
                                        RaftNodeServerMessage::ApplyConfChange(_) => {
                                            response_sender.send(Err(RaftError::NotALeader)).await
                                        }
                                        RaftNodeServerMessage::TransferLeadership => {
                                            response_sender.send(Err(RaftError::NotALeader)).await
                                        }
                                    };
                                },
                                None => (),
                            }
                        },
                        notify_result = self.notify.1.recv() => {
                            match notify_result {
                                Some((_, response_sender))  => {
                                    _ = response_sender.send(Err(RaftError::NotALeader)).await;
                                },
                                None => (),
                            }
                        },
                        advance_commit_rx_result = self.advance_commit_rx.recv() => { 
                            match advance_commit_rx_result {
                                Some((_, response_sender))  => {
                                    _ = response_sender.send(Err(RaftError::NotALeader)).await;
                                },
                                None => (),
                            }
                        },
                        rpc_result = self.rpc_rx.recv() => match rpc_result {
                            Some((request, response_sender)) => {
                                let result = match request {
                                    RaftTransportRequest::AppendEntries(args) => {
                                        self.set_receive_heartbeat();
                                        let response = self.append_entries(args).await;
                                        response_sender.send(RaftTransportResponse::AppendEntries(response)).await
                                    }, 
                                    RaftTransportRequest::RequestVote(args) => {
                                        self.set_receive_heartbeat();
                                        let response = self.request_vote(args).await;
                                        response_sender.send(RaftTransportResponse::RequestVote(response)).await
                                    },
                                    RaftTransportRequest::TimeoutNow(_) => {
                                        let response = self.timeout_now().await;
                                        response_sender.send(RaftTransportResponse::TimeoutNow(response)).await
                                    },
                                };
                                if let Err(e) = result {
                                    error!("error handling request: {}", e);
                                }
                            },
                            None => {
                                error!("channel was closed, restarting listener");
                            }
                        },
                        _ = tokio::time::sleep(election_timeout) => {
                            info!("Election timeout reached, restarting election...");
                            return;
                        },
                        _ = self.shutdown_tx_rx.1.recv() => { 
                            info!("Stopping the candidate loop");
                            self.set_state(NodeState::Stopped);
                            return;
                        },
                    }
                }
                Err(e) => error!("unable to unlock node state: {}", e)
            }
        }
    }

    async fn handle_vote_result(&mut self, election_result: Vec<(String, RequestVoteResponse)> , new_term: u64)-> bool{
        let mut step_down: bool = false;
        let mut granted_votes = 0;
        // vote result
        for (node_id, vote_response) in election_result {

            if node_id == *self.server_id() {
                granted_votes += 1;
                self.raft_state.set_voted_for(Some(node_id.clone()));
                self.raft_state.set_vote_term(new_term);
        
                info!("self.vote.granted: {:?}", node_id);
                continue;
            }
        
            if vote_response.term > self.get_current_term() {
                // Voting Rule According to RAFT:
                // If RPC request or response contains term T > currentTerm:
                // set currentTerm = T, convert to follower (§5.1)
                info!("Looks like the received term is greater than the current term, step down....");
                self.set_current_term(vote_response.term);
                self.become_follower();
                step_down = true;
                return step_down;
            }

            if vote_response.vote_granted {
                granted_votes += 1;
                info!("peer.vote.granted: {:?}", node_id);
            }
        }   

        info!("server.candidate.granted_votes: {:?}", granted_votes);
        if granted_votes >= self.quorum_size() {
            info!("server.election.won ");
            self.become_leader();
        }

        step_down
    }

    async fn run_leader_loop(&mut self) {
        info!("server.leader.loop");

        // Setup leader states
        let start_index = self.get_last_log_index().await + 1;
        self.set_start_index(start_index);
        self.inflight_entry_queue = VecDeque::new();
        self.replica_nodes = None;
        

        // Establishes an immediate connection with replica nodes for the current leader and sends a heartbeat request.
        // This operation is crucial for maintaining communication with replica nodes and verifying their liveness.   
        self.establish_replica_connections().await;

        // Dispatches a No_OP log entry to assert dominance as the new leader of the system.
        // The No_OP log serves the purpose of announcing leadership status without performing any substantive operation.
        // This operation is significant in leader election scenarios to inform other nodes of the leadership change.
        self.dispatch_no_op_log().await;

        loop {
            let state = self.get_state();
            match state{
                Ok(current_state) => {
                    if current_state != Some(NodeState::Leader) {
                        break
                    }
                    
                    tokio::select! {
                        api_message_cmd = self.api_message_rx.recv() => { 
                            match api_message_cmd {
                                Some((request, response_sender))  => {
                                    let result = match request {
                                        RaftNodeServerMessage::Apply(data) => {
                                            if self.leadership_transfer_progress.load(Ordering::SeqCst) {
                                                info!("leadership.transfer.progress");
                                                response_sender.send(Err(RaftError::LeadershipTransferInProgress)).await
                                            } else {
                                                info!("Handle log entry ===/ ");

                                                let last_log_index = self.get_last_log_index().await + 1;
                                                let new_log = log::LogEntry {
                                                    log_entry_type: log::LogEntryType::LogCommand,
                                                    index: last_log_index,
                                                    data,
                                                    term: self.get_current_term(),
                                                };

                                                _ = self.handle_log_entry(new_log).await;
                                                response_sender.send(Ok(())).await
                                            }
                                        }
                                        RaftNodeServerMessage::ApplyConfChange(command) => {
                                            if self.leadership_transfer_progress.load(Ordering::SeqCst) {
                                                info!("leadership.transfer.progress");
                                                response_sender.send(Err(RaftError::LeadershipTransferInProgress)).await
                                            } else {
                                                info!("configuration change command {:?} ", command);
                                                if self.can_modify_configuration() {
                                                    self.append_configuration_entry(command).await;
                                                    self.establish_replica_connections().await;
                                                    response_sender.send(Ok(())).await
                                                } else {
                                                    response_sender.send(Err(RaftError::PendingConfiguration)).await
                                                }
                                            }
                                        }
                                        RaftNodeServerMessage::TransferLeadership => {
                                            if self.leadership_transfer_progress.load(Ordering::SeqCst) {
                                                info!("leadership.transfer.progress");
                                                response_sender.send(Err(RaftError::LeadershipTransferInProgress)).await
                                            } else {
                                                info!("leadership.transfer.begin");
                                                response_sender.send(Ok(())).await
                                            }
                                        }
                                    };

                                    if let Err(e) = result {
                                        error!("error handling request: {}", e);
                                    }
                                },
                                None => (),
                            }
                        },
                        notify_result = self.notify.1.recv() => {
                            match notify_result {
                                Some((_, response_sender))  => {
                                    for (_, node) in self.replica_nodes_txn.as_mut().unwrap() {
                                        if let Err(e) = node.entry_replica_tx.send(()) {
                                            _ = response_sender.send(Err(RaftError::Error(e.to_string()))).await;
                                            break;
                                        }
                                    }
                                    _ = response_sender.send(Ok(())).await;
                                    
                                },
                                None => (),
                            }
                        },
                        advance_commit_result = self.advance_commit_rx.recv() => { 
                            match advance_commit_result {
                                Some((_, response_sender))  => {
                                    info!("advance.commit_index.start");
                                    _ = response_sender.send(Ok(())).await;

                                    // Advance leader commit Index by calculating the matching indexes of the replica nodes
                                    let current_commit_index = self.raft_state.get_commit_index();
                                    if let Some(quorum_index) = self.compute_commit_index().await {
                                        // If there exists an N such that:
                                        //   - N > commitIndex.
                                        //   - A majority of matchIndex[i] ≥ N.
                                        //   - log[N].term == currentTerm.
                                        //   - Set commitIndex = N (§5.3, §5.4).
                                        info!("quorum_index: {}, current_commit_index: {}", quorum_index , current_commit_index);
                                        if quorum_index > current_commit_index {

                                            // We need to ensure that the new configuration that we are about to commit occured
                                            // after the previous commit index and the new commit index is greater than or equal to our current committed configuration index
                                            if current_commit_index <  self.membership_configurations.index && quorum_index >= self.membership_configurations.commit_index {
                                                let latest_configuration = self.membership_configurations.latest.clone();
                                                self.membership_configurations.set_comitted_configuration(latest_configuration, self.membership_configurations.index)
                                            }
 
                                            self.set_commit_index(quorum_index);
                                            _ = self.process_commit_tx.send(quorum_index);
                                        }
                                    }
                                    info!("advance.commit_index.done");
                                },
                                None => (),
                            }
                        },
                        _ = self.process_commit_rx.recv() => {
                            info!("logs.process.commit.start");
                            let commit_index = self.get_commit_index();
                            
                            let mut inflight_logs: HashMap<u64, log::LogEntry> = HashMap::new();
                            let mut last_commit_index = 0;
                        
                            let mut pop_count = 0;
                            for log_entry in &self.inflight_entry_queue {
                                if log_entry.index > commit_index {
                                    break;
                                }
                        
                                info!("Processing entry: {:?}", log_entry);
                                inflight_logs.insert(log_entry.index, log_entry.clone());
                                last_commit_index = log_entry.index;
                                pop_count += 1;
                            }
                        
                            if !inflight_logs.is_empty() {
                                self.process_committed_logs(last_commit_index, inflight_logs).await;
                                for _ in 0..pop_count {
                                    self.inflight_entry_queue.pop_front();
                                }
                            }
                            info!("logs.process.commit.end");
                        },           
                        _ = self.step_down.1.recv() => {
                            self.reset_leader();
                            self.become_follower();
                        }             
                        _ = self.shutdown_tx_rx.1.recv() => { 
                            info!("Stopping the leader loop");

                            // stop all replica nodes for current leader
                            self.stop_replica_nodes().await;
                            self.set_state(NodeState::Stopped);
                            return;
                        },
                        
                    }
                },
                Err(e) =>  error!("unable to unlock node state: {}", e)
            }
        }

    }

    fn reset_leader(&mut self) {
        self.inflight_entry_queue = VecDeque::new();
        self.replica_nodes = None;
        self.set_leader_id(Arc::new("".to_string()));
    }

    fn set_state(&mut self, new_state: NodeState) {
        let mut current_state = self.state.lock().unwrap();
        *current_state = new_state;
    }

    fn set_receive_heartbeat(&mut self) {
        self.received_heartbeat.store(true, Ordering::SeqCst);
    }

    fn reset_receive_heartbeat(&mut self) {
        self.received_heartbeat.swap(false, Ordering::SeqCst) ;
    }

    fn random_election_timeout(&mut self) -> Duration {
        rand::thread_rng().gen_range(Duration::from_secs(self.conf.election_timeout_min)..Duration::from_secs(self.conf.election_timeout_max))
    }
    /// Checks if there is no pending configuration change and it also checks that a 
    /// leader has committed an entry from its current term..
    ///
    /// This follows the guidelines discussed in the Raft consensus algorithm.
    ///
    /// Reference:
    /// "Bug in single-server membership changes", a discussion in the Raft-dev Google group.
    /// https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J)
    ///
    fn can_modify_configuration(&self) -> bool {
        self.membership_configurations.index == self.membership_configurations.commit_index && self.get_commit_index() >= self.get_start_index()
    }

    fn quorum_size(& self) -> u64 {
        // TODO: If a server is known to be down or unresponsive, it should not be counted as part of the quorum during the election process.
        // Because we need to ensure that a new leader is elected based on the availability of the majority of nodes, excluding 
        // the unreachable or failed nodes from the decision-making process.
        let mut qorum_size: u64 = 0;
        for node in  self.membership_configurations.latest.nodes.iter() {
            if node.node_type == node::NodeType::Voter {
                qorum_size +=1
            }
        }

        // NOTE: Adding 1 to this value ensures that the number of nodes required for a quorum is more than half. 
        // This is important because, in case of disagreements or network splits, this ensures that any decision made is agreed upon by the majority.
        // E.G: if there are 5 voting nodes, the quorum size calculated would be (5 / 2) + 1 = 3. 
        // This means at least 3 out of 5 nodes must agree on a decision, which is a majority. Without the +1, 
        // the quorum would be just 2 (half of 5, and this value would typically be rounded down to 2 when dealing with whole numbers),
        //  which is not sufficient for a majority decision.
        qorum_size  / 2  + 1
    }

    ///  Establishes an immediate connection based on the current nodes.
    ///
    /// This function attempts to create new `ReplicaNode`s for each node, 
    /// waits for their asynchronous initialization, and then stores the 
    /// successfully initialized nodes`.
    async fn establish_replica_connections(&mut self) {
        let current_term = self.get_current_term();
        let next_index = self.get_last_log_index().await + 1;
        let match_index = self.get_last_log_index().await;
        let server_id = self.server_id();
        let heartbeat_interval = self.conf.heartbeat_interval;
        let previous_replica_nodes = self.replica_nodes.to_owned();
        if previous_replica_nodes.is_some() {
            info!("nodes.previous: {:?}", previous_replica_nodes.clone().unwrap().keys());
        }

        let nodes = &*self.membership_configurations.latest.nodes;
        let replica_nodes_instance_futures = nodes.iter().map(|node| {
            let server_id = server_id.clone();
            let id = node.id.clone();
            let mut skip= false;
            // Check if the node already exists in previous replica nodes, If YES, skip.
            if previous_replica_nodes.as_ref().map_or(false, |replica_nodes| replica_nodes.contains_key(&id.to_string())) {
                skip = true;
            }

            async move { 
                if skip { 
                    return None;
                }

                // Ignore current raft server.
                if server_id.to_string() == id {
                    return None;
                }

                let result = ReplicaNode::new(
                    node.clone(),
                    current_term,
                    next_index,
                    heartbeat_interval,
                    match_index,
                ).await;
                match result {
                    Ok(replica) => {
                        let replica_shared = Arc::new(tokio::sync::Mutex::new(replica));
                        Some((id, replica_shared))
                    },
                    Err(_) => None,
                }
            }
        });        

        let results: Vec<_> = FuturesUnordered::from_iter(replica_nodes_instance_futures).collect().await;
        let new_replica_nodes: HashMap<String, Arc<tokio::sync::Mutex<ReplicaNode>>> = results.into_iter().filter_map(|x| x).collect();
        if let Some(ref mut replica_nodes) =  self.replica_nodes {
            replica_nodes.extend(new_replica_nodes.clone());
        } else {
            self.replica_nodes = Some(new_replica_nodes.clone());
        }
        
        let mut new_replica_nodes_txn: HashMap<String, ReplicaNodeTxn> = HashMap::new();

        for (node_id, replica_node) in new_replica_nodes.iter() {

               let (stop_heartbeat_rx, stop_heartbeat_tx) = tokio::sync::mpsc::channel::<()>(1);
               {
                    let replica_node_clone: Arc<tokio::sync::Mutex<ReplicaNode>> = replica_node.clone();
                    let leader_id = self.server_id().clone();
                    let raft_state = self.raft_state.clone();
                    
                    tokio::task::spawn(async move {
                        ReplicaNode::run_periodic_heartbeat(stop_heartbeat_tx, leader_id, raft_state, replica_node_clone).await;
                    });
               }

               {
                    let replica_node_clone: Arc<tokio::sync::Mutex<ReplicaNode>> = replica_node.clone();
                    let raft_state = self.raft_state.clone();
                    let logs = self.logs.clone();
                    let advance_commit_tx = self.advance_commit_tx.clone();
                    
                    let (entry_replica_tx, entry_replica_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
                    let replica_node_txn = ReplicaNodeTxn{entry_replica_tx};
                    
                    new_replica_nodes_txn.insert(node_id.to_string(), replica_node_txn);
                    tokio::task::spawn(async move {
                        ReplicaNode::run(logs,stop_heartbeat_rx,  advance_commit_tx, raft_state, replica_node_clone, entry_replica_rx).await;
                    });
               } 
        }

        if let Some(ref mut nodes_txn) = self.replica_nodes_txn  {
            nodes_txn.extend(new_replica_nodes_txn);
        } else {
            self.replica_nodes_txn = Some(new_replica_nodes_txn);
        }

        // Stop replicas that are not in the nodes list anymore
        if let Some(mut replicas) = self.replica_nodes.clone(){
            let current_node_ids: HashSet<String> = nodes.iter().map(|node| node.id.clone()).collect();
            
            let mut nodes_to_remove: Vec<String> = vec![];

            if let Some(replica_nodes) = &self.replica_nodes {
                nodes_to_remove = replica_nodes
                    .keys()
                    .filter(|id| !current_node_ids.contains(*id))
                    .cloned()
                    .collect();
            }
            if nodes_to_remove.len() > 0 {
                info!("nodes.remove: {:?}", nodes_to_remove);
            }

            // Remove nodes and stop them
            for node_id in nodes_to_remove {
                if let Some(replica_node) = replicas.remove(&node_id.clone()) {
                    let mut replica = replica_node.lock().await;
                    replica.stop().await;
                }
            }
            self.replica_nodes = Some(replicas);
        }
    }

    async fn stop_replica_nodes(&mut self) {
        for (_, node) in self.replica_nodes.as_mut().unwrap() {
            node.lock().await.stop().await;
        }
    }

    async fn dispatch_no_op_log(&mut self) {
        let last_log_index = self.get_last_log_index().await + 1;
        let new_log = log::LogEntry {
            log_entry_type: log::LogEntryType::LogNoOp,
            index: last_log_index,
            data: vec![],
            term: self.get_current_term(),
        };
        _ = self.handle_log_entry(new_log).await;
    }

    async fn handle_log_entry(&mut self, mut new_log: log::LogEntry) -> Result<(), RaftError> {    
        info!("Creating new log entry: {:?}", new_log);

        // Keep log up-to-date
        let last_log_index = self.get_last_log_index().await + 1;
        new_log.index = last_log_index;
        new_log.term = self.get_current_term();

        // Track log inflight for processing when committed (only applicable to the leader).
        self.inflight_entry_queue.push_back(new_log.clone());
    
        //Attempt to store the new log entry.
        let store_log_result = {
            let mut log_writer = self.logs.write().await;
            log_writer.store_log(&new_log)
        };

        match store_log_result {
            Ok(_) => {
                info!("log.latest.index: {}", last_log_index);
                let (tx, mut rx ) = tokio::sync::mpsc::channel::<Result<(), RaftError>>(1);
                let notify = self.notify.0.send(((), tx));
                tokio::spawn(async move {
                    match notify {
                        Ok(_) => {
                            match rx.recv().await {
                                Some(Err(e)) => error!("[ERR] notify nodes failed: {}", e),
                                Some(Ok(_)) => info!("Notification sent successfully"),
                                _ => ()
                            }
                        },
                        Err(e) =>  error!("[ERR] Failed to send notification: {}", e)
                    }
                });
                Ok(())
            }
            Err(e) => {
                error!("Failed to store log entry: {:?}", e);
                self.become_follower();
                Err(RaftError::LogEntryFailed)
            }
        }
    }
    
    /// Calculate the highest index that has been replicated on a majority of them.
    /// TODO: locking and unlocking replica nodes will cause other lock to be blocked until it is released
    /// refactor match Indexes into a seperate modules and seperate it entirely from the replica node module
    async fn compute_commit_index(&self) -> Option<u64> {
        if let Some(replicas) = self.replica_nodes.as_ref() {
            info!("commit_index.compute");
            // Obtain match indexes from all replicas
            let mut match_indexes: Vec<u64> = futures::future::join_all(replicas.values().map(|replica| {
                async {
                    let guard_replica = replica.lock().await;
                    guard_replica.match_index
                }
            })).await;
            
            // Sort the match indexes
            match_indexes.sort_unstable();
            
            // Find the middle index which represents the majority in a sorted list
            let middle = replicas.len() / 2; 
            
            return match_indexes.get(middle).cloned();
        }
        None
    }

    // 3.10 Leadership transfer extension
    async fn select_random_node(&self) -> Option<String> {
        if let Some(replicas) = self.replica_nodes.as_ref() {
            info!("node.pick");

            // Obtain match indexes from all replicas and store them in a HashMap
            let match_indexes: HashMap<String, u64> = futures::future::join_all(replicas.iter().map(|(node_id, replica)| {
                async {
                    let guard_replica = replica.lock().await;
                    (node_id.clone(), guard_replica.match_index)
                }
            })).await.into_iter().collect();

            let mut sorted_match_indexes: Vec<(&String, &u64)> = match_indexes.iter().collect();
            // Sort the match indexes in descending order to get the highest match_index first
            sorted_match_indexes.sort_unstable_by(|a, b| b.1.cmp(&a.1));

            if let Some((node_id, _)) = sorted_match_indexes.first() {
                return Some(node_id.to_string());
            }
        }
        None
    }
    /// Processes committed log entries.
    ///
    /// This method applies committed logs to the state machine
    /// based on the Raft consensus algorithm's rules. 
    async fn process_committed_logs(&mut self, last_commit_index: u64, committed_logs: HashMap<u64, log::LogEntry>) {
        // Rule:
        // If commitIndex > lastApplied: increment lastApplied, apply
        // log[lastApplied] to state machine (§5.3)
        let last_applied = self.get_last_applied();
        // info!("Last Applied: {}", last_applied);
        // info!("Last Commit Index: {}", last_commit_index);
        
        if last_commit_index <= last_applied {
            return;
        }
    
        let mut batched_logs : Vec<log::LogEntry> = Vec::new();
        for idx in (last_applied + 1)..=last_commit_index {
            match committed_logs.get(&idx) {
                Some(entry)  => {
                    if entry.index > last_applied && entry.index <= last_commit_index  {
                        match entry.log_entry_type {
                            log::LogEntryType::LogCommand | log::LogEntryType::LogConfCommand => {
                                batched_logs.push(entry.clone());
                            },
                            _ => (),
                        }
                    }  else {
                        break
                    }
                },
                None => {
                    match self.logs.read().await.get_log(idx) {
                        Ok(Some(entry)) => {
                            match entry.log_entry_type {
                                log::LogEntryType::LogCommand | log::LogEntryType::LogConfCommand => {
                                    batched_logs.push(entry.clone());
                                },
                                _ => (),
                            }
                        },
                        Ok(None)  => break,
                        Err(e) => {
                            error!("Unable to batch log: {:?}", e);
                            break;
                        },
                    }
                },
            }
        }
        
        if batched_logs.len() > 0 {
            let result = self.fsm_apply_tx.send(batched_logs);
            match result {
                Ok(_) => info!("fsm.apply.send.success"),
                Err(e) =>  error!("fsm.apply failed to send log: {:?}", e),
            }
        }
        self.set_last_applied(last_commit_index);
    }

    /// Appends a new configuration entry to the log.
    ///
    /// This method creates and appends a new configuration entry
    /// to the log based on the provided configuration command. This is part of
    /// managing cluster membership changes in the Raft consensus algorithm.
    async fn append_configuration_entry(&mut self, command: ConfigCommand) {
        let configuration = Configuration::new_configuration(&self.membership_configurations.latest, command);
        
        let index = self.get_last_log_index().await as u64 + 1;
        let term =  self.get_current_term();
        let log_entry_type = log::LogEntryType::LogConfCommand;

        let serialized_data_result = configuration.serialize();
        
        match serialized_data_result {
            Ok(data) => {
                let log = log::LogEntry{log_entry_type, index, term, data};
                _ = self.handle_log_entry(log).await;
            }  
            Err(e) => {
                error!("Unable to serialize configuration {:?}", e);
            }
        }

        self.membership_configurations.set_latest_configuration(configuration, index);
    }
    async fn process_membership_configuration_entry(&mut self, log: &log::LogEntry) {
        let deseralize_config_result: Result<Configuration, Box<bincode::ErrorKind>> = Configuration::deserialize(&log.data);
        match deseralize_config_result {
            Ok(configuration) => {
                self.membership_configurations.set_latest_configuration(configuration, log.index)
            },
            Err(e)=> {
                error!("unable to process config entry: {:?}", e)
            },
        }   
    }
}
