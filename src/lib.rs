use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use config::Config;
use error::RaftError;
use fsm::FSM;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt; 
use ::log::*;
use rand::Rng;
use node::{Node, ReplicaNode};
use stable::StableStore;
use storage::LogStore;
use tokio::sync::RwLock;
use crate::state::{NodeState, RaftState};
use std::collections::HashMap;
use crate::raft::{
    AppendEntriesRequest, AppendEntriesResponse,
    RequestVoteRequest, RequestVoteResponse,
};
use std::collections::VecDeque;
use crate::grpc_transport::{RaftTransportResponse,RaftTransportRequest};

mod configuration;
pub mod storage;
mod state;
pub mod error;
mod utils;
mod mocks;
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
   Apply(Vec<u8>)
}

/// `RaftNodeServer` represents a node in a Raft cluster. 
///
/// It encapsulates the state and behavior associated with the Raft consensus algorithm.
pub struct RaftNodeServer {
    /// Unique identifier for the node in the Raft cluster.
    id: Arc<String>,

    /// Current state of the node (e.g., follower, candidate, or leader).
    state:  Arc<Mutex<NodeState>>,

    /// Leader's unique identifier. If `None`, the cluster doesn't currently have a leader.
    leader_id: Option<Arc<String>>,

    /// The state of the Raft node.
    raft_state: Arc<RaftState>,

    /// Configuration details for the Raft node.
    conf: Config,

    /// List of all known nodes in the cluster.
    nodes: Arc<Vec<Node>>,

    /// Replicas of other nodes in the Raft cluster.
    replica_nodes: Option<HashMap<String, Arc<tokio::sync::Mutex<ReplicaNode>>>>,

    /// State machine that the Raft algorithm operates on.
    fsm: Box<dyn FSM>,

    /// Log entries storage mechanism.
    logs: Arc<RwLock<Box<dyn LogStore>>>,

    /// Channels to signal stopping server background task.
    shutdown_tx_rx: (tokio::sync::mpsc::Sender<()>, tokio::sync::mpsc::Receiver<()>),

    /// A flag indicating whether a heartbeat has been received.
    received_heartbeat: Arc<AtomicBool>,

    /// Channel to receive incoming Raft RPCs.
    rpc_rx: tokio::sync::mpsc::Receiver<(RaftTransportRequest, tokio::sync::mpsc::Sender<RaftTransportResponse>)>,

    // Leader specific channel for processing commit index
    process_commit_tx: tokio::sync::mpsc::Sender<u64>, 
    process_commit_rx: tokio::sync::mpsc::Receiver<u64>,

    // Leader specific channel for advancing commit index 
    advance_commit_tx: tokio::sync::mpsc::Sender<((), tokio::sync::mpsc::Sender<Result<(), RaftError>>)>,
    advance_commit_rx: tokio::sync::mpsc::Receiver<((), tokio::sync::mpsc::Sender<Result<(), RaftError>>)>,

    fsm_apply_tx: tokio::sync::mpsc::Sender<Vec<log::LogEntry>>,
    fsm_apply_rx: tokio::sync::mpsc::Receiver<Vec<log::LogEntry>>,
 
    // Leader specific queue for applying log entry based on FIFO
    inflight_entry_queue: VecDeque<log::LogEntry>,

    /// Channel interacting with the Raft Node server, this is a temporary solution. 
    /// There should be a way to run the Raft Node server in a way that's shareable for multiple threads to call.
    api_message_rx: tokio::sync::mpsc::UnboundedReceiver<(RaftNodeServerMessage, tokio::sync::mpsc::Sender<Result<(), RaftError>>)>,
}


impl RaftNodeServer {
    pub async fn new(
        id: String,
        conf: Config,
        nodes: Vec<Node>,
        fsm: Box<dyn FSM>,
        stable: Box<dyn StableStore>,
        logs: Box<dyn LogStore>,
        rpc_rx: tokio::sync::mpsc::Receiver<(RaftTransportRequest, tokio::sync::mpsc::Sender<RaftTransportResponse>)>,
        api_message_rx: tokio::sync::mpsc::UnboundedReceiver<(RaftNodeServerMessage, tokio::sync::mpsc::Sender<Result<(), RaftError>>)>,
    ) -> Self {
        let shutdown_tx_rx = tokio::sync::mpsc::channel::<()>(1);
        let (advance_commit_tx, advance_commit_rx, ) = tokio::sync::mpsc::channel::<((), tokio::sync::mpsc::Sender<Result<(), RaftError>>)>(1);
        let ( process_commit_tx,  process_commit_rx, ) = tokio::sync::mpsc::channel::<u64>(1);
        let ( fsm_apply_tx,  fsm_apply_rx, ) = tokio::sync::mpsc::channel::<Vec<log::LogEntry>>(100);
       
        env_logger::Builder::new()
            .filter_level(::log::LevelFilter::Info) 
            .filter_module("sled", ::log::LevelFilter::Warn) 
            .init();

        let raft_state =  Arc::new(RaftState::new(stable));


        Self {
            state: Arc::new(Mutex::new(NodeState::Follower)),
            conf,
            id: Arc::new(id),
            inflight_entry_queue: VecDeque::new(),
            advance_commit_tx,
            advance_commit_rx,
            fsm_apply_tx,
            fsm_apply_rx,
            process_commit_rx,
            process_commit_tx,
            leader_id: None,
            shutdown_tx_rx,
            api_message_rx,
            nodes: Arc::new(nodes),
            replica_nodes: None,
            received_heartbeat: Arc::new(AtomicBool::new(false)),
            fsm,
            logs: Arc::new(RwLock::new(logs)),
            rpc_rx,
            raft_state,
        }
    }


    /// Return true if current node is the leader
    pub fn is_leader(&self) -> bool {
        self.leader_id.is_some() && self.leader_id.clone().unwrap() == self.id
    }

    /// Sets the current leaderid
    pub fn set_leader_id(&mut self, leader_id: String) {
        self.leader_id = Some(Arc::new(leader_id));
    }

    /// Return true if current node is the leader
    pub fn server_id(&self) -> Arc<String> {
        self.id.clone()
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
    fn get_votes(&self) -> u64 {
        self.raft_state.get_votes()
    }

    /// Sets the number of votes the node has received.
    fn set_votes(&self, value: u64) {
        self.raft_state.set_votes(value);
    }

    /// Retrieves the identifier of the node that this node has voted for in the current term.
    fn get_voted_for(&self) -> Option<String> {
        self.raft_state.get_voted_for()
    }

    /// Sets the identifier of the node that this node votes for.
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
        info!("Node {} became the leader", self.id);
    }

    /// Transitions the node's state to `Candidate`.
    ///
    /// When a node becomes a candidate, it will vote for itself,
    /// update the `voted_for` attribute, change its internal state
    fn become_candidate(&mut self) {
        self.set_votes(1); // increment vote for self.
        self.set_voted_for(Some(self.id.to_string()));
        self.set_state(NodeState::Candidate);
        info!("Node {} became the candidate", self.id);
    }

    /// Transitions the node's state to `Follower`.
    ///
    /// When a node becomes a follower, this function will 
    /// update the internal state accordingly.
    fn become_follower(&mut self) {
        self.set_state(NodeState::Follower);
        info!("Node {} became the follower", self.id);
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
            error!("The incoming term is outdated. Current local term: {}. Incoming term: {}", current_term, args.term);
            return AppendEntriesResponse {
                term: self.get_current_term(),
                success: false,
            };
        }
    
        if args.prev_log_index > 0 {
            // Fetch the server's most recent log state to determine if it's consistent with the leader's perspective.
            let last_index = self.logs.read().await.last_index();
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
    
            // STEP 2: Confirm Log Consistency
            // Here we're ensuring that the logs of the follower are consistent with what the leader believes.
            // If they're not consistent, the follower should reject the request to ensure that its logs don't 
            // diverge from the leader's logs.
            if args.prev_log_term != prev_log_term {
                return AppendEntriesResponse {
                    term: self.get_current_term(),
                    success: false,
                };
            }
        }
    
        if args.entries.len() > 0 {
            // STEP 3: Resolving Log Conflicts
            // It's possible that the follower has some log entries that conflict with what the leader is sending.
            // In Raft, logs flow from leaders to follower and they are considered to be upto-date. Thus, conflicting entries in the follower's 
            // log should be deleted and replaced with the entries from the leader.
            // TODO: Implement this logic to remove conflicting entries.
    
            // STEP 4: Appending New Entries
            // Once any conflicts have been resolved, we can proceed to append new entries to the log. 
            // These are the entries that the leader has but the follower does not.
            let converted_entries: Vec<log::LogEntry> = args.entries.iter()
                .filter_map(|entry: &raft::LogEntry| log::LogEntry::from_rpc_raft_log(entry.clone()))
                .collect();
            _ = self.logs.write().await.store_logs(&converted_entries[..]);
        }
    
        // STEP 5: Update Commit Index
        // The leader might have committed some entries that the follower hasn't. This step ensures that 
        // the follower updates its commit index to reflect these newly committed entries, but without 
        // going beyond the last entry it has.
        if args.commit_index > 0 && args.commit_index > self.get_commit_index() {
            let index = args.commit_index.min(self.get_last_log_index().await as u64);
            self.set_commit_index(index);
            self.process_committed_logs(index, HashMap::new()).await;
        }
    
        // Synchronize the follower's perspective of the current term and leader ID with the information from the leader.
        self.set_current_term(args.term);
        self.set_leader_id(args.leader_id);
    
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
        // We need to update our term, revert to follower state, and reset our voted_for status to ensure we
        // can participate in the new election round.
        if args.term > self.get_current_term() {
            self.set_current_term(args.term);
            self.become_follower();
            self.set_voted_for(None);
        }
    
        let last_index = self.logs.read().await.last_index() as u64;
        let last_entry = self.logs.read().await.get_log(last_index);
        let mut last_term= 0;
        if let Ok(Some(entry)) = last_entry {
            last_term = entry.term;
        }

        // STEP 2: Vote Uniqueness Check
        // Each node can vote for only one candidate per term. If our node has already voted in this term,
        // it must reject any subsequent requests for votes.
        if self.get_voted_for().is_some() && self.get_current_term() == args.term {
            info!("Vote request denied because this node has already voted in the current term. Candidate's last log index: {}, Local last log index: {}. Candidate's term: {}, Local last term: {}.", 
                args.last_log_index, 
                last_index,
                args.term,
                last_term
            );
            return RequestVoteResponse {
                term: self.get_current_term(),
                vote_granted: false,
            };
        }

        // STEP 3: Log Term Consistency Check
        // Ensure our log's term is not more recent than the candidate's.
        if args.last_log_term < last_term {
            info!("Vote rejected due to invalid last log term from the candidate. Candidate's last log term: {}, Local last log term: {}.", 
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
            info!("Vote rejected due to outdated log term from the candidate. Candidate's last log index: {}, Local last log index: {}.", 
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
        // We then update our voted_for field to the candidate's ID and synchronize our current term with the candidate's term.
        self.set_voted_for(Some(args.candidate_id.into()));
        self.set_current_term(args.term);

        return RequestVoteResponse {
            term: self.get_current_term(),
            vote_granted: true,
        };
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
    
    async fn start(&mut self) {
        info!("server.loop.start");

        loop {
            let state = self.get_state();
            match state {
                Ok(current_state) => {
            
                    info!("server.loop.run");
                    match current_state {
                        Some(NodeState::Follower) => self.run_follower_loop().await,
                        Some(NodeState::Candidate) => self.run_candidate_loop().await,
                        Some(NodeState::Leader) => self.run_leader_loop().await,
                        _ => {},
                    }
                }
                Err(err) => {
                    error!("server.loop.end: {:?}", err);
                    break
                }
            };
        }
    }
    pub async fn run(&mut self) {   
        if let Ok(Some(state)) = self.get_state() {
            // become follower
            self.become_follower();
            info!("server.start.state: {:?}", state);

           self.start().await;
        } else {
            error!("Failed to start raft server");
        };
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
    
                    // info!("Running as {:?}...", current_state.clone().unwrap()); 
    
                    tokio::select! {
                        
                        msg = self.api_message_rx.recv() => { 
                            match msg {
                                Some((request, response_sender))  => {
                                    let result = match request {
                                        RaftNodeServerMessage::Apply(_) => {
                                            response_sender.send(Err(RaftError::NotLeader)).await
                                        }
                                    };
                                    if let Err(e) = result {
                                        error!("error handling request: {}", e);
                                    }
                                },
                                None => {},
                            }
                        },
                        result = self.advance_commit_rx.recv() => { 
                            match result {
                                Some((_, response_sender))  => {
                                    _ = response_sender.send(Err(RaftError::NotLeader)).await;
                                },
                                None => {},
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
                            self.become_candidate();
                        },
                        _ = self.shutdown_tx_rx.1.recv() => { 
                            info!("Stopping the follower loop");
                            self.set_state(NodeState::Stopped);
                            return;
                        },
                    }
                },
                Err(e) => {
                    error!("unable to unlock node state: {}", e);
                }
            }
        }
    }    
    
    async fn run_candidate_loop(&mut self) {
        info!("server.candidate.loop");
    
        let mut granted_votes = self.get_votes();
        let new_term = self.get_current_term() + 1;
        let mut can_request_vote = true;
        let election_timeout = self.random_election_timeout();
        
        'outer: loop {
            let state = self.get_state();
            match state {
                Ok(current_state) => {
                    if current_state != Some(NodeState::Candidate) {
                        break;
                    }
                    info!("Running as {:?}...", current_state.clone().unwrap()); 
                    info!("total votes ({:?}) needed for election in this term {:?}", self.quorum_size(), new_term);
                        
                    if can_request_vote {
                        self.set_current_term(new_term);

                        // request for votes
                        for node in &*self.nodes {
                            info!("Requesting vote from {}", node.address);
                            
                            let req = RequestVoteRequest { 
                                term: new_term, 
                                candidate_id: self.id.clone().to_string(), 
                                last_log_index: self.get_last_log_index().await, 
                                last_log_term: self.get_last_log_term().await,
                                commit_index: self.get_commit_index(), 
                            };
    
                            let vote_result = Node::send_vote_request(&node,req).await;
                            match vote_result {
                                Ok(vote_response) => {
                                    if vote_response.term > self.get_current_term() {
                                        info!("Looks like the received term is greater than the current term, step down....");
                                        self.set_current_term(vote_response.term);
                                        self.become_follower();
                                        break 'outer;
                                    }
    
                                    if vote_response.vote_granted {
                                        granted_votes += 1;
                                        info!("peer.vote.granted: {:?}", node.id);
                                    }
                                }
                                Err(e) => {
                                    error!("Unable to send vote request to {}: {}", node.address, e);
                                }
                            }
                        }   
                        can_request_vote = false;
                    }
    
                    if self.received_heartbeat.swap(false, Ordering::SeqCst) {
                        info!("Looks like there is a leader, stepping down now.");
                        self.become_follower();
                    }
    
                    info!("server.candidate.granted_votes: {:?}", granted_votes);
                    if granted_votes >= self.quorum_size() {
                        info!("server.election.won ");
                        self.become_leader();
                        return;
                    }
    
                    tokio::select! {
                        msg = self.api_message_rx.recv() => { 
                            match msg {
                                Some((request, response_sender))  => {
                                    _ = match request {
                                        RaftNodeServerMessage::Apply(_) => {
                                            response_sender.send(Err(RaftError::NotLeader)).await
                                        }
                                    };
                                },
                                None => {},
                            }
                        },
                        result = self.advance_commit_rx.recv() => { 
                            match result {
                                Some((_, response_sender))  => {
                                    _ = response_sender.send(Err(RaftError::NotLeader)).await;
                                },
                                None => {},
                            }
                        },
                        rpc_result = self.rpc_rx.recv() => match rpc_result {
                            Some((request, response_sender)) => {
                                _ = match request {
                                    RaftTransportRequest::AppendEntries(args) => {
                                        self.set_receive_heartbeat();
                                        let response = self.append_entries(args).await;
                                        response_sender.send(RaftTransportResponse::AppendEntries(response)).await
                                    },
                                    RaftTransportRequest::RequestVote(args) => {
                                        let response = self.request_vote(args).await;
                                        response_sender.send(RaftTransportResponse::RequestVote(response)).await
                                    },
                                };
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
                Err(e) => {
                    error!("unable to unlock node state: {}", e);
                }
            }
        }
    }
    
    async fn run_leader_loop(&mut self) {
        info!("server.leader.loop");

        // immediately establish connection with replica nodes for current leader
        self.start_replica_nodes().await;

        loop {
            let state = self.get_state();
            match state{
                Ok(current_state) => {
                    if current_state != Some(NodeState::Leader) {
                        break
                    }
                    // info!("Running as {:?}...", current_state.clone().unwrap());
                    tokio::select! {
                        cmd = self.api_message_rx.recv() => { 
                            match cmd {
                                Some((request, response_sender))  => {
                                    let result = match request {
                                        RaftNodeServerMessage::Apply(data) => {
                                            info!("Handle log entry ===/ ");
                                            _ = self.handle_log_entry(data).await;
                                            response_sender.send(Ok(())).await
                                        }
                                    };

                                    if let Err(e) = result {
                                        error!("error handling request: {}", e);
                                    }
                                },
                                None => {},
                            }
                        },
                        result = self.advance_commit_rx.recv() => { 
                            match result {
                                Some((_, response_sender))  => {
                                    info!("advance commit index ==/ ");
                                    // Advance leader commit Index by calculating the matching indexes of the replica nodes
                                    let current_commit_index = self.raft_state.get_commit_index();
                                    if let Some(majority_index) = self.majority_replicated_index().await {
                                        if majority_index > current_commit_index {
                                            self.set_commit_index(majority_index);
                                            _ = self.process_commit_tx.send(majority_index).await;
                                        }
                                    }
                                    _ = response_sender.send(Ok(())).await;
                                    info!("advance commit index  done ==/ ");
                                },
                                None => {},
                            }
                        },
                        _ = self.process_commit_rx.recv() => {
                            info!("process commit  ==/ ");
                            let commit_index = self.get_commit_index();
                            
                            let mut inflight_logs: HashMap<u64, log::LogEntry> = HashMap::new();
                            let mut last_commit_index = 0;
                        
                            // Iterate over the inflight_entry_queue to populate inflight_logs and determine last_commit_index.
                            let mut pop_count = 0;
                            for log_entry in &self.inflight_entry_queue {
                                if log_entry.index > commit_index {
                                    break;
                                }
                        
                                println!("Processing entry: {:?}", log_entry);
                        
                                inflight_logs.insert(log_entry.index, log_entry.clone());
                                last_commit_index = log_entry.index;
                                pop_count += 1;
                            }
                        
                            if !inflight_logs.is_empty() {
                                self.process_committed_logs(last_commit_index, inflight_logs).await;
                                
                                // Remove the processed logs from inflight_entry_queue.
                                for _ in 0..pop_count {
                                    self.inflight_entry_queue.pop_front();
                                }
                            }
                        },                        
                        _ = self.shutdown_tx_rx.1.recv() => { 
                            info!("Stopping the leader loop");
                            self.set_state(NodeState::Stopped);
                            break;
                        },
                        
                    }
                },
                Err(e) =>  {
                    error!("unable to unlock node state: {}", e);
                }
            }
        }

        // stop all replica nodes for current leader
       self.stop_replica_nodes().await;
    }

    fn set_state(&mut self, new_state: NodeState) {
        let mut current_state = self.state.lock().unwrap();
        if new_state == NodeState::Leader {
            self.leader_id = Some(self.id.clone());
        }
        *current_state = new_state;
    }

    fn set_receive_heartbeat(&mut self) {
        self.received_heartbeat.store(true, Ordering::SeqCst);
    }

    fn random_election_timeout(&mut self) -> Duration {
        rand::thread_rng().gen_range(Duration::from_secs(self.conf.election_timeout_min)..Duration::from_secs(self.conf.election_timeout_max))
    }

    fn quorum_size(& self) -> u64 {
        (self.nodes.len() as u64 + 1) / 2 as u64
    }

    /// Start replica nodes based on the current nodes.
    ///
    /// This function attempts to create new `ReplicaNode`s for each node, 
    /// waits for their asynchronous initialization, and then stores the 
    /// successfully initialized nodes`.
    async fn start_replica_nodes(&mut self) {
        let current_term = self.get_current_term();
        let next_index = self.get_last_log_index().await + 1;
        let heartbeat_interval = self.conf.heartbeat_interval;

        let nodes = &*self.nodes;
        let futures = nodes.iter().map(|node| {
            async move {
                let id = node.id.clone();
                let result = ReplicaNode::new(
                    node.clone(),
                    current_term,
                    next_index,
                    heartbeat_interval,
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

        let results: Vec<_> = FuturesUnordered::from_iter(futures).collect().await;
        let replica_map: HashMap<String, Arc<tokio::sync::Mutex<ReplicaNode>>> = results.into_iter().filter_map(|x| x).collect();
        self.replica_nodes = Some(replica_map);

        if let Some(replica_node_map) = &self.replica_nodes {
            for (_, replica_node) in replica_node_map.iter() {
               let (stop_heartbeat_rx, stop_heartbeat_tx) = tokio::sync::mpsc::channel::<()>(1);
               {
                    let replica_node_clone: Arc<tokio::sync::Mutex<ReplicaNode>> = replica_node.clone();
                    let leader_id = self.id.clone();
                    let raft_state = self.raft_state.clone();
                    
                    // spawning Replica heartbeat task
                    tokio::task::spawn(async move {
                        ReplicaNode::run_periodic_heartbeat(stop_heartbeat_tx, leader_id, raft_state, replica_node_clone).await;
                    });
               }

               {
                    let replica_node_clone: Arc<tokio::sync::Mutex<ReplicaNode>> = replica_node.clone();
                    let raft_state = self.raft_state.clone();
                    let logs = self.logs.clone();
                    let advance_commit_tx = self.advance_commit_tx.clone();
                    
                    // spawning Replica task handler
                    tokio::task::spawn(async move {
                        ReplicaNode::run(logs,stop_heartbeat_rx,  advance_commit_tx, raft_state, replica_node_clone).await;
                    });
               }
            }
        }
    }

    async fn stop_replica_nodes(&mut self) {
        for (_, node) in self.replica_nodes.as_mut().unwrap() {
            node.lock().await.stop().await
        }
    }

    async fn notify_all_replica_of_new_entry(&mut self)  {
        for (_, node) in self.replica_nodes.as_mut().unwrap() {
            if let Err(e) = node.lock().await.entry_replica_tx.send(()) {
                error!("unable to notify replica of new log entry: {}", e);
            }
        }
    }

    async fn handle_log_entry(&mut self, data: Vec<u8>) -> Result<(), RaftError> {
        let new_log_index = self.get_last_log_index().await + 1;
        let new_log = log::LogEntry {
            log_entry_type: log::LogEntryType::LogCommand,
            index: new_log_index,
            data,
            term: self.get_current_term(),
        };
        info!("Creating new log entry: {:?}", new_log);
    
        // // Track log inflight for processing when committed (only applicable to the leader).
        self.inflight_entry_queue.push_back(new_log.clone());
    
        // // Attempt to store the new log entry.
        let store_log_result = {
            let mut log_writer = self.logs.write().await;
            log_writer.store_log(&new_log)
        };

        match store_log_result {
            Ok(_) => {
                info!("Successfully stored log entry at index {}", new_log_index);
                self.notify_all_replica_of_new_entry().await;
                Ok(())
            }
            Err(e) => {
                error!("Failed to store log entry: {:?}", e);
                self.become_follower();
                Err(RaftError::LogFailed)
            }
        }
    }
    
    /// Calculate the highest index that has been replicated on a majority of them.
    async fn majority_replicated_index(&self) -> Option<u64> {
        if let Some(replicas) = self.replica_nodes.as_ref() {
            info!("majority_replicated_index.block  ==/ ");
            // Obtain match indexes from all replicas
            let mut match_indexes: Vec<u64> = futures::future::join_all(replicas.values().map(|replica| {
                async {
                    let locked_replica = replica.lock().await;
                    locked_replica.match_index
                }
            })).await;
            info!("majority_replicated_index.unblock  ==/ ");
            
            // Sort the match indexes
            match_indexes.sort_unstable();
            
            // Find the middle index which represents the majority in a sorted list
            let middle = replicas.len() / 2; 
            
            return match_indexes.get(middle).cloned();
        }
        None
    }

    async fn process_committed_logs(&mut self, last_commit_index: u64, committed_logs: HashMap<u64, log::LogEntry>) {
        let last_applied = self.get_last_applied();
        info!("Last Applied: {}", last_applied);
        info!("Last Commit Index: {}", last_commit_index);
        
        if last_commit_index <= last_applied {
            error!("Unable to apply log because commit index <= last applied. Ignore!");
            return;
        }
    
        let mut batched_logs = Vec::new();
        for idx in (last_applied + 1)..=last_commit_index {
            match committed_logs.get(&idx) {
                Some(log)  => {
                    if log.index > last_applied && log.index <= last_commit_index  {
                      batched_logs.push(log.clone());
                    }  else {
                        break
                    }
                },
                None => {
                    match self.logs.read().await.get_log(idx) {
                        Ok(Some(entry)) => batched_logs.push(entry),
                        Ok(None) => {
                            error!("Empty log entry");
                            break;  
                        },
                        Err(e) => {
                            error!("Unable to batch log: {:?}", e);
                            break;  
                        },
                    }
                },
                _ => {}
            }
        }
    
        if !batched_logs.is_empty() {
            for log in batched_logs {
                self.fsm.apply(&log).await;
            }
        }
    
        self.set_last_applied(last_commit_index);
        info!("Logs applied up to index {}", last_commit_index);
    }
    
    
}