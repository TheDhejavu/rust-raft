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
use storage::{LogStore, LogStorage};
use crate::state::{NodeState, RaftState};
use std::collections::HashMap;
use crate::raft::{
    AppendEntriesRequest, AppendEntriesResponse,
    RequestVoteRequest, RequestVoteResponse,
};

use crate::grpc_transport::{RaftTransportResponse,RaftTransportRequest};

mod configuration;
mod storage;
mod state;
mod error;
mod utils;
mod mocks;
pub mod datastore;
pub mod config;
pub mod log;
pub mod fsm;
pub mod grpc_transport;
pub mod node;

mod raft {
    tonic::include_proto!("raft"); 
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
    logs: Box<dyn LogStore>,

    /// Channels to signal stopping server background task.
    stop_channel: (tokio::sync::mpsc::Sender<()>, tokio::sync::mpsc::Receiver<()>),

    /// A flag indicating whether a heartbeat has been received.
    received_heartbeat: Arc<AtomicBool>,

    /// Channel to receive incoming Raft RPCs.
    rpc_recv_channel: tokio::sync::mpsc::Receiver<(RaftTransportRequest, tokio::sync::mpsc::Sender<RaftTransportResponse>)>,
}


impl RaftNodeServer {
    pub async fn new(
        id: String,
        conf: Config,
        nodes: Vec<Node>,
        fsm: Box<dyn FSM>,
        log_store: Box<dyn LogStore>,
        rpc_recv_channel: tokio::sync::mpsc::Receiver<(RaftTransportRequest, tokio::sync::mpsc::Sender<RaftTransportResponse>)>,
    ) -> Self {
        let logs = Box::new(LogStorage::new(256, log_store).unwrap());
        let stop_channel = tokio::sync::mpsc::channel::<()>(1);

        env_logger::Builder::new()
            .filter_level(::log::LevelFilter::Info) 
            .filter_module("sled", ::log::LevelFilter::Warn) 
            .init();
        
        Self {
            state: Arc::new(Mutex::new(NodeState::Follower)),
            conf,
            id: Arc::new(id),
            leader_id: None,
            stop_channel,
            nodes: Arc::new(nodes),
            replica_nodes: None,
            received_heartbeat: Arc::new(AtomicBool::new(false)),
            fsm,
            logs,
            rpc_recv_channel,
            raft_state: Arc::new(RaftState {
                votes: Mutex::new(0),
                voted_for: Mutex::new(None),
                current_term: Mutex::new(0),
                commit_index: Mutex::new(0),
                last_applied: Mutex::new(0),
            }),
        }
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
    fn get_last_log_term(&self) -> u64 {
        match self.logs.get_log(self.logs.last_index()) {
            Some(log) => log.term,
            None => 0,
        }
    }

    /// Retrieves the index of the last log entry.
    fn get_last_log_index(&self) -> u64  {
        self.logs.last_index()
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
    fn append_entries(&mut self, args: AppendEntriesRequest) -> AppendEntriesResponse {
        info!("recv.append_entries request.");
        // Check if term is outdated
        if args.term < self.get_current_term() {
            return AppendEntriesResponse {
                term: self.get_current_term(),
                success: false,
            };
        } 


        let prev_log_entry = self.logs.get_log(args.prev_log_index);
        let mut prev_log_term: u64 = 0;
        if let Some(log_entry) = prev_log_entry {
            prev_log_term = log_entry.term;
        }

        // Check if log is consistent with new entries
        if args.prev_log_index > self.logs.last_index() as  u64 || prev_log_term != args.prev_log_term
        {
            return AppendEntriesResponse {
                term: self.get_current_term(),
                success: false,
            };
        }

        // Remove any conflicting entries and append new entries
        self.logs.delete_range(args.prev_log_index, args.prev_log_index + 1);

        if args.commit_index > self.get_commit_index() {
            self.set_commit_index(args.commit_index.min(self.get_last_log_index() as  u64 - 1));
        }

        // Update term and leader id
        self.set_current_term(args.term);

        if args.entries.len() > 0 {
            
        }

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
    fn request_vote(&mut self, args: RequestVoteRequest) -> RequestVoteResponse {
        // Check if term is outdated
        if args.term < self.get_current_term() {
            return RequestVoteResponse {
                term: self.get_current_term(),
                vote_granted: false,
            };
        } 

        if args.term > self.get_current_term() {
            self.set_current_term(args.term);
            self.become_follower();
            self.set_voted_for(None);
        }

        // Check if candidate's log is at least as up-to-date as receiver's log
        let last_entry = self.logs.get_log(self.logs.last_index());
        if let Some(entry) = last_entry {
            if args.last_log_term < entry.term || (args.last_log_term == entry.term && args.last_log_index < self.logs.last_index() as  u64) {
                return RequestVoteResponse {
                    term: self.get_current_term(),
                    vote_granted: false,
                };
            }
        }
       
        if self.get_voted_for() != None  {
            return RequestVoteResponse {
                term: self.get_current_term(),
                vote_granted: false,
            };
        }

        self.set_voted_for(Some(args.candidate_id.into()));
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
    
    async fn run(&mut self) {
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
    pub async fn start(&mut self) {   
        let state = self.get_state().unwrap();

        // become follower
        self.become_follower();
        info!("server.start.state: {:?}", state.unwrap());

        self.run().await;
    }
    
    fn is_running(&mut self) -> bool {    
        let state = self.get_state().unwrap();
        return !(state == Some(NodeState::Stopped));
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
    
                    info!("Running as {:?}...", current_state.clone().unwrap()); 
    
                    tokio::select! {
                        _ = tokio::time::sleep(election_timeout) => {
                            info!("Election timed-out. do stuff");
                            self.become_candidate();
                        },
                        rpc_result = self.rpc_recv_channel.recv() => match rpc_result {
                            Some((request, response_sender)) => {
                                let result = match request {
                                    RaftTransportRequest::AppendEntries(args) => {
                                        self.set_receive_heartbeat();
                                        let response = self.append_entries(args);
                                        response_sender.send(RaftTransportResponse::AppendEntries(response)).await
                                    },
                                    RaftTransportRequest::RequestVote(args) => {
                                        self.set_receive_heartbeat();
                                        let response = self.request_vote(args);
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
                        _ = self.stop_channel.1.recv() => { 
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
                                last_log_index: self.get_last_log_index(), 
                                last_log_term: self.get_last_log_term(),
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
                        _ = self.stop_channel.1.recv() => { 
                            info!("Stopping the candidate loop");
                            self.set_state(NodeState::Stopped);
                            return;
                        },
                        rpc_result = self.rpc_recv_channel.recv() => match rpc_result {
                            Some((request, response_sender)) => {
                                let result = match request {
                                    RaftTransportRequest::AppendEntries(args) => {
                                        self.set_receive_heartbeat();
                                        let response = self.append_entries(args);
                                        response_sender.send(RaftTransportResponse::AppendEntries(response)).await
                                    },
                                    RaftTransportRequest::RequestVote(args) => {
                                        let response = self.request_vote(args);
                                        response_sender.send(RaftTransportResponse::RequestVote(response)).await
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
        let next_index = self.get_last_log_index() + 1;
        let commit_index = self.get_commit_index();
        let heartbeat_interval = self.conf.heartbeat_interval;

        let nodes = &*self.nodes;
        let futures = nodes.iter().map(|node| {
            async move {
                let id = node.id.clone();
                let result = ReplicaNode::new(
                    node.clone(),
                    current_term,
                    commit_index,
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

        if let Some(replica_map) = &self.replica_nodes {
            for (_, replica_mutex) in replica_map.iter() {
                let replica_clone: Arc<tokio::sync::Mutex<ReplicaNode>> = replica_mutex.clone();
                let leader_id = self.id.clone();
                let raft_state = self.raft_state.clone();
                
                // Spawning heartbeat task
                tokio::task::spawn(async move {
                    ReplicaNode::run_periodic_heartbeat(leader_id, raft_state, replica_clone).await;
                });
            }
        }
    }

    async fn stop_replica_nodes(&mut self) {
        for (_, node) in self.replica_nodes.as_mut().unwrap() {
            node.lock().await.stop().await
        }
    }


}