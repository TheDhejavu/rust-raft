use std::str::Bytes;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use std::thread;
use disk::RaftSledStore;
use fsm::FSM;
use rand::Rng;
use storage::{LogStore, LogStorage};
use crate::{state::NodeState};
use crate::raft::{
    LogEntry,
    AppendEntriesRequest, AppendEntriesResponse,
    RequestVoteRequest, RequestVoteResponse,
};
use crate::grpc_transport::{RaftGrpcTransport, RaftTransportResponse,RaftTransportRequest};

mod configuration;
mod log;
mod fsm;
mod rpc_message;
mod storage;
mod state;
mod grpc_transport;
mod error;
mod utils;
mod disk;
mod mocks;

pub mod raft {
    tonic::include_proto!("raft"); 
}

struct RaftNode {
    id: u64,
    leader_id: Option<u64>,
    state: NodeState,
    votes: u64,
    peers: Vec<Arc<Mutex<RaftNode>>>,

    // persisted state for all servers
    voted_for: Option<u64>,
    log: Vec<LogEntry>,
    current_term: u64,

    // volatile state
    commit_index: u64,
    last_applied: u64,

    // volatile state on leaders
    next_index: u64,

    election_timeout: Duration,

    fsm: Box<dyn FSM>,
    logs: Box<dyn LogStore>,
    grpc_transport: RaftGrpcTransport,
    rpc_recv_channel: tokio::sync::mpsc::Receiver<(RaftTransportRequest, tokio::sync::mpsc::Sender<RaftTransportResponse>)>,
}

impl RaftNode {
    fn new(id: u64, peers: Vec<Arc<Mutex<RaftNode>>>, fsm: Box<dyn FSM>, grpc_transport: RaftGrpcTransport , log_store: Box<dyn LogStore>, rpc_recv_channel: tokio::sync::mpsc::Receiver<(RaftTransportRequest, tokio::sync::mpsc::Sender<RaftTransportResponse>)>,) -> RaftNode {
        let election_timeout = rand::thread_rng().gen_range(Duration::from_secs(60)..Duration::from_secs(600));
        let logs = Box::new(LogStorage::new(5, log_store).unwrap());
        
        RaftNode {
            id,
            current_term: 0,
            leader_id: None,
            state: NodeState::Follower,
            votes: 0,
            peers,
            log: vec![],
            commit_index: 0,
            voted_for: None,
            last_applied: 0,
            next_index: 0,
            election_timeout,
            fsm,
            logs,
            grpc_transport,
            rpc_recv_channel,
        }
    }


    fn become_leader(&mut self) {
        self.state = NodeState::Leader;
        println!("Node {} became the leader", self.id);
    }

    fn become_candidate(&mut self) {
        self.state = NodeState::Candidate;
        println!("Node {} became the candidate", self.id);
    }

    fn become_follower(&mut self) {
        self.state = NodeState::Follower;
        println!("Node {} became the follower", self.id);
    }

    /* 
        Append Log Entries Receiver implementation:
        1. Reply false if term < currentTerm (§5.1)
        2. Reply false if log doesn’t contain an entry at prevLogIndex
        whose term matches prevLogTerm (§5.3)
        3. If an existing entry conflicts with a new one (same index
        but different terms), delete the existing entry and all that
        follow it (§5.3)
        4. Append any new entries not already in the log
        5. If leaderCommit > commitIndex, set commitIndex =
        min(leaderCommit, index of last new entry)
    */
    fn append_entries(&mut self, args: AppendEntriesRequest) -> AppendEntriesResponse {
        // Check if term is outdated
        if args.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
            };
        } 

        // Check if log is consistent with new entries
        if args.prev_log_index > self.log.len() as  u64
            || self.log[args.prev_log_index as usize].term != args.prev_log_term
        {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
            };
        }

        // Remove any conflicting entries and append new entries
        self.log.truncate(args.prev_log_index as usize + 1);
        self.log.extend_from_slice(&args.entries[..]);

        if args.commit_index > self.commit_index {
            self.commit_index = args.commit_index.min(self.log.len() as  u64 - 1);
        }

        // Update term and leader id
        self.current_term = args.term;
        self.leader_id = Some(args.leader_id);

        return AppendEntriesResponse {
            term: self.current_term,
            success: true,
        };
    }

    /*
        Receiver implementation:
        1. Reply false if term < currentTerm (§5.1)
        2. If votedFor is null or candidateId, and candidate’s log is at
        least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)    
    */

    fn request_vote(&mut self, args: RequestVoteRequest) -> RequestVoteResponse {
        // Check if term is outdated
        if args.term < self.current_term {
            return RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        } 

        if args.term > self.current_term {
            self.current_term = args.term;
            self.become_follower();
            self.voted_for = None;
        }

        // Check if candidate's log is at least as up-to-date as receiver's log
        let last_entry = self.log.last().unwrap();
        if args.last_log_term < last_entry.term || (args.last_log_term == last_entry.term && args.last_log_index < self.log.len() as  u64) {
            return RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        }
 

        if self.voted_for != None  {
            return RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        }

        self.voted_for = Some(args.candidate_id);
        return RequestVoteResponse {
            term: self.current_term,
            vote_granted: true,
        };
    }

    async fn listen_for_requests(&mut self) {
        while let Some((request, response_sender)) = self.rpc_recv_channel.recv().await {
            match request {
                RaftTransportRequest::AppendEntries(args) => {
                    let response = self.append_entries(args);
                    let _ = response_sender.send(RaftTransportResponse::AppendEntries(response)).await;
                },
                RaftTransportRequest::RequestVote(args) => {
                    let response = self.request_vote(args);
                    let _ = response_sender.send(RaftTransportResponse::RequestVote(response)).await;
                },
            }
        }
    }

    fn spin_up_process(&mut self, state: NodeState) {}
    fn run_follower_forever(&mut self) {
        loop {
            println!("Running as Follower...");
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
    }
    fn run_leader_forever(&mut self) {
        loop {
            println!("Running as Leader...");
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
    }
    fn run_candidate_forever(&mut self) {
        loop {
            println!("Running as Candidate...");
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
    }

    fn run_fsm_forever(&mut self) {
        loop {
            println!("Running the FSM...");
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
    }


}

fn main() {
    println!("Hello, world!");
}