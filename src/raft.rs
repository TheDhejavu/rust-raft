use std::str::Bytes;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::thread;
use fsm::FSM;
use rand::Rng;
use storage::LogStore;
use crate::{log::LogEntry, state::NodeState};
use crate::rpc_message::{
    AppendEntriesRequest, AppendEntriesResponse,
    RequestVoteRequest, RequestVoteResponse,
};

mod configuration;
mod log;
mod fsm;
mod rpc_message;
mod storage;
mod state;
mod transport;
mod errors;
mod disk;
mod mocks;

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
}

impl RaftNode {
    fn new(id: u64, peers: Vec<Arc<Mutex<RaftNode>>>, fsm: Box<dyn FSM>, logs: Box<dyn LogStore>) -> RaftNode {
        let election_timeout = rand::thread_rng().gen_range(Duration::from_secs(60)..Duration::from_secs(600));

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
    fn append_log_entries(&mut self, args: AppendEntriesRequest) -> AppendEntriesResponse {
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
        self.log.extend_from_slice(&args.entries);

        if args.leader_commit_index > self.commit_index {
            self.commit_index = args.leader_commit_index.min(self.log.len() as  u64 - 1);
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

        self.voted_for = args.candidate_id;
        return RequestVoteResponse {
            term: self.current_term,
            vote_granted: true,
        };
    }
    fn start_election(){

    }

    fn start_election_timeout(){
        
    }

}

fn main() {
    println!("Hello, world!");
}