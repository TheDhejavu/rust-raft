use std::sync::Mutex;
use crate::{error, stable::StableStore};
impl Default for NodeState {
    fn default() -> Self {
        NodeState::Follower
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
    Stopped,
}

pub struct RaftStateKV;

impl RaftStateKV {
    pub const COMMIT_INDEX: &str = "commit_index";
    pub const LAST_APPLIED: &str = "last_applied";
    pub const PREV_VOTED_FOR: &str = "prev_voted_for";
    pub const PREV_VOTE_TERM: &str = "prev_vote_term";
    pub const CURRENT_TERM: &str = "current_term";
}

/// Represents the state associated with a node in a Raft cluster.
pub struct RaftState {
    /// Number of votes the node has received. Relevant during leader election.
    pub votes: Mutex<u64>,

    /// Identifier of the node that this node has voted for in the current term.
    pub voted_for: Mutex<Option<String>>,

    /// Latest term the server has seen (initially set to 0, increases monotonically).
    pub current_term: Mutex<u64>,

    /// The highest log entry known to be committed (initialized to 0, increases monotonically).
    pub commit_index: Mutex<u64>,

    /// The highest log entry applied to the state machine.
    pub last_applied: Mutex<u64>,

    // A stable storage for persisting raft state
    stable: Box<dyn StableStore>,
}

impl RaftState {
    pub fn new(stable: Box<dyn StableStore>) -> Self { 

        let raft_state = Self {
            votes: Mutex::new(0),
            voted_for: Mutex::new(None),
            current_term: Mutex::new(0),
            commit_index: Mutex::new(0),
            last_applied: Mutex::new(0),
            stable,
        };

        let commit_index  = raft_state.stable.get(RaftStateKV::COMMIT_INDEX);
        if let Ok(Some(idx)) = commit_index  {
            raft_state.set_commit_index(idx);
        }

        let current_term  = raft_state.stable.get(RaftStateKV::CURRENT_TERM);
        if let Ok(Some(term)) = current_term  {
            raft_state.set_current_term(term);
        }

        let last_applied  = raft_state.stable.get(RaftStateKV::LAST_APPLIED);
        if let Ok(Some(last_applied_idx)) = last_applied  {
            raft_state.set_last_applied(last_applied_idx);
        }

        raft_state
    }
    /// Retrieves the current term of the node.
    ///
    /// # Returns
    /// 
    /// * `u64` - The current term of the node.
    pub fn get_current_term(&self) -> u64 {
        let current_term = self.current_term.lock().unwrap();
        *current_term
    }

    /// Sets the current term of the node to the specified value.
    ///
    /// # Parameters
    ///
    /// * `value: u64` - The value to set the current term to.
    pub fn set_current_term(&self, value: u64) {
        if let Err(e) = self.stable.set(RaftStateKV::CURRENT_TERM, value){
            error!("unable to persist current term to disk: {:?}", e)
        };

        let mut current_term = self.current_term.lock().unwrap();
        *current_term = value;
    }

    /// Returns the current commit index.
    ///
    /// The commit index is the highest log entry known to be
    /// committed and is used in the Raft consensus algorithm.
    ///
    /// # Returns
    /// 
    /// * `u64` - The current commit index.
    pub fn get_commit_index(&self) -> u64 {
        let commit_index = self.commit_index.lock().unwrap();
        *commit_index
    }

    /// Sets the commit index to the given value.
    ///
    /// # Parameters
    ///
    /// * `value: u64` - The value to set the commit index to.
    pub fn set_commit_index(&self, value: u64) {
        if let Err(e) = self.stable.set(RaftStateKV::COMMIT_INDEX, value){
            error!("unable to persist commit index to disk: {:?}", e)
        };

        let mut commit_index = self.commit_index.lock().unwrap();
        *commit_index = value;
    }

    /// Returns the last applied index.
    ///
    /// The last applied index is the highest log entry applied
    /// to the state machine. It is used to track the entries 
    /// that have been safely stored and applied.
    ///
    /// # Returns
    ///
    /// * `u64` - The last applied index.
    pub fn get_last_applied(&self) -> u64 {
        let last_applied = self.last_applied.lock().unwrap();
        *last_applied
    }

    /// Sets the last applied index to the given value.
    ///
    /// # Parameters
    ///
    /// * `value: u64` - The value to set the last applied index to.
    pub fn set_last_applied(&self, value: u64) {
        if let Err(e) = self.stable.set(RaftStateKV::LAST_APPLIED, value){
            error!("unable to persist last applied to disk: {:?}", e)
        };
        let mut last_applied = self.last_applied.lock().unwrap();
        *last_applied = value;
    }

    /// Retrieves the number of votes the node has received.
    ///
    /// This is relevant during leader election in the Raft algorithm to determine if a node has the majority of votes.
    ///
    /// # Returns
    /// 
    /// * The number of votes received as a 64-bit unsigned integer.
    pub fn get_votes(&self) -> u64 {
        let votes = self.votes.lock().unwrap();
        *votes
    }

    /// Sets the number of votes the node has received.
    ///
    /// During leader election in Raft, nodes send out requests for votes. When a node receives a vote, it will increment its count.
    ///
    /// # Parameters
    ///
    /// * `value`: The new vote count.
    pub fn set_votes(&self, value: u64) {
        let mut votes = self.votes.lock().unwrap();
        *votes = value;
    }

    /// Retrieves the identifier of the node that this node has voted for in the current term.
    pub fn get_voted_for(&self) -> Option<String> {
        let voted_for = self.voted_for.lock().unwrap();
        voted_for.clone()
    }

    /// Sets the identifier of the node that this node votes for.
    pub fn set_voted_for(&self, node_id: Option<String>) {
        if let Err(e) = self.stable.set_str(RaftStateKV::PREV_VOTED_FOR, node_id.clone().unwrap_or("".to_string())){
            error!("unable to persist voted for to disk: {:?}", e)
        };

        let mut voted_for = self.voted_for.lock().unwrap();
        *voted_for = node_id;
    }

    pub fn last_voted_for(&self)->  Option<String> {
        let last_voted_for  = self.stable.get_str(RaftStateKV::PREV_VOTED_FOR);
        if let Ok(Some(voted_for)) = last_voted_for  {
            if voted_for == "" {
                return None
            }
            return Some(voted_for);
        }
        None
    }

    pub fn last_vote_term(&self) -> u64{
        let last_vote_term  = self.stable.get(RaftStateKV::PREV_VOTE_TERM);
        if let Ok(Some(idx)) = last_vote_term  {
            return idx;
        }
        0
    }

    pub fn set_vote_term(&self, term: u64) {
        if let Err(e) = self.stable.set(RaftStateKV::PREV_VOTE_TERM, term){
            error!("unable to persist last vote term to disk: {:?}", e)
        };
    }
}
