use std::sync::{Arc, Mutex};
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


/// Represents the state associated with a node in a Raft cluster.
pub struct RaftState {
    /// Number of votes the node has received. Relevant during leader election.
    pub  votes: Mutex<u64>,

    /// Identifier of the node that this node has voted for in the current term.
    pub voted_for: Mutex<Option<String>>,

    /// Latest term the server has seen (initially set to 0, increases monotonically).
    pub current_term: Mutex<u64>,

    /// The highest log entry known to be committed (initialized to 0, increases monotonically).
    pub commit_index: Mutex<u64>,

    /// The highest log entry applied to the state machine.
    pub last_applied: Mutex<u64>,
}


impl RaftState {
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
        let mut voted_for = self.voted_for.lock().unwrap();
        *voted_for = node_id;
    }
}
