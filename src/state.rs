

impl Default for NodeState {
    fn default() -> Self {
        NodeState::Follower
    }
}

pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}
