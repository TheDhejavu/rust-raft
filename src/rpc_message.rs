use crate::log::LogEntry;

#[derive(Default)]
pub struct AppendEntriesRequest {
    pub term:  u64,
    pub leader_id:  u64,
    pub prev_log_index: u64,
    pub prev_log_term:  u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit_index:  u64,
}

#[derive(Default)]
pub struct AppendEntriesResponse {
    pub term:  u64,
    pub success: bool,
}


#[derive(Default)]
pub struct RequestVoteRequest{
    pub term:  u64,
    pub candidate_id:  Option< u64>,
    pub last_log_index:  u64,
    pub last_log_term:  u64,
    pub leader_commit_index:  u64,
}

#[derive(Default)]
pub struct RequestVoteResponse {
    pub  term:  u64,
    pub vote_granted: bool,
}