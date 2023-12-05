
## Raft state **overview:**

### Server states:

Raft is primarily split into three roles: **Leader**, **Follower**, and **Candidate**. It leverages a replicated state machine to achieve consensus among multiple servers. The Raft protocol maintains a log recording election terms and commands to execute. This log enables the replication of actions from one server to another, ensuring the entire system remains synchronized at all times.

1. **Leader**: The leader takes charge of coordinating the system, simplifying the consensus mechanism. If a follower does not receive regular heartbeats from the leader within a certain time frame, it assumes there's no active leader. The follower then increments its term, transitions to a candidate state, and solicits votes from other servers. Upon securing a majority of votes, the candidate becomes the new leader and begins coordinating the system.
2. **Candidate**: This is the state before the leader;; this state is triggered by the election timeout that forces a new election to start when there is no leader in sight, and it is also possible to have multiple candidates simultaneously. This situation can arise during leader elections when multiple followers times out without receiving heartbeats from a leader and then decide to promote themselves to candidates. Each candidate will then increment its current term, vote for itself, and send **`RequestVote`** RPCs to other nodes in the cluster.
3. **Follower:** This represents the initial state of nodes within the Raft cluster. As followers, nodes serve as integral members of the cluster, primarily responsible for storing replicated logs. They also apply changes from these logs to their local state machines, ensuring redundancy. This makes them crucial for the system's resilience, as they can step in and participate in leader elections should the current leader fail, thereby making the system fault-tolerance.

![Screen Shot 2023-09-06 at 12.32.42 PM.png](https://github.com/TheDhejavu/rust-raft/blob/main/public/raftstate.png)
                        *high-level diagram of how states transition works in raft.* 

### Persisted states on all servers:

1. **Current Term**: The **`currentTerm`** denotes the current election cycle within the Raft system. Each node, regardless of whether it's a leader, follower, or candidate, maintains and persists this value.  It’s a value that starts from zero(0) on the first boot and increases monotonically, and whenever a server restarts, it resumes from its last known term.
    
    **Purpose?** 
    
    - When a follower doesn't receive any communication from the leader and its election timeout expires, it increments its **`currentTerm`**, transitions to a candidate, and starts a new election.
    - When a server receives an RPC with a term number larger than its **`currentTerm`**, it sets its **`currentTerm`** to the received term, transitions back to follower state (if it was a candidate or leader), and updates its persisted state.
    - When a server receives an RPC with a term number less than its **`currenTerm`**, it rejects the RPC as it is considered outdated.
2. **votedFor:** This is the candidate ID that received a vote in the currentTerm and **`votedFor`** for each server is set to null value, indicating that the server has not yet voted in this term.
    
    **Purpose ?**:
    
    - **Safety**: By keeping track of **`votedFor`**, the system ensures that a server only casts one vote per term, upholding the safety properties of the Raft consensus.
    - **Rejection of duplicate vote request**: If a server has already voted in a term and receives another **`RequestVote`** RPC for the same term, the server can promptly reject the request.
3. **logs:** logs play an important role in ensuring that all servers in the cluster arrive at a consistent state. Each log entry is a record of a specific **action** or **command** with a term that the system needs to execute, and it flows from the leader to its followers. 

### Volatile state:

The term "volatile" refers to states that don't necessarily need to be persisted to disk, as they can be recreated from other persisted data

1. **CommitIndex:** 
    - The **`commitIndex`** on each server represents the highest log entry known to be committed *on every nodes in the clusters*.
    - A log entry is committed once, and it's safely stored on a majority of servers and can be applied to the state machine.
    - It is initialized at zero(0) and increases monotonically
2. **LastApplied**:
    - The **`lastApplied`** index is a local value for each server, and it represents the highest log entry that has been applied to that server's state machine.
    - It starts at 0 and will be less than or equal to the **`commitIndex`**.
    - It is initialized at zero(0) and increases monotonically

**In simple term:** 

- **Acknowledgement** is like saying, "I've received and stored the data you sent me."
- **Commit** is like announcing, "This data is now safe, and all of us (or at least the majority) agree on it, and we can act on it."

$$
0 ≤ last_applied ≤ commit_index ≤ length of log.
$$

1. **NextIndex:** 
    - For each follower, the leader maintains a **`NextIndex`,** which signifies the index of the next log entry that the leader wants to send to that follower.
2. **MatchIndex:** 
    - For each follower, the leader maintains a **`MatchIndex`,** which indicates the highest log index where the leader and follower logs are consistent.
    
$$
MatchIndex+1≤NextIndex
$$
    

> *The formular above indicates that at every point in time matchIndex will always be less than nextIndex and in most cases we expect MatchIndex + 1 to be equal to NextIndex*
> 

## **Raft consists of two major RPC:**

**RequestVoteRPC** and **AppendEntriesRPC**, the requestVote RPC is used for requesting for votes from peers within the cluster by candidate, this request consists of the below arguments in rust:

**RequestVoteRPC**

```rust
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequestVoteRequest {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(string, tag = "2")]
    pub candidate_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub last_log_index: u64,
    #[prost(uint64, tag = "4")]
    pub last_log_term: u64,
    #[prost(uint64, tag = "5")]
    pub commit_index: u64,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequestVoteResponse {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bool, tag = "2")]
    pub vote_granted: bool,
}
```

while **AppendEntries** RPC is used for two things, namely:

1. **Log Replication**: When a leader has new log entries to share with followers, it sends them using the **`AppendEntries`** RPC. 
2. **Heartbeating**: When the leader doesn't have new log entries to share, it periodically sends out **`AppendEntries`** RPCs with an empty entries list. This serves as a heartbeat to let followers know that the leader is still alive and operational.

**AppendEntriesRPC**

```rust
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendEntriesRequest {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(string, tag = "2")]
    pub leader_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub prev_log_index: u64,
    #[prost(uint64, tag = "4")]
    pub prev_log_term: u64,
    #[prost(message, repeated, tag = "5")]
    pub entries: ::prost::alloc::vec::Vec<LogEntry>,
    #[prost(uint64, tag = "6")]
    pub commit_index: u64,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendEntriesResponse {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bool, tag = "2")]
    pub success: bool,
}
```