use std::{sync::{Arc, Mutex}, time::Duration, cmp::max};
use log::{error, info, warn};
use tokio::{sync::RwLock, time::{sleep, Instant}};
use crate::{grpc_transport::{RaftGrpcTransport, RaftTransport}, error::RaftError, state::RaftState, raft::{LogEntry, AppendEntriesResponse, TimeoutNowRequest, TimeoutNowResponse}, storage::LogStore};
use crate::raft::{
    AppendEntriesRequest, RequestVoteRequest, RequestVoteResponse
};
use serde_derive::{Serialize, Deserialize};
use std::collections::HashMap;
use lazy_static::lazy_static;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum NodeType {
    Voter,
    NonVoter,
}

impl NodeType {
    pub fn describe(&self) -> &'static str {
        match self {
            NodeType::Voter => "Voter",
            NodeType::NonVoter => "NonVoter",
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Node {
    pub id: String,
    pub address: String,
    pub node_type: NodeType, 
}

lazy_static! {
    static ref TRANSPORT_CACHE: tokio::sync::Mutex<HashMap<String, Arc<Mutex<RaftGrpcTransport>>>> = tokio::sync::Mutex::new(HashMap::new());
}

pub async fn send_vote_request(
    node: &Node,
    request: RequestVoteRequest,
) -> Result<RequestVoteResponse, tonic::Status> {
    let mut cache = TRANSPORT_CACHE.lock().await;

    if !cache.contains_key(&node.address) {
        match RaftGrpcTransport::new(node.address.as_str()).await {
            Ok(transport) => {
                cache.insert(node.address.clone(), Arc::new(Mutex::new(transport)));
            },
            Err(_) => {
                return Err(tonic::Status::internal("Failed to create GRPC transport"));
            }
        }
    }

    let transport = cache.get(&node.address).unwrap();
    let locked_transport = transport.lock().unwrap();
    locked_transport.request_vote(request).await
}

/// `ReplicaNode` represents a replica of a node within the Raft cluster.
///
/// It captures details and states associated with communication and synchronization with a remote node.

pub struct ReplicaNode {
    /// Current term known by the replica.
    current_term: u64,

    /// The index of the next log entry that the leader plans to send to this replica.
    /// This could potentially be beyond the current length of the leader's log.
    pub next_index: u64,

    /// Index of the highest log entry known to be replicated on this replica.
    /// Used by the leader to identify how far the replica has progressed.
    pub match_index: u64,

    /// Details of the peer (remote replica) including its network address and identifier.
    pub node: Arc<Node>,

    /// Mechanism for sending and receiving RPCs using gRPC.
    grpc_transport: Arc<dyn RaftTransport + 'static>,

    // The interval at which the leader sends heartbeat messages, in milliseconds.
    heartbeat_interval: Duration,

    /// Channels used to signal stopping replica  
    shutdown_tx: tokio::sync::mpsc::Sender<()>,

    shutdown_rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<()>>>,

    is_active: bool,

    last_activity: Option<Instant>,
}
#[derive(Clone)]

pub struct ReplicaNodeTxn {
    pub entry_replica_tx: tokio::sync::mpsc::UnboundedSender<()>,
}

impl ReplicaNode {
    pub async fn new(
        node: Arc<Node>, 
        current_term: u64,  
        next_index: u64, 
        heartbeat_interval: u64, 
        match_index: u64,  
    ) -> Result<Self, RaftError> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
        let server_addr = node.address.clone();
        let grpc_transport = RaftGrpcTransport::new(server_addr.as_str()).await;
        
        match grpc_transport {
            Ok(transprt) => {
                Ok(Self {
                    is_active: true,
                    heartbeat_interval: Duration::from_millis(heartbeat_interval),
                    shutdown_rx: Arc::new(tokio::sync::Mutex::new(shutdown_rx)),
                    shutdown_tx,
                    current_term,
                    next_index,
                    match_index,
                    node,
                    grpc_transport: Arc::new(transprt),
                    last_activity: None,
                })
            },
            Err(err) => {
                error!("{:?}", err);
                Err(RaftError::ConnectionRefusedError)
            }
        }
    }
    pub async fn send_heartbeat(&mut self, leader_id:  String, commit_index: u64) -> Result<(), RaftError> {
        let heartbeat_message = AppendEntriesRequest {
            term: self.current_term,
            leader_id,
            prev_log_index: 0,
            prev_log_term: 0,
            commit_index,
            entries: vec![],
        };

        let response = self.grpc_transport.append_entries(heartbeat_message).await;
        match response {
            Ok(_) => {
                self.update_last_activity(Instant::now());
                Ok(())
            },
            Err(_) => Err(RaftError::HeartbeatFailure) 
        }
    }

    pub async fn send_timeout_now(
        &mut self,
        leader_id:  Arc<String>,
    ) -> Result<TimeoutNowResponse, RaftError> {
        let request = TimeoutNowRequest {
            leader_id: leader_id.to_string(),
        };

        let response = self.grpc_transport.timeout_now(request).await;
        match response {
            Ok(response) =>Ok(response),
            Err(e) =>  Err(RaftError::Error(e.to_string())),
        }
    }
    
    pub async fn send_append_entries(
        &mut self, 
        leader_id:  String, 
        prev_index: u64, 
        prev_term: u64, 
        term: u64,
        commit_index: u64,
        entries: Vec<LogEntry>,
    ) -> Result<AppendEntriesResponse, RaftError> {
        let request = AppendEntriesRequest {
            term,
            leader_id,
            prev_log_index: prev_index,
            prev_log_term: prev_term,
            commit_index,
            entries,
        };

        let response = self.grpc_transport.append_entries(request).await;
        match response {
            Ok(response) =>Ok(response),
            Err(e) =>  Err(RaftError::Error(e.to_string())),
        }
    }

    fn update_match_index(&mut self, match_index: u64) {
        self.match_index = max(self.match_index, match_index)
    }

    fn update_next_index(&mut self, next_index: u64) {
        self.next_index = next_index;
    }

    fn update_last_activity(&mut self, now: Instant) {
        self.is_active = true;
        self.last_activity = Some(now);
    }

    pub async fn stop(&mut self) {
        let result = self.shutdown_tx.send(()).await;
        if let Err(e) = result {
            error!("stop replica node channel failed: {}", e);
        }
    }

    // 3.10 Leadership transfer extension
    pub async fn transfer_leadership(replica_node: Arc<tokio::sync::Mutex<ReplicaNode>>, leader_id: Arc<String>) -> Result<(), RaftError> {
        let mut replica = replica_node.lock().await;
    
        match replica.send_timeout_now(leader_id).await {
            Ok(_) => Ok(()),
            Err(e) => Err(RaftError::Error(e.to_string())),
        }
    }
    
    /// Runs a periodic heartbeat for a replica node.
    ///
    /// This function continuously sends heartbeat messages to a replica node at a specified interval.(§5.2).
    /// It also listens for a stop signal to gracefully exit the loop.
    /// 
    /// # Arguments
    ///
    /// - `leader_id`: The ID of the leader node.
    /// - `raft_state`: The shared state of the Raft consensus algorithm.
    /// - `replica_node`: The replica node to which heartbeats will be sent.
    pub async fn run_periodic_heartbeat(
        mut stop_heartbeat_rx: tokio::sync::mpsc::Receiver<()>,
        leader_id: Arc<String>,
        raft_state: Arc<RaftState>,
        replica_node: Arc<tokio::sync::Mutex<ReplicaNode>>,
    ) {
        info!("node.heartbeat.run");
        let heartbeat_interval: Duration;
        {
            let replica = replica_node.lock().await;
            heartbeat_interval = replica.heartbeat_interval;
        };

        loop {
            tokio::select! {
                _ = stop_heartbeat_rx.recv() => { 
                    let replica = replica_node.lock().await;
                    let node_address = replica.node.address.clone();

                    info!("node.replica.heartbeat.stop <> {}", node_address);
                    break;
                },
                _ = tokio::time::sleep(heartbeat_interval) => {
                    let mut replica = replica_node.lock().await;
                    let commit_index = raft_state.get_commit_index();

                    let result = replica.send_heartbeat(leader_id.to_string(), commit_index).await;
                    if let Err(_) = result {
                        // sleep for a few milli seconds before retrying.
                        sleep(Duration::from_millis(100)).await;
                    }
                }
            };
        }
    }

    pub async fn run(
        logs: Arc<RwLock<Box<dyn LogStore>>>,
        stop_heartbeat_tx: tokio::sync::mpsc::Sender<()>,
        advance_commit_tx: tokio::sync::mpsc::UnboundedSender<((String, u64), tokio::sync::mpsc::Sender<Result<(), RaftError>>)>,
        raft_state: Arc<RaftState>,
        replica_node: Arc<tokio::sync::Mutex<ReplicaNode>>,
        mut entry_replica_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    ) {
        let shutdown_receiver;
        let node_address;
        let node_id;
       
        {
            let guard_replica = replica_node.lock().await;
            node_id= guard_replica.node.id.clone();
            shutdown_receiver = guard_replica.shutdown_rx.clone();
            node_address = guard_replica.node.address.clone();
        }
        let current_term = raft_state.get_current_term();
        info!("node.replica.run <> {}", node_address); 
        
        let mut guard_shutdown_rx = shutdown_receiver.lock().await;
        loop {
           
            tokio::select! {
                _ = guard_shutdown_rx.recv() => { 
                    let _ = stop_heartbeat_tx.send(()).await;
                    info!("node.replica.shutdown <> {}", node_address);
                    break;
                },
                _ = entry_replica_rx.recv() => {
                    let last_log_index = logs.read().await.last_index();
                    
                    let mut next_index;
                    let leader_id;
                    {
                        let replica = replica_node.lock().await;
                        next_index = replica.next_index;
                        leader_id = replica.node.id.clone();
                    }
                    
                    let mut retries = 10;
                    let mut backoff_duration = Duration::from_millis(50); 
                    if next_index > last_log_index {
                        continue
                    }
                    info!("Start Replication <> {}", node_address);

                    while next_index <= last_log_index && retries > 0 {
                        if next_index <= 0 {
                            break
                        }

                        let last_index = next_index - 1;
                        let last_term = logs.read().await.get_log(last_index).map_or(0, |opt_log| opt_log.map_or(0, |log| log.term));
                        
                        info!("Replication attempt: next_index: {}, last_index: {}, retries: {}, leader_id: {}, last_term: {}",
                            next_index, 
                            last_index, 
                            retries, 
                            leader_id, 
                            last_term,
                        );

                        let entries = logs.read().await.get_logs_from_range(next_index, last_log_index).unwrap();
                        let entries_to_replicate: Vec<LogEntry> = entries.into_iter()
                            .map(|entry| entry.to_rpc_raft_log())
                            .collect();
    
                        let commit_index = raft_state.get_commit_index();
                        info!("replica.send.before");
                        let result = {
                            let mut replica = replica_node.lock().await;
                            replica.send_append_entries(leader_id.clone(), last_index, last_term, current_term, commit_index, entries_to_replicate.clone()).await
                        };
                        info!("replica.send.after");
                
                        match result {
                            Ok(response) => {
                                if response.success {
                                    info!("replica.send.success");
                                    let match_index = last_log_index;
                                    {
                                        let mut guard_replica = replica_node.lock().await;
                                        next_index = last_log_index + 1;
                                        // Upon successful: Update nextIndex and matchIndex for follower (§5.3).
                                        guard_replica.update_next_index(next_index);
                                        guard_replica.update_match_index(match_index);
                                        guard_replica.update_last_activity(Instant::now());
                                    }

                                    info!("commit.advance.begin");
                                    let (tx, mut rx ) = tokio::sync::mpsc::channel::<Result<(), RaftError>>(1);
                                    match advance_commit_tx.send(((node_id.clone(), match_index), tx)) {
                                        Ok(_) => {
                                            let result = rx.recv().await;
                                            match result {
                                                Some(Err(e)) =>  error!("[Err] unable to advance commit: {}", e),
                                                Some(Ok(_)) =>  info!("commit.advance.ok"),
                                                _ => ()
                                            }
                                        },
                                        Err(e) => error!("[ERR] unable to advance commit: {}", e),
                                    }
                                } else {
                                    warn!("replica.send.failed: {} retries left: {} ", node_address, retries);
                                    // If AppendEntries fails due to log inconsistency:
                                    // Decrement nextIndex and retry (§5.3).
                                    if next_index > 0 {    
                                        next_index -= 1;
                                    }
                                    backoff_duration = Duration::from_millis(backoff_duration.as_millis() as u64 * 2);
                                }
                            },
                            Err(err) => {
                                error!("Failed to replicate entries to {}: {}", node_address, err);
                                backoff_duration = Duration::from_millis(backoff_duration.as_millis() as u64 * 2);
                            }
                        }
                
                        // sleep for the backoff duration
                        sleep(backoff_duration).await;
                        retries -= 1;
                    }
                    if retries == 0 {
                        // This is where snapshot comes in, when we fail to replicate a log
                        // then there is a probability that the node logs were too backward and we 
                        // might need to send them a snapshot instead. 
                        info!("Failed to replicate log with no retries left ==== / ");
                    } else {
                        info!("Completed Replication <> {} - Retries left: {}", node_address, retries);
                    }
                },
                
                _ = sleep(std::time::Duration::from_millis(100)) => {}

            }
        }
    }    
}