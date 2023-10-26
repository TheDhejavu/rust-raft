use std::{sync::{Arc, Mutex}, time::Duration, cmp::{max, min}};
use log::{error, info};
use tokio::sync::RwLock;
use crate::{grpc_transport::{RaftGrpcTransport, RaftTransport}, error::{RaftError}, state::RaftState, raft::{LogEntry, AppendEntriesResponse}, storage::LogStore, log::LogEntryType};
use crate::raft::{
    AppendEntriesRequest, RequestVoteRequest, RequestVoteResponse
};

use std::collections::HashMap;
use lazy_static::lazy_static;


#[derive(Debug, Clone)]
pub struct Node {
    pub id: String,
    pub address: String,
}

lazy_static! {
    static ref TRANSPORT_CACHE: tokio::sync::Mutex<HashMap<String, Arc<Mutex<RaftGrpcTransport>>>> = tokio::sync::Mutex::new(HashMap::new());
}

impl Node {
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
}

/// `ReplicaNode` represents a replica of a node within the Raft cluster.
///
/// It captures details and states associated with communication and synchronization with a remote node.

pub struct ReplicaNode {
    /// Current term known by the replica.
    current_term: u64,

    /// The index of the next log entry that the leader plans to send to this replica.
    /// This could potentially be beyond the current length of the leader's log.
    next_index: u64,

    /// Index of the highest log entry known to be replicated on this replica.
    /// Used by the leader to identify how far the replica has progressed.
    pub match_index: u64,

    /// Details of the peer (remote replica) including its network address and identifier.
    node: Node,
    /// Mechanism for sending and receiving RPCs using gRPC.
    grpc_transport: Arc<dyn RaftTransport + 'static>,

    // The interval at which the leader sends heartbeat messages, in milliseconds.
    heartbeat_interval: Duration,

    /// Channels used to signal stopping replica  
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
    shutdown_rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<()>>>,

    pub entry_replica_tx: tokio::sync::mpsc::UnboundedSender<()>, 
    entry_replica_rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<()>>>,
}

impl ReplicaNode {
    pub async fn new(node: Node, current_term: u64,  next_index: u64, heartbeat_interval: u64) -> Result<Self, RaftError> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
        let (entry_replica_tx, entry_replica_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let server_addr = node.address.clone();
        let grpc_transport = RaftGrpcTransport::new(server_addr.as_str()).await;
        
        match grpc_transport {
            Ok(transprt) => {
                Ok(Self {
                    heartbeat_interval: Duration::from_millis(heartbeat_interval),
                    shutdown_rx: Arc::new(tokio::sync::Mutex::new(shutdown_rx)),
                    shutdown_tx,
                    entry_replica_rx: Arc::new(tokio::sync::Mutex::new(entry_replica_rx)),
                    entry_replica_tx,
                    current_term,
                    next_index,
                    match_index: 0,
                    node,
                    grpc_transport: Arc::new(transprt),
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
                // info!("Heartbeat sent successfully to: {}", self.node.id);
                Ok(())
            },
            Err(err) => {
                error!("Failed to send heartbeat to {}: {}", self.node.id, err);
                Err(RaftError::HeartbeatFailure) 
            }
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
        let heartbeat_message = AppendEntriesRequest {
            term: term,
            leader_id,
            prev_log_index: prev_index,
            prev_log_term: prev_term,
            commit_index,
            entries,
        };

        let response = self.grpc_transport.append_entries(heartbeat_message).await;
        match response {
            Ok(response) =>Ok(response),
            Err(_) =>  Err(RaftError::HeartbeatFailure),
        }
    }
    fn update_match_index(&mut self, match_index: u64) {
        self.match_index = max(self.match_index, match_index)
    }
    fn update_next_index(&mut self, next_index: u64) {
        self.next_index = next_index;
    }

    pub async fn stop(&mut self) {
        let result = self.shutdown_tx.send(()).await;
        if let Err(e) = result {
            error!("stop replica node channel failed: {}", e);
        }
    }
    
    /// Runs a periodic heartbeat for a replica node.
    ///
    /// This function continuously sends heartbeat messages to a replica node at a specified interval.
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
                    break;
                },
                _ = tokio::time::sleep(heartbeat_interval) => {
                    let mut replica = replica_node.lock().await;
                    let node_address = replica.node.address.clone();
                    let commit_index = raft_state.get_commit_index();

                    // info!("node.heartbeat.send <> {}", node_address);
                    _ = replica.send_heartbeat(leader_id.to_string(), commit_index).await;
                }
            };
        }
    }

    pub async fn run(
        logs: Arc<RwLock<Box<dyn LogStore>>>,
        stop_heartbeat_tx: tokio::sync::mpsc::Sender<()>,
        advance_commit_tx: tokio::sync::mpsc::Sender<((), tokio::sync::mpsc::Sender<Result<(), RaftError>>)>,
        raft_state: Arc<RaftState>,
        replica_node: Arc<tokio::sync::Mutex<ReplicaNode>>,
    ) {
        let shutdown_receiver;
        let entry_receiver;
        let node_address;
        {
            let replica = replica_node.lock().await;
            shutdown_receiver = replica.shutdown_rx.clone();
            entry_receiver = replica.entry_replica_rx.clone();
            node_address = replica.node.address.clone();
        }
        let current_term = raft_state.get_current_term();
        info!("node.replica.run <> {}", node_address); 
        
        let mut locked_shutdown = shutdown_receiver.lock().await;
        let mut locked_entry = entry_receiver.lock().await;

        loop {
           
            tokio::select! {
                _ = locked_shutdown.recv() => { 
                    let _ = stop_heartbeat_tx.send(()).await;
                    break;
                },
                _ = locked_entry.recv() => {
                    info!("Start Replication ==== / ");
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
    
                    while next_index <= last_log_index && retries > 0 {
                        if next_index <= 0 {
                            break
                        }

                        let last_index = next_index - 1;
                        let last_term = logs.read().await.get_log(last_index).map_or(0, |opt_log| opt_log.map_or(0, |log| log.term));
                        info!("next_index: {} last_index: {} retries: {}  leader_id: {} last_term: {}", next_index, last_index ,retries, leader_id, last_term);

                        let entries = logs.read().await.get_logs_from_range(next_index, last_log_index).unwrap();
                        info!("read entries done ==/ ");

                        let entries_to_replicate: Vec<LogEntry> = entries.into_iter()
                            .map(|entry| entry.to_rpc_raft_log())
                            .collect();
    
                        let commit_index = raft_state.get_commit_index();
                        let result = {
                            let mut replica = replica_node.lock().await;
                            replica.send_append_entries(leader_id.clone(), last_index, last_term, current_term, commit_index, entries_to_replicate.clone()).await
                        };
                        info!("replica.send_append_entries.done ==/ ");
                
                        match result {
                            Ok(response) => {
                                if response.success {
                                    info!("Success: update match and next index");
                                    {
                                        let mut replica = replica_node.lock().await;
                                        replica.update_match_index(next_index);

                                        next_index += 1;
                                        replica.update_next_index(next_index);
                                    }

                                    info!("start advance commit index ==/ ");
                                    let (tx, mut rx ) = tokio::sync::mpsc::channel::<Result<(), RaftError>>(1);
                                    match advance_commit_tx.send(((), tx)).await {
                                        Ok(_) => {
                                            info!("response.wait ==/ ");
                                            let result = rx.recv().await;
                                            if let Some(Err(e) )= result {
                                                error!("unable to advance commit: {}", e);
                                            }
                                            info!("response.wait.after ==/ ");
                                        },
                                        Err(e) => error!("unable to advance commit: {}", e),
                                    }
                                   
                                } else {
                                    if next_index > 0 {    
                                        next_index -= 1;
                                    }
                                    backoff_duration = Duration::from_millis(backoff_duration.as_millis() as u64 * 2);
                                }
                            },
                            Err(err) => {
                                error!("Failed to replicate entries to {}: {}", leader_id, err);
                                backoff_duration = Duration::from_millis(backoff_duration.as_millis() as u64 * 2);
                            }
                        }
                
                        // sleep for the backoff duration
                        tokio::time::sleep(backoff_duration).await;
                        retries -= 1;
                    }
                    if retries == 0 {
                        info!(" failed to replicate log with no retries left ==== / ");
                    } else {
                        info!(" == Completed Replication ==== / ");
                    }
                },
                
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
            }
        }
    }    
}