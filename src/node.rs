use std::{sync::{Arc, Mutex}, time::Duration};
use log::{error, info};
use crate::{grpc_transport::{RaftGrpcTransport, RaftTransport}, error::{RaftError, self}, state::RaftState};
use crate::raft::{
    AppendEntriesRequest, RequestVoteRequest, RequestVoteResponse,
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
    pub current_term: u64,

    /// The index of the next log entry that the leader plans to send to this replica.
    /// This could potentially be beyond the current length of the leader's log.
    pub next_index: u64,

    /// Index of the highest log entry known to be replicated on this replica.
    /// Used by the leader to identify how far the replica has progressed.
    pub match_index: u64,

    /// Details of the peer (remote replica) including its network address and identifier.
    pub node: Node,

    /// Channels used to signal stopping of background tasks related to this replica.
    pub stop_channel: (tokio::sync::mpsc::Sender<()>, tokio::sync::mpsc::Receiver<()>),

    /// Mechanism for sending and receiving RPCs using gRPC.
    pub grpc_transport: Arc<dyn RaftTransport + 'static>,

    /// The highest log entry known to be committed by the replica.
    pub commit_index: u64,

    // The interval at which the leader sends heartbeat messages, in milliseconds.
    pub heartbeat_interval: Duration,
}


impl ReplicaNode {
    pub async fn new(node: Node, current_term: u64, commit_index: u64, next_index: u64, heartbeat_interval: u64) -> Result<Self, RaftError> {
        let stop_channel = tokio::sync::mpsc::channel::<()>(1);
        let server_addr = node.address.clone();
        let grpc_transport = RaftGrpcTransport::new(server_addr.as_str()).await;
        
        match grpc_transport {
            Ok(transprt) => {
                Ok(Self {
                    heartbeat_interval: Duration::from_millis(heartbeat_interval),
                    stop_channel,
                    current_term,
                    next_index,
                    commit_index,
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
            leader_id: leader_id,
            prev_log_index: self.next_index - 1,
            prev_log_term: 0,
            commit_index: commit_index,
            entries: vec![],
        };

        let response = self.grpc_transport.append_entries(heartbeat_message).await;
        match response {
            Ok(_) => {
                info!("heartbeat received successfully by: {}", self.node.id);
                Ok(())
            },
            Err(err) => {
                error!("Failed to send heartbeat to {}: {}", self.node.id, err);
                Err(RaftError::HeartbeatFailure) 
            }
        }
    }

    pub async fn stop(&mut self) {
        let result = self.stop_channel.0.send(()).await;
        if let Err(e) = result {
            error!("stop node channel failed: {}", e);
        }
    }

    async fn get_replica_values(
        replica_node: &Arc<tokio::sync::Mutex<ReplicaNode>>,
        raft_state: &Arc<RaftState>,
    ) -> (Duration, String, u64) {
        let replica = replica_node.lock().await;

        let heartbeat_interval = replica.heartbeat_interval;
        let node_address = replica.node.address.clone();
        let commit_index = raft_state.get_commit_index();
    
        (heartbeat_interval, node_address, commit_index)
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
        leader_id: Arc<String>,
        raft_state: Arc<RaftState>,
        replica_node: Arc<tokio::sync::Mutex<ReplicaNode>>,
    ) {
        loop {
            let (
                heartbeat_interval, 
                node_address, 
                commit_index,
            ) = ReplicaNode::get_replica_values(&replica_node, &raft_state).await;
    
            info!("node.heartbeat.send <> {}", node_address);

            let stop_signal = {
                let mut replica = replica_node.lock().await;
                let stop_or_heartbeat =  tokio::select! {
                    _ = replica.stop_channel.1.recv() => { 
                        true 
                    },
                    _ = tokio::time::sleep(heartbeat_interval) => {
                        false
                    },
                };
                stop_or_heartbeat
            };

            if stop_signal {
                break;
            }
    
            match replica_node.lock().await.send_heartbeat(leader_id.to_string(), commit_index).await {
                Ok(_) => {
                    info!(
                        "Successfully sent heartbeat from leader {} with commit index {}",
                        leader_id, commit_index
                    )
                }
                Err(e) => error!("Failed to send heartbeat: {:?}", e),
            }

            info!("node.heartbeat.after <> {}", node_address);

        }
    }
}