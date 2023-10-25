use log::{error, info};
use tonic::{Request, Response, Status, transport::{Channel, Server, Endpoint}};
use async_trait::async_trait;
use tokio::sync::{Mutex, mpsc};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use crate::{raft::raft_grpc_transport_client::RaftGrpcTransportClient, utils::{format_endpoint_addr, format_server_addr}};
use crate::raft::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse, raft_grpc_transport_server};


/// Represents a request message for the Raft protocol, which can either be an
/// `AppendEntries` request or a `RequestVote` request.
pub enum RaftTransportRequest {
    AppendEntries(AppendEntriesRequest),
    RequestVote(RequestVoteRequest),
}


/// Represents a response message for the Raft protocol, which can either be an
/// `AppendEntries` response or a `RequestVote` response.
pub enum RaftTransportResponse {
    AppendEntries(AppendEntriesResponse),
    RequestVote(RequestVoteResponse),
}

/// A gRPC transport layer for the Raft protocol, allowing for communication between Raft nodes.
pub struct RaftGrpcTransport {
    client: Arc<Mutex<RaftGrpcTransportClient<Channel>>>,
}

#[async_trait]
pub trait RaftTransport: Send + Sync{
    async fn append_entries(&self, request: AppendEntriesRequest) -> Result<AppendEntriesResponse, tonic::Status>;
    async fn request_vote(&self, request: RequestVoteRequest) -> Result<RequestVoteResponse, tonic::Status>;
}

impl RaftGrpcTransport {
    /// Instantiates a new gRPC client for the Raft protocol.
    /// This function attempts to create a connection to the specified address
    /// and returns a wrapped gRPC client ready for Raft communication.
    pub async fn new(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let endpoint_addr = format_endpoint_addr(addr);
        let endpoint = Endpoint::from_shared(endpoint_addr)?;
        let channel = endpoint.connect().await?;
        let client: RaftGrpcTransportClient<Channel> = RaftGrpcTransportClient::new(channel);
        Ok(RaftGrpcTransport { client: Arc::new(Mutex::new(client)) })
    }
}

#[async_trait]
impl RaftTransport for RaftGrpcTransport {
    async fn append_entries(&self, request: AppendEntriesRequest) -> Result<AppendEntriesResponse, tonic::Status> {
        let mut client = self.client.lock().await;
        let response = client.append_entries(request).await?;
        Ok(response.into_inner())
    }

    async fn request_vote(&self, request: RequestVoteRequest) -> Result<RequestVoteResponse, tonic::Status> {
        let mut client = self.client.lock().await;
        let response = client.request_vote(request).await?;
        Ok(response.into_inner())
    }
}

pub struct RaftGrpcTransportServer {
    rpc_send_channel: mpsc::Sender<(RaftTransportRequest, mpsc::Sender<RaftTransportResponse>)>,
}

impl RaftGrpcTransportServer {
    /// Creates a new gRPC server instance with a given channel for communication.
    /// The channel allows the server to communicate with the core Raft logic, passing
    /// incoming requests and receiving responses.
    pub fn new(rpc_send_channel: mpsc::Sender<(RaftTransportRequest, mpsc::Sender<RaftTransportResponse>)>) -> Self {
        RaftGrpcTransportServer { rpc_send_channel }
    }

    pub async fn run(self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let server_addr = format_server_addr(addr).parse()?;

        // Just spawn the server task and don't wait on it
        tokio::spawn(async move {
            Server::builder()
                .add_service(raft_grpc_transport_server::RaftGrpcTransportServer::new(self))
                .serve(server_addr)
                .await.expect("unable to start up grpc server");
        });
        sleep(Duration::from_millis(100)).await;

        info!("Server is running on {}", addr);
        Ok(())
    }

}

#[async_trait]
impl raft_grpc_transport_server::RaftGrpcTransport for RaftGrpcTransportServer {
    
    async fn append_entries(&self, request: Request<AppendEntriesRequest>) -> Result<Response<AppendEntriesResponse>, Status> {
        let (response_sender, mut response_receiver) = mpsc::channel(1);
        match self.rpc_send_channel.send((RaftTransportRequest::AppendEntries(request.into_inner()), response_sender)).await {
            Ok(_) => {
                match response_receiver.recv().await {
                    Some(RaftTransportResponse::AppendEntries(res)) => Ok(Response::new(res)),
                   _ => Err(Status::internal("Received unexpected response type")),
                }
            },
            Err(e) =>  {
                error!("Unable to process append entries request: {}", e);
                Err(Status::internal(format!("Unable to process append entries request: {}", e)))
            }
        }
    }

    async fn request_vote(&self, request: Request<RequestVoteRequest>) -> Result<Response<RequestVoteResponse>, Status> {
        let (response_sender, mut response_receiver) = mpsc::channel(1);
        match self.rpc_send_channel.send((RaftTransportRequest::RequestVote(request.into_inner()), response_sender)).await{
            Ok(_) => {
                match response_receiver.recv().await {
                    Some(RaftTransportResponse::RequestVote(res)) => Ok(Response::new(res)),
                    _ => Err(Status::internal("Unexpected response type"))
                }
            },
            Err(e) =>  {
                error!("Unable to process request vote request: {}", e);
                Err(Status::internal(format!("Unable to process request vote request: {}", e)))
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::raft::{LogEntry, LogEntryType};
    use super::*;
    use tokio::sync::mpsc;

    async fn setup_server(server_addr: &str) -> mpsc::Receiver<(RaftTransportRequest, mpsc::Sender<RaftTransportResponse>)> {
        let (send, recv) = mpsc::channel::<(RaftTransportRequest, mpsc::Sender<RaftTransportResponse>)>(1);
        let server = RaftGrpcTransportServer::new(send);
        assert_eq!(server.run(server_addr).await.is_ok(), true, "Failed to start server");
        recv
    }

    async fn setup_client(server_addr: &str) -> RaftGrpcTransport {
        RaftGrpcTransport::new(server_addr).await.expect("Failed to create client")
    }

    async fn spawn_request_listener(
        mut recv: mpsc::Receiver<(RaftTransportRequest, mpsc::Sender<RaftTransportResponse>)>,
        response_type: RaftTransportResponse,
    ) {
        tokio::spawn(async move {
            if let Some((_, response_sender)) = recv.recv().await {
                response_sender.send(response_type).await.expect("Failed to send response");
            }
        });
    }

    #[tokio::test]
    async fn test_append_entries() {
        let server_addr = "[::]:50051";
        let  recv = setup_server(server_addr).await;

        let append_entries_response: AppendEntriesResponse = AppendEntriesResponse {
            term: 1,
            success: true,
        };
        spawn_request_listener(recv, RaftTransportResponse::AppendEntries(append_entries_response)).await;

        let client_transport: RaftGrpcTransport = setup_client(server_addr).await;

        let request: AppendEntriesRequest = AppendEntriesRequest {
            term: 1,
            leader_id: "aws-node-1".to_string(),
            prev_log_index: 3,
            prev_log_term: 4,
            entries: vec![
                LogEntry { 
                    index: 1, 
                    term: 1, 
                    log_entry_type: LogEntryType::LogCommand as i32, 
                    data: "config_change_cmd".as_bytes().to_vec() 
                },
                LogEntry { 
                    index: 2, 
                    term: 1, 
                    log_entry_type: LogEntryType::ConfCommand as i32, 
                    data: "log_cmd".as_bytes().to_vec() 
                },
                LogEntry { 
                    index: 3, 
                    term: 2, 
                    log_entry_type: LogEntryType::LogCommand as i32, 
                    data: "log_cmd".as_bytes().to_vec() 
                },
            ],
            commit_index: 3,
        };

        let response: AppendEntriesResponse = client_transport.append_entries(request).await.expect("Append entries request failed");
        assert_eq!(response.success, true);
        assert_eq!(response.term, 1);
    }

    #[tokio::test]
    async fn test_request_vote() {
        let server_addr = "[::]:50052";
        let  recv = setup_server(server_addr).await;

        let request_vote_response = RequestVoteResponse {
            term: 1,
            vote_granted: true,
        };
        spawn_request_listener(recv, RaftTransportResponse::RequestVote(request_vote_response)).await;

        let client_transport: RaftGrpcTransport = setup_client(server_addr).await;

        let request: RequestVoteRequest = RequestVoteRequest {
            term: 1,
            candidate_id: "aws-node-1".to_string(),
            last_log_index: 10,
            last_log_term: 1,
            commit_index: 3,
        };

        let response = client_transport.request_vote(request).await.expect("Request vote request failed");
        assert_eq!(response.vote_granted, true);
        assert_eq!(response.term, 1);
    }
}
