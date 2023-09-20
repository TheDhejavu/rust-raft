use tonic::{Request, Response, Status, transport::{Channel, Server, Endpoint}};
use async_trait::async_trait;
use tokio::sync::{Mutex, mpsc};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use crate::{raft::raft_grpc_transport_client::RaftGrpcTransportClient, utils::{format_endpoint_addr, format_server_addr}};
use crate::raft::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse, raft_grpc_transport_server};

pub enum RaftTransportRequest {
    AppendEntries(AppendEntriesRequest),
    RequestVote(RequestVoteRequest),
}

pub enum RaftTransportResponse {
    AppendEntries(AppendEntriesResponse),
    RequestVote(RequestVoteResponse),
}

pub struct RaftGrpcTransport {
    client: Arc<Mutex<RaftGrpcTransportClient<Channel>>>,
}

#[async_trait]
pub trait RaftTransport {
    async fn append_entries(&self, request: AppendEntriesRequest) -> Result<AppendEntriesResponse, tonic::Status>;
    async fn request_vote(&self, request: RequestVoteRequest) -> Result<RequestVoteResponse, tonic::Status>;
}

impl RaftGrpcTransport {
    pub async fn new(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let endpoint_addr = format_endpoint_addr(addr);
        let endpoint = Endpoint::from_shared(endpoint_addr)?;
        let channel = endpoint.connect().await?;
        let client = RaftGrpcTransportClient::new(channel);
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
    pub fn new(rpc_send_channel: mpsc::Sender<(RaftTransportRequest, mpsc::Sender<RaftTransportResponse>)>) -> Self {
        RaftGrpcTransportServer { rpc_send_channel }
    }

    pub async fn run(self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let server_addr = format_server_addr(addr).parse()?;

        // Just spawn the server task and don't wait on it
        tokio::spawn(async move {
            if let Err(e) = Server::builder()
                .add_service(raft_grpc_transport_server::RaftGrpcTransportServer::new(self))
                .serve(server_addr)
                .await {
                    eprintln!("Server error: {}", e);
                }
        });
        sleep(Duration::from_millis(100)).await;

        println!("Server scheduled to start on {}", addr);
        Ok(())
    }
}

#[async_trait]
impl raft_grpc_transport_server::RaftGrpcTransport for RaftGrpcTransportServer {
    
    async fn append_entries(&self, request: Request<AppendEntriesRequest>) -> Result<Response<AppendEntriesResponse>, Status> {
        let (response_sender, mut response_receiver) = mpsc::channel(1);
        let _ = self.rpc_send_channel.send((RaftTransportRequest::AppendEntries(request.into_inner()), response_sender)).await;

        match response_receiver.recv().await {
            Some(RaftTransportResponse::AppendEntries(res)) => Ok(Response::new(res)),
            _ => Err(Status::internal("Unexpected response type"))
        }
    }

    async fn request_vote(&self, request: Request<RequestVoteRequest>) -> Result<Response<RequestVoteResponse>, Status> {
        let (response_sender, mut response_receiver) = mpsc::channel(1);
        let _ = self.rpc_send_channel.send((RaftTransportRequest::RequestVote(request.into_inner()), response_sender)).await;

        match response_receiver.recv().await {
            Some(RaftTransportResponse::RequestVote(res)) => Ok(Response::new(res)),
            _ => Err(Status::internal("Unexpected response type"))
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::raft::{LogEntry, LogEntryType};
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_append_entries() {
        // Setup server
        let (send, mut recv) = mpsc::channel::<(RaftTransportRequest, mpsc::Sender<RaftTransportResponse>)>(1);
        let server = RaftGrpcTransportServer::new(send);
        let server_addr = "[::]:50051";
       
        let result = server.run(server_addr).await.is_ok();
        assert_eq!(result, true, "failed to start server");

        // set up request listerners on the handler
        tokio::spawn(async move {
            if let Some((RaftTransportRequest::AppendEntries(req), response_sender)) = recv.recv().await {
                let response = AppendEntriesResponse {
                    term: req.term,
                    success: true,
                };
                response_sender.send(RaftTransportResponse::AppendEntries(response)).await.expect("Failed to send response");
            }
        });

        // setup client
        let client_transport = RaftGrpcTransport::new(server_addr).await.expect("Failed to create client");
        
        // create a request
        let request = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 3,
            prev_log_term: 4,
            entries: vec![
                LogEntry { 
                    index: 1, 
                    term: 1, 
                    log_entry_type: LogEntryType::CommandLog as i32, 
                    data: "command_data".as_bytes().to_vec() 
                },
                LogEntry { 
                    index: 2, 
                    term: 1, 
                    log_entry_type: LogEntryType::ConfigurationLog as i32, 
                    data: "config_data".as_bytes().to_vec() 
                },
                LogEntry { 
                    index: 3, 
                    term: 2, 
                    log_entry_type: LogEntryType::CommandLog as i32, 
                    data: "another_command_data".as_bytes().to_vec() 
                },
            ],
            commit_index: 3,
        };
        
        // Send request and await response
        let response = client_transport.append_entries(request).await.expect("RPC failed");
        assert_eq!(response.success, true);
        assert_eq!(response.term, 1);
    }

}
