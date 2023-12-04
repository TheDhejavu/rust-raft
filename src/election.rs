
use std::sync::Arc;
use futures::{Future, future::join_all};
use log::{error, info};
use crate::{raft::{
    RequestVoteRequest, RequestVoteResponse
}, node::{Node, NodeType, self}};


pub enum ElectionResult {
    Won,
    Lost,
    StepDown,
}

/// Initiates a campaign for a Raft node to request votes from all other nodes.
pub async fn campaign(
    nodes: &Vec<Arc<Node>>, 
    server_id: Arc<String>, 
    req: RequestVoteRequest,
    can_campaign: bool,
) -> impl Future<Output = Vec<(String, RequestVoteResponse)>> {

    let mut futures = Vec::new();

    let req = Arc::new(req);

    if can_campaign {
        for node in nodes {
            if node.node_type == NodeType::NonVoter {
                continue;
            }
            
            let req_clone = req.clone();
            let node_clone = node.clone();
            let current_server_id = server_id.clone();
            info!("vote.request.from <> {}", node.address);
            
            let future = async move {
                if node_clone.id.to_string() == current_server_id.to_string() {
                    Some((node_clone.id.clone(), RequestVoteResponse { term: req_clone.term, vote_granted: true }))
                } else {
                    match node::send_vote_request(&node_clone, (*req_clone).clone()).await {
                        Ok(response) => Some((node_clone.id.clone(), response)),
                        Err(e) => {
                            error!("unable to send vote request to {}: {}", node_clone.address, e);
                            return  None;
                        }
                    }
                }
            };

            futures.push(future);
        }
    }
    async move {
        join_all(futures).await.into_iter().filter_map(|res| res).collect()
    }
}