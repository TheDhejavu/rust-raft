extern crate rust_raft;
use std::vec;
use rust_raft::{
    RaftNodeServer, log::LogEntry,
    grpc_transport::{
        RaftTransportRequest, RaftTransportResponse, RaftGrpcTransportServer,
    }, config::Config, node::Node, datastore::RaftSledStore
};
use tokio::sync::mpsc;
use clap::{App, Arg};

#[derive(Debug)]
struct FSM {}

impl FSM {
    fn new() -> FSM {
        FSM{}
    }
}

impl rust_raft::fsm::FSM for FSM {
    fn apply(&self, log_entry: &LogEntry) -> Box<dyn std::any::Any> {
        println!("{:?}", log_entry);
        Box::new(())  
    }
}

#[tokio::main]
async fn main() {
    let matches = App::new("Rust Raft Example")
        .arg(Arg::with_name("addr")
            .long("addr")
            .value_name("ADDRESS")
            .help("Sets the current server address")
            .required(true)
            .takes_value(true))
        .arg(Arg::with_name("id")
            .long("id")
            .value_name("IDENTIFIER")
            .help("Sets server identifier")
            .required(true)
            .takes_value(true))
        .arg(Arg::with_name("peers")
            .long("peers")
            .value_name("PEERS")
            .help("Comma-separated list of peer addresses")
            .required(true)
            .takes_value(true))
        .get_matches();

    let server_addr = matches.value_of("addr").unwrap();
    let server_id = matches.value_of("id").unwrap();

    // Parse peer addresses from the comma-separated list
    let peer_addresses = matches.value_of("peers").unwrap().split(',').collect::<Vec<&str>>();
    
    // Step 1: Create your channels (SEND & RECEIVE)
    let (rpc_send_channel, rpc_recv_channel) = mpsc::channel::<(RaftTransportRequest, mpsc::Sender<RaftTransportResponse>)>(1);

    // Step 2: Create the grpc_server using the sender part of the channel
    let grpc_server: RaftGrpcTransportServer = RaftGrpcTransportServer::new(rpc_send_channel);

    let cfg = Config::build().server_id(server_id.to_string()).validate().unwrap();
    
    let mut nodes: Vec<Node> = vec![];

    for (index, address) in peer_addresses.iter().enumerate() {
        nodes.push(Node {
            id: format!("peer-{}", index + 1), 
            address: address.to_string(),
        });
    }

    let fsm: FSM = FSM::new();
    let log_store: RaftSledStore = RaftSledStore::new(&format!("./tmp/raft-db_{}", server_id.to_string())).unwrap();

    // Step 3: Create the RaftNodeServer 
    let mut raft_node: RaftNodeServer = RaftNodeServer::new(
        server_addr.to_string(),
        cfg,
        nodes,
        Box::new(fsm),
        Box::new(log_store),
        rpc_recv_channel,
    ).await;

    // Step 4: GRPC start
    grpc_server.run(server_addr).await.unwrap();

    // Step 5: Start raft server node
    raft_node.start().await;
}
