// // Step 1: Create your channels and declare variables
// let server_addr = "127.0.0.1:50051";
// let (rpc_send_channel, mut rpc_recv_channel) = mpsc::channel::<(RaftTransportRequest, mpsc::Sender<RaftTransportResponse>)>(1);

//  Step 2: Setup client
//  let client_transport = RaftGrpcTransport::new(server_addr).await.expect("Failed to create client");
        
// // Step 2: Create the grpc_server using the sender part of the channel
// let grpc_server = RaftGrpcTransportServer::new(rpc_send_channel);

// // Step 3: Create the RaftNode using the receiver part of the channel
// let raft_node = RaftNode::new(
//     // ... (other parameters)
//     grpc_transport,
//     rpc_recv_channel,
// );


// Step 4
// grpc_server.run(server_addr).await.unwrap();

