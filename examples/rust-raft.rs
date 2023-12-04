extern crate rust_raft;
extern crate serde_derive;
extern crate bincode;
extern crate clap;

use serde_json::json;
use actix_web::{App as ActixWeb, HttpServer, HttpResponse,Error,  web};
use log::{info, error};
use rust_raft::{
    RaftNodeServer, log::{LogEntry, LogEntryType},
    grpc_transport::{
        RaftTransportRequest, RaftTransportResponse, RaftGrpcTransportServer,
    }, config::Config, node::Node, datastore::{RaftSledLogStore, RaftSledKVStore}, RaftNodeServerMessage, error::RaftError, configuration, RaftCore
};

use async_trait::async_trait;
use tokio::sync::mpsc;
use clap::{App, Arg, SubCommand};
use serde_derive::{Serialize, Deserialize};
pub use database::MyDatabase;
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;
use rand::Rng;
use reqwest;
mod database;

struct FSM {
    database: Arc<RwLock<MyDatabase>>,
}

#[derive(Serialize, Deserialize, Debug)]
enum StateMachineCommand {
    Add { key: String, value: String },
    Remove { key: String },
}

impl FSM {
    fn new(database: Arc<RwLock<MyDatabase>>) -> FSM {
        FSM { database }
    }
}   

#[async_trait]
impl rust_raft::fsm::FSM for FSM {
    async fn apply(&mut self, log_entry: &LogEntry) -> Box<dyn std::any::Any> {
        info!("{:?}", log_entry);

        let log_entry_type = log_entry.log_entry_type;
        match log_entry_type {
            LogEntryType::LogCommand => {
                let command: StateMachineCommand = bincode::deserialize(&log_entry.data).expect("Failed to deserialize command");
                match command {
                    StateMachineCommand::Add { key, value } => {
                        self.database.write().await.add(&key, &value).expect("failed to add data");
                    }
                    StateMachineCommand::Remove { key } => {
                        self.database.write().await.delete(&key).expect("failed to remove data");
                    }
                }
            }
            LogEntryType::LogConfCommand => {}
            LogEntryType::LogNoOp => {}
        }
        Box::new(())
    }
}

fn setup_cli() -> clap::ArgMatches<'static> {
    App::new("Rust Raft Example")
        .about("A Raft implementation example in Rust")
        .subcommand(
            SubCommand::with_name("start")
                .about("Start the Raft server")
                .arg(Arg::with_name("addr").short("a").long("addr").value_name("ADDRESS").required(true).takes_value(true))
                .arg(Arg::with_name("http-port").short("h").long("http-port").value_name("HTTP_PORT").required(true).takes_value(true))
                .arg(Arg::with_name("id").short("i").long("id").value_name("IDENTIFIER").required(true).takes_value(true))
                .arg(Arg::with_name("peers").short("p").long("peers").value_name("PEERS").required(true).takes_value(true))
        )
        .subcommand(
            SubCommand::with_name("store")
                .about("Send multiple store requests to the leader")
                .arg(Arg::with_name("leader-addr").short("l").long("leader-addr").value_name("LEADER_ADDRESS").required(true).takes_value(true))
        )
        .get_matches()
}


#[tokio::main]
async fn main() {
    let cli = setup_cli();
    match cli.subcommand() {
        ("start", Some(cli)) => {
            let http_port: u16 = cli.value_of("http-port").unwrap().parse().unwrap();
            let server_addr = cli.value_of("addr").unwrap();
            let server_id = cli.value_of("id").unwrap();
            let peer_addresses = cli.value_of("peers").unwrap().split(',').collect::<Vec<&str>>();

            let (rpc_tx, rpc_rx) = mpsc::channel::<(RaftTransportRequest, mpsc::Sender<RaftTransportResponse>)>(1);

            let grpc_server: RaftGrpcTransportServer = RaftGrpcTransportServer::new(rpc_tx);

            let cfg = Config::build().server_id(server_id.to_string()).validate().unwrap();

            let mut nodes: Vec<Arc<Node>> = vec![];

            for (index, address) in peer_addresses.iter().enumerate() {
                nodes.push(Arc::new(Node {
                    id: format!("node-aws-{}", index),
                    address: address.to_string(),
                    node_type: rust_raft::node::NodeType::Voter,
                }));
            }

            let fsm_database = MyDatabase::new(&format!("./tmp/db_{}/state_machine", server_id.to_string())).unwrap();
            let db = Arc::new(RwLock::new(fsm_database));
            let fsm: FSM = FSM::new(db.clone());

            let logs = RaftSledLogStore::new(&format!("./tmp/db_{}/logs", server_id.to_string())).unwrap();
            let stable = RaftSledKVStore::new(&format!("./tmp/db_{}/stable", server_id.to_string())).unwrap();

            let (send, recv) = mpsc::unbounded_channel::<(RaftNodeServerMessage, mpsc::Sender<Result<(), RaftError>>)>();

            let mut raft_node: RaftNodeServer = RaftNodeServer::new(
                server_id.to_string(),
                cfg,
                nodes,
                Box::new(fsm),
                Box::new(stable),
                Box::new(logs),
                rpc_rx,
                recv,
            ).await;


            // Create the web server.
            let server = WebServer::new(send.clone(), db, raft_node.core.clone());

            // Run the server.
            let addr = format!("127.0.0.1:{}", http_port.to_string());
          
            // Start Raft node, GRPC server and API API
            _ = tokio::join!(
                raft_node.run(),
                grpc_server.run(server_addr),
                server.run(&addr),
            )
        },
        ("store", Some(cli)) => {
            let leader_addr = cli.value_of("leader-addr").unwrap();
            send_store_requests(leader_addr).await;
        },
        _ => {
            eprintln!("Invalid command. Use --help for more info.");
        }
    }
}

const NUM_REQUESTS: usize = 10;

async fn send_store_requests(leader_addr: &str) {
    let client = reqwest::Client::new();

    for _ in 0..NUM_REQUESTS {
        let key = generate_random_string(5);
        let value = generate_random_string(10);

        let res = client.post(&format!("{}/store", leader_addr))
            .json(&StoreRequestBody {
                key: key.clone(),
                value: value.clone(),
            })
            .send()
            .await;

        match res {
            Ok(response) => {
                if response.status().is_success() {
                    println!("Stored key-value pair: {}-{}", key, value);
                } else {
                    println!("Failed to store key-value pair: {}-{}. Status: {:?}", key, value, response.status());
                }
            },
            Err(e) => {
                println!("Failed to send request: {}", e);
            }
        }
    }
}

fn generate_random_string(length: usize) -> String {
    let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyz".chars().collect();
    let mut rng = rand::thread_rng();
    (0..length).map(|_| chars[rng.gen_range(0..chars.len())]).collect()
}

pub struct WebServer {
    message_tx: mpsc::UnboundedSender<(RaftNodeServerMessage, mpsc::Sender<Result<(), RaftError>>)>,
    database: Arc<RwLock<MyDatabase>>,
    raft_node_core: Arc<Mutex<RaftCore>>,
}


struct AppState {
    message_tx: mpsc::UnboundedSender<(RaftNodeServerMessage, mpsc::Sender<Result<(), RaftError>>)>,
    database: Arc<RwLock<MyDatabase>>,
    raft_node_core: Arc<Mutex<RaftCore>>,
}

impl WebServer {
    pub fn new(
        message_tx: mpsc::UnboundedSender<(RaftNodeServerMessage, mpsc::Sender<Result<(), RaftError>>)>,
        database: Arc<RwLock<MyDatabase>>,
        raft_node_core: Arc<Mutex<RaftCore>>,
    ) -> Self {
        WebServer {message_tx, database, raft_node_core }
    }

    pub async fn run(&self, addr: &str) -> std::io::Result<()> {
        let database = self.database.clone();
        let message_tx = self.message_tx.clone();
        let raft_node_core = self.raft_node_core.clone();
      
        HttpServer::new(move || {
            ActixWeb::new()
                .app_data(web::Data::new(AppState {
                    message_tx: message_tx.clone(),
                    database: database.clone(),
                    raft_node_core: raft_node_core.clone(),
                }))
                .route("/inspect", web::get().to(inspect_handler))
                .route("/store", web::post().to(store_handler))
                .route("/delete", web::delete().to(delete_handler))
                .route("/config", web::post().to(config_handler))
        })
        .bind(addr)?
        .run()
        .await
    }
}

async fn inspect_handler(
    data: web::Data<AppState>,
) -> Result<HttpResponse, Error> {
    let database = &data.database;
    let db = database.read().await;
    let contents = db.list().unwrap_or_default();

    // Construct the HTML table header
    let mut response = String::from("<table border='1'>\n<tr><th>Key</th><th>Value</th></tr>\n");

    // Add each key-value pair as a table row
    for (key, value) in contents {
        response.push_str(&format!("<tr><td>{}</td><td>{}</td></tr>\n", key, value));
    }
    
    response.push_str("</table>");
    Ok(HttpResponse::Ok().body(response))
}

#[derive(Serialize, Deserialize, Debug)]
struct StoreRequestBody {
    key: String,
    value: String,
}
async fn store_handler(
    data: web::Data<AppState>,
    req_body: web::Json<StoreRequestBody>,
) -> Result<HttpResponse, actix_web::Error> {
    let raft_message_tx = &data.message_tx;
    let command = StateMachineCommand::Add { key: req_body.key.to_string(), value: req_body.value.to_string() };
    if !data.raft_node_core.lock().unwrap().is_leader() {
        error!("[Err] Not a leader");
    }

    let data = bincode::serialize(&command)
        .map_err(|_| actix_web::error::ErrorInternalServerError("Serialization error"))?;
    
    let (tx, mut rx) = mpsc::channel::<Result<(), RaftError>>(1);

    raft_message_tx.send((RaftNodeServerMessage::Apply(data), tx))
        .map_err(|e| {
            error!("unable to store data: {}", e);
            actix_web::error::ErrorInternalServerError("Internal error")
        })?;
    
    match rx.recv().await {
        Some(Ok(_)) => Ok(HttpResponse::Ok().finish()),
        Some(Err(RaftError::NotALeader)) => Ok(HttpResponse::BadRequest().json(json!({ "message": "Node is not a leader"}))),
        _ => Ok(HttpResponse::InternalServerError().finish()),
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct DeleteRequestBody {
    key: String,
}

async fn delete_handler(
    data: web::Data<AppState>,
    req_body: web::Json<DeleteRequestBody>,
) -> Result<HttpResponse, actix_web::Error> {
    let raft_message_tx = &data.message_tx;
    let command = StateMachineCommand::Remove { key: req_body.key.to_string() };

    let data = bincode::serialize(&command)
        .map_err(|_| actix_web::error::ErrorInternalServerError("Serialization error"))?;
    
    let (tx, mut rx) = mpsc::channel::<Result<(), RaftError>>(1);
    
    raft_message_tx.send((RaftNodeServerMessage::Apply(data), tx))
        .map_err(|e| {
            error!("unable to store data: {}", e);
            actix_web::error::ErrorInternalServerError("Internal error")
        })?;

    match rx.recv().await {
        Some(Ok(_)) => Ok(HttpResponse::Ok().finish()),
        Some(Err(RaftError::NotALeader)) => Ok(HttpResponse::BadRequest().json(json!({ "message": "Node is not a leader"}))),
        _ => Ok(HttpResponse::InternalServerError().finish()),
    }
}


#[derive(Serialize, Deserialize, Debug, PartialEq)]
enum ConfigAction {
    REMOVE,
    ADD,
}

#[derive(Serialize, Deserialize, Debug)]
struct ConfigRequestBody {
    id: String,
    action: ConfigAction,
    address: String,
}
async fn config_handler(
    data: web::Data<AppState>,
    req_body: web::Json<ConfigRequestBody>,
) -> Result<HttpResponse, actix_web::Error> {
    let raft_message_tx = &data.message_tx;
    let command: configuration::ConfigCommand;

    if req_body.action == ConfigAction::ADD {
        command = configuration::ConfigCommand::AddNode { node: Arc::new(Node {
            id: req_body.id.to_string(),
            address: req_body.address.to_string(),
            node_type: rust_raft::node::NodeType::Voter,
        })};
    } else {
        command = configuration::ConfigCommand::RemoveNode { node: Arc::new(Node {
            id: req_body.id.to_string(),
            address: req_body.address.to_string(),
            node_type: rust_raft::node::NodeType::Voter,
        })};
    }

    let (tx, mut rx) = mpsc::channel::<Result<(), RaftError>>(1);
    
    raft_message_tx.send((RaftNodeServerMessage::ApplyConfChange(command), tx))
        .map_err(|e| {
            error!("unable to apply config data: {}", e);
            actix_web::error::ErrorInternalServerError("Internal error")
        })?;

    match rx.recv().await {
        Some(Ok(_)) => Ok(HttpResponse::Ok().finish()),
        Some(Err(RaftError::NotALeader)) => Ok(HttpResponse::BadRequest().json(json!({ "message": "Node is not a leader"}))),
        _ => Ok(HttpResponse::InternalServerError().finish()),
    }
}
