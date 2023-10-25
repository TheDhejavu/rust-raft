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
    }, config::Config, node::Node, datastore::{RaftSledLogStore, RaftSledKVStore}, RaftNodeServerMessage, error::RaftError
};
use async_trait::async_trait;
use tokio::sync::mpsc;
use clap::{App, Arg, SubCommand};
use serde_derive::{Serialize, Deserialize};
pub use database::MyDatabase;
use std::sync::Arc;
use tokio::sync::RwLock;

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
    async fn apply(&self, log_entry: &LogEntry) -> Box<dyn std::any::Any> {
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
            LogEntryType::ConfCommand => {}
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

            let cfg = Config::build().server_id(server_id.to_string()).heartbeat_interval(130).validate().unwrap();

            let mut nodes: Vec<Node> = vec![];

            for (index, address) in peer_addresses.iter().enumerate() {
                nodes.push(Node {
                    id: format!("peer-{}", index + 1),
                    address: address.to_string(),
                });
            }

            let fsm_database = MyDatabase::new(&format!("./tmp/raft-db_{}/state_machine", server_id.to_string())).unwrap();
            let db = Arc::new(RwLock::new(fsm_database));
            let fsm: FSM = FSM::new(db.clone());

            let logs = RaftSledLogStore::new(&format!("./tmp/raft-db_{}/logs", server_id.to_string())).unwrap();
            let stable = RaftSledKVStore::new(&format!("./tmp/raft-db_{}/stable", server_id.to_string())).unwrap();

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
            let server = WebServer::new(send, db);

            // Run the server.
            let addr = format!("127.0.0.1:{}", http_port.to_string());
            let api_server = server.run(&addr);
           
            // Start Raft node, GRPC server and API API
            _ = tokio::join!(
                raft_node.start(),
                grpc_server.run(server_addr),
                api_server,
            );
        },
        _ => {
            eprintln!("Invalid command. Use --help for more info.");
        }
    }
}


pub struct WebServer {
    message_tx: mpsc::UnboundedSender<(RaftNodeServerMessage, mpsc::Sender<Result<(), RaftError>>)>,
    database: Arc<RwLock<MyDatabase>>,
}


struct AppState {
    message_tx: mpsc::UnboundedSender<(RaftNodeServerMessage, mpsc::Sender<Result<(), RaftError>>)>,
    database: Arc<RwLock<MyDatabase>>,
}

impl WebServer {
    pub fn new(
        message_tx: mpsc::UnboundedSender<(RaftNodeServerMessage, mpsc::Sender<Result<(), RaftError>>)>,
        database: Arc<RwLock<MyDatabase>>,
    ) -> Self {
        WebServer {message_tx, database }
    }

    pub async fn run(&self, addr: &str) -> std::io::Result<()> {
        let database = self.database.clone();
        let message_tx = self.message_tx.clone();
      
        HttpServer::new(move || {
            ActixWeb::new()
                .app_data(web::Data::new(AppState {
                    message_tx: message_tx.clone(),
                    database: database.clone(),
                }))
                .route("/inspect", web::get().to(inspect_handler))
                .route("/store", web::post().to(store_handler))
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
struct RequestBody {
    key: String,
    value: String,
}

async fn store_handler(
    data: web::Data<AppState>,
    req_body: web::Json<RequestBody>,
) -> Result<HttpResponse, Error> {
    let raft_message_tx = &data.message_tx;
    match bincode::serialize(&*req_body) {
        Ok(data) => {
            let (tx, mut rx) =  mpsc::channel::<Result<(), RaftError>>(1);
            match raft_message_tx.send((RaftNodeServerMessage::Apply(data), tx)) {
                Ok(_) => {
                    match rx.recv().await {
                        Some(Ok(_)) => Ok(HttpResponse::Ok().finish()),
                        Some(Err(e)) => {
                            match e {
                                RaftError::NotLeader => {
                                    Ok(HttpResponse::BadRequest().json(json!({ "message": "Node is not a leader"})))
                                }
                                _ => {
                                    Ok(HttpResponse::InternalServerError().finish())
                                }
                            }
                        }
                        _ => {
                            Ok(HttpResponse::InternalServerError().finish())
                        },
                    }
                },
                Err(e) =>  {
                    error!("unable to store data: {}", e);
                    Ok(HttpResponse::InternalServerError().finish())
                }
            }
        }
        Err(_) => {
            Ok(HttpResponse::InternalServerError().finish())
        }
    }
}
