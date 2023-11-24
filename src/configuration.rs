use std::{sync::Arc, fs::{File, self}, io::Write, path::Path};
use futures::io;
use serde::{Deserialize, Serialize};
use crate::node::Node;

#[derive(Debug, Clone)]
pub enum ConfigCommand {
    AddNode { node: Arc<Node> },
    RemoveNode { node: Arc<Node> },
    TransitionNode { node: Arc<Node> },
}


#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct  MembershipConfigurations {
    // represents the current configuration denoted by (C_old) which has been comitted.
    pub comitted: Configuration,

    /// The index for the current configuration and its relative to the committed log 
    pub commit_index: u64,

    // This is the log index of the new configuration and it is relative to latest 
    pub index: u64,

    // Represents the new configuration denoted by (C_new) and according to Raft Paper, The new configuration takes effect on each server as soon as it is added
    // to that server’s log: the C_new entry is replicated to the C_new servers, and a majority of the new
    // configuration is used to determine the C_new entry’s commitment
    pub latest: Configuration,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConfigStore {
    config_path: String, 
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub config: Configuration,
    pub index: u64,
}

impl ConfigStore {
    pub fn new(config_path: String) -> Self {
        ConfigStore { config_path }
    }
    pub fn persist(&self, configuration: Configuration, index: u64) -> io::Result<()> {
        let c = Config{ config: configuration, index};

        let serialized_data = bincode::serialize(&c).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let path = Path::new(&self.config_path);
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }

        let mut file = File::create(path)?;
        file.write_all(&serialized_data)?;
        Ok(())
    }

    pub fn load(&self) -> Result<Config, Box<dyn std::error::Error>> {
        let serialized_data = fs::read(self.config_path.clone())?;
        let config = bincode::deserialize(&serialized_data)?;
        Ok(config)
    }
}


impl MembershipConfigurations {
    pub fn new(comitted: Configuration, commit_index: u64, index: u64, latest: Configuration) -> Self {
        MembershipConfigurations { comitted, commit_index, index, latest}
    }

    pub fn set_latest_configuration(&mut self, new_config: Configuration, index: u64) {
        self.latest = new_config;
        self.index = index;
    }

    pub fn set_comitted_configuration(&mut self, current_config: Configuration, commit_index: u64) {
        self.comitted = current_config;
        self.commit_index = commit_index;
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Configuration {
    #[serde(serialize_with = "serialize_nodes", deserialize_with = "deserialize_nodes")]
    pub nodes: Vec<Arc<Node>>,
}

fn serialize_nodes<S>(nodes: &Vec<Arc<Node>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let nodes_as_plain: Vec<_> = nodes.iter().map(|node| (**node).clone()).collect();
    Serialize::serialize(&nodes_as_plain, serializer)
}

fn deserialize_nodes<'de, D>(deserializer: D) -> Result<Vec<Arc<Node>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let nodes_as_plain: Vec<Node> = Vec::deserialize(deserializer)?;
    Ok(nodes_as_plain.into_iter().map(|data| Arc::new(data)).collect())
}

impl Configuration {
    pub fn new(nodes: Vec<Arc<Node>>) -> Self {
        Configuration {  nodes }
    }
    pub fn serialize(&self) -> Result<Vec<u8>, Box<bincode::ErrorKind>>  {
       bincode::serialize(self)
    }
    pub fn deserialize(data: &Vec<u8>) -> Result<Configuration, Box<bincode::ErrorKind>>  {
        bincode::deserialize(data)
    }
    pub fn new_configuration(old: &Configuration, command: ConfigCommand) -> Self {
        let mut new_config = old.clone();
        match command {
            ConfigCommand::AddNode { node } => {
                if !new_config.nodes.iter().any(|existing_node| existing_node.id == node.id) {
                    new_config.nodes.push(node);
                }
            },
            ConfigCommand::RemoveNode { node } => {
                new_config.nodes.retain(|existing_node| existing_node.id != node.id);
            },
            ConfigCommand::TransitionNode { node } => {
                new_config.nodes = new_config.nodes
                    .into_iter()
                    .map(|existing_node| {
                        if existing_node.id == node.id {
                            let mut updated_node = (*existing_node).clone();
                            updated_node.node_type = node.node_type.clone();
                            Arc::new(updated_node)
                        } else {
                            existing_node
                        }
                    })
                    .collect();
            }
        }
        new_config
    }
}


#[cfg(test)]
mod tests {
    use crate::node::NodeType;

    use super::*;

    #[test]
    fn test_add_node() {
        let node1 = Arc::new(Node { id:"1".to_string(), address: "127.0.0.1:5000".to_string(), node_type: NodeType::Voter });
        let node2 = Arc::new(Node { id: "2".to_string(), address: "127.0.0.1:5001".to_string(),node_type: NodeType::Voter });
        let old_config = Configuration::new(vec![node1.clone()]);
        
        let new_config = Configuration::new_configuration(&old_config, ConfigCommand::AddNode { node: node2.clone() });

        assert_eq!(new_config.nodes.len(), 2);
        assert!(new_config.nodes.contains(&node1));
        assert!(new_config.nodes.contains(&node2));

        assert!(!old_config.nodes.contains(&node2));
    }

    #[test]
    fn test_remove_node() {
        let node1 = Arc::new(Node { id:"1".to_string(), address: "127.0.0.1:5000".to_string(), node_type: NodeType::Voter });
        let node2 = Arc::new(Node { id: "2".to_string(), address: "127.0.0.1:5001".to_string(),node_type: NodeType::Voter });
        let old_config = Configuration::new(vec![node1.clone(), node2.clone()]);
        
        let new_config = Configuration::new_configuration(&old_config, ConfigCommand::RemoveNode { node: node2.clone() });

        assert_eq!(new_config.nodes.len(), 1);
        assert!(new_config.nodes.contains(&node1));
        assert!(!new_config.nodes.contains(&node2));
    }

    #[test]
    fn test_transition_node() {
        let node1 = Arc::new(Node { id:"1".to_string(), address: "127.0.0.1:5000".to_string(), node_type: NodeType::Voter });
        let node2 = Arc::new(Node { id: "2".to_string(), address: "127.0.0.1:5001".to_string(),node_type: NodeType::Voter });
        let old_config = Configuration::new(vec![node1.clone(), node2.clone()]);

        let updated_node2 = Arc::new(Node { id: "2".to_string(), address: "127.0.0.1:5001".to_string(),node_type: NodeType::NonVoter });
        let new_config = Configuration::new_configuration(&old_config, ConfigCommand::TransitionNode { node: updated_node2.clone() });

        assert_eq!(new_config.nodes.len(), 2);
        assert!(new_config.nodes.contains(&node1));
        let transitioned_node = new_config.nodes.iter().find(|&n| n.id == "2").unwrap();
        assert_eq!(transitioned_node.node_type, NodeType::NonVoter);
    }

    #[test]
    fn test_serialize_configuration() {
        let node1 = Arc::new(Node { id:"1".to_string(), address: "127.0.0.1:5000".to_string(), node_type: NodeType::Voter });
        let node2 = Arc::new(Node { id: "2".to_string(), address: "127.0.0.1:5001".to_string(),node_type: NodeType::Voter });
        let config = Configuration::new(vec![node1.clone(), node2.clone()]);

        let serialized_data = config.serialize().expect("Failed to serialize the configuration");
        let deserialized_config = Configuration::deserialize(&serialized_data).expect("Failed to deserialize the configuration");
       
        assert_eq!(config, deserialized_config);
    }

    #[test]
    fn test_config_store_persistence() {
       
        let node1 = Arc::new(Node { id: "1".to_string(), address: "127.0.0.1:5000".to_string(), node_type: NodeType::Voter });
        let node2 = Arc::new(Node { id: "2".to_string(), address: "127.0.0.1:5001".to_string(), node_type: NodeType::Voter });
        let configuration = Configuration::new(vec![node1, node2]);
        let index = 42; 

       
        let config_store = ConfigStore::new("./tmp/config_file.bin".to_string());
        config_store.persist(configuration.clone(), index).expect("Failed to persist configuration");

    
        let loaded_config = config_store.load().expect("Failed to load configuration");

        
        assert_eq!(loaded_config.config, configuration);
        assert_eq!(loaded_config.index, index);
    }
}
