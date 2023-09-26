
use std::error::Error;
use thiserror;

use crate::config::{DEFAULT_ELECTION_TIMEOUT_MAX, DEFAULT_ELECTION_TIMEOUT_MIN};


#[derive(Debug, Clone)]
pub enum RaftError {
    InvalidStorageCapacity,
}

impl Error for RaftError {}

// Implement the Display trait, which is required by the Error trait
impl std::fmt::Display for RaftError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RaftError::InvalidStorageCapacity => write!(f, "Invalid capacity for LogStorage"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum StoreError {
    InsertionError(String),
}


#[derive(Debug)]
pub enum ConfigError {
    InvalidElectionTimeout,
    InvalidHeartbeatInterval,
    InvalidMaxLogEntries,
    InvalidMaxPayloadSize,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::InvalidElectionTimeout => write!(f, "Election timeout should be between {} and {} ms", DEFAULT_ELECTION_TIMEOUT_MIN, DEFAULT_ELECTION_TIMEOUT_MAX),
            ConfigError::InvalidHeartbeatInterval => write!(f, "Heartbeat interval should be less than election timeout"),
            ConfigError::InvalidMaxLogEntries => write!(f, "Maximum number of log entries should be greater than 0"),
            ConfigError::InvalidMaxPayloadSize => write!(f, "Maximum payload size should be greater than 0"),
        }
    }
}

impl std::error::Error for ConfigError {}

