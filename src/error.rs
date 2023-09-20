
use std::error::Error;
use thiserror;


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
