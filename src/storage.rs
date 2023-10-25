use crate::{log::LogEntry, error::StoreError};

pub trait LogStore: Send + Sync {
    fn get_log(&self, idx: u64) -> Result<Option<LogEntry>, StoreError>;
    fn store_log(&mut self, log: &LogEntry) -> Result<(), StoreError>;
    fn store_logs(&mut self, logs: &[LogEntry]) -> Result<(), StoreError>;
    fn first_index(&self) -> u64;
    fn last_index(&self) -> u64;
    fn delete_range(&mut self, min_idx: u64, max_idx: u64);
    fn get_logs_from_range(&self, min_idx: u64, max_idx: u64)-> Result<(Vec<LogEntry>), StoreError>;
}