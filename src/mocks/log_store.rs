use crate::{log::LogEntry,storage::LogStore, errors::StoreError};
use std::collections::HashMap;

pub struct MockLogStore {
    pub logs: HashMap<u64, LogEntry>,
}

impl MockLogStore {
    pub fn new() -> Self {
        MockLogStore { logs: HashMap::new() }
    }
}

impl LogStore for MockLogStore {
    fn get_log(&self, idx: u64) -> Option<LogEntry> {
        self.logs.get(&idx).cloned()
    }

    fn store_log(&mut self, log: &LogEntry) -> Result<(), StoreError> {
        self.logs.insert(log.index, log.clone());
        Ok(())
    }

    fn store_logs(&mut self, logs: &[LogEntry]) -> Result<(), StoreError> {
        for log in logs {
            self.logs.insert(log.index, log.clone());
        }
        Ok(())
    }

    fn first_index(&self) -> u64 {
        *self.logs.keys().min().unwrap_or(&0)
    }

    fn last_index(&self) -> u64 {
        *self.logs.keys().max().unwrap_or(&0)
    }

    fn delete_range(&mut self, min_idx: u64, max_idx: u64) {
        for idx in min_idx..=max_idx {
            self.logs.remove(&idx);
        }
    }
}
