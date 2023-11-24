use crate::{log::LogEntry,storage::LogStore, error::StoreError};
use std::{collections::HashMap, vec};

pub struct MockLogStore {
    pub logs: HashMap<u64, LogEntry>,
}

impl MockLogStore {
    #[allow(dead_code)]
    pub fn new() -> Self {
        MockLogStore { logs: HashMap::new() }
    }
}

impl LogStore for MockLogStore {
    fn get_log(&self, idx: u64) -> Result<Option<LogEntry>, StoreError> {
        let result = self.logs.get(&idx).cloned();
        Ok(result)
    }
    #[allow(dead_code)]
    fn get_logs_from_range(&self, _min_idx: u64, _max_idx: u64)-> Result<Vec<LogEntry>, StoreError> {
        Ok(vec![])
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

    fn delete_range(&mut self, min_idx: u64, max_idx: u64)->  Result<(), StoreError> {
        for idx in min_idx..=max_idx {
            self.logs.remove(&idx);
        }
        Ok(())
    }
}
