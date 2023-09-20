use std::sync::{Arc, Mutex};
use crate::{log::LogEntry, error::{RaftError, StoreError}};

pub trait LogStore: Send + Sync {
    fn get_log(&self, idx: u64) -> Option<LogEntry>;
    fn store_log(&mut self, log: &LogEntry) -> Result<(), StoreError>;
    fn store_logs(&mut self, logs: &[LogEntry]) -> Result<(), StoreError>;
    fn first_index(&self) -> u64;
    fn last_index(&self) -> u64;
    fn delete_range(&mut self, min_idx: u64, max_idx: u64);
}

pub struct LogStorage {
    store: Box<dyn LogStore>,
    cache: Mutex<Vec<Option<Arc<LogEntry>>>>,
}

impl LogStorage {
    pub fn new(capacity: usize, store: Box<dyn LogStore>) -> Result<LogStorage, RaftError> {
        if capacity == 0 {
            return Err(RaftError::InvalidStorageCapacity);
        }

        Ok(LogStorage {
            store,
            cache: Mutex::new(vec![None; capacity]),
        })
    }
}

impl LogStore for LogStorage {
    fn get_log(&self, idx: u64) -> Option<LogEntry> {
        let cache = self.cache.lock().unwrap();
        let cached_idx = (idx % cache.len() as u64) as usize;
        if let Some(log) = &cache[cached_idx] {
            if log.index == idx {
                return Some(log.clone().as_ref().clone());
            }
        }
        drop(cache); 
        self.store.get_log(idx)
    }

    fn store_log(&mut self, log: &LogEntry) -> Result<(), StoreError> {
        self.store_logs(&[log.clone()])
    }

    fn store_logs(&mut self, logs: &[LogEntry]) -> Result<(), StoreError> {
        self.store.store_logs(logs)?;
        let mut cache = self.cache.lock().unwrap();
        for log in logs {
            let cached_idx = (log.index % cache.len() as u64) as usize;
            cache[cached_idx] = Some(Arc::new(log.clone()));
        }
        Ok(())
    }

    fn first_index(&self) -> u64 {
        self.store.first_index()
    }

    fn last_index(&self) -> u64 {
        self.store.last_index()
    }

    fn delete_range(&mut self, min_idx: u64, max_idx: u64) {
        let mut cache = self.cache.lock().unwrap();
        for idx in min_idx..=max_idx {
            let cached_idx = (idx % cache.len() as u64) as usize;
            cache[cached_idx] = None;
        }
        self.store.delete_range(min_idx, max_idx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{log::{LogEntry, LogEntryType}, mocks::MockLogStore};

    #[test]
    fn test_log_storage_operations() {
        let mock_store = Box::new(MockLogStore::new());
        let mut logs = LogStorage::new(5, mock_store).unwrap();

        let log_entries = vec![
            LogEntry {
                index: 1,
                term: 1,
                log_entry_type: LogEntryType::CommandLog,
                data: vec![1, 2, 3, 4],
            },
            LogEntry {
                index: 2,
                term: 2,
                log_entry_type: LogEntryType::ConfigurationLog,
                data: vec![5, 6, 7, 8],
            },
        ];

        logs.store_logs(&log_entries).unwrap();

        // Test retrieval
        assert_eq!(logs.get_log(1).unwrap().index, 1);
        assert_eq!(logs.get_log(2).unwrap().index, 2);
        assert!(logs.get_log(3).is_none());

        // Test deletion
        logs.delete_range(1, 1);
        assert!(logs.get_log(1).is_none());
        assert_eq!(logs.get_log(2).unwrap().index, 2);

        // Test first and last index
        assert_eq!(logs.first_index(), 2);
        assert_eq!(logs.last_index(), 2);
    }
}
