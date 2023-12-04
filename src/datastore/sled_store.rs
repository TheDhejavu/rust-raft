use sled::{Config, Db, IVec};
use std::convert::TryInto;
use crate::{log::LogEntry, storage::LogStore, error::StoreError};

/// RaftSledLogStore is a Raft log store implementation using the Sled embedded database.
pub struct RaftSledLogStore {
    db: Db,
}

impl RaftSledLogStore {
    /// Creates a new RaftSledLogStore instance with the specified path.
    pub fn new(path: &str) -> Result<Self, sled::Error> {
        let config = Config::new()
            .path(path)
            .cache_capacity(1_000_000)
            .mode(sled::Mode::LowSpace);

        let db = config.open()?;
        Ok(Self { db })
    }

    fn key_to_bytes(&self, key: u64) -> IVec {
        IVec::from(&key.to_be_bytes())
    }
}

impl LogStore for RaftSledLogStore {
    /// Retrieves a log entry from the store based on the given index.
    ///
    /// # Arguments
    ///
    /// * `idx` - The index of the log entry to retrieve.
    ///
    /// # Returns
    ///
    /// A Result containing the log entry if found, or a StoreError if an error occurs.
    fn get_log(&self, idx: u64) -> Result<Option<LogEntry>, StoreError> {
        let key_bytes = self.key_to_bytes(idx);
        match self.db.get(&key_bytes) {
            Ok(Some(ivec)) => Ok(LogEntry::from_bytes(&ivec)),
            Ok(None) => Ok(None),
            Err(e) => Err(StoreError::Error(e.to_string())),
        }
    }

    /// Retrieves a range of log entries from the store.
    ///
    /// # Arguments
    ///
    /// * `min_idx` - The minimum index of the log entries to retrieve.
    /// * `max_idx` - The maximum index of the log entries to retrieve.
    ///
    /// # Returns
    ///
    /// A Result containing a vector of log entries within the specified range, or a StoreError if an error occurs.
    fn get_logs_from_range(&self, min_idx: u64, max_idx: u64) -> Result<Vec<LogEntry>, StoreError> {
        let mut logs = Vec::new();
        for idx in min_idx..=max_idx {
            let key_bytes = self.key_to_bytes(idx);
            match self.db.get(&key_bytes) {
                Ok(Some(ivec)) => {
                    let log_entry = LogEntry::from_bytes(&ivec).ok_or_else(|| {
                        StoreError::Error(format!("Failed to deserialize log entry at index {}", idx))
                    })?;
                    logs.push(log_entry);
                }
                Ok(None) => {}
                Err(e) => return Err(StoreError::Error(e.to_string())),
            }
        }

        Ok(logs)
    }

    /// Stores a single log entry in the store.
    ///
    /// # Arguments
    ///
    /// * `log` - The log entry to store.
    ///
    /// # Returns
    ///
    /// A Result indicating success or a StoreError if an error occurs.
    fn store_log(&mut self, log: &LogEntry) -> Result<(), StoreError> {
        let bytes = log.to_bytes();
        let key_bytes = self.key_to_bytes(log.index);
        self.db
            .insert(key_bytes, bytes)
            .map_err(|e| StoreError::InsertionError(e.to_string()))?;

        self.db.flush().map_err(|e| StoreError::Error(e.to_string()))?;
        Ok(())
    }

    /// Stores multiple log entries in the store as a batch operation.
    ///
    /// # Arguments
    ///
    /// * `logs` - A slice of log entries to store.
    ///
    /// # Returns
    ///
    /// A Result indicating success or a StoreError if an error occurs.
    fn store_logs(&mut self, logs: &[LogEntry]) -> Result<(), StoreError> {
        let mut batch = sled::Batch::default();

        for log in logs {
            let key_bytes = self.key_to_bytes(log.index);
            batch.insert(key_bytes, log.to_bytes());
        }

        self.db
            .apply_batch(batch)
            .map_err(|e| StoreError::InsertionError(e.to_string()))?;

        self.db.flush().map_err(|e| StoreError::Error(e.to_string()))?;

        Ok(())
    }

    /// Retrieves the index of the first log entry in the store.
    fn first_index(&self) -> u64 {
        self.db
            .iter()
            .keys()
            .next()
            .and_then(|result| result.ok())
            .and_then(|ivec| {
                let bytes: [u8; 8] = ivec.as_ref().try_into().ok()?;
                Some(u64::from_be_bytes(bytes))
            })
            .unwrap_or_default()
    }

    /// Retrieves the index of the last log entry in the store.
    fn last_index(&self) -> u64 {
        self.db
            .iter()
            .keys()
            .last()
            .and_then(|result| result.ok())
            .and_then(|ivec| {
                let bytes: [u8; 8] = ivec.as_ref().try_into().ok()?;
                Some(u64::from_be_bytes(bytes))
            })
            .unwrap_or_default()
    }

    /// Deletes a range of log entries from the store.
    ///
    /// # Arguments
    ///
    /// * `min_idx` - The minimum index of the log entries to delete.
    /// * `max_idx` - The maximum index of the log entries to delete.
    ///
    /// # Returns
    ///
    /// A Result indicating success or a StoreError if an error occurs.
    fn delete_range(&mut self, min_idx: u64, max_idx: u64) -> Result<(), StoreError> {
        for idx in min_idx..=max_idx {
            let key_bytes = self.key_to_bytes(idx);
            self.db.remove(key_bytes).map_err(|e| StoreError::Error(e.to_string()))?;
        }

        self.db.flush().map_err(|e| StoreError::Error(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use crate::log::LogEntryType;

    
    #[test]
    fn test_store_and_retrieve_log() {
      
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        let mut store = RaftSledLogStore::new(path).unwrap();

        let log = LogEntry {
            index: 1,
            data: vec![1, 2, 3, 4],
            term: 1,
            log_entry_type: LogEntryType::LogCommand,
        };

        let stored_log = store.store_log(&log);
        assert_eq!(stored_log.is_ok(), true, "Expected an ok, but got {:?}", stored_log);
        
        let log_result = store.get_log(1); 
        assert_eq!(log_result.is_ok(), true, "Expected log to be okay.");

        let log_result_data = log_result.unwrap();
        assert_eq!(log_result_data.is_some(), true, "Expected log entry at index 1, but found None.");
        
        // unrap log
        let retrieved_log = log_result_data.unwrap();
        assert_eq!(log.index, retrieved_log.index);
        assert_eq!(log.data, retrieved_log.data);

        dir.close().unwrap();
    }

    #[test]
    fn test_store_and_retrieve_multiple_logs() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        let mut store = RaftSledLogStore::new(path).unwrap();

        let logs = vec![
            LogEntry {
                index:1,
                data:vec![1,2,3], 
                term: 1, 
                log_entry_type: 
                LogEntryType::LogCommand, 
            },
            LogEntry {
                index:2,
                data:vec![4,5,6], 
                term: 1,  
                log_entry_type: LogEntryType::LogCommand,
            },
        ];

        let stored_logs = store.store_logs(&logs);
        assert_eq!(stored_logs.is_ok(), true, "Expected an ok, but got {:?}", stored_logs);
        

        for log in logs.iter() {
            let retrieved_log = store.get_log(log.index).unwrap().unwrap();
            
            assert_eq!(log.index, retrieved_log.index);
            assert_eq!(log.data, retrieved_log.data);
        }

        dir.close().unwrap();
    }

    #[test]
    fn test_store_and_retrieve_logs() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        let mut store = RaftSledLogStore::new(path).unwrap();  

        let logs = vec![
            LogEntry {
                index: 1,
                term: 1,
                log_entry_type: LogEntryType::LogConfCommand,
                data: vec![1, 2, 3, 4],
            },
            LogEntry {
                index: 2,
                term: 2,
                log_entry_type: LogEntryType::LogConfCommand,
                data: vec![5, 6, 7, 8],
            },
        ];

        store.store_logs(&logs).unwrap();

        assert_eq!(store.first_index(), 1);
        assert_eq!(store.last_index(), 2);

        dir.close().unwrap();
    }

    #[test]
    fn test_delete_range() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        let mut store = RaftSledLogStore::new(path).unwrap();

        let logs = vec![
            LogEntry {
                index: 1,
                term: 1,
                log_entry_type: LogEntryType::LogCommand,
                data: vec![1, 2, 3, 4],
            },
            LogEntry {
                index: 2,
                term: 2,
                log_entry_type: LogEntryType::LogConfCommand,
                data: vec![5, 6, 7, 8],
            },
        ];

        store.store_logs(&logs).unwrap();
        _ = store.delete_range(1, 1);

        assert_eq!(store.first_index(), 2);
        assert_eq!(store.last_index(), 2);

        dir.close().unwrap();
    }
}
