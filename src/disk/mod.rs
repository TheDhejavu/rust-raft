use sled::{Config, Db, IVec};
use std::convert::TryInto;
use crate::{log::LogEntry,storage::LogStore, errors::StoreError};

pub struct RaftSledStore {
    db: Db,
}

impl RaftSledStore {
    pub fn new(path: &str) -> Result<Self, sled::Error> {
        let config = Config::new().path(path);
        let db = config.open()?;
        Ok(Self { db })
    }

    // Sled utilizes IVec as the Rust counterpart for bytes, necessitating the conversion of the index to IVec.
    fn key_for_index(idx: u64) -> IVec {
        IVec::from(&idx.to_be_bytes())
    }
}

impl LogStore for RaftSledStore {
    fn get_log(&self, idx: u64) -> Option<LogEntry> {
        // sled returns a result, so we map errors to None
        self.db.get(Self::key_for_index(idx))
        .ok()?
        .and_then(|ivec| { LogEntry::from_bytes(&ivec) })     
    }

    fn store_log(&mut self, log: &LogEntry) -> Result<(), StoreError> {
        let bytes = log.to_bytes();
        self.db.insert(Self::key_for_index(log.index), bytes)
            .map_err(|e| StoreError::InsertionError(e.to_string()))?;
        Ok(())
    }

    fn store_logs(&mut self, logs: &[LogEntry]) -> Result<(), StoreError> {
        let mut batch = sled::Batch::default();
        
        for log in logs {
            let key = Self::key_for_index(log.index);
            batch.insert(key, log.to_bytes());
        }
    
        self.db.apply_batch(batch)
            .map_err(|e| StoreError::InsertionError(e.to_string()))?;
    
        Ok(())
    }
    
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

    fn delete_range(&mut self, min_idx: u64, max_idx: u64) {
        for idx in min_idx..=max_idx {
            let _ = self.db.remove(Self::key_for_index(idx));
        }
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

        let mut store = RaftSledStore::new(path).unwrap();

        let log = LogEntry {
            index: 1,
            data: vec![1, 2, 3, 4],
            term: 1,
            log_entry_type: LogEntryType::CommandLog,
        };

        let stored_log = store.store_log(&log);
        assert_eq!(stored_log.is_ok(), true, "Expected an ok, but got {:?}", stored_log);
        
        let retrieved = store.get_log(1);
        assert_eq!(retrieved.is_some(), true, "Expected log entry at index 1, but found None.");

        // unrap log
        let retrieved_log = retrieved.unwrap();
        assert_eq!(log.index, retrieved_log.index);
        assert_eq!(log.data, retrieved_log.data);

        dir.close().unwrap();
    }

    #[test]
    fn test_store_and_retrieve_multiple_logs() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        let mut store = RaftSledStore::new(path).unwrap();

        let logs = vec![
            LogEntry {
                index:1,
                data:vec![1,2,3], 
                term: 1, 
                log_entry_type: 
                LogEntryType::CommandLog, 
            },
            LogEntry {
                index:2,
                data:vec![4,5,6], 
                term: 1,  
                log_entry_type: LogEntryType::CommandLog,
            },
        ];

        let stored_logs = store.store_logs(&logs);
        assert_eq!(stored_logs.is_ok(), true, "Expected an ok, but got {:?}", stored_logs);
        

        for log in logs.iter() {
            let retrieved_log = store.get_log(log.index).unwrap();
            assert_eq!(log.index, retrieved_log.index);
            assert_eq!(log.data, retrieved_log.data);
        }

        dir.close().unwrap();
    }

    #[test]
    fn test_store_and_retrieve_logs() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        let mut store = RaftSledStore::new(path).unwrap();  // Assuming you have such a constructor

        let logs = vec![
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

        store.store_logs(&logs).unwrap();

        assert_eq!(store.first_index(), 1);
        assert_eq!(store.last_index(), 2);

        dir.close().unwrap();
    }

    #[test]
    fn test_delete_range() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        let mut store = RaftSledStore::new(path).unwrap();

        let logs = vec![
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

        store.store_logs(&logs).unwrap();
        store.delete_range(1, 1);

        assert_eq!(store.first_index(), 2);
        assert_eq!(store.last_index(), 2);

        dir.close().unwrap();
    }

}
