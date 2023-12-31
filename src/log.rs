use crate::raft;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogEntryType {
    LogCommand,
    LogNoOp,
    LogConfCommand,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub log_entry_type: LogEntryType,
    pub data: Vec<u8>,
}

impl LogEntry {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        
        // Store index and term as 8 bytes each
        bytes.extend(&self.index.to_be_bytes()); 
        bytes.extend(&self.term.to_be_bytes());
        
        // Use a single byte for the log entry type
        let type_byte = match self.log_entry_type {
            LogEntryType::LogCommand => 0u8,
            LogEntryType::LogConfCommand => 1u8,
            LogEntryType::LogNoOp => 2u8,
        };
        bytes.push(type_byte);
        
        // Store the length of the data vector as 4 bytes, followed by the actual data
        bytes.extend(&(self.data.len() as u32).to_be_bytes());
        bytes.extend(&self.data);

        // Total of 8 + 8 + 1 + 4
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 21 { 
            return None;
        }

        let index = u64::from_be_bytes(bytes[0..8].try_into().ok()?);
        let term = u64::from_be_bytes(bytes[8..16].try_into().ok()?);
        
        let log_entry_type = match bytes[16] {
            0 => LogEntryType::LogCommand,
            1 => LogEntryType::LogConfCommand,
            2 => LogEntryType::LogNoOp,
            _ => return None,
        };
        
        // Convert the next 4 bytes into a usize for the length of the data
        let data_len = u32::from_be_bytes(bytes[17..21].try_into().ok()?);
        if bytes.len() < 21 + data_len as usize {
            return None;
        }
        
        let data = bytes[21..21 + data_len as usize].to_vec();

        Some(LogEntry { index, term, log_entry_type, data })
    }

    pub fn from_rpc_raft_log(entry: raft::LogEntry) -> Option<LogEntry> {
        let log_entry_type = match entry.log_entry_type {
            x if x == raft::LogEntryType::LogCommand as i32 => Some(LogEntryType::LogCommand),
            x if x == raft::LogEntryType::LogConfCommand as i32 => Some(LogEntryType::LogConfCommand),
            x if x == raft::LogEntryType::LogNoOp as i32 => Some(LogEntryType::LogNoOp),
            _ => None,  
        };

        log_entry_type.map(|log_type| {
            LogEntry {
                index: entry.index,
                term: entry.term,
                log_entry_type: log_type,
                data: entry.data,
            }
        })
    }

    pub fn to_rpc_raft_log(self)  -> raft::LogEntry {
        raft::LogEntry  {
            index: self.index,
            term: self.term,
            log_entry_type: match self.log_entry_type {
                LogEntryType::LogCommand => 0,  
                LogEntryType::LogConfCommand => 1,
                LogEntryType::LogNoOp => 2,
            },
            data: self.data.clone(),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_bytes() {
        let log_entry = LogEntry {
            index: 12345,
            term: 67890,
            log_entry_type: LogEntryType::LogCommand,
            data: vec![1, 2, 3, 4, 5],
        };

        let bytes = log_entry.to_bytes();

       
        assert_eq!(bytes.len(), 21 + log_entry.data.len()); // 21 for index, term, type, and data length. Plus the actual data.

    
        assert_eq!(bytes[0..8], log_entry.index.to_be_bytes());
        assert_eq!(bytes[8..16], log_entry.term.to_be_bytes());
        assert_eq!(bytes[16], 0u8);
    }

    #[test]
    fn test_from_bytes() {
        let expected_log_entry = LogEntry {
            index: 12345,
            term: 67890,
            log_entry_type: LogEntryType::LogCommand,
            data: vec![1, 2, 3, 4, 5],
        };

        let bytes = expected_log_entry.to_bytes();

        let log_entry = LogEntry::from_bytes(&bytes).expect("Failed to deserialize");

        assert_eq!(expected_log_entry, log_entry);
    }
}

