// Configuration for a Raft node.
// 
// This configuration is used to set up the behavior of a Raft node in terms of
// election timeout, heartbeat intervals, maximum log entries, and payload sizes e.t.c.
// 
use crate::error::ConfigError;

// The default value for the election timeot in illiseconds.
const DEFAULT_ELECTION_TIMEOUT: u64 = 100;
// The minimum permissible value for the election timeout in milliseconds.
pub const DEFAULT_ELECTION_TIMEOUT_MIN: u64 = 100;
// The maximum permissible value for the election timeout in milliseconds.
pub const DEFAULT_ELECTION_TIMEOUT_MAX: u64 = 150;
// The default interval at which heartbeats are sent, in milliseconds.
pub const DEFAULT_HEARTBEAT_INTERVAL: u64 = 100;
// The default maximum number of entries that can be included in a single replication payload.
pub const DEFAULT_MAX_PAYLOAD_ENTRIES: u64 = 30;
// The default maximum number of log entries that the Raft node can store.
pub const DEFAULT_MAX_LOG_ENTRIES: u64 = 100;

// Represents the configuration for a Raft node.
pub struct Config {
    pub server_id: String,
    // The election timeout for this Raft node, in milliseconds.
    pub election_timeout: u64,
    // The minimum allowed election timeout, in milliseconds.
    pub election_timeout_min: u64,
    // The maximum allowed election timeout, in milliseconds.
    pub election_timeout_max: u64,
    // The interval at which the leader sends heartbeat messages, in milliseconds.
    pub heartbeat_interval: u64,
    // The maximum number of log entries the node should store.
    pub max_log_entries: u64,
    // The maximum number of entries a node will include in a single replication payload.
    pub max_payload_size: u64,
}

impl Config {
    pub fn build() -> ConfigBuilder {
        ConfigBuilder {
            server_id: None,
            election_timeout_max: None,
            election_timeout_min: None,
            election_timeout: None,
            heartbeat_interval: None,
            max_log_entries: None,
            max_payload_size: None,
        }
    }
}

// Builder for RaftConfig
pub struct ConfigBuilder {
    server_id: Option<String>,
    election_timeout: Option<u64>,
    heartbeat_interval: Option<u64>,
    max_log_entries: Option<u64>,
    max_payload_size: Option<u64>,
    election_timeout_min: Option<u64>,
    election_timeout_max: Option<u64>,
}

impl ConfigBuilder {
    // Sets server id for config
    pub fn server_id(mut self, value: String) -> Self {
        self.server_id = Some(value);
        self
    }
    // Sets the election timeout for the config.
    pub fn election_timeout(mut self, value: u64) -> Self {
        self.election_timeout = Some(value);
        self
    }
    // Sets the election timeout minimum for the config.
    pub fn election_timeout_min(mut self, value: u64) -> Self {
        self.election_timeout_min = Some(value);
        self
    }
    // Sets the election timeout maximum for the config.
    pub fn election_timeout_max(mut self, value: u64) -> Self {
        self.election_timeout_max = Some(value);
        self
    }

    // Sets the heartbeat interval for the config.
    pub fn heartbeat_interval(mut self, value: u64) -> Self {
        self.heartbeat_interval = Some(value);
        self
    }

    // Sets the maximum number of log entries for the config.
    pub fn max_log_entries(mut self, value: u64) -> Self {
        self.max_log_entries = Some(value);
        self
    }

    // Sets the maximum payload size for the config.
    pub fn max_payload_size(mut self, value: u64) -> Self {
        self.max_payload_size = Some(value);
        self
    }

    pub fn validate(self) -> Result<Config, ConfigError> {
        if self.server_id.is_none() {
            return Err(ConfigError::InvalidServerID);
        }        

        let server_id = self.server_id.unwrap();
        let election_timeout = self.election_timeout.unwrap_or(DEFAULT_ELECTION_TIMEOUT);
        let election_timeout_max = self.election_timeout.unwrap_or(DEFAULT_ELECTION_TIMEOUT_MAX);
        let election_timeout_min = self.election_timeout.unwrap_or(DEFAULT_ELECTION_TIMEOUT_MIN);
        let heartbeat_interval = self.heartbeat_interval.unwrap_or(DEFAULT_HEARTBEAT_INTERVAL);
        let max_log_entries = self.max_log_entries.unwrap_or(DEFAULT_MAX_LOG_ENTRIES);
        let max_payload_size = self.max_payload_size.unwrap_or(DEFAULT_MAX_PAYLOAD_ENTRIES);


        if election_timeout < DEFAULT_ELECTION_TIMEOUT_MIN || election_timeout > DEFAULT_ELECTION_TIMEOUT_MAX {
            return Err(ConfigError::InvalidElectionTimeout);
        }

        if election_timeout_max > DEFAULT_ELECTION_TIMEOUT_MAX {
            return Err(ConfigError::InvalidElectionTimeout);
        }

        if election_timeout_min < DEFAULT_ELECTION_TIMEOUT_MIN {
            return Err(ConfigError::InvalidElectionTimeout);
        }
        
        if heartbeat_interval > DEFAULT_ELECTION_TIMEOUT_MAX {
            return Err(ConfigError::InvalidHeartbeatInterval);
        }
        
        if max_log_entries == 0 {
            return Err(ConfigError::InvalidMaxLogEntries);
        }
        
        if max_payload_size == 0 {
            return Err(ConfigError::InvalidMaxPayloadSize);
        }

        let config = Config {
            server_id,
            election_timeout,
            heartbeat_interval,
            max_log_entries,
            election_timeout_max,
            election_timeout_min,
            max_payload_size,
        };

        Ok(config)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = Config::build().
                                server_id("peer01".to_string()).
                                validate().
                                unwrap();

        assert!(cfg.election_timeout >= DEFAULT_ELECTION_TIMEOUT_MIN);
        assert!(cfg.election_timeout <= DEFAULT_ELECTION_TIMEOUT_MAX);
        assert!(cfg.election_timeout_min >= DEFAULT_ELECTION_TIMEOUT_MIN);
        assert!(cfg.election_timeout_max <= DEFAULT_ELECTION_TIMEOUT_MAX);
        assert!(cfg.max_log_entries == DEFAULT_MAX_LOG_ENTRIES);
        assert!(cfg.max_payload_size == DEFAULT_MAX_PAYLOAD_ENTRIES);
        assert!(cfg.heartbeat_interval == DEFAULT_HEARTBEAT_INTERVAL);
    }
}
