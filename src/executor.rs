
use std::sync::Arc;

use crate::{configuration::{Configuration, ConfigStore}, log::{LogEntry, LogEntryType}, error, fsm::FSM};
use log::info;
use tokio::sync::Mutex;


/// `FSMExecutor` is responsible for managing the execution of a Finite State Machine (FSM).
pub struct FSMExecutor {
    /// Receiver for applying logs to the FSM.
    fsm_apply_rx: tokio::sync::mpsc::UnboundedReceiver<Vec<LogEntry>>,

   /// State machine that the Raft algorithm operates on.
    fsm: Box<dyn FSM>,

    /// Shared configuration store protected by a mutex.
    config_store: Arc<Mutex<ConfigStore>>,

   // Receiver for acknowledging the server background task has stopped.
    shutdown_rx: tokio::sync::mpsc::Receiver<()>,
}

impl FSMExecutor {
    // Creates a new `FSMExecutor`.
    /// 
    /// This function initializes the FSMExecutor with a receiver for log entries,
    /// a finite state machine, and a configuration store.
    /// 
    /// # Arguments
    /// - `fsm_apply_rx`: The receiver for incoming batches of `LogEntry`.
    /// - `fsm`: The finite state machine to be executed.
    /// - `config_store`: The configuration store.
    /// 
    /// # Returns
    /// Returns an instance of `FSMExecutor`.
    pub async fn new(
        fsm_apply_rx: tokio::sync::mpsc::UnboundedReceiver<Vec<LogEntry>>,
        fsm: Box<dyn FSM>,
        config_store:Arc<Mutex<ConfigStore>>,
        shutdown_rx: tokio::sync::mpsc::Receiver<()>,
    )-> Self {
        Self{ fsm_apply_rx, fsm, config_store, shutdown_rx}
    }
    /// The main loop for the FSMExecutor.
    /// 
    /// This asynchronous function continuously receives and processes batches of `LogEntry` and
    /// it applies each log entry to the FSM and also handles configuration changes.
    pub async fn fsm_loop(&mut self) {
        info!("[FSM] fsm.loop.run");
        loop { 
            tokio::select! {
                result = self.fsm_apply_rx.recv() => {
                    match result {
                        Some(batched_logs)  => {
                            info!("fsm.recv: {:?}", batched_logs.len());

                            let mut last_applied_index = 0;
                            for log in batched_logs {
                                match log.log_entry_type {
                                LogEntryType::LogCommand => {
                                        info!("[FSM] log.apply: {}", log.index);

                                        // What should we do here when apply fails? 
                                        // how do we maintain consistency and ensure linearizability? 
                                        self.fsm.apply(&log).await;
                                        last_applied_index = log.index;
                                    },
                                    LogEntryType::LogConfCommand => {
                                        let deseralize_config_result: Result<Configuration, Box<bincode::ErrorKind>> = Configuration::deserialize(&log.data);
                                        match deseralize_config_result {
                                            Ok(configuration) => {
                                                info!("[FSM] configurations.persist {:?}", configuration);
                                                _ = self.config_store.lock().await.persist(configuration, log.index);
                                                last_applied_index = log.index;
                                            },
                                            Err(e)=> {
                                                error!("[FSM] unable to process config entry: {:?}", e)
                                            },
                                        }  
                                    },
                                    _ => {},
                                }
                            }
                            if last_applied_index > 0 {
                                info!("Logs applied up to index {}", last_applied_index);
                            }
                        },
                        None => (),
                    }
                }
                _ = self.shutdown_rx.recv() => { 
                    info!("[FSM] fsm.loop.stop");
                    return;
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use async_trait::async_trait;
    use super::*;

    // Mock FSM for testing purposes
    struct MockFSM {
        db: Arc<tokio::sync::Mutex<Vec<LogEntry>>>,
    }

    #[async_trait]
    impl FSM for MockFSM {
        async fn apply(&mut self, log: &LogEntry) -> Box<dyn std::any::Any> {
            self.db.lock().await.push(log.clone());
            Box::new(())
        }
    }

    #[tokio::test]
    async fn test_fsm_executor() {
        // Create a mock configuration store
        let config_store = Arc::new(tokio::sync::Mutex::new(ConfigStore::new("./".to_string())));
        let db = Arc::new(tokio::sync::Mutex::new(vec![]));

        // Create a mock FSM
        let fsm = Box::new(MockFSM{ db: db.clone()  });

        // Create a channel for log entry batches
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // Create a channel for shutdown signal
        let (fsm_shutdown_tx, fsm_shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

        // Create a new FSMExecutor
        let mut fsm_executor = FSMExecutor::new(rx, fsm, Arc::clone(&config_store), fsm_shutdown_rx).await;

        // Spawn the FSMExecutor in a separate task
        let fsm_executor_handle = tokio::spawn(async move {
            fsm_executor.fsm_loop().await;
        });

        // Send a batch of log entries to the FSMExecutor
        let log_entries = vec![
            LogEntry {
                index: 1,
                term: 1,
                log_entry_type: LogEntryType::LogCommand,
                data: vec![],
            },
            LogEntry {
                index: 2,
                term: 1,
                log_entry_type: LogEntryType::LogCommand,
                data: vec![],
            },
        ];
        tx.send(log_entries.clone()).unwrap();

        // Wait for a short duration to allow the FSMExecutor to process the log entries
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send a shutdown signal to stop the FSMExecutor
        _ = fsm_shutdown_tx.send(()).await;

        // Wait for the FSMExecutor task to finish
        fsm_executor_handle.await.unwrap();
       
        // Assert that the applied logs match the sent log entries
        assert_eq!(*db.lock().await.clone(), log_entries);
    }
}
