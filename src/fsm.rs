use crate::log::LogEntry;
use async_trait::async_trait;

#[async_trait]
pub trait FSM: Send + Sync  + 'static {
    async fn apply(&self, log: &LogEntry) -> Box<dyn std::any::Any>;
}