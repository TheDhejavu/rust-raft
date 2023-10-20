use crate::log::LogEntry;

pub trait FSM: Send + 'static {
    fn apply(&self, log: &LogEntry) -> Box<dyn std::any::Any>;
}

