use crate::log::LogEntry;

pub trait FSM {
    fn apply(&self, log: &LogEntry) -> Box<dyn std::any::Any>;
}

