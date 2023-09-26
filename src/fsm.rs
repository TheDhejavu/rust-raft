use crate::log::LogEntry;

pub trait FSM: Send {
    fn apply(&self, log: &LogEntry) -> Box<dyn std::any::Any>;
}

    