use std::sync::RwLock;

pub fn format_endpoint_addr(addr: &str) -> String {
    if addr.starts_with("http://") {
        return addr.to_string();
    }
    return format!("http://{}", addr);
}

pub fn format_server_addr(addr: &str) -> String {
    if addr.starts_with("http://") {
        return addr["http://".len()..].to_string();
    } 
    return addr.to_string();
}

pub struct ThreadSafeBool {
    value: RwLock<bool>,
}

impl ThreadSafeBool {
    pub fn new(initial_value: bool) -> Self {
        ThreadSafeBool {
            value: RwLock::new(initial_value),
        }
    }

    pub fn get(&self) -> bool {
        *self.value.read().unwrap()
    }

    pub fn set(&self, value: bool) {
        *self.value.write().unwrap() = value;
    }
}