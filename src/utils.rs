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
