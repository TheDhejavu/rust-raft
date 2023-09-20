pub fn format_endpoint_addr(addr: &str) -> String {
    if addr.starts_with("http://") {
        addr.to_string()
    } else {
        format!("http://{}", addr)
    }
}

pub fn format_server_addr(addr: &str) -> String {
    if addr.starts_with("http://") {
        addr["http://".len()..].to_string()
    } else {
        addr.to_string()
    }
}
