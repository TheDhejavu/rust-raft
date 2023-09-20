fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting protobuf compilation...");
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(
            &["src/proto/raft.proto"], 
            &["src/proto/"]
        )?;
    println!("Protobuf compilation completed.");
    Ok(())
}