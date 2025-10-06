fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::Config::new()
        .bytes(["."]) // Use bytes::Bytes for bytes fields
        .compile_protos(&["proto/replication.proto"], &["proto/"])?;
    Ok(())
}
