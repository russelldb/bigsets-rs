use bigsets::{Config, Server};
use std::sync::Arc;
use tracing::info;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    info!("BigSets Server starting...");

    // Load configuration
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());

    info!("Loading config from: {}", config_path);
    let config = Config::from_file(&config_path)?;

    // Create and start server
    let server = Arc::new(Server::new(config).await?);
    server.start().await?;

    Ok(())
}
