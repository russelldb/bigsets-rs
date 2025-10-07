use bigsets::{Config, Server};
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber;

#[derive(Parser, Debug)]
#[command(author, version, about = "BigSets Server", long_about = None)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: String,

    /// Data directory (overrides db_path in config)
    #[arg(short, long)]
    data_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    info!("BigSets Server starting...");

    // Parse command line arguments
    let args = Args::parse();

    // Load configuration
    info!("Loading config from: {}", args.config);
    let mut config = Config::from_file(&args.config)?;

    // Override db_path if --data-dir is provided
    if let Some(data_dir) = args.data_dir {
        let db_path = data_dir.join(format!("node-{}.db", config.server.node_id));
        info!("Overriding db_path with: {:?}", db_path);
        config.server.db_path = db_path;
    }

    // Create and start server
    let server = Arc::new(Server::new(config).await?);
    server.start().await?;

    Ok(())
}
