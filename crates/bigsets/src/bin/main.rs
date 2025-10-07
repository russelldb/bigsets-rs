use bigsets::{
    ApiServer, Config, ReplicationManager, ReplicationServer, Server, ServerWrapper, SqliteStorage,
};
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Load configuration
    let config = Config::from_file("config.toml")?;
    info!("Starting BigSets server");
    info!("Actor ID: {}", config.server.actor_id());

    // Ensure data directory exists
    if let Some(parent) = config.server.db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // 1. Create storage layer
    info!("Opening database at: {:?}", config.server.db_path);
    let storage = Arc::new(SqliteStorage::open(
        &config.server.db_path,
        &config.storage,
    )?);

    // 2. Create core server (business logic)
    let server = Arc::new(Server::new(config.server.actor_id(), Arc::clone(&storage)).await?);
    info!("Core server initialized");

    // 3. Create replication manager (networking, buffers)
    let replication = Arc::new(ReplicationManager::new(
        config
            .cluster
            .replicas
            .iter()
            .filter(|r| r.actor_id() != config.server.actor_id())
            .cloned()
            .collect(),
        config.replication.buffer_size,
    ));
    info!("Replication manager initialized");

    // 4. Create wrapper (coordinates server + replication)
    let wrapper = Arc::new(ServerWrapper::new(
        Arc::clone(&server),
        Arc::clone(&replication),
    ));
    info!("Server wrapper initialized");

    // 5. Start API server (RESP/TCP)
    let api_server = ApiServer::new(Arc::clone(&wrapper), config.server.api_addr.clone());
    let api_handle = tokio::spawn(async move {
        if let Err(e) = api_server.run().await {
            tracing::error!("API server error: {}", e);
        }
    });
    info!("API server started on {}", config.server.api_addr);

    // 6. Start replication server (protobuf/TCP)
    let replication_server = ReplicationServer::new(
        Arc::clone(&server),
        Arc::clone(&replication),
        config.server.replication_addr.clone(),
    );
    let repl_handle = tokio::spawn(async move {
        if let Err(e) = replication_server.run().await {
            tracing::error!("Replication server error: {}", e);
        }
    });
    info!(
        "Replication server started on {}",
        config.server.replication_addr
    );

    info!("BigSets server fully initialized and running");

    // Wait for both servers
    tokio::try_join!(api_handle, repl_handle)?;

    Ok(())
}
