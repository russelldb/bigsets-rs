use crate::buffers::{PendingBuffer, UnackedBuffer};
use crate::config::{Config, ReplicaInfo};
use crate::db::Database;
use crate::types::VersionVector;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};

/// Main server state
pub struct Server {
    pub config: Config,
    pub db: Arc<RwLock<Database>>,
    // Does this work? A single VV across all the sets? Doesn't it lead to gaps in a set?
    // Do gaps matter? I guess if every replicas is a total replica, then no,
    // except we don't know which dots are for which sets,
    // unless that information is included in ops and anti-entropy
    pub version_vector: Arc<RwLock<VersionVector>>,
    pub unacked_buffer: Arc<RwLock<UnackedBuffer>>,
    pub pending_buffer: Arc<RwLock<PendingBuffer>>,
}

impl Server {
    pub async fn new(config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        info!(
            "Starting server with actor_id: {}",
            config.server.actor_id()
        );

        // Ensure data directory exists
        if let Some(parent) = config.server.db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Open database
        info!("Opening database at: {:?}", config.server.db_path);
        let db = Database::open(&config.server.db_path, &config.storage)?;

        // Load version vector from database (or initialize empty)
        let version_vector = VersionVector::new(); // TODO: load from DB

        // Initialize buffers
        let unacked_buffer = UnackedBuffer::new();
        let pending_buffer = PendingBuffer::new(config.replication.buffer_size);

        Ok(Self {
            config,
            db: Arc::new(RwLock::new(db)),
            version_vector: Arc::new(RwLock::new(version_vector)),
            unacked_buffer: Arc::new(RwLock::new(unacked_buffer)),
            pending_buffer: Arc::new(RwLock::new(pending_buffer)),
        })
    }

    pub async fn start(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        info!("Server starting...");

        // Stub: RBILT on startup
        self.rbilt_startup().await?;

        // Start API server
        let api_server = crate::api::ApiServer::new(Arc::clone(&self));
        let api_handle = tokio::spawn(async move {
            if let Err(e) = api_server.run().await {
                error!("API server error: {}", e);
            }
        });

        // TODO: Start replication server
        info!(
            "Replication server would start on: {}",
            self.config.server.replication_addr
        );

        // Wait for API server
        api_handle.await?;

        Ok(())
    }

    async fn rbilt_startup(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!(
            "Waiting {}ms before startup RBILT...",
            self.config.replication.rbilt_startup_delay_ms
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(
            self.config.replication.rbilt_startup_delay_ms,
        ))
        .await;

        info!("Starting RBILT with peers (STUBBED)");
        for replica in &self.config.cluster.replicas {
            if replica.actor_id() != self.config.server.actor_id() {
                info!(
                    "  Would RBILT with peer: {} at {}",
                    replica.actor_id(),
                    replica.addr
                );
            }
        }

        Ok(())
    }

    pub fn get_peers(&self) -> Vec<ReplicaInfo> {
        self.config
            .cluster
            .replicas
            .iter()
            .filter(|r| r.actor_id() != self.config.server.actor_id())
            .cloned()
            .collect()
    }
}
