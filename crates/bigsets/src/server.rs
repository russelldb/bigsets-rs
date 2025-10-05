use crate::config::{Config, ReplicaInfo};
use crate::db::Database;
use crate::types::{Operation, VersionVector};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

/// Sender-side unacked buffer for retry logic
#[derive(Debug)]
pub struct UnackedBuffer {
    ops: HashMap<String, Vec<(Operation, Instant, u32)>>, // peer_id -> [(op, sent_at, retry_count)]
}

impl UnackedBuffer {
    fn new() -> Self {
        Self {
            ops: HashMap::new(),
        }
    }

    pub fn add(&mut self, peer_id: String, op: Operation) {
        self.ops
            .entry(peer_id)
            .or_insert_with(Vec::new)
            .push((op, Instant::now(), 0));
    }

    pub fn remove(&mut self, peer_id: &str, _op_id: usize) {
        if let Some(ops) = self.ops.get_mut(peer_id) {
            ops.retain(|(_, _, _)| true); // TODO: proper op_id tracking
        }
    }
}

/// Receiver-side pending buffer for out-of-order operations
#[derive(Debug)]
pub struct PendingBuffer {
    ops: Vec<Operation>,
    max_size: usize,
}

impl PendingBuffer {
    fn new(max_size: usize) -> Self {
        Self {
            ops: Vec::new(),
            max_size,
        }
    }

    pub fn add(&mut self, op: Operation) -> bool {
        if self.ops.len() >= self.max_size {
            warn!("Pending buffer overflow, triggering RBILT");
            return false; // Signal overflow
        }
        self.ops.push(op);
        true
    }

    pub fn is_full(&self) -> bool {
        self.ops.len() >= self.max_size
    }
}

/// Main server state
pub struct Server {
    pub config: Config,
    pub db: Arc<RwLock<Database>>,
    pub version_vector: Arc<RwLock<VersionVector>>,
    pub unacked_buffer: Arc<RwLock<UnackedBuffer>>,
    pub pending_buffer: Arc<RwLock<PendingBuffer>>,
}

impl Server {
    pub async fn new(config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        info!("Starting server with actor_id: {}", config.server.actor_id);

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
            if replica.id != self.config.server.actor_id {
                info!("  Would RBILT with peer: {} at {}", replica.id, replica.addr);
            }
        }

        Ok(())
    }

    pub fn get_peers(&self) -> Vec<ReplicaInfo> {
        self.config
            .cluster
            .replicas
            .iter()
            .filter(|r| r.id != self.config.server.actor_id)
            .cloned()
            .collect()
    }
}
