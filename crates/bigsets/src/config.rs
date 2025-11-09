use crate::types::ActorId;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub cluster: ClusterConfig,
    pub replication: ReplicationConfig,
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub node_id: u16,
    #[serde(default)]
    pub epoch: u8,
    pub api_addr: String,
    pub replication_addr: String,
    pub db_path: PathBuf,
}

impl ServerConfig {
    /// Get the ActorId for this server
    pub fn actor_id(&self) -> ActorId {
        ActorId::new(self.node_id, self.epoch)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub replicas: Vec<ReplicaInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct ReplicaInfo {
    pub node_id: u16,
    #[serde(default)]
    pub epoch: u8,
    pub addr: String,
}

impl ReplicaInfo {
    /// Get the ActorId for this replica
    pub fn actor_id(&self) -> ActorId {
        ActorId::new(self.node_id, self.epoch)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
    pub buffer_size: usize,
    pub ack_timeout_ms: u64,
    pub rbilt_startup_delay_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub sqlite_cache_size: i32,
    pub sqlite_busy_timeout: i32,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, config::ConfigError> {
        let settings = config::Config::builder()
            .add_source(config::File::with_name(path))
            .build()?;

        settings.try_deserialize()
    }
}
