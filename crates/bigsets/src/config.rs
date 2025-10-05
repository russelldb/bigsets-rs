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
    pub actor_id: String,
    pub api_addr: String,
    pub replication_addr: String,
    pub db_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub replicas: Vec<ReplicaInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub id: String,
    pub addr: String,
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
