use crate::replication::ReplicationManager;
use crate::server::{CommandResult, Server};
use crate::storage::Storage;
use crate::types::VersionVector;
use bytes::Bytes;
use rusqlite::Result;
use std::sync::Arc;
use tracing::error;

/// Wrapper that coordinates Server and ReplicationManager
///
/// This is the glue layer that:
/// - Calls Server methods for business logic
/// - Sends operations to ReplicationManager for distribution
/// - Returns clean results to API layer
///
/// Write commands split the response: result goes to API, operation goes to replication.
/// Read commands pass through directly to Server.
pub struct ServerWrapper<S: Storage> {
    server: Arc<Server<S>>,
    replication: Arc<ReplicationManager>,
}

impl<S: Storage> ServerWrapper<S> {
    pub fn new(server: Arc<Server<S>>, replication: Arc<ReplicationManager>) -> Self {
        Self {
            server,
            replication,
        }
    }

    /// Add members to a set
    ///
    /// Calls server, spawns replication task, returns result
    pub async fn sadd(&self, set_name: &str, members: &[Bytes]) -> Result<CommandResult> {
        let (result, operation) = self.server.sadd(set_name, members).await?;

        // Send operation to replication (fire and forget)
        if let Some(op) = operation {
            tracing::info!(
                "SADD wrapper spawning replication task for set={}",
                set_name
            );
            let replication = Arc::clone(&self.replication);
            tokio::spawn(async move {
                tracing::info!("Replication task started, calling send()");
                if let Err(e) = replication.send(op).await {
                    error!("Failed to replicate SADD: {}", e);
                } else {
                    tracing::info!("Replication send() completed successfully");
                }
            });
        } else {
            tracing::warn!("SADD produced no operation to replicate");
        }

        Ok(result)
    }

    /// Remove members from a set
    pub async fn srem(&self, set_name: &str, members: &[Bytes]) -> Result<CommandResult> {
        let (result, operation) = self.server.srem(set_name, members).await?;

        // Send operation to replication (fire and forget)
        if let Some(op) = operation {
            let replication = Arc::clone(&self.replication);
            tokio::spawn(async move {
                if let Err(e) = replication.send(op).await {
                    error!("Failed to replicate SREM: {}", e);
                }
            });
        }

        Ok(result)
    }

    /// Get cardinality of a set (read-only, pass through)
    pub async fn scard(
        &self,
        set_name: &str,
        client_vv: Option<&VersionVector>,
    ) -> Result<CommandResult> {
        self.server.scard(set_name, client_vv).await
    }

    /// Get all members of a set (read-only, pass through)
    pub async fn smembers(
        &self,
        set_name: &str,
        client_vv: Option<&VersionVector>,
    ) -> Result<CommandResult> {
        self.server.smembers(set_name, client_vv).await
    }

    /// Check if element is member (read-only, pass through)
    pub async fn sismember(
        &self,
        set_name: &str,
        member: &Bytes,
        client_vv: Option<&VersionVector>,
    ) -> Result<CommandResult> {
        self.server.sismember(set_name, member, client_vv).await
    }

    /// Check membership for multiple elements (read-only, pass through)
    pub async fn smismember(
        &self,
        set_name: &str,
        members: &[Bytes],
        client_vv: Option<&VersionVector>,
    ) -> Result<CommandResult> {
        self.server.smismember(set_name, members, client_vv).await
    }
}
