use crate::storage::Storage;
use crate::types::{ActorId, OpType, Operation, VersionVector};
use bytes::Bytes;
use rusqlite::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, trace};

/// Result type for command execution
#[derive(Debug, Clone, PartialEq)]
pub enum CommandResult {
    /// OK with optional version vector
    Ok { vv: Option<VersionVector> },
    /// Integer result
    Integer(i64),
    /// Boolean array for multi-membership
    BoolArray(Vec<bool>),
    /// Array of bytes (for SMEMBERS)
    BytesArray(Vec<Bytes>),
    /// Error message
    Error(String),
    /// Not ready to serve read (with current VV)
    NotReady(VersionVector),
}

/// Core server containing business logic for CRDT operations
///
/// This is the heart of the system - manages version vectors, causality,
/// and coordinates with storage. Generic over Storage to allow testing
/// with different backends.
pub struct Server<S: Storage> {
    actor_id: ActorId,
    storage: Arc<S>,
    version_vector: Arc<RwLock<VersionVector>>,
}

impl<S: Storage> Server<S> {
    /// Create a new server with the given storage
    ///
    /// Loads the version vector from storage on startup
    pub async fn new(actor_id: ActorId, storage: Arc<S>) -> Result<Self> {
        // Load VV from storage
        let vv = storage.load_vv()?;

        Ok(Self {
            actor_id,
            storage,
            version_vector: Arc::new(RwLock::new(vv)),
        })
    }

    /// Add members to a set
    ///
    /// Returns both the command result and an optional operation for replication.
    /// The operation contains the context (VV before increment) for causality tracking.
    pub async fn sadd(
        &self,
        set_name: &str,
        members: &[Bytes],
    ) -> Result<(CommandResult, Option<Operation>)> {
        if members.is_empty() {
            return Ok((
                CommandResult::Error(
                    "ERR wrong number of arguments for 'sadd' command".to_string(),
                ),
                None,
            ));
        }

        let context = self.version_vector.read().await.clone();

        let mut vv = self.version_vector.write().await;
        let dot = vv.increment(self.actor_id);
        trace!("calling storage for SADD");
        let rem_dots = self.storage.add_elements(set_name, members, dot)?;

        let operation = Operation {
            set_name: set_name.to_string(),
            op_type: OpType::Add {
                elements: members.to_vec(),
                dot,
                removed_dots: rem_dots,
            },
            context,
        };

        debug!(
            "SADD {} added {} members with dot {:?}",
            set_name,
            members.len(),
            dot
        );

        Ok((
            CommandResult::Ok {
                vv: Some(vv.clone()),
            },
            Some(operation),
        ))
    }

    /// Remove members from a set
    ///
    /// Returns both the command result and an optional operation for replication.
    pub async fn srem(
        &self,
        set_name: &str,
        members: &[Bytes],
    ) -> Result<(CommandResult, Option<Operation>)> {
        if members.is_empty() {
            return Ok((
                CommandResult::Error(
                    "ERR wrong number of arguments for 'srem' command".to_string(),
                ),
                None,
            ));
        }

        let context = self.version_vector.read().await.clone();

        let mut vv = self.version_vector.write().await;
        let dot = vv.increment(self.actor_id);

        let rem_dots = self.storage.remove_elements(set_name, members, dot)?;

        // 4. Create operation for replication
        let operation = if !rem_dots.is_empty() {
            let operation = Operation {
                set_name: set_name.to_string(),
                op_type: OpType::Remove {
                    elements: members.to_vec(),
                    dot,
                    removed_dots: rem_dots,
                },
                context,
            };
            Some(operation)
        } else {
            // no-op
            None
        };

        debug!(
            "SREM {} removed {} members with dot {:?}",
            set_name,
            members.len(),
            dot
        );

        // 5. Return both result and operation
        Ok((
            CommandResult::Ok {
                vv: Some(vv.clone()),
            },
            operation,
        ))
    }

    /// Get cardinality of a set
    ///
    /// Checks causality if client provides a version vector.
    pub async fn scard(
        &self,
        set_name: &str,
        client_vv: Option<&VersionVector>,
    ) -> Result<CommandResult> {
        // Check causality
        let local_vv = self.version_vector.read().await;
        if let Some(cv) = client_vv {
            if !local_vv.descends(cv) {
                return Ok(CommandResult::NotReady(local_vv.clone()));
            }
        }

        let count = self.storage.count_elements(set_name)?;
        Ok(CommandResult::Integer(count))
    }

    /// Get all members of a set
    pub async fn smembers(
        &self,
        set_name: &str,
        client_vv: Option<&VersionVector>,
    ) -> Result<CommandResult> {
        // Check causality
        let local_vv = self.version_vector.read().await;
        if let Some(cv) = client_vv {
            if !local_vv.descends(cv) {
                return Ok(CommandResult::NotReady(local_vv.clone()));
            }
        }

        let members = self.storage.get_elements(set_name)?;
        Ok(CommandResult::BytesArray(members))
    }

    /// Check if element is a member of set
    pub async fn sismember(
        &self,
        set_name: &str,
        member: &Bytes,
        client_vv: Option<&VersionVector>,
    ) -> Result<CommandResult> {
        // Check causality
        let local_vv = self.version_vector.read().await;
        if let Some(cv) = client_vv {
            if !local_vv.descends(cv) {
                return Ok(CommandResult::NotReady(local_vv.clone()));
            }
        }

        let is_member = self.storage.is_member(set_name, member)?;
        Ok(CommandResult::Integer(if is_member { 1 } else { 0 }))
    }

    /// Check membership for multiple elements
    pub async fn smismember(
        &self,
        set_name: &str,
        members: &[Bytes],
        client_vv: Option<&VersionVector>,
    ) -> Result<CommandResult> {
        if members.is_empty() {
            return Ok(CommandResult::Error(
                "ERR wrong number of arguments for 'smismember' command".to_string(),
            ));
        }

        // Check causality
        let local_vv = self.version_vector.read().await;
        if let Some(cv) = client_vv {
            if !local_vv.descends(cv) {
                return Ok(CommandResult::NotReady(local_vv.clone()));
            }
        }

        let membership = self.storage.are_members(set_name, members)?;
        Ok(CommandResult::BoolArray(membership))
    }

    /// Apply a remote operation (called by ReplicationServer)
    ///
    /// Checks causality and applies the operation atomically.
    /// Returns Ok(true) if applied, Ok(false) if causality not satisfied (needs buffering),
    /// or Err if there's a storage error.
    pub async fn apply_remote_operation(&self, operation: Operation) -> Result<bool> {
        let mut vv = self.version_vector.write().await;

        if !vv.descends(&operation.context) {
            return Ok(false); // Causality not satisfied, needs buffering
        }

        let dot = match &operation.op_type {
            OpType::Add { dot, .. } | OpType::Remove { dot, .. } => *dot,
        };

        vv.update(dot.actor_id, dot.counter);

        match &operation.op_type {
            OpType::Add {
                elements,
                removed_dots,
                ..
            } => {
                self.storage.remote_add_elements(
                    &operation.set_name,
                    elements,
                    removed_dots,
                    dot,
                )?;
            }
            OpType::Remove {
                elements,
                removed_dots,
                ..
            } => {
                self.storage.remote_remove_elements(
                    &operation.set_name,
                    elements,
                    removed_dots,
                    dot,
                )?;
            }
        }

        debug!(
            "Applied remote operation for {} with dot {:?}",
            operation.set_name, dot
        );

        Ok(true)
    }

    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }

    pub fn version_vector(&self) -> Arc<RwLock<VersionVector>> {
        Arc::clone(&self.version_vector)
    }
}
