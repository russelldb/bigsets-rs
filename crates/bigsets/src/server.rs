use crate::storage::Storage;
use crate::types::{ActorId, OpType, Operation, VersionVector};
use bytes::Bytes;
use rusqlite::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

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

        // 1. Capture context BEFORE incrementing (what we had seen)
        let context = self.version_vector.read().await.clone();

        // 2. Increment VV (in-memory)
        let mut vv = self.version_vector.write().await;
        let dot = vv.increment(self.actor_id);

        // 3. Write to storage (passing VV and dot)
        self.storage
            .add_elements(set_name, members, dot, &[], &vv)?;

        // 4. Create operation for replication
        let operation = Operation {
            set_name: set_name.to_string(),
            op_type: OpType::Add {
                elements: members.to_vec(),
                dot,
                removed_dots: vec![], // TODO: Track concurrent removes
            },
            context, // Context BEFORE increment
        };

        debug!(
            "SADD {} added {} members with dot {:?}",
            set_name,
            members.len(),
            dot
        );

        // 5. Return both result and operation
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

        // 1. Capture context BEFORE incrementing
        let context = self.version_vector.read().await.clone();

        // 2. Increment VV (in-memory)
        let mut vv = self.version_vector.write().await;
        let dot = vv.increment(self.actor_id);

        // 3. Write to storage (passing VV and dot)

        self.storage.remove_elements(set_name, members, &[], &vv)?;

        // 4. Create operation for replication
        let operation = Operation {
            set_name: set_name.to_string(),
            op_type: OpType::Remove {
                elements: members.to_vec(),
                dot,
                removed_dots: vec![], // TODO: Track which dots were on these elements
            },
            context, // Context BEFORE increment
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
            Some(operation),
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
    /// Returns error if causality is not satisfied (caller should buffer).
    pub async fn apply_remote_operation(&self, operation: Operation) -> Result<()> {
        // 1. Acquire VV write lock (serializes all writes)
        let mut vv = self.version_vector.write().await;

        // 2. Check causality: have we seen all events the sender had seen?
        if !vv.descends(&operation.context) {
            return Err(rusqlite::Error::InvalidQuery); // TODO: Better error type
        }

        // 3. Extract dot from operation
        let dot = match &operation.op_type {
            OpType::Add { dot, .. } | OpType::Remove { dot, .. } => *dot,
        };

        // 4. Update VV
        vv.update(dot.actor_id, dot.counter);

        // 5. Apply to storage (passing updated VV)

        match &operation.op_type {
            OpType::Add {
                elements,
                removed_dots,
                ..
            } => {
                self.storage
                    .add_elements(&operation.set_name, elements, dot, removed_dots, &vv)?;
            }
            OpType::Remove {
                elements,
                removed_dots,
                ..
            } => {
                self.storage
                    .remove_elements(&operation.set_name, elements, removed_dots, &vv)?;
            }
        }

        debug!(
            "Applied remote operation for {} with dot {:?}",
            operation.set_name, dot
        );

        Ok(())
    }

    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }

    pub fn version_vector(&self) -> Arc<RwLock<VersionVector>> {
        Arc::clone(&self.version_vector)
    }
}
