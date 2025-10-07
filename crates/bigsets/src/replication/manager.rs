use crate::buffers::{PendingBuffer, UnackedBuffer};
use crate::config::ReplicaInfo;
use crate::types::Operation;
use prost::Message;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Manages replication: sending operations to peers, buffering, connection management
///
/// This handles all the networking and reliability concerns for replication,
/// keeping the core Server focused on business logic.
pub struct ReplicationManager {
    peers: Vec<ReplicaInfo>,
    pending_buffer: Arc<RwLock<PendingBuffer>>,
    unacked_buffer: Arc<RwLock<UnackedBuffer>>,
}

impl ReplicationManager {
    pub fn new(peers: Vec<ReplicaInfo>, buffer_size: usize) -> Self {
        Self {
            peers,
            pending_buffer: Arc::new(RwLock::new(PendingBuffer::new(buffer_size))),
            unacked_buffer: Arc::new(RwLock::new(UnackedBuffer::new())),
        }
    }

    /// Send operation to all peers
    ///
    /// Attempts to send to each peer. On failure, buffers in unacked_buffer
    /// for retry. This is fire-and-forget from the caller's perspective.
    pub async fn send(
        &self,
        operation: Operation,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for peer in &self.peers {
            if let Err(e) = self.send_to_peer(&peer.addr, &operation).await {
                warn!("Failed to send operation to peer {}: {}", peer.addr, e);
                // Buffer for retry
                self.unacked_buffer
                    .write()
                    .await
                    .add(peer.addr.clone(), operation.clone());
            } else {
                debug!("Sent operation to peer {}", peer.addr);
            }
        }
        Ok(())
    }

    /// Send a single operation to a peer
    ///
    /// Opens a new connection, sends the operation, and closes.
    /// TODO: Connection pooling/reuse for better performance
    async fn send_to_peer(
        &self,
        addr: &str,
        operation: &Operation,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Convert to protobuf
        let proto_op = crate::proto::operation_to_proto(operation);
        let mut buf = Vec::new();
        proto_op.encode(&mut buf)?;

        // Connect and send (length-prefixed)
        let mut stream = TcpStream::connect(addr).await?;

        // Write length prefix (4 bytes big-endian)
        stream.write_u32(buf.len() as u32).await?;

        // Write message body
        stream.write_all(&buf).await?;
        stream.flush().await?;

        Ok(())
    }

    pub fn pending_buffer(&self) -> Arc<RwLock<PendingBuffer>> {
        Arc::clone(&self.pending_buffer)
    }

    pub fn unacked_buffer(&self) -> Arc<RwLock<UnackedBuffer>> {
        Arc::clone(&self.unacked_buffer)
    }
}
