use crate::replication::ReplicationManager;
use crate::server::Server;
use crate::storage::Storage;
use prost::Message;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

/// TCP server that receives operations from peers
///
/// Listens for incoming operations, applies them via Server,
/// and manages causality buffering via ReplicationManager.
pub struct ReplicationServer<S: Storage> {
    server: Arc<Server<S>>,
    replication: Arc<ReplicationManager>,
    addr: String,
}

impl<S: Storage + 'static> ReplicationServer<S> {
    pub fn new(server: Arc<Server<S>>, replication: Arc<ReplicationManager>, addr: String) -> Self {
        Self {
            server,
            replication,
            addr,
        }
    }

    /// Try to apply buffered operations
    ///
    /// Iterates through the pending buffer and attempts to apply each operation.
    /// Removes successfully applied operations. Keeps looping until a full pass
    /// applies nothing (reaching a fixed point).
    ///
    /// Returns the total number of operations applied.
    async fn try_apply_buffered(
        server: Arc<Server<S>>,
        replication: Arc<ReplicationManager>,
    ) -> usize {
        let mut total_applied = 0;

        loop {
            let mut applied_this_pass = 0;

            let pending_buffer = replication.pending_buffer();
            let buffer_len = pending_buffer.read().await.len();

            let mut i = 0;
            while i < buffer_len {
                // Clone the operation to avoid holding the buffer lock during apply
                let op = {
                    let buffer = pending_buffer.read().await;
                    if i >= buffer.len() {
                        break; // Buffer changed size
                    }
                    buffer.operations()[i].clone()
                };

                match server.apply_remote_operation(op.clone()).await {
                    Ok(true) => {
                        // Operation applied successfully, remove it from buffer
                        let mut buffer = pending_buffer.write().await;
                        if i < buffer.len() {
                            buffer.remove(i);
                            applied_this_pass += 1;
                            debug!("Applied buffered operation for set={}", op.set_name);
                        }
                        // Don't increment i, since we removed an element
                    }
                    Ok(false) => {
                        // Still can't apply, move to next operation
                        i += 1;
                    }
                    Err(e) => {
                        // Storage error - this is unexpected, log and skip
                        error!("Storage error applying buffered operation: {}", e);
                        i += 1;
                    }
                }
            }

            total_applied += applied_this_pass;

            // If we didn't apply anything this pass, we've reached a fixed point
            if applied_this_pass == 0 {
                break;
            }
        }

        if total_applied > 0 {
            info!("Applied {} buffered operations", total_applied);
        }

        total_applied
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&self.addr).await?;
        info!("Replication server listening on {}", self.addr);

        loop {
            let (socket, peer_addr) = listener.accept().await?;
            debug!("Replication connection from {}", peer_addr);

            let server = Arc::clone(&self.server);
            let replication = Arc::clone(&self.replication);

            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(socket, server, replication).await {
                    error!("Replication connection error from {}: {}", peer_addr, e);
                }
            });
        }
    }

    async fn handle_connection(
        mut socket: TcpStream,
        server: Arc<Server<S>>,
        replication: Arc<ReplicationManager>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            // Read length prefix (4 bytes big-endian)
            let len = match socket.read_u32().await {
                Ok(len) => len as usize,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    debug!("Peer closed connection");
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            };

            // Read message body
            let mut buf = vec![0u8; len];
            socket.read_exact(&mut buf).await?;

            // Decode protobuf Operation
            let proto_op = crate::proto::replication::Operation::decode(&buf[..])?;
            let operation = match crate::proto::proto_to_operation(&proto_op) {
                Some(op) => op,
                None => {
                    warn!("Failed to decode operation from protobuf");
                    continue;
                }
            };

            info!("Received operation for set={}", operation.set_name);

            // Try to apply operation
            match server.apply_remote_operation(operation.clone()).await {
                Ok(true) => {
                    debug!("Applied operation successfully");
                    // Try to drain the buffer - newly applied operation might unblock others
                    Self::try_apply_buffered(Arc::clone(&server), Arc::clone(&replication)).await;
                }
                Ok(false) => {
                    // Causality not satisfied, buffer it
                    debug!(
                        "Operation for set={} needs buffering (causality not satisfied)",
                        operation.set_name
                    );
                    let pending_buffer = replication.pending_buffer();
                    let mut buffer = pending_buffer.write().await;
                    if !buffer.add(operation) {
                        warn!(
                            "Pending buffer is full! Buffer size: {}/{}",
                            buffer.len(),
                            buffer.max_size()
                        );
                        // TODO: Consider triggering anti-entropy here
                    }
                }
                Err(e) => {
                    error!(
                        "Storage error applying operation for set={}: {}",
                        operation.set_name, e
                    );
                }
            }
        }
    }
}
