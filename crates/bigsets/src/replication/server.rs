use crate::core_server::Server;
use crate::replication::ReplicationManager;
use crate::storage::Storage;
use crate::types::Operation;
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
                Ok(()) => {
                    debug!("Applied operation successfully");
                    // TODO: Try to apply buffered operations
                }
                Err(e) => {
                    // Check if it's a causality error
                    // For now, just buffer it
                    warn!("Failed to apply operation (causality?): {}", e);
                    replication.pending_buffer().write().await.add(operation);
                }
            }
        }
    }
}
