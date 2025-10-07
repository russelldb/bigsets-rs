use crate::types::Operation;
use async_trait::async_trait;
use bytes::BytesMut;
use prost::Message;
use std::error::Error;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::RwLock;

/// Network abstraction trait for sending/receiving operations
#[async_trait]
pub trait NetworkTransport: Send + Sync {
    /// Send an operation to a peer
    async fn send_operation(&self, peer_addr: &str, op: &Operation) -> Result<(), Box<dyn Error>>;

    /// Send an acknowledgment to a peer
    async fn send_ack(&self, peer_addr: &str, op_id: &[u8]) -> Result<(), Box<dyn Error>>;

    /// Receive the next operation (blocking until available)
    async fn recv_operation(&self) -> Result<Operation, Box<dyn Error>>;

    /// Receive the next acknowledgment (blocking until available)
    async fn recv_ack(&self) -> Result<Vec<u8>, Box<dyn Error>>;
}

/// Production TCP-based network transport
pub struct TcpTransport {
    // Connection pool or similar could be added here
}

impl TcpTransport {
    pub fn new() -> Self {
        Self {}
    }

    async fn send_message(&self, peer_addr: &str, data: &[u8]) -> Result<(), Box<dyn Error>> {
        let mut stream = TcpStream::connect(peer_addr).await?;

        // Send length prefix (4 bytes, big-endian)
        let len = data.len() as u32;
        stream.write_all(&len.to_be_bytes()).await?;

        // Send data
        stream.write_all(data).await?;
        stream.flush().await?;

        Ok(())
    }
}

#[async_trait]
impl NetworkTransport for TcpTransport {
    async fn send_operation(&self, peer_addr: &str, op: &Operation) -> Result<(), Box<dyn Error>> {
        // Serialize operation using protobuf
        let proto_op = crate::proto::operation_to_proto(op);
        let mut buf = BytesMut::new();
        proto_op.encode(&mut buf)?;

        self.send_message(peer_addr, &buf).await
    }

    async fn send_ack(&self, peer_addr: &str, op_id: &[u8]) -> Result<(), Box<dyn Error>> {
        // Simple ACK message: just the operation ID
        self.send_message(peer_addr, op_id).await
    }

    async fn recv_operation(&self) -> Result<Operation, Box<dyn Error>> {
        // This would be implemented in the replication server listener
        unimplemented!("recv_operation should be implemented in replication server")
    }

    async fn recv_ack(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        // This would be implemented in the replication server listener
        unimplemented!("recv_ack should be implemented in replication server")
    }
}

/// In-memory network transport for testing
#[derive(Clone)]
pub struct InMemoryTransport {
    // Shared queues for operations and acks between peers
    operation_queue: Arc<RwLock<Vec<(String, Operation)>>>, // (from_addr, operation)
    ack_queue: Arc<RwLock<Vec<(String, Vec<u8>)>>>,         // (from_addr, op_id)
}

impl InMemoryTransport {
    pub fn new() -> Self {
        Self {
            operation_queue: Arc::new(RwLock::new(Vec::new())),
            ack_queue: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Create a pair of connected in-memory transports for testing
    pub fn create_pair() -> (Self, Self) {
        let op_queue = Arc::new(RwLock::new(Vec::new()));
        let ack_queue = Arc::new(RwLock::new(Vec::new()));

        let t1 = Self {
            operation_queue: op_queue.clone(),
            ack_queue: ack_queue.clone(),
        };

        let t2 = Self {
            operation_queue: op_queue,
            ack_queue,
        };

        (t1, t2)
    }

    /// Check if there are pending operations
    pub async fn has_pending_operations(&self) -> bool {
        !self.operation_queue.read().await.is_empty()
    }

    /// Check if there are pending acks
    pub async fn has_pending_acks(&self) -> bool {
        !self.ack_queue.read().await.is_empty()
    }

    /// Drain all operations (for testing)
    pub async fn drain_operations(&self) -> Vec<(String, Operation)> {
        self.operation_queue.write().await.drain(..).collect()
    }

    /// Drain all acks (for testing)
    pub async fn drain_acks(&self) -> Vec<(String, Vec<u8>)> {
        self.ack_queue.write().await.drain(..).collect()
    }
}

#[async_trait]
impl NetworkTransport for InMemoryTransport {
    async fn send_operation(&self, peer_addr: &str, op: &Operation) -> Result<(), Box<dyn Error>> {
        let mut queue = self.operation_queue.write().await;
        queue.push((peer_addr.to_string(), op.clone()));
        Ok(())
    }

    async fn send_ack(&self, peer_addr: &str, op_id: &[u8]) -> Result<(), Box<dyn Error>> {
        let mut queue = self.ack_queue.write().await;
        queue.push((peer_addr.to_string(), op_id.to_vec()));
        Ok(())
    }

    async fn recv_operation(&self) -> Result<Operation, Box<dyn Error>> {
        loop {
            let mut queue = self.operation_queue.write().await;
            if let Some((_, op)) = queue.pop() {
                return Ok(op);
            }
            drop(queue);

            // Brief sleep to avoid busy-waiting
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    async fn recv_ack(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        loop {
            let mut queue = self.ack_queue.write().await;
            if let Some((_, ack_id)) = queue.pop() {
                return Ok(ack_id);
            }
            drop(queue);

            // Brief sleep to avoid busy-waiting
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{OpType, VersionVector};
    use bytes::Bytes;

    #[tokio::test]
    async fn test_in_memory_transport_operations() {
        let (t1, t2) = InMemoryTransport::create_pair();

        // Create a test operation
        let mut vv = VersionVector::new();
        let actor = crate::types::ActorId::from_node_id(1);
        let dot = vv.increment(actor);
        let op = Operation {
            set_id: 1,
            op_type: OpType::Add {
                elements: vec![Bytes::from("test")],
                dot,
                removed_dots: vec![],
            },
            context: vv,
        };

        // Send from t1
        t1.send_operation("node-2", &op).await.unwrap();

        // Verify it's pending
        assert!(t2.has_pending_operations().await);

        // Receive on t2
        let received = t2.recv_operation().await.unwrap();
        assert_eq!(received.set_id, op.set_id);
    }

    #[tokio::test]
    async fn test_in_memory_transport_acks() {
        let (t1, t2) = InMemoryTransport::create_pair();

        let op_id = vec![1, 2, 3, 4];

        // Send ACK from t1
        t1.send_ack("node-2", &op_id).await.unwrap();

        // Verify it's pending
        assert!(t2.has_pending_acks().await);

        // Receive on t2
        let received = t2.recv_ack().await.unwrap();
        assert_eq!(received, op_id);
    }

    #[tokio::test]
    async fn test_drain_operations() {
        let transport = InMemoryTransport::new();

        let mut vv = VersionVector::new();
        let actor = crate::types::ActorId::from_node_id(1);
        let dot = vv.increment(actor);
        let op = Operation {
            set_id: 1,
            op_type: OpType::Add {
                elements: vec![Bytes::from("test")],
                dot,
                removed_dots: vec![],
            },
            context: vv,
        };

        transport.send_operation("node-2", &op).await.unwrap();
        transport.send_operation("node-3", &op).await.unwrap();

        let drained = transport.drain_operations().await;
        assert_eq!(drained.len(), 2);
        assert!(!transport.has_pending_operations().await);
    }
}
