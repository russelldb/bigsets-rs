use crate::types::Operation;
use std::collections::HashMap;
use std::time::Instant;

/// Sender-side unacked buffer for retry logic
///
/// Tracks operations sent to peers that haven't been acknowledged yet.
/// Used for retransmission on timeout and managing flow control.
#[derive(Debug)]
pub struct UnackedBuffer {
    ops: HashMap<String, Vec<(Operation, Instant, u32)>>, // peer_id -> [(op, sent_at, retry_count)]
}

impl UnackedBuffer {
    pub fn new() -> Self {
        Self {
            ops: HashMap::new(),
        }
    }

    /// Add an operation to the unacked buffer for a specific peer
    pub fn add(&mut self, peer_id: String, op: Operation) {
        self.ops
            .entry(peer_id)
            .or_insert_with(Vec::new)
            .push((op, Instant::now(), 0));
    }

    /// Remove a specific operation from the buffer after acknowledgment
    pub fn remove(&mut self, peer_id: &str, op_index: usize) -> bool {
        if let Some(ops) = self.ops.get_mut(peer_id) {
            if op_index < ops.len() {
                ops.remove(op_index);
                return true;
            }
        }
        false
    }

    /// Get all unacked operations for a specific peer
    pub fn get_peer_ops(&self, peer_id: &str) -> Option<&[(Operation, Instant, u32)]> {
        self.ops.get(peer_id).map(|v| v.as_slice())
    }

    /// Get mutable reference to all unacked operations for a specific peer
    pub fn get_peer_ops_mut(
        &mut self,
        peer_id: &str,
    ) -> Option<&mut Vec<(Operation, Instant, u32)>> {
        self.ops.get_mut(peer_id)
    }

    /// Get number of unacked operations for a specific peer
    pub fn peer_count(&self, peer_id: &str) -> usize {
        self.ops.get(peer_id).map(|v| v.len()).unwrap_or(0)
    }

    /// Get total number of unacked operations across all peers
    pub fn total_count(&self) -> usize {
        self.ops.values().map(|v| v.len()).sum()
    }

    /// Get all peer IDs that have unacked operations
    pub fn peers(&self) -> Vec<&String> {
        self.ops.keys().collect()
    }

    /// Clear all unacked operations for a specific peer
    pub fn clear_peer(&mut self, peer_id: &str) {
        self.ops.remove(peer_id);
    }

    /// Clear all unacked operations
    pub fn clear_all(&mut self) {
        self.ops.clear();
    }
}

impl Default for UnackedBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// Receiver-side pending buffer for out-of-order operations
///
/// Stores operations that cannot be applied yet due to causality constraints.
/// When the buffer fills up, it signals the need for RBILT (Reliable Broadcast with Incremental Learning).
#[derive(Debug)]
pub struct PendingBuffer {
    ops: Vec<Operation>,
    max_size: usize,
}

impl PendingBuffer {
    pub fn new(max_size: usize) -> Self {
        Self {
            ops: Vec::new(),
            max_size,
        }
    }

    /// Add an operation to the pending buffer
    ///
    /// Returns false if the buffer is full (overflow condition), true otherwise
    pub fn add(&mut self, op: Operation) -> bool {
        if self.ops.len() >= self.max_size {
            return false; // Signal overflow
        }
        self.ops.push(op);
        true
    }

    /// Check if the buffer is full
    pub fn is_full(&self) -> bool {
        self.ops.len() >= self.max_size
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Get current number of pending operations
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Get maximum buffer size
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Get reference to all pending operations
    pub fn operations(&self) -> &[Operation] {
        &self.ops
    }

    /// Get mutable reference to all pending operations
    pub fn operations_mut(&mut self) -> &mut Vec<Operation> {
        &mut self.ops
    }

    /// Remove and return an operation at a specific index
    pub fn remove(&mut self, index: usize) -> Option<Operation> {
        if index < self.ops.len() {
            Some(self.ops.remove(index))
        } else {
            None
        }
    }

    /// Remove operations that satisfy a predicate
    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&Operation) -> bool,
    {
        self.ops.retain(f);
    }

    /// Clear all pending operations
    pub fn clear(&mut self) {
        self.ops.clear();
    }

    /// Drain all operations, clearing the buffer
    pub fn drain(&mut self) -> Vec<Operation> {
        std::mem::take(&mut self.ops)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ActorId, Dot, OpType, VersionVector};
    use bytes::Bytes;

    fn create_test_op(set_id: u64, counter: u64) -> Operation {
        Operation {
            set_id,
            op_type: OpType::Add {
                elements: vec![Bytes::from("test")],
                dot: Dot {
                    actor_id: ActorId::from_node_id(1),
                    counter,
                },
                removed_dots: vec![],
            },
            context: VersionVector::new(),
        }
    }

    #[test]
    fn test_unacked_buffer_new() {
        let buffer = UnackedBuffer::new();
        assert_eq!(buffer.total_count(), 0);
        assert!(buffer.peers().is_empty());
    }

    #[test]
    fn test_unacked_buffer_add() {
        let mut buffer = UnackedBuffer::new();
        let op1 = create_test_op(1, 1);
        let op2 = create_test_op(1, 2);

        buffer.add("peer1".to_string(), op1);
        buffer.add("peer1".to_string(), op2);

        assert_eq!(buffer.peer_count("peer1"), 2);
        assert_eq!(buffer.total_count(), 2);
    }

    #[test]
    fn test_unacked_buffer_add_multiple_peers() {
        let mut buffer = UnackedBuffer::new();

        buffer.add("peer1".to_string(), create_test_op(1, 1));
        buffer.add("peer2".to_string(), create_test_op(2, 1));
        buffer.add("peer1".to_string(), create_test_op(1, 2));

        assert_eq!(buffer.peer_count("peer1"), 2);
        assert_eq!(buffer.peer_count("peer2"), 1);
        assert_eq!(buffer.total_count(), 3);
        assert_eq!(buffer.peers().len(), 2);
    }

    #[test]
    fn test_unacked_buffer_remove() {
        let mut buffer = UnackedBuffer::new();
        buffer.add("peer1".to_string(), create_test_op(1, 1));
        buffer.add("peer1".to_string(), create_test_op(1, 2));

        assert!(buffer.remove("peer1", 0));
        assert_eq!(buffer.peer_count("peer1"), 1);

        assert!(buffer.remove("peer1", 0));
        assert_eq!(buffer.peer_count("peer1"), 0);

        assert!(!buffer.remove("peer1", 0)); // Nothing left to remove
    }

    #[test]
    fn test_unacked_buffer_get_peer_ops() {
        let mut buffer = UnackedBuffer::new();
        let op1 = create_test_op(1, 1);

        buffer.add("peer1".to_string(), op1.clone());

        let ops = buffer.get_peer_ops("peer1").unwrap();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].0.set_id, 1);

        assert!(buffer.get_peer_ops("peer2").is_none());
    }

    #[test]
    fn test_unacked_buffer_clear_peer() {
        let mut buffer = UnackedBuffer::new();
        buffer.add("peer1".to_string(), create_test_op(1, 1));
        buffer.add("peer2".to_string(), create_test_op(2, 1));

        buffer.clear_peer("peer1");

        assert_eq!(buffer.peer_count("peer1"), 0);
        assert_eq!(buffer.peer_count("peer2"), 1);
        assert_eq!(buffer.total_count(), 1);
    }

    #[test]
    fn test_unacked_buffer_clear_all() {
        let mut buffer = UnackedBuffer::new();
        buffer.add("peer1".to_string(), create_test_op(1, 1));
        buffer.add("peer2".to_string(), create_test_op(2, 1));

        buffer.clear_all();

        assert_eq!(buffer.total_count(), 0);
        assert!(buffer.peers().is_empty());
    }

    #[test]
    fn test_pending_buffer_new() {
        let buffer = PendingBuffer::new(10);
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.max_size(), 10);
        assert!(!buffer.is_full());
    }

    #[test]
    fn test_pending_buffer_add() {
        let mut buffer = PendingBuffer::new(3);

        assert!(buffer.add(create_test_op(1, 1)));
        assert_eq!(buffer.len(), 1);
        assert!(!buffer.is_full());

        assert!(buffer.add(create_test_op(1, 2)));
        assert_eq!(buffer.len(), 2);
        assert!(!buffer.is_full());

        assert!(buffer.add(create_test_op(1, 3)));
        assert_eq!(buffer.len(), 3);
        assert!(buffer.is_full());
    }

    #[test]
    fn test_pending_buffer_overflow() {
        let mut buffer = PendingBuffer::new(2);

        assert!(buffer.add(create_test_op(1, 1)));
        assert!(buffer.add(create_test_op(1, 2)));
        assert!(!buffer.add(create_test_op(1, 3))); // Should fail - buffer full

        assert_eq!(buffer.len(), 2); // Still 2 operations
        assert!(buffer.is_full());
    }

    #[test]
    fn test_pending_buffer_remove() {
        let mut buffer = PendingBuffer::new(10);
        buffer.add(create_test_op(1, 1));
        buffer.add(create_test_op(1, 2));
        buffer.add(create_test_op(1, 3));

        let op = buffer.remove(1).unwrap();
        assert_eq!(op.set_id, 1);
        assert_eq!(buffer.len(), 2);

        assert!(buffer.remove(5).is_none()); // Out of bounds
    }

    #[test]
    fn test_pending_buffer_retain() {
        let mut buffer = PendingBuffer::new(10);
        buffer.add(create_test_op(1, 1));
        buffer.add(create_test_op(2, 2));
        buffer.add(create_test_op(1, 3));

        // Keep only operations for set_id 1
        buffer.retain(|op| op.set_id == 1);

        assert_eq!(buffer.len(), 2);
        assert!(buffer.operations().iter().all(|op| op.set_id == 1));
    }

    #[test]
    fn test_pending_buffer_clear() {
        let mut buffer = PendingBuffer::new(10);
        buffer.add(create_test_op(1, 1));
        buffer.add(create_test_op(1, 2));

        buffer.clear();

        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_pending_buffer_drain() {
        let mut buffer = PendingBuffer::new(10);
        buffer.add(create_test_op(1, 1));
        buffer.add(create_test_op(1, 2));

        let ops = buffer.drain();

        assert_eq!(ops.len(), 2);
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_pending_buffer_operations() {
        let mut buffer = PendingBuffer::new(10);
        buffer.add(create_test_op(1, 1));
        buffer.add(create_test_op(2, 2));

        let ops = buffer.operations();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].set_id, 1);
        assert_eq!(ops[1].set_id, 2);
    }

    #[test]
    fn test_unacked_buffer_retry_tracking() {
        let mut buffer = UnackedBuffer::new();
        buffer.add("peer1".to_string(), create_test_op(1, 1));

        // Get mutable reference and increment retry count
        if let Some(ops) = buffer.get_peer_ops_mut("peer1") {
            ops[0].2 += 1; // Increment retry count
        }

        let ops = buffer.get_peer_ops("peer1").unwrap();
        assert_eq!(ops[0].2, 1); // Verify retry count
    }

    #[test]
    fn test_unacked_buffer_timestamp_tracking() {
        let mut buffer = UnackedBuffer::new();
        let before = Instant::now();
        buffer.add("peer1".to_string(), create_test_op(1, 1));
        let after = Instant::now();

        let ops = buffer.get_peer_ops("peer1").unwrap();
        assert!(ops[0].1 >= before);
        assert!(ops[0].1 <= after);
    }
}
