# BigSets Replication Design Plan

## Executive Summary

Design a **gossip-based, eventually consistent replication system** with causal delivery guarantees, optimized for CRDT semantics. Start with a simple "eager push" model before adding anti-entropy via RIBLT (Rateless IBLT) set reconciliation.

---

## Key Design Decisions

### 1. **Topology: Fully Connected Mesh (for now)**
- **PRO**: Simple, low latency, no routing complexity
- **PRO**: Every node can receive writes, full symmetry
- **PRO**: Works well for small clusters (3-10 nodes)
- **CON**: O(N²) connections, doesn't scale beyond ~10-20 nodes
- **Decision**: Start with mesh, consider libp2p later for federated/larger clusters

### 2. **Replication Model: Eager Push + Lazy Anti-Entropy**
- **Phase 1 (this design)**: Eager push on write path
  - When node receives SADD/SREM, apply locally then immediately push to all peers
  - Simple "fire and forget" with retry on failure
- **Phase 2 (future)**: RIBLT-based anti-entropy for repair
  - Periodic gossip to detect missing operations
  - Use fixed-size dot encoding (4-byte ActorId + 8-byte counter = 12 bytes)

### 3. **Buffer Architecture**

#### **Sender Side (per node): Single UnackedBuffer**
- One buffer tracking unacked ops across ALL peers: `HashMap<Dot, (Operation, HashSet<ActorId>, Instant, u32)>`
- Track which peers haven't acked each operation
- **Retry policy**: Exponential backoff, max retries from config

#### **Receiver Side (per node): Single PendingBuffer**
- One causal delivery buffer for out-of-order operations
- When op arrives, check: `op.context.dominates(local_vv)?`
  - YES → apply immediately, update local VV, try pending buffer
  - NO → add to pending buffer (if space), defer application
- When buffer fills → trigger anti-entropy (future: RIBLT)

### 4. **ACK Semantics: Receipt ACK (Buffering), NOT Application**

**CRITICAL DECISION**: ACK when operation is received/buffered, not after application to database.

#### **Rationale**:
- **Retry is for network reliability**: Did the message arrive at the receiver?
- **Anti-entropy is for state convergence**: Do both nodes have the same operations?
- **Causal delivery is independent**: Receiver applies buffered ops when causality permits

#### **Flow**:
```
Sender:
1. Send operation to peer
2. Add to unacked_buffer[peer]
3. Wait for ACK (with timeout)
4. On ACK: Remove from unacked_buffer
5. On timeout: Retry (exponential backoff)
6. After max_retries: Give up, rely on anti-entropy

Receiver:
1. Receive operation
2. Send ACK immediately (message received)  ← KEY DECISION
3. Check causality: op.context.dominates(local_vv)?
   - YES: Apply to database, update VV, try pending buffer
   - NO: Add to pending_buffer (wait for missing ops)
```

#### **Why NOT ACK after application?**:
- ✅ Sender stops retrying as soon as receiver has the message
- ✅ Decouples network reliability from causal delivery
- ✅ Receiver can handle buffered operations at its own pace
- ✅ Anti-entropy repairs any divergence (lost messages, stuck buffers)
- ✅ No waiting for database I/O before ACKing

#### **Buffer Overflow Case**:
If `pending_buffer.add(op)` returns false (buffer full):
- **Still send ACK** (we received it, just can't buffer right now)
- **Log warning**: Pending buffer overflow
- **Trigger immediate anti-entropy** with sender (something is wrong)
- Anti-entropy will re-deliver missing operations

### 5. **Message Types & Protocol**

```protobuf
// replication.proto (additions needed)

// Wrapper for all replication messages
message ReplicationMessage {
  oneof message_type {
    Operation operation = 1;        // Push operation to peer
    Ack ack = 2;                    // Acknowledge operation received
    AntiEntropy anti_entropy = 3;   // Future: RIBLT request/response
  }
}

// ACK message (already defined, may need receiver_vv field)
message Ack {
  Dot operation_dot = 1;             // Which operation we're acknowledging
  VersionVector receiver_vv = 2;     // Optional: piggyback VV for debugging
}
```

### 6. **Connection Management**

#### **Persistent TCP Connections**
- Maintain one persistent connection per peer (bidirectional)
- Multiplex operations and ACKs over same connection
- **Framing**: Length-prefixed protobuf messages (4-byte big-endian length + payload)
- **Reconnection**: Exponential backoff on connection failure
- **Heartbeat**: Optional PING/PONG every 5s to detect dead connections

#### **No libp2p (Phase 1)**
- **PRO**: Simpler, fewer dependencies, easier to debug
- **PRO**: Direct TCP gives us full control over framing and backpressure
- **CON**: No NAT traversal, no DHT, no discovery
- **Decision**: Manual peer configuration (config.toml) sufficient for phase 1
- **Future**: Consider libp2p for federated/WAN deployments

### 7. **Replication Server Architecture**

```rust
// New: ReplicationServer component
pub struct ReplicationServer {
    server: Arc<Server>,                    // Access to DB, VV, buffers
    transport: Arc<dyn NetworkTransport>,   // Abstraction for testing
    peer_connections: Arc<RwLock<HashMap<ActorId, PeerConnection>>>,
}

struct PeerConnection {
    actor_id: ActorId,
    addr: String,
    tx: mpsc::Sender<ReplicationMessage>,   // Send queue
    state: ConnectionState,                  // Connected/Disconnected/Reconnecting
    last_heartbeat: Instant,
}

enum ConnectionState {
    Connected,
    Disconnected,
    Reconnecting { attempts: u32 },
}
```

#### **Key Tasks**:
1. **Listen for incoming connections** on `replication_addr`
2. **Maintain outgoing connections** to all configured peers
3. **Send operations** when local writes occur (hook into commands.rs)
4. **Receive operations** and attempt causal delivery
5. **Send ACKs** after receiving operation (immediate)
6. **Receive ACKs** and remove from unacked buffer
7. **Retry loop** for unacked operations (background task)
8. **Heartbeat loop** to detect failures (background task)

---

## Implementation Flow (Phase 1)

### **Write Path (SADD on Node 1)**

```
1. Client → Node 1: SADD myset apple
2. Node 1: Apply locally (increment VV, insert dot)
3. Node 1: Create Operation { dot: (node1, 5), elements: [apple], context: VV }
4. Node 1: Add to unacked_buffer for all peers (node2, node3)
5. Node 1: Send Operation to node2 and node3 via ReplicationServer
6. Node 1: Return OK to client (don't wait for ACKs)
```

### **Replication Path (Node 2 receives operation)**

```
1. Node 2: Receive Operation { dot: (node1, 5), context: VV_node1 }
2. Node 2: Send ACK { dot: (node1, 5) } immediately  ← RECEIPT ACK
3. Node 2: Check causality: VV_node1.dominates(VV_node2)?
   - YES: Apply immediately (step 4)
   - NO: Add to pending_buffer, return (wait for missing ops)
4. Node 2: Apply operation (insert element, dot, update VV)
5. Node 2: Try to apply ops from pending_buffer (might now be ready)
6. Node 1: Receive ACK, remove from unacked_buffer
```

### **Retry Path (ACK timeout)**

```
1. Background task on Node 1: Check unacked_buffer every 500ms
2. Find operations older than ack_timeout_ms (500ms default)
3. For each timed-out operation and each peer that hasn't ACKed:
   - Increment retry_count for that peer
   - If retry_count > max_retries (5): Log error, give up (rely on anti-entropy)
   - Else: Resend operation to peer
4. Exponential backoff: delay = 100ms * 2^retry_count
```

---

## Data Structures

### **Updated UnackedBuffer (Sender)**
```rust
pub struct UnackedBuffer {
    // Dot -> (operation, peers_pending_ack, sent_at, retry_count_per_peer)
    ops: HashMap<Dot, (Operation, HashSet<ActorId>, Instant, HashMap<ActorId, u32>)>,
}

// Key operations:
// - add(op, peers) - Add operation to buffer for list of peers
// - ack(dot, peer) - Remove peer from pending set, cleanup if all ACKed
// - get_timedout(timeout) - Find ops that need retry per peer
// - retry(dot, peer) - Increment retry count for specific peer
```

### **Existing PendingBuffer (Receiver)**
```rust
pub struct PendingBuffer {
    ops: Vec<Operation>,  // Out-of-order operations
    max_size: usize,      // Trigger anti-entropy when full
}

// No changes needed - already implemented
```

### **Version Vector (existing)**
```rust
pub struct VersionVector {
    counters: HashMap<ActorId, u64>,  // Already exists
}

// Key operation needed:
// - dominates(&self, other: &VersionVector) -> bool
//   Returns true if self ≥ other (all counters in self >= corresponding in other)
```

---

## Causality & Concurrency Guarantees

### **Causal Delivery**
- Operation with context `C` can be applied when `C ⊆ VV_local` (C.dominates(VV_local))
- Ensures all causally prior operations have been applied
- **No gaps in VV**: Each actor's counter advances sequentially

### **Concurrent Operations**
- Two operations from different actors are concurrent if neither's context dominates
- Add-wins semantics: concurrent adds win over removes
- Dots make operations idempotent (duplicate delivery is safe)

### **Convergence**
- All nodes eventually apply the same set of operations (via retry + anti-entropy)
- Deterministic conflict resolution (add-wins)
- Anti-entropy repairs missing operations and unblocks stuck buffers

---

## Configuration Parameters

```toml
[replication]
max_retries = 5                # Give up after 5 retries per peer
retry_backoff_ms = 100         # Base delay between retries
buffer_size = 1000             # Pending buffer max size
ack_timeout_ms = 500           # Wait 500ms for ACK before retry
heartbeat_interval_ms = 5000   # Send heartbeat every 5s
connection_timeout_ms = 10000  # Consider peer dead after 10s silence
rbilt_startup_delay_ms = 1000  # Wait before initial anti-entropy (future)
```

---

## Testing Strategy

### **Unit Tests**
- Causality checking (VV.dominates)
- UnackedBuffer operations (add, ack, retry per peer)
- PendingBuffer operations (add, overflow, drain)
- Protobuf serialization roundtrip (ReplicationMessage, Ack)

### **Integration Tests (using InMemoryTransport)**
- 3-node cluster, write to node 1, read from node 2
- Verify operation propagates and is applied
- Concurrent writes to different nodes converge
- Out-of-order delivery → pending buffer → eventual application
- ACK removes from unacked buffer
- Timeout triggers retry
- Max retries → give up (test without anti-entropy for now)

### **Manual Testing with bigsets-dev**
- Start 3 nodes: `cargo run --bin bigsets-dev -- -n 3`
- Connect redis-cli to node 1 (port 6379): `redis-cli -p 6379`
- Write: `SADD myset apple banana`
- Connect to node 2 (port 6380): `redis-cli -p 6380`
- Read: `SCARD myset` (expect 2 after replication)
- Test failure: Kill node 3, write to node 1, restart node 3 (should catch up via retry or anti-entropy)

---

## Future Enhancements (Phase 2+)

### **RIBLT Anti-Entropy**
- Periodic gossip (every 10s?) to reconcile dots between peers
- Encode dots as 12-byte values: `ActorId(4) + Counter(8)`
- RIBLT over dots, not elements (per NOTES.md requirement)
- When pending buffer fills → trigger immediate anti-entropy
- Repairs: lost messages, stuck buffers, operations that exceeded max_retries

### **Compression & Batching**
- Batch multiple operations into single ReplicationMessage
- Snappy compression for large element payloads
- Reduces TCP overhead and network usage

### **Read Repair**
- When SCARD returns result, piggyback local VV in response
- Client can detect staleness (local VV vs returned VV)
- Trigger targeted anti-entropy or wait

### **Quorum Reads (optional)**
- Wait for acknowledgment from majority of nodes before returning
- Trade latency for stronger consistency guarantees
- Useful for read-heavy workloads requiring fresher data

### **libp2p Migration**
- DHT-based peer discovery
- NAT traversal for federated clusters (home users, edge deployments)
- QUIC for multiplexed streams with better congestion control
- Circuit relay for firewall traversal

### **Monitoring & Observability**
- Metrics: replication lag, unacked buffer size, pending buffer size, retry rate
- Distributed tracing: trace operation from write through replication to application
- Health checks: per-peer connection status, last heartbeat time

---

## Files to Create/Modify

### **New Files**
1. `crates/bigsets/src/replication.rs` - ReplicationServer implementation
   - Listen for incoming connections
   - Maintain outgoing connections
   - Send/receive operations and ACKs
   - Retry loop
   - Heartbeat loop

2. `crates/bigsets/src/connection.rs` - PeerConnection management
   - Connection state machine
   - Reconnection logic with exponential backoff
   - Message framing (length-prefix)
   - Send/receive queues

### **Modified Files**
1. `crates/bigsets/src/server.rs`
   - Add `replication_server: Option<Arc<ReplicationServer>>` field
   - Start ReplicationServer in `start()` method
   - Expose hook for commands to trigger replication

2. `crates/bigsets/src/commands.rs`
   - After successful SADD/SREM, create Operation and call replication hook
   - Hook: `server.replicate_operation(operation).await`

3. `crates/bigsets/src/buffers.rs`
   - Update UnackedBuffer to track per-peer ACK status
   - Add methods: `add(op, peers)`, `ack(dot, peer)`, `get_timedout()`, `retry(dot, peer)`

4. `crates/bigsets/src/types.rs`
   - Add `VersionVector::dominates(&self, other: &VersionVector) -> bool` method
   - Possibly add `Dot` impl for hashing (use as HashMap key)

5. `crates/bigsets/proto/replication.proto`
   - Add `ReplicationMessage` wrapper with oneof
   - Ensure `Ack` message has optional `receiver_vv` field

6. `crates/bigsets/src/proto.rs`
   - Add conversion helpers for `ReplicationMessage` and `Ack`
   - Update `operation_to_proto` / `proto_to_operation` if needed

7. `crates/bigsets/src/network.rs`
   - Implement `recv_operation` and `recv_ack` for `TcpTransport`
   - Add framing logic (length-prefix read/write)
   - Connection pooling or per-peer connections

8. `crates/bigsets/src/lib.rs`
   - Export new modules: `pub mod replication;`, `pub mod connection;`

---

## Pros & Cons Summary

### **Chosen Approach (Eager Push + Mesh + Receipt ACK)**

**PROS**:
- ✅ Low latency (immediate propagation)
- ✅ Simple to implement and debug
- ✅ Natural fit for CRDT semantics
- ✅ No external dependencies (no libp2p yet)
- ✅ Testable with InMemoryTransport
- ✅ Works well for 3-10 node clusters
- ✅ Clear separation: retry (network) vs delivery (causality) vs repair (anti-entropy)
- ✅ Receiver controls application pace (buffered ops applied when ready)

**CONS**:
- ❌ O(N) messages per write (doesn't scale to 100+ nodes)
- ❌ No WAN/NAT traversal (manual config required)
- ❌ Retry storms possible under high load (mitigated by exponential backoff)
- ❌ Sender doesn't know if operation was applied (only that it was received)

### **Alternative: Gossip + Anti-Entropy Only**

**PROS**:
- ✅ More scalable (probabilistic gossip, O(log N))
- ✅ Self-healing, no retries needed

**CONS**:
- ❌ Higher latency (eventual propagation, not immediate)
- ❌ More complex (gossip rounds, fanout tuning, membership protocols)
- ❌ Still need causal delivery buffer
- ❌ Harder to reason about and debug

**Decision**: Start simple (eager push), add gossip optimization later if needed for larger clusters.

### **Alternative: Application ACK (ACK after DB write)**

**PROS**:
- ✅ Sender knows operation was successfully applied
- ✅ Can implement quorum writes (wait for majority ACK)

**CONS**:
- ❌ Retry continues even when operation is buffered (wasted bandwidth)
- ❌ Couples network reliability with database performance
- ❌ Receiver can't ACK quickly if database is slow/busy
- ❌ ACK timeout must be tuned to account for DB latency (harder)

**Decision**: Receipt ACK is cleaner, separates concerns, relies on anti-entropy for convergence.

---

## Recommendation

**Implement Phase 1: Eager Push Replication with Receipt ACK**

This gives us:
- Working 3-node cluster with causal consistency
- Foundation for RIBLT anti-entropy
- Real-world testing with redis-cli
- Clear path to optimization (batching, compression, gossip)

Then iterate:
- Add RIBLT when anti-entropy is needed (periodic reconciliation)
- Add libp2p when federation is needed (WAN, NAT traversal)
- Add compression when bandwidth is constrained
- Add monitoring/metrics for observability

---

## Open Questions / TODOs

1. **VV persistence**: Should we persist version vector to database on every update? (Performance vs durability trade-off)
2. **Operation replay on restart**: How to rebuild unacked_buffer after crash? (May need operation log)
3. **Peer failure detection**: How long to wait before marking peer as dead? (Heartbeat timeout vs false positives)
4. **Buffer eviction policy**: What to do when pending buffer fills and can't accept new ops? (Drop oldest? Trigger immediate anti-entropy? Reject with backpressure?)
5. **Set-scoped VV vs global VV**: Current design uses single global VV per node. Is this sufficient or do we need per-set VV? (Current approach is simpler, matches design)
6. **RIBLT implementation**: Which Rust crate? Roll our own? (Defer to Phase 2)
7. **Testing anti-entropy**: How to test that anti-entropy repairs divergence? (Simulate message loss, clock skew, network partition)

---

## References

- **RIBLT Paper**: "Practical Rateless Set Reconciliation" (2024) - https://arxiv.org/abs/2402.02668
- **CRDT Survey**: Shapiro et al., "A comprehensive study of Convergent and Commutative Replicated Data Types"
- **Add-Wins Set**: Bieniusa et al., "An Optimized Conflict-free Replicated Set"
- **BigSets Original**: Basho BigSets (Erlang) - YouTube talk, EUC 2016 slides
- **NOTES.md**: "RBILT needs fixed size elements" → Use dots (12 bytes: ActorId + counter)
