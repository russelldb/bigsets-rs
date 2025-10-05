# BigSets-RS Architecture

## Overview

BigSets-RS is a distributed CRDT-based set database inspired by the 2016 BigSets paper, with significant architectural improvements based on lessons learned. It provides eventually consistent, partition-tolerant set operations with causal consistency guarantees.

### Key Design Principles

1. **No tombstone clock** - Direct element deletion instead of tombstone tracking
2. **Compact version vectors** - No dot clouds, just version vectors for causal consistency
3. **Simple causal delivery** - In-memory buffers for reordering, RBILT for anti-entropy
4. **Durable by default** - SQLite WAL provides restart safety
5. **Redis-compatible API** - Use existing Redis clients and tools

## Architecture Components

```
┌─────────────────────────────────────────────────┐
│                  Client Layer                    │
│  (Redis clients - standard or VV-aware)         │
└─────────────────────────────────────────────────┘
                        │
                        ↓ RESP Protocol
┌─────────────────────────────────────────────────┐
│              API Server (per node)               │
│  - RESP protocol handler                         │
│  - Redis subset: SADD, SREM, SMEMBERS, etc      │
│  - VV-aware reads (optional context)            │
│  - Immediate local ACK                          │
└─────────────────────────────────────────────────┘
                        │
                        ↓
┌─────────────────────────────────────────────────┐
│           BigSet Core (per node)                │
│  - Dot generation (actor_id, counter++)         │
│  - ORSWOT semantics                             │
│  - Version vector maintenance                   │
│  - SQLite storage                               │
└─────────────────────────────────────────────────┘
                        │
        ┌───────────────┴───────────────┐
        ↓                               ↓
┌──────────────────┐          ┌──────────────────┐
│ Replication      │          │   SQLite         │
│ Module           │          │   Storage        │
│                  │          │                  │
│ - Sender buffer  │          │ - Sets table     │
│ - Receiver buffer│          │ - Elements table │
│ - ACK/retry      │          │ - Dots table     │
│ - RBILT fallback │          │ - VV table       │
└──────────────────┘          └──────────────────┘
        │
        ↓ Inter-replica protocol
┌─────────────────────────────────────────────────┐
│          Peer Replication Servers                │
│         (all-to-all static topology)            │
└─────────────────────────────────────────────────┘
```

## Data Model

### SQLite Schema

```sql
-- Sets namespace
CREATE TABLE sets (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

-- Version vector per set (critical for causal consistency)
CREATE TABLE version_vectors (
    set_id INTEGER NOT NULL,
    actor_id TEXT NOT NULL,
    counter INTEGER NOT NULL,
    PRIMARY KEY (set_id, actor_id),
    FOREIGN KEY (set_id) REFERENCES sets(id) ON DELETE CASCADE
);

-- Unique element values (deduplication)
CREATE TABLE elements (
    id INTEGER PRIMARY KEY,
    set_id INTEGER NOT NULL,
    value BLOB NOT NULL,  -- Binary-safe like Redis, lexicographic ordering
    FOREIGN KEY (set_id) REFERENCES sets(id) ON DELETE CASCADE,
    UNIQUE (set_id, value)
);

-- Dots pointing to elements (ORSWOT: multiple concurrent adds)
CREATE TABLE dots (
    element_id INTEGER NOT NULL,
    actor_id TEXT NOT NULL,
    counter INTEGER NOT NULL,
    PRIMARY KEY (element_id, actor_id, counter),
    FOREIGN KEY (element_id) REFERENCES elements(id) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX idx_elements_set_value ON elements(set_id, value);
CREATE INDEX idx_dots_element ON dots(element_id);
```

### ORSWOT Semantics

- **Add element E with dot D**:
  - Insert E into elements (if not exists)
  - Insert D into dots table for E
  - Multiple dots per element = concurrent adds preserved

- **Remove element E (observed remove)**:
  - Delete E from elements
  - Cascade delete all dots for E
  - Replication carries removed dots to handle concurrent add/remove

- **Version Vector**:
  - Tracks causal context
  - Dot assignment: `(actor_id, vv[actor_id]++)`
  - Stored in SQLite = durable across restarts

## API Layer

### RESP Protocol

Standard Redis Serialization Protocol with optional VV extensions.

#### Basic Commands (Standard Redis)

```bash
# Add element (immediate local ACK)
> SADD SetB Tree
"+OK vv:A:5,B:3,C:2\r\n"

# Remove element
> SREM SetB Oak
"+OK vv:A:6,B:3,C:2\r\n"

# Check membership (eventual consistency, no VV)
> SISMEMBER SetB Tree
:1\r\n

# Get cardinality
> SCARD SetB
:2\r\n
```

#### VV-Aware Extensions

```bash
# Read with causal context
> SISMEMBER SetB Tree vv:A:5,B:3,C:2

# Case 1: Server VV dominates client VV (safe to read)
:1\r\n
+vv:A:6,B:3,C:2\r\n

# Case 2: Server VV doesn't dominate (not ready)
-NOTREADY vv:A:4,B:3,C:2\r\n

# Multi-member check with causal context
> SMISMEMBER SetB Tree Oak Maple vv:A:5,B:3,C:2
*3\r\n
:1\r\n
:0\r\n
:1\r\n
+vv:A:6,B:3,C:2\r\n
```

**Client behavior:**
- No VV context → immediate read (eventual consistency)
- With VV context → causal read (may get NOTREADY, retry with backoff)

#### Supported Commands (Minimal Subset)

- `SADD key member [member ...]` - Add one or more members
- `SREM key member [member ...]` - Remove one or more members
- `SCARD key [vv:...]` - Get cardinality (count)
- `SISMEMBER key member [vv:...]` - Check if member exists (returns 0 or 1)
- `SMISMEMBER key member [member ...] [vv:...]` - Check multiple members (returns array of 0/1)

## Replication Protocol

### Operation Format

```rust
struct Operation {
    set_id: u64,
    op_type: OpType,
    timestamp: VersionVector,  // VV after this op
}

enum OpType {
    Add {
        element: String,
        dot: Dot,
        removed_dots: Vec<Dot>,  // Concurrent removes we observed
    },
    Remove {
        element: String,
        removed_dots: Vec<Dot>,  // All dots we're removing
    },
}

struct Dot {
    actor_id: String,
    counter: u64,
}
```

### Sender Side

**On local write:**
1. Generate dot: `(self.actor_id, self.vv[self.actor_id] + 1)`
2. Apply to local SQLite
3. Update version vector
4. Broadcast `Operation` to all peers
5. Add to `unacked` buffer with timestamp

**Unacked buffer management:**
```rust
struct UnackedBuffer {
    ops: HashMap<PeerId, Vec<(Operation, Instant, retry_count)>>,
}

// On timer tick:
for (peer_id, ops) in unacked.iter_mut() {
    for (op, sent_at, retries) in ops {
        if now() - sent_at > timeout(*retries) {
            if *retries < MAX_RETRIES {
                resend(op, peer_id);
                *retries += 1;
                *sent_at = now();  // exponential backoff: 100ms, 200ms, 400ms...
            } else {
                // Give up, trigger RBILT
                trigger_rbilt(peer_id);
                ops.clear();
            }
        }
    }
}

// On ACK received from peer:
unacked[peer_id].retain(|op| op.id != ack.op_id);
```

### Receiver Side

**On operation received:**
1. Check if causally ready: `op.timestamp[sender] == local_vv[sender] + 1 && op.timestamp[others] <= local_vv[others]`
2. If ready:
   - Send ACK to sender
   - Apply operation to SQLite
   - Update local VV
   - Recursively check pending buffer for newly unblocked ops
3. If not ready:
   - Add to pending buffer
   - If buffer size > MAX_BUFFER_SIZE: trigger RBILT, clear buffer

**Pending buffer:**
```rust
struct PendingBuffer {
    ops: Vec<Operation>,
    max_size: usize,  // config: e.g., 1000
}

fn try_apply_pending(&mut self) -> Vec<Operation> {
    let mut applied = vec![];
    let mut made_progress = true;

    while made_progress {
        made_progress = false;
        self.ops.retain(|op| {
            if is_causally_ready(op, &self.local_vv) {
                apply_operation(op);
                applied.push(op.clone());
                made_progress = true;
                false  // remove from buffer
            } else {
                true  // keep in buffer
            }
        });
    }

    applied
}
```

### RBILT Anti-Entropy

**Trigger conditions:**
- Startup (in-memory buffers lost)
- Sender exhausts retries
- Receiver buffer overflow

**Protocol:**
1. Exchange set element digests
2. Rateless encoding: generate universal coded symbols
3. Decode differences (additions/removals on both sides)
4. Apply missing operations
5. Update version vector to union of both VVs
6. Clear sender unacked + receiver pending buffers

## Configuration

### config.toml

```toml
[server]
actor_id = "node-1"              # This replica's unique ID
api_addr = "127.0.0.1:6379"      # Redis-compatible API endpoint
replication_addr = "127.0.0.1:7379"  # Inter-replica communication
db_path = "./data/node-1.db"    # SQLite database path

[cluster]
# Static membership for first cut
replicas = [
    { id = "node-1", addr = "127.0.0.1:7379" },
    { id = "node-2", addr = "127.0.0.1:7380" },
    { id = "node-3", addr = "127.0.0.1:7381" },
]

[replication]
max_retries = 5                  # Before triggering RBILT
retry_backoff_ms = 100           # Initial backoff (exponential)
buffer_size = 1000               # Receiver buffer before RBILT
ack_timeout_ms = 500             # Initial ACK timeout
rbilt_startup_delay_ms = 1000    # Wait before startup RBILT

[storage]
sqlite_cache_size = 10000        # SQLite page cache
sqlite_busy_timeout = 5000       # Busy timeout in ms
```

## Implementation Phases

### Phase 1: Core Foundation
- [ ] Config parsing and validation
- [ ] SQLite schema setup and migrations
- [ ] Core data structures (VersionVector, Dot, Operation)
- [ ] Basic ORSWOT operations (add, remove, query)

### Phase 2: API Server
- [ ] RESP protocol parser
- [ ] Command handlers (SADD, SREM, SMEMBERS, etc.)
- [ ] VV-aware read logic (domination check)
- [ ] Response serialization

### Phase 3: Replication
- [ ] Operation serialization (protobuf/bincode)
- [ ] Sender: broadcast, unacked buffer, retry logic
- [ ] Receiver: pending buffer, causal delivery
- [ ] ACK protocol

### Phase 4: RBILT Integration
- [ ] RBILT implementation (from paper)
- [ ] Trigger logic (startup, retries, overflow)
- [ ] Set reconciliation protocol
- [ ] VV merging after sync

### Phase 5: Testing & Optimization
- [ ] Integration tests with redis-cli
- [ ] Chaos testing (message loss, reordering, partitions)
- [ ] Performance benchmarks
- [ ] Monitoring and observability

## Open Questions

1. **Serialization format**: Protobuf, bincode, or custom binary?
2. **RBILT implementation**: Pure Rust or bind to existing C library?
3. **Multi-set operations**: How to handle SINTER/SUNION across replicas?
4. **Compression**: Should we compress elements/operations?
5. **Metrics**: What observability do we need? (Prometheus? OpenTelemetry?)

## Future Extensions

- Dynamic membership (join/leave protocol)
- Partitioning/sharding (random slicing)
- Rich element types (JSON, binary blobs with schema)
- Delta-state hybrid (for bulk sync)
- Snapshot isolation for multi-key operations
- Read-only replicas
- Quorum reads/writes (R+W=N tuning)
