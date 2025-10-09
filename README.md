# BigSets-RS

A Rust implementation of a distributed CRDT-based set database with add-wins semantics.

This is a reimplementation of the original [BigSets (Erlang)](https://github.com/russelldb/bigsets-2) project.

## Features

- **CRDT Add-Wins Set**: Conflict-free replicated data type with add-wins semantics
- **SQLite Storage**: Persistent storage
- **Redis-compatible API**: RESP protocol support for familiar commands (SADD, SREM, SCARD, etc.)
- **Multi-node Replication**: Designed for cluster deployment

## Development

### Running a Single Node

```bash
cargo run --bin bigsets-server config.toml
```

### Running Multiple Nodes Locally

The `bigsets-dev` binary makes it easy to run a local cluster for testing:

```bash
# Run 3 nodes (default)
cargo run --bin bigsets-dev

# Run 5 nodes
cargo run --bin bigsets-dev -- -n 5

# Run 2 nodes with persistent data
cargo run --bin bigsets-dev -- -n 2 --keep-data

# Show help
cargo run --bin bigsets-dev -- --help
```

**Port allocation:**
- Node 1: API=6379, Replication=7379
- Node 2: API=6380, Replication=7380
- Node 3: API=6381, Replication=7381
- etc.

**Data storage:**
- Default: Temporary directories (auto-cleanup on exit)
- With `--keep-data`: `./data/dev-node-{N}.db`

### Testing

```bash
cargo test --package bigsets --lib
```

## Configuration

Example `config.toml`:

```toml
[server]
node_id = 1
# epoch = 0  # Optional, defaults to 0
api_addr = "127.0.0.1:6379"
replication_addr = "127.0.0.1:7379"
db_path = "./data/node-1.db"

[cluster]
replicas = [
    { node_id = 1, addr = "127.0.0.1:7379" },
    { node_id = 2, addr = "127.0.0.1:7380" },
    { node_id = 3, addr = "127.0.0.1:7381" },
]

[replication]
max_retries = 5
retry_backoff_ms = 100
buffer_size = 1000
ack_timeout_ms = 500
rbilt_startup_delay_ms = 1000

[storage]
sqlite_cache_size = 10000
sqlite_busy_timeout = 5000
```

## Architecture

See [ARCHITECTURE.md](./ARCHITECTURE.md)

## License

MIT OR Apache-2.0
