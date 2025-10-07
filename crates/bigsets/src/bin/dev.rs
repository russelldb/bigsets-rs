use bigsets::{
    ApiServer, ReplicationManager, ReplicationServer, Server, ServerWrapper, SqliteStorage,
    config::{ClusterConfig, Config, ReplicaInfo, ReplicationConfig, ServerConfig, StorageConfig},
};
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(author, version, about = "Run multiple BigSets nodes locally for development", long_about = None)]
struct Args {
    /// Number of nodes to start
    #[arg(short, long, default_value = "3")]
    nodes: u16,

    /// Data directory root (each node gets a subdirectory)
    #[arg(short, long)]
    data_dir: Option<PathBuf>,
}

struct NodeSetup {
    config: Config,
    _temp_dir: Option<TempDir>, // Keep alive to prevent cleanup
}

fn generate_node_configs(num_nodes: u16, data_dir: Option<PathBuf>) -> Vec<NodeSetup> {
    let mut configs = Vec::new();

    // Generate replica list for cluster config (all nodes)
    let replicas: Vec<ReplicaInfo> = (1..=num_nodes)
        .map(|i| ReplicaInfo {
            node_id: i,
            epoch: 0,
            addr: format!("127.0.0.1:{}", 7379 + i - 1),
        })
        .collect();

    // Default replication and storage configs
    let replication_config = ReplicationConfig {
        max_retries: 5,
        retry_backoff_ms: 100,
        buffer_size: 1000,
        ack_timeout_ms: 500,
        rbilt_startup_delay_ms: 1000,
    };

    let storage_config = StorageConfig {
        sqlite_cache_size: 10000,
        sqlite_busy_timeout: 5000,
    };

    // Generate config for each node
    for node_id in 1..=num_nodes {
        let (db_path, temp_dir) = if let Some(ref base_dir) = data_dir {
            // Use data_dir/node_id/ subdirectory
            let node_dir = base_dir.join(format!("{}", node_id));
            let path = node_dir.join("node.db");
            (path, None)
        } else {
            // Use temporary directory
            let temp_dir = TempDir::new().expect("Failed to create temp directory");
            let path = temp_dir.path().join("node.db");
            (path, Some(temp_dir))
        };

        let server_config = ServerConfig {
            node_id,
            epoch: 0,
            api_addr: format!("127.0.0.1:{}", 6379 + node_id - 1),
            replication_addr: format!("127.0.0.1:{}", 7379 + node_id - 1),
            db_path,
        };

        let config = Config {
            server: server_config,
            cluster: ClusterConfig {
                replicas: replicas.clone(),
            },
            replication: replication_config.clone(),
            storage: storage_config.clone(),
        };

        configs.push(NodeSetup {
            config,
            _temp_dir: temp_dir,
        });
    }

    configs
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args = Args::parse();

    if args.nodes == 0 {
        eprintln!("Error: Number of nodes must be at least 1");
        std::process::exit(1);
    }

    info!("Starting {}-node local cluster...", args.nodes);

    // Create data directories if needed
    if let Some(ref data_dir) = args.data_dir {
        for node_id in 1..=args.nodes {
            let node_dir = data_dir.join(format!("{}", node_id));
            std::fs::create_dir_all(&node_dir)?;
        }
    }

    // Generate configurations for all nodes
    let node_setups = generate_node_configs(args.nodes, args.data_dir);

    // Print node information
    for setup in &node_setups {
        info!(
            "Node {}: API={}, Replication={}, DB={:?}",
            setup.config.server.node_id,
            setup.config.server.api_addr,
            setup.config.server.replication_addr,
            setup.config.server.db_path
        );
    }

    // Start all nodes as separate tokio tasks
    let mut tasks = Vec::new();

    for setup in &node_setups {
        let config = setup.config.clone();
        let node_id = config.server.node_id;

        let task = tokio::spawn(async move {
            // Create storage
            let storage = match SqliteStorage::open(&config.server.db_path, &config.storage) {
                Ok(s) => Arc::new(s),
                Err(e) => {
                    error!("Failed to create storage for node {}: {}", node_id, e);
                    return;
                }
            };

            // Create server
            let server = match Server::new(config.server.actor_id(), Arc::clone(&storage)).await {
                Ok(s) => Arc::new(s),
                Err(e) => {
                    error!("Failed to create server for node {}: {}", node_id, e);
                    return;
                }
            };

            // Create replication manager
            let peers: Vec<_> = config
                .cluster
                .replicas
                .iter()
                .filter(|r| r.actor_id() != config.server.actor_id())
                .cloned()
                .collect();
            tracing::info!("Node {} configured with {} peers", node_id, peers.len());
            let replication = Arc::new(ReplicationManager::new(
                peers,
                config.replication.buffer_size,
            ));

            // Create wrapper
            let wrapper = Arc::new(ServerWrapper::new(
                Arc::clone(&server),
                Arc::clone(&replication),
            ));

            // Start API server
            let api_server = ApiServer::new(Arc::clone(&wrapper), config.server.api_addr.clone());
            let api_handle = tokio::spawn(async move {
                if let Err(e) = api_server.run().await {
                    error!("API server error: {}", e);
                }
            });

            // Start replication server
            let replication_server = ReplicationServer::new(
                Arc::clone(&server),
                Arc::clone(&replication),
                config.server.replication_addr.clone(),
            );
            let repl_handle = tokio::spawn(async move {
                if let Err(e) = replication_server.run().await {
                    error!("Replication server error: {}", e);
                }
            });

            // Wait for both servers
            if let Err(e) = tokio::try_join!(api_handle, repl_handle) {
                error!("Node {} error: {}", node_id, e);
            }
        });

        tasks.push(task);
    }

    // Keep node_setups alive to prevent temp directories from being deleted
    let _keep_alive = node_setups;

    info!("All nodes started. Press Ctrl+C to stop.");

    // Wait for Ctrl+C or any task to complete
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl+C, shutting down...");
        }
        result = async {
            for task in tasks {
                if let Err(e) = task.await {
                    error!("Task error: {}", e);
                }
            }
        } => {
            info!("All tasks completed");
            result
        }
    }

    info!("Shutdown complete");
    Ok(())
}
