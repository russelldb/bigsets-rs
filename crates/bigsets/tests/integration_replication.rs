use bigsets::config::{ReplicaInfo, ReplicationConfig, StorageConfig};
use bigsets::types::ActorId;
use bigsets::{
    ApiServer, ReplicationManager, ReplicationServer, Server, ServerWrapper, SqliteStorage,
};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

/// Integration test: Start 3 nodes, write to one, verify replication to others
#[tokio::test]
async fn test_three_node_replication() {
    // Initialize logging for test output
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();

    // Create temp directories for each node
    let temp1 = TempDir::new().unwrap();
    let temp2 = TempDir::new().unwrap();
    let temp3 = TempDir::new().unwrap();

    let db1 = temp1.path().join("node.db");
    let db2 = temp2.path().join("node.db");
    let db3 = temp3.path().join("node.db");

    // Define node addresses
    let api_addrs = ["127.0.0.1:16379", "127.0.0.1:16380", "127.0.0.1:16381"];
    let repl_addrs = ["127.0.0.1:17379", "127.0.0.1:17380", "127.0.0.1:17381"];

    // Create replica info for all nodes
    let replicas = vec![
        ReplicaInfo {
            node_id: 1,
            epoch: 0,
            addr: repl_addrs[0].to_string(),
        },
        ReplicaInfo {
            node_id: 2,
            epoch: 0,
            addr: repl_addrs[1].to_string(),
        },
        ReplicaInfo {
            node_id: 3,
            epoch: 0,
            addr: repl_addrs[2].to_string(),
        },
    ];

    let storage_config = StorageConfig {
        sqlite_cache_size: 1000,
        sqlite_busy_timeout: 5000,
    };

    let repl_config = ReplicationConfig {
        max_retries: 5,
        retry_backoff_ms: 100,
        buffer_size: 1000,
        ack_timeout_ms: 500,
        rbilt_startup_delay_ms: 100,
    };

    // Helper to start a node
    let start_node = |node_id: u16,
                      db_path: std::path::PathBuf,
                      api_addr: &str,
                      repl_addr: &str| {
        let replicas = replicas.clone();
        let storage_config = storage_config.clone();
        let repl_config = repl_config.clone();
        let api_addr = api_addr.to_string();
        let repl_addr = repl_addr.to_string();

        tokio::spawn(async move {
            tracing::info!("Starting node {}", node_id);

            // Create storage
            let storage = Arc::new(SqliteStorage::open(&db_path, &storage_config).unwrap());

            // Create server
            let actor_id = ActorId::new(node_id, 0);
            let server = Arc::new(Server::new(actor_id, Arc::clone(&storage)).await.unwrap());

            // Create peers list (exclude self)
            let peers: Vec<_> = replicas
                .iter()
                .filter(|r| r.node_id != node_id)
                .cloned()
                .collect();

            tracing::info!(
                "Node {} has {} peers: {:?}",
                node_id,
                peers.len(),
                peers.iter().map(|p| &p.addr).collect::<Vec<_>>()
            );

            // Create replication manager
            let replication = Arc::new(ReplicationManager::new(peers, repl_config.buffer_size));

            // Create wrapper
            let wrapper = Arc::new(ServerWrapper::new(
                Arc::clone(&server),
                Arc::clone(&replication),
            ));

            // Start API server
            let api_server = ApiServer::new(Arc::clone(&wrapper), api_addr.clone());
            let api_handle = tokio::spawn(async move {
                if let Err(e) = api_server.run().await {
                    tracing::error!("Node {} API server error: {}", node_id, e);
                }
            });

            // Start replication server
            let replication_server =
                ReplicationServer::new(Arc::clone(&server), Arc::clone(&replication), repl_addr);
            let repl_handle = tokio::spawn(async move {
                if let Err(e) = replication_server.run().await {
                    tracing::error!("Node {} replication server error: {}", node_id, e);
                }
            });

            // Wait for both servers
            let _ = tokio::try_join!(api_handle, repl_handle);
        })
    };

    // Start all three nodes
    let handle1 = start_node(1, db1, api_addrs[0], repl_addrs[0]);
    let handle2 = start_node(2, db2, api_addrs[1], repl_addrs[1]);
    let handle3 = start_node(3, db3, api_addrs[2], repl_addrs[2]);

    // Give nodes time to start up
    sleep(Duration::from_secs(1)).await;

    tracing::info!("All nodes started, testing replication...");

    // Helper to run redis-cli command
    let redis_cli = |port: u16, args: &[&str]| -> String {
        let output = Command::new("redis-cli")
            .arg("-p")
            .arg(port.to_string())
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .expect("Failed to execute redis-cli");

        String::from_utf8_lossy(&output.stdout).trim().to_string()
    };

    // Test 1: SADD on node 1
    tracing::info!("Test 1: SADD on node 1");
    let result = redis_cli(16379, &["SADD", "myset", "foo", "bar", "baz"]);
    tracing::info!("SADD result: {}", result);
    assert!(result.contains("OK"), "SADD should return OK");

    // Wait for replication
    sleep(Duration::from_millis(500)).await;

    // Test 2: Check node 1 has the data
    tracing::info!("Test 2: SMEMBERS on node 1");
    let members1 = redis_cli(16379, &["SMEMBERS", "myset"]);
    tracing::info!("Node 1 members: {}", members1);
    assert!(members1.contains("foo"));
    assert!(members1.contains("bar"));
    assert!(members1.contains("baz"));

    // Test 3: Check node 2 has the data (replicated)
    tracing::info!("Test 3: SMEMBERS on node 2");
    let members2 = redis_cli(16380, &["SMEMBERS", "myset"]);
    tracing::info!("Node 2 members: {}", members2);
    assert!(
        members2.contains("foo"),
        "Node 2 should have replicated data"
    );
    assert!(
        members2.contains("bar"),
        "Node 2 should have replicated data"
    );
    assert!(
        members2.contains("baz"),
        "Node 2 should have replicated data"
    );

    // Test 4: Check node 3 has the data (replicated)
    tracing::info!("Test 4: SMEMBERS on node 3");
    let members3 = redis_cli(16381, &["SMEMBERS", "myset"]);
    tracing::info!("Node 3 members: {}", members3);
    assert!(
        members3.contains("foo"),
        "Node 3 should have replicated data"
    );
    assert!(
        members3.contains("bar"),
        "Node 3 should have replicated data"
    );
    assert!(
        members3.contains("baz"),
        "Node 3 should have replicated data"
    );

    // Test 5: SREM on node 2
    tracing::info!("Test 5: SREM on node 2");
    let result = redis_cli(16380, &["SREM", "myset", "bar"]);
    tracing::info!("SREM result: {}", result);
    assert!(result.contains("OK"), "SREM should return OK");

    // Wait for replication
    sleep(Duration::from_millis(500)).await;

    // Test 6: Verify removal replicated to all nodes
    tracing::info!("Test 6: Verify removal on all nodes");
    let members1 = redis_cli(16379, &["SMEMBERS", "myset"]);
    let members2 = redis_cli(16380, &["SMEMBERS", "myset"]);
    let members3 = redis_cli(16381, &["SMEMBERS", "myset"]);

    tracing::info!("After SREM - Node 1: {}", members1);
    tracing::info!("After SREM - Node 2: {}", members2);
    tracing::info!("After SREM - Node 3: {}", members3);

    assert!(!members1.contains("bar"), "Node 1 should not have 'bar'");
    assert!(!members2.contains("bar"), "Node 2 should not have 'bar'");
    assert!(!members3.contains("bar"), "Node 3 should not have 'bar'");
    assert!(members1.contains("foo") && members1.contains("baz"));
    assert!(members2.contains("foo") && members2.contains("baz"));
    assert!(members3.contains("foo") && members3.contains("baz"));

    tracing::info!("All replication tests passed!");

    // Cleanup: abort node tasks
    handle1.abort();
    handle2.abort();
    handle3.abort();

    // Temp directories will be cleaned up automatically
}
