use bigsets::config::StorageConfig;
use bigsets::types::ActorId;
use bigsets::{Server, SqliteStorage};
use bytes::Bytes;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_server_sadd_returns_operation() {
    // Create temp storage
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("test.db");
    let config = StorageConfig {
        sqlite_cache_size: 1000,
        sqlite_busy_timeout: 5000,
    };

    let storage = Arc::new(SqliteStorage::open(&db_path, &config).unwrap());

    // Create server
    let actor_id = ActorId::new(1, 0);
    let server = Server::new(actor_id, storage).await.unwrap();

    // Call SADD
    let members = vec![Bytes::from("foo"), Bytes::from("bar")];
    let (result, operation) = server.sadd("myset", &members).await.unwrap();

    // Verify we got a result
    println!("Result: {:?}", result);

    // Verify we got an operation for replication
    assert!(
        operation.is_some(),
        "SADD should return an operation for replication"
    );

    let op = operation.unwrap();
    println!(
        "Operation: set_name={}, op_type={:?}",
        op.set_name, op.op_type
    );

    assert_eq!(op.set_name, "myset");

    // Check it's an Add operation
    match op.op_type {
        bigsets::types::OpType::Add { elements, dot, .. } => {
            assert_eq!(elements.len(), 2);
            assert!(elements.contains(&Bytes::from("foo")));
            assert!(elements.contains(&Bytes::from("bar")));
            assert_eq!(dot.actor_id, actor_id);
            println!("✓ Operation contains correct elements and dot");
        }
        _ => panic!("Expected Add operation"),
    }
}

#[tokio::test]
async fn test_server_apply_remote_operation() {
    // Create two servers
    let temp1 = TempDir::new().unwrap();
    let temp2 = TempDir::new().unwrap();
    let config = StorageConfig {
        sqlite_cache_size: 1000,
        sqlite_busy_timeout: 5000,
    };

    let storage1 = Arc::new(SqliteStorage::open(&temp1.path().join("node1.db"), &config).unwrap());
    let storage2 = Arc::new(SqliteStorage::open(&temp2.path().join("node2.db"), &config).unwrap());

    let server1 = Server::new(ActorId::new(1, 0), storage1).await.unwrap();
    let server2 = Server::new(ActorId::new(2, 0), storage2).await.unwrap();

    // Server 1: SADD
    let members = vec![Bytes::from("foo"), Bytes::from("bar")];
    let (_result, operation) = server1.sadd("myset", &members).await.unwrap();

    let op = operation.expect("Should have operation");
    println!("Server 1 created operation for replication");

    // Server 2: Apply the remote operation
    let apply_result = server2.apply_remote_operation(op).await;
    println!("Server 2 apply result: {:?}", apply_result);

    assert!(
        apply_result.is_ok(),
        "Should successfully apply remote operation"
    );

    // Verify Server 2 now has the data
    let members_result = server2.smembers("myset", None).await.unwrap();

    match members_result {
        bigsets::server::CommandResult::BytesArray(bytes) => {
            println!("Server 2 members: {:?}", bytes);
            assert_eq!(bytes.len(), 2);
            assert!(bytes.contains(&Bytes::from("foo")));
            assert!(bytes.contains(&Bytes::from("bar")));
            println!("✓ Remote operation successfully replicated!");
        }
        _ => panic!("Expected BytesArray result"),
    }
}
