use crate::commands::{self, CommandResult};
use crate::resp::{RespError, RespValue};
use crate::server::Server;
use crate::types::VersionVector;
use bytes::{Buf, Bytes, BytesMut};
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

pub struct ApiServer {
    server: Arc<Server>,
}

impl ApiServer {
    pub fn new(server: Arc<Server>) -> Self {
        Self { server }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&self.server.config.server.api_addr).await?;
        info!(
            "API server listening on {}",
            self.server.config.server.api_addr
        );

        loop {
            let (socket, addr) = listener.accept().await?;
            debug!("New connection from {}", addr);

            let server = Arc::clone(&self.server);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, server).await {
                    error!("Connection error: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(
    mut socket: TcpStream,
    server: Arc<Server>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = BytesMut::with_capacity(4096);

    loop {
        // Read from socket
        let n = socket.read_buf(&mut buffer).await?;
        if n == 0 {
            debug!("Connection closed");
            return Ok(());
        }

        // Try to parse RESP command
        let mut cursor = Cursor::new(&buffer[..]);
        match RespValue::parse(&mut cursor) {
            Ok(value) => {
                let pos = cursor.position() as usize;
                buffer.advance(pos);

                // Process command
                let response = process_command(&server, value).await;

                // Serialize and send response
                let mut response_buf = BytesMut::new();
                response.serialize(&mut response_buf);
                socket.write_all(&response_buf).await?;
            }
            Err(RespError::Incomplete) => {
                // Need more data
                continue;
            }
            Err(e) => {
                error!("Protocol error: {}", e);
                let response = RespValue::Error(format!("ERR {}", e));
                let mut response_buf = BytesMut::new();
                response.serialize(&mut response_buf);
                socket.write_all(&response_buf).await?;
                return Ok(());
            }
        }
    }
}

async fn process_command(server: &Arc<Server>, value: RespValue) -> RespValue {
    // Extract command array
    let parts = match value.as_bulk_string_array() {
        Some(parts) if !parts.is_empty() => parts,
        _ => return RespValue::Error("ERR invalid command format".to_string()),
    };

    // Get command name (case-insensitive)
    let cmd = String::from_utf8_lossy(&parts[0]).to_uppercase();

    match cmd.as_str() {
        "SADD" => cmd_sadd(server, &parts).await,
        "SREM" => cmd_srem(server, &parts).await,
        "SCARD" => cmd_scard(server, &parts).await,
        "SISMEMBER" => cmd_sismember(server, &parts).await,
        "SMISMEMBER" => cmd_smismember(server, &parts).await,
        "SMEMBERS" => cmd_smembers(server, &parts).await,
        "PING" => RespValue::SimpleString("PONG".to_string()),
        _ => RespValue::Error(format!("ERR unknown command '{}'", cmd)),
    }
}

async fn cmd_sadd(server: &Arc<Server>, parts: &[Bytes]) -> RespValue {
    if parts.len() < 3 {
        return RespValue::Error("ERR wrong number of arguments for 'sadd' command".to_string());
    }

    let key_name = String::from_utf8_lossy(&parts[1]).to_string();
    let members = &parts[2..];

    // Execute command using extracted business logic
    let mut vv = server.version_vector.write().await;
    let result = commands::sadd(
        server.db.read().await.pool(),
        server.config.server.actor_id(),
        &mut *vv,
        &key_name,
        members,
    );

    match result {
        Ok(CommandResult::Ok {
            vv: Some(returned_vv),
            operation,
        }) => {
            let vv_str = returned_vv.to_string();
            debug!("SADD key={} members={}", key_name, members.len());
            // TODO: Send operation to replication if Some(operation)
            RespValue::SimpleString(format!("OK vv:{}", vv_str))
        }
        Ok(CommandResult::Error(msg)) => RespValue::Error(msg),
        Err(e) => RespValue::Error(format!("ERR database error: {}", e)),
        _ => RespValue::Error("ERR unexpected result".to_string()),
    }
}

async fn cmd_smembers(server: &Arc<Server>, parts: &[Bytes]) -> RespValue {
    if parts.len() < 2 {
        return RespValue::Error(
            "ERR wrong number of arguments for 'smembers' command".to_string(),
        );
    }

    let key_name = String::from_utf8_lossy(&parts[1]).to_string();

    // Optional client version vector for causality
    let client_vv = if parts.len() > 2 {
        let vv_str = String::from_utf8_lossy(&parts[2]);
        if let Some(vv_str) = vv_str.strip_prefix("vv:") {
            VersionVector::from_str(vv_str)
        } else {
            None
        }
    } else {
        None
    };

    let local_vv = server.version_vector.read().await;
    let result = commands::smembers(
        server.db.read().await.pool(),
        &*local_vv,
        &key_name,
        client_vv.as_ref(),
    );

    match result {
        Ok(CommandResult::BytesArray(members)) => {
            let results: Vec<RespValue> = members
                .iter()
                .map(|bytes| RespValue::BulkString(bytes.clone()))
                .collect();
            RespValue::Array(results)
        }
        Ok(CommandResult::NotReady(vv)) => {
            RespValue::Error(format!("NOTREADY vv:{}", vv.to_string()))
        }
        Ok(CommandResult::Error(msg)) => RespValue::Error(msg),
        Err(e) => RespValue::Error(format!("ERR database error: {}", e)),
        _ => RespValue::Error("ERR unexpected result".to_string()),
    }
}

async fn cmd_srem(server: &Arc<Server>, parts: &[Bytes]) -> RespValue {
    if parts.len() < 3 {
        return RespValue::Error("ERR wrong number of arguments for 'srem' command".to_string());
    }

    let key_name = String::from_utf8_lossy(&parts[1]).to_string();
    let members = &parts[2..];

    // Execute command using extracted business logic
    let mut vv = server.version_vector.write().await;
    let result = commands::srem(
        server.db.read().await.pool(),
        server.config.server.actor_id(),
        &mut *vv,
        &key_name,
        members,
    );

    match result {
        Ok(CommandResult::Ok {
            vv: Some(returned_vv),
            operation,
        }) => {
            let vv_str = returned_vv.to_string();
            debug!("SREM key={} members={}", key_name, members.len());
            // TODO: Send operation to replication if Some(operation)
            RespValue::SimpleString(format!("OK vv:{}", vv_str))
        }
        Ok(CommandResult::Error(msg)) => RespValue::Error(msg),
        Err(e) => RespValue::Error(format!("ERR database error: {}", e)),
        _ => RespValue::Error("ERR unexpected result".to_string()),
    }
}

async fn cmd_scard(server: &Arc<Server>, parts: &[Bytes]) -> RespValue {
    if parts.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'scard' command".to_string());
    }

    let key_name = String::from_utf8_lossy(&parts[1]).to_string();

    // Check for optional VV context
    let client_vv = if parts.len() > 2 {
        let vv_str = String::from_utf8_lossy(&parts[2]);
        if let Some(vv_str) = vv_str.strip_prefix("vv:") {
            VersionVector::from_str(vv_str)
        } else {
            None
        }
    } else {
        None
    };

    let local_vv = server.version_vector.read().await;
    let result = commands::scard(
        server.db.read().await.pool(),
        &*local_vv,
        &key_name,
        client_vv.as_ref(),
    );

    match result {
        Ok(CommandResult::Integer(count)) => RespValue::Integer(count),
        Ok(CommandResult::NotReady(vv)) => {
            RespValue::Error(format!("NOTREADY vv:{}", vv.to_string()))
        }
        Ok(CommandResult::Error(msg)) => RespValue::Error(msg),
        Err(e) => RespValue::Error(format!("ERR database error: {}", e)),
        _ => RespValue::Error("ERR unexpected result".to_string()),
    }
}

async fn cmd_sismember(server: &Arc<Server>, parts: &[Bytes]) -> RespValue {
    if parts.len() < 3 {
        return RespValue::Error(
            "ERR wrong number of arguments for 'sismember' command".to_string(),
        );
    }

    let key_name = String::from_utf8_lossy(&parts[1]).to_string();
    let member = &parts[2];

    // Check for optional VV context
    let client_vv = if parts.len() > 3 {
        let vv_str = String::from_utf8_lossy(&parts[3]);
        if let Some(vv_str) = vv_str.strip_prefix("vv:") {
            VersionVector::from_str(vv_str)
        } else {
            None
        }
    } else {
        None
    };

    let local_vv = server.version_vector.read().await;
    let result = commands::sismember(
        server.db.read().await.pool(),
        &*local_vv,
        &key_name,
        member,
        client_vv.as_ref(),
    );

    match result {
        Ok(CommandResult::Integer(val)) => RespValue::Integer(val),
        Ok(CommandResult::NotReady(vv)) => {
            RespValue::Error(format!("NOTREADY vv:{}", vv.to_string()))
        }
        Ok(CommandResult::Error(msg)) => RespValue::Error(msg),
        Err(e) => RespValue::Error(format!("ERR database error: {}", e)),
        _ => RespValue::Error("ERR unexpected result".to_string()),
    }
}

async fn cmd_smismember(server: &Arc<Server>, parts: &[Bytes]) -> RespValue {
    if parts.len() < 3 {
        return RespValue::Error(
            "ERR wrong number of arguments for 'smismember' command".to_string(),
        );
    }

    let key_name = String::from_utf8_lossy(&parts[1]).to_string();

    // Find where VV context starts (if present)
    let (members, client_vv) = {
        let mut member_end = parts.len();
        let mut vv = None;

        if let Some(last) = parts.last() {
            let last_str = String::from_utf8_lossy(last);
            if let Some(vv_str) = last_str.strip_prefix("vv:") {
                vv = VersionVector::from_str(vv_str);
                member_end = parts.len() - 1;
            }
        }

        (&parts[2..member_end], vv)
    };

    let local_vv = server.version_vector.read().await;
    let result = commands::smismember(
        server.db.read().await.pool(),
        &*local_vv,
        &key_name,
        members,
        client_vv.as_ref(),
    );

    match result {
        Ok(CommandResult::BoolArray(membership)) => {
            let results: Vec<RespValue> = membership
                .iter()
                .map(|&is_member| RespValue::Integer(if is_member { 1 } else { 0 }))
                .collect();
            RespValue::Array(results)
        }
        Ok(CommandResult::NotReady(vv)) => {
            RespValue::Error(format!("NOTREADY vv:{}", vv.to_string()))
        }
        Ok(CommandResult::Error(msg)) => RespValue::Error(msg),
        Err(e) => RespValue::Error(format!("ERR database error: {}", e)),
        _ => RespValue::Error("ERR unexpected result".to_string()),
    }
}
