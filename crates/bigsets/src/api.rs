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
        info!("API server listening on {}", self.server.config.server.api_addr);

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

    // Get or create set
    let set_id = match crate::orswot::get_or_create_set(server.db.read().await.pool(), &key_name) {
        Ok(id) => id,
        Err(e) => return RespValue::Error(format!("ERR database error: {}", e)),
    };

    // Generate dot and add elements
    let mut vv = server.version_vector.write().await;
    let dot = vv.increment(&server.config.server.actor_id);

    if let Err(e) = crate::orswot::add_elements(server.db.read().await.pool(), set_id, members, &dot) {
        return RespValue::Error(format!("ERR database error: {}", e));
    }

    // Save VV to database
    if let Err(e) = crate::orswot::save_version_vector(server.db.read().await.pool(), set_id, &vv) {
        return RespValue::Error(format!("ERR database error: {}", e));
    }

    let vv_str = vv.to_string();
    drop(vv);

    debug!(
        "SADD key={} members={} dot={:?}",
        key_name,
        members.len(),
        dot
    );

    RespValue::SimpleString(format!("OK vv:{}", vv_str))
}

async fn cmd_srem(server: &Arc<Server>, parts: &[Bytes]) -> RespValue {
    if parts.len() < 3 {
        return RespValue::Error("ERR wrong number of arguments for 'srem' command".to_string());
    }

    let key_name = String::from_utf8_lossy(&parts[1]).to_string();
    let members = &parts[2..];

    // Get or create set
    let set_id = match crate::orswot::get_or_create_set(server.db.read().await.pool(), &key_name) {
        Ok(id) => id,
        Err(e) => return RespValue::Error(format!("ERR database error: {}", e)),
    };

    // Remove elements and get their dots
    let _removed_dots = match crate::orswot::remove_elements(server.db.read().await.pool(), set_id, members) {
        Ok(dots) => dots,
        Err(e) => return RespValue::Error(format!("ERR database error: {}", e)),
    };

    // Generate dot for remove operation (causality only)
    let mut vv = server.version_vector.write().await;
    let dot = vv.increment(&server.config.server.actor_id);

    // Save VV to database
    if let Err(e) = crate::orswot::save_version_vector(server.db.read().await.pool(), set_id, &vv) {
        return RespValue::Error(format!("ERR database error: {}", e));
    }

    let vv_str = vv.to_string();
    drop(vv);

    debug!(
        "SREM key={} members={} dot={:?}",
        key_name,
        members.len(),
        dot
    );

    RespValue::SimpleString(format!("OK vv:{}", vv_str))
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

    // Check if we can serve this read
    if let Some(cv) = client_vv {
        let local_vv = server.version_vector.read().await;
        if !local_vv.dominates(&cv) {
            return RespValue::Error(format!("NOTREADY vv:{}", local_vv.to_string()));
        }
    }

    // Get set ID (return 0 if doesn't exist)
    let set_id = match crate::orswot::get_or_create_set(server.db.read().await.pool(), &key_name) {
        Ok(id) => id,
        Err(e) => return RespValue::Error(format!("ERR database error: {}", e)),
    };

    // Get cardinality
    let count = match crate::orswot::cardinality(server.db.read().await.pool(), set_id) {
        Ok(c) => c,
        Err(e) => return RespValue::Error(format!("ERR database error: {}", e)),
    };

    RespValue::Integer(count)
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

    // Check if we can serve this read
    if let Some(cv) = client_vv {
        let local_vv = server.version_vector.read().await;
        if !local_vv.dominates(&cv) {
            return RespValue::Error(format!("NOTREADY vv:{}", local_vv.to_string()));
        }
    }

    // Get set ID
    let set_id = match crate::orswot::get_or_create_set(server.db.read().await.pool(), &key_name) {
        Ok(id) => id,
        Err(e) => return RespValue::Error(format!("ERR database error: {}", e)),
    };

    // Check membership
    let is_member = match crate::orswot::is_member(server.db.read().await.pool(), set_id, member) {
        Ok(exists) => exists,
        Err(e) => return RespValue::Error(format!("ERR database error: {}", e)),
    };

    RespValue::Integer(if is_member { 1 } else { 0 })
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

    // Check if we can serve this read
    if let Some(cv) = client_vv {
        let local_vv = server.version_vector.read().await;
        if !local_vv.dominates(&cv) {
            return RespValue::Error(format!("NOTREADY vv:{}", local_vv.to_string()));
        }
    }

    // Get set ID
    let set_id = match crate::orswot::get_or_create_set(server.db.read().await.pool(), &key_name) {
        Ok(id) => id,
        Err(e) => return RespValue::Error(format!("ERR database error: {}", e)),
    };

    // Check membership for all elements
    let membership = match crate::orswot::are_members(server.db.read().await.pool(), set_id, members) {
        Ok(results) => results,
        Err(e) => return RespValue::Error(format!("ERR database error: {}", e)),
    };

    let results: Vec<RespValue> = membership
        .iter()
        .map(|&is_member| RespValue::Integer(if is_member { 1 } else { 0 }))
        .collect();

    RespValue::Array(results)
}
