use crate::resp::{RespError, RespValue};
use crate::server::CommandResult;
use crate::storage::Storage;
use crate::types::VersionVector;
use crate::wrapper::ServerWrapper;
use bytes::{Buf, Bytes, BytesMut};
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, trace};

/// API server handling RESP protocol over TCP
///
/// Receives Redis-protocol commands, calls ServerWrapper methods,
/// and returns RESP-formatted responses.
pub struct ApiServer<S: Storage> {
    wrapper: Arc<ServerWrapper<S>>,
    addr: String,
}

impl<S: Storage + 'static> ApiServer<S> {
    pub fn new(wrapper: Arc<ServerWrapper<S>>, addr: String) -> Self {
        Self { wrapper, addr }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&self.addr).await?;
        info!("API server listening on {}", self.addr);

        loop {
            let (socket, addr) = listener.accept().await?;
            debug!("New connection from {}", addr);

            let wrapper = Arc::clone(&self.wrapper);
            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(socket, wrapper).await {
                    error!("Connection error: {}", e);
                }
            });
        }
    }

    async fn handle_connection(
        mut socket: TcpStream,
        wrapper: Arc<ServerWrapper<S>>,
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
                    let response = Self::process_command(&wrapper, value).await;

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

    async fn process_command(wrapper: &Arc<ServerWrapper<S>>, value: RespValue) -> RespValue {
        // Extract command array
        let parts = match value.as_bulk_string_array() {
            Some(parts) if !parts.is_empty() => parts,
            _ => return RespValue::Error("ERR invalid command format".to_string()),
        };

        // Get command name (case-insensitive)
        let cmd = String::from_utf8_lossy(&parts[0]).to_uppercase();

        match cmd.as_str() {
            "SADD" => Self::cmd_sadd(wrapper, &parts).await,
            "SREM" => Self::cmd_srem(wrapper, &parts).await,
            "SCARD" => Self::cmd_scard(wrapper, &parts).await,
            "SISMEMBER" => Self::cmd_sismember(wrapper, &parts).await,
            "SMISMEMBER" => Self::cmd_smismember(wrapper, &parts).await,
            "SMEMBERS" => Self::cmd_smembers(wrapper, &parts).await,
            "PING" => RespValue::SimpleString("PONG".to_string()),
            _ => RespValue::Error(format!("ERR unknown command '{}'", cmd)),
        }
    }

    async fn cmd_sadd(wrapper: &Arc<ServerWrapper<S>>, parts: &[Bytes]) -> RespValue {
        if parts.len() < 3 {
            return RespValue::Error(
                "ERR wrong number of arguments for 'sadd' command".to_string(),
            );
        }

        let key_name = String::from_utf8_lossy(&parts[1]).to_string();
        let members = &parts[2..];
        trace!("Calling wrapper for sadd");
        match wrapper.sadd(&key_name, members).await {
            Ok(CommandResult::Ok { vv: Some(vv) }) => {
                trace!("Got a vv result from sadd");
                RespValue::SimpleString(format!("OK vv:{}", vv.to_string()))
            }
            Ok(CommandResult::Ok { vv: None }) => RespValue::SimpleString("OK".to_string()),
            Ok(CommandResult::Error(msg)) => RespValue::Error(msg),
            Err(e) => {
                error!("{}", e);
                RespValue::Error(format!("ERR database error: {}", e))
            }
            _ => RespValue::Error("ERR unexpected result".to_string()),
        }
    }

    async fn cmd_srem(wrapper: &Arc<ServerWrapper<S>>, parts: &[Bytes]) -> RespValue {
        if parts.len() < 3 {
            return RespValue::Error(
                "ERR wrong number of arguments for 'srem' command".to_string(),
            );
        }

        let key_name = String::from_utf8_lossy(&parts[1]).to_string();
        let members = &parts[2..];

        match wrapper.srem(&key_name, members).await {
            Ok(CommandResult::Ok { vv: Some(vv) }) => {
                RespValue::SimpleString(format!("OK vv:{}", vv.to_string()))
            }
            Ok(CommandResult::Ok { vv: None }) => RespValue::SimpleString("OK".to_string()),
            Ok(CommandResult::Error(msg)) => RespValue::Error(msg),
            Err(e) => RespValue::Error(format!("ERR database error: {}", e)),
            _ => RespValue::Error("ERR unexpected result".to_string()),
        }
    }

    async fn cmd_scard(wrapper: &Arc<ServerWrapper<S>>, parts: &[Bytes]) -> RespValue {
        if parts.len() < 2 {
            return RespValue::Error(
                "ERR wrong number of arguments for 'scard' command".to_string(),
            );
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

        match wrapper.scard(&key_name, client_vv.as_ref()).await {
            Ok(CommandResult::Integer(count)) => RespValue::Integer(count),
            Ok(CommandResult::NotReady(vv)) => {
                RespValue::Error(format!("NOTREADY vv:{}", vv.to_string()))
            }
            Ok(CommandResult::Error(msg)) => RespValue::Error(msg),
            Err(e) => RespValue::Error(format!("ERR database error: {}", e)),
            _ => RespValue::Error("ERR unexpected result".to_string()),
        }
    }

    async fn cmd_smembers(wrapper: &Arc<ServerWrapper<S>>, parts: &[Bytes]) -> RespValue {
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

        match wrapper.smembers(&key_name, client_vv.as_ref()).await {
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

    async fn cmd_sismember(wrapper: &Arc<ServerWrapper<S>>, parts: &[Bytes]) -> RespValue {
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

        match wrapper
            .sismember(&key_name, member, client_vv.as_ref())
            .await
        {
            Ok(CommandResult::Integer(val)) => RespValue::Integer(val),
            Ok(CommandResult::NotReady(vv)) => {
                RespValue::Error(format!("NOTREADY vv:{}", vv.to_string()))
            }
            Ok(CommandResult::Error(msg)) => RespValue::Error(msg),
            Err(e) => RespValue::Error(format!("ERR database error: {}", e)),
            _ => RespValue::Error("ERR unexpected result".to_string()),
        }
    }

    async fn cmd_smismember(wrapper: &Arc<ServerWrapper<S>>, parts: &[Bytes]) -> RespValue {
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

        match wrapper
            .smismember(&key_name, members, client_vv.as_ref())
            .await
        {
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
}
