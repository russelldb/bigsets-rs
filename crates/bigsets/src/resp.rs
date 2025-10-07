use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::{self, Cursor};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RespError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid protocol")]
    InvalidProtocol,
    #[error("Incomplete")]
    Incomplete,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    Array(Vec<RespValue>),
    Null,
}

impl RespValue {
    /// Parse RESP value from buffer
    pub fn parse(buf: &mut Cursor<&[u8]>) -> Result<RespValue, RespError> {
        if !buf.has_remaining() {
            return Err(RespError::Incomplete);
        }

        match buf.get_u8() {
            b'+' => {
                let line = read_line(buf)?;
                Ok(RespValue::SimpleString(
                    String::from_utf8_lossy(&line).to_string(),
                ))
            }
            b'-' => {
                let line = read_line(buf)?;
                Ok(RespValue::Error(String::from_utf8_lossy(&line).to_string()))
            }
            b':' => {
                let line = read_line(buf)?;
                let n = String::from_utf8_lossy(&line)
                    .parse::<i64>()
                    .map_err(|_| RespError::InvalidProtocol)?;
                Ok(RespValue::Integer(n))
            }
            b'$' => {
                let line = read_line(buf)?;
                let len = String::from_utf8_lossy(&line)
                    .parse::<i64>()
                    .map_err(|_| RespError::InvalidProtocol)?;

                if len == -1 {
                    return Ok(RespValue::Null);
                }

                let len = len as usize;
                if buf.remaining() < len + 2 {
                    return Err(RespError::Incomplete);
                }

                let data = Bytes::copy_from_slice(&buf.chunk()[..len]);
                buf.advance(len);

                // Expect \r\n
                if buf.get_u8() != b'\r' || buf.get_u8() != b'\n' {
                    return Err(RespError::InvalidProtocol);
                }

                Ok(RespValue::BulkString(data))
            }
            b'*' => {
                let line = read_line(buf)?;
                let count = String::from_utf8_lossy(&line)
                    .parse::<i64>()
                    .map_err(|_| RespError::InvalidProtocol)?;

                if count == -1 {
                    return Ok(RespValue::Null);
                }

                let mut array = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    array.push(RespValue::parse(buf)?);
                }

                Ok(RespValue::Array(array))
            }
            _ => Err(RespError::InvalidProtocol),
        }
    }

    /// Serialize RESP value to buffer
    pub fn serialize(&self, buf: &mut BytesMut) {
        match self {
            RespValue::SimpleString(s) => {
                buf.put_u8(b'+');
                buf.put(s.as_bytes());
                buf.put(&b"\r\n"[..]);
            }
            RespValue::Error(s) => {
                buf.put_u8(b'-');
                buf.put(s.as_bytes());
                buf.put(&b"\r\n"[..]);
            }
            RespValue::Integer(n) => {
                buf.put_u8(b':');
                buf.put(n.to_string().as_bytes());
                buf.put(&b"\r\n"[..]);
            }
            RespValue::BulkString(data) => {
                buf.put_u8(b'$');
                buf.put(data.len().to_string().as_bytes());
                buf.put(&b"\r\n"[..]);
                buf.put(data.as_ref());
                buf.put(&b"\r\n"[..]);
            }
            RespValue::Array(arr) => {
                buf.put_u8(b'*');
                buf.put(arr.len().to_string().as_bytes());
                buf.put(&b"\r\n"[..]);
                for val in arr {
                    val.serialize(buf);
                }
            }
            RespValue::Null => {
                buf.put(&b"$-1\r\n"[..]);
            }
        }
    }

    /// Helper to extract array of bulk strings (for commands)
    pub fn as_bulk_string_array(&self) -> Option<Vec<Bytes>> {
        if let RespValue::Array(arr) = self {
            let mut result = Vec::new();
            for val in arr {
                if let RespValue::BulkString(data) = val {
                    result.push(data.clone());
                } else {
                    return None;
                }
            }
            Some(result)
        } else {
            None
        }
    }
}

fn read_line(buf: &mut Cursor<&[u8]>) -> Result<Vec<u8>, RespError> {
    let start = buf.position() as usize;
    let slice = &buf.get_ref()[start..];

    for i in 0..slice.len() - 1 {
        if slice[i] == b'\r' && slice[i + 1] == b'\n' {
            let line = slice[..i].to_vec();
            buf.advance(i + 2);
            return Ok(line);
        }
    }

    Err(RespError::Incomplete)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let mut buf = Cursor::new(b"+OK\r\n".as_ref());
        let val = RespValue::parse(&mut buf).unwrap();
        assert_eq!(val, RespValue::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_parse_integer() {
        let mut buf = Cursor::new(b":42\r\n".as_ref());
        let val = RespValue::parse(&mut buf).unwrap();
        assert_eq!(val, RespValue::Integer(42));
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut buf = Cursor::new(b"$5\r\nhello\r\n".as_ref());
        let val = RespValue::parse(&mut buf).unwrap();
        assert_eq!(val, RespValue::BulkString(Bytes::from("hello")));
    }

    #[test]
    fn test_parse_array() {
        let mut buf = Cursor::new(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n".as_ref());
        let val = RespValue::parse(&mut buf).unwrap();
        assert_eq!(
            val,
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("foo")),
                RespValue::BulkString(Bytes::from("bar")),
            ])
        );
    }

    #[test]
    fn test_serialize() {
        let val = RespValue::SimpleString("OK".to_string());
        let mut buf = BytesMut::new();
        val.serialize(&mut buf);
        assert_eq!(&buf[..], b"+OK\r\n");
    }
}
