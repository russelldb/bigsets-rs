use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

/// Fixed-size actor identifier
///
/// Binary layout (4 bytes): [version: u8][node_id: u16][epoch: u8]
/// - version: Protocol version (currently 0)
/// - node_id: Node identifier (0-65535)
/// - epoch: Restart/generation counter (0-255, defaults to 0)
///
/// Human-readable format: "v0:1234:5" (version:node:epoch)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ActorId {
    bytes: [u8; 4],
}

impl ActorId {
    pub fn version(&self) -> u8 {
        self.bytes[0]
    }
    pub fn node_id(&self) -> u16 {
        ((self.bytes[1] as u16) << 8) | (self.bytes[2] as u16)
    }
    pub fn epoch(&self) -> u8 {
        self.bytes[3]
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl ActorId {
    pub fn new(node_id: u16, epoch: u8) -> Self {
        Self {
            bytes: [
                0,                      // version (default 0)
                (node_id >> 8) as u8,   // node_id high byte
                (node_id & 0xFF) as u8, // node_id low byte
                epoch,
            ],
        }
    }

    fn new_with_version(version: u8, node_id: u16, epoch: u8) -> Self {
        Self {
            bytes: [
                version,                // version
                (node_id >> 8) as u8,   // node_id high byte
                (node_id & 0xFF) as u8, // node_id low byte
                epoch,
            ],
        }
    }

    pub fn from_node_id(node_id: u16) -> Self {
        Self::new(node_id, 0)
    }

    /// Deserialize from fixed 4-byte format
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ActorIdError> {
        if bytes.len() != 4 {
            return Err(ActorIdError::InvalidLength(bytes.len()));
        }

        Ok(Self {
            bytes: [bytes[0], bytes[1], bytes[2], bytes[3]],
        })
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}:{}:{}", self.version(), self.node_id(), self.epoch())
    }
}

impl FromStr for ActorId {
    type Err = ActorIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 3 {
            return Err(ActorIdError::InvalidFormat);
        }

        // Parse version (expect "v0" format)
        let version_str = parts[0];
        if !version_str.starts_with('v') {
            return Err(ActorIdError::InvalidFormat);
        }
        let version: u8 = version_str[1..]
            .parse()
            .map_err(|_| ActorIdError::InvalidFormat)?;

        let node_id: u16 = parts[1].parse().map_err(|_| ActorIdError::InvalidFormat)?;
        let epoch: u8 = parts[2].parse().map_err(|_| ActorIdError::InvalidFormat)?;

        Ok(Self::new_with_version(version, node_id, epoch))
    }
}

/// Errors that can occur when working with ActorId
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorIdError {
    InvalidLength(usize),
    InvalidFormat,
}

impl fmt::Display for ActorIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActorIdError::InvalidLength(len) => {
                write!(f, "Invalid ActorId length: {} (expected 4)", len)
            }
            ActorIdError::InvalidFormat => write!(f, "Invalid ActorId format"),
        }
    }
}

impl std::error::Error for ActorIdError {}

/// A logical timestamp representing an actor and counter pair
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Dot {
    pub actor_id: ActorId,
    pub counter: u64,
}

impl Dot {
    pub fn new(actor_id: ActorId, counter: u64) -> Self {
        Self { actor_id, counter }
    }

    pub fn from_parts(actor_id: Vec<u8>, counter: u64) -> Result<Self, ActorIdError> {
        let actor_id = ActorId::from_bytes(&actor_id)?;
        Ok(Self { actor_id, counter })
    }
}

/// Version vector for causal consistency
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionVector {
    pub counters: HashMap<ActorId, u64>,
}

impl VersionVector {
    pub fn new() -> Self {
        Self {
            counters: HashMap::new(),
        }
    }

    /// Get counter for an actor
    pub fn get(&self, actor_id: ActorId) -> u64 {
        self.counters.get(&actor_id).copied().unwrap_or(0)
    }

    /// Increment counter for an actor and return the new dot
    pub fn increment(&mut self, actor_id: ActorId) -> Dot {
        let counter = self.counters.entry(actor_id).or_insert(0);
        *counter += 1;
        Dot::new(actor_id, *counter)
    }

    /// Update counter for an actor to at least the given value
    pub fn update(&mut self, actor_id: ActorId, counter: u64) {
        let current = self.counters.entry(actor_id).or_insert(0);
        *current = (*current).max(counter);
    }

    /// Merge another version vector (take maximum of each counter)
    pub fn merge(&mut self, other: &VersionVector) {
        for (actor_id, &counter) in &other.counters {
            self.update(*actor_id, counter);
        }
    }

    /// Check if this VV descends from another (has seen all events in other)
    /// Returns true if self >= other (all of other's counters are in self)
    pub fn descends(&self, other: &VersionVector) -> bool {
        for (actor_id, &other_counter) in &other.counters {
            if self.get(*actor_id) < other_counter {
                return false;
            }
        }
        true
    }

    /// Parse from string format "v0:1:0:5,v0:2:0:3" (actorId:counter pairs)
    pub fn from_str(s: &str) -> Option<Self> {
        if s.is_empty() {
            return Some(Self::new());
        }

        let mut counters = HashMap::new();
        for pair in s.split(',') {
            let (actor_str, counter_str) = pair.rsplit_once(':')?;
            let actor_id = ActorId::from_str(actor_str).ok()?;
            let counter = counter_str.parse().ok()?;
            counters.insert(actor_id, counter);
        }

        Some(Self { counters })
    }

    /// Format as string "v0:1:0:5,v0:2:0:3" (actorId:counter pairs)
    pub fn to_string(&self) -> String {
        if self.counters.is_empty() {
            return String::new();
        }

        let mut pairs: Vec<_> = self.counters.iter().collect();
        pairs.sort_by_key(|(actor_id, _)| *actor_id);

        pairs
            .iter()
            .map(|(actor_id, counter)| format!("{}:{}", actor_id, counter))
            .collect::<Vec<_>>()
            .join(",")
    }
}

impl Default for VersionVector {
    fn default() -> Self {
        Self::new()
    }
}

/// Operation type for replication
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Operation {
    pub set_name: String,
    pub op_type: OpType,
    pub context: VersionVector,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OpType {
    Add {
        elements: Vec<Bytes>,   // Multiple elements, single dot
        dot: Dot,               // Single dot for this add operation
        removed_dots: Vec<Dot>, // Concurrent removes observed
    },
    Remove {
        elements: Vec<Bytes>,   // Multiple elements being removed
        dot: Dot,               // New dot for this remove (causality only, VV only)
        removed_dots: Vec<Dot>, // Dots that were on these elements
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    // ActorId tests
    #[test]
    fn test_actor_id_new() {
        let actor = ActorId::new(1234, 5);
        assert_eq!(actor.version(), 0);
        assert_eq!(actor.node_id(), 1234);
        assert_eq!(actor.epoch(), 5);
    }

    #[test]
    fn test_actor_id_from_node_id() {
        let actor = ActorId::from_node_id(42);
        assert_eq!(actor.version(), 0);
        assert_eq!(actor.node_id(), 42);
        assert_eq!(actor.epoch(), 0);
    }

    #[test]
    fn test_actor_id_to_bytes() {
        let actor = ActorId::new(0x1234, 0x56);
        let bytes = actor.bytes();
        assert_eq!(bytes, [0x00, 0x12, 0x34, 0x56]);
    }

    #[test]
    fn test_actor_id_from_bytes() {
        let bytes = [0x00, 0x12, 0x34, 0x56];
        let actor = ActorId::from_bytes(&bytes).unwrap();
        assert_eq!(actor.version(), 0);
        assert_eq!(actor.node_id(), 0x1234);
        assert_eq!(actor.epoch(), 0x56);
    }

    #[test]
    fn test_actor_id_from_bytes_invalid_length() {
        let bytes = [0x00, 0x12, 0x34];
        assert!(ActorId::from_bytes(&bytes).is_err());
    }

    #[test]
    fn test_actor_id_roundtrip_bytes() {
        let actor1 = ActorId::new(12345, 7);
        let bytes = actor1.bytes();
        let actor2 = ActorId::from_bytes(&bytes).unwrap();
        assert_eq!(actor1, actor2);
    }

    #[test]
    fn test_actor_id_display() {
        let actor = ActorId::new(1234, 5);
        assert_eq!(format!("{}", actor), "v0:1234:5");
    }

    #[test]
    fn test_actor_id_from_str() {
        let actor = ActorId::from_str("v0:1234:5").unwrap();
        assert_eq!(actor.version(), 0);
        assert_eq!(actor.node_id(), 1234);
        assert_eq!(actor.epoch(), 5);
    }

    #[test]
    fn test_actor_id_from_str_invalid() {
        assert!(ActorId::from_str("invalid").is_err());
        assert!(ActorId::from_str("1234:5").is_err());
        assert!(ActorId::from_str("v0:1234").is_err());
        assert!(ActorId::from_str("v0:abc:5").is_err());
    }

    #[test]
    fn test_actor_id_roundtrip_string() {
        let actor1 = ActorId::new(999, 3);
        let s = actor1.to_string();
        let actor2 = ActorId::from_str(&s).unwrap();
        assert_eq!(actor1, actor2);
    }

    #[test]
    fn test_actor_id_ordering() {
        let a1 = ActorId::new(1, 0);
        let a2 = ActorId::new(2, 0);
        let a3 = ActorId::new(1, 1);

        assert!(a1 < a2);
        assert!(a1 < a3);
        assert!(a3 < a2); // version:0, node:1, epoch:1 < version:0, node:2, epoch:0
    }

    // Dot tests
    #[test]
    fn test_dot_creation() {
        let actor = ActorId::new(1, 0);
        let dot = Dot::new(actor, 5);
        assert_eq!(dot.actor_id, actor);
        assert_eq!(dot.counter, 5);
    }

    // VersionVector tests
    #[test]
    fn test_version_vector_new() {
        let vv = VersionVector::new();
        assert!(vv.counters.is_empty());
    }

    #[test]
    fn test_version_vector_increment() {
        let mut vv = VersionVector::new();
        let actor_a = ActorId::from_node_id(1);
        let actor_b = ActorId::from_node_id(2);

        let dot1 = vv.increment(actor_a);
        assert_eq!(dot1.actor_id, actor_a);
        assert_eq!(dot1.counter, 1);
        assert_eq!(vv.get(actor_a), 1);

        let dot2 = vv.increment(actor_a);
        assert_eq!(dot2.counter, 2);
        assert_eq!(vv.get(actor_a), 2);

        let dot3 = vv.increment(actor_b);
        assert_eq!(dot3.actor_id, actor_b);
        assert_eq!(dot3.counter, 1);
        assert_eq!(vv.get(actor_b), 1);
    }

    #[test]
    fn test_version_vector_get() {
        let mut vv = VersionVector::new();
        let actor = ActorId::from_node_id(1);

        assert_eq!(vv.get(actor), 0); // Non-existent actor returns 0

        vv.increment(actor);
        assert_eq!(vv.get(actor), 1);
    }

    #[test]
    fn test_version_vector_update() {
        let mut vv = VersionVector::new();
        let actor = ActorId::from_node_id(1);

        vv.update(actor, 5);
        assert_eq!(vv.get(actor), 5);

        vv.update(actor, 3); // Should not decrease
        assert_eq!(vv.get(actor), 5);

        vv.update(actor, 7); // Should increase
        assert_eq!(vv.get(actor), 7);
    }

    #[test]
    fn test_version_vector_merge() {
        let mut vv1 = VersionVector::new();
        let actor_a = ActorId::from_node_id(1);
        let actor_b = ActorId::from_node_id(2);
        let actor_c = ActorId::from_node_id(3);

        vv1.increment(actor_a);
        vv1.increment(actor_a);
        vv1.increment(actor_b);

        let mut vv2 = VersionVector::new();
        vv2.increment(actor_a);
        vv2.increment(actor_c);
        vv2.increment(actor_c);

        vv1.merge(&vv2);

        assert_eq!(vv1.get(actor_a), 2); // max(2, 1)
        assert_eq!(vv1.get(actor_b), 1); // only in vv1
        assert_eq!(vv1.get(actor_c), 2); // only in vv2
    }

    #[test]
    fn test_version_vector_descends() {
        let mut vv1 = VersionVector::new();
        let actor_a = ActorId::from_node_id(1);
        let actor_b = ActorId::from_node_id(2);
        let actor_c = ActorId::from_node_id(3);

        vv1.increment(actor_a);
        vv1.increment(actor_a);
        vv1.increment(actor_b);

        let mut vv2 = VersionVector::new();
        vv2.increment(actor_a);

        assert!(vv1.descends(&vv2)); // vv1 >= vv2
        assert!(!vv2.descends(&vv1)); // vv2 < vv1

        let mut vv3 = VersionVector::new();
        vv3.increment(actor_c);

        assert!(!vv1.descends(&vv3)); // Concurrent - vv1 doesn't have C
        assert!(!vv3.descends(&vv1)); // Concurrent - vv3 doesn't have A or B
    }

    #[test]
    fn test_version_vector_descends_self() {
        let mut vv = VersionVector::new();
        let actor = ActorId::from_node_id(1);

        vv.increment(actor);

        assert!(vv.descends(&vv)); // Should descend itself (reflexive)
    }

    #[test]
    fn test_version_vector_from_str() {
        let vv = VersionVector::from_str("v0:1:0:5,v0:2:0:3,v0:3:0:2").unwrap();
        assert_eq!(vv.get(ActorId::from_node_id(1)), 5);
        assert_eq!(vv.get(ActorId::from_node_id(2)), 3);
        assert_eq!(vv.get(ActorId::from_node_id(3)), 2);

        let empty = VersionVector::from_str("").unwrap();
        assert!(empty.counters.is_empty());

        assert!(VersionVector::from_str("invalid").is_none());
        assert!(VersionVector::from_str("v0:1:0:5,v0:2:0").is_none());
    }

    #[test]
    fn test_version_vector_to_string() {
        let mut vv = VersionVector::new();
        let actor_a = ActorId::from_node_id(1);
        let actor_b = ActorId::from_node_id(2);

        vv.increment(actor_b);
        vv.increment(actor_a);
        vv.increment(actor_b);

        let s = vv.to_string();
        // Should be sorted by ActorId (node_id ordering)
        assert_eq!(s, "v0:1:0:1,v0:2:0:2");

        let empty = VersionVector::new();
        assert_eq!(empty.to_string(), "");
    }

    #[test]
    fn test_version_vector_roundtrip() {
        let mut vv1 = VersionVector::new();
        let actor_a = ActorId::from_node_id(1);
        let actor_b = ActorId::from_node_id(2);
        let actor_c = ActorId::from_node_id(3);

        vv1.increment(actor_a);
        vv1.increment(actor_a);
        vv1.increment(actor_b);
        vv1.increment(actor_c);

        let s = vv1.to_string();
        let vv2 = VersionVector::from_str(&s).unwrap();

        assert_eq!(vv1, vv2);
    }
}
