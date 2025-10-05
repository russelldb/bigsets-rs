use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A logical timestamp representing an actor and counter pair
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Dot {
    pub actor_id: String,
    pub counter: u64,
}

impl Dot {
    pub fn new(actor_id: String, counter: u64) -> Self {
        Self { actor_id, counter }
    }
}

/// Version vector for causal consistency
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionVector {
    pub counters: HashMap<String, u64>,
}

impl VersionVector {
    pub fn new() -> Self {
        Self {
            counters: HashMap::new(),
        }
    }

    /// Get counter for an actor
    pub fn get(&self, actor_id: &str) -> u64 {
        self.counters.get(actor_id).copied().unwrap_or(0)
    }

    /// Increment counter for an actor and return the new dot
    pub fn increment(&mut self, actor_id: &str) -> Dot {
        let counter = self.counters.entry(actor_id.to_string()).or_insert(0);
        *counter += 1;
        Dot::new(actor_id.to_string(), *counter)
    }

    /// Update counter for an actor to at least the given value
    pub fn update(&mut self, actor_id: &str, counter: u64) {
        let current = self.counters.entry(actor_id.to_string()).or_insert(0);
        *current = (*current).max(counter);
    }

    /// Merge another version vector (take maximum of each counter)
    pub fn merge(&mut self, other: &VersionVector) {
        for (actor_id, &counter) in &other.counters {
            self.update(actor_id, counter);
        }
    }

    /// Check if this VV dominates another (all counters >= other's)
    pub fn dominates(&self, other: &VersionVector) -> bool {
        for (actor_id, &other_counter) in &other.counters {
            if self.get(actor_id) < other_counter {
                return false;
            }
        }
        true
    }

    /// Parse from string format "A:5,B:3,C:2"
    pub fn from_str(s: &str) -> Option<Self> {
        if s.is_empty() {
            return Some(Self::new());
        }

        let mut counters = HashMap::new();
        for pair in s.split(',') {
            let parts: Vec<&str> = pair.split(':').collect();
            if parts.len() != 2 {
                return None;
            }
            let actor_id = parts[0].to_string();
            let counter = parts[1].parse().ok()?;
            counters.insert(actor_id, counter);
        }

        Some(Self { counters })
    }

    /// Format as string "A:5,B:3,C:2"
    pub fn to_string(&self) -> String {
        if self.counters.is_empty() {
            return String::new();
        }

        let mut pairs: Vec<_> = self.counters.iter().collect();
        pairs.sort_by_key(|(actor_id, _)| actor_id.as_str());

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    pub set_id: u64,
    pub op_type: OpType,
    pub timestamp: VersionVector,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpType {
    Add {
        element: Bytes,
        dot: Dot,
        removed_dots: Vec<Dot>,
    },
    Remove {
        element: Bytes,
        removed_dots: Vec<Dot>,
    },
}
