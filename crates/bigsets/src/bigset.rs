use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BigSet {
    // Implementation to be added
}

impl BigSet {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for BigSet {
    fn default() -> Self {
        Self::new()
    }
}
