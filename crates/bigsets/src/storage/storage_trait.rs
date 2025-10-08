use crate::types::{Dot, VersionVector};
use bytes::Bytes;
use rusqlite::Result;

/// Storage abstraction for CRDT set operations
///
/// This trait defines the persistence layer for the add-wins set CRDT.
/// Implementations handle storing elements, dots, and version vectors.
///
/// Key design principles:
/// - Storage does NOT manage the version vector lifecycle (Server does)
/// - VV is passed in on writes for transactional consistency
/// - Storage just persists the data structures
/// - No knowledge of Operation or replication concerns
pub trait Storage: Send + Sync {
    /// Load the persisted version vector from storage
    /// Called once at Server startup
    fn load_vv(&self) -> Result<VersionVector>;

    fn remote_add_elements(
        &self,
        set_name: &str,
        elements: &[Bytes],
        removed_dots: &[Dot],
        dot: Dot,
    ) -> Result<()>;

    fn add_elements(&self, set_name: &str, elements: &[Bytes], dot: Dot) -> Result<Vec<Dot>>;

    fn remove_elements(&self, set_name: &str, elements: &[Bytes], dot: Dot) -> Result<Vec<Dot>>;

    fn remote_remove_elements(
        &self,
        set_name: &str,
        elements: &[Bytes],
        removed_dots: &[Dot],
        dot: Dot,
    ) -> Result<()>;

    /// Get all elements in a set
    fn get_elements(&self, set_name: &str) -> Result<Vec<Bytes>>;

    /// Count elements in a set
    fn count_elements(&self, set_name: &str) -> Result<i64>;

    /// Check if an element is a member of a set
    fn is_member(&self, set_name: &str, element: &Bytes) -> Result<bool>;

    /// Check membership for multiple elements
    fn are_members(&self, set_name: &str, elements: &[Bytes]) -> Result<Vec<bool>>;
}
