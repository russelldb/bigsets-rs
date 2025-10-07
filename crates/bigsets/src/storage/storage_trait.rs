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

    /// Store the version vector to disk
    /// Called by Server after applying writes
    fn store_vv(&mut self, vv: &VersionVector) -> Result<()>;

    /// Add elements to a set with a given dot
    ///
    /// This is the core CRDT add operation:
    /// - Creates the set if it doesn't exist
    /// - Inserts elements if not present
    /// - Associates the dot with each element
    /// - Removes any tombstoned dots (from concurrent removes)
    /// - Persists the VV atomically with the operation
    ///
    /// # Arguments
    /// * `set_name` - Name of the set
    /// * `elements` - Elements to add
    /// * `dot` - The dot identifying this operation (actor_id, counter)
    /// * `removed_dots` - Dots to remove (tombstones from concurrent operations)
    /// * `vv` - Current version vector (to be persisted)
    fn add_elements(
        &mut self,
        set_name: &str,
        elements: &[Bytes],
        dot: Dot,
        removed_dots: &[Dot],
        vv: &VersionVector,
    ) -> Result<()>;

    /// Remove elements from a set
    ///
    /// This is the core CRDT remove operation:
    /// - Removes all dots associated with the elements
    /// - Cleans up elements that have no remaining dots
    /// - Removes any tombstoned dots
    /// - Persists the VV atomically with the operation
    ///
    /// # Arguments
    /// * `set_name` - Name of the set
    /// * `elements` - Elements to remove
    /// * `removed_dots` - Dots to remove (tombstones)
    /// * `vv` - Current version vector (to be persisted)
    fn remove_elements(
        &mut self,
        set_name: &str,
        elements: &[Bytes],
        removed_dots: &[Dot],
        vv: &VersionVector,
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
