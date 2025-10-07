use crate::addwinsset;
use crate::types::{ActorId, VersionVector};
use bytes::Bytes;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Result;

/// Result type for command execution
#[derive(Debug, Clone, PartialEq)]
pub enum CommandResult {
    /// OK with optional version vector
    Ok { vv: Option<VersionVector> },
    /// Integer result
    Integer(i64),
    /// Boolean array for multi-membership
    BoolArray(Vec<bool>),
    /// Array of bytes (for SMEMBERS)
    BytesArray(Vec<Bytes>),
    /// Error message
    Error(String),
    /// Not ready to serve read (with current VV)
    NotReady(VersionVector),
}

type DbPool = Pool<SqliteConnectionManager>;

/// Execute SADD command - add members to set
pub fn sadd(
    pool: &DbPool,
    actor_id: ActorId,
    vv: &mut VersionVector,
    set_name: &str,
    members: &[Bytes],
) -> Result<CommandResult> {
    if members.is_empty() {
        return Ok(CommandResult::Error(
            "ERR wrong number of arguments for 'sadd' command".to_string(),
        ));
    }

    // Get connection and start transaction
    let mut conn = pool
        .get()
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    let tx = conn.transaction()?;

    // Get or create set
    let set_id = addwinsset::get_or_create_set(&tx, set_name)?;

    // Increment VV and get new counter for our dot
    let counter = addwinsset::increment_vv(&tx, actor_id)?;

    // Batch insert elements and get their IDs using RETURNING
    let element_placeholders = members
        .iter()
        .map(|_| "(?, ?)")
        .collect::<Vec<_>>()
        .join(", ");

    let insert_elements_query = format!(
        "INSERT INTO elements (set_id, value) VALUES {}
         ON CONFLICT (set_id, value) DO UPDATE SET value = value
         RETURNING id",
        element_placeholders
    );

    // Build params: (set_id, value) for each member
    let mut element_params: Vec<Box<dyn rusqlite::ToSql>> = Vec::with_capacity(members.len() * 2);
    for member in members {
        element_params.push(Box::new(set_id));
        element_params.push(Box::new(member.to_vec()));
    }

    let element_params_refs: Vec<&dyn rusqlite::ToSql> =
        element_params.iter().map(|p| p.as_ref()).collect();

    // Execute and collect element IDs
    let element_ids: Vec<i64> = {
        let mut stmt = tx.prepare(&insert_elements_query)?;
        let rows = stmt.query_map(element_params_refs.as_slice(), |row| row.get(0))?;
        rows.collect::<Result<Vec<i64>>>()?
    };

    // Batch insert dots for all elements
    if !element_ids.is_empty() {
        let dot_placeholders = element_ids
            .iter()
            .map(|_| "(?, ?, ?)")
            .collect::<Vec<_>>()
            .join(", ");
        let insert_dots_query = format!(
            "INSERT INTO dots (element_id, actor_id, counter) VALUES {}
             ON CONFLICT (element_id, actor_id) DO UPDATE SET counter = excluded.counter",
            dot_placeholders
        );

        let actor_bytes = actor_id.to_bytes();
        let mut dot_params: Vec<Box<dyn rusqlite::ToSql>> =
            Vec::with_capacity(element_ids.len() * 3);
        for element_id in element_ids {
            dot_params.push(Box::new(element_id));
            dot_params.push(Box::new(actor_bytes.to_vec()));
            dot_params.push(Box::new(counter));
        }

        let dot_params_refs: Vec<&dyn rusqlite::ToSql> =
            dot_params.iter().map(|p| p.as_ref()).collect();
        tx.execute(&insert_dots_query, dot_params_refs.as_slice())?;
    }

    tx.commit()?;

    // Update local VV
    vv.update(actor_id, counter);

    Ok(CommandResult::Ok {
        vv: Some(vv.clone()),
    })
}

/// Execute SREM command - remove members from set
pub fn srem(
    pool: &DbPool,
    actor_id: ActorId,
    vv: &mut VersionVector,
    set_name: &str,
    members: &[Bytes],
) -> Result<CommandResult> {
    if members.is_empty() {
        return Ok(CommandResult::Error(
            "ERR wrong number of arguments for 'srem' command".to_string(),
        ));
    }

    // Get connection and start transaction
    let mut conn = pool
        .get()
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    let tx = conn.transaction()?;

    // Check if set exists (don't create on remove!)
    let set_id = match addwinsset::get_set(&tx, set_name)? {
        Some(id) => id,
        None => {
            // Set doesn't exist, nothing to remove
            tx.commit()?;
            return Ok(CommandResult::Ok {
                vv: Some(vv.clone()),
            });
        }
    };

    // Increment VV for causality tracking (even though we're removing)
    let counter = addwinsset::increment_vv(&tx, actor_id)?;

    // Batch delete elements
    for member in members {
        // Delete element (cascade deletes dots)
        tx.execute(
            "DELETE FROM elements WHERE set_id = ? AND value = ?",
            rusqlite::params![set_id, member.as_ref()],
        )?;
    }

    tx.commit()?;

    // Update local VV
    vv.update(actor_id, counter);

    Ok(CommandResult::Ok {
        vv: Some(vv.clone()),
    })
}

/// Execute SCARD command - get cardinality of set
pub fn scard(
    pool: &DbPool,
    local_vv: &VersionVector,
    set_name: &str,
    client_vv: Option<&VersionVector>,
) -> Result<CommandResult> {
    // Check if we can serve this read (if client provided VV)
    if let Some(cv) = client_vv {
        if !local_vv.descends(cv) {
            return Ok(CommandResult::NotReady(local_vv.clone()));
        }
    }

    // Get connection and start transaction
    let mut conn = pool
        .get()
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    let tx = conn.transaction()?;

    // Get set ID (return 0 if doesn't exist)
    let set_id = match addwinsset::get_set(&tx, set_name)? {
        Some(id) => id,
        None => {
            tx.commit()?;
            return Ok(CommandResult::Integer(0));
        }
    };

    // Get cardinality
    let count: i64 = tx.query_row(
        "SELECT COUNT(*) FROM elements WHERE set_id = ?",
        [set_id],
        |row| row.get(0),
    )?;

    tx.commit()?;

    Ok(CommandResult::Integer(count))
}

/// Execute SISMEMBER command - check if member exists in set
pub fn sismember(
    pool: &DbPool,
    local_vv: &VersionVector,
    set_name: &str,
    member: &Bytes,
    client_vv: Option<&VersionVector>,
) -> Result<CommandResult> {
    // Check if we can serve this read (if client provided VV)
    if let Some(cv) = client_vv {
        if !local_vv.descends(cv) {
            return Ok(CommandResult::NotReady(local_vv.clone()));
        }
    }

    // Get connection and start transaction
    let mut conn = pool
        .get()
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    let tx = conn.transaction()?;

    // Get set ID (return 0 if doesn't exist)
    let set_id = match addwinsset::get_set(&tx, set_name)? {
        Some(id) => id,
        None => {
            tx.commit()?;
            return Ok(CommandResult::Integer(0));
        }
    };

    // Check membership
    let exists: i64 = tx.query_row(
        "SELECT COUNT(*) FROM elements WHERE set_id = ? AND value = ?",
        rusqlite::params![set_id, member.as_ref()],
        |row| row.get(0),
    )?;

    tx.commit()?;

    Ok(CommandResult::Integer(if exists > 0 { 1 } else { 0 }))
}

/// Execute SMISMEMBER command - check if multiple members exist in set
pub fn smismember(
    pool: &DbPool,
    local_vv: &VersionVector,
    set_name: &str,
    members: &[Bytes],
    client_vv: Option<&VersionVector>,
) -> Result<CommandResult> {
    if members.is_empty() {
        return Ok(CommandResult::Error(
            "ERR wrong number of arguments for 'smismember' command".to_string(),
        ));
    }

    // Check if we can serve this read (if client provided VV)
    if let Some(cv) = client_vv {
        if !local_vv.descends(cv) {
            return Ok(CommandResult::NotReady(local_vv.clone()));
        }
    }

    // Get connection and start transaction
    let mut conn = pool
        .get()
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    let tx = conn.transaction()?;

    // Get set ID (return all false if doesn't exist)
    let set_id = match addwinsset::get_set(&tx, set_name)? {
        Some(id) => id,
        None => {
            tx.commit()?;
            return Ok(CommandResult::BoolArray(vec![false; members.len()]));
        }
    };

    // Batch check membership using IN clause
    // Build placeholders for IN clause
    let placeholders = members.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let query = format!(
        "SELECT value FROM elements WHERE set_id = ? AND value IN ({})",
        placeholders
    );

    // Query expects: set_id as first param, then all values
    let existing: std::collections::HashSet<Vec<u8>> = {
        let mut stmt = tx.prepare(&query)?;

        let mut query_params: Vec<Box<dyn rusqlite::ToSql>> = Vec::with_capacity(members.len() + 1);
        query_params.push(Box::new(set_id));
        for member in members {
            query_params.push(Box::new(member.to_vec()));
        }

        let params_refs: Vec<&dyn rusqlite::ToSql> =
            query_params.iter().map(|p| p.as_ref()).collect();

        let rows = stmt.query_map(params_refs.as_slice(), |row| row.get::<_, Vec<u8>>(0))?;

        rows.collect::<Result<std::collections::HashSet<_>>>()?
        // stmt is dropped here
    };

    // Map results back to input order
    let membership: Vec<bool> = members
        .iter()
        .map(|m| existing.contains(m.as_ref()))
        .collect();

    tx.commit()?;

    Ok(CommandResult::BoolArray(membership))
}

/// Execute SMEMBERS command - return all members of set
pub fn smembers(
    pool: &DbPool,
    local_vv: &VersionVector,
    set_name: &str,
    client_vv: Option<&VersionVector>,
) -> Result<CommandResult> {
    // Check if we can serve this read (if client provided VV)
    if let Some(cv) = client_vv {
        if !local_vv.descends(cv) {
            return Ok(CommandResult::NotReady(local_vv.clone()));
        }
    }

    // Get connection and start transaction
    let mut conn = pool
        .get()
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    let tx = conn.transaction()?;

    // Get set ID (return empty array if doesn't exist)
    let set_id = match addwinsset::get_set(&tx, set_name)? {
        Some(id) => id,
        None => {
            tx.commit()?;
            return Ok(CommandResult::BytesArray(Vec::new()));
        }
    };

    // Get all members
    let members = {
        let mut stmt = tx.prepare("SELECT value FROM elements WHERE set_id = ?")?;
        let rows = stmt.query_map([set_id], |row| {
            let value: Vec<u8> = row.get(0)?;
            Ok(Bytes::from(value))
        })?;

        rows.collect::<Result<Vec<Bytes>>>()?
    };

    tx.commit()?;

    Ok(CommandResult::BytesArray(members))
}

#[cfg(test)]
mod tests {
    use super::*;
    use r2d2::Pool;
    use r2d2_sqlite::SqliteConnectionManager;

    fn create_test_pool() -> Result<DbPool> {
        let manager = SqliteConnectionManager::memory();
        let pool = Pool::builder()
            .max_size(1)
            .build(manager)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        // Initialize schema
        let conn = pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS sets (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            );

            CREATE TABLE IF NOT EXISTS version_vector (
                actor_id BLOB NOT NULL,
                counter INTEGER NOT NULL,
                PRIMARY KEY (actor_id)
            );

            CREATE TABLE IF NOT EXISTS elements (
                id INTEGER PRIMARY KEY,
                set_id INTEGER NOT NULL,
                value BLOB NOT NULL,
                FOREIGN KEY (set_id) REFERENCES sets(id) ON DELETE CASCADE,
                UNIQUE (set_id, value)
            );

            CREATE TABLE IF NOT EXISTS dots (
                element_id INTEGER NOT NULL,
                actor_id BLOB NOT NULL,
                counter INTEGER NOT NULL,
                PRIMARY KEY (element_id, actor_id),
                FOREIGN KEY (element_id) REFERENCES elements(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_elements_set_value ON elements(set_id, value);
            CREATE INDEX IF NOT EXISTS idx_dots_element ON dots(element_id);
            "#,
        )?;

        Ok(pool)
    }

    #[test]
    fn test_sadd_command() -> Result<()> {
        let pool = create_test_pool()?;
        let mut vv = VersionVector::new();
        let actor = ActorId::from_node_id(1);

        let members = vec![Bytes::from("apple"), Bytes::from("banana")];
        let result = sadd(&pool, actor, &mut vv, "myset", &members)?;

        match result {
            CommandResult::Ok {
                vv: Some(returned_vv),
            } => {
                assert_eq!(returned_vv.get(actor), 1);
            }
            _ => panic!("Expected Ok result"),
        }

        // Verify elements were added
        let card_result = scard(&pool, &vv, "myset", None)?;
        assert_eq!(card_result, CommandResult::Integer(2));

        Ok(())
    }

    #[test]
    fn test_srem_command() -> Result<()> {
        let pool = create_test_pool()?;
        let mut vv = VersionVector::new();
        let actor = ActorId::from_node_id(1);

        // Add elements first
        let members = vec![Bytes::from("apple"), Bytes::from("banana")];
        sadd(&pool, actor, &mut vv, "myset", &members)?;

        // Remove one
        let to_remove = vec![Bytes::from("apple")];
        let result = srem(&pool, actor, &mut vv, "myset", &to_remove)?;

        match result {
            CommandResult::Ok {
                vv: Some(returned_vv),
            } => {
                assert_eq!(returned_vv.get(actor), 2); // Incremented for remove
            }
            _ => panic!("Expected Ok result"),
        }

        // Verify cardinality
        let card_result = scard(&pool, &vv, "myset", None)?;
        assert_eq!(card_result, CommandResult::Integer(1));

        Ok(())
    }

    #[test]
    fn test_scard_with_client_vv() -> Result<()> {
        let pool = create_test_pool()?;
        let mut vv = VersionVector::new();
        let actor = ActorId::from_node_id(1);

        // Add elements
        let members = vec![Bytes::from("apple")];
        sadd(&pool, actor, &mut vv, "myset", &members)?;

        // Client VV behind - should serve
        let mut client_vv = VersionVector::new();
        let result = scard(&pool, &vv, "myset", Some(&client_vv))?;
        assert_eq!(result, CommandResult::Integer(1));

        // Client VV ahead - should return NotReady
        client_vv.update(actor, 5);
        let result = scard(&pool, &vv, "myset", Some(&client_vv))?;
        match result {
            CommandResult::NotReady(local) => {
                assert_eq!(local.get(actor), 1);
            }
            _ => panic!("Expected NotReady"),
        }

        Ok(())
    }

    #[test]
    fn test_sismember_command() -> Result<()> {
        let pool = create_test_pool()?;
        let mut vv = VersionVector::new();
        let actor = ActorId::from_node_id(1);

        // Add elements
        let members = vec![Bytes::from("apple")];
        sadd(&pool, actor, &mut vv, "myset", &members)?;

        // Check membership
        let result = sismember(&pool, &vv, "myset", &Bytes::from("apple"), None)?;
        assert_eq!(result, CommandResult::Integer(1));

        let result = sismember(&pool, &vv, "myset", &Bytes::from("banana"), None)?;
        assert_eq!(result, CommandResult::Integer(0));

        Ok(())
    }

    #[test]
    fn test_smismember_command() -> Result<()> {
        let pool = create_test_pool()?;
        let mut vv = VersionVector::new();
        let actor = ActorId::from_node_id(1);

        // Add elements
        let members = vec![Bytes::from("apple"), Bytes::from("banana")];
        sadd(&pool, actor, &mut vv, "myset", &members)?;

        // Check multiple members
        let check = vec![
            Bytes::from("apple"),
            Bytes::from("grape"),
            Bytes::from("banana"),
        ];
        let result = smismember(&pool, &vv, "myset", &check, None)?;

        match result {
            CommandResult::BoolArray(results) => {
                assert_eq!(results, vec![true, false, true]);
            }
            _ => panic!("Expected BoolArray"),
        }

        Ok(())
    }

    #[test]
    fn test_sadd_empty_members() -> Result<()> {
        let pool = create_test_pool()?;
        let mut vv = VersionVector::new();
        let actor = ActorId::from_node_id(1);

        let result = sadd(&pool, actor, &mut vv, "myset", &[])?;

        match result {
            CommandResult::Error(msg) => {
                assert!(msg.contains("wrong number of arguments"));
            }
            _ => panic!("Expected Error result"),
        }

        Ok(())
    }
}
