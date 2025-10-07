use crate::types::{ActorId, VersionVector};
use rusqlite::{Connection, Result};
use std::collections::HashMap;

/// Get set ID by name, or None if it doesn't exist
pub fn get_set(conn: &Connection, set_name: &str) -> Result<Option<u64>> {
    let mut stmt = conn.prepare("SELECT id FROM sets WHERE name = ?")?;
    let result: Result<u64> = stmt.query_row([set_name], |row| row.get(0));

    match result {
        Ok(id) => Ok(Some(id)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Get or create a set by name, returning its ID
pub fn get_or_create_set(conn: &Connection, set_name: &str) -> Result<u64> {
    if let Some(id) = get_set(conn, set_name)? {
        return Ok(id);
    }

    // Create new set
    conn.execute("INSERT INTO sets (name) VALUES (?)", [set_name])?;
    Ok(conn.last_insert_rowid() as u64)
}

/// Load global version vector from database
pub fn load_version_vector(conn: &Connection) -> Result<VersionVector> {
    let mut stmt = conn.prepare("SELECT actor_id, counter FROM version_vector")?;

    let rows = stmt.query_map([], |row| {
        let actor_bytes: Vec<u8> = row.get(0)?;
        let counter: u64 = row.get(1)?;
        Ok((actor_bytes, counter))
    })?;

    let mut counters = HashMap::new();
    for row in rows {
        let (actor_bytes, counter) = row?;
        if let Ok(actor_id) = ActorId::from_bytes(&actor_bytes) {
            counters.insert(actor_id, counter);
        }
    }

    Ok(VersionVector { counters })
}

/// Increment actor's counter in global VV and return new counter value
pub fn increment_vv(conn: &Connection, actor_id: ActorId) -> Result<u64> {
    let actor_bytes = actor_id.to_bytes();

    conn.execute(
        "INSERT INTO version_vector (actor_id, counter) VALUES (?, 1)
         ON CONFLICT (actor_id) DO UPDATE SET counter = counter + 1",
        [&actor_bytes[..]],
    )?;

    // Get the new counter value
    let counter: u64 = conn.query_row(
        "SELECT counter FROM version_vector WHERE actor_id = ?",
        [&actor_bytes[..]],
        |row| row.get(0),
    )?;

    Ok(counter)
}
