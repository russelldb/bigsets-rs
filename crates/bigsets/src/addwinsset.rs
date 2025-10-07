use crate::types::{ActorId, OpType, Operation, VersionVector};
use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
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

/// Update actor's counter in global VV to a specific value (for replication)
pub fn update_vv(conn: &Connection, actor_id: ActorId, counter: u64) -> Result<()> {
    let actor_bytes = actor_id.to_bytes();

    conn.execute(
        "INSERT INTO version_vector (actor_id, counter) VALUES (?, ?)
         ON CONFLICT (actor_id) DO UPDATE SET counter = MAX(counter, excluded.counter)",
        rusqlite::params![&actor_bytes[..], counter],
    )?;

    Ok(())
}

/// Apply a remote operation to the database
/// This is the proper abstraction for replication - takes an operation and applies it atomically
pub fn apply_remote_operation(
    conn: &mut PooledConnection<SqliteConnectionManager>,
    operation: &Operation,
) -> Result<()> {
    let tx = conn.transaction()?;

    // Get or create the set by name
    let set_id = get_or_create_set(&tx, &operation.set_name)?;

    match &operation.op_type {
        OpType::Add {
            elements,
            dot,
            removed_dots,
        } => {
            // Batch insert elements with the operation's dot
            if !elements.is_empty() {
                let query = format!(
                    "INSERT INTO elements (set_id, value) VALUES {}
                     ON CONFLICT (set_id, value) DO UPDATE SET value = value
                     RETURNING id",
                    elements
                        .iter()
                        .map(|_| "(?, ?)")
                        .collect::<Vec<_>>()
                        .join(", ")
                );

                // Prepare params
                let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
                for element in elements {
                    params.push(Box::new(set_id));
                    params.push(Box::new(element.to_vec()));
                }
                let params_refs: Vec<&dyn rusqlite::ToSql> =
                    params.iter().map(|p| p.as_ref()).collect();

                let mut stmt = tx.prepare(&query)?;
                let element_ids: Vec<u64> = stmt
                    .query_map(params_refs.as_slice(), |row| row.get(0))?
                    .collect::<Result<Vec<_>>>()?;

                // Insert dots for each element
                let dot_actor = dot.actor_id.to_bytes();
                for element_id in element_ids {
                    tx.execute(
                        "INSERT OR IGNORE INTO dots (element_id, actor_id, counter) VALUES (?, ?, ?)",
                        rusqlite::params![element_id, &dot_actor[..], dot.counter],
                    )?;
                }
            }

            // Remove tombstoned dots
            for removed_dot in removed_dots {
                let removed_actor = removed_dot.actor_id.to_bytes();
                tx.execute(
                    "DELETE FROM dots WHERE element_id IN (
                        SELECT id FROM elements WHERE set_id = ?
                    ) AND actor_id = ? AND counter = ?",
                    rusqlite::params![set_id, &removed_actor[..], removed_dot.counter],
                )?;
            }
        }
        OpType::Remove {
            elements,
            dot: _,
            removed_dots,
        } => {
            // For each element to remove, delete all its dots
            for element in elements {
                tx.execute(
                    "DELETE FROM dots WHERE element_id IN (
                        SELECT id FROM elements WHERE set_id = ? AND value = ?
                    )",
                    rusqlite::params![set_id, element.to_vec()],
                )?;
            }

            // Clean up elements with no dots
            tx.execute(
                "DELETE FROM elements WHERE set_id = ? AND NOT EXISTS (
                    SELECT 1 FROM dots WHERE dots.element_id = elements.id
                )",
                [set_id],
            )?;

            // Remove tombstoned dots
            for removed_dot in removed_dots {
                let removed_actor = removed_dot.actor_id.to_bytes();
                tx.execute(
                    "DELETE FROM dots WHERE element_id IN (
                        SELECT id FROM elements WHERE set_id = ?
                    ) AND actor_id = ? AND counter = ?",
                    rusqlite::params![set_id, &removed_actor[..], removed_dot.counter],
                )?;
            }
        }
    }

    // Update version vector
    let dot = match &operation.op_type {
        OpType::Add { dot, .. } | OpType::Remove { dot, .. } => dot,
    };
    update_vv(&tx, dot.actor_id, dot.counter)?;

    tx.commit()?;
    Ok(())
}
