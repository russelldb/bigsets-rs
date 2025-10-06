use crate::db::DbPool;
use crate::types::{Dot, VersionVector};
use bytes::Bytes;
use rusqlite::{params, Result};
use std::collections::HashMap;

/// Get or create a set by name, returning its ID
pub fn get_or_create_set(pool: &DbPool, set_name: &str) -> Result<u64> {
    let conn = pool.get().map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    // Try to get existing set
    let mut stmt = conn.prepare("SELECT id FROM sets WHERE name = ?")?;
    let result: Result<u64> = stmt.query_row([set_name], |row| row.get(0));

    match result {
        Ok(id) => Ok(id),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            // Create new set
            conn.execute("INSERT INTO sets (name) VALUES (?)", [set_name])?;
            Ok(conn.last_insert_rowid() as u64)
        }
        Err(e) => Err(e),
    }
}

/// Add elements to a set with a given dot
pub fn add_elements(
    pool: &DbPool,
    set_id: u64,
    elements: &[Bytes],
    dot: &Dot,
) -> Result<()> {
    let conn = pool.get().map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    for element in elements {
        // Insert or get element
        conn.execute(
            "INSERT INTO elements (set_id, value) VALUES (?, ?) ON CONFLICT DO NOTHING",
            params![set_id, element.as_ref()],
        )?;

        // Get element ID
        let element_id: i64 = conn.query_row(
            "SELECT id FROM elements WHERE set_id = ? AND value = ?",
            params![set_id, element.as_ref()],
            |row| row.get(0),
        )?;

        // Add dot for this element
        conn.execute(
            "INSERT INTO dots (element_id, actor_id, counter) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
            params![element_id, &dot.actor_id, dot.counter],
        )?;
    }

    Ok(())
}

/// Remove elements from a set and return their dots
pub fn remove_elements(
    pool: &DbPool,
    set_id: u64,
    elements: &[Bytes],
) -> Result<Vec<Dot>> {
    let conn = pool.get().map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    let mut removed_dots = Vec::new();

    for element in elements {
        // Get element ID
        let element_id: Result<i64> = conn.query_row(
            "SELECT id FROM elements WHERE set_id = ? AND value = ?",
            params![set_id, element.as_ref()],
            |row| row.get(0),
        );

        if let Ok(element_id) = element_id {
            // Get all dots for this element
            let mut stmt = conn.prepare(
                "SELECT actor_id, counter FROM dots WHERE element_id = ?",
            )?;

            let dots = stmt.query_map([element_id], |row| {
                Ok(Dot {
                    actor_id: row.get(0)?,
                    counter: row.get(1)?,
                })
            })?;

            for dot in dots {
                removed_dots.push(dot?);
            }

            // Delete element (cascade deletes dots)
            conn.execute(
                "DELETE FROM elements WHERE id = ?",
                [element_id],
            )?;
        }
    }

    Ok(removed_dots)
}

/// Get cardinality (number of elements) in a set
pub fn cardinality(pool: &DbPool, set_id: u64) -> Result<i64> {
    let conn = pool.get().map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM elements WHERE set_id = ?",
        [set_id],
        |row| row.get(0),
    )?;

    Ok(count)
}

/// Check if an element is a member of a set
pub fn is_member(pool: &DbPool, set_id: u64, element: &Bytes) -> Result<bool> {
    let conn = pool.get().map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    let exists: i64 = conn.query_row(
        "SELECT COUNT(*) FROM elements WHERE set_id = ? AND value = ?",
        params![set_id, element.as_ref()],
        |row| row.get(0),
    )?;

    Ok(exists > 0)
}

/// Check membership for multiple elements
pub fn are_members(pool: &DbPool, set_id: u64, elements: &[Bytes]) -> Result<Vec<bool>> {
    let mut results = Vec::new();

    for element in elements {
        results.push(is_member(pool, set_id, element)?);
    }

    Ok(results)
}

/// Load version vector for a set from database
pub fn load_version_vector(pool: &DbPool, set_id: u64) -> Result<VersionVector> {
    let conn = pool.get().map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    let mut stmt = conn.prepare(
        "SELECT actor_id, counter FROM version_vectors WHERE set_id = ?",
    )?;

    let rows = stmt.query_map([set_id], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
    })?;

    let mut counters = HashMap::new();
    for row in rows {
        let (actor_id, counter) = row?;
        counters.insert(actor_id, counter);
    }

    Ok(VersionVector { counters })
}

/// Save version vector for a set to database
pub fn save_version_vector(pool: &DbPool, set_id: u64, vv: &VersionVector) -> Result<()> {
    let conn = pool.get().map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    // Delete existing VV entries
    conn.execute("DELETE FROM version_vectors WHERE set_id = ?", [set_id])?;

    // Insert new VV entries
    for (actor_id, &counter) in &vv.counters {
        conn.execute(
            "INSERT INTO version_vectors (set_id, actor_id, counter) VALUES (?, ?, ?)",
            params![set_id, actor_id, counter],
        )?;
    }

    Ok(())
}
