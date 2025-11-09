use crate::config::StorageConfig;
use crate::types::{ActorId, Dot, VersionVector};
use bytes::Bytes;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, OptionalExtension, Result, ToSql};
use std::collections::HashMap;
use std::path::Path;
use tracing::trace;

pub type DbPool = Pool<SqliteConnectionManager>;

/// SQLite implementation of the Storage trait
/// All the AddWinsSet logic is in the sql.
/// The purpose of bigsets is to not pay the price
/// - of reading an entire set from disk and deserialising it before mutatating
/// - nor after of reserialising it and writing it all back to disk
/// See the add/remove_[remote]_elements methods for how the AddWins semantics are maintained.
#[derive(Clone, Debug)]
pub struct SqliteStorage {
    pool: DbPool,
}

impl SqliteStorage {
    pub fn open<P: AsRef<Path>>(path: P, config: &StorageConfig) -> Result<Self> {
        let cache_size = config.sqlite_cache_size;
        let busy_timeout = config.sqlite_busy_timeout;
        let path_ref = path.as_ref();

        {
            let conn = rusqlite::Connection::open(path_ref)?;
            conn.pragma_update(None, "cache_size", cache_size)?;
            conn.pragma_update(None, "busy_timeout", busy_timeout)?;
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", "NORMAL")?;

            Self::create_schema(&conn)?;
        }

        let manager = SqliteConnectionManager::file(path_ref).with_init(move |conn| {
            conn.pragma_update(None, "cache_size", cache_size)?;
            conn.pragma_update(None, "busy_timeout", busy_timeout)?;
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", "NORMAL")?;
            Ok(())
        });

        let pool = Pool::builder()
            .max_size(5) // Sized for concurrent reads only (writes are serialized)
            .min_idle(Some(1))
            .build(manager)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        Ok(SqliteStorage { pool })
    }

    /// The schema is the AddWinsSet design.
    /// Some properties:
    /// - Every dot actor is in the version vector table
    /// - Every dot counter will be <= the counter in the version_vector table for that actor
    /// - There will be at most one dot per actor per element
    /// - Every element has at least one dot
    fn create_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            r#"
            -- Sets namespace
            CREATE TABLE IF NOT EXISTS sets (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            );

            -- version vector
            CREATE TABLE IF NOT EXISTS version_vector (
                actor_id BLOB NOT NULL,  -- 4-byte ActorId
                counter INTEGER NOT NULL,
                PRIMARY KEY (actor_id)
            );

            -- Unique element values
            CREATE TABLE IF NOT EXISTS elements (
                id INTEGER PRIMARY KEY,
                set_id INTEGER NOT NULL,
                value BLOB NOT NULL,
                FOREIGN KEY (set_id) REFERENCES sets(id) ON DELETE CASCADE,
                UNIQUE (set_id, value)
            );

            -- Dots pointing to elements (at most one dot per element per actor)
            CREATE TABLE IF NOT EXISTS dots (
                element_id INTEGER NOT NULL,
                actor_id BLOB NOT NULL,  -- 4-byte ActorId
                counter INTEGER NOT NULL,
                PRIMARY KEY (element_id, actor_id),
                FOREIGN KEY (element_id) REFERENCES elements(id) ON DELETE CASCADE
            ) WITHOUT ROWID;

            -- Indexes for performance
            CREATE INDEX IF NOT EXISTS idx_elements_set_value ON elements(set_id, value);
            CREATE INDEX IF NOT EXISTS idx_dots_element ON dots(element_id);
            "#,
        )?;

        Ok(())
    }

    pub fn pool(&self) -> &DbPool {
        &self.pool
    }

    pub fn load_vv(&self) -> Result<VersionVector> {
        let conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

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

    /// Adding an element to an AddWinsSet "joins" all the observed concurrent writes for that element (if any).
    /// The process is:
    /// - generate a new dot for this add
    /// - if the set does not exist, create it
    /// - insert the element into the elements table
    /// - delete and return every existing dot for this element
    /// - insert the new element
    /// - return the set of dots, as these must be replicated to peers as part of the context of the operation.
    /// Adding an element results in single dot for that element,
    /// a dot that has replaced (joined) the previously observed concurrent adds.
    pub fn add_elements(&self, set_name: &str, elements: &[Bytes], dot: Dot) -> Result<Vec<Dot>> {
        if elements.is_empty() {
            return Ok(vec![]);
        }

        let mut conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        let tx = conn.transaction()?;

        // Get the set_id (creating if needed)
        let set_id: i64 = tx.query_row(
            "INSERT INTO sets (name) VALUES (?1) ON CONFLICT(name) DO UPDATE SET name=name RETURNING id",
            [set_name],
            |row| row.get(0),
        )?;

        let mut deleted = Vec::new();
        let actor_id = dot.actor_id.bytes();

        for element in elements {
            // Insert element (or get existing element_id)
            let element_id: i64 = tx.query_row(
                "INSERT INTO elements (set_id, value) VALUES (?1, ?2) ON CONFLICT(set_id, value) DO UPDATE SET value=value RETURNING id",
                rusqlite::params![set_id, element.as_ref()],
                |row| row.get(0),
            )?;

            // Remove and return each existing dot for this element_id
            let mut stmt =
                tx.prepare("DELETE FROM dots WHERE element_id = ?1 RETURNING actor_id, counter")?;
            let rows = stmt.query_map([element_id], |row| {
                Ok(Dot::from_parts(row.get(0)?, row.get(1)?)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?)
            })?;

            for r in rows {
                deleted.push(r?);
            }
            drop(stmt);

            // Insert the new dot for this element_id
            tx.execute(
                "INSERT INTO dots (element_id, actor_id, counter) VALUES (?1, ?2, ?3)",
                rusqlite::params![element_id, actor_id, dot.counter],
            )?;
        }

        // Update version vector with the new dot
        tx.execute(
            "INSERT INTO version_vector (actor_id, counter) VALUES (?1, ?2) ON CONFLICT(actor_id) DO UPDATE SET counter = MAX(counter, excluded.counter)",
            rusqlite::params![actor_id, dot.counter],
        )?;

        tx.commit()?;
        Ok(deleted)
    }

    /// Removing an element is much like adding one, in that it returns the set of dots currently supporting that element.
    /// The main difference is that it doesn't insert a new dot, and it actually _removes_ the element.
    /// The removed dots are returned to be replicated.
    pub fn remove_elements(
        &self,
        set_name: &str,
        elements: &[Bytes],
        dot: Dot,
    ) -> Result<Vec<Dot>> {
        if elements.is_empty() {
            return Ok(vec![]);
        }

        let mut conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        let tx = conn.transaction()?;

        // Get the set_id (exit if it doesn't exist)
        let set_id: Option<i64> = tx
            .query_row("SELECT id FROM sets WHERE name = ?1", [set_name], |row| {
                row.get(0)
            })
            .optional()?;

        let set_id = match set_id {
            Some(id) => id,
            None => {
                // Set doesn't exist, nothing to remove
                println!("Set {} doesn't exist", set_name);
                return Ok(vec![]);
            }
        };

        let mut deleted = Vec::new();
        let actor_id = dot.actor_id.bytes();

        for element in elements {
            let mut stmt = tx.prepare(
                "DELETE FROM dots
                        WHERE element_id IN (
                            SELECT id FROM elements
                            WHERE set_id =  ?1
                            AND value = ?2
                        )
                        RETURNING actor_id, counter",
            )?;

            let rows = stmt.query_map(rusqlite::params![set_id, element.as_ref()], |row| {
                Ok(Dot::from_parts(row.get(0)?, row.get(1)?)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?)
            })?;

            for r in rows {
                trace!("Deleted {:?} dots for element {:?}", r, element);
                deleted.push(r?);
            }
            drop(stmt);

            // Only delete the element if we found dots for it (meaning it existed)
            if !deleted.is_empty() {
                tx.execute(
                    "DELETE FROM elements
                                WHERE set_id = (SELECT id FROM sets WHERE name = ?1)
                                AND value = ?2",
                    rusqlite::params![set_name, element.as_ref()],
                )?;
            }
        }

        // Update version vector with the new dot
        tx.execute(
            "INSERT INTO version_vector (actor_id, counter) VALUES (?1, ?2) ON CONFLICT(actor_id) DO UPDATE SET counter = MAX(counter, excluded.counter)",
            rusqlite::params![actor_id, dot.counter],
        )?;

        tx.commit()?;
        Ok(deleted)
    }

    /// Since we don't have tombstones this is simply the set of elements for the given set.
    pub fn get_elements(&self, set_name: &str) -> Result<Vec<Bytes>> {
        let conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        let mut stmt = conn.prepare(
            r#"
                SELECT e.value
                FROM elements e
                JOIN sets s ON s.id = e.set_id
                WHERE s.name = ?1
                ORDER BY e.id;
                "#,
        )?;
        let rows = stmt.query_map([set_name], |row| {
            let value: Vec<u8> = row.get(0)?;
            Ok(Bytes::from(value))
        })?;

        rows.collect::<Result<Vec<Bytes>>>()
    }

    /// Return the count of elements in the set
    pub fn count_elements(&self, set_name: &str) -> Result<u64> {
        let conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        // Get cardinality
        let count: u64 = conn.query_row(
            r#"
                SELECT COUNT(e.id)
                FROM elements e
                JOIN sets s ON s.id = e.set_id
                WHERE s.name = ?1;
                "#,
            [set_name],
            |row| row.get(0),
        )?;

        Ok(count)
    }

    // given an element, true if it is present in the set at this replica
    pub fn is_member(&self, set_name: &str, element: &Bytes) -> Result<bool> {
        let conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        let exists: i64 = conn.query_row(
            r#"
                SELECT EXISTS (
                  SELECT 1
                  FROM elements e
                  JOIN sets s ON s.id = e.set_id
                  WHERE s.name = ?1
                    AND e.value = ?2
                );
                "#,
            rusqlite::params![set_name, element.as_ref()],
            |row| row.get(0),
        )?;
        Ok(exists != 0)
    }

    // Given elements, returns a vec of bool, positionally matching the elements where
    // true is in the set, and false is not.
    pub fn are_members(&self, set_name: &str, elements: &[Bytes]) -> Result<Vec<bool>> {
        if elements.is_empty() {
            return Ok(Vec::new());
        }

        let conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        // Build "(?),(?),(?)" for vals(value)
        let vals_placeholders = std::iter::repeat("(?)")
            .take(elements.len())
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            r#"
                WITH
                s AS (
                  SELECT id AS set_id FROM sets WHERE name = ?1
                ),
                vals(value) AS (VALUES {vals}),
                joined AS (
                  SELECT v.value, e.value AS present
                  FROM vals v
                  LEFT JOIN elements e
                    ON e.value = v.value
                   AND e.set_id = (SELECT set_id FROM s)
                )
                SELECT CASE WHEN present IS NOT NULL THEN 1 ELSE 0 END
                FROM joined;
                "#,
            vals = vals_placeholders
        );
        let element_slices: Vec<&[u8]> = elements.iter().map(|e| e.as_ref()).collect();

        // Bind params: ?1 = set_name, then the element values
        let mut params: Vec<&dyn ToSql> = vec![&set_name];
        params.extend(element_slices.iter().map(|s| s as &dyn ToSql));

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
            let val: i64 = row.get(0)?;
            Ok(val != 0)
        })?;

        let mut out = Vec::with_capacity(elements.len());
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// A replication received add event.
    /// Assumption is that if the `Dot` of the event has already been observed this method will not be called.
    ///
    /// Much like add_elements above, here the given dot is added for each of the elements,
    /// and all the dots on removed_dots are removed from the set of supporting dots for each added element.
    /// Another way to implement this would be to use the remote actors version vector to remove all dots for the given
    /// elements (and that is (maybe?) a better idea, but demands causal consistency)
    pub fn replicate_add(
        &self,
        set_name: &str,
        elements: &[Bytes],
        removed_dots: &[Dot],
        dot: Dot,
    ) -> Result<()> {
        if elements.is_empty() {
            return Ok(());
        }

        let mut conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        let tx = conn.transaction()?;

        // Get the set_id (creating if needed)
        let set_id: i64 = tx.query_row(
            "INSERT INTO sets (name) VALUES (?1) ON CONFLICT(name) DO UPDATE SET name=name RETURNING id",
            [set_name],
            |row| row.get(0),
        )?;

        let actor_id = dot.actor_id.bytes();

        // For each element
        for element in elements {
            // Insert element (or get existing element_id)
            let element_id: i64 = tx.query_row(
                "INSERT INTO elements (set_id, value) VALUES (?1, ?2) ON CONFLICT(set_id, value) DO UPDATE SET value=value RETURNING id",
                rusqlite::params![set_id, element.as_ref()],
                |row| row.get(0),
            )?;

            // remove each dot from the remove set for this element
            if !removed_dots.is_empty() {
                let placeholders = std::iter::repeat("(?, ?)")
                    .take(removed_dots.len())
                    .collect::<Vec<_>>()
                    .join(", ");

                let sql = format!(
                    "DELETE FROM dots WHERE element_id = ?1 AND (actor_id, counter) IN ({})",
                    placeholders
                );

                // Collect actor_id bytes first to ensure stable lifetimes
                let removed_dots_params: Vec<(&[u8], u64)> = removed_dots
                    .iter()
                    .map(|d| (d.actor_id.bytes(), d.counter))
                    .collect();

                let mut params: Vec<&dyn ToSql> = vec![&element_id];
                for i in 0..removed_dots.len() {
                    params.push(&removed_dots_params[i].0);
                    params.push(&removed_dots_params[i].1);
                }

                tx.execute(&sql, rusqlite::params_from_iter(params))?;
            }

            // Insert the new dot for this element_id
            tx.execute(
                "INSERT INTO dots (element_id, actor_id, counter) VALUES (?1, ?2, ?3)",
                rusqlite::params![element_id, actor_id, dot.counter],
            )?;
        }

        // Update version vector with the new dot
        tx.execute(
            "INSERT INTO version_vector (actor_id, counter) VALUES (?1, ?2) ON CONFLICT(actor_id) DO UPDATE SET counter = MAX(counter, excluded.counter)",
            rusqlite::params![actor_id, dot.counter],
        )?;

        tx.commit()?;
        Ok(())
    }

    /// A replication received remove event.
    /// Assumption is that if the `Dot` of the event has already been observed this method will not be called.
    ///
    /// Much like replicated_add aboce, all the dots in removed_dots are removed from the set of supporting dots for each added element.
    /// Another way to implement this would be to use the remote actors version vector to remove all dots for the given
    /// elements (and that is maybe a better idea, but demands causal consistency).
    /// If any element has no dots left, it is removed from the set.
    pub fn replicate_remove(
        &self,
        set_name: &str,
        elements: &[Bytes],
        removed_dots: &[Dot],
        dot: Dot,
    ) -> Result<()> {
        if elements.is_empty() {
            return Ok(());
        }

        let mut conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        let tx = conn.transaction()?;

        // Get the set_id (exit if it doesn't exist)
        let set_id: Option<i64> = tx
            .query_row("SELECT id FROM sets WHERE name = ?1", [set_name], |row| {
                row.get(0)
            })
            .optional()?;

        let set_id = match set_id {
            Some(id) => id,
            None => {
                // Set doesn't exist, nothing to remove, it would be an error to be here
                return Ok(());
            }
        };

        let actor_id = dot.actor_id.bytes();

        // For each element
        for element in elements {
            // Get existing element_id (skip this element if no such element)
            let element_id: Option<i64> = tx
                .query_row(
                    "SELECT id FROM elements WHERE set_id = ?1 AND value = ?2",
                    rusqlite::params![set_id, element.as_ref()],
                    |row| row.get(0),
                )
                .optional()?;

            if let Some(element_id) = element_id {
                // Remove each of the removed_dots for this element
                if !removed_dots.is_empty() {
                    let placeholders = std::iter::repeat("(?, ?)")
                        .take(removed_dots.len())
                        .collect::<Vec<_>>()
                        .join(", ");

                    let sql = format!(
                        "DELETE FROM dots WHERE element_id = ?1 AND (actor_id, counter) IN ({})",
                        placeholders
                    );

                    // Collect actor_id bytes first to ensure stable lifetimes
                    let removed_dots_params: Vec<(&[u8], u64)> = removed_dots
                        .iter()
                        .map(|d| (d.actor_id.bytes(), d.counter))
                        .collect();

                    let mut params: Vec<&dyn ToSql> = vec![&element_id];
                    for i in 0..removed_dots.len() {
                        params.push(&removed_dots_params[i].0);
                        params.push(&removed_dots_params[i].1);
                    }

                    tx.execute(&sql, rusqlite::params_from_iter(params))?;
                }

                // If there are no dots left for this element, remove the element
                let dot_count: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM dots WHERE element_id = ?1",
                    [element_id],
                    |row| row.get(0),
                )?;

                if dot_count == 0 {
                    tx.execute("DELETE FROM elements WHERE id = ?1", [element_id])?;
                }
            }
        }

        // Update version vector with the new dot
        tx.execute(
            "INSERT INTO version_vector (actor_id, counter) VALUES (?1, ?2) ON CONFLICT(actor_id) DO UPDATE SET counter = MAX(counter, excluded.counter)",
            rusqlite::params![actor_id, dot.counter],
        )?;

        tx.commit()?;
        Ok(())
    }
}
