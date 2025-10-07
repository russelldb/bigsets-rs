use crate::config::StorageConfig;
use crate::storage::Storage;
use crate::types::{ActorId, Dot, VersionVector};
use bytes::Bytes;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, Result};
use std::collections::HashMap;
use std::path::Path;

pub type DbPool = Pool<SqliteConnectionManager>;

/// SQLite implementation of the Storage trait
pub struct SqliteStorage {
    pool: DbPool,
}

impl SqliteStorage {
    pub fn open<P: AsRef<Path>>(path: P, config: &StorageConfig) -> Result<Self> {
        let cache_size = config.sqlite_cache_size;
        let busy_timeout = config.sqlite_busy_timeout;
        let path_ref = path.as_ref();

        // Initialize schema with a single connection first
        {
            let conn = rusqlite::Connection::open(path_ref)?;
            conn.pragma_update(None, "cache_size", cache_size)?;
            conn.pragma_update(None, "busy_timeout", busy_timeout)?;
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", "NORMAL")?;

            Self::create_schema(&conn)?;
        }

        // Now create the pool - schema already exists
        let manager = SqliteConnectionManager::file(path_ref).with_init(move |conn| {
            conn.pragma_update(None, "cache_size", cache_size)?;
            conn.pragma_update(None, "busy_timeout", busy_timeout)?;
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", "NORMAL")?;
            Ok(())
        });

        let pool = Pool::builder()
            .max_size(5) // Sized for concurrent reads only (writes are serialized by VV lock)
            .min_idle(Some(1))
            .build(manager)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        Ok(SqliteStorage { pool })
    }

    fn create_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            r#"
            -- Sets namespace
            CREATE TABLE IF NOT EXISTS sets (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            );

            -- Global version vector (tracks causality across entire database)
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

            -- Dots pointing to elements (one dot per element per actor)
            CREATE TABLE IF NOT EXISTS dots (
                element_id INTEGER NOT NULL,
                actor_id BLOB NOT NULL,  -- 4-byte ActorId
                counter INTEGER NOT NULL,
                PRIMARY KEY (element_id, actor_id),
                FOREIGN KEY (element_id) REFERENCES elements(id) ON DELETE CASCADE
            );

            -- Indexes for performance
            CREATE INDEX IF NOT EXISTS idx_elements_set_value ON elements(set_id, value);
            CREATE INDEX IF NOT EXISTS idx_dots_element ON dots(element_id);
            "#,
        )?;

        Ok(())
    }

    /// Get set ID by name, or None if it doesn't exist
    fn get_set(conn: &Connection, set_name: &str) -> Result<Option<u64>> {
        let mut stmt = conn.prepare("SELECT id FROM sets WHERE name = ?")?;
        let result: Result<u64> = stmt.query_row([set_name], |row| row.get(0));

        match result {
            Ok(id) => Ok(Some(id)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Get or create a set by name, returning its ID
    fn get_or_create_set(conn: &Connection, set_name: &str) -> Result<u64> {
        if let Some(id) = Self::get_set(conn, set_name)? {
            return Ok(id);
        }

        // Create new set
        conn.execute("INSERT INTO sets (name) VALUES (?)", [set_name])?;
        Ok(conn.last_insert_rowid() as u64)
    }

    pub fn pool(&self) -> &DbPool {
        &self.pool
    }
}

impl Storage for SqliteStorage {
    fn load_vv(&self) -> Result<VersionVector> {
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

    fn store_vv(&self, vv: &VersionVector) -> Result<()> {
        let mut conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        let tx = conn.transaction()?;

        for (actor_id, counter) in &vv.counters {
            let actor_bytes = actor_id.to_bytes();
            tx.execute(
                "INSERT INTO version_vector (actor_id, counter) VALUES (?, ?)
                 ON CONFLICT (actor_id) DO UPDATE SET counter = excluded.counter",
                rusqlite::params![&actor_bytes[..], counter],
            )?;
        }

        tx.commit()
    }

    fn add_elements(
        &self,
        set_name: &str,
        elements: &[Bytes],
        dot: Dot,
        removed_dots: &[Dot],
        vv: &VersionVector,
    ) -> Result<()> {
        let mut conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        let tx = conn.transaction()?;

        // Get or create the set
        let set_id = Self::get_or_create_set(&tx, set_name)?;

        if !elements.is_empty() {
            // Batch insert elements
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

        // Store VV
        for (actor_id, counter) in &vv.counters {
            let actor_bytes = actor_id.to_bytes();
            tx.execute(
                "INSERT INTO version_vector (actor_id, counter) VALUES (?, ?)
                 ON CONFLICT (actor_id) DO UPDATE SET counter = MAX(counter, excluded.counter)",
                rusqlite::params![&actor_bytes[..], counter],
            )?;
        }

        tx.commit()
    }

    fn remove_elements(
        &self,
        set_name: &str,
        elements: &[Bytes],
        removed_dots: &[Dot],
        vv: &VersionVector,
    ) -> Result<()> {
        let mut conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        let tx = conn.transaction()?;

        // Get set ID (if doesn't exist, nothing to remove)
        let set_id = match Self::get_set(&tx, set_name)? {
            Some(id) => id,
            None => {
                tx.commit()?;
                return Ok(());
            }
        };

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

        // Store VV
        for (actor_id, counter) in &vv.counters {
            let actor_bytes = actor_id.to_bytes();
            tx.execute(
                "INSERT INTO version_vector (actor_id, counter) VALUES (?, ?)
                 ON CONFLICT (actor_id) DO UPDATE SET counter = MAX(counter, excluded.counter)",
                rusqlite::params![&actor_bytes[..], counter],
            )?;
        }

        tx.commit()
    }

    fn get_elements(&self, set_name: &str) -> Result<Vec<Bytes>> {
        let conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        // Get set ID (return empty if doesn't exist)
        let set_id = match Self::get_set(&conn, set_name)? {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };

        // Get all members
        let mut stmt = conn.prepare("SELECT value FROM elements WHERE set_id = ?")?;
        let rows = stmt.query_map([set_id], |row| {
            let value: Vec<u8> = row.get(0)?;
            Ok(Bytes::from(value))
        })?;

        rows.collect::<Result<Vec<Bytes>>>()
    }

    fn count_elements(&self, set_name: &str) -> Result<i64> {
        let conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        // Get set ID (return 0 if doesn't exist)
        let set_id = match Self::get_set(&conn, set_name)? {
            Some(id) => id,
            None => return Ok(0),
        };

        // Get cardinality
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM elements WHERE set_id = ?",
            [set_id],
            |row| row.get(0),
        )?;

        Ok(count)
    }

    fn is_member(&self, set_name: &str, element: &Bytes) -> Result<bool> {
        let conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        // Get set ID (return false if doesn't exist)
        let set_id = match Self::get_set(&conn, set_name)? {
            Some(id) => id,
            None => return Ok(false),
        };

        // Check membership
        let exists: i64 = conn.query_row(
            "SELECT COUNT(*) FROM elements WHERE set_id = ? AND value = ?",
            rusqlite::params![set_id, element.as_ref()],
            |row| row.get(0),
        )?;

        Ok(exists > 0)
    }

    fn are_members(&self, set_name: &str, elements: &[Bytes]) -> Result<Vec<bool>> {
        if elements.is_empty() {
            return Ok(Vec::new());
        }

        let conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        // Get set ID (return all false if doesn't exist)
        let set_id = match Self::get_set(&conn, set_name)? {
            Some(id) => id,
            None => return Ok(vec![false; elements.len()]),
        };

        // Batch check membership using IN clause
        let placeholders = elements.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let query = format!(
            "SELECT value FROM elements WHERE set_id = ? AND value IN ({})",
            placeholders
        );

        // Build params
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(set_id)];
        for element in elements {
            params.push(Box::new(element.to_vec()));
        }
        let params_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();

        // Query and collect existing values
        let mut stmt = conn.prepare(&query)?;
        let existing_values: std::collections::HashSet<Vec<u8>> = stmt
            .query_map(params_refs.as_slice(), |row| row.get::<_, Vec<u8>>(0))?
            .collect::<Result<_>>()?;

        // Build result array
        let result = elements
            .iter()
            .map(|element| existing_values.contains(element.as_ref()))
            .collect();

        Ok(result)
    }
}
