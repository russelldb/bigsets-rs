use crate::config::StorageConfig;
use crate::sqlf;
use crate::storage::Storage;
use crate::storage::sql_utils::{placeholders_1, placeholders_2};
use crate::types::{ActorId, Dot, VersionVector};
use bytes::Bytes;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, Result, ToSql};
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

    fn execute_local_update(
        &self,
        set_name: &str,
        elements: &[Bytes],
        dot: Dot,
        sql: String,
    ) -> Result<Vec<Dot>> {
        let mut conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        if elements.is_empty() {
            return Ok(vec![]);
        }

        let tx = conn.transaction()?;

        // Param order must match ?1.. in the SQL:
        //   ?1 = set_name
        //   ?2 = dot.actor_id
        //   ?3 = dot.counter
        //   ?4.. = each element value
        let actor_id = dot.actor_id.bytes();
        let element_slices: Vec<&[u8]> = elements.iter().map(|e| e.as_ref()).collect();

        let mut bind: Vec<&dyn ToSql> = vec![&set_name, &actor_id, &dot.counter];
        bind.extend(element_slices.iter().map(|s| s as &dyn ToSql));
        let mut deleted = Vec::new();
        // Prepare and execute query in a scope to drop stmt before commit
        {
            let mut stmt = tx.prepare(&sql)?;
            let rows = stmt.query_map(rusqlite::params_from_iter(bind), |row| {
                Ok(Dot::from_parts(row.get(1)?, row.get(2)?)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?)
            })?;

            for r in rows {
                let dot = r?;
                deleted.push(dot);
            }
        }
        tx.commit()?;
        Ok(deleted)
    }

    fn execute_remote_update(
        &self,
        set_name: &str,
        elements: &[Bytes],
        removed_dots: &[Dot],
        dot: Dot,
        sql: String,
    ) -> Result<()> {
        let mut conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        let mut sql = sql;
        // If inputs are empty, make the CTEs zero-row so we need no extra bindings.
        if removed_dots.is_empty() {
            sql = sql.replace(
                "delpairs(actor_id, counter) AS (VALUES (?,?))",
                "delpairs(actor_id, counter) AS (SELECT X'', 0 WHERE 0)",
            );
        }
        if elements.is_empty() {
            sql = sql.replace(
                "vals(value) AS (VALUES (?))",
                "vals(value) AS (SELECT X'' WHERE 0)",
            );
        }

        // Bind order:
        //   ?1 = new_actor_id
        //   ?2 = new_counter
        //   ?3 = set_name
        //   then all delpairs (actor_id, counter)*,
        //   then all element values
        let element_slices: Vec<&[u8]> = elements.iter().map(|e| e.as_ref()).collect();
        let removed_dots_params: Vec<(&[u8], u64)> = removed_dots
            .iter()
            .map(|d| (d.actor_id.bytes(), d.counter))
            .collect();
        let actor_id = &dot.actor_id.bytes();
        let mut params: Vec<&dyn ToSql> = vec![actor_id, &dot.counter, &set_name];
        for i in 0..removed_dots.len() {
            params.push(&removed_dots_params[i].0);
            params.push(&removed_dots_params[i].1);
        }
        params.extend(element_slices.iter().map(|s| s as &dyn ToSql));

        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare(&sql)?;
            let _ = stmt.execute(rusqlite::params_from_iter(params))?;
        }
        tx.commit()?;
        Ok(())
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

    fn add_elements(&self, set_name: &str, elements: &[Bytes], dot: Dot) -> Result<Vec<Dot>> {
        if elements.is_empty() {
            return Ok(vec![]);
        }

        // Build "(?),(?),(?)" for vals(value)
        let vals = placeholders_1(elements.len());
        let sql = sqlf!("../../sql/add_elems.sql", vals = vals);
        self.execute_local_update(set_name, elements, dot, sql)
    }

    fn remove_elements(&self, set_name: &str, elements: &[Bytes], dot: Dot) -> Result<Vec<Dot>> {
        if elements.is_empty() {
            return Ok(vec![]);
        }

        let vals = placeholders_1(elements.len());
        let sql = sqlf!("../../sql/remove_elems.sql", vals = vals);
        self.execute_local_update(set_name, elements, dot, sql)
    }

    fn get_elements(&self, set_name: &str) -> Result<Vec<Bytes>> {
        let conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        let mut stmt = conn.prepare(
            r#"
                SELECT e.id AS element_id, e.value
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

    fn count_elements(&self, set_name: &str) -> Result<i64> {
        let conn = self
            .pool
            .get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        // Get cardinality
        let count: i64 = conn.query_row(
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

    fn is_member(&self, set_name: &str, element: &Bytes) -> Result<bool> {
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

    fn are_members(&self, set_name: &str, elements: &[Bytes]) -> Result<Vec<bool>> {
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

    fn remote_add_elements(
        &self,
        set_name: &str,
        elements: &[Bytes],
        removed_dots: &[Dot],
        dot: Dot,
    ) -> Result<()> {
        let vals_ph = placeholders_1(elements.len());
        let delpairs_ph = placeholders_2(removed_dots.len());

        let sql = format!(
            include_str!("../../sql/remote_add.sql"),
            vals = vals_ph,
            delpairs = delpairs_ph,
        );

        self.execute_remote_update(set_name, elements, removed_dots, dot, sql)
    }

    fn remote_remove_elements(
        &self,
        set_name: &str,
        elements: &[Bytes],
        removed_dots: &[Dot],
        dot: Dot,
    ) -> Result<()> {
        let vals_ph = placeholders_1(elements.len());
        let delpairs_ph = placeholders_2(removed_dots.len());

        let sql = format!(
            include_str!("../../sql/remote_rem.sql"),
            vals = vals_ph,
            delpairs = delpairs_ph,
        );

        self.execute_remote_update(set_name, elements, removed_dots, dot, sql)
    }
}
