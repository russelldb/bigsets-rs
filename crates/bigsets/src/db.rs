use crate::config::StorageConfig;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Result;
use std::path::Path;

pub type DbPool = Pool<SqliteConnectionManager>;

pub struct Database {
    pool: DbPool,
}

impl Database {
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

        Ok(Database { pool })
    }

    fn create_schema(conn: &rusqlite::Connection) -> Result<()> {
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

    pub fn pool(&self) -> &DbPool {
        &self.pool
    }
}
