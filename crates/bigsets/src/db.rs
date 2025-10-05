use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Result;
use std::path::Path;
use crate::config::StorageConfig;

pub type DbPool = Pool<SqliteConnectionManager>;

pub struct Database {
    pool: DbPool,
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P, config: &StorageConfig) -> Result<Self> {
        let cache_size = config.sqlite_cache_size;
        let busy_timeout = config.sqlite_busy_timeout;

        let manager = SqliteConnectionManager::file(path)
            .with_init(move |conn| {
                conn.pragma_update(None, "cache_size", cache_size)?;
                conn.pragma_update(None, "busy_timeout", busy_timeout)?;
                conn.pragma_update(None, "journal_mode", "WAL")?;
                conn.pragma_update(None, "synchronous", "NORMAL")?;
                Ok(())
            });

        let pool = Pool::builder()
            .max_size(15)
            .build(manager)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        let db = Database { pool };
        db.initialize_schema()?;

        Ok(db)
    }

    fn initialize_schema(&self) -> Result<()> {
        let conn = self.pool.get()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        conn.execute_batch(
            r#"
            -- Sets namespace
            CREATE TABLE IF NOT EXISTS sets (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            );

            -- Version vector per set (critical for causal consistency)
            CREATE TABLE IF NOT EXISTS version_vectors (
                set_id INTEGER NOT NULL,
                actor_id TEXT NOT NULL,
                counter INTEGER NOT NULL,
                PRIMARY KEY (set_id, actor_id),
                FOREIGN KEY (set_id) REFERENCES sets(id) ON DELETE CASCADE
            );

            -- Unique element values (deduplication)
            CREATE TABLE IF NOT EXISTS elements (
                id INTEGER PRIMARY KEY,
                set_id INTEGER NOT NULL,
                value BLOB NOT NULL,
                FOREIGN KEY (set_id) REFERENCES sets(id) ON DELETE CASCADE,
                UNIQUE (set_id, value)
            );

            -- Dots pointing to elements (ORSWOT: multiple concurrent adds)
            CREATE TABLE IF NOT EXISTS dots (
                element_id INTEGER NOT NULL,
                actor_id TEXT NOT NULL,
                counter INTEGER NOT NULL,
                PRIMARY KEY (element_id, actor_id, counter),
                FOREIGN KEY (element_id) REFERENCES elements(id) ON DELETE CASCADE
            );

            -- Indexes for performance
            CREATE INDEX IF NOT EXISTS idx_elements_set_value ON elements(set_id, value);
            CREATE INDEX IF NOT EXISTS idx_dots_element ON dots(element_id);
            "#
        )?;

        Ok(())
    }

    pub fn pool(&self) -> &DbPool {
        &self.pool
    }
}
