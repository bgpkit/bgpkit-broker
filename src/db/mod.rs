//! Database module for BGPKIT Broker.
//!
//! This module provides database backends for the broker service.
//! Currently supported backends:
//! - SQLite (default, for single-instance deployments)
//! - PostgreSQL (for scalable, production deployments)
//!
//! # Usage
//!
//! Both backends implement the `BrokerDb` trait, allowing transparent switching:
//!
//! ```ignore
//! use bgpkit_broker::db::{BrokerDb, SqliteDb, PostgresDb};
//!
//! // SQLite backend
//! let sqlite_db = SqliteDb::new("broker.sqlite3").await?;
//!
//! // PostgreSQL backend (from environment variables)
//! let pg_db = PostgresDb::from_env().await?;
//! ```

pub mod postgres;
pub mod sqlite;
mod traits;
mod utils;

// Re-export the trait and common types
pub use traits::{BrokerDb, DbSearchResult, UpdatesMeta, DEFAULT_PAGE_SIZE};

// Re-export backend implementations
pub use postgres::{PostgresConfig, PostgresDb};
pub use sqlite::SqliteDb;

// Legacy alias for backward compatibility
pub type LocalBrokerDb = SqliteDb;

use crate::query::BrokerCollector;
use crate::{BrokerError, BrokerItem, Collector};
use async_trait::async_trait;
use chrono::NaiveDateTime;

/// A unified database backend that can be either SQLite or PostgreSQL.
/// This enum provides a single type that can be used throughout the application
/// regardless of which backend is configured.
#[derive(Clone)]
pub enum DatabaseBackend {
    Sqlite(SqliteDb),
    Postgres(PostgresDb),
}

impl DatabaseBackend {
    /// Create a database backend from CLI arguments and environment.
    /// If db_path is provided, uses SQLite. Otherwise, tries PostgreSQL from env.
    pub async fn from_config(db_path: Option<String>) -> Result<Self, BrokerError> {
        match db_path {
            Some(path) => {
                tracing::info!("using SQLite database: {}", path);
                let db = SqliteDb::new(&path).await?;
                Ok(DatabaseBackend::Sqlite(db))
            }
            None => {
                if PostgresConfig::is_configured() {
                    tracing::info!("using PostgreSQL database from environment");
                    let config = PostgresConfig::from_env()?;
                    let db = PostgresDb::new(config).await?;
                    Ok(DatabaseBackend::Postgres(db))
                } else {
                    Err(BrokerError::BrokerError(
                        "No database configured. Provide a SQLite path or set BROKER_DATABASE_* environment variables.".to_string()
                    ))
                }
            }
        }
    }

    /// Check if this is a SQLite backend.
    pub fn is_sqlite(&self) -> bool {
        matches!(self, DatabaseBackend::Sqlite(_))
    }

    /// Check if this is a PostgreSQL backend.
    pub fn is_postgres(&self) -> bool {
        matches!(self, DatabaseBackend::Postgres(_))
    }

    /// Get the backend name for logging.
    pub fn backend_name(&self) -> &'static str {
        match self {
            DatabaseBackend::Sqlite(_) => "SQLite",
            DatabaseBackend::Postgres(_) => "PostgreSQL",
        }
    }
}

#[async_trait]
impl BrokerDb for DatabaseBackend {
    fn collectors(&self) -> Vec<BrokerCollector> {
        match self {
            DatabaseBackend::Sqlite(db) => db.collectors(),
            DatabaseBackend::Postgres(db) => db.collectors(),
        }
    }

    async fn reload_collectors(&mut self) -> Result<(), BrokerError> {
        match self {
            DatabaseBackend::Sqlite(db) => db.reload_collectors().await,
            DatabaseBackend::Postgres(db) => db.reload_collectors().await,
        }
    }

    async fn analyze(&self) -> Result<(), BrokerError> {
        match self {
            DatabaseBackend::Sqlite(db) => db.analyze().await,
            DatabaseBackend::Postgres(db) => db.analyze().await,
        }
    }

    async fn search(
        &self,
        collectors: Option<Vec<String>>,
        project: Option<String>,
        data_type: Option<String>,
        ts_start: Option<NaiveDateTime>,
        ts_end: Option<NaiveDateTime>,
        page: Option<usize>,
        page_size: Option<usize>,
    ) -> Result<DbSearchResult, BrokerError> {
        match self {
            DatabaseBackend::Sqlite(db) => {
                db.search(
                    collectors, project, data_type, ts_start, ts_end, page, page_size,
                )
                .await
            }
            DatabaseBackend::Postgres(db) => {
                db.search(
                    collectors, project, data_type, ts_start, ts_end, page, page_size,
                )
                .await
            }
        }
    }

    async fn insert_items(
        &self,
        items: &[BrokerItem],
        update_latest: bool,
    ) -> Result<Vec<BrokerItem>, BrokerError> {
        match self {
            DatabaseBackend::Sqlite(db) => db.insert_items(items, update_latest).await,
            DatabaseBackend::Postgres(db) => db.insert_items(items, update_latest).await,
        }
    }

    async fn insert_collector(&self, collector: &Collector) -> Result<(), BrokerError> {
        match self {
            DatabaseBackend::Sqlite(db) => db.insert_collector(collector).await,
            DatabaseBackend::Postgres(db) => db.insert_collector(collector).await,
        }
    }

    async fn get_latest_timestamp(&self) -> Result<Option<NaiveDateTime>, BrokerError> {
        match self {
            DatabaseBackend::Sqlite(db) => db.get_latest_timestamp().await,
            DatabaseBackend::Postgres(db) => db.get_latest_timestamp().await,
        }
    }

    async fn get_latest_files(&self) -> Vec<BrokerItem> {
        match self {
            DatabaseBackend::Sqlite(db) => db.get_latest_files().await,
            DatabaseBackend::Postgres(db) => db.get_latest_files().await,
        }
    }

    async fn update_latest_files(&self, files: &[BrokerItem], bootstrap: bool) {
        match self {
            DatabaseBackend::Sqlite(db) => db.update_latest_files(files, bootstrap).await,
            DatabaseBackend::Postgres(db) => db.update_latest_files(files, bootstrap).await,
        }
    }

    async fn bootstrap_latest_table(&self) {
        match self {
            DatabaseBackend::Sqlite(db) => db.bootstrap_latest_table().await,
            DatabaseBackend::Postgres(db) => db.bootstrap_latest_table().await,
        }
    }

    async fn insert_meta(
        &self,
        crawl_duration: i32,
        item_inserted: i32,
    ) -> Result<Vec<UpdatesMeta>, BrokerError> {
        match self {
            DatabaseBackend::Sqlite(db) => db.insert_meta(crawl_duration, item_inserted).await,
            DatabaseBackend::Postgres(db) => db.insert_meta(crawl_duration, item_inserted).await,
        }
    }

    async fn get_latest_updates_meta(&self) -> Result<Option<UpdatesMeta>, BrokerError> {
        match self {
            DatabaseBackend::Sqlite(db) => db.get_latest_updates_meta().await,
            DatabaseBackend::Postgres(db) => db.get_latest_updates_meta().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BrokerItem;
    use chrono::DateTime;
    use std::path::PathBuf;

    /// Helper function to create a temporary database file path
    fn create_temp_db_path(test_name: &str) -> PathBuf {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push(format!(
            "bgpkit_broker_test_{}_{}.sqlite3",
            test_name,
            chrono::Utc::now().timestamp_millis()
        ));
        temp_dir
    }

    /// Helper function to ensure cleanup of database files
    fn cleanup_db_file(path: &PathBuf) {
        if path.exists() {
            let _ = std::fs::remove_file(path);
        }
        let wal_path = path.with_extension("sqlite3-wal");
        if wal_path.exists() {
            let _ = std::fs::remove_file(wal_path);
        }
        let shm_path = path.with_extension("sqlite3-shm");
        if shm_path.exists() {
            let _ = std::fs::remove_file(shm_path);
        }
    }

    #[tokio::test]
    async fn test_sqlite_basic() {
        let db_path = create_temp_db_path("test");
        let db_path_str = db_path.to_str().unwrap();

        let db = SqliteDb::new(db_path_str).await.unwrap();

        let result = db
            .search(
                Some(vec!["rrc21".to_string(), "route-views2".to_string()]),
                None,
                Some("rib".to_string()),
                Some(DateTime::from_timestamp(1672531200, 0).unwrap().naive_utc()),
                Some(DateTime::from_timestamp(1672617600, 0).unwrap().naive_utc()),
                None,
                None,
            )
            .await
            .unwrap();

        assert!(result.items.is_empty());
        assert_eq!(result.total, 0);

        drop(db);
        cleanup_db_file(&db_path);
    }

    #[tokio::test]
    async fn test_sqlite_insert() {
        let db_path = create_temp_db_path("inserts");
        let db_path_str = db_path.to_str().unwrap();

        let mut db = SqliteDb::new(db_path_str).await.unwrap();

        use crate::Collector;

        let test_collectors = vec![
            Collector {
                id: "rrc00".to_string(),
                project: "riperis".to_string(),
                url: "https://data.ris.ripe.net/rrc00/".to_string(),
            },
            Collector {
                id: "rrc01".to_string(),
                project: "riperis".to_string(),
                url: "https://data.ris.ripe.net/rrc01/".to_string(),
            },
        ];

        for collector in &test_collectors {
            db.insert_collector(collector).await.unwrap();
        }
        db.reload_collectors().await.unwrap();

        drop(db);
        cleanup_db_file(&db_path);
    }
}
