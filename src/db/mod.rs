mod latest_files;
mod meta;
mod utils;

use crate::db::utils::infer_url;
use crate::query::{BrokerCollector, BrokerItemType};
use crate::{BrokerError, BrokerItem, Collector};
use chrono::{DateTime, Duration, NaiveDateTime};
use libsql::{Builder, Connection, Database};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

pub use meta::UpdatesMeta;

pub const DEFAULT_PAGE_SIZE: usize = 100;

/// Database mode: local file or remote Turso cloud
#[derive(Clone, Debug)]
pub enum DbMode {
    /// Local SQLite file
    Local { path: String },
    /// Remote Turso database
    Remote { url: String },
}

impl DbMode {
    pub fn is_local(&self) -> bool {
        matches!(self, DbMode::Local { .. })
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, DbMode::Remote { .. })
    }

    pub fn path(&self) -> Option<&str> {
        match self {
            DbMode::Local { path } => Some(path),
            DbMode::Remote { .. } => None,
        }
    }
}

#[derive(Clone)]
pub struct LocalBrokerDb {
    db: Arc<Database>,
    mode: DbMode,
    collectors: Arc<RwLock<Vec<BrokerCollector>>>,
    types: Arc<RwLock<Vec<BrokerItemType>>>,
}

pub struct DbSearchResult {
    pub items: Vec<BrokerItem>,
    pub page: usize,
    pub page_size: usize,
    pub total: usize,
}

fn get_ts_start_clause(ts: i64) -> String {
    format!(
        r#"
            (
                (project_name='ripe-ris' AND type='updates' AND timestamp > {} - {})
                OR (project_name='route-views' AND type='updates' AND timestamp > {} - {})
                OR (type='rib' AND timestamp >= {})
            )
                "#,
        ts,
        5 * 60,
        ts,
        15 * 60,
        ts
    )
}

fn get_ts_end_clause(ts: i64) -> String {
    format!("timestamp < {}", ts)
}

impl LocalBrokerDb {
    /// Create a new LocalBrokerDb from a local file path
    pub async fn new(path: &str) -> Result<Self, BrokerError> {
        info!("opening local broker db at {}", path);
        Self::new_local(path).await
    }

    /// Create a new LocalBrokerDb from a local file path
    pub async fn new_local(path: &str) -> Result<Self, BrokerError> {
        let db = Builder::new_local(path).build().await?;

        let mut broker_db = LocalBrokerDb {
            db: Arc::new(db),
            mode: DbMode::Local {
                path: path.to_string(),
            },
            collectors: Arc::new(RwLock::new(vec![])),
            types: Arc::new(RwLock::new(vec![])),
        };
        broker_db.initialize().await?;

        Ok(broker_db)
    }

    /// Create a new LocalBrokerDb from a remote Turso database
    ///
    /// Uses TURSO_DATABASE_URL and TURSO_AUTH_TOKEN environment variables
    pub async fn new_remote(url: &str, auth_token: &str) -> Result<Self, BrokerError> {
        info!("connecting to remote Turso database at {}", url);
        let db = Builder::new_remote(url.to_string(), auth_token.to_string())
            .build()
            .await?;

        let mut broker_db = LocalBrokerDb {
            db: Arc::new(db),
            mode: DbMode::Remote {
                url: url.to_string(),
            },
            collectors: Arc::new(RwLock::new(vec![])),
            types: Arc::new(RwLock::new(vec![])),
        };
        broker_db.initialize().await?;

        Ok(broker_db)
    }

    /// Create a new LocalBrokerDb from environment variables
    ///
    /// If db_path is provided, uses local file mode.
    /// Otherwise, uses TURSO_DATABASE_URL and TURSO_AUTH_TOKEN for remote mode.
    pub async fn from_env(db_path: Option<&str>) -> Result<Self, BrokerError> {
        if let Some(path) = db_path {
            return Self::new_local(path).await;
        }

        // Try remote mode from environment variables
        let url = std::env::var("TURSO_DATABASE_URL").map_err(|_| {
            BrokerError::ConfigurationError(
                "No database path provided and TURSO_DATABASE_URL not set. \
                 Either provide a local file path or set TURSO_DATABASE_URL and TURSO_AUTH_TOKEN."
                    .to_string(),
            )
        })?;

        let auth_token = std::env::var("TURSO_AUTH_TOKEN").unwrap_or_else(|_| {
            warn!("TURSO_AUTH_TOKEN not set, using empty token");
            String::new()
        });

        Self::new_remote(&url, &auth_token).await
    }

    /// Get the database mode
    pub fn mode(&self) -> &DbMode {
        &self.mode
    }

    /// Check if this is a local database
    pub fn is_local(&self) -> bool {
        self.mode.is_local()
    }

    /// Check if this is a remote database
    pub fn is_remote(&self) -> bool {
        self.mode.is_remote()
    }

    /// Get a connection to the database
    fn connect(&self) -> Result<Connection, BrokerError> {
        self.db.connect().map_err(|e| e.into())
    }

    async fn initialize(&mut self) -> Result<(), BrokerError> {
        let conn = self.connect()?;

        // Create tables one by one to avoid execute_batch issues
        conn.execute(
            "CREATE TABLE IF NOT EXISTS meta(
                update_ts INTEGER,
                update_duration INTEGER,
                insert_count INTEGER
            )",
            (),
        )
        .await?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS collectors (
                id INTEGER PRIMARY KEY,
                name TEXT,
                url TEXT,
                project TEXT,
                updates_interval INTEGER
            )",
            (),
        )
        .await?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS types (
                id INTEGER PRIMARY KEY,
                name TEXT
            )",
            (),
        )
        .await?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS files(
                timestamp INTEGER,
                collector_id INTEGER,
                type_id INTEGER,
                rough_size INTEGER,
                exact_size INTEGER,
                constraint files_unique_pk
                    unique (timestamp, collector_id, type_id)
            )",
            (),
        )
        .await?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS latest(
                timestamp INTEGER,
                collector_name TEXT,
                type TEXT,
                rough_size INTEGER,
                exact_size INTEGER,
                constraint latest_unique_pk
                    unique (collector_name, type)
            )",
            (),
        )
        .await?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_files_timestamp ON files(timestamp)",
            (),
        )
        .await?;

        // Drop and recreate view to handle schema changes
        let _ = conn.execute("DROP VIEW IF EXISTS files_view", ()).await;

        conn.execute(
            "CREATE VIEW IF NOT EXISTS files_view AS
            SELECT
                i.timestamp, i.rough_size, i.exact_size,
                t.name AS type,
                c.name AS collector_name,
                c.url AS collector_url,
                c.project AS project_name,
                c.updates_interval AS updates_interval
            FROM collectors c
            JOIN files i ON c.id = i.collector_id
            JOIN types t ON t.id = i.type_id",
            (),
        )
        .await?;

        // Only set WAL mode for local databases
        if self.is_local() {
            // Use query instead of execute for PRAGMA since it may return rows
            let _ = conn.query("PRAGMA journal_mode=WAL", ()).await;
        }

        self.reload_collectors().await;
        self.reload_types().await;

        Ok(())
    }

    pub async fn reload_collectors(&self) {
        let conn = match self.connect() {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to connect for reload_collectors: {}", e);
                return;
            }
        };

        let mut rows = match conn
            .query(
                "SELECT id, name, url, project, updates_interval FROM collectors",
                (),
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!("Failed to query collectors: {}", e);
                return;
            }
        };

        let mut collectors = vec![];
        while let Ok(Some(row)) = rows.next().await {
            let id: i64 = row.get(0).unwrap_or(0);
            let name: String = row.get(1).unwrap_or_default();
            let url: String = row.get(2).unwrap_or_default();
            let project: String = row.get(3).unwrap_or_default();
            let updates_interval: i64 = row.get(4).unwrap_or(0);

            collectors.push(BrokerCollector {
                id,
                name,
                url,
                project,
                updates_interval,
            });
        }

        let mut guard = self.collectors.write().await;
        *guard = collectors;
    }

    async fn reload_types(&self) {
        let conn = match self.connect() {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to connect for reload_types: {}", e);
                return;
            }
        };

        let mut rows = match conn.query("SELECT id, name FROM types", ()).await {
            Ok(r) => r,
            Err(e) => {
                warn!("Failed to query types: {}", e);
                return;
            }
        };

        let mut types = vec![];
        while let Ok(Some(row)) = rows.next().await {
            let id: i64 = row.get(0).unwrap_or(0);
            let name: String = row.get(1).unwrap_or_default();
            types.push(BrokerItemType { id, name });
        }

        let mut guard = self.types.write().await;
        *guard = types;
    }

    async fn force_checkpoint(&self) {
        // Only applicable for local databases
        if self.is_remote() {
            return;
        }

        let conn = match self.connect() {
            Ok(c) => c,
            Err(_) => return,
        };
        let _ = conn.execute("PRAGMA wal_checkpoint(TRUNCATE);", ()).await;
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn search(
        &self,
        collectors: Option<Vec<String>>,
        project: Option<String>,
        data_type: Option<String>,
        ts_start: Option<NaiveDateTime>,
        ts_end: Option<NaiveDateTime>,
        page: Option<usize>,
        page_size: Option<usize>,
    ) -> Result<DbSearchResult, BrokerError> {
        let mut where_clauses: Vec<String> = vec![];
        if let Some(collectors) = collectors {
            if !collectors.is_empty() {
                let collectors_array_str = collectors
                    .into_iter()
                    .map(|c| format!("'{}'", c))
                    .collect::<Vec<String>>()
                    .join(",");
                where_clauses.push(format!("collector_name IN ({})", collectors_array_str));
            }
        }
        if let Some(project) = project {
            match project.to_lowercase().as_str() {
                "ris" | "riperis" | "ripe-ris" => {
                    where_clauses.push("project_name='ripe-ris'".to_string());
                }
                "routeviews" | "rv" | "route-views" => {
                    where_clauses.push("project_name='route-views'".to_string());
                }
                _ => {
                    return Err(BrokerError::BrokerError(format!(
                        "Unknown project: {}",
                        project
                    )));
                }
            }
        }
        if let Some(data_type) = data_type {
            match data_type.as_str() {
                "updates" | "update" | "u" => {
                    where_clauses.push("type = 'updates'".to_string());
                }
                "rib" | "ribs" | "r" => {
                    where_clauses.push("type = 'rib'".to_string());
                }
                _ => {
                    return Err(BrokerError::BrokerError(format!(
                        "Unknown data_type: {}",
                        data_type
                    )));
                }
            }
        }

        match (ts_start, ts_end) {
            (Some(ts_start), None) => {
                where_clauses.push(get_ts_start_clause(ts_start.and_utc().timestamp()));
            }
            (None, Some(ts_end)) => {
                where_clauses.push(get_ts_end_clause(ts_end.and_utc().timestamp()));
            }
            (Some(ts_start), Some(ts_end)) => {
                let start = ts_start;
                let end = match ts_start == ts_end {
                    true => {
                        // making sure when searching with the same timestamp, we always include the given timestamp
                        ts_start + Duration::seconds(1)
                    }
                    false => ts_end,
                };
                where_clauses.push(get_ts_start_clause(start.and_utc().timestamp()));
                where_clauses.push(get_ts_end_clause(end.and_utc().timestamp()));
            }
            (None, None) => {}
        }

        // page starting from 1
        let (limit, offset) = match (page, page_size) {
            (Some(page), Some(page_size)) => (page_size, page_size * (page - 1)),
            (Some(page), None) => (DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE * (page - 1)),
            (None, Some(page_size)) => (page_size, 0),
            (None, None) => (0, 0),
        };

        let limit_clause = match limit {
            0 => "".to_string(),
            _ => format!("LIMIT {} OFFSET {}", limit, offset),
        };

        // Build the WHERE clause string once to use in both queries
        let where_clause_str = match where_clauses.len() {
            0 => "".to_string(),
            _ => format!("WHERE {}", where_clauses.join(" AND ")),
        };

        let conn = self.connect()?;

        // First query: Get total count
        let count_query = format!(
            "SELECT COUNT(*) as total FROM files_view {}",
            where_clause_str
        );
        debug!("Count query: {}", count_query.as_str());

        let mut count_rows = conn.query(&count_query, ()).await?;
        let total_count: usize = if let Ok(Some(row)) = count_rows.next().await {
            row.get::<i64>(0).unwrap_or(0) as usize
        } else {
            0
        };

        // Second query: Get paginated results
        let query_string = format!(
            r#"
            SELECT collector_name, collector_url, project_name, timestamp, type, rough_size, exact_size, updates_interval
            FROM files_view
            {}
            ORDER BY timestamp ASC, type, collector_name
            {}
            "#,
            where_clause_str, limit_clause,
        );
        debug!("Data query: {}", query_string.as_str());

        let collectors_guard = self.collectors.read().await;
        let collector_name_to_info: HashMap<String, BrokerCollector> = collectors_guard
            .iter()
            .map(|c| (c.name.clone(), c.clone()))
            .collect();
        drop(collectors_guard);

        let mut data_rows = conn.query(&query_string, ()).await?;
        let mut items = vec![];

        while let Ok(Some(row)) = data_rows.next().await {
            let collector_name: String = row.get(0).unwrap_or_default();
            let timestamp: i64 = row.get(3).unwrap_or(0);
            let type_name: String = row.get(4).unwrap_or_default();
            let rough_size: i64 = row.get(5).unwrap_or(0);
            let exact_size: i64 = row.get(6).unwrap_or(0);

            if let Some(collector) = collector_name_to_info.get(&collector_name) {
                let ts_start = DateTime::from_timestamp(timestamp, 0).unwrap().naive_utc();
                let (url, ts_end) = infer_url(collector, &ts_start, type_name.as_str() == "rib");
                items.push(BrokerItem {
                    ts_start,
                    ts_end,
                    collector_id: collector_name,
                    data_type: type_name,
                    url,
                    rough_size,
                    exact_size,
                });
            }
        }

        Ok(DbSearchResult {
            items,
            page: page.unwrap_or(1),
            page_size: page_size.unwrap_or(DEFAULT_PAGE_SIZE),
            total: total_count,
        })
    }

    /// Runs the SQLite `ANALYZE` command on the database connection.
    /// Note: ANALYZE is only supported for local databases. For remote Turso databases,
    /// this is a no-op since ANALYZE is not supported over the Turso protocol.
    pub async fn analyze(&self) -> Result<(), BrokerError> {
        // ANALYZE is not supported for remote Turso databases
        if self.is_remote() {
            debug!("skipping ANALYZE for remote database");
            return Ok(());
        }

        info!("doing sqlite3 analyze...");
        let conn = self.connect()?;
        conn.execute("ANALYZE", ()).await?;
        info!("doing sqlite3 analyze...done");
        Ok(())
    }

    /// Inserts a batch of items into the "files" table.
    pub async fn insert_items(
        &self,
        items: &[BrokerItem],
        update_latest: bool,
    ) -> Result<Vec<BrokerItem>, BrokerError> {
        let collectors_guard = self.collectors.read().await;
        let collector_name_to_id: HashMap<String, i64> = collectors_guard
            .iter()
            .map(|c| (c.name.clone(), c.id))
            .collect();
        let collector_id_to_info: HashMap<i64, BrokerCollector> =
            collectors_guard.iter().map(|c| (c.id, c.clone())).collect();
        drop(collectors_guard);

        let types_guard = self.types.read().await;
        let type_name_to_id: HashMap<String, i64> =
            types_guard.iter().map(|t| (t.name.clone(), t.id)).collect();
        let type_id_to_name: HashMap<i64, String> =
            types_guard.iter().map(|t| (t.id, t.name.clone())).collect();
        drop(types_guard);

        debug!("Inserting {} items...", items.len());
        let conn = self.connect()?;
        let mut inserted: Vec<BrokerItem> = vec![];

        for batch in items.chunks(1000) {
            let values_str = batch
                .iter()
                .filter_map(|item| {
                    let collector_id = collector_name_to_id.get(item.collector_id.as_str())?;
                    let type_id = type_name_to_id.get(item.data_type.as_str())?;
                    Some(format!(
                        "({}, {}, {}, {}, {})",
                        item.ts_start.and_utc().timestamp(),
                        collector_id,
                        type_id,
                        item.rough_size,
                        item.exact_size,
                    ))
                })
                .collect::<Vec<String>>()
                .join(", ");

            if values_str.is_empty() {
                continue;
            }

            let insert_query = format!(
                r#"INSERT OR IGNORE INTO files (timestamp, collector_id, type_id, rough_size, exact_size) VALUES {}
                    RETURNING timestamp, collector_id, type_id, rough_size, exact_size"#,
                values_str
            );

            let mut rows = conn.query(&insert_query, ()).await?;

            while let Ok(Some(row)) = rows.next().await {
                let timestamp: i64 = row.get(0).unwrap_or(0);
                let collector_id: i64 = row.get(1).unwrap_or(0);
                let type_id: i64 = row.get(2).unwrap_or(0);
                let rough_size: i64 = row.get(3).unwrap_or(0);
                let exact_size: i64 = row.get(4).unwrap_or(0);

                if let (Some(collector), Some(type_name)) = (
                    collector_id_to_info.get(&collector_id),
                    type_id_to_name.get(&type_id),
                ) {
                    let is_rib = type_name.as_str() == "rib";
                    let ts_start = DateTime::from_timestamp(timestamp, 0).unwrap().naive_utc();
                    let (url, ts_end) = infer_url(collector, &ts_start, is_rib);

                    inserted.push(BrokerItem {
                        ts_start,
                        ts_end,
                        collector_id: collector.name.clone(),
                        data_type: type_name.clone(),
                        url,
                        rough_size,
                        exact_size,
                    });
                }
            }
        }

        debug!("Inserted {} items", inserted.len());
        if update_latest {
            self.update_latest_files(&inserted, false).await;
        }

        self.force_checkpoint().await;
        Ok(inserted)
    }

    pub async fn insert_collector(&self, collector: &Collector) -> Result<(), BrokerError> {
        let conn = self.connect()?;

        let mut rows = conn
            .query(
                "SELECT count(*) FROM collectors where name = ?1",
                [collector.id.as_str()],
            )
            .await?;

        let count: i64 = if let Ok(Some(row)) = rows.next().await {
            row.get(0).unwrap_or(0)
        } else {
            0
        };

        if count > 0 {
            return Ok(());
        }

        let (project, interval) = match collector.project.to_lowercase().as_str() {
            "riperis" | "ripe-ris" => ("ripe-ris", 5 * 60),
            "routeviews" | "route-views" => ("route-views", 15 * 60),
            _ => panic!("Unknown project: {}", collector.project),
        };

        conn.execute(
            "INSERT INTO collectors (name, url, project, updates_interval) VALUES (?1, ?2, ?3, ?4)",
            libsql::params![
                collector.id.as_str(),
                collector.url.as_str(),
                project,
                interval
            ],
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        // Remove the main database file
        if path.exists() {
            let _ = std::fs::remove_file(path);
        }

        // Remove WAL and SHM files that SQLite creates
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
    async fn test() {
        let db_path = create_temp_db_path("test");
        let db_path_str = db_path.to_str().unwrap();

        let db = LocalBrokerDb::new(db_path_str).await.unwrap();

        // Test basic database operations
        let entry_count = db.get_entry_count().await.unwrap();
        assert_eq!(entry_count, 0); // New database should be empty

        let _latest_timestamp = db.get_latest_timestamp().await.unwrap();
        // New database might return None or some default timestamp depending on SQLite behavior
        // The important thing is that the call succeeds

        // Test search with filters
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

        assert!(result.items.is_empty()); // No data in fresh database
        assert_eq!(result.total, 0); // Total should also be 0

        // Cleanup
        drop(db);
        cleanup_db_file(&db_path);
    }

    #[tokio::test]
    async fn test_get_mappings() {
        let db_path = create_temp_db_path("get_mappings");
        let db_path_str = db_path.to_str().unwrap();

        let db = LocalBrokerDb::new(db_path_str).await.unwrap();

        // Verify collectors and types are loaded (should be empty in fresh database)
        let collectors = db.collectors.read().await;
        assert!(collectors.is_empty());
        drop(collectors);

        let types = db.types.read().await;
        assert!(types.is_empty());
        drop(types);

        // Cleanup
        drop(db);
        cleanup_db_file(&db_path);
    }

    #[tokio::test]
    async fn test_inserts() {
        let db_path = create_temp_db_path("inserts");
        let db_path_str = db_path.to_str().unwrap();

        let db = LocalBrokerDb::new(db_path_str).await.unwrap();

        // First we need to populate collectors and types for the test data
        use crate::Collector;

        // Insert test collectors
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
            Collector {
                id: "route-views2".to_string(),
                project: "routeviews".to_string(),
                url: "http://archive.routeviews.org/route-views2/".to_string(),
            },
        ];

        for collector in &test_collectors {
            db.insert_collector(collector).await.unwrap();
        }

        // Insert test data types
        let conn = db.connect().unwrap();
        conn.execute("INSERT INTO types (name) VALUES ('updates'), ('rib')", ())
            .await
            .unwrap();

        // Reload mappings after insertions
        db.reload_collectors().await;
        db.reload_types().await;

        // Now test item insertion
        let items = vec![
            BrokerItem {
                ts_start: DateTime::from_timestamp(1640995200, 0).unwrap().naive_utc(), // 2022-01-01
                ts_end: Default::default(),
                collector_id: "rrc00".to_string(),
                data_type: "updates".to_string(),
                url: "test.com".to_string(),
                rough_size: 1000,
                exact_size: 1024,
            },
            BrokerItem {
                ts_start: DateTime::from_timestamp(1640995200, 0).unwrap().naive_utc(),
                ts_end: Default::default(),
                collector_id: "rrc01".to_string(),
                data_type: "rib".to_string(),
                url: "test.com".to_string(),
                rough_size: 2000,
                exact_size: 2048,
            },
            BrokerItem {
                ts_start: DateTime::from_timestamp(1640995200, 0).unwrap().naive_utc(),
                ts_end: Default::default(),
                collector_id: "route-views2".to_string(),
                data_type: "updates".to_string(),
                url: "test.com".to_string(),
                rough_size: 3000,
                exact_size: 3072,
            },
        ];

        let inserted = db.insert_items(&items, true).await.unwrap();
        assert_eq!(inserted.len(), 3);

        // Verify insertion worked
        let entry_count = db.get_entry_count().await.unwrap();
        assert_eq!(entry_count, 3);

        // Cleanup
        drop(db);
        cleanup_db_file(&db_path);
    }

    #[tokio::test]
    async fn test_get_latest() {
        let db_path = create_temp_db_path("get_latest");
        let db_path_str = db_path.to_str().unwrap();

        let db = LocalBrokerDb::new(db_path_str).await.unwrap();

        // Test get_latest_files on empty database
        let files = db.get_latest_files().await;
        assert!(files.is_empty());

        // Cleanup
        drop(db);
        cleanup_db_file(&db_path);
    }

    #[tokio::test]
    async fn test_update_latest() {
        let db_path = create_temp_db_path("update_latest");
        let db_path_str = db_path.to_str().unwrap();

        let db = LocalBrokerDb::new(db_path_str).await.unwrap();

        // Test update_latest_files with empty items (should not crash)
        db.update_latest_files(&[], false).await;

        let files = db.get_latest_files().await;
        assert!(files.is_empty());

        // Cleanup
        drop(db);
        cleanup_db_file(&db_path);
    }
}
