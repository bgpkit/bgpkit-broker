mod latest_files;
mod meta;
mod utils;

use crate::db::utils::infer_url;
use crate::query::{BrokerCollector, BrokerItemType};
use crate::{BrokerError, BrokerItem, Collector};
use chrono::{DateTime, Duration, NaiveDateTime};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use sqlx::SqlitePool;
use sqlx::{migrate::MigrateDatabase, Sqlite};
use std::collections::HashMap;
use tracing::{debug, error, info};

pub use meta::UpdatesMeta;

pub const DEFAULT_PAGE_SIZE: usize = 100;

#[derive(Clone)]
pub struct LocalBrokerDb {
    /// shared connection pool for reading and writing
    conn_pool: SqlitePool,
    collectors: Vec<BrokerCollector>,
    types: Vec<BrokerItemType>,
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
    pub async fn new(path: &str) -> Result<Self, BrokerError> {
        info!("open local broker db at {}", path);

        if !Sqlite::database_exists(path).await? {
            match Sqlite::create_database(path).await {
                Ok(_) => info!("Created db at {}", path),
                Err(error) => panic!("error: {}", error),
            }
        }
        let conn_pool = SqlitePool::connect(path).await?;

        let mut db = LocalBrokerDb {
            conn_pool,
            collectors: vec![],
            types: vec![],
        };
        db.initialize().await?;

        Ok(db)
    }

    async fn initialize(&mut self) -> Result<(), BrokerError> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS meta(
                update_ts INTEGER,
                update_duration INTEGER,
                insert_count INTEGER
            );

            CREATE TABLE IF NOT EXISTS collectors (
                id INTEGER PRIMARY KEY,
                name TEXT,
                url TEXT,
                project TEXT,
                updates_interval INTEGER
                );

            CREATE TABLE IF NOT EXISTS types (
                id INTEGER PRIMARY KEY,
                name TEXT
            );

            CREATE TABLE IF NOT EXISTS files(
                timestamp INTEGER,
                collector_id INTEGER,
                type_id INTEGER,
                rough_size INTEGER,
                exact_size INTEGER,
                constraint files_unique_pk
                    unique (timestamp, collector_id, type_id)
            );

            CREATE TABLE IF NOT EXISTS latest(
                timestamp INTEGER,
                collector_name TEXT,
                type TEXT,
                rough_size INTEGER,
                exact_size INTEGER,
                constraint latest_unique_pk
                    unique (collector_name, type)
            );

            CREATE INDEX IF NOT EXISTS idx_files_timestamp
                ON files(timestamp);

            CREATE VIEW IF NOT EXISTS files_view AS
            SELECT
                i.timestamp, i.rough_size, i.exact_size,
                t.name AS type,
                c.name AS collector_name,
                c.url AS collector_url,
                c.project AS project_name,
                c.updates_interval AS updates_interval
            FROM collectors c
            JOIN files i ON c.id = i.collector_id
            JOIN types t ON t.id = i.type_id;

            PRAGMA journal_mode=WAL;
        "#,
        )
        .execute(&self.conn_pool)
        .await?;

        self.reload_collectors().await;
        self.types = sqlx::query("select id, name from types")
            .map(|row: SqliteRow| BrokerItemType {
                id: row.get::<i64, _>("id"),
                name: row.get::<String, _>("name"),
            })
            .fetch_all(&self.conn_pool)
            .await?;

        Ok(())
    }

    pub async fn reload_collectors(&mut self) {
        match sqlx::query("select id, name, url, project, updates_interval from collectors")
            .map(|row: SqliteRow| BrokerCollector {
                id: row.get::<i64, _>("id"),
                name: row.get::<String, _>("name"),
                url: row.get::<String, _>("url"),
                project: row.get::<String, _>("project"),
                updates_interval: row.get::<i64, _>("updates_interval"),
            })
            .fetch_all(&self.conn_pool)
            .await
        {
            Ok(collectors) => self.collectors = collectors,
            Err(e) => {
                error!("failed to reload collectors: {}", e);
            }
        }
    }

    async fn force_checkpoint(&self) {
        if let Err(e) = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE);")
            .execute(&self.conn_pool)
            .await
        {
            error!("failed to force checkpoint: {}", e);
        }
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

        // First query: Get total count
        let count_query = format!(
            "SELECT COUNT(*) as total FROM files_view {}",
            where_clause_str
        );
        debug!("Count query: {}", count_query.as_str());

        let total_count = sqlx::query(count_query.as_str())
            .map(|row: SqliteRow| row.get::<i64, _>("total") as usize)
            .fetch_one(&self.conn_pool)
            .await?;

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

        let collector_name_to_info = self
            .collectors
            .iter()
            .map(|c| (c.name.clone(), c.clone()))
            .collect::<HashMap<String, BrokerCollector>>();

        let items: Vec<Option<BrokerItem>> = sqlx::query(query_string.as_str())
            .map(|row: SqliteRow| {
                let collector_name = row.get::<String, _>("collector_name");
                let _collector_url = row.get::<String, _>("collector_url");
                let _project_name = row.get::<String, _>("project_name");
                let timestamp = row.get::<i64, _>("timestamp");
                let type_name = row.get::<String, _>("type");
                let rough_size = row.get::<i64, _>("rough_size");
                let exact_size = row.get::<i64, _>("exact_size");
                let _updates_interval = row.get::<i64, _>("updates_interval");

                let collector = collector_name_to_info.get(collector_name.as_str())?;

                let ts_start = DateTime::from_timestamp(timestamp, 0)?.naive_utc();

                let (url, ts_end) = infer_url(collector, &ts_start, type_name.as_str() == "rib");
                Some(BrokerItem {
                    ts_start,
                    ts_end,
                    collector_id: collector_name,
                    data_type: type_name,
                    url,
                    rough_size,
                    exact_size,
                })
            })
            .fetch_all(&self.conn_pool)
            .await?;

        Ok(DbSearchResult {
            items: items.into_iter().flatten().collect(),
            page: page.unwrap_or(1),
            page_size: page_size.unwrap_or(DEFAULT_PAGE_SIZE),
            total: total_count,
        })
    }

    /// Runs the SQLite `ANALYZE` command on the database connection pool.
    ///
    /// This method updates SQLite's internal statistics used for query planning,
    /// helping to optimize database query performance.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If the analysis operation executed successfully.
    /// * `Err(BrokerError)` - If an error occurred during the execution of the analysis command.
    pub async fn analyze(&self) -> Result<(), BrokerError> {
        info!("doing sqlite3 analyze...");
        sqlx::query("ANALYZE").execute(&self.conn_pool).await?;
        info!("doing sqlite3 analyze...done");
        Ok(())
    }

    /// Inserts a batch of items into the "files" table.
    ///
    /// # Arguments
    ///
    /// * `items` - A reference to a vector of `BrokerItem` structs to be inserted.
    /// * `update_latest` - A boolean value indicating whether to update the latest files.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a vector of inserted `BrokerItem` structs or a `BrokerError`.
    pub async fn insert_items(
        &self,
        items: &[BrokerItem],
        update_latest: bool,
    ) -> Result<Vec<BrokerItem>, BrokerError> {
        // 1. fetch all collectors, get collector name-to-id mapping
        let collector_name_to_id = self
            .collectors
            .iter()
            .map(|c| (c.name.clone(), c.id))
            .collect::<HashMap<String, i64>>();
        let collector_id_to_info = self
            .collectors
            .iter()
            .map(|c| (c.id, c.clone()))
            .collect::<HashMap<i64, BrokerCollector>>();

        // 2. fetch all types, get file type name-to-id mapping
        let type_name_to_id = self
            .types
            .iter()
            .map(|t| (t.name.clone(), t.id))
            .collect::<HashMap<String, i64>>();
        let type_id_to_name = self
            .types
            .iter()
            .map(|t| (t.id, t.name.clone()))
            .collect::<HashMap<i64, String>>();

        // 3. batch insert into "files" table
        debug!("Inserting {} items...", items.len());
        let mut inserted: Vec<BrokerItem> = vec![];
        for batch in items.chunks(1000) {
            let values_str = batch
                .iter()
                .filter_map(|item| {
                    let collector_id = match collector_name_to_id.get(item.collector_id.as_str()) {
                        Some(id) => *id,
                        None => {
                            error!(
                                "Collector name to id mapping {} not found",
                                item.collector_id
                            );
                            return None;
                        }
                    };
                    let type_id = match type_name_to_id.get(item.data_type.as_str()) {
                        Some(id) => *id,
                        None => {
                            error!("Type name to id mapping {} not found", item.data_type);
                            return None;
                        }
                    };
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
            let inserted_rows: Vec<Option<BrokerItem>> = sqlx::query(
                format!(
                r#"INSERT OR IGNORE INTO files (timestamp, collector_id, type_id, rough_size, exact_size) VALUES {}
                    RETURNING timestamp, collector_id, type_id, rough_size, exact_size
                    "#,
                    values_str
                ).as_str()
            ).map(|row: SqliteRow|{
                let timestamp = row.get::<i64,_>(0);
                let collector_id = row.get::<i64,_>(1);
                let type_id = row.get::<i64,_>(2);
                let rough_size = row.get::<i64,_>(3);
                let exact_size = row.get::<i64,_>(4);

                let collector = collector_id_to_info.get(&collector_id)?;
                let type_name = type_id_to_name.get(&type_id)?.to_owned();
                let is_rib = type_name.as_str() == "rib";

                let ts_start = DateTime::from_timestamp(timestamp, 0)?.naive_utc();
                let (url, ts_end) = infer_url(
                    collector,
                    &ts_start,
                    is_rib,
                );

                Some(BrokerItem{
                    ts_start,
                    ts_end,
                    collector_id: collector.name.clone(),
                    data_type: type_name,
                    url,
                    rough_size,
                    exact_size,
                })
            }).fetch_all(&self.conn_pool).await?;
            inserted.extend(inserted_rows.into_iter().flatten());
        }
        debug!("Inserted {} items", inserted.len());
        if update_latest {
            self.update_latest_files(&inserted, false).await;
        }

        self.force_checkpoint().await;
        Ok(inserted)
    }

    pub async fn insert_collector(&self, collector: &Collector) -> Result<(), BrokerError> {
        let count = sqlx::query(
            r#"
            SELECT count(*) FROM collectors where name = ?
            "#,
        )
        .bind(collector.id.as_str())
        .map(|row: SqliteRow| row.get::<i64, _>(0))
        .fetch_one(&self.conn_pool)
        .await?;
        if count > 0 {
            // the collector already exists
            return Ok(());
        }

        let (project, interval) = match collector.project.to_lowercase().as_str() {
            "riperis" | "ripe-ris" => ("ripe-ris", 5 * 60),
            "routeviews" | "route-views" => ("route-views", 15 * 60),
            _ => panic!("Unknown project: {}", collector.project),
        };

        sqlx::query(
            r#"
            INSERT INTO collectors (name, url, project, updates_interval)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(collector.id.as_str())
        .bind(collector.url.as_str())
        .bind(project)
        .bind(interval)
        .execute(&self.conn_pool)
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
        assert!(db.collectors.is_empty());
        assert!(db.types.is_empty());

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
        sqlx::query("INSERT INTO types (name) VALUES ('updates'), ('rib')")
            .execute(&db.conn_pool)
            .await
            .unwrap();

        // Reload mappings after insertions
        let mut db = db; // Take ownership to call mutable method
        db.reload_collectors().await;
        db.types = sqlx::query("select id, name from types")
            .map(|row: SqliteRow| BrokerItemType {
                id: row.get::<i64, _>("id"),
                name: row.get::<String, _>("name"),
            })
            .fetch_all(&db.conn_pool)
            .await
            .unwrap();

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
