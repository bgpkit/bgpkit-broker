//! SQLite database implementation.

use crate::db::traits::{BrokerDb, DbSearchResult, UpdatesMeta, DEFAULT_PAGE_SIZE};
use crate::db::utils::infer_url;
use crate::query::{BrokerCollector, BrokerItemType};
use crate::{BrokerError, BrokerItem, Collector};
use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDateTime};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use sqlx::SqlitePool;
use sqlx::{migrate::MigrateDatabase, Sqlite};
use std::collections::HashMap;
use std::sync::RwLock;
use tracing::{debug, info};

/// SQLite database backend.
#[derive(Clone)]
pub struct SqliteDb {
    /// Shared connection pool for reading and writing
    conn_pool: SqlitePool,
    /// Cached collectors (wrapped in RwLock for interior mutability)
    collectors: std::sync::Arc<RwLock<Vec<BrokerCollector>>>,
    /// Cached types
    types: std::sync::Arc<RwLock<Vec<BrokerItemType>>>,
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

impl SqliteDb {
    /// Create a new SQLite database connection.
    pub async fn new(path: &str) -> Result<Self, BrokerError> {
        info!("open sqlite broker db at {}", path);

        if !Sqlite::database_exists(path).await? {
            match Sqlite::create_database(path).await {
                Ok(_) => info!("Created db at {}", path),
                Err(error) => panic!("error: {}", error),
            }
        }
        let conn_pool = SqlitePool::connect(path).await?;

        let mut db = SqliteDb {
            conn_pool,
            collectors: std::sync::Arc::new(RwLock::new(vec![])),
            types: std::sync::Arc::new(RwLock::new(vec![])),
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

        self.reload_collectors_internal().await?;
        self.reload_types().await?;

        Ok(())
    }

    async fn reload_collectors_internal(&self) -> Result<(), BrokerError> {
        let collectors =
            sqlx::query("select id, name, url, project, updates_interval from collectors")
                .map(|row: SqliteRow| BrokerCollector {
                    id: row.get::<i64, _>("id"),
                    name: row.get::<String, _>("name"),
                    url: row.get::<String, _>("url"),
                    project: row.get::<String, _>("project"),
                    updates_interval: row.get::<i64, _>("updates_interval"),
                })
                .fetch_all(&self.conn_pool)
                .await?;

        let mut guard = self.collectors.write().unwrap();
        *guard = collectors;
        Ok(())
    }

    async fn reload_types(&self) -> Result<(), BrokerError> {
        let types = sqlx::query("select id, name from types")
            .map(|row: SqliteRow| BrokerItemType {
                id: row.get::<i64, _>("id"),
                name: row.get::<String, _>("name"),
            })
            .fetch_all(&self.conn_pool)
            .await?;

        let mut guard = self.types.write().unwrap();
        *guard = types;
        Ok(())
    }

    async fn force_checkpoint(&self) {
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE);")
            .execute(&self.conn_pool)
            .await
            .unwrap();
    }

    /// Get a clone of collectors for internal use.
    fn get_collectors(&self) -> Vec<BrokerCollector> {
        self.collectors.read().unwrap().clone()
    }

    /// Get a clone of types for internal use.
    fn get_types(&self) -> Vec<BrokerItemType> {
        self.types.read().unwrap().clone()
    }
}

#[async_trait]
impl BrokerDb for SqliteDb {
    fn collectors(&self) -> Vec<BrokerCollector> {
        self.collectors.read().unwrap().clone()
    }

    async fn reload_collectors(&mut self) -> Result<(), BrokerError> {
        self.reload_collectors_internal().await
    }

    async fn analyze(&self) -> Result<(), BrokerError> {
        info!("doing sqlite3 analyze...");
        sqlx::query("ANALYZE").execute(&self.conn_pool).await?;
        info!("doing sqlite3 analyze...done");
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
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
                    true => ts_start + Duration::seconds(1),
                    false => ts_end,
                };
                where_clauses.push(get_ts_start_clause(start.and_utc().timestamp()));
                where_clauses.push(get_ts_end_clause(end.and_utc().timestamp()));
            }
            (None, None) => {}
        }

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

        let where_clause_str = match where_clauses.len() {
            0 => "".to_string(),
            _ => format!("WHERE {}", where_clauses.join(" AND ")),
        };

        let count_query = format!(
            "SELECT COUNT(*) as total FROM files_view {}",
            where_clause_str
        );
        debug!("Count query: {}", count_query.as_str());

        let total_count = sqlx::query(count_query.as_str())
            .map(|row: SqliteRow| row.get::<i64, _>("total") as usize)
            .fetch_one(&self.conn_pool)
            .await?;

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

        let collectors = self.get_collectors();
        let collector_name_to_info: HashMap<String, BrokerCollector> = collectors
            .iter()
            .map(|c| (c.name.clone(), c.clone()))
            .collect();

        let items = sqlx::query(query_string.as_str())
            .map(|row: SqliteRow| {
                let collector_name = row.get::<String, _>("collector_name");
                let timestamp = row.get::<i64, _>("timestamp");
                let type_name = row.get::<String, _>("type");
                let rough_size = row.get::<i64, _>("rough_size");
                let exact_size = row.get::<i64, _>("exact_size");

                let collector = collector_name_to_info.get(collector_name.as_str()).unwrap();
                let ts_start = DateTime::from_timestamp(timestamp, 0).unwrap().naive_utc();
                let (url, ts_end) = infer_url(collector, &ts_start, type_name.as_str() == "rib");

                BrokerItem {
                    ts_start,
                    ts_end,
                    collector_id: collector_name,
                    data_type: type_name,
                    url,
                    rough_size,
                    exact_size,
                }
            })
            .fetch_all(&self.conn_pool)
            .await?;

        Ok(DbSearchResult {
            items,
            page: page.unwrap_or(1),
            page_size: page_size.unwrap_or(DEFAULT_PAGE_SIZE),
            total: total_count,
        })
    }

    async fn insert_items(
        &self,
        items: &[BrokerItem],
        update_latest: bool,
    ) -> Result<Vec<BrokerItem>, BrokerError> {
        let collectors = self.get_collectors();
        let types = self.get_types();

        let collector_name_to_id: HashMap<String, i64> =
            collectors.iter().map(|c| (c.name.clone(), c.id)).collect();
        let collector_id_to_info: HashMap<i64, BrokerCollector> =
            collectors.iter().map(|c| (c.id, c.clone())).collect();

        let type_name_to_id: HashMap<String, i64> =
            types.iter().map(|t| (t.name.clone(), t.id)).collect();
        let type_id_to_name: HashMap<i64, String> =
            types.iter().map(|t| (t.id, t.name.clone())).collect();

        debug!("Inserting {} items...", items.len());
        let mut inserted: Vec<BrokerItem> = vec![];

        for batch in items.chunks(1000) {
            let values_str = batch
                .iter()
                .map(|item| {
                    let collector_id = match collector_name_to_id.get(item.collector_id.as_str()) {
                        Some(id) => *id,
                        None => {
                            panic!(
                                "Collector name to id mapping {} not found",
                                item.collector_id
                            );
                        }
                    };
                    format!(
                        "({}, {}, {}, {}, {})",
                        item.ts_start.and_utc().timestamp(),
                        collector_id,
                        type_name_to_id.get(item.data_type.as_str()).unwrap(),
                        item.rough_size,
                        item.exact_size,
                    )
                })
                .collect::<Vec<String>>()
                .join(", ");

            let inserted_rows = sqlx::query(
                format!(
                    r#"INSERT OR IGNORE INTO files (timestamp, collector_id, type_id, rough_size, exact_size) VALUES {}
                    RETURNING timestamp, collector_id, type_id, rough_size, exact_size
                    "#,
                    values_str
                )
                .as_str(),
            )
            .map(|row: SqliteRow| {
                let timestamp = row.get::<i64, _>(0);
                let collector_id = row.get::<i64, _>(1);
                let type_id = row.get::<i64, _>(2);
                let rough_size = row.get::<i64, _>(3);
                let exact_size = row.get::<i64, _>(4);

                let collector = collector_id_to_info.get(&collector_id).unwrap();
                let type_name = type_id_to_name.get(&type_id).unwrap().to_owned();
                let is_rib = type_name.as_str() == "rib";

                let ts_start = DateTime::from_timestamp(timestamp, 0).unwrap().naive_utc();
                let (url, ts_end) = infer_url(collector, &ts_start, is_rib);

                BrokerItem {
                    ts_start,
                    ts_end,
                    collector_id: collector.name.clone(),
                    data_type: type_name,
                    url,
                    rough_size,
                    exact_size,
                }
            })
            .fetch_all(&self.conn_pool)
            .await?;

            inserted.extend(inserted_rows);
        }

        debug!("Inserted {} items", inserted.len());
        if update_latest {
            self.update_latest_files(&inserted, false).await;
        }

        self.force_checkpoint().await;
        Ok(inserted)
    }

    async fn insert_collector(&self, collector: &Collector) -> Result<(), BrokerError> {
        let count = sqlx::query(r#"SELECT count(*) FROM collectors where name = ?"#)
            .bind(collector.id.as_str())
            .map(|row: SqliteRow| row.get::<i64, _>(0))
            .fetch_one(&self.conn_pool)
            .await?;

        if count > 0 {
            return Ok(());
        }

        let (project, interval) = match collector.project.to_lowercase().as_str() {
            "riperis" | "ripe-ris" => ("ripe-ris", 5 * 60),
            "routeviews" | "route-views" => ("route-views", 15 * 60),
            _ => panic!("Unknown project: {}", collector.project),
        };

        sqlx::query(
            r#"INSERT INTO collectors (name, url, project, updates_interval) VALUES (?, ?, ?, ?)"#,
        )
        .bind(collector.id.as_str())
        .bind(collector.url.as_str())
        .bind(project)
        .bind(interval)
        .execute(&self.conn_pool)
        .await?;

        Ok(())
    }

    async fn get_latest_timestamp(&self) -> Result<Option<NaiveDateTime>, BrokerError> {
        let timestamp = sqlx::query(r#"SELECT MAX(timestamp) FROM files"#)
            .map(|row: SqliteRow| row.get::<i64, _>(0))
            .fetch_one(&self.conn_pool)
            .await?;

        let datetime = DateTime::from_timestamp(timestamp, 0).map(|dt| dt.naive_utc());
        Ok(datetime)
    }

    async fn get_latest_files(&self) -> Vec<BrokerItem> {
        let collectors = self.get_collectors();
        let collector_name_to_info: HashMap<String, BrokerCollector> = collectors
            .iter()
            .map(|c| (c.name.clone(), c.clone()))
            .collect();

        sqlx::query("select timestamp, collector_name, type, rough_size, exact_size from latest")
            .map(|row: SqliteRow| {
                let timestamp = row.get::<i64, _>(0);
                let collector_name = row.get::<String, _>(1);
                let type_name = row.get::<String, _>(2);
                let rough_size = row.get::<i64, _>(3);
                let exact_size = row.get::<i64, _>(4);
                let collector = collector_name_to_info.get(&collector_name).unwrap();

                let is_rib = type_name.as_str() == "rib";
                let ts_start = DateTime::from_timestamp(timestamp, 0).unwrap().naive_utc();
                let (url, ts_end) = infer_url(collector, &ts_start, is_rib);

                BrokerItem {
                    ts_start,
                    ts_end,
                    collector_id: collector_name,
                    data_type: type_name,
                    url,
                    rough_size,
                    exact_size,
                }
            })
            .fetch_all(&self.conn_pool)
            .await
            .unwrap()
    }

    async fn update_latest_files(&self, files: &[BrokerItem], bootstrap: bool) {
        let value_str = match bootstrap {
            true => r#"
                SELECT
                    MAX("timestamp") AS timestamp,
                    collector_name,
                    type,
                    MAX(rough_size) AS rough_size,
                    MAX(exact_size) AS exact_size
                FROM
                    files_view
                GROUP BY
                    collector_name, type
                "#
            .to_string(),
            false => {
                if files.is_empty() {
                    return;
                }
                let values = files
                    .iter()
                    .map(|item| {
                        let ts = item.ts_start.and_utc().timestamp();
                        format!(
                            "({}, '{}', '{}', {}, {})",
                            ts,
                            item.collector_id.as_str(),
                            item.data_type.as_str(),
                            item.rough_size,
                            item.exact_size
                        )
                    })
                    .collect::<Vec<String>>()
                    .join(", ");
                format!(" VALUES {} ", values)
            }
        };

        let query_str = format!(
            r#"
            INSERT INTO "latest" ("timestamp", "collector_name", "type", "rough_size", "exact_size")
            {}
            ON CONFLICT (collector_name, type)
            DO UPDATE SET
                "timestamp" = CASE
                    WHEN excluded."timestamp" > "latest"."timestamp" THEN excluded."timestamp"
                    ELSE "latest"."timestamp"
                END,
                "rough_size" = CASE
                    WHEN excluded."timestamp" > "latest"."timestamp" THEN excluded."rough_size"
                    ELSE "latest"."rough_size"
                END,
                "exact_size" = CASE
                    WHEN excluded."timestamp" > "latest"."timestamp" THEN excluded."exact_size"
                    ELSE "latest"."exact_size"
                END;
            "#,
            value_str
        );

        sqlx::query(query_str.as_str())
            .execute(&self.conn_pool)
            .await
            .unwrap();
    }

    async fn bootstrap_latest_table(&self) {
        sqlx::query(
            r#"
            INSERT INTO "latest" ("timestamp", "collector_name", "type", "rough_size", "exact_size")
            SELECT
                MAX("timestamp") AS timestamp,
                collector_name,
                type,
                MAX(rough_size) AS rough_size,
                MAX(exact_size) AS exact_size
            FROM
                files_view
            GROUP BY
                collector_name, type
            ON CONFLICT (collector_name, type)
            DO UPDATE SET
                "timestamp" = CASE
                    WHEN excluded."timestamp" > "latest"."timestamp" THEN excluded."timestamp"
                    ELSE "latest"."timestamp"
                END,
                "rough_size" = CASE
                    WHEN excluded."timestamp" > "latest"."timestamp" THEN excluded."rough_size"
                    ELSE "latest"."rough_size"
                END,
                "exact_size" = CASE
                    WHEN excluded."timestamp" > "latest"."timestamp" THEN excluded."exact_size"
                    ELSE "latest"."exact_size"
                END;
            "#,
        )
        .execute(&self.conn_pool)
        .await
        .unwrap();
    }

    async fn insert_meta(
        &self,
        crawl_duration: i32,
        item_inserted: i32,
    ) -> Result<Vec<UpdatesMeta>, BrokerError> {
        debug!("Inserting meta information...");
        let now_ts = chrono::Utc::now().timestamp();

        let inserted: Vec<UpdatesMeta> = sqlx::query(&format!(
            r#"
            INSERT INTO meta (update_ts, update_duration, insert_count) 
            VALUES ('{}', {}, {})
            RETURNING update_ts, update_duration, insert_count
            "#,
            now_ts, crawl_duration, item_inserted
        ))
        .map(|row: SqliteRow| {
            let update_ts = row.get::<i64, _>(0);
            let update_duration = row.get::<i32, _>(1);
            let insert_count = row.get::<i32, _>(2);
            UpdatesMeta {
                update_ts,
                update_duration,
                insert_count,
            }
        })
        .fetch_all(&self.conn_pool)
        .await?;

        Ok(inserted)
    }

    async fn get_latest_updates_meta(&self) -> Result<Option<UpdatesMeta>, BrokerError> {
        let entries = sqlx::query(
            r#"SELECT update_ts, update_duration, insert_count FROM meta ORDER BY update_ts DESC LIMIT 1;"#,
        )
        .map(|row: SqliteRow| {
            let update_ts = row.get::<i64, _>(0);
            let update_duration = row.get::<i32, _>(1);
            let insert_count = row.get::<i32, _>(2);
            UpdatesMeta {
                update_ts,
                update_duration,
                insert_count,
            }
        })
        .fetch_all(&self.conn_pool)
        .await?;

        if entries.is_empty() {
            Ok(None)
        } else {
            Ok(Some(entries[0].clone()))
        }
    }
}
