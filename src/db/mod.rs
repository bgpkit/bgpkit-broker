mod latest_files;
mod utils;

use crate::db::utils::infer_url;
use crate::query::{BrokerCollector, BrokerItemType};
use crate::{BrokerError, BrokerItem};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use sqlx::SqlitePool;
use sqlx::{migrate::MigrateDatabase, Sqlite};
use std::collections::HashMap;
use tracing::{debug, info};

pub const DEFAULT_PAGE_SIZE: usize = 100;

#[derive(Clone)]
pub struct LocalBrokerDb {
    /// shared connection pool for reading and writing
    conn_pool: SqlitePool,
    collectors: Vec<BrokerCollector>,
    types: Vec<BrokerItemType>,
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
        15 * 60,
        ts,
        5 * 60,
        ts
    )
}

fn get_ts_end_clause(ts: i64) -> String {
    format!("timestamp <= {}", ts)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "cli", derive(poem_openapi::Object))]
pub struct UpdatesMeta {
    /// database update timestamp
    pub update_ts: NaiveDateTime,
    /// database update duration in seconds
    pub update_duration: i32,
    /// number of items inserted
    pub insert_count: i32,
}

impl LocalBrokerDb {
    pub async fn new(path: &str) -> Result<Self, BrokerError> {
        info!("open local broker db at {}", path);

        if !Sqlite::database_exists(path).await.unwrap() {
            match Sqlite::create_database(path).await {
                Ok(_) => info!("Created db at {}", path),
                Err(error) => panic!("error: {}", error),
            }
        }
        let conn_pool = SqlitePool::connect(path).await.unwrap();

        let mut db = LocalBrokerDb {
            conn_pool,
            collectors: vec![],
            types: vec![],
        };
        db.initialize().await.unwrap();

        Ok(db)
    }

    async fn initialize(&mut self) -> Result<(), BrokerError> {
        let _ = sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS "collectors" (
                "id" INTEGER PRIMARY KEY,
                "name" TEXT, 
                "url" TEXT, 
                "project" TEXT,
                "update_interval INTEGER
                );

            CREATE TABLE IF NOT EXISTS "types" (
                "id" INTEGER PRIMARY KEY,
                "name" TEXT
            );

            CREATE TABLE IF NOT EXISTS "files"(
                "timestamp" INTEGER,
                "collector_id" INTEGER,
                "type_id" INTEGER,
                "rough_size" INTEGER,
                "exact_size" INTEGER,
                constraint files_unique_pk
                    unique (timestamp, collector_id, type_id)
            );
            
            CREATE TABLE IF NOT EXISTS "latest"(
                "timestamp" INTEGER,
                "collector_name" TEXT,
                "type" TEXT,
                "rough_size" INTEGER,
                "exact_size" INTEGER,
                constraint latest_unique_pk
                    unique (collector_name, type)
            );
            
            CREATE INDEX IF NOT EXISTS idx_files_timestamp 
                ON "files"("timestamp");

            CREATE VIEW IF NOT EXISTS files_view AS
            SELECT
                i."timestamp", i."rough_size", i."exact_size",
                t."name" AS type,
                c."name" AS collector_name,
                c."url" AS collector_url,
                c."project" AS project_name,
                c."updates_interval" AS updates_interval
            FROM "collectors" c
            JOIN "files" i ON c."id" = i."collector_id"
            JOIN "types" t ON t."id" = i."type_id";
            
            PRAGMA journal_mode=WAL;
        "#,
        )
        .execute(&self.conn_pool)
        .await;

        self.collectors =
            sqlx::query("select id, name, url, project, updates_interval from collectors")
                .map(|row: SqliteRow| BrokerCollector {
                    id: row.get::<i64, _>("id"),
                    name: row.get::<String, _>("name"),
                    url: row.get::<String, _>("url"),
                    project: row.get::<String, _>("project"),
                    updates_interval: row.get::<i64, _>("updates_interval"),
                })
                .fetch_all(&self.conn_pool)
                .await
                .unwrap();
        self.types = sqlx::query("select id, name from types")
            .map(|row: SqliteRow| BrokerItemType {
                id: row.get::<i64, _>("id"),
                name: row.get::<String, _>("name"),
            })
            .fetch_all(&self.conn_pool)
            .await
            .unwrap();

        Ok(())
    }

    async fn force_checkpoint(&self) {
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE);")
            .execute(&self.conn_pool)
            .await
            .unwrap();
    }

    /// Check if data bootstrap is needed
    #[allow(dead_code)]
    async fn get_entry_count(&self) -> Result<i64, BrokerError> {
        let count = sqlx::query(
            r#"
            SELECT count(*) FROM files
            "#,
        )
        .map(|row: SqliteRow| row.get::<i64, _>(0))
        .fetch_one(&self.conn_pool)
        .await
        .unwrap();
        Ok(count)
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
    ) -> Result<Vec<BrokerItem>, BrokerError> {
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
                where_clauses.push(get_ts_start_clause(ts_start.timestamp()));
            }
            (None, Some(ts_end)) => {
                where_clauses.push(get_ts_end_clause(ts_end.timestamp()));
            }
            (Some(ts_start), Some(ts_end)) => {
                where_clauses.push(get_ts_start_clause(ts_start.timestamp()));
                where_clauses.push(get_ts_end_clause(ts_end.timestamp()));
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

        let query_string = format!(
            r#"
            SELECT collector_name, collector_url, project_name, timestamp, type, rough_size, exact_size, updates_interval
            FROM files_view
            {}
            ORDER BY timestamp ASC, type, collector_name
            {}
            "#,
            match where_clauses.len() {
                0 => "".to_string(),
                _ => format!("WHERE {}", where_clauses.join(" AND ")),
            },
            limit_clause,
        );
        debug!("{}", query_string.as_str());

        let collector_name_to_info = self
            .collectors
            .iter()
            .map(|c| (c.name.clone(), c.clone()))
            .collect::<HashMap<String, BrokerCollector>>();

        let items = sqlx::query(query_string.as_str())
            .map(|row: SqliteRow| {
                let collector_name = row.get::<String, _>("collector_name");
                let _collector_url = row.get::<String, _>("collector_url");
                let _project_name = row.get::<String, _>("project_name");
                let timestamp = row.get::<i64, _>("timestamp");
                let type_name = row.get::<String, _>("type");
                let rough_size = row.get::<i64, _>("rough_size");
                let exact_size = row.get::<i64, _>("exact_size");
                let _updates_interval = row.get::<i64, _>("updates_interval");

                let collector = collector_name_to_info.get(collector_name.as_str()).unwrap();

                let ts_start = NaiveDateTime::from_timestamp_opt(timestamp, 0).unwrap();

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
            .await
            .unwrap();
        Ok(items)
    }

    pub async fn insert_items(
        &self,
        items: &Vec<BrokerItem>,
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

        // 3. batch insert into files table
        debug!("Inserting {} items...", items.len());
        let mut inserted: Vec<BrokerItem> = vec![];
        for batch in items.chunks(1000) {
            let values_str = batch
                .iter()
                .map(|item| {
                    format!(
                        "({}, {}, {}, {}, {})",
                        item.ts_start.timestamp(),
                        collector_name_to_id
                            .get(item.collector_id.as_str())
                            .unwrap(),
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
                ).as_str()
            ).map(|row: SqliteRow|{
                let timestamp = row.get::<i64,_>(0);
                let collector_id = row.get::<i64,_>(1);
                let type_id = row.get::<i64,_>(2);
                let rough_size = row.get::<i64,_>(3);
                let exact_size = row.get::<i64,_>(4);

                let collector = collector_id_to_info.get(&collector_id).unwrap();
                let type_name = type_id_to_name.get(&type_id).unwrap().to_owned();
                let is_rib = type_name.as_str() == "rib";

                let ts_start = NaiveDateTime::from_timestamp_opt(timestamp, 0).unwrap();
                let (url, ts_end) = infer_url(
                    collector,
                    &ts_start,
                    is_rib,
                );

                BrokerItem{
                    ts_start,
                    ts_end,
                    collector_id: collector.name.clone(),
                    data_type: type_name,
                    url,
                    rough_size,
                    exact_size,
                }
            }).fetch_all(&self.conn_pool).await.unwrap();
            inserted.extend(inserted_rows);
        }
        debug!("Inserted {} items", inserted.len());
        if update_latest {
            self.update_latest_files(&inserted, false).await;
        }

        self.force_checkpoint().await;
        Ok(inserted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test() {
        let db = LocalBrokerDb::new("test.sqlite3").await.unwrap();
        println!("{:?}", db.get_entry_count().await.unwrap());
        println!("{:?}", db.get_latest_timestamp().await.unwrap());
        let items = db
            .search(
                Some(vec!["rrc21".to_string(), "route-views2".to_string()]),
                None,
                Some("rib".to_string()),
                Some(NaiveDateTime::from_timestamp_opt(1672531200, 0).unwrap()),
                Some(NaiveDateTime::from_timestamp_opt(1672617600, 0).unwrap()),
                None,
                None,
            )
            .await
            .unwrap();

        dbg!(items);
    }

    #[tokio::test]
    async fn test_get_mappings() {
        let db = LocalBrokerDb::new("test.sqlite3").await.unwrap();
        dbg!(db.collectors);
        dbg!(db.types);
    }

    #[tokio::test]
    async fn test_inserts() {
        let items = vec![
            BrokerItem {
                ts_start: NaiveDateTime::from_timestamp_opt(0, 0).unwrap(),
                ts_end: Default::default(),
                collector_id: "rrc00".to_string(),
                data_type: "updates".to_string(),
                url: "test.com".to_string(),
                rough_size: 0,
                exact_size: 0,
            },
            BrokerItem {
                ts_start: NaiveDateTime::from_timestamp_opt(0, 0).unwrap(),
                ts_end: Default::default(),
                collector_id: "rrc01".to_string(),
                data_type: "rib".to_string(),
                url: "test.com".to_string(),
                rough_size: 0,
                exact_size: 0,
            },
            BrokerItem {
                ts_start: NaiveDateTime::from_timestamp_opt(0, 0).unwrap(),
                ts_end: Default::default(),
                collector_id: "route-views2".to_string(),
                data_type: "updates".to_string(),
                url: "test.com".to_string(),
                rough_size: 0,
                exact_size: 0,
            },
        ];

        let db = LocalBrokerDb::new("test.sqlite3").await.unwrap();

        let inserted = db.insert_items(&items, true).await.unwrap();
        dbg!(inserted);
    }

    #[tokio::test]
    async fn test_get_latest() {
        let db = LocalBrokerDb::new("test.sqlite3").await.unwrap();
        let files = db.get_latest_files().await;
        dbg!(files);
    }

    #[tokio::test]
    async fn test_update_latest() {
        let db = LocalBrokerDb::new("test.sqlite3").await.unwrap();
        db.update_latest_files(&vec![], false).await;
        let files = db.get_latest_files().await;
        dbg!(files);
    }
}
