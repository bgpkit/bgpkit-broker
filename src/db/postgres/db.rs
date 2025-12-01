//! PostgreSQL database implementation.

use crate::db::traits::{BrokerDb, DbSearchResult, UpdatesMeta, DEFAULT_PAGE_SIZE};
use crate::db::utils::infer_url;
use crate::query::{BrokerCollector, BrokerItemType};
use crate::{BrokerError, BrokerItem, Collector};
use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use sqlx::postgres::PgRow;
use sqlx::PgPool;
use sqlx::Row;
use std::collections::HashMap;
use std::sync::RwLock;
use tracing::{debug, info};

/// PostgreSQL database backend.
#[derive(Clone)]
pub struct PostgresDb {
    /// Shared connection pool
    conn_pool: PgPool,
    /// Cached collectors
    collectors: std::sync::Arc<RwLock<Vec<BrokerCollector>>>,
    /// Cached types (for PostgreSQL, we use enums but keep this for compatibility)
    types: std::sync::Arc<RwLock<Vec<BrokerItemType>>>,
    /// Optional schema name (stored for potential future use)
    #[allow(dead_code)]
    schema: Option<String>,
}

fn get_ts_start_clause(ts: i64) -> String {
    let ts_dt = DateTime::from_timestamp(ts, 0).unwrap();
    format!(
        r#"
            (
                (project_name='ripe-ris' AND type='updates' AND ts > '{}'::timestamptz - interval '5 minutes')
                OR (project_name='route-views' AND type='updates' AND ts > '{}'::timestamptz - interval '15 minutes')
                OR (type='rib' AND ts >= '{}'::timestamptz)
            )
        "#,
        ts_dt.format("%Y-%m-%d %H:%M:%S%z"),
        ts_dt.format("%Y-%m-%d %H:%M:%S%z"),
        ts_dt.format("%Y-%m-%d %H:%M:%S%z"),
    )
}

fn get_ts_end_clause(ts: i64) -> String {
    let ts_dt = DateTime::from_timestamp(ts, 0).unwrap();
    format!(
        "ts < '{}'::timestamptz",
        ts_dt.format("%Y-%m-%d %H:%M:%S%z")
    )
}

/// PostgreSQL connection configuration.
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    pub schema: Option<String>,
    /// SSL mode: "disable", "prefer", "require" (default: "require")
    pub ssl_mode: String,
}

impl PostgresConfig {
    /// Create config from environment variables.
    /// Uses BROKER_ prefix: BROKER_DATABASE_HOST, BROKER_DATABASE_PORT, etc.
    pub fn from_env() -> Result<Self, BrokerError> {
        let host =
            std::env::var("BROKER_DATABASE_HOST").unwrap_or_else(|_| "localhost".to_string());
        let port = std::env::var("BROKER_DATABASE_PORT")
            .unwrap_or_else(|_| "5432".to_string())
            .parse::<u16>()
            .map_err(|e| {
                BrokerError::BrokerError(format!("Invalid BROKER_DATABASE_PORT: {}", e))
            })?;
        let database = std::env::var("BROKER_DATABASE").map_err(|_| {
            BrokerError::BrokerError("BROKER_DATABASE environment variable not set".to_string())
        })?;
        let username = std::env::var("BROKER_DATABASE_USERNAME").map_err(|_| {
            BrokerError::BrokerError(
                "BROKER_DATABASE_USERNAME environment variable not set".to_string(),
            )
        })?;
        let password = std::env::var("BROKER_DATABASE_PASSWORD").map_err(|_| {
            BrokerError::BrokerError(
                "BROKER_DATABASE_PASSWORD environment variable not set".to_string(),
            )
        })?;
        let schema = std::env::var("BROKER_DATABASE_SCHEMA").ok();
        // Default to "require" for SSL, which is what most cloud providers need
        let ssl_mode =
            std::env::var("BROKER_DATABASE_SSLMODE").unwrap_or_else(|_| "require".to_string());

        Ok(Self {
            host,
            port,
            database,
            username,
            password,
            schema,
            ssl_mode,
        })
    }

    /// Check if PostgreSQL environment variables are configured.
    pub fn is_configured() -> bool {
        std::env::var("BROKER_DATABASE").is_ok()
            && std::env::var("BROKER_DATABASE_USERNAME").is_ok()
            && std::env::var("BROKER_DATABASE_PASSWORD").is_ok()
    }

    /// Build connection URL with SSL mode.
    pub fn connection_url(&self) -> String {
        format!(
            "postgresql://{}:{}@{}:{}/{}?sslmode={}",
            self.username, self.password, self.host, self.port, self.database, self.ssl_mode
        )
    }
}

impl PostgresDb {
    /// Create a new PostgreSQL database connection from config.
    pub async fn new(config: PostgresConfig) -> Result<Self, BrokerError> {
        info!(
            "connecting to PostgreSQL at {}:{}/{}",
            config.host, config.port, config.database
        );

        // Get pool size from environment, default to 3 for serverless databases
        let max_connections: u32 = std::env::var("BROKER_DATABASE_POOL_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3);

        // Build connection pool with settings optimized for serverless PostgreSQL
        // (e.g., PlanetScale, Neon, Supabase) which have aggressive connection timeouts
        let conn_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(max_connections) // Keep low for serverless databases
            .min_connections(0) // Don't maintain idle connections
            .acquire_timeout(std::time::Duration::from_secs(30))
            .idle_timeout(std::time::Duration::from_secs(10)) // Very short idle timeout for serverless
            .max_lifetime(std::time::Duration::from_secs(60)) // 1 min max lifetime to avoid stale connections
            .test_before_acquire(true) // Always test connections before use
            .connect(&config.connection_url())
            .await
            .map_err(|e| {
                BrokerError::BrokerError(format!("Failed to connect to PostgreSQL: {}", e))
            })?;

        info!(
            "PostgreSQL pool created with max {} connections",
            max_connections
        );

        // Set search_path if schema is specified
        if let Some(ref schema) = config.schema {
            sqlx::query(&format!("SET search_path TO {}", schema))
                .execute(&conn_pool)
                .await
                .map_err(|e| BrokerError::BrokerError(format!("Failed to set schema: {}", e)))?;
            info!("using schema: {}", schema);
        }

        let db = PostgresDb {
            conn_pool,
            collectors: std::sync::Arc::new(RwLock::new(vec![])),
            types: std::sync::Arc::new(RwLock::new(vec![])),
            schema: config.schema,
        };

        db.reload_collectors_internal().await?;
        db.reload_types().await?;

        info!("PostgreSQL connection established");
        Ok(db)
    }

    /// Create from environment variables.
    pub async fn from_env() -> Result<Self, BrokerError> {
        let config = PostgresConfig::from_env()?;
        Self::new(config).await
    }

    async fn reload_collectors_internal(&self) -> Result<(), BrokerError> {
        let collectors = sqlx::query(
            "SELECT id, name, url, project::text as project, updates_interval FROM collectors",
        )
        .map(|row: PgRow| BrokerCollector {
            id: row.get::<i32, _>("id") as i64,
            name: row.get::<String, _>("name"),
            url: row.get::<Option<String>, _>("url").unwrap_or_default(),
            project: row.get::<String, _>("project"),
            updates_interval: row.get::<i32, _>("updates_interval") as i64,
        })
        .fetch_all(&self.conn_pool)
        .await
        .map_err(|e| BrokerError::BrokerError(format!("Failed to load collectors: {}", e)))?;

        let mut guard = self.collectors.write().unwrap();
        *guard = collectors;
        Ok(())
    }

    async fn reload_types(&self) -> Result<(), BrokerError> {
        // PostgreSQL uses enums, so we create synthetic type entries
        let types = vec![
            BrokerItemType {
                id: 0,
                name: "updates".to_string(),
            },
            BrokerItemType {
                id: 1,
                name: "rib".to_string(),
            },
        ];

        let mut guard = self.types.write().unwrap();
        *guard = types;
        Ok(())
    }

    fn get_collectors(&self) -> Vec<BrokerCollector> {
        self.collectors.read().unwrap().clone()
    }

    #[allow(dead_code)]
    fn get_types(&self) -> Vec<BrokerItemType> {
        self.types.read().unwrap().clone()
    }

    /// Update the latest table with new entries.
    /// Uses UPSERT - only updates if new timestamp is greater than existing.
    pub async fn update_latest(&self, items: &[BrokerItem]) -> Result<(), BrokerError> {
        if items.is_empty() {
            return Ok(());
        }

        // Build batch upsert - more efficient than calling function per row
        let values_str = items
            .iter()
            .map(|item| {
                let ts = DateTime::from_timestamp(item.ts_start.and_utc().timestamp(), 0).unwrap();
                let rough_size = if item.rough_size == 0 {
                    "NULL".to_string()
                } else {
                    item.rough_size.to_string()
                };
                let exact_size = if item.exact_size == 0 {
                    "NULL".to_string()
                } else {
                    item.exact_size.to_string()
                };
                format!(
                    "('{}', '{}', '{}'::timestamptz, {}, {})",
                    item.collector_id,
                    item.data_type,
                    ts.format("%Y-%m-%d %H:%M:%S%z"),
                    rough_size,
                    exact_size,
                )
            })
            .collect::<Vec<String>>()
            .join(", ");

        let query = format!(
            r#"
            INSERT INTO latest (collector_name, type, ts, rough_size, exact_size)
            VALUES {}
            ON CONFLICT (collector_name, type)
            DO UPDATE SET
                ts = CASE 
                    WHEN EXCLUDED.ts > latest.ts THEN EXCLUDED.ts 
                    ELSE latest.ts 
                END,
                rough_size = CASE 
                    WHEN EXCLUDED.ts > latest.ts THEN EXCLUDED.rough_size 
                    ELSE latest.rough_size 
                END,
                exact_size = CASE 
                    WHEN EXCLUDED.ts > latest.ts THEN EXCLUDED.exact_size 
                    ELSE latest.exact_size 
                END
            "#,
            values_str
        );

        sqlx::query(&query)
            .execute(&self.conn_pool)
            .await
            .map_err(|e| BrokerError::BrokerError(format!("Failed to update latest: {}", e)))?;

        Ok(())
    }

    /// Bootstrap the latest table from existing files data.
    /// This is a one-time operation for initial setup or recovery.
    pub async fn bootstrap_latest_from_files(&self) -> Result<i64, BrokerError> {
        info!("bootstrapping latest table from files...");

        let result: i64 = sqlx::query_scalar("SELECT bootstrap_latest()")
            .fetch_one(&self.conn_pool)
            .await
            .map_err(|e| BrokerError::BrokerError(format!("Failed to bootstrap latest: {}", e)))?;

        info!("bootstrapped {} latest entries", result);
        Ok(result)
    }

    /// Cleanup old meta entries (keep last N days).
    pub async fn cleanup_meta(&self, retention_days: i32) -> Result<i64, BrokerError> {
        let result = sqlx::query(&format!(
            "DELETE FROM meta WHERE update_ts < NOW() - interval '{} days'",
            retention_days
        ))
        .execute(&self.conn_pool)
        .await
        .map_err(|e| BrokerError::BrokerError(format!("Failed to cleanup meta: {}", e)))?;

        Ok(result.rows_affected() as i64)
    }
}

#[async_trait]
impl BrokerDb for PostgresDb {
    fn collectors(&self) -> Vec<BrokerCollector> {
        self.get_collectors()
    }

    async fn reload_collectors(&mut self) -> Result<(), BrokerError> {
        self.reload_collectors_internal().await
    }

    async fn analyze(&self) -> Result<(), BrokerError> {
        info!("running PostgreSQL ANALYZE...");
        sqlx::query("ANALYZE files")
            .execute(&self.conn_pool)
            .await
            .map_err(|e| BrokerError::BrokerError(format!("Failed to analyze: {}", e)))?;
        sqlx::query("ANALYZE collectors")
            .execute(&self.conn_pool)
            .await
            .map_err(|e| BrokerError::BrokerError(format!("Failed to analyze: {}", e)))?;
        info!("running PostgreSQL ANALYZE...done");
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

        // Count query
        let count_query = format!(
            "SELECT COUNT(*) as total FROM files_view {}",
            where_clause_str
        );
        debug!("Count query: {}", count_query);

        let total_count: i64 = sqlx::query_scalar(&count_query)
            .fetch_one(&self.conn_pool)
            .await
            .map_err(|e| BrokerError::BrokerError(format!("Count query failed: {}", e)))?;

        // Data query - note: PostgreSQL files_view uses 'ts' not 'timestamp'
        let query_string = format!(
            r#"
            SELECT collector_name, collector_url, project_name, 
                   EXTRACT(EPOCH FROM ts)::bigint as timestamp, 
                   type, rough_size, exact_size, updates_interval
            FROM files_view
            {}
            ORDER BY ts ASC, type, collector_name
            {}
            "#,
            where_clause_str, limit_clause,
        );
        debug!("Data query: {}", query_string);

        let collectors = self.get_collectors();
        let collector_name_to_info: HashMap<String, BrokerCollector> = collectors
            .iter()
            .map(|c| (c.name.clone(), c.clone()))
            .collect();

        let items = sqlx::query(&query_string)
            .map(|row: PgRow| {
                let collector_name: String = row.get("collector_name");
                let timestamp: i64 = row.get("timestamp");
                let type_name: String = row.get("type");
                let rough_size: Option<i64> = row.get("rough_size");
                let exact_size: Option<i64> = row.get("exact_size");

                let collector = collector_name_to_info.get(collector_name.as_str()).unwrap();
                let ts_start = DateTime::from_timestamp(timestamp, 0).unwrap().naive_utc();
                let (url, ts_end) = infer_url(collector, &ts_start, type_name.as_str() == "rib");

                BrokerItem {
                    ts_start,
                    ts_end,
                    collector_id: collector_name,
                    data_type: type_name,
                    url,
                    rough_size: rough_size.unwrap_or(0),
                    exact_size: exact_size.unwrap_or(0),
                }
            })
            .fetch_all(&self.conn_pool)
            .await
            .map_err(|e| BrokerError::BrokerError(format!("Data query failed: {}", e)))?;

        Ok(DbSearchResult {
            items,
            page: page.unwrap_or(1),
            page_size: page_size.unwrap_or(DEFAULT_PAGE_SIZE),
            total: total_count as usize,
        })
    }

    async fn insert_items(
        &self,
        items: &[BrokerItem],
        update_latest: bool,
    ) -> Result<Vec<BrokerItem>, BrokerError> {
        if items.is_empty() {
            return Ok(vec![]);
        }

        let collectors = self.get_collectors();
        let collector_name_to_id: HashMap<String, i64> =
            collectors.iter().map(|c| (c.name.clone(), c.id)).collect();
        let collector_id_to_info: HashMap<i64, BrokerCollector> =
            collectors.iter().map(|c| (c.id, c.clone())).collect();

        debug!("Inserting {} items...", items.len());
        let mut inserted: Vec<BrokerItem> = vec![];

        // Batch size for INSERT statements - use smaller batches for serverless DBs
        // to reduce transaction time and memory pressure
        let batch_size: usize = std::env::var("BROKER_INSERT_BATCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(500);

        let total_batches = items.len().div_ceil(batch_size);

        // Process batches sequentially to minimize connection pressure on serverless databases
        // This is intentionally sequential to avoid overwhelming PlanetScale/Neon/etc.
        for (batch_num, batch) in items.chunks(batch_size).enumerate() {
            // Log progress for large inserts
            if total_batches > 5 && (batch_num + 1) % 5 == 0 {
                debug!(
                    "Insert progress: batch {}/{} ({} items processed)",
                    batch_num + 1,
                    total_batches,
                    (batch_num + 1) * batch_size
                );
            }

            let values_str = batch
                .iter()
                .filter_map(|item| {
                    let collector_id = collector_name_to_id.get(item.collector_id.as_str())?;
                    let ts = DateTime::from_timestamp(item.ts_start.and_utc().timestamp(), 0)?;
                    let rough_size = if item.rough_size == 0 {
                        "NULL".to_string()
                    } else {
                        item.rough_size.to_string()
                    };
                    let exact_size = if item.exact_size == 0 {
                        "NULL".to_string()
                    } else {
                        item.exact_size.to_string()
                    };
                    Some(format!(
                        "('{}'::timestamptz, {}, '{}'::data_type, {}, {})",
                        ts.format("%Y-%m-%d %H:%M:%S%z"),
                        collector_id,
                        item.data_type,
                        rough_size,
                        exact_size,
                    ))
                })
                .collect::<Vec<String>>()
                .join(", ");

            if values_str.is_empty() {
                continue;
            }

            let query = format!(
                r#"
                INSERT INTO files (ts, collector_id, data_type, rough_size, exact_size) 
                VALUES {}
                ON CONFLICT DO NOTHING
                RETURNING EXTRACT(EPOCH FROM ts)::bigint as timestamp, collector_id, 
                          data_type::text, rough_size, exact_size
                "#,
                values_str
            );

            // Retry logic for transient connection failures
            let mut last_error = None;
            for attempt in 0..3 {
                if attempt > 0 {
                    // Exponential backoff: 1s, 2s, 4s
                    let delay = std::time::Duration::from_secs(1 << attempt);
                    debug!("Retrying insert batch {} after {:?}...", batch_num, delay);
                    tokio::time::sleep(delay).await;
                }

                match sqlx::query(&query)
                    .map(|row: PgRow| {
                        let timestamp: i64 = row.get("timestamp");
                        let collector_id: i32 = row.get("collector_id");
                        let type_name: String = row.get("data_type");
                        let rough_size: Option<i64> = row.get("rough_size");
                        let exact_size: Option<i64> = row.get("exact_size");

                        let collector = collector_id_to_info.get(&(collector_id as i64)).unwrap();
                        let is_rib = type_name.as_str() == "rib";
                        let ts_start = DateTime::from_timestamp(timestamp, 0).unwrap().naive_utc();
                        let (url, ts_end) = infer_url(collector, &ts_start, is_rib);

                        BrokerItem {
                            ts_start,
                            ts_end,
                            collector_id: collector.name.clone(),
                            data_type: type_name,
                            url,
                            rough_size: rough_size.unwrap_or(0),
                            exact_size: exact_size.unwrap_or(0),
                        }
                    })
                    .fetch_all(&self.conn_pool)
                    .await
                {
                    Ok(rows) => {
                        inserted.extend(rows);
                        last_error = None;
                        break;
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        // Check if it's a transient error worth retrying
                        if err_str.contains("connection")
                            || err_str.contains("EOF")
                            || err_str.contains("server login")
                            || err_str.contains("failed to connect")
                        {
                            debug!("Transient error on attempt {}: {}", attempt + 1, e);
                            last_error = Some(e);
                            continue;
                        } else {
                            // Non-transient error, fail immediately
                            return Err(BrokerError::BrokerError(format!("Insert failed: {}", e)));
                        }
                    }
                }
            }

            // If we exhausted all retries
            if let Some(e) = last_error {
                return Err(BrokerError::BrokerError(format!(
                    "Insert failed after 3 retries: {}",
                    e
                )));
            }
        }

        debug!("Inserted {} items", inserted.len());

        if update_latest && !inserted.is_empty() {
            // Update the latest table with inserted items
            self.update_latest(&inserted).await?;
        }

        Ok(inserted)
    }

    async fn insert_collector(&self, collector: &Collector) -> Result<(), BrokerError> {
        let (project, interval) = match collector.project.to_lowercase().as_str() {
            "riperis" | "ripe-ris" => ("ripe-ris", 5 * 60),
            "routeviews" | "route-views" => ("route-views", 15 * 60),
            _ => {
                return Err(BrokerError::BrokerError(format!(
                    "Unknown project: {}",
                    collector.project
                )))
            }
        };

        sqlx::query(
            r#"
            INSERT INTO collectors (project, name, url, updates_interval) 
            VALUES ($1::project_type, $2, $3, $4)
            ON CONFLICT (project, name) DO NOTHING
            "#,
        )
        .bind(project)
        .bind(&collector.id)
        .bind(&collector.url)
        .bind(interval)
        .execute(&self.conn_pool)
        .await
        .map_err(|e| BrokerError::BrokerError(format!("Failed to insert collector: {}", e)))?;

        Ok(())
    }

    async fn get_latest_timestamp(&self) -> Result<Option<NaiveDateTime>, BrokerError> {
        let result: Option<DateTime<Utc>> = sqlx::query_scalar("SELECT MAX(ts) FROM files")
            .fetch_one(&self.conn_pool)
            .await
            .map_err(|e| {
                BrokerError::BrokerError(format!("Failed to get latest timestamp: {}", e))
            })?;

        Ok(result.map(|dt| dt.naive_utc()))
    }

    async fn get_latest_files(&self) -> Vec<BrokerItem> {
        let collectors = self.get_collectors();
        let collector_name_to_info: HashMap<String, BrokerCollector> = collectors
            .iter()
            .map(|c| (c.name.clone(), c.clone()))
            .collect();

        sqlx::query(
            r#"
            SELECT EXTRACT(EPOCH FROM ts)::bigint as timestamp, 
                   collector_name, type, rough_size, exact_size 
            FROM latest
            "#,
        )
        .map(|row: PgRow| {
            let timestamp: i64 = row.get("timestamp");
            let collector_name: String = row.get("collector_name");
            let type_name: String = row.get("type");
            let rough_size: Option<i64> = row.get("rough_size");
            let exact_size: Option<i64> = row.get("exact_size");

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
                rough_size: rough_size.unwrap_or(0),
                exact_size: exact_size.unwrap_or(0),
            }
        })
        .fetch_all(&self.conn_pool)
        .await
        .unwrap_or_default()
    }

    async fn update_latest_files(&self, files: &[BrokerItem], bootstrap: bool) {
        if bootstrap {
            // Bootstrap from files table
            let _ = self.bootstrap_latest_from_files().await;
        } else if !files.is_empty() {
            // Incremental update with UPSERT
            let _ = self.update_latest(files).await;
        }
    }

    async fn bootstrap_latest_table(&self) {
        // Bootstrap latest table from files
        let _ = self.bootstrap_latest_from_files().await;
    }

    async fn insert_meta(
        &self,
        crawl_duration: i32,
        item_inserted: i32,
    ) -> Result<Vec<UpdatesMeta>, BrokerError> {
        debug!("Inserting meta information...");

        let inserted: Vec<UpdatesMeta> = sqlx::query(
            r#"
            INSERT INTO meta (update_ts, update_duration, insert_count) 
            VALUES (NOW(), $1, $2)
            RETURNING EXTRACT(EPOCH FROM update_ts)::bigint as update_ts, update_duration, insert_count
            "#,
        )
        .bind(crawl_duration)
        .bind(item_inserted)
        .map(|row: PgRow| {
            UpdatesMeta {
                update_ts: row.get("update_ts"),
                update_duration: row.get("update_duration"),
                insert_count: row.get("insert_count"),
            }
        })
        .fetch_all(&self.conn_pool)
        .await
        .map_err(|e| BrokerError::BrokerError(format!("Failed to insert meta: {}", e)))?;

        Ok(inserted)
    }

    async fn get_latest_updates_meta(&self) -> Result<Option<UpdatesMeta>, BrokerError> {
        let entries: Vec<UpdatesMeta> = sqlx::query(
            r#"
            SELECT EXTRACT(EPOCH FROM update_ts)::bigint as update_ts, 
                   update_duration, insert_count 
            FROM meta 
            ORDER BY update_ts DESC 
            LIMIT 1
            "#,
        )
        .map(|row: PgRow| UpdatesMeta {
            update_ts: row.get("update_ts"),
            update_duration: row.get("update_duration"),
            insert_count: row.get("insert_count"),
        })
        .fetch_all(&self.conn_pool)
        .await
        .map_err(|e| BrokerError::BrokerError(format!("Failed to get meta: {}", e)))?;

        Ok(entries.into_iter().next())
    }
}
