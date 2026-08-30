use super::{
    DbSearchResult, UpdatesMeta, DEFAULT_PAGE_SIZE, UPDATES_LOOKBACK_RIPE_RIS_SECS,
    UPDATES_LOOKBACK_ROUTE_VIEWS_SECS,
};
use crate::db::utils::infer_url;
use crate::query::BrokerCollector;
use crate::{BrokerError, BrokerItem, Collector};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{PgPool, Postgres, QueryBuilder, Row};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// PostgreSQL adapter for a Broker catalog initialized with
/// `migration/postgres_bootstrap/bootstrap.py`.
///
/// It uses fully-qualified relations from the PostgreSQL compatibility schema and
/// supports the same catalog writes, latest-file maintenance, and update metadata
/// lifecycle as the SQLite backend.
#[derive(Clone)]
pub struct PostgresDb {
    conn_pool: PgPool,
    collectors: Arc<RwLock<Vec<BrokerCollector>>>,
}

impl PostgresDb {
    pub async fn new(database_url: &str, max_connections: u32) -> Result<Self, BrokerError> {
        let conn_pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await
            .map_err(database_error)?;
        let db = Self {
            conn_pool,
            collectors: Arc::new(RwLock::new(Vec::new())),
        };
        db.reload_collectors().await?;
        Ok(db)
    }

    pub async fn reload_collectors(&self) -> Result<(), BrokerError> {
        let collectors = sqlx::query(
            "SELECT c.collector_id, c.name, c.base_uri, p.name AS project, c.updates_interval_seconds \
             FROM broker.collector AS c \
             JOIN broker.project AS p USING (project_id) \
             ORDER BY c.collector_id",
        )
        .map(|row: PgRow| BrokerCollector {
            id: row.get("collector_id"),
            name: row.get("name"),
            url: row.get("base_uri"),
            project: row.get("project"),
            updates_interval: i64::from(
                row.try_get::<i32, _>("updates_interval_seconds")
                    .unwrap_or_default(),
            ),
        })
        .fetch_all(&self.conn_pool)
        .await
        .map_err(database_error)?;
        let mut cached = self.collectors.write().map_err(|_| {
            BrokerError::BrokerError("PostgreSQL collector cache lock poisoned".to_string())
        })?;
        *cached = collectors;
        Ok(())
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
        validate_project(project.as_deref())?;
        let data_type = normalize_data_type(data_type.as_deref())?;
        let ts_end = normalize_search_end(ts_start, ts_end);
        let page = page.unwrap_or(1);
        if page == 0 {
            return Err(BrokerError::BrokerError("page must start at 1".to_string()));
        }
        let page_size = page_size.unwrap_or(DEFAULT_PAGE_SIZE);
        let offset = (page - 1)
            .checked_mul(page_size)
            .ok_or_else(|| BrokerError::BrokerError("pagination offset overflow".to_string()))?;
        let limit = i64::try_from(page_size)
            .map_err(|_| BrokerError::BrokerError("page size is too large".to_string()))?;
        let offset = i64::try_from(offset)
            .map_err(|_| BrokerError::BrokerError("pagination offset is too large".to_string()))?;
        let total = self
            .search_count(&collectors, project.as_deref(), data_type, ts_start, ts_end)
            .await?;

        let mut query = QueryBuilder::<Postgres>::new(
            "SELECT collector_name, timestamp, type, rough_size, exact_size \
             FROM broker.file_search_view",
        );
        apply_search_filters(
            &mut query,
            &collectors,
            project.as_deref(),
            data_type,
            ts_start,
            ts_end,
        );
        query.push(" ORDER BY timestamp ASC, type ASC, collector_name ASC");
        query.push(" LIMIT ").push_bind(limit);
        query.push(" OFFSET ").push_bind(offset);

        let rows = query
            .build()
            .fetch_all(&self.conn_pool)
            .await
            .map_err(database_error)?;
        let collector_map = self.collector_map()?;
        let items = rows
            .into_iter()
            .filter_map(|row| pg_row_to_item(row, &collector_map))
            .collect();
        Ok(DbSearchResult {
            items,
            page,
            page_size,
            total,
        })
    }

    pub async fn get_latest_timestamp(&self) -> Result<Option<NaiveDateTime>, BrokerError> {
        let timestamp: Option<DateTime<Utc>> =
            sqlx::query_scalar("SELECT max(ts_start) FROM broker.file")
                .fetch_one(&self.conn_pool)
                .await
                .map_err(database_error)?;
        Ok(timestamp.map(|timestamp| timestamp.naive_utc()))
    }

    pub async fn get_latest_files(&self) -> Result<Vec<BrokerItem>, BrokerError> {
        let rows = sqlx::query(
            "SELECT collector_name, timestamp, type, rough_size, exact_size \
             FROM broker.file_latest_view",
        )
        .fetch_all(&self.conn_pool)
        .await
        .map_err(database_error)?;
        let collector_map = self.collector_map()?;
        Ok(rows
            .into_iter()
            .filter_map(|row| pg_row_to_item(row, &collector_map))
            .collect())
    }

    pub async fn get_latest_updates_meta(&self) -> Result<Option<UpdatesMeta>, BrokerError> {
        let row = sqlx::query(
            "SELECT extract(epoch FROM update_ts)::bigint AS update_ts, \
                update_duration_seconds AS update_duration, insert_count \
             FROM broker.update_meta ORDER BY update_ts DESC LIMIT 1",
        )
        .fetch_optional(&self.conn_pool)
        .await
        .map_err(database_error)?;
        Ok(row.map(|row| UpdatesMeta {
            update_ts: row.get("update_ts"),
            update_duration: row.get("update_duration"),
            insert_count: row.get("insert_count"),
        }))
    }

    pub async fn analyze(&self) -> Result<(), BrokerError> {
        for statement in [
            "ANALYZE broker.file",
            "ANALYZE broker.collector",
            "ANALYZE broker.latest_file",
        ] {
            sqlx::query(statement)
                .execute(&self.conn_pool)
                .await
                .map_err(database_error)?;
        }
        Ok(())
    }

    pub async fn insert_items(
        &self,
        items: &[BrokerItem],
        update_latest: bool,
    ) -> Result<Vec<BrokerItem>, BrokerError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let collector_map = self.collector_map()?;
        let mut inserted = Vec::new();
        for batch in items.chunks(1000) {
            // Filter out items whose collector is not in the cache, matching the
            // SQLite backend's filter_map behavior. Binding None for an unknown
            // collector would violate the NOT NULL constraint and fail the entire
            // batch insert.
            let resolvable: Vec<&BrokerItem> = batch
                .iter()
                .filter(|item| collector_map.contains_key(item.collector_id.as_str()))
                .collect();
            if resolvable.is_empty() {
                continue;
            }
            let mut query = QueryBuilder::<Postgres>::new(
                "INSERT INTO broker.file (ts_start, collector_id, data_type, rough_size, exact_size) ",
            );
            query.push_values(resolvable.iter(), |mut values, item| {
                let collector_id = collector_map[item.collector_id.as_str()].id;
                values
                    .push_bind(item.ts_start.and_utc())
                    .push_bind(collector_id)
                    .push_bind(item.data_type.as_str())
                    .push_bind(item.rough_size)
                    .push_bind(item.exact_size);
            });
            query.push(
                " ON CONFLICT (ts_start, collector_id, data_type) DO NOTHING \
                 RETURNING ts_start, collector_id, data_type, rough_size, exact_size",
            );
            let rows = query
                .build()
                .fetch_all(&self.conn_pool)
                .await
                .map_err(database_error)?;
            for row in rows {
                let collector_id: i64 = row.get("collector_id");
                if let Some(collector) = collector_map
                    .values()
                    .find(|collector| collector.id == collector_id)
                {
                    inserted.push(row_to_item(row, collector));
                }
            }
        }
        if update_latest {
            self.update_latest_files(&inserted, false).await?;
        }
        Ok(inserted)
    }

    pub async fn insert_collector(&self, collector: &Collector) -> Result<(), BrokerError> {
        let project = normalize_project(&collector.project)?;
        let interval = if project == "ripe-ris" { 300 } else { 900 };
        sqlx::query(
            "INSERT INTO broker.collector (project_id, name, base_uri, updates_interval_seconds) \
             SELECT project_id, $1, $2, $3 FROM broker.project WHERE name = $4 \
             ON CONFLICT (project_id, name) DO UPDATE SET \
                 base_uri = EXCLUDED.base_uri, updates_interval_seconds = EXCLUDED.updates_interval_seconds",
        )
        .bind(&collector.id)
        .bind(&collector.url)
        .bind(interval)
        .bind(project)
        .execute(&self.conn_pool)
        .await
        .map_err(database_error)?;
        Ok(())
    }

    pub async fn update_latest_files(
        &self,
        files: &[BrokerItem],
        bootstrap: bool,
    ) -> Result<(), BrokerError> {
        if bootstrap {
            sqlx::query("TRUNCATE broker.latest_file")
                .execute(&self.conn_pool)
                .await
                .map_err(database_error)?;
            sqlx::query(
                "INSERT INTO broker.latest_file (collector_id, data_type, ts_start, rough_size, exact_size) \
                 SELECT DISTINCT ON (collector_id, data_type) \
                    collector_id, data_type, ts_start, rough_size, exact_size \
                 FROM broker.file ORDER BY collector_id, data_type, ts_start DESC",
            )
            .execute(&self.conn_pool)
            .await
            .map_err(database_error)?;
            return Ok(());
        }
        if files.is_empty() {
            return Ok(());
        }
        let collector_map = self.collector_map()?;
        let mut latest_by_key: HashMap<(i64, &str), &BrokerItem> = HashMap::new();
        for item in files {
            let Some(collector) = collector_map.get(item.collector_id.as_str()) else {
                continue;
            };
            let key = (collector.id, item.data_type.as_str());
            match latest_by_key.get(&key) {
                Some(existing) if existing.ts_start >= item.ts_start => {}
                _ => {
                    latest_by_key.insert(key, item);
                }
            }
        }

        let latest: Vec<_> = latest_by_key.into_values().collect();
        for batch in latest.chunks(1000) {
            let mut query = QueryBuilder::<Postgres>::new(
                "INSERT INTO broker.latest_file (collector_id, data_type, ts_start, rough_size, exact_size) ",
            );
            query.push_values(batch, |mut values, item| {
                let collector_id = collector_map[item.collector_id.as_str()].id;
                values
                    .push_bind(collector_id)
                    .push_bind(item.data_type.as_str())
                    .push_bind(item.ts_start.and_utc())
                    .push_bind(item.rough_size)
                    .push_bind(item.exact_size);
            });
            query.push(
                " ON CONFLICT (collector_id, data_type) DO UPDATE SET \
                    ts_start = EXCLUDED.ts_start, rough_size = EXCLUDED.rough_size, \
                    exact_size = EXCLUDED.exact_size \
                  WHERE EXCLUDED.ts_start > broker.latest_file.ts_start",
            );
            query
                .build()
                .execute(&self.conn_pool)
                .await
                .map_err(database_error)?;
        }
        Ok(())
    }

    pub async fn insert_meta(
        &self,
        crawl_duration: i32,
        item_inserted: i32,
    ) -> Result<Vec<UpdatesMeta>, BrokerError> {
        let row = sqlx::query(
            "INSERT INTO broker.update_meta (update_ts, update_duration_seconds, insert_count) \
             VALUES (now(), $1, $2) \
             RETURNING extract(epoch FROM update_ts)::bigint AS update_ts, \
                       update_duration_seconds AS update_duration, insert_count",
        )
        .bind(crawl_duration)
        .bind(item_inserted)
        .fetch_one(&self.conn_pool)
        .await
        .map_err(database_error)?;
        Ok(vec![UpdatesMeta {
            update_ts: row.get("update_ts"),
            update_duration: row.get("update_duration"),
            insert_count: row.get("insert_count"),
        }])
    }

    pub async fn cleanup_old_meta_entries(&self, retention_days: i64) -> Result<(), BrokerError> {
        sqlx::query(
            "DELETE FROM broker.update_meta WHERE update_ts < now() - ($1::bigint * interval '1 day')",
        )
        .bind(retention_days)
        .execute(&self.conn_pool)
        .await
        .map_err(database_error)?;
        Ok(())
    }

    fn collector_map(&self) -> Result<HashMap<String, BrokerCollector>, BrokerError> {
        self.collectors
            .read()
            .map_err(|_| {
                BrokerError::BrokerError("PostgreSQL collector cache lock poisoned".to_string())
            })
            .map(|collectors| {
                collectors
                    .iter()
                    .map(|collector| (collector.name.clone(), collector.clone()))
                    .collect()
            })
    }

    async fn search_count(
        &self,
        collectors: &Option<Vec<String>>,
        project: Option<&str>,
        data_type: Option<&str>,
        ts_start: Option<NaiveDateTime>,
        ts_end: Option<NaiveDateTime>,
    ) -> Result<usize, BrokerError> {
        let mut query =
            QueryBuilder::<Postgres>::new("SELECT count(*) FROM broker.file_search_view");
        apply_search_filters(&mut query, collectors, project, data_type, ts_start, ts_end);
        let total: i64 = query
            .build_query_scalar()
            .fetch_one(&self.conn_pool)
            .await
            .map_err(database_error)?;
        usize::try_from(total)
            .map_err(|_| BrokerError::BrokerError("database count exceeds usize".to_string()))
    }
}

fn database_error(error: sqlx::Error) -> BrokerError {
    BrokerError::BrokerError(format!("PostgreSQL error: {error}"))
}

fn normalize_project(project: &str) -> Result<&str, BrokerError> {
    match project.to_lowercase().as_str() {
        "ris" | "riperis" | "ripe-ris" => Ok("ripe-ris"),
        "routeviews" | "rv" | "route-views" => Ok("route-views"),
        _ => Err(BrokerError::BrokerError(format!(
            "Unknown project: {project}"
        ))),
    }
}

fn validate_project(project: Option<&str>) -> Result<(), BrokerError> {
    if let Some(project) = project {
        normalize_project(project)?;
    }
    Ok(())
}

fn normalize_data_type(data_type: Option<&str>) -> Result<Option<&str>, BrokerError> {
    match data_type {
        None => Ok(None),
        Some("updates" | "update" | "u") => Ok(Some("updates")),
        Some("rib" | "ribs" | "r") => Ok(Some("rib")),
        Some(other) => Err(BrokerError::BrokerError(format!(
            "Unknown data_type: {other}"
        ))),
    }
}

/// Normalize the search end timestamp.
///
/// When `ts_start` and `ts_end` are identical, expand `ts_end` by one second so
/// the given timestamp is always included in the result set. This mirrors the
/// inline logic in `LocalBrokerDb::search` (db/mod.rs); the SQLite path applies
/// the same +1-second rule but does so inside its match arms rather than via a
/// helper. Both backends must keep this behavior in sync.
fn normalize_search_end(
    ts_start: Option<NaiveDateTime>,
    ts_end: Option<NaiveDateTime>,
) -> Option<NaiveDateTime> {
    match (ts_start, ts_end) {
        (Some(start), Some(end)) if start == end => Some(end + Duration::seconds(1)),
        (_, end) => end,
    }
}

fn apply_search_filters(
    query: &mut QueryBuilder<Postgres>,
    collectors: &Option<Vec<String>>,
    project: Option<&str>,
    data_type: Option<&str>,
    ts_start: Option<NaiveDateTime>,
    ts_end: Option<NaiveDateTime>,
) {
    let mut where_started = false;
    let mut push_condition = |query: &mut QueryBuilder<Postgres>| {
        query.push(if where_started { " AND " } else { " WHERE " });
        where_started = true;
    };

    if let Some(collectors) = collectors
        .as_ref()
        .filter(|collectors| !collectors.is_empty())
    {
        push_condition(query);
        query.push("collector_name IN (");
        let mut separated = query.separated(", ");
        for collector in collectors {
            separated.push_bind(collector);
        }
        separated.push_unseparated(")");
    }
    if let Some(project) = project {
        let project =
            normalize_project(project).expect("validated before building PostgreSQL query");
        push_condition(query);
        query.push("project_name = ").push_bind(project);
    }
    if let Some(data_type) = data_type {
        push_condition(query);
        query.push("type = ").push_bind(data_type);
    }
    if let Some(start) = ts_start {
        let start = start.and_utc();
        push_condition(query);
        query
            .push("((project_name = 'ripe-ris' AND type = 'updates' AND timestamp > ")
            .push_bind(start - Duration::seconds(UPDATES_LOOKBACK_RIPE_RIS_SECS))
            .push(") OR (project_name = 'route-views' AND type = 'updates' AND timestamp > ")
            .push_bind(start - Duration::seconds(UPDATES_LOOKBACK_ROUTE_VIEWS_SECS))
            .push(") OR (type = 'rib' AND timestamp >= ")
            .push_bind(start)
            .push("))");
    }
    if let Some(end) = ts_end {
        push_condition(query);
        query.push("timestamp < ").push_bind(end.and_utc());
    }
}

fn pg_row_to_item(
    row: PgRow,
    collector_map: &HashMap<String, BrokerCollector>,
) -> Option<BrokerItem> {
    let collector_name: String = row.get("collector_name");
    let collector = collector_map.get(&collector_name)?;
    let timestamp: DateTime<Utc> = row.get("timestamp");
    let data_type: String = row.get("type");
    let (url, ts_end) = infer_url(collector, &timestamp.naive_utc(), data_type == "rib");
    Some(BrokerItem {
        ts_start: timestamp.naive_utc(),
        ts_end,
        collector_id: collector_name,
        data_type,
        url,
        rough_size: row.get("rough_size"),
        exact_size: row.get("exact_size"),
    })
}

fn row_to_item(row: PgRow, collector: &BrokerCollector) -> BrokerItem {
    let timestamp: DateTime<Utc> = row.get("ts_start");
    let data_type: String = row.get("data_type");
    let (url, ts_end) = infer_url(collector, &timestamp.naive_utc(), data_type == "rib");
    BrokerItem {
        ts_start: timestamp.naive_utc(),
        ts_end,
        collector_id: collector.name.clone(),
        data_type,
        url,
        rough_size: row.get("rough_size"),
        exact_size: row.get("exact_size"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires BGPKIT_BROKER_TEST_POSTGRES_URL pointing to a disposable bootstrapped database"]
    async fn postgres_catalog_writes_update_latest_and_meta(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let database_url = std::env::var("BGPKIT_BROKER_TEST_POSTGRES_URL")?;
        let database = PostgresDb::new(&database_url, 10).await?;
        let collector = Collector {
            id: "pg-runtime-test".to_string(),
            project: "riperis".to_string(),
            url: "https://example.invalid/pg-runtime-test".to_string(),
        };
        database.insert_collector(&collector).await?;
        database.reload_collectors().await?;
        let timestamp = Utc::now().naive_utc();
        let item = BrokerItem {
            ts_start: timestamp,
            ts_end: timestamp + Duration::seconds(300),
            collector_id: collector.id.clone(),
            data_type: "updates".to_string(),
            url: "https://example.invalid/updates.20240101.0000.gz".to_string(),
            rough_size: 56,
            exact_size: 78,
        };

        let inserted = database.insert_items(&[item], true).await?;
        assert_eq!(inserted.len(), 1);
        assert!(database
            .get_latest_files()
            .await?
            .iter()
            .any(|latest| latest.collector_id == collector.id));
        assert_eq!(database.insert_meta(3, 1).await?.len(), 1);
        assert_eq!(
            database
                .get_latest_updates_meta()
                .await?
                .map(|meta| meta.insert_count),
            Some(1)
        );
        Ok(())
    }

    #[test]
    fn exact_timestamp_search_includes_one_second() {
        let timestamp = DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap_or(DateTime::UNIX_EPOCH)
            .naive_utc();
        assert_eq!(
            normalize_search_end(Some(timestamp), Some(timestamp)),
            Some(timestamp + Duration::seconds(1))
        );
    }
}
