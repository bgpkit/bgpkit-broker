use crate::{BrokerError, BrokerItem};
use chrono::{NaiveDateTime, Utc};
use duckdb::{AccessMode, Config, DuckdbConnectionManager, Row};
use r2d2::Pool;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

pub const DEFAULT_PAGE_SIZE: usize = 100;

#[derive(Clone)]
pub struct LocalBrokerDb {
    /// shared connection pool for reading and writing
    conn_pool: Pool<DuckdbConnectionManager>,
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

unsafe impl Send for LocalBrokerDb {}
unsafe impl Sync for LocalBrokerDb {}

impl LocalBrokerDb {
    pub fn new(
        path: &str,
        force_reset: bool,
        try_bootstrap: Option<String>,
    ) -> Result<Self, BrokerError> {
        info!("open local broker db at {}", path);
        let writer_config = Config::default().access_mode(AccessMode::ReadWrite)?;
        let writer_manager = DuckdbConnectionManager::file_with_flags(path, writer_config).unwrap();
        let conn_pool = Pool::builder().max_size(20).build(writer_manager).unwrap();

        let mut db = LocalBrokerDb { conn_pool };
        db.create_table(force_reset).unwrap();

        if let Some(remote_path) = try_bootstrap {
            if db.get_entry_count()? <= 100_000 {
                info!(
                    "database needs bootstrap, bootstrapping from {}...",
                    remote_path.as_str()
                );
                match remote_path.ends_with("duckdb") {
                    true => {
                        drop(db);
                        db = Self::bootstrap_from_duckdb(remote_path.as_str(), path)?;
                    }
                    false => db.bootstrap_from_parquet(remote_path.as_str())?,
                }
            }
        }

        Ok(db)
    }

    /// Bootstrap from remote duckdb file
    pub fn bootstrap_from_duckdb(remote_path: &str, local_path: &str) -> Result<Self, BrokerError> {
        if let Err(error) = oneio::download(remote_path, local_path, None) {
            return Err(BrokerError::BrokerError(error.to_string()));
        };
        Ok(LocalBrokerDb::new(local_path, false, None)?)
    }

    /// Bootstrap from remote parquet file
    pub fn bootstrap_from_parquet(&self, path: &str) -> Result<(), BrokerError> {
        self.create_table(true).unwrap();
        let conn = self.conn_pool.get().unwrap();
        info!("bootstrap from {}", path);
        conn.execute(
            format!("INSERT INTO items SELECT * FROM read_parquet('{}')", path).as_str(),
            [],
        )?;
        Ok(())
    }

    fn create_table(&self, reset: bool) -> Result<(), BrokerError> {
        let conn = self.conn_pool.get().unwrap();
        let create_statement = match reset {
            true => "CREATE OR REPLACE TABLE",
            false => "CREATE TABLE IF NOT EXISTS",
        };
        conn.execute(
            &format!(
                r#"
        {} items (
            ts_start TIMESTAMP,
            ts_end TIMESTAMP,
            collector_id TEXT,
            data_type TEXT,
            url TEXT,
            rough_size UBIGINT,
            exact_size UBIGINT,
            PRIMARY KEY(collector_id, ts_start, data_type)
        )
        "#,
                create_statement
            ),
            [],
        )?;

        conn.execute(
            &format!(
                r#"
        {} updates_meta (
            update_ts TIMESTAMP,
            update_duration INTEGER,
            insert_count INTEGER,
        )
        "#,
                create_statement
            ),
            [],
        )?;
        Ok(())
    }

    /// Check if data bootstrap is needed
    pub fn get_entry_count(&self) -> Result<i64, BrokerError> {
        let conn = self.conn_pool.get().unwrap();
        let mut statement = conn.prepare(
            r#"
            SELECT count(*) FROM items
            "#,
        )?;
        let mut rows = statement.query([])?;
        if let Some(row) = rows.next()? {
            // the duckdb returns timestamp in microseconds (10^-6 seconds)
            let count: Option<i64> = row.get(0)?;
            Ok(count.unwrap_or(0))
        } else {
            Err(BrokerError::BrokerError(
                "failed to get db entry count".to_string(),
            ))
        }
    }

    pub fn insert_items(&self, items: &Vec<BrokerItem>) -> Result<Vec<BrokerItem>, BrokerError> {
        let conn = self.conn_pool.get().unwrap();
        debug!("Inserting {} items...", items.len());
        let mut inserted: Vec<BrokerItem> = vec![];
        for batch in items.chunks(1000) {
            let values_str = batch
                .iter()
                .map(|item| {
                    format!(
                        "('{}', '{}', '{}', '{}', '{}', {}, {})",
                        item.collector_id,
                        item.ts_start,
                        item.ts_end,
                        item.data_type,
                        item.url,
                        item.rough_size,
                        item.exact_size,
                    )
                })
                .collect::<Vec<String>>()
                .join(", ");
            let mut statement = conn.prepare(
                &format!(
                    r#"INSERT OR IGNORE INTO items (collector_id, ts_start, ts_end, data_type, url, rough_size, exact_size) VALUES {}
                    RETURNING collector_id, epoch(ts_start), epoch(ts_end), data_type, url, rough_size, exact_size
                    "#,
                    values_str
                )
            )?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                inserted.push(row.into());
            }
        }
        debug!("Inserted {} items", inserted.len());
        Ok(inserted)
    }

    pub fn insert_meta(
        &self,
        crawl_duration: i32,
        item_inserted: i32,
    ) -> Result<Vec<UpdatesMeta>, BrokerError> {
        let conn = self.conn_pool.get().unwrap();
        let mut inserted = vec![];
        debug!("Inserting updates_meta...");
        let now_ts = Utc::now().naive_utc();
        let mut statement = conn.prepare(&format!(
            r#"
            INSERT INTO updates_meta (update_ts, update_duration, insert_count) 
            VALUES ('{}', {}, {})
            RETURNING epoch(update_ts), update_duration, insert_count
            "#,
            now_ts, crawl_duration, item_inserted
        ))?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            inserted.push(row.into());
        }
        Ok(inserted)
    }

    pub fn get_latest_updates_meta(&self) -> Result<Option<UpdatesMeta>, BrokerError> {
        let conn = self.conn_pool.get().unwrap();
        let mut statement = conn.prepare(
            r#"
            SELECT epoch(update_ts), update_duration, insert_count FROM updates_meta ORDER BY update_ts DESC LIMIT 1;
            "#,
        )?;
        let mut rows = statement.query([])?;
        if let Some(row) = rows.next()? {
            Ok(Some(row.into()))
        } else {
            Ok(None)
        }
    }

    #[allow(dead_code)]
    pub fn get_duckdb_version(&self) -> Result<String, BrokerError> {
        let conn = self.conn_pool.get().unwrap();
        let version = conn.query_row("SELECT version()", [], |row| {
            let version: String = row.get(0)?;
            Ok(version)
        })?;

        Ok(version)
    }

    /// Export current duckdb to another duckdb file using file system copy
    pub fn backup_duckdb(db_path: &str, backup_path: &str) -> Result<(), BrokerError> {
        info!("backing up  duckdb from {} to {}...", db_path, backup_path);

        if let Err(e) = std::fs::copy(db_path, backup_path) {
            return Err(BrokerError::BrokerError(format!(
                "backup_duckdb: failed to backup duckdb file: {}",
                e
            )));
        };

        Ok(())
    }

    pub fn backup_parquet(&self, path: &str) -> Result<(), BrokerError> {
        let conn = self.conn_pool.get().unwrap();
        info!("backing up duckdb to parquet file to {}...", path);

        conn.execute(
            format!(
                "COPY (select * from items) TO '{}' (FORMAT 'parquet')",
                path
            )
            .as_str(),
            [],
        )?;
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<(), BrokerError> {
        let conn = self.conn_pool.get().unwrap();
        conn.execute("CHECKPOINT", [])?;
        Ok(())
    }

    /// get the latest timestamp (ts_start) of data entries in broker database
    pub fn get_latest_timestamp(&self) -> Result<Option<NaiveDateTime>, BrokerError> {
        let conn = self.conn_pool.get().unwrap();
        let mut statement = conn.prepare(
            r#"
            SELECT MAX(ts_start) FROM items
            "#,
        )?;
        let mut rows = statement.query([])?;
        if let Some(row) = rows.next()? {
            // the duckdb returns timestamp in microseconds (10^-6 seconds)
            let ts_end: Option<i64> = row.get(0)?;
            if let Some(ts_end) = ts_end {
                return Ok(Some(NaiveDateTime::from_timestamp_micros(ts_end).unwrap()));
            }
        }
        Ok(None)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn search_items(
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

                where_clauses.push(format!(
                    "list_has([{}], collector_id)",
                    collectors_array_str
                ));
            }
        }
        if let Some(project) = project {
            match project.to_lowercase().as_str() {
                "ris" | "riperis" | "ripe-ris" => {
                    where_clauses.push("collector_id like 'rrc%'".to_string());
                }
                "routeviews" | "rv" | "route-views" => {
                    where_clauses.push("collector_id like 'route-views%'".to_string());
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
            where_clauses.push(format!("data_type = '{}'", data_type));
        }

        match (ts_start, ts_end) {
            (Some(ts_start), None) => {
                where_clauses.push(format!(
                    "(ts_end > '{}' OR ts_end='{}' and ts_start=ts_end)",
                    ts_start, ts_start
                ));
            }
            (None, Some(ts_end)) => {
                where_clauses.push(format!(
                    "(ts_start < '{}' OR ts_start='{}' and ts_start=ts_end)",
                    ts_end, ts_end
                ));
            }
            (Some(ts_start), Some(ts_end)) => {
                if ts_start == ts_end {
                    where_clauses.push(format!(
                        "(ts_start <= '{}' AND (ts_end > '{}' OR ts_end>='{}' and ts_start=ts_end))",
                        ts_start, ts_start, ts_start
                    ));
                } else {
                    where_clauses.push(format!(
                        "(ts_end > '{}' OR ts_end='{}' and ts_start=ts_end)",
                        ts_start, ts_start
                    ));
                    where_clauses.push(format!(
                        "(ts_start < '{}' OR ts_start='{}' and ts_start=ts_end)",
                        ts_end, ts_end
                    ));
                }
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
            SELECT collector_id, epoch(ts_start), epoch(ts_end), data_type, url, rough_size, exact_size
            FROM items
            {}
            ORDER BY ts_start ASC, data_type, collector_id
            {}
            "#,
            match where_clauses.len() {
                0 => "".to_string(),
                _ => format!("WHERE {}", where_clauses.join(" AND ")),
            },
            limit_clause,
        );

        debug!("{}", query_string.as_str());
        let conn = self.conn_pool.get().unwrap();

        let mut stmt = conn.prepare(query_string.as_str())?;
        let mut rows = stmt.query([])?;
        let mut items: Vec<BrokerItem> = vec![];
        while let Some(row) = rows.next()? {
            items.push(row.into());
        }

        Ok(items)
    }

    pub fn get_latest_items(&self) -> Result<Vec<BrokerItem>, BrokerError> {
        let query_string = r#"
        WITH urls AS (SELECT arg_max(url, ts_start) AS max_url FROM items GROUP BY collector_id, data_type)
        SELECT collector_id, epoch(ts_start), epoch(ts_end), data_type, url, rough_size, exact_size
        FROM items JOIN urls ON items.url = urls.max_url
        ORDER BY collector_id, data_type;
        "#;
        let conn = self.conn_pool.get().unwrap();

        let mut stmt = conn.prepare(query_string)?;
        let mut rows = stmt.query([])?;
        let mut items: Vec<BrokerItem> = vec![];
        while let Some(row) = rows.next()? {
            items.push(row.into());
        }

        Ok(items)
    }
}

impl From<&Row<'_>> for BrokerItem {
    fn from(row: &Row) -> Self {
        let ts_start = NaiveDateTime::from_timestamp_opt(row.get::<_, i64>(1).unwrap(), 0).unwrap();
        let ts_end = NaiveDateTime::from_timestamp_opt(row.get::<_, i64>(2).unwrap(), 0).unwrap();
        BrokerItem {
            collector_id: row.get(0).unwrap(),
            ts_start,
            ts_end,
            data_type: row.get(3).unwrap(),
            url: row.get(4).unwrap(),
            rough_size: row.get(5).unwrap(),
            exact_size: row.get(6).unwrap(),
        }
    }
}

impl From<&Row<'_>> for UpdatesMeta {
    fn from(row: &Row) -> Self {
        UpdatesMeta {
            update_ts: NaiveDateTime::from_timestamp_opt(row.get::<_, i64>(0).unwrap(), 0).unwrap(),
            update_duration: row.get(1).unwrap(),
            insert_count: row.get(2).unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crawler::{crawl_collector, Collector};
    use chrono::Utc;

    #[test]
    fn test_new() {
        LocalBrokerDb::new("broker-test.duckdb", true, None).unwrap();
    }

    #[tokio::test]
    async fn test_insert() {
        let db = LocalBrokerDb::new("broker-test.duckdb", true, None).unwrap();
        let two_months_ago = Utc::now().date_naive() - chrono::Duration::days(1);
        let collector = Collector {
            id: "route-views2".to_string(),
            project: "routeviews".to_string(),
            url: "https://routeviews.org/bgpdata/".to_string(),
        };
        let crawled_items = crawl_collector(&collector, Some(two_months_ago))
            .await
            .unwrap();
        let inserted = db.insert_items(&crawled_items).unwrap();
        assert_eq!(inserted.len(), crawled_items.len());
        let inserted = db.insert_items(&crawled_items).unwrap();
        assert_eq!(inserted.len(), 0);
    }

    #[test]
    fn test_search() {
        tracing_subscriber::fmt::init();
        let db = LocalBrokerDb::new("broker-test.duckdb", false).unwrap();

        let items = db
            .search_items(
                Some(vec!["route-views2".to_string()]),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(items.len(), 100);

        let items = db
            .search_items(None, None, Some("rib".to_string()), None, None, None, None)
            .unwrap();
        assert!(items.iter().all(|item| item.data_type == "rib"));
    }

    #[test]
    fn test_loop() {
        loop {
            let db = LocalBrokerDb::new("broker-test.duckdb", false, None).unwrap();
            let _items = db
                .search_items(
                    Some(vec!["route-views2".to_string()]),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap();
        }
    }

    #[test]
    fn test_get_latest_ts() {
        let db = LocalBrokerDb::new("broker-test.duckdb", false, None).unwrap();
        let ts = db.get_latest_timestamp().unwrap();
        dbg!(ts);
    }

    #[test]
    fn test_get_latest_items() {
        let db = LocalBrokerDb::new("broker-test.duckdb", false, None).unwrap();
        let items = db.get_latest_items().unwrap();
        dbg!(items);
    }

    #[test]
    fn test_get_meta() {
        tracing_subscriber::fmt::init();
        let db = LocalBrokerDb::new("~/.bgpkit/broker.duckdb", false, None).unwrap();
        let meta = db.get_latest_updates_meta().unwrap();
        dbg!(meta);
    }

    #[test]
    fn test_get_count() {
        tracing_subscriber::fmt::init();
        let db = LocalBrokerDb::new("~/.bgpkit/broker.duckdb", false, None).unwrap();
        dbg!(db.get_entry_count().unwrap());
    }
}
