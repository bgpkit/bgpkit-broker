use crate::{BrokerError, BrokerItem};
use chrono::NaiveDateTime;
use duckdb::{AccessMode, Config, DuckdbConnectionManager, Row};
use r2d2::Pool;
use tracing::{debug, info};

#[derive(Clone)]
pub struct LocalBrokerDb {
    reader_pool: Pool<DuckdbConnectionManager>,
    writer_pool: Pool<DuckdbConnectionManager>,
}

unsafe impl Send for LocalBrokerDb {}
unsafe impl Sync for LocalBrokerDb {}

impl LocalBrokerDb {
    pub fn new(path: &str, force_reset: bool) -> Result<Self, BrokerError> {
        let writer_config = Config::default().access_mode(AccessMode::ReadWrite)?;
        let reader_config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let writer_manager = DuckdbConnectionManager::file_with_flags(path, writer_config).unwrap();
        let reader_manager = DuckdbConnectionManager::file_with_flags(path, reader_config).unwrap();

        let writer_pool = Pool::builder().max_size(1).build(writer_manager).unwrap();
        let reader_pool = Pool::builder().max_size(20).build(reader_manager).unwrap();

        let db = LocalBrokerDb {
            reader_pool,
            writer_pool,
        };
        db.create_table(force_reset).unwrap();
        Ok(db)
    }

    /// Bootstrap from remote file
    pub fn bootstrap(&self, path: &str) -> Result<(), BrokerError> {
        self.create_table(true).unwrap();
        let conn = self.writer_pool.get().unwrap();
        info!("bootstrap from {}", path);
        conn.execute(
            format!("INSERT INTO items SELECT * FROM read_parquet('{}')", path).as_str(),
            [],
        )?;
        Ok(())
    }

    fn create_table(&self, reset: bool) -> Result<(), BrokerError> {
        let conn = self.writer_pool.get().unwrap();
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
        Ok(())
    }

    pub fn insert_items(&self, items: &Vec<BrokerItem>) -> Result<Vec<BrokerItem>, BrokerError> {
        let conn = self.writer_pool.get().unwrap();
        info!("Inserting {} items...", items.len());
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
        info!("Inserted {} items", inserted.len());
        Ok(inserted)
    }

    pub fn get_latest_timestamp(&self) -> Result<Option<NaiveDateTime>, BrokerError> {
        let conn = self.reader_pool.get().unwrap();
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
        if let Some(ts_start) = ts_start {
            where_clauses.push(format!(
                "(ts_end > '{}' OR ts_end='{}' and ts_start=ts_end)",
                ts_start, ts_start
            ));
        }
        if let Some(ts_end) = ts_end {
            where_clauses.push(format!(
                "(ts_start < '{}' OR ts_start='{}' and ts_start=ts_end)",
                ts_end, ts_end
            ));
        }
        let (limit, offset) = match (page, page_size) {
            (Some(page), Some(page_size)) => (page_size, page_size * page),
            (Some(page), None) => (100, 100 * page),
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
            ORDER BY data_type, ts_start ASC, collector_id
            {}
            "#,
            match where_clauses.len() {
                0 => "".to_string(),
                _ => format!("WHERE {}", where_clauses.join(" AND ")),
            },
            limit_clause,
        );

        debug!("{}", query_string.as_str());
        let conn = self.reader_pool.get().unwrap();

        let mut stmt = conn.prepare(query_string.as_str())?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crawler::{crawl_collector, Collector};
    use chrono::Utc;

    #[test]
    fn test_new() {
        LocalBrokerDb::new("broker-test.duckdb", true).unwrap();
    }

    #[tokio::test]
    async fn test_insert() {
        let mut db = LocalBrokerDb::new("broker-test.duckdb", true).unwrap();
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
            let db = LocalBrokerDb::new("broker-test.duckdb", false).unwrap();
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
        let db = LocalBrokerDb::new("broker-test.duckdb", false).unwrap();
        let ts = db.get_latest_timestamp().unwrap();
        dbg!(ts);
    }

    #[test]
    fn test_bootstrap() {
        tracing_subscriber::fmt::init();
        let db = LocalBrokerDb::new("broker-test-bootstrap.duckdb", true).unwrap();
        db.bootstrap("items.parquet").unwrap();
    }
}
