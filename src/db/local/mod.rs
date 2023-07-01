use crate::{BrokerError, BrokerItem};
use chrono::NaiveDateTime;
use duckdb::{AccessMode, Config, Connection, Error, Row};
use tracing::info;

pub struct LocalBrokerDb {
    conn: Connection,
}

fn open_db_with_retry(
    path: &str,
    read_only: bool,
    wait_millis: u64,
) -> Result<Connection, BrokerError> {
    loop {
        let config = Config::default().access_mode(if read_only {
            AccessMode::ReadOnly
        } else {
            AccessMode::Automatic
        })?;
        let conn = match Connection::open_with_flags(path, config) {
            Ok(c) => Some(c),
            Err(err) => {
                if let Error::DuckDBFailure(e, _msg) = &err {
                    if e.extended_code == 1 {
                        None
                    } else {
                        return Err(BrokerError::from(err));
                    }
                } else {
                    return Err(BrokerError::from(err));
                }
            }
        };
        if let Some(conn) = conn {
            return Ok(conn);
        }
        std::thread::sleep(std::time::Duration::from_millis(wait_millis));
    }
}

impl LocalBrokerDb {
    pub fn new(path: Option<String>, force_reset: bool) -> Result<Self, BrokerError> {
        let conn = match path {
            Some(path) => open_db_with_retry(&path, false, 100)?,
            None => Connection::open_in_memory()?,
        };

        Self::create_table(&conn, force_reset)?;
        Ok(LocalBrokerDb { conn })
    }

    pub fn new_reader(path: &str) -> Result<Self, BrokerError> {
        let conn = open_db_with_retry(path, true, 500)?;
        Ok(LocalBrokerDb { conn })
    }

    fn create_table(conn: &Connection, reset: bool) -> Result<(), BrokerError> {
        let create_statement = match reset {
            true => "CREATE OR REPLACE TABLE",
            false => "CREATE TABLE IF NOT EXISTS",
        };
        conn.execute(
            &format!(
                r#"
        {} items (
            collector_id TEXT,
            ts_start TIMESTAMP,
            ts_end TIMESTAMP,
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

    pub fn insert_items(
        &mut self,
        items: &Vec<BrokerItem>,
    ) -> Result<Vec<BrokerItem>, BrokerError> {
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
            let mut statement = self.conn.prepare(
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

    #[allow(clippy::too_many_arguments)]
    pub fn search_items(
        &self,
        collector_id: Option<String>,
        project: Option<String>,
        data_type: Option<String>,
        ts_start: Option<NaiveDateTime>,
        ts_end: Option<NaiveDateTime>,
        page: Option<usize>,
        page_size: Option<usize>,
    ) -> Result<Vec<BrokerItem>, BrokerError> {
        let mut where_clauses: Vec<String> = vec![];
        if let Some(collector_id) = collector_id {
            where_clauses.push(format!("collector_id = '{}'", collector_id));
        }
        if let Some(project) = project {
            match project.to_lowercase().as_str() {
                "ris" | "riperis" | "ripe-ris" => {
                    where_clauses.push("collector_id like 'rrc%'".to_string());
                }
                "routeviews" | "rv" => {
                    where_clauses.push("collector_id like 'route-views%'".to_string());
                }
                _ => {
                    return Err(BrokerError::BrokerError(format!(
                        "Unknown project: {}",
                        project
                    )));
                }
            }
            where_clauses.push(format!("project = '{}'", project));
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
        let page = page.unwrap_or(0);
        let page_size = page_size.unwrap_or(100);
        let offset = page * page_size;
        let limit = page_size;

        let mut stmt = self.conn.prepare(
            format!(
                r#"
            SELECT collector_id, epoch(ts_start), epoch(ts_end), data_type, url, rough_size, exact_size
            FROM items
            WHERE {}
            ORDER BY ts_start ASC
            LIMIT {}
            OFFSET {}
            "#,
                where_clauses.join(" AND "),
                limit,
                offset
            )
                .as_str(),
        )?;
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
        LocalBrokerDb::new(Some("broker-test.duckdb".to_string()), true).unwrap();
    }

    #[tokio::test]
    async fn test_insert() {
        let mut db = LocalBrokerDb::new(Some("broker-test.duckdb".to_string()), true).unwrap();
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

    #[tokio::test]
    async fn test_search() {
        let db = LocalBrokerDb::new_reader("broker-test.duckdb").unwrap();

        let items = db
            .search_items(
                Some("route-views2".to_string()),
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

    #[tokio::test]
    async fn test_loop() {
        loop {
            let db = LocalBrokerDb::new_reader("broker-test.duckdb").unwrap();
            let _items = db
                .search_items(
                    Some("route-views2".to_string()),
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
}
