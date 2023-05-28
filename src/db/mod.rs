use crate::{BrokerError, BrokerItem};
use chrono::NaiveDateTime;
use duckdb::{params, Connection, Row};

pub struct BrokerDb {
    conn: duckdb::Connection,
}

impl BrokerDb {
    pub fn new(path: Option<String>, reset: bool) -> Result<Self, BrokerError> {
        let conn = match path {
            Some(path) => duckdb::Connection::open(path)?,
            None => duckdb::Connection::open_in_memory()?,
        };

        if reset {
            Self::reset_db(&conn)?;
        }
        Ok(BrokerDb { conn })
    }

    fn reset_db(conn: &Connection) -> Result<(), BrokerError> {
        conn.execute(
            r#"
        CREATE OR REPLACE TABLE items (
            collector_id TEXT,
            ts_start TIMESTAMP,
            ts_end TIMESTAMP,
            data_type TEXT,
            url TEXT,
            rough_size INT,
            exact_size INT,
            PRIMARY KEY(collector_id, ts_start, data_type)
        )
        "#,
            [],
        )?;
        Ok(())
    }

    pub fn insert_items(&mut self, items: &Vec<BrokerItem>) -> Result<(), BrokerError> {
        // TODO: should return all inserted items
        for batch in items.chunks(1000) {
            let tx = self.conn.transaction()?;
            let mut stmt = tx.prepare(
                    r#"
            INSERT OR IGNORE INTO items (collector_id, ts_start, ts_end, data_type, url, rough_size, exact_size)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#
            )?;

            for item in batch {
                stmt.execute(params![
                    &item.collector_id,
                    &item.ts_start.to_string(),
                    &item.ts_end.to_string(),
                    &item.data_type,
                    &item.url,
                    &item.rough_size.to_string(),
                    &item.exact_size.to_string(),
                ])?;
            }
            tx.commit()?;
        }
        Ok(())
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
    use crate::crawler::{crawl_routeviews, Collector};
    use chrono::Utc;

    #[test]
    fn test_new() {
        BrokerDb::new(Some("broker.duckdb".to_string()), true).unwrap();
    }

    #[tokio::test]
    async fn test_insert() {
        let mut db = BrokerDb::new(Some("broker.duckdb".to_string()), true).unwrap();
        let two_months_ago = Utc::now().date_naive() - chrono::Duration::days(60);
        let collector = Collector {
            id: "route-views2".to_string(),
            project: "routeviews".to_string(),
            url: "https://routeviews.org/bgpdata/".to_string(),
        };
        let routeviews_crawler = crawl_routeviews(&collector, Some(two_months_ago))
            .await
            .unwrap();
        db.insert_items(&routeviews_crawler).unwrap();
    }

    #[tokio::test]
    async fn test_search() {
        let mut db = BrokerDb::new(Some("broker.duckdb".to_string()), true).unwrap();
        let two_months_ago = Utc::now().date_naive() - chrono::Duration::days(60);
        let collector = Collector {
            id: "route-views2".to_string(),
            project: "routeviews".to_string(),
            url: "https://routeviews.org/bgpdata/".to_string(),
        };
        let routeviews_crawler = crawl_routeviews(&collector, Some(two_months_ago))
            .await
            .unwrap();
        db.insert_items(&routeviews_crawler).unwrap();

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

        let items = db
            .search_items(
                None,
                None,
                None,
                Some(NaiveDateTime::from_timestamp_opt(1685246400, 0).unwrap()),
                Some(NaiveDateTime::from_timestamp_opt(1685250000, 0).unwrap()),
                None,
                None,
            )
            .unwrap();

        dbg!(items.iter().take(2).collect::<Vec<&BrokerItem>>());

        let items = db
            .search_items(
                None,
                None,
                None,
                Some(NaiveDateTime::from_timestamp_opt(1685246401, 0).unwrap()),
                Some(NaiveDateTime::from_timestamp_opt(1685250000, 0).unwrap()),
                None,
                None,
            )
            .unwrap();

        dbg!(items.iter().take(2).collect::<Vec<&BrokerItem>>());
    }
}
