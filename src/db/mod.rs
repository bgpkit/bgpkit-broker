use crate::{BrokerError, BrokerItem};
use duckdb::{params, Connection};
use std::path::Path;

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
        CREATE OR IGNORE TABLE items (
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
}
