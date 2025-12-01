use crate::{BrokerError, LocalBrokerDb};
use serde::{Deserialize, Serialize};
use tracing::debug;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdatesMeta {
    /// database update timestamp
    pub update_ts: i64,
    /// database update duration in seconds
    pub update_duration: i32,
    /// number of items inserted
    pub insert_count: i32,
}

impl LocalBrokerDb {
    pub async fn insert_meta(
        &self,
        crawl_duration: i32,
        item_inserted: i32,
    ) -> Result<Vec<UpdatesMeta>, BrokerError> {
        debug!("Inserting meta information...");
        let now_ts = chrono::Utc::now().timestamp();
        let conn = self.connect()?;

        let query = format!(
            r#"
            INSERT INTO meta (update_ts, update_duration, insert_count) 
            VALUES ('{}', {}, {})
            RETURNING update_ts, update_duration, insert_count
            "#,
            now_ts, crawl_duration, item_inserted
        );

        let mut rows = conn.query(&query, ()).await?;
        let mut inserted = vec![];

        while let Ok(Some(row)) = rows.next().await {
            let update_ts: i64 = row.get(0).unwrap_or(0);
            let update_duration: i32 = row.get::<i64>(1).unwrap_or(0) as i32;
            let insert_count: i32 = row.get::<i64>(2).unwrap_or(0) as i32;
            inserted.push(UpdatesMeta {
                update_ts,
                update_duration,
                insert_count,
            });
        }

        Ok(inserted)
    }

    pub async fn get_latest_updates_meta(&self) -> Result<Option<UpdatesMeta>, BrokerError> {
        let conn = self.connect()?;
        let mut rows = conn
            .query(
                "SELECT update_ts, update_duration, insert_count FROM meta ORDER BY update_ts DESC LIMIT 1",
                (),
            )
            .await?;

        if let Ok(Some(row)) = rows.next().await {
            let update_ts: i64 = row.get(0).unwrap_or(0);
            let update_duration: i32 = row.get::<i64>(1).unwrap_or(0) as i32;
            let insert_count: i32 = row.get::<i64>(2).unwrap_or(0) as i32;
            Ok(Some(UpdatesMeta {
                update_ts,
                update_duration,
                insert_count,
            }))
        } else {
            Ok(None)
        }
    }

    /// Retrieves the total number of entries in the `files` table.
    pub async fn get_entry_count(&self) -> Result<i64, BrokerError> {
        let conn = self.connect()?;
        let mut rows = conn.query("SELECT count(*) FROM files", ()).await?;

        if let Ok(Some(row)) = rows.next().await {
            let count: i64 = row.get(0).unwrap_or(0);
            Ok(count)
        } else {
            Ok(0)
        }
    }
}
