use crate::{BrokerError, LocalBrokerDb};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use tracing::debug;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "cli", derive(utoipa::ToSchema))]
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
        .await
        .unwrap();
        Ok(inserted)
    }

    pub async fn get_latest_updates_meta(&self) -> Result<Option<UpdatesMeta>, BrokerError> {
        let entries = sqlx::query(
            r#"
            SELECT update_ts, update_duration, insert_count FROM meta ORDER BY update_ts DESC LIMIT 1;
            "#,
        ).map(|row: SqliteRow| {
            let update_ts = row.get::<i64, _>(0);
            let update_duration = row.get::<i32, _>(1);
            let insert_count = row.get::<i32, _>(2);
            UpdatesMeta {
                update_ts,
                update_duration,
                insert_count,
            }
        }).fetch_all(&self.conn_pool).await?;
        if entries.is_empty() {
            Ok(None)
        } else {
            Ok(Some(entries[0].clone()))
        }
    }

    /// Check if data bootstrap is needed
    pub async fn get_entry_count(&self) -> Result<i64, BrokerError> {
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
}
