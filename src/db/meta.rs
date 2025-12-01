use crate::{BrokerError, LocalBrokerDb};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use tracing::{debug, info};

/// Default number of days to retain meta entries.
const DEFAULT_META_RETENTION_DAYS: i64 = 30;

/// Get the number of days to retain meta entries.
/// Default is 30 days. Can be configured via BGPKIT_BROKER_META_RETENTION_DAYS.
fn get_meta_retention_days() -> i64 {
    std::env::var("BGPKIT_BROKER_META_RETENTION_DAYS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_META_RETENTION_DAYS)
}

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
        .await?;
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

    /// Retrieves the total number of entries in the `files` table.
    ///
    /// # Returns
    ///
    /// * `Ok(i64)` - If the query is successful, this contains the count of entries in the `files` table.
    /// * `Err(BrokerError)` - If there is an issue executing the query or fetching the result.
    pub async fn get_entry_count(&self) -> Result<i64, BrokerError> {
        let count = sqlx::query(
            r#"
            SELECT count(*) FROM files
            "#,
        )
        .map(|row: SqliteRow| row.get::<i64, _>(0))
        .fetch_one(&self.conn_pool)
        .await?;
        Ok(count)
    }

    /// Deletes meta table entries older than the configured retention period.
    ///
    /// # Environment Variables
    /// * `BGPKIT_BROKER_META_RETENTION_DAYS` - Number of days to retain meta entries (default: 30)
    ///
    /// # Returns
    /// * `Ok(u64)` - Number of deleted entries
    /// * `Err(BrokerError)` - If there is an issue executing the query
    pub async fn cleanup_old_meta_entries(&self) -> Result<u64, BrokerError> {
        let retention_days = get_meta_retention_days();
        let cutoff_ts = chrono::Utc::now().timestamp() - (retention_days * 24 * 60 * 60);

        debug!(
            "Cleaning up meta entries older than {} days (before timestamp {})",
            retention_days, cutoff_ts
        );

        let result = sqlx::query(&format!("DELETE FROM meta WHERE update_ts < {}", cutoff_ts))
            .execute(&self.conn_pool)
            .await?;

        let deleted = result.rows_affected();
        if deleted > 0 {
            info!(
                "Cleaned up {} old meta entries (older than {} days)",
                deleted, retention_days
            );
        }

        Ok(deleted)
    }
}
