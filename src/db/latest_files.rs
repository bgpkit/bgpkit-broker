use crate::db::utils::infer_url;
use crate::query::BrokerCollector;
use crate::{BrokerError, BrokerItem};
use chrono::{DateTime, NaiveDateTime};
use std::collections::HashMap;
use tracing::warn;

use super::LocalBrokerDb;

impl LocalBrokerDb {
    /// get the latest timestamp (ts_start) of data entries in broker database
    pub async fn get_latest_timestamp(&self) -> Result<Option<NaiveDateTime>, BrokerError> {
        let conn = self.connect()?;
        let mut rows = conn.query("SELECT MAX(timestamp) FROM files", ()).await?;

        if let Ok(Some(row)) = rows.next().await {
            let timestamp: Option<i64> = row.get(0).ok();
            if let Some(ts) = timestamp {
                let datetime = DateTime::from_timestamp(ts, 0).map(|dt| dt.naive_utc());
                return Ok(datetime);
            }
        }
        Ok(None)
    }

    pub async fn bootstrap_latest_table(&self) {
        let conn = match self.connect() {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to connect for bootstrap_latest_table: {}", e);
                return;
            }
        };

        let _ = conn
            .execute(
                r#"
                INSERT INTO "latest" ("timestamp", "collector_name", "type", "rough_size", "exact_size")
                SELECT
                    MAX("timestamp") AS timestamp,
                    collector_name,
                    type,
                    MAX(rough_size) AS rough_size,
                    MAX(exact_size) AS exact_size
                FROM
                    files_view
                GROUP BY
                    collector_name, type
                ON CONFLICT (collector_name, type)
                DO UPDATE SET
                    "timestamp" = CASE
                        WHEN excluded."timestamp" > "latest"."timestamp" THEN excluded."timestamp"
                        ELSE "latest"."timestamp"
                    END,
                    "rough_size" = CASE
                        WHEN excluded."timestamp" > "latest"."timestamp" THEN excluded."rough_size"
                        ELSE "latest"."rough_size"
                    END,
                    "exact_size" = CASE
                        WHEN excluded."timestamp" > "latest"."timestamp" THEN excluded."exact_size"
                        ELSE "latest"."exact_size"
                    END;
            "#,
                (),
            )
            .await;
    }

    pub async fn update_latest_files(&self, files: &[BrokerItem], bootstrap: bool) {
        let value_str = match bootstrap {
            true => r#"
                SELECT
                    MAX("timestamp") AS timestamp,
                    collector_name,
                    type,
                    MAX(rough_size) AS rough_size,
                    MAX(exact_size) AS exact_size
                FROM
                    files_view
                GROUP BY
                    collector_name, type
                "#
            .to_string(),
            false => {
                if files.is_empty() {
                    return;
                }
                let values = files
                    .iter()
                    .map(|item| {
                        let ts = item.ts_start.and_utc().timestamp();
                        format!(
                            "({}, '{}', '{}', {}, {})",
                            ts,
                            item.collector_id.as_str(),
                            item.data_type.as_str(),
                            item.rough_size,
                            item.exact_size
                        )
                    })
                    .collect::<Vec<String>>()
                    .join(", ");
                format!(" VALUES {} ", values)
            }
        };
        let query_str = format!(
            r#"
                INSERT INTO "latest" ("timestamp", "collector_name", "type", "rough_size", "exact_size")
                {}
                ON CONFLICT (collector_name, type)
                DO UPDATE SET
                    "timestamp" = CASE
                        WHEN excluded."timestamp" > "latest"."timestamp" THEN excluded."timestamp"
                        ELSE "latest"."timestamp"
                    END,
                    "rough_size" = CASE
                        WHEN excluded."timestamp" > "latest"."timestamp" THEN excluded."rough_size"
                        ELSE "latest"."rough_size"
                    END,
                    "exact_size" = CASE
                        WHEN excluded."timestamp" > "latest"."timestamp" THEN excluded."exact_size"
                        ELSE "latest"."exact_size"
                    END;
                    "#,
            value_str
        );

        let conn = match self.connect() {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to connect for update_latest_files: {}", e);
                return;
            }
        };
        let _ = conn.execute(&query_str, ()).await;
    }

    pub async fn get_latest_files(&self) -> Vec<BrokerItem> {
        let collectors_guard = self.collectors.read().await;
        let collector_name_to_info: HashMap<String, BrokerCollector> = collectors_guard
            .iter()
            .map(|c| (c.name.clone(), c.clone()))
            .collect();
        drop(collectors_guard);

        let conn = match self.connect() {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to connect for get_latest_files: {}", e);
                return vec![];
            }
        };

        let mut rows = match conn
            .query(
                "SELECT timestamp, collector_name, type, rough_size, exact_size FROM latest",
                (),
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!("Failed to query latest files: {}", e);
                return vec![];
            }
        };

        let mut items = vec![];
        while let Ok(Some(row)) = rows.next().await {
            let timestamp: i64 = row.get(0).unwrap_or(0);
            let collector_name: String = row.get(1).unwrap_or_default();
            let type_name: String = row.get(2).unwrap_or_default();
            let rough_size: i64 = row.get(3).unwrap_or(0);
            let exact_size: i64 = row.get(4).unwrap_or(0);

            if let Some(collector) = collector_name_to_info.get(&collector_name) {
                let is_rib = type_name.as_str() == "rib";
                let ts_start = DateTime::from_timestamp(timestamp, 0).unwrap().naive_utc();
                let (url, ts_end) = infer_url(collector, &ts_start, is_rib);

                items.push(BrokerItem {
                    ts_start,
                    ts_end,
                    collector_id: collector_name,
                    data_type: type_name,
                    url,
                    rough_size,
                    exact_size,
                });
            }
        }

        items
    }
}
