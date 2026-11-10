use crate::db::utils::infer_url;
use crate::query::BrokerCollector;
use crate::{BrokerError, BrokerItem};
use chrono::{DateTime, NaiveDateTime};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use std::collections::HashMap;
use tracing::error;

use super::LocalBrokerDb;

impl LocalBrokerDb {
    /// get the latest timestamp (ts_start) of data entries in broker database
    pub async fn get_latest_timestamp(&self) -> Result<Option<NaiveDateTime>, BrokerError> {
        // MAX() over an empty table is NULL, which the row reader would turn into 1970.
        let timestamp: Option<i64> = sqlx::query(
            r#"
            SELECT MAX(timestamp) FROM files
            "#,
        )
        .map(|row: SqliteRow| row.get::<Option<i64>, _>(0))
        .fetch_one(&self.conn_pool)
        .await?;

        let datetime = timestamp
            .and_then(|ts| DateTime::from_timestamp(ts, 0))
            .map(|dt| dt.naive_utc());
        Ok(datetime)
    }

    /// Newest file timestamp per project, for the crawler's freshness check.
    pub async fn get_latest_by_project(&self) -> Result<Vec<(String, NaiveDateTime)>, BrokerError> {
        let rows = sqlx::query(
            r#"
            SELECT c.project, MAX(f.timestamp)
            FROM files f JOIN collectors c ON f.collector_id = c.id
            WHERE c.project IS NOT NULL
            GROUP BY c.project
            "#,
        )
        .fetch_all(&self.conn_pool)
        .await?;

        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let project: String = row.get(0);
                let timestamp: Option<i64> = row.get(1);
                let ts = timestamp.and_then(|ts| DateTime::from_timestamp(ts, 0))?;
                Some((project, ts.naive_utc()))
            })
            .collect())
    }

    pub async fn bootstrap_latest_table(&self) {
        if let Err(e) = sqlx::query(
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
            "#
        ).execute(&self.conn_pool).await {
            error!("failed to bootstrap latest table: {}", e);
        }
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
        if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(query_str.as_str()))
            .execute(&self.conn_pool)
            .await
        {
            error!("failed to update latest files: {}", e);
        }
    }

    pub async fn get_latest_files(&self) -> Result<Vec<BrokerItem>, BrokerError> {
        let collector_name_to_info = self
            .collectors
            .iter()
            .map(|c| (c.name.clone(), c.clone()))
            .collect::<HashMap<String, BrokerCollector>>();
        let rows = sqlx::query(
            "select timestamp, collector_name, type, rough_size, exact_size from latest",
        )
        .map(|row: SqliteRow| {
            let timestamp = row.get::<i64, _>(0);
            let collector_name = row.get::<String, _>(1);
            let type_name = row.get::<String, _>(2);
            let rough_size = row.get::<i64, _>(3);
            let exact_size = row.get::<i64, _>(4);

            // Skip if collector not found
            let collector = match collector_name_to_info.get(&collector_name) {
                Some(c) => c,
                None => return None,
            };

            let is_rib = type_name.as_str() == "rib";

            let ts_start = match DateTime::from_timestamp(timestamp, 0) {
                Some(dt) => dt.naive_utc(),
                None => return None,
            };
            let (url, ts_end) = infer_url(collector, &ts_start, is_rib);

            Some(BrokerItem {
                ts_start,
                ts_end,
                collector_id: collector_name,
                data_type: type_name,
                url,
                rough_size,
                exact_size,
            })
        })
        .fetch_all(&self.conn_pool)
        .await?;
        Ok(rows.into_iter().flatten().collect())
    }
}
