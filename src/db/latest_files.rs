use crate::db::utils::infer_url;
use crate::query::BrokerCollector;
use crate::{BrokerError, BrokerItem};
use chrono::NaiveDateTime;
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use std::collections::HashMap;

use super::LocalBrokerDb;

impl LocalBrokerDb {
    /// get the latest timestamp (ts_start) of data entries in broker database
    pub async fn get_latest_timestamp(&self) -> Result<Option<NaiveDateTime>, BrokerError> {
        // FIXME: handle empty database case
        let timestamp = sqlx::query(
            r#"
            SELECT MAX(timestamp) FROM files
            "#,
        )
        .map(|row: SqliteRow| row.get::<i64, _>(0))
        .fetch_one(&self.conn_pool)
        .await
        .unwrap();

        let datetime = NaiveDateTime::from_timestamp_opt(timestamp, 0);
        Ok(datetime)
    }

    pub async fn bootstrap_latest_table(&self) {
        sqlx::query(
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
        ).execute(&self.conn_pool).await.unwrap();
    }

    pub async fn update_latest_files(&self, files: &Vec<BrokerItem>, bootstrap: bool) {
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
                        format!(
                            "({}, '{}', '{}', {}, {})",
                            item.ts_start.timestamp(),
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
        sqlx::query(query_str.as_str())
            .execute(&self.conn_pool)
            .await
            .unwrap();
    }

    pub async fn get_latest_files(&self) -> Vec<BrokerItem> {
        let collector_name_to_info = self
            .collectors
            .iter()
            .map(|c| (c.name.clone(), c.clone()))
            .collect::<HashMap<String, BrokerCollector>>();
        sqlx::query(
            "select timestamp, collector_name, type, rough_size, exact_size from latest",
        )
        .map(|row: SqliteRow| {
            let timestamp = row.get::<i64, _>(0);
            let collector_name = row.get::<String, _>(1);
            let type_name = row.get::<String, _>(2);
            let rough_size = row.get::<i64, _>(3);
            let exact_size = row.get::<i64, _>(4);
            let collector = collector_name_to_info.get(&collector_name).unwrap();

            let is_rib = type_name.as_str() == "rib";

            let ts_start = NaiveDateTime::from_timestamp_opt(timestamp, 0).unwrap();
            let (url, ts_end) = infer_url(collector, &ts_start, is_rib);

            BrokerItem {
                ts_start,
                ts_end,
                collector_id: collector_name,
                data_type: type_name,
                url,
                rough_size,
                exact_size,
            }
        })
        .fetch_all(&self.conn_pool)
        .await
        .unwrap()
    }
}
