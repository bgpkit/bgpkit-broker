//! Extend the published SQLite snapshot with the rows the writer has indexed since it was
//! built.
//!
//! The snapshot is a product of its own: it is what a self-hosted broker bootstraps from,
//! and it used to be refreshed as a side effect of a crawler that also served the API. Once
//! a single writer owns the catalogue, nothing refreshes the snapshot, so this command does
//! it explicitly against the PostgreSQL catalogue:
//!
//! 1. make sure every collector the writer knows about exists in the SQLite file,
//! 2. insert the files newer than the SQLite file's newest timestamp, in batches,
//! 3. rebuild the `latest` table so `/latest` is correct on the result.
//!
//! Insertion is idempotent on both sides (`INSERT OR IGNORE` in SQLite, a unique key on
//! `(timestamp, collector_id, type_id)`), so a run that is interrupted and repeated adds
//! nothing twice.

use bgpkit_broker::db::{LocalBrokerDb, PostgresDb};
use bgpkit_broker::{BrokerError, BrokerItem};
use tracing::info;

/// Rows read from PostgreSQL per batch. The daily increment is a few thousand rows, but a
/// first run against a stale snapshot can be millions, so never load the whole range.
pub const DEFAULT_BATCH: i64 = 5_000;

pub struct ExtendReport {
    pub collectors_added: usize,
    pub rows_examined: usize,
    pub rows_inserted: usize,
    pub newest_before: Option<chrono::NaiveDateTime>,
    pub newest_after: Option<chrono::NaiveDateTime>,
}

/// Extend `database` from `postgres`, starting at the SQLite file's newest timestamp.
///
/// Takes the database by value because adding collectors has to refresh its in-memory
/// collector cache before any file insert can resolve a collector name to an id.
pub async fn extend(
    mut database: LocalBrokerDb,
    postgres: &PostgresDb,
    batch_size: i64,
) -> Result<ExtendReport, BrokerError> {
    // A file indexed by the writer for a collector the snapshot has never seen would be
    // dropped silently by the insert path, so materialise the collector list first.
    let collectors = postgres.collectors_as_list()?;
    let mut collectors_added = 0usize;
    for collector in &collectors {
        let before = database.collector_exists(&collector.id).await?;
        database.insert_collector(collector).await?;
        if !before {
            collectors_added += 1;
        }
    }
    if collectors_added > 0 {
        database.reload_collectors().await;
    }
    database.ensure_file_types().await?;

    let newest_before = database.get_latest_timestamp().await?;
    // Start at the snapshot's newest slot, not at the beginning of the catalogue. The empty
    // collector/type sort with the group, so the whole slot is re-read and the idempotent
    // insert drops the rows the snapshot already has.
    let mut cursor: Option<(chrono::NaiveDateTime, String, String)> =
        newest_before.map(|timestamp| (timestamp, String::new(), String::new()));
    let mut rows_examined = 0usize;
    let mut rows_inserted = 0usize;

    loop {
        let items: Vec<BrokerItem> = postgres.get_items_after(cursor.clone(), batch_size).await?;
        if items.is_empty() {
            break;
        }
        rows_examined += items.len();
        rows_inserted += database.insert_items(&items, false).await?.len();

        // Advance on the last row's full sort key. A timestamp alone would skip the rest of
        // a slot group when one batch ends in the middle of it.
        cursor = items.last().map(|item| {
            (
                item.ts_start,
                item.collector_id.clone(),
                item.data_type.clone(),
            )
        });

        if (items.len() as i64) < batch_size {
            // Fewer rows than asked for means the writer had nothing more to give.
            break;
        }
        info!(
            "extended to {} ({} rows inserted so far)",
            cursor
                .as_ref()
                .map(|(ts, collector, _)| format!("{} {}", ts, collector))
                .unwrap_or_else(|| "nothing".to_string()),
            rows_inserted
        );
    }

    if rows_inserted > 0 {
        // The `latest` table is what `/latest` reads; rebuild it from the extended files
        // table so the snapshot is internally consistent.
        database.bootstrap_latest_table().await;
    }
    let newest_after = database.get_latest_timestamp().await?;

    Ok(ExtendReport {
        collectors_added,
        rows_examined,
        rows_inserted,
        newest_before,
        newest_after,
    })
}
