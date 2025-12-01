//! Database trait definitions for the BGPKIT Broker.
//!
//! This module defines the `BrokerDb` trait that abstracts database operations,
//! allowing for multiple backend implementations (SQLite, PostgreSQL).

use crate::query::BrokerCollector;
use crate::{BrokerError, BrokerItem, Collector};
use async_trait::async_trait;
use chrono::NaiveDateTime;

/// Result of a database search operation with pagination info.
#[derive(Debug, Clone)]
pub struct DbSearchResult {
    pub items: Vec<BrokerItem>,
    pub page: usize,
    pub page_size: usize,
    pub total: usize,
}

/// Metadata about a database update operation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UpdatesMeta {
    /// Database update timestamp (Unix epoch seconds)
    pub update_ts: i64,
    /// Database update duration in seconds
    pub update_duration: i32,
    /// Number of items inserted
    pub insert_count: i32,
}

/// Default page size for search results.
pub const DEFAULT_PAGE_SIZE: usize = 100;

/// The `BrokerDb` trait defines the interface for database backends.
///
/// This trait is implemented by both SQLite and PostgreSQL backends,
/// providing a unified interface for database operations.
#[async_trait]
pub trait BrokerDb: Send + Sync {
    // =========================================================================
    // Initialization and metadata
    // =========================================================================

    /// Get all collectors from the database.
    fn collectors(&self) -> Vec<BrokerCollector>;

    /// Reload collectors from the database.
    async fn reload_collectors(&mut self) -> Result<(), BrokerError>;

    /// Run database-specific optimization (e.g., ANALYZE).
    async fn analyze(&self) -> Result<(), BrokerError>;

    // =========================================================================
    // Search operations
    // =========================================================================

    /// Search for broker items with optional filters.
    ///
    /// # Arguments
    /// * `collectors` - Optional list of collector names to filter by
    /// * `project` - Optional project name ("ripe-ris" or "route-views")
    /// * `data_type` - Optional data type ("rib" or "updates")
    /// * `ts_start` - Optional start timestamp
    /// * `ts_end` - Optional end timestamp
    /// * `page` - Optional page number (1-indexed)
    /// * `page_size` - Optional page size
    #[allow(clippy::too_many_arguments)]
    async fn search(
        &self,
        collectors: Option<Vec<String>>,
        project: Option<String>,
        data_type: Option<String>,
        ts_start: Option<NaiveDateTime>,
        ts_end: Option<NaiveDateTime>,
        page: Option<usize>,
        page_size: Option<usize>,
    ) -> Result<DbSearchResult, BrokerError>;

    // =========================================================================
    // Insert operations
    // =========================================================================

    /// Insert a batch of items into the database.
    ///
    /// # Arguments
    /// * `items` - Items to insert
    /// * `update_latest` - Whether to update the latest files table/view
    ///
    /// # Returns
    /// Vector of actually inserted items (excluding duplicates)
    async fn insert_items(
        &self,
        items: &[BrokerItem],
        update_latest: bool,
    ) -> Result<Vec<BrokerItem>, BrokerError>;

    /// Insert a new collector into the database.
    async fn insert_collector(&self, collector: &Collector) -> Result<(), BrokerError>;

    // =========================================================================
    // Latest files operations
    // =========================================================================

    /// Get the latest timestamp in the database.
    async fn get_latest_timestamp(&self) -> Result<Option<NaiveDateTime>, BrokerError>;

    /// Get the latest files for each collector/type combination.
    async fn get_latest_files(&self) -> Vec<BrokerItem>;

    /// Update the latest files table/view.
    ///
    /// # Arguments
    /// * `files` - Files to update
    /// * `bootstrap` - If true, bootstrap from files table instead of using provided files
    async fn update_latest_files(&self, files: &[BrokerItem], bootstrap: bool);

    /// Bootstrap the latest files table from the files table.
    async fn bootstrap_latest_table(&self);

    // =========================================================================
    // Meta operations
    // =========================================================================

    /// Insert update metadata.
    async fn insert_meta(
        &self,
        crawl_duration: i32,
        item_inserted: i32,
    ) -> Result<Vec<UpdatesMeta>, BrokerError>;

    /// Get the latest update metadata.
    async fn get_latest_updates_meta(&self) -> Result<Option<UpdatesMeta>, BrokerError>;
}
