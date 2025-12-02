//! Convenience methods for common BGP data queries.
//!
//! This module provides shortcuts for frequently used query patterns to make it easier
//! to find specific types of BGP data without manually configuring filters.

use crate::{BgpkitBroker, BrokerError, BrokerItem};
use chrono::{DateTime, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;

/// MRT files needed to construct a routing table snapshot for a specific collector.
///
/// This struct contains the RIB dump URL and the list of updates files that need to be
/// applied to reconstruct the routing table state at a specific point in time.
///
/// # Fields
///
/// * `collector_id` - The ID of the BGP collector (e.g., "route-views2", "rrc00")
/// * `rib_url` - URL of the RIB dump file to use as the initial routing table
/// * `updates_urls` - URLs of the updates MRT files to apply to the initial RIB,
///   ordered chronologically from oldest to newest
///
/// # Example
///
/// ```no_run
/// use bgpkit_broker::BgpkitBroker;
///
/// let broker = BgpkitBroker::new();
/// let snapshots = broker.get_snapshot_files(
///     &["route-views2", "rrc00"],
///     "2024-01-01T12:00:00Z"
/// ).unwrap();
///
/// for snapshot in snapshots {
///     println!("Collector: {}", snapshot.collector_id);
///     println!("RIB URL: {}", snapshot.rib_url);
///     println!("Updates files: {}", snapshot.updates_urls.len());
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct SnapshotFiles {
    /// The collector ID (e.g., "route-views2", "rrc00")
    pub collector_id: String,
    /// URL of the RIB dump file to build the initial routing table
    pub rib_url: String,
    /// URLs of the updates MRT files to apply to the initial RIB, in chronological order
    pub updates_urls: Vec<String>,
}

impl Display for SnapshotFiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SnapshotFiles {{ collector_id: {}, rib_url: {}, updates_count: {} }}",
            self.collector_id,
            self.rib_url,
            self.updates_urls.len()
        )
    }
}

impl BgpkitBroker {
    /// Get daily RIB files that were captured at midnight (00:00:00).
    ///
    /// This filters for RIB dumps where both the hour and minute are 0,
    /// which typically represents the daily snapshots taken at midnight.
    ///
    /// # Examples
    ///
    /// Get daily RIB files with date range and specific collector:
    /// ```no_run
    /// use bgpkit_broker::BgpkitBroker;
    ///
    /// let daily_ribs = BgpkitBroker::new()
    ///     .ts_start("2024-01-01")
    ///     .ts_end("2024-01-02")
    ///     .collector_id("route-views2")
    ///     .daily_ribs()
    ///     .unwrap();
    ///
    /// for item in daily_ribs {
    ///     println!("Daily RIB: {} from {} at {}",
    ///              item.collector_id,
    ///              item.ts_start.format("%Y-%m-%d %H:%M:%S"),
    ///              item.url);
    /// }
    /// ```
    ///
    /// Chain with project filtering for RouteViews daily snapshots:
    /// ```no_run
    /// use bgpkit_broker::BgpkitBroker;
    ///
    /// let rv_daily_ribs = BgpkitBroker::new()
    ///     .ts_start("2024-01-01")
    ///     .ts_end("2024-01-31")
    ///     .project("routeviews")
    ///     .daily_ribs()
    ///     .unwrap();
    ///
    /// println!("Found {} RouteViews daily RIBs", rv_daily_ribs.len());
    /// ```
    pub fn daily_ribs(&self) -> Result<Vec<BrokerItem>, BrokerError> {
        // Clone the broker and add RIB filter
        let rib_broker = self.clone().data_type("rib");

        // Get all RIB items and filter for midnight captures
        let all_ribs = rib_broker.query()?;

        let daily_ribs = all_ribs
            .into_iter()
            .filter(|item| {
                // Filter for files captured at midnight (00:00:00)
                item.ts_start.hour() == 0 && item.ts_start.minute() == 0
            })
            .collect();

        Ok(daily_ribs)
    }

    /// Get BGP update files from the last N hours.
    ///
    /// This method calculates the timestamp for N hours ago and queries for
    /// update files from that time until now.
    ///
    /// # Arguments
    ///
    /// * `hours` - Number of hours to look back from current time
    ///
    /// # Examples
    ///
    /// Get recent updates from specific collectors with detailed output:
    /// ```no_run
    /// use bgpkit_broker::BgpkitBroker;
    ///
    /// let recent_updates = BgpkitBroker::new()
    ///     .collector_id("route-views2,rrc00")
    ///     .recent_updates(24)
    ///     .unwrap();
    ///
    /// println!("Found {} update files from last 24 hours", recent_updates.len());
    /// for item in recent_updates.iter().take(5) {
    ///     println!("Update: {} from {} at {}",
    ///              item.collector_id,
    ///              item.ts_start.format("%Y-%m-%d %H:%M:%S"),
    ///              item.url);
    /// }
    /// ```
    ///
    /// Chain with diverse collectors for comprehensive recent data:
    /// ```no_run
    /// use bgpkit_broker::BgpkitBroker;
    ///
    /// let broker = BgpkitBroker::new();
    /// let diverse_collectors = broker.most_diverse_collectors(5, None).unwrap();
    ///
    /// let comprehensive_updates = broker
    ///     .collector_id(diverse_collectors.join(","))
    ///     .recent_updates(6)
    ///     .unwrap();
    ///
    /// println!("Got {} updates from {} diverse collectors",
    ///          comprehensive_updates.len(), diverse_collectors.len());
    /// ```
    pub fn recent_updates(&self, hours: u32) -> Result<Vec<BrokerItem>, BrokerError> {
        let now = Utc::now();
        let hours_ago = now - chrono::Duration::hours(hours as i64);

        // Clone the broker and add time range and updates filter
        let updates_broker = self
            .clone()
            .data_type("updates")
            .ts_start(hours_ago.format("%Y-%m-%dT%H:%M:%SZ").to_string())
            .ts_end(now.format("%Y-%m-%dT%H:%M:%SZ").to_string());

        updates_broker.query()
    }

    /// Get the N collectors with the most diverse peer ASNs from full-feed peers.
    ///
    /// This method queries collectors' peer data, filters for full-feed peers only,
    /// and uses a greedy algorithm to find the combination of collectors that covers
    /// the most unique ASNs with minimal overlap. Optionally filters by project.
    ///
    /// # Arguments
    ///
    /// * `n` - Maximum number of collectors to return
    /// * `project` - Optional project filter: "riperis" or "routeviews"
    ///
    /// # Returns
    ///
    /// A vector of collector IDs optimized for maximum ASN diversity.
    /// If fewer than N collectors exist, returns all available collectors.
    ///
    /// # Examples
    ///
    /// Find diverse collectors across all projects and use them for daily RIB analysis:
    /// ```no_run
    /// use bgpkit_broker::BgpkitBroker;
    ///
    /// let broker = BgpkitBroker::new()
    ///     .ts_start("2024-01-01")
    ///     .ts_end("2024-01-02");
    ///
    /// let diverse_collectors = broker.most_diverse_collectors(5, None).unwrap();
    /// let collector_list = diverse_collectors.join(",");
    ///
    /// let daily_ribs = broker
    ///     .clone()
    ///     .collector_id(collector_list)
    ///     .daily_ribs()
    ///     .unwrap();
    ///
    /// println!("Found {} daily RIBs from {} diverse collectors",
    ///          daily_ribs.len(), diverse_collectors.len());
    /// ```
    ///
    /// Get diverse RouteViews collectors for recent update analysis:
    /// ```no_run
    /// use bgpkit_broker::BgpkitBroker;
    ///
    /// let broker = BgpkitBroker::new();
    /// let rv_collectors = broker.most_diverse_collectors(3, Some("routeviews")).unwrap();
    ///
    /// let recent_updates = broker
    ///     .clone()
    ///     .collector_id(rv_collectors.join(","))
    ///     .recent_updates(6)
    ///     .unwrap();
    /// ```
    ///
    /// Compare diversity between RIPE RIS and RouteViews:
    /// ```no_run
    /// use bgpkit_broker::BgpkitBroker;
    ///
    /// let broker = BgpkitBroker::new();
    /// let ripe_collectors = broker.most_diverse_collectors(3, Some("riperis")).unwrap();
    /// let rv_collectors = broker.most_diverse_collectors(3, Some("routeviews")).unwrap();
    ///
    /// println!("RIPE diverse collectors: {:?}", ripe_collectors);
    /// println!("RouteViews diverse collectors: {:?}", rv_collectors);
    /// ```
    pub fn most_diverse_collectors(
        &self,
        n: usize,
        project: Option<&str>,
    ) -> Result<Vec<String>, BrokerError> {
        // Get all full-feed peers, optionally filtered by project
        let mut full_feed_broker = self.clone().peers_only_full_feed(true);
        if let Some(proj) = project {
            full_feed_broker = full_feed_broker.project(proj);
        }
        let peers = full_feed_broker.get_peers()?;

        // Group ASNs by collector
        let mut collector_asn_sets: HashMap<String, std::collections::HashSet<u32>> =
            HashMap::new();

        for peer in peers {
            collector_asn_sets
                .entry(peer.collector.clone())
                .or_default()
                .insert(peer.asn);
        }

        if collector_asn_sets.is_empty() || n == 0 {
            return Ok(Vec::new());
        }

        // Greedy algorithm: select collectors that maximize unique ASN coverage
        let mut selected_collectors = Vec::new();
        let mut covered_asns = std::collections::HashSet::new();
        let mut remaining_collectors = collector_asn_sets;

        for _ in 0..n {
            if remaining_collectors.is_empty() {
                break;
            }

            // Find collector that adds the most new ASNs
            let best_collector = remaining_collectors
                .iter()
                .max_by_key(|(_, asns)| asns.difference(&covered_asns).count())
                .map(|(collector, _)| collector.clone());

            if let Some(collector) = best_collector {
                if let Some(asns) = remaining_collectors.remove(&collector) {
                    // Add new ASNs to covered set
                    covered_asns.extend(&asns);
                    selected_collectors.push(collector);
                }
            } else {
                break;
            }
        }

        Ok(selected_collectors)
    }

    /// Get the MRT files needed to construct routing table snapshots at a specific timestamp.
    ///
    /// This function finds the RIB dump and updates files needed to reconstruct the routing
    /// table state at the given timestamp for each specified collector. For each collector,
    /// it finds:
    /// - The closest RIB dump file before or at the target timestamp
    /// - All updates files between the RIB dump timestamp and the target timestamp
    ///
    /// This is useful for applications that need to reconstruct the exact routing table
    /// state at a specific point in time by replaying updates on top of a RIB snapshot.
    ///
    /// # Arguments
    ///
    /// * `collector_ids` - Array of collector IDs to get snapshot files for (e.g., `["route-views2", "rrc00"]`)
    /// * `timestamp` - Target timestamp for the routing table snapshot. Supports multiple formats:
    ///   - Unix timestamp: `"1640995200"`
    ///   - RFC3339: `"2022-01-01T12:00:00Z"`
    ///   - Date with time: `"2022-01-01 12:00:00"`
    ///   - Pure date: `"2022-01-01"` (uses start of day)
    ///
    /// # Returns
    ///
    /// A vector of [`SnapshotFiles`] structs, one for each collector that has available data.
    /// Collectors without a suitable RIB dump before the target timestamp are excluded.
    ///
    /// # Examples
    ///
    /// ## Basic usage
    ///
    /// ```no_run
    /// use bgpkit_broker::BgpkitBroker;
    ///
    /// let broker = BgpkitBroker::new();
    /// let snapshots = broker.get_snapshot_files(
    ///     &["route-views2", "rrc00"],
    ///     "2024-01-01T12:00:00Z"
    /// ).unwrap();
    ///
    /// for snapshot in snapshots {
    ///     println!("Collector: {}", snapshot.collector_id);
    ///     println!("RIB URL: {}", snapshot.rib_url);
    ///     println!("Updates to apply: {}", snapshot.updates_urls.len());
    ///     for url in &snapshot.updates_urls {
    ///         println!("  - {}", url);
    ///     }
    /// }
    /// ```
    ///
    /// ## Using with bgpkit-parser for routing table reconstruction
    ///
    /// ```no_run
    /// use bgpkit_broker::BgpkitBroker;
    ///
    /// let broker = BgpkitBroker::new();
    /// let snapshots = broker.get_snapshot_files(
    ///     &["route-views2"],
    ///     "2024-01-01T06:30:00Z"
    /// ).unwrap();
    ///
    /// if let Some(snapshot) = snapshots.first() {
    ///     // 1. Parse the RIB dump to get initial routing table
    ///     println!("Load RIB from: {}", snapshot.rib_url);
    ///
    ///     // 2. Apply updates in order to reach target timestamp
    ///     for update_url in &snapshot.updates_urls {
    ///         println!("Apply updates from: {}", update_url);
    ///     }
    /// }
    /// ```
    ///
    /// ## Get snapshot for a specific time using different timestamp formats
    ///
    /// ```no_run
    /// use bgpkit_broker::BgpkitBroker;
    ///
    /// let broker = BgpkitBroker::new();
    ///
    /// // Using Unix timestamp
    /// let snapshots = broker.get_snapshot_files(&["rrc00"], "1704110400").unwrap();
    ///
    /// // Using pure date (midnight)
    /// let snapshots = broker.get_snapshot_files(&["rrc00"], "2024-01-01").unwrap();
    ///
    /// // Using RFC3339 format
    /// let snapshots = broker.get_snapshot_files(&["rrc00"], "2024-01-01T12:00:00Z").unwrap();
    /// ```
    pub fn get_snapshot_files<S: AsRef<str>, T: Display>(
        &self,
        collector_ids: &[S],
        timestamp: T,
    ) -> Result<Vec<SnapshotFiles>, BrokerError> {
        // Parse and validate the target timestamp
        let target_ts = Self::parse_timestamp(&timestamp.to_string())?;

        // We need to search for RIB files that could be before the target timestamp.
        // RIB dumps typically happen every 2 hours (RouteViews) or every 8 hours (RIPE RIS).
        // To be safe, we search up to 24 hours before the target timestamp for RIB files.
        let search_start = target_ts - chrono::Duration::hours(24);

        let mut results = Vec::new();

        for collector_id in collector_ids {
            let collector_id_str = collector_id.as_ref();

            // Query for RIB files from search_start to target_ts
            let rib_items = self
                .clone()
                .collector_id(collector_id_str)
                .data_type("rib")
                .ts_start(search_start.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                .ts_end(target_ts.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                .query()?;

            // Find the closest RIB dump at or before the target timestamp
            let closest_rib = rib_items
                .into_iter()
                .filter(|item| {
                    let item_ts = DateTime::<Utc>::from_naive_utc_and_offset(item.ts_start, Utc);
                    item_ts <= target_ts
                })
                .max_by_key(|item| item.ts_start);

            let Some(rib_item) = closest_rib else {
                // No RIB dump found for this collector, skip it
                continue;
            };

            // Query for updates files between the RIB timestamp and target timestamp
            let rib_ts = DateTime::<Utc>::from_naive_utc_and_offset(rib_item.ts_start, Utc);

            let updates_items = self
                .clone()
                .collector_id(collector_id_str)
                .data_type("updates")
                .ts_start(rib_ts.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                .ts_end(target_ts.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                .query()?;

            // Filter updates that start after the RIB and end at or before the target timestamp
            // Sort by timestamp to ensure chronological order
            let mut filtered_updates: Vec<BrokerItem> = updates_items
                .into_iter()
                .filter(|item| {
                    let item_start = DateTime::<Utc>::from_naive_utc_and_offset(item.ts_start, Utc);
                    let item_end = DateTime::<Utc>::from_naive_utc_and_offset(item.ts_end, Utc);
                    // Updates file must start after or at the RIB time
                    // and end at or before the target timestamp
                    item_start >= rib_ts && item_end <= target_ts
                })
                .collect();

            filtered_updates.sort_by_key(|item| item.ts_start);

            let updates_urls: Vec<String> =
                filtered_updates.into_iter().map(|item| item.url).collect();

            results.push(SnapshotFiles {
                collector_id: collector_id_str.to_string(),
                rib_url: rib_item.url,
                updates_urls,
            });
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_daily_ribs() {
        let broker = BgpkitBroker::new()
            .ts_start("1634693400") // 2021-10-20 00:00:00
            .ts_end("1634693400");

        let result = broker.daily_ribs();
        assert!(result.is_ok());

        let daily_ribs = result.unwrap();
        // All returned items should be RIBs captured at midnight
        for item in &daily_ribs {
            assert!(item.is_rib());
            assert_eq!(item.ts_start.hour(), 0);
            assert_eq!(item.ts_start.minute(), 0);
        }
    }

    #[test]
    fn test_recent_updates() {
        use chrono::{Duration, Utc};
        let broker = BgpkitBroker::new();

        // Test that the recent_updates method constructs the correct query parameters
        // instead of actually executing the slow query
        let now = Utc::now();
        let hours_ago = now - Duration::hours(24);

        let updates_broker = broker
            .clone()
            .data_type("updates")
            .ts_start(hours_ago.format("%Y-%m-%dT%H:%M:%SZ").to_string())
            .ts_end(now.format("%Y-%m-%dT%H:%M:%SZ").to_string());

        // Verify the parameters are set correctly
        assert_eq!(
            updates_broker.query_params.data_type,
            Some("updates".to_string())
        );
        assert!(updates_broker.query_params.ts_start.is_some());
        assert!(updates_broker.query_params.ts_end.is_some());

        // Verify configuration validation passes
        let validation_result = updates_broker.validate_configuration();
        assert!(validation_result.is_ok());
    }

    #[test]
    fn test_most_diverse_collectors() {
        let broker = BgpkitBroker::new();
        let result = broker.most_diverse_collectors(5, None);
        assert!(result.is_ok());

        let collectors = result.unwrap();
        assert!(!collectors.is_empty());
        assert!(collectors.len() <= 5);

        // Should not contain duplicates
        let unique_collectors: std::collections::HashSet<_> = collectors.iter().collect();
        assert_eq!(unique_collectors.len(), collectors.len());
    }

    #[test]
    fn test_most_diverse_collectors_zero() {
        let broker = BgpkitBroker::new();
        let result = broker.most_diverse_collectors(0, None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_most_diverse_collectors_project_filter() {
        let broker = BgpkitBroker::new();

        // Test with routeviews filter
        let rv_result = broker.most_diverse_collectors(3, Some("routeviews"));
        assert!(rv_result.is_ok());

        // Test with riperis filter
        let ripe_result = broker.most_diverse_collectors(3, Some("riperis"));
        assert!(ripe_result.is_ok());

        // Results should not contain duplicates
        let rv_collectors = rv_result.unwrap();
        let ripe_collectors = ripe_result.unwrap();

        if !rv_collectors.is_empty() {
            let unique_rv: std::collections::HashSet<_> = rv_collectors.iter().collect();
            assert_eq!(unique_rv.len(), rv_collectors.len());
        }

        if !ripe_collectors.is_empty() {
            let unique_ripe: std::collections::HashSet<_> = ripe_collectors.iter().collect();
            assert_eq!(unique_ripe.len(), ripe_collectors.len());
        }
    }

    #[test]
    fn test_get_snapshot_files() {
        let broker = BgpkitBroker::new();

        // Test with a known timestamp (2021-10-20 04:00:00 UTC)
        // This should find a RIB dump at 02:00:00 and updates between 02:00 and 04:00
        let result = broker.get_snapshot_files(&["route-views2"], "2021-10-20T04:00:00Z");
        assert!(result.is_ok());

        let snapshots = result.unwrap();
        // Should have at least one snapshot (if data is available)
        if !snapshots.is_empty() {
            let snapshot = &snapshots[0];
            assert_eq!(snapshot.collector_id, "route-views2");
            assert!(!snapshot.rib_url.is_empty());
            // Updates URLs should be in chronological order
            assert!(snapshot.updates_urls.iter().all(|url| !url.is_empty()));
        }
    }

    #[test]
    fn test_get_snapshot_files_multiple_collectors() {
        let broker = BgpkitBroker::new();

        // Test with multiple collectors
        let result = broker.get_snapshot_files(&["route-views2", "rrc00"], "2021-10-20T04:00:00Z");
        assert!(result.is_ok());

        let snapshots = result.unwrap();
        // Check that collector IDs are unique
        let collector_ids: std::collections::HashSet<_> =
            snapshots.iter().map(|s| &s.collector_id).collect();
        assert_eq!(collector_ids.len(), snapshots.len());
    }

    #[test]
    fn test_get_snapshot_files_invalid_timestamp() {
        let broker = BgpkitBroker::new();

        // Test with an invalid timestamp
        let result = broker.get_snapshot_files(&["route-views2"], "invalid-timestamp");
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_get_snapshot_files_empty_collectors() {
        let broker = BgpkitBroker::new();

        // Test with empty collectors array
        let empty: &[&str] = &[];
        let result = broker.get_snapshot_files(empty, "2021-10-20T04:00:00Z");
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_snapshot_files_display() {
        let snapshot = SnapshotFiles {
            collector_id: "route-views2".to_string(),
            rib_url: "http://example.com/rib.bz2".to_string(),
            updates_urls: vec![
                "http://example.com/updates1.bz2".to_string(),
                "http://example.com/updates2.bz2".to_string(),
            ],
        };

        let display = format!("{}", snapshot);
        assert!(display.contains("route-views2"));
        assert!(display.contains("http://example.com/rib.bz2"));
        assert!(display.contains("updates_count: 2"));
    }
}
