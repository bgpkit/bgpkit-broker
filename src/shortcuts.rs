//! Convenience methods for common BGP data queries.
//!
//! This module provides shortcuts for frequently used query patterns to make it easier
//! to find specific types of BGP data without manually configuring filters.

use crate::{BgpkitBroker, BrokerError, BrokerItem};
use chrono::{Timelike, Utc};
use std::collections::HashMap;

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
    pub fn most_diverse_collectors(&self, n: usize, project: Option<&str>) -> Result<Vec<String>, BrokerError> {
        // Get all full-feed peers, optionally filtered by project
        let mut full_feed_broker = self.clone().peers_only_full_feed(true);
        if let Some(proj) = project {
            full_feed_broker = full_feed_broker.project(proj)?;
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
                .max_by_key(|(_, asns)| {
                    asns.difference(&covered_asns).count()
                })
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
        let broker = BgpkitBroker::new();
        let result = broker.recent_updates(24);
        assert!(result.is_ok());

        let updates = result.unwrap();
        // All returned items should be updates
        for item in &updates {
            assert!(!item.is_rib());
        }
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
}
