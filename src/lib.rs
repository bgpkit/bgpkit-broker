/*!
# Overview

[bgpkit-broker][crate] is a package that allows accessing the BGPKIT Broker API and search for BGP archive
files with different search parameters available.

# Examples

## Basic Usage with Iterator

The recommended usage to collect [BrokerItem]s is to use the built-in iterator. The
[BrokerItemIterator] handles making API queries so that it can continuously stream new items until
it reaches the end of items. This is useful for simply getting **all** matching items without need
to worry about pagination.

```no_run
use bgpkit_broker::{BgpkitBroker, BrokerItem};

let broker = BgpkitBroker::new()
    .ts_start("2022-01-01")
    .ts_end("2022-01-02")
    .collector_id("route-views2");

// Iterate by reference (reusable broker)
for item in &broker {
    println!("BGP file: {} from {} ({})",
             item.url, item.collector_id, item.data_type);
}

// Or collect into vector
let items: Vec<BrokerItem> = broker.into_iter().collect();
println!("Found {} BGP archive files", items.len());
```

## Practical BGP Data Analysis with Shortcuts

The SDK provides convenient shortcuts for common BGP data analysis patterns:

### Daily RIB Analysis Across Diverse Collectors

```no_run
use bgpkit_broker::BgpkitBroker;

// Find the most diverse collectors for comprehensive analysis
let broker = BgpkitBroker::new()
    .ts_start("2024-01-01")
    .ts_end("2024-01-31");

let diverse_collectors = broker.most_diverse_collectors(5, None).unwrap();
println!("Selected {} diverse collectors: {:?}",
         diverse_collectors.len(), diverse_collectors);

// Get daily RIB snapshots from these collectors
let daily_ribs = broker
    .clone()
    .collector_id(&diverse_collectors.join(","))
    .daily_ribs().unwrap();

println!("Found {} daily RIB snapshots for analysis", daily_ribs.len());
for rib in daily_ribs.iter().take(3) {
    println!("Daily snapshot: {} from {} at {}",
             rib.collector_id,
             rib.ts_start.format("%Y-%m-%d"),
             rib.url);
}
```

### Recent BGP Updates Monitoring

```no_run
use bgpkit_broker::BgpkitBroker;

// Monitor recent BGP updates from multiple collectors
let recent_updates = BgpkitBroker::new()
    .collector_id("route-views2,rrc00,route-views6")
    .recent_updates(6).unwrap(); // last 6 hours

println!("Found {} recent BGP update files", recent_updates.len());
for update in recent_updates.iter().take(5) {
    println!("Update: {} from {} at {}",
             update.collector_id,
             update.ts_start.format("%Y-%m-%d %H:%M:%S"),
             update.url);
}
```

### Project-specific Analysis

```no_run
use bgpkit_broker::BgpkitBroker;

// Compare RouteViews vs RIPE RIS daily snapshots
let routeviews_ribs = BgpkitBroker::new()
    .ts_start("2024-01-01")
    .ts_end("2024-01-07")
    .project("routeviews")
    .daily_ribs().unwrap();

let ripe_ribs = BgpkitBroker::new()
    .ts_start("2024-01-01")
    .ts_end("2024-01-07")
    .project("riperis")
    .daily_ribs().unwrap();

println!("RouteViews daily RIBs: {}", routeviews_ribs.len());
println!("RIPE RIS daily RIBs: {}", ripe_ribs.len());
```

### Advanced Collector Selection

```no_run
use bgpkit_broker::BgpkitBroker;

let broker = BgpkitBroker::new();

// Get diverse RouteViews collectors for focused analysis
let rv_collectors = broker.most_diverse_collectors(3, Some("routeviews")).unwrap();
println!("Diverse RouteViews collectors: {:?}", rv_collectors);

// Use them to get comprehensive recent updates
let comprehensive_updates = broker
    .clone()
    .collector_id(&rv_collectors.join(","))
    .recent_updates(12).unwrap(); // last 12 hours

println!("Got {} updates from {} collectors",
         comprehensive_updates.len(), rv_collectors.len());
```

## Manual Page Queries

For fine-grained control over pagination or custom iteration patterns:

```rust,no_run
use bgpkit_broker::BgpkitBroker;

let mut broker = BgpkitBroker::new()
    .ts_start("2022-01-01")
    .ts_end("2022-01-02")
    .page(1)
    .page_size(50);

// Query specific page
let page1_items = broker.query_single_page().unwrap();
println!("Page 1: {} items", page1_items.len());

// Move to next page
broker.turn_page(2);
let page2_items = broker.query_single_page().unwrap();
println!("Page 2: {} items", page2_items.len());
```

## Getting Latest Files and Peer Information

Access the most recent data and peer information:

```rust,no_run
use bgpkit_broker::BgpkitBroker;

// Get latest files from all collectors
let broker = BgpkitBroker::new();
let latest_files = broker.latest().unwrap();
println!("Latest files from {} collectors", latest_files.len());

// Get full-feed peers from specific collector
let peers = BgpkitBroker::new()
    .collector_id("route-views2")
    .peers_only_full_feed(true)
    .get_peers().unwrap();

println!("Found {} full-feed peers", peers.len());
for peer in peers.iter().take(3) {
    println!("Peer: AS{} ({}) - v4: {}, v6: {}",
             peer.asn, peer.ip, peer.num_v4_pfxs, peer.num_v6_pfxs);
}
```
*/

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/bgpkit/assets/main/logos/icon-transparent.png",
    html_favicon_url = "https://raw.githubusercontent.com/bgpkit/assets/main/logos/favicon.ico"
)]
#![allow(unknown_lints)]

mod collector;
#[cfg(feature = "cli")]
mod crawler;
#[cfg(feature = "backend")]
pub mod db;
mod error;
mod item;
#[cfg(feature = "nats")]
pub mod notifier;
mod peer;
mod query;
mod shortcuts;

use crate::collector::DEFAULT_COLLECTORS_CONFIG;
use crate::peer::BrokerPeersResult;
use crate::query::{BrokerQueryResult, CollectorLatestResult};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
pub use collector::{load_collectors, Collector};

#[cfg(feature = "cli")]
pub use crawler::crawl_collector;
#[cfg(feature = "backend")]
pub use db::{LocalBrokerDb, UpdatesMeta, DEFAULT_PAGE_SIZE};
pub use error::BrokerError;
pub use item::BrokerItem;
pub use peer::BrokerPeer;
pub use query::{QueryParams, SortOrder};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::net::IpAddr;

/// BgpkitBroker struct maintains the broker's URL and handles making API queries.
///
/// See [module doc][crate#examples] for usage examples.
#[derive(Clone)]
pub struct BgpkitBroker {
    pub broker_url: String,
    pub query_params: QueryParams,
    client: reqwest::blocking::Client,
    collector_project_map: HashMap<String, String>,
}

impl Default for BgpkitBroker {
    fn default() -> Self {
        dotenvy::dotenv().ok();
        let url = match std::env::var("BGPKIT_BROKER_URL") {
            Ok(url) => url.trim_end_matches('/').to_string(),
            Err(_) => "https://api.bgpkit.com/v3/broker".to_string(),
        };

        let collector_project_map = DEFAULT_COLLECTORS_CONFIG.clone().to_project_map();

        let accept_invalid_certs = match std::env::var("ONEIO_ACCEPT_INVALID_CERTS") {
            Ok(t) => {
                let l = t.to_lowercase();
                l.starts_with("true") || l.starts_with("y")
            }
            Err(_) => false,
        };

        let client = match reqwest::blocking::ClientBuilder::new()
            .danger_accept_invalid_certs(accept_invalid_certs)
            .user_agent(concat!("bgpkit-broker/", env!("CARGO_PKG_VERSION")))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                panic!("Failed to build HTTP client for broker requests: {}", e);
            }
        };

        Self {
            broker_url: url,
            query_params: Default::default(),
            client,
            collector_project_map,
        }
    }
}

impl BgpkitBroker {
    /// Construct a new BgpkitBroker object.
    ///
    /// The URL and query parameters can be adjusted with other functions.
    ///
    /// Users can opt in to accept invalid SSL certificates by setting the environment variable
    /// `ONEIO_ACCEPT_INVALID_CERTS` to `true`.
    ///
    /// # Examples
    /// ```
    /// use bgpkit_broker::BgpkitBroker;
    /// let broker = BgpkitBroker::new();
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure broker URL.
    ///
    /// You can change the default broker URL to point to your own broker instance.
    /// You can also change the URL by setting the environment variable `BGPKIT_BROKER_URL`.
    ///
    /// # Examples
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .broker_url("api.broker.example.com/v3");
    /// ```
    pub fn broker_url<S: Display>(self, url: S) -> Self {
        let broker_url = url.to_string().trim_end_matches('/').to_string();
        Self {
            broker_url,
            query_params: self.query_params,
            client: self.client,
            collector_project_map: self.collector_project_map,
        }
    }

    /// DANGER: Accept invalid SSL certificates.
    pub fn accept_invalid_certs(self) -> Self {
        #[allow(clippy::unwrap_used)]
        Self {
            broker_url: self.broker_url,
            query_params: self.query_params,
            client: reqwest::blocking::ClientBuilder::new()
                .danger_accept_invalid_certs(true)
                .build()
                .unwrap(),
            collector_project_map: self.collector_project_map,
        }
    }

    /// Disable SSL certificate check.
    #[deprecated(since = "0.7.1", note = "Please use `accept_invalid_certs` instead.")]
    pub fn disable_ssl_check(self) -> Self {
        Self::accept_invalid_certs(self)
    }

    /// Parse and validate timestamp string with support for multiple formats.
    ///
    /// Supported formats:
    /// - Unix timestamp: "1640995200"
    /// - RFC3339/ISO8601: "2022-01-01T00:00:00Z", "2022-01-01T12:30:45Z"
    /// - RFC3339 without Z: "2022-01-01T00:00:00", "2022-01-01T12:30:45"
    /// - Date with time: "2022-01-01 00:00:00", "2022-01-01 12:30:45"
    /// - Pure date (start of day): "2022-01-01", "2022/01/01"
    /// - Pure date with dots: "2022.01.01"
    /// - Compact date: "20220101"
    ///
    /// For pure date formats, the time component defaults to 00:00:00 (start of day).
    /// Returns a `DateTime<Utc>` for consistent handling and formatting.
    fn parse_timestamp(timestamp: &str) -> Result<DateTime<Utc>, BrokerError> {
        let ts_str = timestamp.trim();

        // Try parsing as RFC3339 with timezone (including +00:00, -05:00, Z, etc.)
        if let Ok(dt_with_tz) = DateTime::parse_from_rfc3339(ts_str) {
            return Ok(dt_with_tz.with_timezone(&Utc));
        }

        // Try parsing as RFC3339/ISO8601 with Z
        if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(ts_str, "%Y-%m-%dT%H:%M:%SZ") {
            return Ok(Utc.from_utc_datetime(&naive_dt));
        }

        // Try parsing as RFC3339 without Z (assume UTC)
        if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(ts_str, "%Y-%m-%dT%H:%M:%S") {
            return Ok(Utc.from_utc_datetime(&naive_dt));
        }

        // Try parsing as "YYYY-MM-DD HH:MM:SS" (assume UTC)
        if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(ts_str, "%Y-%m-%d %H:%M:%S") {
            return Ok(Utc.from_utc_datetime(&naive_dt));
        }

        // Try parsing pure date formats and convert to start of day
        let date_formats = [
            "%Y-%m-%d", // 2022-01-01
            "%Y/%m/%d", // 2022/01/01
            "%Y.%m.%d", // 2022.01.01
            "%Y%m%d",   // 20220101 - must be exactly 8 digits
        ];

        for format in &date_formats {
            if let Ok(date) = NaiveDate::parse_from_str(ts_str, format) {
                // Additional validation for compact format to ensure it's actually a date
                if format == &"%Y%m%d" && ts_str.len() != 8 {
                    continue;
                }
                // Convert to start of day in UTC
                if let Some(naive_datetime) = date.and_hms_opt(0, 0, 0) {
                    return Ok(Utc.from_utc_datetime(&naive_datetime));
                }
            }
        }

        // Finally, try parsing as Unix timestamp (only if it's reasonable length and all digits)
        if ts_str.len() >= 9 && ts_str.len() <= 13 && ts_str.chars().all(|c| c.is_ascii_digit()) {
            if let Ok(timestamp) = ts_str.parse::<i64>() {
                if let Some(dt) = Utc.timestamp_opt(timestamp, 0).single() {
                    return Ok(dt);
                }
            }
        }

        Err(BrokerError::ConfigurationError(format!(
            "Invalid timestamp format '{ts_str}'. Supported formats:\n\
                - Unix timestamp: '1640995200'\n\
                - RFC3339 with timezone: '2022-01-01T00:00:00+00:00', '2022-01-01T00:00:00Z', '2022-01-01T05:00:00-05:00'\n\
                - RFC3339 without timezone: '2022-01-01T00:00:00' (assumes UTC)\n\
                - Date with time: '2022-01-01 00:00:00'\n\
                - Pure date: '2022-01-01', '2022/01/01', '2022.01.01', '20220101'"
        )))
    }

    /// Validate all configuration parameters before making API calls.
    ///
    /// This performs the same validation that was previously done at configuration time,
    /// but now happens just before queries are executed. Returns normalized query parameters.
    fn validate_configuration(&self) -> Result<QueryParams, BrokerError> {
        // Validate timestamps and normalize them
        let mut normalized_params = self.query_params.clone();

        if let Some(ts) = &self.query_params.ts_start {
            let parsed_datetime = Self::parse_timestamp(ts)?;
            normalized_params.ts_start =
                Some(parsed_datetime.format("%Y-%m-%dT%H:%M:%SZ").to_string());
        }

        if let Some(ts) = &self.query_params.ts_end {
            let parsed_datetime = Self::parse_timestamp(ts)?;
            normalized_params.ts_end =
                Some(parsed_datetime.format("%Y-%m-%dT%H:%M:%SZ").to_string());
        }

        // Permissive collector validation: normalize only, no network I/O
        if let Some(collector_str) = &self.query_params.collector_id {
            let collectors: Vec<String> = collector_str
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();

            if collectors.is_empty() {
                return Err(BrokerError::ConfigurationError(
                    "Collector ID cannot be empty".to_string(),
                ));
            }

            // Deduplicate while preserving order
            let mut seen = HashSet::new();
            let mut deduped = Vec::with_capacity(collectors.len());
            for c in collectors {
                if seen.insert(c.clone()) {
                    deduped.push(c);
                }
            }

            normalized_params.collector_id = Some(deduped.join(","));
        }

        // Validate project
        if let Some(project_str) = &self.query_params.project {
            let project_lower = project_str.to_lowercase();
            match project_lower.as_str() {
                "rrc" | "riperis" | "ripe_ris" | "routeviews" | "route_views" | "rv" => {
                    // Valid project
                }
                _ => {
                    return Err(BrokerError::ConfigurationError(format!(
                        "Invalid project '{project_str}'. Valid projects are: 'riperis' (aliases: 'rrc', 'ripe_ris') or 'routeviews' (aliases: 'route_views', 'rv')"
                    )));
                }
            }
        }

        // Validate data type
        if let Some(data_type_str) = &self.query_params.data_type {
            let data_type_lower = data_type_str.to_lowercase();
            match data_type_lower.as_str() {
                "rib" | "ribs" | "r" | "update" | "updates" => {
                    // Valid data type
                }
                _ => {
                    return Err(BrokerError::ConfigurationError(format!(
                        "Invalid data type '{data_type_str}'. Valid data types are: 'rib' (aliases: 'ribs', 'r') or 'updates' (alias: 'update')"
                    )));
                }
            }
        }

        // Validate page number
        if self.query_params.page < 1 {
            return Err(BrokerError::ConfigurationError(format!(
                "Invalid page number {}. Page number must be >= 1",
                self.query_params.page
            )));
        }

        // Validate page size
        if !(1..=100000).contains(&self.query_params.page_size) {
            return Err(BrokerError::ConfigurationError(format!(
                "Invalid page size {}. Page size must be between 1 and 100000",
                self.query_params.page_size
            )));
        }

        Ok(normalized_params)
    }

    /// Add a filter of starting timestamp.
    ///
    /// Supports multiple timestamp formats including Unix timestamps, RFC3339 dates, and pure dates.
    /// Validation occurs at query time.
    ///
    /// # Examples
    ///
    /// Specify a Unix timestamp:
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_start("1640995200");
    /// ```
    ///
    /// Specify a RFC3339-formatted time string:
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_start("2022-01-01T00:00:00Z");
    /// ```
    ///
    /// Specify a pure date (defaults to start of day):
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_start("2022-01-01");
    /// ```
    ///
    /// Other supported formats:
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_start("2022/01/01")  // slash format
    ///     .ts_start("2022.01.01")  // dot format
    ///     .ts_start("20220101");   // compact format
    /// ```
    pub fn ts_start<S: Display>(self, ts_start: S) -> Self {
        let mut query_params = self.query_params;
        query_params.ts_start = Some(ts_start.to_string());
        Self {
            broker_url: self.broker_url,
            query_params,
            client: self.client,
            collector_project_map: self.collector_project_map,
        }
    }

    /// Add a filter of ending timestamp.
    ///
    /// Supports the same multiple timestamp formats as `ts_start`.
    /// Validation occurs at query time.
    ///
    /// # Examples
    ///
    /// Specify a Unix timestamp:
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_end("1640995200");
    /// ```
    ///
    /// Specify a RFC3339-formatted time string:
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_end("2022-01-01T00:00:00Z");
    /// ```
    ///
    /// Specify a pure date (defaults to start of day):
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_end("2022-01-01");
    /// ```
    pub fn ts_end<S: Display>(self, ts_end: S) -> Self {
        let mut query_params = self.query_params;
        query_params.ts_end = Some(ts_end.to_string());
        Self {
            broker_url: self.broker_url,
            client: self.client,
            query_params,
            collector_project_map: self.collector_project_map,
        }
    }

    /// Add a filter of collector ID (e.g. `rrc00` or `route-views2`).
    ///
    /// See the full list of collectors [here](https://github.com/bgpkit/bgpkit-broker-backend/blob/main/deployment/full-config.json).
    /// Validation occurs at query time.
    ///
    /// # Examples
    ///
    /// filter by single collector
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .collector_id("rrc00");
    /// ```
    ///
    /// filter by multiple collector
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .collector_id("route-views2,route-views6");
    /// ```
    pub fn collector_id<S: Display>(self, collector_id: S) -> Self {
        let mut query_params = self.query_params;
        query_params.collector_id = Some(collector_id.to_string());
        Self {
            client: self.client,
            broker_url: self.broker_url,
            query_params,
            collector_project_map: self.collector_project_map,
        }
    }

    /// Add a filter of project name with validation, i.e. `riperis` or `routeviews`.
    ///
    /// # Examples
    ///
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .project("riperis");
    /// ```
    ///
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .project("routeviews");
    /// ```
    pub fn project<S: Display>(self, project: S) -> Self {
        let mut query_params = self.query_params;
        query_params.project = Some(project.to_string());
        Self {
            client: self.client,
            broker_url: self.broker_url,
            query_params,
            collector_project_map: self.collector_project_map,
        }
    }

    /// Add filter of data type, i.e. `rib` or `updates`.
    ///
    /// Validation occurs at query time.
    ///
    /// # Examples
    ///
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .data_type("rib");
    /// ```
    ///
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .data_type("updates");
    /// ```
    pub fn data_type<S: Display>(self, data_type: S) -> Self {
        let mut query_params = self.query_params;
        query_params.data_type = Some(data_type.to_string());
        Self {
            broker_url: self.broker_url,
            client: self.client,
            query_params,
            collector_project_map: self.collector_project_map,
        }
    }

    /// Change the current page number, starting from 1.
    ///
    /// Validation occurs at query time.
    ///
    /// # Examples
    ///
    /// Start iterating with page 2.
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .page(2);
    /// ```
    pub fn page(self, page: i64) -> Self {
        let mut query_params = self.query_params;
        query_params.page = page;
        Self {
            broker_url: self.broker_url,
            client: self.client,
            query_params,
            collector_project_map: self.collector_project_map,
        }
    }

    /// Change current page size, default 100.
    ///
    /// Validation occurs at query time.
    ///
    /// # Examples
    ///
    /// Set page size to 20.
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .page_size(10);
    /// ```
    pub fn page_size(self, page_size: i64) -> Self {
        let mut query_params = self.query_params;
        query_params.page_size = page_size;
        Self {
            broker_url: self.broker_url,
            client: self.client,
            query_params,
            collector_project_map: self.collector_project_map,
        }
    }

    /// Add a filter of peer IP address when listing peers.
    ///
    /// # Examples
    ///
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///    .peers_ip("192.168.1.1".parse().unwrap());
    /// ```
    pub fn peers_ip(self, peer_ip: IpAddr) -> Self {
        let mut query_params = self.query_params;
        query_params.peers_ip = Some(peer_ip);
        Self {
            broker_url: self.broker_url,
            client: self.client,
            query_params,
            collector_project_map: self.collector_project_map,
        }
    }

    /// Add a filter of peer ASN when listing peers.
    ///
    /// # Examples
    ///
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///    .peers_asn(64496);
    /// ```
    pub fn peers_asn(self, peer_asn: u32) -> Self {
        let mut query_params = self.query_params;
        query_params.peers_asn = Some(peer_asn);
        Self {
            broker_url: self.broker_url,
            client: self.client,
            query_params,
            collector_project_map: self.collector_project_map,
        }
    }

    /// Add a filter of peer full feed status when listing peers.
    ///
    /// # Examples
    ///
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///   .peers_only_full_feed(true);
    /// ```
    pub fn peers_only_full_feed(self, peer_full_feed: bool) -> Self {
        let mut query_params = self.query_params;
        query_params.peers_only_full_feed = peer_full_feed;
        Self {
            broker_url: self.broker_url,
            client: self.client,
            query_params,
            collector_project_map: self.collector_project_map,
        }
    }

    /// Turn to specified page, page starting from 1.
    ///
    /// This works with [Self::query_single_page] function to manually paginate.
    ///
    /// # Examples
    ///
    /// Manually get the first two pages of items.
    /// ```no_run
    /// let mut broker = bgpkit_broker::BgpkitBroker::new();
    /// let mut items = vec![];
    /// items.extend(broker.query_single_page().unwrap());
    /// broker.turn_page(2);
    /// items.extend(broker.query_single_page().unwrap());
    /// ```
    pub fn turn_page(&mut self, page: i64) {
        self.query_params.page = page;
    }

    /// Send API for a single page of items.
    ///
    /// # Examples
    ///
    /// Manually get the first page of items.
    /// ```no_run
    /// let broker = bgpkit_broker::BgpkitBroker::new();
    /// let items = broker.query_single_page().unwrap();
    /// ```
    pub fn query_single_page(&self) -> Result<Vec<BrokerItem>, BrokerError> {
        let validated_params = self.validate_configuration()?;
        let url = format!("{}/search{}", &self.broker_url, &validated_params);
        log::info!("sending broker query to {}", &url);
        match self.run_files_query(url.as_str()) {
            Ok(res) => Ok(res.data),
            Err(e) => Err(e),
        }
    }

    /// Query the total count of items matching the current search criteria without fetching the items.
    ///
    /// This method is useful when you need to know how many items match your search criteria
    /// without downloading all the items. It performs the same validation as a regular query
    /// but only returns the count.
    ///
    /// # Returns
    /// - `Ok(i64)`: The total number of matching items
    /// - `Err(BrokerError)`: If the query fails or the count is missing from the response
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use bgpkit_broker::BgpkitBroker;
    ///
    /// let broker = BgpkitBroker::new()
    ///     .ts_start("2024-01-01")
    ///     .ts_end("2024-01-02")
    ///     .collector_id("route-views2");
    ///
    /// let count = broker.query_total_count().unwrap();
    /// println!("Found {} matching items", count);
    /// ```
    pub fn query_total_count(&self) -> Result<i64, BrokerError> {
        let validated_params = self.validate_configuration()?;
        let url = format!("{}/search{}", &self.broker_url, &validated_params);
        match self.run_files_query(url.as_str()) {
            Ok(res) => res.total.ok_or(BrokerError::BrokerError(
                "count not found in response".to_string(),
            )),
            Err(e) => Err(e),
        }
    }

    /// Check if the broker instance is healthy.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let broker = bgpkit_broker::BgpkitBroker::new();
    /// assert!(broker.health_check().is_ok())
    /// ```
    pub fn health_check(&self) -> Result<(), BrokerError> {
        let url = format!("{}/health", &self.broker_url.trim_end_matches('/'));
        match self.client.get(url.as_str()).send() {
            Ok(response) => {
                if response.status() == reqwest::StatusCode::OK {
                    Ok(())
                } else {
                    Err(BrokerError::BrokerError(format!(
                        "endpoint unhealthy {}",
                        self.broker_url
                    )))
                }
            }
            Err(_e) => Err(BrokerError::BrokerError(format!(
                "endpoint unhealthy {}",
                self.broker_url
            ))),
        }
    }

    /// Send a query to get **all** data times returned.
    ///
    /// This usually is what one needs.
    ///
    /// # Examples
    ///
    /// Get all RIB files on 2022-01-01 from route-views2.
    /// ```no_run
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_start("2022-01-01T00:00:00Z")
    ///     .ts_end("2022-01-01T23:59:00Z")
    ///     .data_type("rib")
    ///     .collector_id("route-views2");
    /// let items = broker.query().unwrap();
    ///
    /// // 1 RIB dump very 2 hours, total of 12 files for 1 day
    /// assert_eq!(items.len(), 12);
    /// ```
    pub fn query(&self) -> Result<Vec<BrokerItem>, BrokerError> {
        let mut p = self.validate_configuration()?;

        let mut items = vec![];
        loop {
            let url = format!("{}/search{}", &self.broker_url, &p);

            let res_items = self.run_files_query(url.as_str())?.data;

            let items_count = res_items.len() as i64;

            if items_count == 0 {
                // reaches the end
                break;
            }

            items.extend(res_items);
            let cur_page = p.page;
            p = p.page(cur_page + 1);

            if items_count < p.page_size {
                // reaches the end
                break;
            }
        }
        Ok(items)
    }

    /// Send a query to get the **latest** data for each collector.
    ///
    /// The returning result is structured as a vector of [CollectorLatestItem] objects.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let broker = bgpkit_broker::BgpkitBroker::new();
    /// let latest_items = broker.latest().unwrap();
    /// for item in &latest_items {
    ///     println!("{}", item);
    /// }
    /// ```
    pub fn latest(&self) -> Result<Vec<BrokerItem>, BrokerError> {
        let latest_query_url = format!("{}/latest", self.broker_url);
        let mut items = match self.client.get(latest_query_url.as_str()).send() {
            Ok(response) => match response.json::<CollectorLatestResult>() {
                Ok(result) => result.data,
                Err(_) => {
                    return Err(BrokerError::BrokerError(
                        "Error parsing response".to_string(),
                    ));
                }
            },
            Err(e) => {
                return Err(BrokerError::BrokerError(format!(
                    "Unable to connect to the URL ({latest_query_url}): {e}"
                )));
            }
        };

        items.retain(|item| {
            let mut matches = true;
            if let Some(project) = &self.query_params.project {
                match project.to_lowercase().as_str() {
                    "rrc" | "riperis" | "ripe_ris" => {
                        matches = self
                            .collector_project_map
                            .get(&item.collector_id)
                            .cloned()
                            .unwrap_or_default()
                            .as_str()
                            == "riperis";
                    }
                    "routeviews" | "route_views" | "rv" => {
                        matches = self
                            .collector_project_map
                            .get(&item.collector_id)
                            .cloned()
                            .unwrap_or_default()
                            .as_str()
                            == "routeviews";
                    }
                    _ => {}
                }
            }

            if let Some(data_type) = &self.query_params.data_type {
                match data_type.to_lowercase().as_str() {
                    "rib" | "ribs" | "r" => {
                        if !item.is_rib() {
                            // if not RIB file, not match
                            matches = false
                        }
                    }
                    "update" | "updates" => {
                        if item.is_rib() {
                            // if is RIB file, not match
                            matches = false
                        }
                    }
                    _ => {}
                }
            }

            if let Some(collector_id) = &self.query_params.collector_id {
                let wanted: HashSet<&str> = collector_id
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .collect();

                if !wanted.contains(item.collector_id.as_str()) {
                    return false;
                }
            }

            matches
        });

        Ok(items)
    }

    /// Get the most recent information for collector peers.
    ///
    /// The returning result is structured as a vector of [BrokerPeer] objects.
    ///
    /// # Examples
    ///
    /// ## Get all peers
    ///
    /// ```no_run
    /// let broker = bgpkit_broker::BgpkitBroker::new();
    /// let peers = broker.get_peers().unwrap();
    /// for peer in &peers {
    ///     println!("{:?}", peer);
    /// }
    /// ```
    ///
    /// ## Get peers from a specific collector
    ///
    /// ```no_run
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///    .collector_id("route-views2");
    /// let peers = broker.get_peers().unwrap();
    /// for peer in &peers {
    ///    println!("{:?}", peer);
    /// }
    /// ```
    ///
    /// ## Get peers from a specific ASN
    ///
    /// ```no_run
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///   .peers_asn(64496);
    /// let peers = broker.get_peers().unwrap();
    /// for peer in &peers {
    ///    println!("{:?}", peer);
    /// }
    /// ```
    ///
    /// ## Get peers from a specific IP address
    ///
    /// ```no_run
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///   .peers_ip("192.168.1.1".parse().unwrap());
    /// let peers = broker.get_peers().unwrap();
    /// for peer in &peers {
    ///   println!("{:?}", peer);
    /// }
    /// ```
    ///
    /// ## Get peers with full feed
    ///
    /// ```no_run
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///  .peers_only_full_feed(true);
    /// let peers = broker.get_peers().unwrap();
    /// for peer in &peers {
    ///     println!("{:?}", peer);
    /// }
    /// ```
    ///
    /// ## Get peers from a specific collector with full feed
    ///
    /// ```no_run
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///  .collector_id("route-views2")
    /// .peers_only_full_feed(true);
    /// let peers = broker.get_peers().unwrap();
    /// for peer in &peers {
    ///    println!("{:?}", peer);
    /// }
    /// ```
    pub fn get_peers(&self) -> Result<Vec<BrokerPeer>, BrokerError> {
        let mut url = format!("{}/peers", self.broker_url);
        let mut param_strings = vec![];
        if let Some(ip) = &self.query_params.peers_ip {
            param_strings.push(format!("ip={ip}"));
        }
        if let Some(asn) = &self.query_params.peers_asn {
            param_strings.push(format!("asn={asn}"));
        }
        if self.query_params.peers_only_full_feed {
            param_strings.push("full_feed=true".to_string());
        }
        if let Some(collector_id) = &self.query_params.collector_id {
            param_strings.push(format!("collector={collector_id}"));
        }
        if !param_strings.is_empty() {
            let param_string = param_strings.join("&");
            url = format!("{url}?{param_string}");
        }

        let peers = match self.client.get(url.as_str()).send() {
            Ok(response) => match response.json::<BrokerPeersResult>() {
                Ok(result) => result.data,
                Err(_) => {
                    return Err(BrokerError::BrokerError(
                        "Error parsing response".to_string(),
                    ));
                }
            },
            Err(e) => {
                return Err(BrokerError::BrokerError(format!(
                    "Unable to connect to the URL ({url}): {e}"
                )));
            }
        };
        Ok(peers)
    }

    fn run_files_query(&self, url: &str) -> Result<BrokerQueryResult, BrokerError> {
        log::info!("sending broker query to {}", &url);
        match self.client.get(url).send() {
            Ok(res) => match res.json::<BrokerQueryResult>() {
                Ok(res) => {
                    if let Some(e) = res.error {
                        Err(BrokerError::BrokerError(e))
                    } else {
                        Ok(res)
                    }
                }
                Err(e) => {
                    // json decoding error. most likely the service returns an error message without
                    // `data` field.
                    Err(BrokerError::BrokerError(e.to_string()))
                }
            },
            Err(e) => Err(BrokerError::from(e)),
        }
    }
}

/// Iterator for BGPKIT Broker that iterates through one [BrokerItem] at a time.
///
/// The [IntoIterator] trait is implemented for both the struct and the reference, so that you can
/// either iterate through items by taking the ownership of the broker, or use the reference to broker
/// to iterate.
///
/// ```no_run
/// use bgpkit_broker::{BgpkitBroker, BrokerItem};
///
/// let mut broker = BgpkitBroker::new()
///     .ts_start("1634693400")
///     .ts_end("1634693400")
///     .page_size(10)
///     .page(2);
///
/// // create iterator from reference (so that you can reuse the broker object)
/// // same as `&broker.into_intr()`
/// for item in &broker {
///     println!("{}", item);
/// }
///
/// // create iterator from the broker object (taking ownership)
/// let items = broker.into_iter().collect::<Vec<BrokerItem>>();
///
/// assert_eq!(items.len(), 43);
/// ```
pub struct BrokerItemIterator {
    broker: BgpkitBroker,
    cached_items: Vec<BrokerItem>,
    first_run: bool,
}

impl BrokerItemIterator {
    pub fn new(broker: BgpkitBroker) -> BrokerItemIterator {
        BrokerItemIterator {
            broker,
            cached_items: vec![],
            first_run: true,
        }
    }
}

impl Iterator for BrokerItemIterator {
    type Item = BrokerItem;

    fn next(&mut self) -> Option<Self::Item> {
        // if we have cached items, simply pop and return
        if let Some(item) = self.cached_items.pop() {
            return Some(item);
        }

        // no more cached items, refill cache by one more broker query
        if self.first_run {
            // if it's the first time running, do not change page, and switch the flag.
            self.first_run = false;
        } else {
            // if it's not the first time running, add page number by one.
            self.broker.query_params.page += 1;
        }

        // query the current page
        let items = match self.broker.query_single_page() {
            Ok(i) => i,
            Err(_) => return None,
        };

        if items.is_empty() {
            // break out the iteration
            return None;
        } else {
            // fill the cache
            self.cached_items = items;
            self.cached_items.reverse();
        }

        #[allow(clippy::unwrap_used)]
        Some(self.cached_items.pop().unwrap())
    }
}

impl IntoIterator for BgpkitBroker {
    type Item = BrokerItem;
    type IntoIter = BrokerItemIterator;

    fn into_iter(self) -> Self::IntoIter {
        BrokerItemIterator::new(self)
    }
}

impl IntoIterator for &BgpkitBroker {
    type Item = BrokerItem;
    type IntoIter = BrokerItemIterator;

    fn into_iter(self) -> Self::IntoIter {
        BrokerItemIterator::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query() {
        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .ts_end("1634693400");
        let res = broker.query();
        assert!(&res.is_ok());
        let data = res.unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_network_error() {
        let broker = BgpkitBroker::new().broker_url("https://api.broker.example.com/v2");
        let res = broker.query();
        // when testing a must-fail query, you could use `matches!` macro to do so
        assert!(res.is_err());
        assert!(matches!(res.err(), Some(BrokerError::NetworkError(_))));
    }

    #[test]
    fn test_broker_error() {
        let broker = BgpkitBroker::new().page(-1);
        let result = broker.query();
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_query_all() {
        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .ts_end("1634693400")
            .page_size(100);
        let res = broker.query();
        assert!(res.is_ok());
        assert!(res.ok().unwrap().len() >= 54);
    }

    #[test]
    fn test_iterator() {
        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .ts_end("1634693400");
        assert!(broker.into_iter().count() >= 54);
    }

    #[test]
    fn test_filters() {
        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .ts_end("1634693400");
        let items = broker.query().unwrap();
        assert!(items.len() >= 54);

        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .ts_end("1634693400")
            .collector_id("rrc00");
        let items = broker.query().unwrap();
        assert_eq!(items.len(), 1);

        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .ts_end("1634693400")
            .project("riperis");
        let items = broker.query().unwrap();
        assert_eq!(items.len(), 23);
    }

    #[test]
    fn test_latest() {
        let broker = BgpkitBroker::new();
        let items = broker.latest().unwrap();
        assert!(items.len() >= 125);

        let broker = BgpkitBroker::new().project("routeviews".to_string());
        let items = broker.latest().unwrap();
        assert!(!items.is_empty());
        assert!(items
            .iter()
            .all(|item| !item.collector_id.starts_with("rrc")));

        let broker = BgpkitBroker::new().project("riperis".to_string());
        let items = broker.latest().unwrap();
        assert!(!items.is_empty());
        assert!(items
            .iter()
            .all(|item| item.collector_id.starts_with("rrc")));

        let broker = BgpkitBroker::new().data_type("rib".to_string());
        let items = broker.latest().unwrap();
        assert!(!items.is_empty());
        assert!(items.iter().all(|item| item.is_rib()));

        let broker = BgpkitBroker::new().data_type("update".to_string());
        let items = broker.latest().unwrap();
        assert!(!items.is_empty());
        assert!(items.iter().all(|item| !item.is_rib()));

        let broker = BgpkitBroker::new().collector_id("rrc00".to_string());
        let items = broker.latest().unwrap();
        assert!(!items.is_empty());
        assert!(items
            .iter()
            .all(|item| item.collector_id.as_str() == "rrc00"));
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn test_latest_no_ssl() {
        let broker = BgpkitBroker::new().accept_invalid_certs();
        let items = broker.latest().unwrap();
        assert!(items.len() >= 125);
    }

    #[test]
    fn test_health_check() {
        let broker = BgpkitBroker::new();
        let res = broker.health_check();
        assert!(res.is_ok());
    }

    #[test]
    fn test_peers() {
        let broker = BgpkitBroker::new();
        let all_peers = broker.get_peers().unwrap();
        assert!(!all_peers.is_empty());
        let first_peer = all_peers.first().unwrap();
        let first_ip = first_peer.ip;
        let first_asn = first_peer.asn;

        let broker = BgpkitBroker::new().peers_ip(first_ip);
        let peers = broker.get_peers().unwrap();
        assert!(!peers.is_empty());

        let broker = BgpkitBroker::new().peers_asn(first_asn);
        let peers = broker.get_peers().unwrap();
        assert!(!peers.is_empty());

        let broker = BgpkitBroker::new().peers_only_full_feed(true);
        let full_feed_peers = broker.get_peers().unwrap();
        assert!(!full_feed_peers.is_empty());
        assert!(full_feed_peers.len() < all_peers.len());

        let broker = BgpkitBroker::new().collector_id("rrc00");
        let rrc_peers = broker.get_peers().unwrap();
        assert!(!rrc_peers.is_empty());
        assert!(rrc_peers.iter().all(|peer| peer.collector == "rrc00"));

        let broker = BgpkitBroker::new().collector_id("rrc00,route-views2");
        let rrc_rv_peers = broker.get_peers().unwrap();
        assert!(!rrc_rv_peers.is_empty());
        assert!(rrc_rv_peers
            .iter()
            .any(|peer| peer.collector == "rrc00" || peer.collector == "route-views2"));

        assert!(rrc_rv_peers.len() > rrc_peers.len());
    }

    #[test]
    fn test_timestamp_parsing_unix() {
        let broker = BgpkitBroker::new();

        // Valid Unix timestamps - configuration succeeds, normalization happens at query time
        let result = broker.clone().ts_start("1640995200");
        // Raw input is stored during configuration
        assert_eq!(result.query_params.ts_start, Some("1640995200".to_string()));

        let result = broker.clone().ts_end("1640995200");
        assert_eq!(result.query_params.ts_end, Some("1640995200".to_string()));
    }

    #[test]
    fn test_timestamp_parsing_rfc3339() {
        let broker = BgpkitBroker::new();

        // RFC3339 with Z - raw input stored during configuration
        let result = broker.clone().ts_start("2022-01-01T00:00:00Z");
        assert_eq!(
            result.query_params.ts_start,
            Some("2022-01-01T00:00:00Z".to_string())
        );

        // RFC3339 without Z - raw input stored during configuration
        let result = broker.clone().ts_start("2022-01-01T12:30:45");
        assert_eq!(
            result.query_params.ts_start,
            Some("2022-01-01T12:30:45".to_string())
        );

        // Date with time format - raw input stored during configuration
        let result = broker.clone().ts_end("2022-01-01 12:30:45");
        assert_eq!(
            result.query_params.ts_end,
            Some("2022-01-01 12:30:45".to_string())
        );
    }

    #[test]
    fn test_timestamp_parsing_pure_dates() {
        let broker = BgpkitBroker::new();

        // Standard date format - raw input stored during configuration
        let result = broker.clone().ts_start("2022-01-01");
        assert_eq!(result.query_params.ts_start, Some("2022-01-01".to_string()));

        // Slash format
        let result = broker.clone().ts_start("2022/01/01");
        assert_eq!(result.query_params.ts_start, Some("2022/01/01".to_string()));

        // Dot format
        let result = broker.clone().ts_end("2022.01.01");
        assert_eq!(result.query_params.ts_end, Some("2022.01.01".to_string()));

        // Compact format
        let result = broker.clone().ts_end("20220101");
        assert_eq!(result.query_params.ts_end, Some("20220101".to_string()));
    }

    #[test]
    fn test_timestamp_parsing_whitespace() {
        let broker = BgpkitBroker::new();

        // Test that raw input with whitespace is stored during configuration
        let result = broker.clone().ts_start("  2022-01-01  ");
        assert_eq!(
            result.query_params.ts_start,
            Some("  2022-01-01  ".to_string())
        );

        let result = broker.clone().ts_end("\t1640995200\n");
        assert_eq!(
            result.query_params.ts_end,
            Some("\t1640995200\n".to_string())
        );
    }

    #[test]
    fn test_timestamp_parsing_errors() {
        let broker = BgpkitBroker::new();

        // Invalid format - error occurs at query time
        let broker_with_invalid = broker.clone().ts_start("invalid-timestamp");
        let result = broker_with_invalid.query();
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));

        // Invalid date - error occurs at query time
        let broker_with_invalid = broker.clone().ts_end("2022-13-01");
        let result = broker_with_invalid.query();
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));

        // Invalid compact date - error occurs at query time
        let broker_with_invalid = broker.clone().ts_start("20221301");
        let result = broker_with_invalid.query();
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));

        // Partially valid format - error occurs at query time
        let broker_with_invalid = broker.clone().ts_start("2022-01");
        let result = broker_with_invalid.query();
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_parse_timestamp_direct() {
        use chrono::{NaiveDate, NaiveDateTime};

        // Test the parse_timestamp function directly - it now returns DateTime<Utc>

        // Unix timestamp
        let expected_unix = Utc.timestamp_opt(1640995200, 0).single().unwrap();
        assert_eq!(
            BgpkitBroker::parse_timestamp("1640995200").unwrap(),
            expected_unix
        );

        // RFC3339 formats
        let expected_rfc3339_z = Utc.from_utc_datetime(
            &NaiveDateTime::parse_from_str("2022-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S").unwrap(),
        );
        assert_eq!(
            BgpkitBroker::parse_timestamp("2022-01-01T00:00:00Z").unwrap(),
            expected_rfc3339_z
        );

        let expected_rfc3339_no_z = Utc.from_utc_datetime(
            &NaiveDateTime::parse_from_str("2022-01-01T12:30:45", "%Y-%m-%dT%H:%M:%S").unwrap(),
        );
        assert_eq!(
            BgpkitBroker::parse_timestamp("2022-01-01T12:30:45").unwrap(),
            expected_rfc3339_no_z
        );

        let expected_space_format = Utc.from_utc_datetime(
            &NaiveDateTime::parse_from_str("2022-01-01 12:30:45", "%Y-%m-%d %H:%M:%S").unwrap(),
        );
        assert_eq!(
            BgpkitBroker::parse_timestamp("2022-01-01 12:30:45").unwrap(),
            expected_space_format
        );

        // Pure date formats (all convert to start of day in UTC)
        let expected_date = Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(2022, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );
        assert_eq!(
            BgpkitBroker::parse_timestamp("2022-01-01").unwrap(),
            expected_date
        );
        assert_eq!(
            BgpkitBroker::parse_timestamp("2022/01/01").unwrap(),
            expected_date
        );
        assert_eq!(
            BgpkitBroker::parse_timestamp("2022.01.01").unwrap(),
            expected_date
        );
        assert_eq!(
            BgpkitBroker::parse_timestamp("20220101").unwrap(),
            expected_date
        );

        // Test timezone formats - these should now work
        let result_plus_tz = BgpkitBroker::parse_timestamp("2022-01-01T00:00:00+00:00").unwrap();
        assert_eq!(result_plus_tz, expected_date);
        println!("✓ +00:00 timezone format works");

        // Test timezone conversion: 2022-01-01T05:00:00-05:00 = 2022-01-01T10:00:00Z
        let result_minus_tz = BgpkitBroker::parse_timestamp("2022-01-01T05:00:00-05:00").unwrap();
        let expected_10am = Utc.with_ymd_and_hms(2022, 1, 1, 10, 0, 0).unwrap();
        assert_eq!(result_minus_tz, expected_10am);
        println!("✓ -05:00 timezone format works (05:00-05:00 = 10:00Z)");

        // Error cases
        assert!(BgpkitBroker::parse_timestamp("invalid").is_err());
        assert!(BgpkitBroker::parse_timestamp("2022-13-01").is_err());
        assert!(BgpkitBroker::parse_timestamp("2022-01").is_err());
    }

    #[test]
    fn test_collector_id_validation() {
        let broker = BgpkitBroker::new();

        // Valid single collector - no error at validation time
        let broker_valid = broker.clone().collector_id("rrc00");
        let result = broker_valid.validate_configuration();
        assert!(result.is_ok());

        // Valid multiple collectors - no error at validation time
        let broker_valid = broker.clone().collector_id("rrc00,route-views2");
        let result = broker_valid.validate_configuration();
        assert!(result.is_ok());

        // Unknown collector should be allowed (permissive behavior)
        let broker_unknown = broker.clone().collector_id("brand-new-collector");
        let result = broker_unknown.validate_configuration();
        assert!(result.is_ok());

        // Mixed known and unknown collectors should be allowed
        let broker_mixed = broker.clone().collector_id("rrc00,brand-new-collector");
        let result = broker_mixed.validate_configuration();
        assert!(result.is_ok());

        // Empty/whitespace-only should error
        let broker_empty = broker.clone().collector_id(", ,  ,");
        let result = broker_empty.validate_configuration();
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_project_validation() {
        let broker = BgpkitBroker::new();

        // Valid projects - no error at configuration time
        let broker_valid = broker.clone().project("riperis");
        let result = broker_valid.validate_configuration();
        assert!(result.is_ok());

        let broker_valid = broker.clone().project("routeviews");
        let result = broker_valid.validate_configuration();
        assert!(result.is_ok());

        // Valid aliases - no error at configuration time
        let broker_valid = broker.clone().project("rrc");
        let result = broker_valid.validate_configuration();
        assert!(result.is_ok());

        let broker_valid = broker.clone().project("rv");
        let result = broker_valid.validate_configuration();
        assert!(result.is_ok());

        // Invalid project - error occurs at validation
        let broker_invalid = broker.clone().project("invalid-project");
        let result = broker_invalid.validate_configuration();
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_data_type_validation() {
        let broker = BgpkitBroker::new();

        // Valid data types - no error at configuration time
        let broker_valid = broker.clone().data_type("rib");
        let result = broker_valid.validate_configuration();
        assert!(result.is_ok());

        let broker_valid = broker.clone().data_type("updates");
        let result = broker_valid.validate_configuration();
        assert!(result.is_ok());

        // Valid aliases - no error at configuration time
        let broker_valid = broker.clone().data_type("ribs");
        let result = broker_valid.validate_configuration();
        assert!(result.is_ok());

        let broker_valid = broker.clone().data_type("update");
        let result = broker_valid.validate_configuration();
        assert!(result.is_ok());

        // Invalid data type - error occurs at validation
        let broker_invalid = broker.clone().data_type("invalid-type");
        let result = broker_invalid.validate_configuration();
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_page_validation() {
        let broker = BgpkitBroker::new();

        // Valid page number - no error at configuration time
        let broker_valid = broker.clone().page(1);
        let result = broker_valid.validate_configuration();
        assert!(result.is_ok());

        let broker_valid = broker.clone().page(100);
        let result = broker_valid.validate_configuration();
        assert!(result.is_ok());

        // Invalid page number - error occurs at validation
        let broker_invalid = broker.clone().page(0);
        let result = broker_invalid.validate_configuration();
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_page_size_validation() {
        let broker = BgpkitBroker::new();

        // Valid page sizes - no error at configuration time
        let broker_valid = broker.clone().page_size(1);
        let result = broker_valid.validate_configuration();
        assert!(result.is_ok());

        let broker_valid = broker.clone().page_size(100);
        let result = broker_valid.validate_configuration();
        assert!(result.is_ok());

        let broker_valid = broker.clone().page_size(100000);
        let result = broker_valid.validate_configuration();
        assert!(result.is_ok());

        // Invalid page sizes - error occurs at validation
        let broker_invalid = broker.clone().page_size(0);
        let result = broker_invalid.validate_configuration();
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));

        let broker_invalid = broker.clone().page_size(100001);
        let result = broker_invalid.validate_configuration();
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_method_chaining() {
        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .ts_end("1634693400")
            .collector_id("rrc00")
            .project("riperis")
            .data_type("rib")
            .page(1)
            .page_size(10);

        // Raw input is stored during configuration
        assert_eq!(broker.query_params.ts_start, Some("1634693400".to_string()));
        assert_eq!(broker.query_params.ts_end, Some("1634693400".to_string()));
        assert_eq!(broker.query_params.collector_id, Some("rrc00".to_string()));
        assert_eq!(broker.query_params.project, Some("riperis".to_string()));
        assert_eq!(broker.query_params.data_type, Some("rib".to_string()));
        assert_eq!(broker.query_params.page, 1);
        assert_eq!(broker.query_params.page_size, 10);
    }
}
