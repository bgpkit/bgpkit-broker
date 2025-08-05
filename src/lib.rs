/*!
# Overview

[bgpkit-broker][crate] is a package that allows accessing the BGPKIT Broker API and search for BGP archive
files with different search parameters available.

# Examples

## Using Iterator

The recommended usage to collect [BrokerItem]s is to use the built-in iterator. The
[BrokerItemIterator] handles making API queries so that it can continuously stream new items until
it reaches the end of items. This is useful for simply getting **all** matching items without need
to worry about pagination.

```no_run
use bgpkit_broker::{BgpkitBroker, BrokerItem};

let broker = BgpkitBroker::new()
        .ts_start("1634693400").unwrap()
        .ts_end("1634693400").unwrap();


// method 1: create iterator from reference (so that you can reuse the broker object)
// same as `&broker.into_iter()`
for item in &broker {
    println!("{}", item);
}

// method 2: create iterator from the broker object (taking ownership)
let items = broker.into_iter().collect::<Vec<BrokerItem>>();

assert_eq!(items.len(), 106);
```

## Making Individual Queries

User can make individual queries to the BGPKIT broker backend by calling [BgpkitBroker::query_single_page]
function.

Below is an example of creating a new struct instance and make queries to the API:
```rust
use bgpkit_broker::BgpkitBroker;

let mut broker = BgpkitBroker::new()
    .ts_start("1634693400").unwrap()
    .ts_end("1634693400").unwrap()
    .page(3).unwrap()
    .page_size(10).unwrap();

let res = broker.query_single_page();
for data in res.unwrap() {
    println!("{} {} {} {}", data.ts_start, data.data_type, data.collector_id, data.url);
}

broker.turn_page(4);
let res = broker.query_single_page();
for data in res.unwrap() {
    println!("{} {} {} {}", data.ts_start, data.data_type, data.collector_id, data.url);
}
```

Making individual queries is useful when you care about a specific page or want to implement
 a customized iteration procedure. Use [BgpkitBroker::turn_page] to manually change to a different
page.

## Getting the Latest File for Each Collector

We also provide way to fetch the latest file information for each collector available with the
[BgpkitBroker::latest] call. The function returns a JSON-deserialized result (see [CollectorLatestItem])
to the RESTful API at <https://api.broker.bgpkit.com/v3/latest>.

```rust
use bgpkit_broker::BgpkitBroker;

let broker = BgpkitBroker::new();
for item in broker.latest().unwrap() {
    println!("{}", item);
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
use std::collections::HashMap;
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
        let client = match std::env::var("ONEIO_ACCEPT_INVALID_CERTS")
            .unwrap_or_default()
            .to_lowercase()
            .as_str()
        {
            "true" | "yes" | "y" => reqwest::blocking::ClientBuilder::new()
                .danger_accept_invalid_certs(true)
                .build()
                .unwrap(),
            _ => reqwest::blocking::Client::new(),
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

        // Try parsing as RFC3339/ISO8601 with Z first
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
                let naive_datetime = date.and_hms_opt(0, 0, 0).unwrap();
                return Ok(Utc.from_utc_datetime(&naive_datetime));
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
                - RFC3339: '2022-01-01T00:00:00Z', '2022-01-01T00:00:00'\n\
                - Date with time: '2022-01-01 00:00:00'\n\
                - Pure date: '2022-01-01', '2022/01/01', '2022.01.01', '20220101'"
        )))
    }

    /// Add a filter of starting timestamp with validation.
    ///
    /// Supports multiple timestamp formats including Unix timestamps, RFC3339 dates, and pure dates.
    ///
    /// # Examples
    ///
    /// Specify a Unix timestamp:
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_start("1640995200").unwrap();
    /// ```
    ///
    /// Specify a RFC3339-formatted time string:
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_start("2022-01-01T00:00:00Z").unwrap();
    /// ```
    ///
    /// Specify a pure date (defaults to start of day):
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_start("2022-01-01").unwrap();
    /// ```
    ///
    /// Other supported formats:
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_start("2022/01/01").unwrap()  // slash format
    ///     .ts_start("2022.01.01").unwrap()  // dot format
    ///     .ts_start("20220101").unwrap();   // compact format
    /// ```
    pub fn ts_start<S: Display>(self, ts_start: S) -> Result<Self, BrokerError> {
        let parsed_datetime = Self::parse_timestamp(&ts_start.to_string())?;

        let mut query_params = self.query_params;
        query_params.ts_start = Some(parsed_datetime.format("%Y-%m-%dT%H:%M:%SZ").to_string());
        Ok(Self {
            broker_url: self.broker_url,
            query_params,
            client: self.client,
            collector_project_map: self.collector_project_map,
        })
    }

    /// Add a filter of ending timestamp with validation.
    ///
    /// Supports the same multiple timestamp formats as `ts_start`.
    ///
    /// # Examples
    ///
    /// Specify a Unix timestamp:
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_end("1640995200").unwrap();
    /// ```
    ///
    /// Specify a RFC3339-formatted time string:
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_end("2022-01-01T00:00:00Z").unwrap();
    /// ```
    ///
    /// Specify a pure date (defaults to start of day):
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_end("2022-01-01").unwrap();
    /// ```
    pub fn ts_end<S: Display>(self, ts_end: S) -> Result<Self, BrokerError> {
        let parsed_datetime = Self::parse_timestamp(&ts_end.to_string())?;

        let mut query_params = self.query_params;
        query_params.ts_end = Some(parsed_datetime.format("%Y-%m-%dT%H:%M:%SZ").to_string());
        Ok(Self {
            broker_url: self.broker_url,
            client: self.client,
            query_params,
            collector_project_map: self.collector_project_map,
        })
    }

    /// Add a filter of collector ID with validation (e.g. `rrc00` or `route-views2`).
    ///
    /// See the full list of collectors [here](https://github.com/bgpkit/bgpkit-broker-backend/blob/main/deployment/full-config.json).
    ///
    /// # Examples
    ///
    /// filter by single collector
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .collector_id("rrc00").unwrap();
    /// ```
    ///
    /// filter by multiple collector
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .collector_id("route-views2,route-views6").unwrap();
    /// ```
    ///
    /// Invalid collector returns error:
    /// ```
    /// let result = bgpkit_broker::BgpkitBroker::new()
    ///     .collector_id("invalid-collector");
    /// assert!(result.is_err());
    /// ```
    pub fn collector_id<S: Display>(self, collector_id: S) -> Result<Self, BrokerError> {
        let collector_str = collector_id.to_string();

        // Split by comma for multiple collectors
        let collectors: Vec<&str> = collector_str.split(',').map(|s| s.trim()).collect();

        for collector in &collectors {
            if !self.collector_project_map.contains_key(*collector) {
                let valid_collectors: Vec<String> =
                    self.collector_project_map.keys().cloned().collect();
                return Err(BrokerError::ConfigurationError(format!(
                    "Invalid collector ID '{collector}'. Valid collectors are: {}",
                    valid_collectors.join(", ")
                )));
            }
        }

        let mut query_params = self.query_params;
        query_params.collector_id = Some(collector_str);
        Ok(Self {
            client: self.client,
            broker_url: self.broker_url,
            query_params,
            collector_project_map: self.collector_project_map,
        })
    }

    /// Add a filter of project name with validation, i.e. `riperis` or `routeviews`.
    ///
    /// # Examples
    ///
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .project("riperis").unwrap();
    /// ```
    ///
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .project("routeviews").unwrap();
    ///```
    ///
    /// Invalid project returns error:
    /// ```
    /// let result = bgpkit_broker::BgpkitBroker::new()
    ///     .project("invalid");
    /// assert!(result.is_err());
    /// ```
    pub fn project<S: Display>(self, project: S) -> Result<Self, BrokerError> {
        let project_str = project.to_string();
        let project_lower = project_str.to_lowercase();

        match project_lower.as_str() {
            "rrc" | "riperis" | "ripe_ris" | "routeviews" | "route_views" | "rv" => {
                let mut query_params = self.query_params;
                query_params.project = Some(project_str);
                Ok(Self {
                    client: self.client,
                    broker_url: self.broker_url,
                    query_params,
                    collector_project_map: self.collector_project_map,
                })
            }
            _ => Err(BrokerError::ConfigurationError(format!(
                "Invalid project '{project_str}'. Valid projects are: 'riperis' (aliases: 'rrc', 'ripe_ris') or 'routeviews' (aliases: 'route_views', 'rv')"
            ))),
        }
    }

    /// Add filter of data type with validation, i.e. `rib` or `updates`.
    ///
    /// # Examples
    ///
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .data_type("rib").unwrap();
    /// ```
    ///
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .data_type("updates").unwrap();
    /// ```
    ///
    /// Invalid data type returns error:
    /// ```
    /// let result = bgpkit_broker::BgpkitBroker::new()
    ///     .data_type("invalid");
    /// assert!(result.is_err());
    /// ```
    pub fn data_type<S: Display>(self, data_type: S) -> Result<Self, BrokerError> {
        let data_type_str = data_type.to_string();
        let data_type_lower = data_type_str.to_lowercase();

        match data_type_lower.as_str() {
            "rib" | "ribs" | "r" | "update" | "updates" => {
                let mut query_params = self.query_params;
                query_params.data_type = Some(data_type_str);
                Ok(Self {
                    broker_url: self.broker_url,
                    client: self.client,
                    query_params,
                    collector_project_map: self.collector_project_map,
                })
            }
            _ => Err(BrokerError::ConfigurationError(format!(
                "Invalid data type '{data_type_str}'. Valid data types are: 'rib' (aliases: 'ribs', 'r') or 'updates' (alias: 'update')"
            ))),
        }
    }

    /// Change the current page number with validation, starting from 1.
    ///
    /// # Examples
    ///
    /// Start iterating with page 2.
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .page(2).unwrap();
    /// ```
    ///
    /// Invalid page number returns error:
    /// ```
    /// let result = bgpkit_broker::BgpkitBroker::new()
    ///     .page(0);
    /// assert!(result.is_err());
    /// ```
    pub fn page(self, page: i64) -> Result<Self, BrokerError> {
        if page < 1 {
            return Err(BrokerError::ConfigurationError(format!(
                "Invalid page number {page}. Page number must be >= 1"
            )));
        }

        let mut query_params = self.query_params;
        query_params.page = page;
        Ok(Self {
            broker_url: self.broker_url,
            client: self.client,
            query_params,
            collector_project_map: self.collector_project_map,
        })
    }

    /// Change current page size with validation, default 100.
    ///
    /// # Examples
    ///
    /// Set page size to 20.
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .page_size(10).unwrap();
    /// ```
    ///
    /// Invalid page size returns error:
    /// ```
    /// let result = bgpkit_broker::BgpkitBroker::new()
    ///     .page_size(0);
    /// assert!(result.is_err());
    /// ```
    pub fn page_size(self, page_size: i64) -> Result<Self, BrokerError> {
        if !(1..=100000).contains(&page_size) {
            return Err(BrokerError::ConfigurationError(format!(
                "Invalid page size {page_size}. Page size must be between 1 and 100000"
            )));
        }

        let mut query_params = self.query_params;
        query_params.page_size = page_size;
        Ok(Self {
            broker_url: self.broker_url,
            client: self.client,
            query_params,
            collector_project_map: self.collector_project_map,
        })
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
    /// ```
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
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new();
    /// let items = broker.query_single_page().unwrap();
    /// ```
    pub fn query_single_page(&self) -> Result<Vec<BrokerItem>, BrokerError> {
        let url = format!("{}/search{}", &self.broker_url, &self.query_params);
        log::info!("sending broker query to {}", &url);
        match self.run_files_query(url.as_str()) {
            Ok(res) => Ok(res),
            Err(e) => Err(e),
        }
    }

    /// Check if the broker instance is healthy.
    ///
    /// # Examples
    ///
    /// ```
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
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_start("2022-01-01T00:00:00Z").unwrap()
    ///     .ts_end("2022-01-01T23:59:00Z").unwrap()
    ///     .data_type("rib").unwrap()
    ///     .collector_id("route-views2").unwrap();
    /// let items = broker.query().unwrap();
    ///
    /// // 1 RIB dump very 2 hours, total of 12 files for 1 day
    /// assert_eq!(items.len(), 12);
    /// ```
    pub fn query(&self) -> Result<Vec<BrokerItem>, BrokerError> {
        let mut p: QueryParams = self.query_params.clone();

        let mut items = vec![];
        loop {
            let url = format!("{}/search{}", &self.broker_url, &p);

            let res_items = self.run_files_query(url.as_str())?;

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
    /// ```
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
                if item.collector_id.as_str() != collector_id.as_str() {
                    matches = false
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
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new();
    /// let peers = broker.get_peers().unwrap();
    /// for peer in &peers {
    ///     println!("{:?}", peer);
    /// }
    /// ```
    ///
    /// ## Get peers from a specific collector
    ///
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///    .collector_id("route-views2").unwrap();
    /// let peers = broker.get_peers().unwrap();
    /// for peer in &peers {
    ///    println!("{:?}", peer);
    /// }
    /// ```
    ///
    /// ## Get peers from a specific ASN
    ///
    /// ```
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
    /// ```
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
    /// ```
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
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///  .collector_id("route-views2").unwrap()
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

    fn run_files_query(&self, url: &str) -> Result<Vec<BrokerItem>, BrokerError> {
        log::info!("sending broker query to {}", &url);
        match self.client.get(url).send() {
            Ok(res) => match res.json::<BrokerQueryResult>() {
                Ok(res) => {
                    if let Some(e) = res.error {
                        Err(BrokerError::BrokerError(e))
                    } else {
                        Ok(res.data)
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
/// ```
/// use bgpkit_broker::{BgpkitBroker, BrokerItem};
///
/// let mut broker = BgpkitBroker::new()
///     .ts_start("1634693400").unwrap()
///     .ts_end("1634693400").unwrap()
///     .page_size(10).unwrap()
///     .page(2).unwrap();
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
            .unwrap()
            .ts_end("1634693400")
            .unwrap();
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
        let result = BgpkitBroker::new().page(-1);
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
            .unwrap()
            .ts_end("1634693400")
            .unwrap()
            .page_size(100)
            .unwrap();
        let res = broker.query();
        assert!(res.is_ok());
        assert_eq!(res.ok().unwrap().len(), 53);
    }

    #[test]
    fn test_iterator() {
        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .unwrap()
            .ts_end("1634693400")
            .unwrap();

        assert_eq!(broker.into_iter().count(), 53);
    }

    #[test]
    fn test_filters() {
        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .unwrap()
            .ts_end("1634693400")
            .unwrap();
        let items = broker.query().unwrap();
        assert_eq!(items.len(), 53);

        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .unwrap()
            .ts_end("1634693400")
            .unwrap()
            .collector_id("rrc00")
            .unwrap();
        let items = broker.query().unwrap();
        assert_eq!(items.len(), 1);

        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .unwrap()
            .ts_end("1634693400")
            .unwrap()
            .project("riperis")
            .unwrap();
        let items = broker.query().unwrap();
        assert_eq!(items.len(), 23);
    }

    #[test]
    fn test_latest() {
        let broker = BgpkitBroker::new();
        let items = broker.latest().unwrap();
        assert!(items.len() >= 125);

        let broker = BgpkitBroker::new()
            .project("routeviews".to_string())
            .unwrap();
        let items = broker.latest().unwrap();
        assert!(!items.is_empty());
        assert!(items
            .iter()
            .all(|item| !item.collector_id.starts_with("rrc")));

        let broker = BgpkitBroker::new().project("riperis".to_string()).unwrap();
        let items = broker.latest().unwrap();
        assert!(!items.is_empty());
        assert!(items
            .iter()
            .all(|item| item.collector_id.starts_with("rrc")));

        let broker = BgpkitBroker::new().data_type("rib".to_string()).unwrap();
        let items = broker.latest().unwrap();
        assert!(!items.is_empty());
        assert!(items.iter().all(|item| item.is_rib()));

        let broker = BgpkitBroker::new().data_type("update".to_string()).unwrap();
        let items = broker.latest().unwrap();
        assert!(!items.is_empty());
        assert!(items.iter().all(|item| !item.is_rib()));

        let broker = BgpkitBroker::new()
            .collector_id("rrc00".to_string())
            .unwrap();
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

        let broker = BgpkitBroker::new().collector_id("rrc00").unwrap();
        let rrc_peers = broker.get_peers().unwrap();
        assert!(!rrc_peers.is_empty());
        assert!(rrc_peers.iter().all(|peer| peer.collector == "rrc00"));

        let broker = BgpkitBroker::new()
            .collector_id("rrc00,route-views2")
            .unwrap();
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

        // Valid Unix timestamps (now normalized to RFC3339)
        let result = broker.clone().ts_start("1640995200");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().query_params.ts_start,
            Some("2022-01-01T00:00:00Z".to_string())
        );

        let result = broker.clone().ts_end("1640995200");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().query_params.ts_end,
            Some("2022-01-01T00:00:00Z".to_string())
        );
    }

    #[test]
    fn test_timestamp_parsing_rfc3339() {
        let broker = BgpkitBroker::new();

        // RFC3339 with Z (preserved as RFC3339)
        let result = broker.clone().ts_start("2022-01-01T00:00:00Z");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().query_params.ts_start,
            Some("2022-01-01T00:00:00Z".to_string())
        );

        // RFC3339 without Z (now normalized to RFC3339 with Z)
        let result = broker.clone().ts_start("2022-01-01T12:30:45");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().query_params.ts_start,
            Some("2022-01-01T12:30:45Z".to_string())
        );

        // Date with time format (now normalized to RFC3339 with Z)
        let result = broker.clone().ts_end("2022-01-01 12:30:45");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().query_params.ts_end,
            Some("2022-01-01T12:30:45Z".to_string())
        );
    }

    #[test]
    fn test_timestamp_parsing_pure_dates() {
        let broker = BgpkitBroker::new();

        // Standard date format (defaults to start of day, normalized to RFC3339)
        let result = broker.clone().ts_start("2022-01-01");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().query_params.ts_start,
            Some("2022-01-01T00:00:00Z".to_string())
        );

        // Slash format
        let result = broker.clone().ts_start("2022/01/01");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().query_params.ts_start,
            Some("2022-01-01T00:00:00Z".to_string())
        );

        // Dot format
        let result = broker.clone().ts_end("2022.01.01");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().query_params.ts_end,
            Some("2022-01-01T00:00:00Z".to_string())
        );

        // Compact format
        let result = broker.clone().ts_end("20220101");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().query_params.ts_end,
            Some("2022-01-01T00:00:00Z".to_string())
        );
    }

    #[test]
    fn test_timestamp_parsing_whitespace() {
        let broker = BgpkitBroker::new();

        // Test that whitespace is trimmed (normalized to RFC3339)
        let result = broker.clone().ts_start("  2022-01-01  ");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().query_params.ts_start,
            Some("2022-01-01T00:00:00Z".to_string())
        );

        let result = broker.clone().ts_end("\t1640995200\n");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().query_params.ts_end,
            Some("2022-01-01T00:00:00Z".to_string())
        );
    }

    #[test]
    fn test_timestamp_parsing_errors() {
        let broker = BgpkitBroker::new();

        // Invalid format
        let result = broker.clone().ts_start("invalid-timestamp");
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));

        // Invalid date
        let result = broker.clone().ts_end("2022-13-01");
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));

        // Invalid compact date
        let result = broker.clone().ts_start("20221301");
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));

        // Partially valid format
        let result = broker.clone().ts_start("2022-01");
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

        // Error cases
        assert!(BgpkitBroker::parse_timestamp("invalid").is_err());
        assert!(BgpkitBroker::parse_timestamp("2022-13-01").is_err());
        assert!(BgpkitBroker::parse_timestamp("2022-01").is_err());
    }

    #[test]
    fn test_collector_id_validation() {
        let broker = BgpkitBroker::new();

        // Valid single collector
        let result = broker.clone().collector_id("rrc00");
        assert!(result.is_ok());

        // Valid multiple collectors
        let result = broker.clone().collector_id("rrc00,route-views2");
        assert!(result.is_ok());

        // Invalid collector
        let result = broker.clone().collector_id("invalid-collector");
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));

        // Mixed valid and invalid collectors
        let result = broker.clone().collector_id("rrc00,invalid-collector");
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_project_validation() {
        let broker = BgpkitBroker::new();

        // Valid projects
        let result = broker.clone().project("riperis");
        assert!(result.is_ok());

        let result = broker.clone().project("routeviews");
        assert!(result.is_ok());

        // Valid aliases
        let result = broker.clone().project("rrc");
        assert!(result.is_ok());

        let result = broker.clone().project("rv");
        assert!(result.is_ok());

        // Invalid project
        let result = broker.clone().project("invalid-project");
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_data_type_validation() {
        let broker = BgpkitBroker::new();

        // Valid data types
        let result = broker.clone().data_type("rib");
        assert!(result.is_ok());

        let result = broker.clone().data_type("updates");
        assert!(result.is_ok());

        // Valid aliases
        let result = broker.clone().data_type("ribs");
        assert!(result.is_ok());

        let result = broker.clone().data_type("update");
        assert!(result.is_ok());

        // Invalid data type
        let result = broker.clone().data_type("invalid-type");
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_page_validation() {
        let broker = BgpkitBroker::new();

        // Valid page number
        let result = broker.clone().page(1);
        assert!(result.is_ok());

        let result = broker.clone().page(100);
        assert!(result.is_ok());

        // Invalid page number
        let result = broker.clone().page(0);
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));

        let result = broker.clone().page(-1);
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_page_size_validation() {
        let broker = BgpkitBroker::new();

        // Valid page sizes
        let result = broker.clone().page_size(1);
        assert!(result.is_ok());

        let result = broker.clone().page_size(100);
        assert!(result.is_ok());

        let result = broker.clone().page_size(100000);
        assert!(result.is_ok());

        // Invalid page sizes
        let result = broker.clone().page_size(0);
        assert!(result.is_err());
        assert!(matches!(
            result.err(),
            Some(BrokerError::ConfigurationError(_))
        ));

        let result = broker.clone().page_size(100001);
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
            .unwrap()
            .ts_end("1634693400")
            .unwrap()
            .collector_id("rrc00")
            .unwrap()
            .project("riperis")
            .unwrap()
            .data_type("rib")
            .unwrap()
            .page(1)
            .unwrap()
            .page_size(10)
            .unwrap();

        assert_eq!(
            broker.query_params.ts_start,
            Some("2021-10-20T01:30:00Z".to_string())
        );
        assert_eq!(
            broker.query_params.ts_end,
            Some("2021-10-20T01:30:00Z".to_string())
        );
        assert_eq!(broker.query_params.collector_id, Some("rrc00".to_string()));
        assert_eq!(broker.query_params.project, Some("riperis".to_string()));
        assert_eq!(broker.query_params.data_type, Some("rib".to_string()));
        assert_eq!(broker.query_params.page, 1);
        assert_eq!(broker.query_params.page_size, 10);
    }
}
