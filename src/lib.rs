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
        .ts_start("1634693400")
        .ts_end("1634693400");


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
    .ts_start("1634693400")
    .ts_end("1634693400")
    .page(3)
    .page_size(10);

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

    /// Add a filter of starting timestamp.
    ///
    /// # Examples
    ///
    /// Specify a Unix timestamp.
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_start("1640995200");
    /// ```
    ///
    /// Specify a RFC3335-formatted time string.
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_start("2022-01-01T00:00:00Z");
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
    /// # Examples
    ///
    /// Specify a Unix timestamp.
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_end("1640995200");
    /// ```
    ///
    /// Specify a RFC3335-formatted time string.
    /// ```
    /// let broker = bgpkit_broker::BgpkitBroker::new()
    ///     .ts_end("2022-01-01T00:00:00Z");
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

    /// Add a filter of project name, i.e. `riperis` or `routeviews`.
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
    ///```
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
                    ))
                }
            },
            Err(e) => {
                return Err(BrokerError::BrokerError(format!(
                    "Unable to connect to the URL ({}): {}",
                    latest_query_url, e
                )))
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
    ///    .collector_id("route-views2");
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
            param_strings.push(format!("ip={}", ip));
        }
        if let Some(asn) = &self.query_params.peers_asn {
            param_strings.push(format!("asn={}", asn));
        }
        if self.query_params.peers_only_full_feed {
            param_strings.push("full_feed=true".to_string());
        }
        if let Some(collector_id) = &self.query_params.collector_id {
            param_strings.push(format!("collector={}", collector_id));
        }
        if !param_strings.is_empty() {
            let param_string = param_strings.join("&");
            url = format!("{}?{}", url, param_string);
        }

        let peers = match self.client.get(url.as_str()).send() {
            Ok(response) => match response.json::<BrokerPeersResult>() {
                Ok(result) => result.data,
                Err(_) => {
                    return Err(BrokerError::BrokerError(
                        "Error parsing response".to_string(),
                    ))
                }
            },
            Err(e) => {
                return Err(BrokerError::BrokerError(format!(
                    "Unable to connect to the URL ({}): {}",
                    url, e
                )))
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
        let res = broker.query();

        assert!(res.is_err());
        // assert!(matches!(res.err(), Some(BrokerError::NetworkError(_))));
    }

    #[test]
    fn test_query_all() {
        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .ts_end("1634693400")
            .page_size(100);
        let res = broker.query();
        assert!(res.is_ok());
        assert_eq!(res.ok().unwrap().len(), 53);
    }

    #[test]
    fn test_iterator() {
        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .ts_end("1634693400");

        assert_eq!(broker.into_iter().count(), 53);
    }

    #[test]
    fn test_filters() {
        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .ts_end("1634693400");
        let items = broker.query().unwrap();
        assert_eq!(items.len(), 53);

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
}
