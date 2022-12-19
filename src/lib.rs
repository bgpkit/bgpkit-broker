/*!
# Overview

[bgpkit-broker][crate] is a package that allow access the BGPKIT Broker API and search for BGP archive
files with different search parameters available.

# Examples

## Using Iterator

The recommended usage to collect [BrokerItem]s is to use the built-in iterator. The
[BrokerItemIterator] handles making API queries so that it can continuously stream new items until
it reaches the end of items. This is useful for simply getting **all** matching items without need
to worry about pagination.

```rust
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

Below is an example of creating an new struct instance and make queries to the API:
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

Making individual queries is useful when you care about specific pages, or want to implement
customized iteration procedure. Use [BgpkitBroker::turn_page] to manually change to a different
page.
*/

mod query;
mod error;

pub use ureq::Error;
pub use query::{QueryParams, SortOrder, BrokerItem};
pub use error::BrokerError;
use crate::query::QueryResult;

/// BgpkitBroker struct maintains the broker's URL and handles making API queries.
///
/// See [module doc][crate#examples] for usage examples.
#[derive(Clone)]
pub struct BgpkitBroker {
    pub broker_url: String,
    pub query_params: QueryParams,
}

impl Default for BgpkitBroker {
    fn default() -> Self {
        Self{
            broker_url: "https://api.broker.bgpkit.com/v2".to_string(),
            query_params: Default::default()
        }
    }
}

impl BgpkitBroker {

    /// Construct new BgpkitBroker given a broker URL.
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure broker URL.
    ///
    /// You can change the default broker URL to point to your own broker instance.
    pub fn broker_url(self, url: &str) -> Self {
        Self {
            broker_url: url.to_string(),
            query_params: self.query_params
        }
    }

    /// Add filter by starting timestamp.
    pub fn ts_start(self, ts_start: &str) -> Self {
        let mut query_params = self.query_params;
        query_params.ts_start = Some(ts_start.to_string());
        Self {
            broker_url: self.broker_url,
            query_params
        }
    }

    /// Add filter by ending timestamp.
    pub fn ts_end(self, ts_end: &str) -> Self {
        let mut query_params = self.query_params;
        query_params.ts_end = Some(ts_end.to_string());
        Self {
            broker_url: self.broker_url,
            query_params
        }
    }

    /// Add filter by collector ID (e.g. `rrc00` or `route-views2`).
    pub fn collector_id(self, collector_id: &str) -> Self {
        let mut query_params = self.query_params;
        query_params.collector_id = Some(collector_id.to_string());
        Self {
            broker_url: self.broker_url,
            query_params
        }
    }

    /// Add filter by project name, i.e. `riperis` or `routeviews`.
    pub fn project(self, project: &str) -> Self {
        let mut query_params = self.query_params;
        query_params.project = Some(project.to_string());
        Self {
            broker_url: self.broker_url,
            query_params
        }
    }

    /// Add filter by data type, i.e. `rib` or `update`.
    pub fn data_type(self, data_type: &str) -> Self {
        let mut query_params = self.query_params;
        query_params.data_type = Some(data_type.to_string());
        Self {
            broker_url: self.broker_url,
            query_params
        }
    }

    /// Change current page number, starting from 1.
    pub fn page(self, page: i64) -> Self {
        let mut query_params = self.query_params;
        query_params.page = page;
        Self {
            broker_url: self.broker_url,
            query_params
        }
    }

    /// Change current page size, default 100.
    pub fn page_size(self, page_size: i64) -> Self {
        let mut query_params = self.query_params;
        query_params.page_size = page_size;
        Self {
            broker_url: self.broker_url,
            query_params
        }
    }

    /// Turn to specified page, page starting from 1.
    ///
    /// This works with [Self::query_single_page] function to manually paginate.
    pub fn turn_page(&mut self, page: i64) {
        self.query_params.page = page;
    }

    /// Send API for a single page of items.
    pub fn query_single_page(&self) -> Result<Vec<BrokerItem>, BrokerError> {
        let url = format!("{}/search{}", &self.broker_url, &self.query_params);
        log::info!("sending broker query to {}", &url);
        match run_query(url.as_str()) {
            Ok(res) => Ok(res),
            Err(e) => Err(e)
        }
    }

    /// Send query to get **all** data times returned.
    pub fn query(&self) -> Result<Vec<BrokerItem>, BrokerError> {
        let mut p: QueryParams = self.query_params.clone();

        let mut items = vec![];
        loop {
            let url = format!("{}/search{}", &self.broker_url, &p);
            let res_items = match run_query(url.as_str()) {
                Ok(res) => res,
                Err(e) => {return Err(e)}
            };

            let items_count = res_items.len() as i64;

            if items_count ==0 {
                // reaches the end
                break;
            }

            items.extend(res_items);
            let cur_page = p.page;
            p = p.page(cur_page+1);

            if items_count < p.page_size {
                // reaches the end
                break;
            }
        }
        Ok(items)
    }

    /// Send query to get the **latest** data for each collector.
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
    pub fn latest(&self) -> Result<Vec<CollectorLatestItem>, BrokerError> {
        let latest_query_url = format!("{}/latest", self.broker_url);
        match ureq::get(latest_query_url.as_str()).call() {
            Ok(response) => {
                match response.into_json::<Vec<CollectorLatestItem>>() {
                    Ok(items) => {
                        Ok(items)
                    }
                    Err(_) => {
                        Err(BrokerError::BrokerError("Error parsing response".to_string()))
                    }
                }
            }
            Err(_) => {
                Err(BrokerError::BrokerError(format!("Unable to connect to the URL: {}", latest_query_url)))
            }
        }
    }
}

fn run_query(url: &str) -> Result<Vec<BrokerItem>, BrokerError>{
    log::info!("sending broker query to {}", &url);
    match ureq::get(url).call() {
        Ok(res) => {
            match res.into_json::<QueryResult>()
            {
                Ok(res) => {
                    if let Some(e) = res.error {
                        Err(BrokerError::BrokerError(e))
                    } else {
                        Ok(res.data)
                    }
                },
                Err(e) => {
                    // json decoding error. most likely the service returns an error message without
                    // `data` field.
                    Err(BrokerError::BrokerError(e.to_string()))
                }
            }
        }
        Err(e) => { Err(BrokerError::from(e)) }
    }
}

/// Iterator for BGPKIT Broker that iterates through one [BrokerItem] at a time.
///
/// The [IntoIterator] trait is implemented for both the struct and the reference, so that you can
/// either iterating through items by taking the ownership of the broker, or use the reference to broker
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
/// assert_eq!(items.len(), 96);
/// ```
pub struct BrokerItemIterator {
    broker: BgpkitBroker,
    cached_items: Vec<BrokerItem>,
    first_run: bool,
}

impl BrokerItemIterator {
    pub fn new(broker: BgpkitBroker) -> BrokerItemIterator {
        BrokerItemIterator{broker, cached_items: vec![], first_run: true}
    }
}

impl Iterator for BrokerItemIterator {
    type Item = BrokerItem;

    fn next(&mut self) -> Option<Self::Item> {
        // if we have cached items, simply pop and return
        if let Some(item) = self.cached_items.pop() {
            return Some(item)
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
            Err(_)  => return None
        };

        if items.is_empty() {
            // break out the iteration
            return None
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

        // this will result in a 422 network error code from the server
        // when testing a must-fail query, you could use `matches!` macro to do so
        assert!(res.is_err());
        assert!(matches!(res.err(), Some(BrokerError::NetworkError(_))));
    }

    #[test]
    fn test_query_all() {
        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .ts_end("1634693400")
            .page_size(100);
        let res = broker.query();
        assert!(res.is_ok());
        assert_eq!(res.ok().unwrap().len(), 106);
    }

    #[test]
    fn test_iterator() {
        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .ts_end("1634693400");

        assert_eq!(broker.into_iter().count(), 106);

        // test iterating from second page
        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .ts_end("1634693400")
            .page(2);
        assert_eq!(broker.into_iter().count(), 6);
    }

    #[test]
    fn test_filters() {
        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .ts_end("1634693400");
        let items = broker.query().unwrap();
        assert_eq!(items.len(), 106);

        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .ts_end("1634693400")
            .collector_id("rrc00");
        let items = broker.query().unwrap();
        assert_eq!(items.len(), 2);

        let broker = BgpkitBroker::new()
            .ts_start("1634693400")
            .ts_end("1634693400")
            .project("riperis");
        let items = broker.query().unwrap();
        assert_eq!(items.len(), 46);
    }

    #[test]
    fn test_latest() {
        let broker = BgpkitBroker::new();
        let items = broker.latest().unwrap();
        assert!(items.len()>=125);
    }
}
