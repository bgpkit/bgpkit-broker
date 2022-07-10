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

```
use bgpkit_broker::{BgpkitBroker, BrokerItem, QueryParams};

let broker = BgpkitBroker::new_with_params(
    "https://api.broker.bgpkit.com/v2",
    QueryParams{
        ts_start: Some("1634693400".to_string()),
        ts_end: Some("1634693400".to_string()),
        ..Default::default()
    });


// method 1: create iterator from reference (so that you can reuse the broker object)
// same as `&broker.into_iter()`
for item in &broker {
    println!("{:?}", item);
}

// method 2: create iterator from the broker object (taking ownership)
let items = broker.into_iter().collect::<Vec<BrokerItem>>();

assert_eq!(items.len(), 106);
```

## Making Individual Queries

User can make individual queries to the BGPKIT broker backend by calling [BgpkitBroker::query]
function. The function takes a [QueryParams] reference as parameter to construct the query URL.

Below is an example of creating an new struct instance and make queries to the API:
```
use bgpkit_broker::{BgpkitBroker, QueryParams};

let mut params = QueryParams::new();
params = params.ts_start("1634693400");
params = params.ts_end("1634693400");
params = params.page(3);
params = params.page_size(10);

let broker = BgpkitBroker::new("https://api.broker.bgpkit.com/v2");
let res = broker.query(&params);
for data in res.unwrap() {
    println!("{} {} {} {}", data.ts_start, data.data_type, data.collector_id, data.url);
}
```

Making individual queries is useful when you care about specific pages, or want to implement
customized iteration procedure.
*/

mod query;
mod error;

pub use reqwest::Error;
pub use query::{QueryParams, SortOrder, BrokerItem};
pub use error::{BrokerError};
use crate::query::QueryResult;

/// BgpkitBroker struct maintains the broker's URL and handles making API queries.
///
/// See [module doc][crate#examples] for usage examples.
#[derive(Clone)]
pub struct BgpkitBroker {
    pub broker_url: String,
    pub query_params: QueryParams,
}

impl BgpkitBroker {

    /// Construct new BgpkitBroker given a broker URL.
    pub fn new(broker_url: &str) -> Self {
        let url = broker_url.trim_end_matches("/").to_string();
        Self { broker_url: url , query_params: QueryParams{..Default::default()}}
    }

    /// Construct new BgpkitBroker given a broker URL.
    pub fn new_with_params(broker_url: &str, query_params: QueryParams) -> Self {
        let url = broker_url.trim_end_matches("/").to_string();
        Self { broker_url: url , query_params}
    }

    /// Send API queries to broker API endpoint.
    ///
    /// See [QueryParams] for the parameters you can pass in.
    pub fn query(&self, params: &QueryParams) -> Result<Vec<BrokerItem>, BrokerError> {
        let url = format!("{}/search{}", &self.broker_url, params);
        log::info!("sending broker query to {}", &url);
        match run_query(url.as_str()) {
            Ok(res) => return Ok(res),
            Err(e) => return Err(e)
        };
    }

    /// Send query to get **all** data times returned.
    pub fn query_all(&self, params: &QueryParams) -> Result<Vec<BrokerItem>, BrokerError> {
        let mut p: QueryParams = params.clone();
        let mut items = vec![];
        loop {
            let url = format!("{}/search{}", &self.broker_url, &p);
            let res_items = match run_query(url.as_str()) {
                Ok(res) => res,
                Err(e) => {return Err(e)}
            };
            if res_items.len()==0 {
                // reaches the end
                break;
            }
            items.extend(res_items);
            let cur_page = p.page;
            p = p.page(cur_page+1);
        }
        Ok(items)
    }

    /// set query parameters for broker. needed for iterator.
    pub fn set_params(&mut self, params: QueryParams) {
        self.query_params = params;
    }
}

fn run_query(url: &str) -> Result<Vec<BrokerItem>, BrokerError>{
    log::info!("sending broker query to {}", &url);
    match reqwest::blocking::get(url) {
        Ok(res) => {
            match res.json::<QueryResult>()
            {
                Ok(res) => {
                    if let Some(e) = res.error {
                        return Err(BrokerError::BrokerError(e));
                    } else {
                        Ok(res.data)
                    }
                },
                Err(e) => {
                    // json decoding error. most likely the service returns an error message without
                    // `data` field.
                    return Err(BrokerError::BrokerError(e.to_string()))
                }
            }
        }
        Err(e) => { return Err(BrokerError::from(e)) }
    }
}

/// Iterator for BGPKIT Broker that iterates through one [BrokerItem] at a time.
///
/// The [IntoIterator] trait is implemented for both the struct and the reference, so that you can
/// either iterating through items by taking the ownership of the broker, or use the reference to broker
/// to iterate.
///
/// ```
/// use bgpkit_broker::{BgpkitBroker, BrokerItem, QueryParams};
///
/// let mut params = QueryParams::new();
/// params = params.ts_start("1634693400");
/// params = params.ts_end("1634693400");
/// params = params.page_size(10);
/// let mut broker = BgpkitBroker::new("https://api.broker.bgpkit.com/v2");
/// params = params.page(2);
/// broker.set_params(params);
///
/// // create iterator from reference (so that you can reuse the broker object)
/// // same as `&broker.into_intr()`
/// for item in &broker {
///     println!("{:?}", item);
/// }
///
/// // create iterator from the broker object (taking ownership)
/// let items = broker.into_iter().collect::<Vec<BrokerItem>>();
///
/// assert_eq!(items.len(), 96);
/// ```
pub struct BrokerItemIterator {
    broker_url: String,
    query_params: QueryParams,
    cached_items: Vec<BrokerItem>,
    first_run: bool,
}

impl BrokerItemIterator {
    pub fn new(broker: BgpkitBroker) -> BrokerItemIterator {
        let params = broker.query_params.clone();
        BrokerItemIterator{broker_url: broker.broker_url, query_params: params, cached_items: vec![], first_run: true}
    }
}

impl Iterator for BrokerItemIterator {
    type Item = BrokerItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.first_run {
            let url = format!("{}/search{}", &self.broker_url, &self.query_params);
            let items = match run_query(url.as_str()) {
                Ok(i) => i,
                Err(_)  => return None
            };
            if items.len()==0 {
                // first run, nothing returned
                return None
            } else {
                self.cached_items = items;
                self.cached_items.reverse();
            }
            self.first_run=false;
        }

        if let Some(item) = self.cached_items.pop() {
            return Some(item)
        } else {
            self.query_params.page += 1;
            let url = format!("{}/search{}", &self.broker_url, &self.query_params);
            let items = match run_query(url.as_str()) {
                Ok(i) => i,
                Err(_)  => return None
            };
            if items.len()==0 {
                // first run, nothing returned
                return None
            } else {
                self.cached_items = items;
                self.cached_items.reverse();
            }
            return Some(self.cached_items.pop().unwrap())
        }
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
    use env_logger::Env;
    use super::*;

    #[test]
    fn test_query() {
        let mut params = QueryParams::new();
        params = params.ts_start("1634693400");
        params = params.ts_end("1634693400");

        let broker = BgpkitBroker::new("https://api.broker.bgpkit.com/v2");
        let res = broker.query(&params);
        assert!(&res.is_ok());
        let data = res.unwrap();
        assert!(data.len()>0);
    }

    #[test]
    fn test_network_error() {
        let params = QueryParams::new();
        let broker = BgpkitBroker::new("https://api.broker.example.com/v2");
        let res = broker.query(&params);
        // when testing a must-fail query, you could use `matches!` macro to do so
        assert!(res.is_err());
        assert!(matches!(res.err(), Some(BrokerError::NetworkError(_))));
    }

    #[test]
    fn test_broker_error() {
        let mut params = QueryParams::new();
        params = params.page(-1);
        let broker = BgpkitBroker::new("https://api.broker.bgpkit.com/v2");
        let res = broker.query(&params);
        // when testing a must-fail query, you could use `matches!` macro to do so
        assert!(res.is_err());
        assert!(matches!(res.err(), Some(BrokerError::BrokerError(_))));
    }

    #[test]
    fn test_query_all() {
        let mut params = QueryParams::new();
        params = params.ts_start("1634693400");
        params = params.ts_end("1634693400");
        params = params.page_size(100);

        let broker = BgpkitBroker::new("https://api.broker.bgpkit.com/v2");
        let res = broker.query_all(&params);
        assert!(res.is_ok());
        assert_eq!(res.ok().unwrap().len(), 106);
    }

    #[test]
    fn test_iterator() {
        env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

        let broker = BgpkitBroker::new_with_params(
            "https://api.broker.bgpkit.com/v2",
            QueryParams{
                ts_start: Some("1634693400".to_string()),
                ts_end: Some("1634693400".to_string()),
                ..Default::default()
            });
        assert_eq!(broker.into_iter().count(), 106);

        // test iterating from second page
        let broker = BgpkitBroker::new_with_params(
            "https://api.broker.bgpkit.com/v2",
            QueryParams{
                ts_start: Some("1634693400".to_string()),
                ts_end: Some("1634693400".to_string()),
                page: 2,
                ..Default::default()
            });
        assert_eq!(broker.into_iter().count(), 6);
    }

    #[test]
    fn test_filters() {
        env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

        let mut params = QueryParams {
            ts_start: Some("1634693400".to_string()),
            ts_end: Some("1634693400".to_string()),
            ..Default::default()
        };
        let broker = BgpkitBroker::new("https://api.broker.bgpkit.com/v2");
        let items = broker.query_all(&params).unwrap();
        assert_eq!(items.len(), 106);

        params.collector_id = Some("rrc00".to_string());
        let items = broker.query_all(&params).unwrap();
        assert_eq!(items.len(), 2);

        params.collector_id = None;
        params.project = Some("riperis".to_string());
        let items = broker.query_all(&params).unwrap();
        assert_eq!(items.len(), 46);
    }
}