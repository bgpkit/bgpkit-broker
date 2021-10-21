//! bgpkit-broker is a package that allow access the BGPKIT Broker API and search for BGP archive
//! files with different search parameters available.
//!
//! Below is an example of creating an new struct instance and make queries to the API.
//! ```
//! use bgpkit_broker::{BgpkitBroker, QueryParams};
//!
//! let mut params = QueryParams::new();
//! params = params.start_ts(1634693400);
//! params = params.end_ts(1634693400);
//!
//! let broker = BgpkitBroker::new("https://api.broker.bgpkit.com/v1");
//! let res = broker.query(&params);
//! for data in res.unwrap().data.unwrap().items {
//!     println!("{} {} {} {}", data.timestamp, data.data_type, data.collector_id, data.url);
//! }
//! ```

mod query;
mod error;

pub use reqwest::Error;
pub use query::*;
use crate::error::BrokerError;
use crate::query::QueryResult;

/// BgpkitBroker struct maintains the broker's URL and handles making API queries.
///
/// Below is an example of creating an new struct instance and make queries to the API.
/// ```
/// use bgpkit_broker::{BgpkitBroker, QueryParams};
///
/// let mut params = QueryParams::new();
/// params = params.start_ts(1634693400);
/// params = params.end_ts(1634693400);
///
/// let broker = BgpkitBroker::new("https://api.broker.bgpkit.com/v1");
/// let res = broker.query(&params);
/// for data in res.unwrap().data.unwrap().items {
///     println!("{} {} {} {}", data.timestamp, data.data_type, data.collector_id, data.url);
/// }
/// ```
pub struct BgpkitBroker {
    broker_url: String,
}

impl BgpkitBroker {

    /// Construct new BgpkitBroker given a broker URL.
    pub fn new(broker_url: &str) -> Self {
        let url = broker_url.trim_end_matches("/").to_string();
        Self { broker_url: url }
    }

    /// Send API queries to broker API endpoint.
    ///
    /// See [QueryParams] for the parameters you can pass in.
    pub fn query(&self, params: &QueryParams) -> Result<QueryResult, BrokerError> {
        let url = format!("{}/search{}", &self.broker_url, params);
        match reqwest::blocking::get(url) {
            Ok(res) => {
                match res.json::<QueryResult>()
                {
                    Ok(res) => {
                        if let Some(e) = res.error {
                            Err(BrokerError::BrokerError(e))
                        } else {
                            Ok(res)
                        }
                    },
                    Err(e) => { return Err(BrokerError::from(e)) }
                }
            }
            Err(e) => { return Err(BrokerError::from(e)) }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query() {
        let mut params = QueryParams::new();
        params = params.start_ts(1634693400);
        params = params.end_ts(1634693400);

        let broker = BgpkitBroker::new("https://api.broker.bgpkit.com/v1");
        let res = broker.query(&params);
        assert!(&res.is_ok());
        let data = res.unwrap().data.unwrap();
        assert!(data.items.len()>0);
        assert_eq!(data.items[0].timestamp, 1634693400);
    }

    #[test]
    fn test_network_error() {
        let params = QueryParams::new();
        let broker = BgpkitBroker::new("https://api.broker.example.com/v1");
        let res = broker.query(&params);
        // when testing a must-fail query, you could use `matches!` macro to do so
        assert!(res.is_err());
        assert!(matches!(res.err(), Some(BrokerError::NetworkError(_))));
    }

    #[test]
    fn test_broker_error() {
        let mut params = QueryParams::new();
        params = params.page(-1);
        let broker = BgpkitBroker::new("https://api.broker.bgpkit.com/v1");
        let res = broker.query(&params);
        // when testing a must-fail query, you could use `matches!` macro to do so
        assert!(res.is_err());
        assert!(matches!(res.err(), Some(BrokerError::BrokerError(_))));
    }
}