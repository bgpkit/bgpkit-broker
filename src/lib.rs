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

pub mod query;
pub mod error;

pub use reqwest::Error;
pub use query::*;
pub use error::*;

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
    pub broker_url: String,
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
        log::info!("sending broker query to {}", &url);
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

    /// Send query to get **all** data times returned.
    pub fn query_all(&self, params: &QueryParams) -> Result<Vec<BrokerItem>, BrokerError> {
        let mut p: QueryParams = params.clone();
        let mut items = vec![];
        loop {
            let url = format!("{}/search{}", &self.broker_url, &p);
            log::info!("sending broker query to {}", &url);
            let total_page ;
            match reqwest::blocking::get(url) {
                Ok(res) => {
                    match res.json::<QueryResult>()
                    {
                        Ok(res) => {
                            if let Some(e) = res.error {
                                return Err(BrokerError::BrokerError(e));
                            } else {
                                let data = res.data.unwrap();
                                total_page = data.total_pages;
                                items.extend(data.items);
                            }
                        },
                        Err(e) => { return Err(BrokerError::from(e)) }
                    }
                }
                Err(e) => { return Err(BrokerError::from(e)) }
            }

            let cur_page = p.page;
            if cur_page >= total_page {
                // reaches the end
                break;
            }
            p = p.page(cur_page+1);
        }
        Ok(items)
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

    #[test]
    fn test_query_all() {
        let mut params = QueryParams::new();
        params = params.start_ts(1634693400);
        params = params.end_ts(1634693400);
        params = params.page_size(10);

        let broker = BgpkitBroker::new("https://api.broker.bgpkit.com/v1");
        let res = broker.query_all(&params);
        assert!(res.is_ok());
        assert_eq!(res.ok().unwrap().len(), 58);
    }
}