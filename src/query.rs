//! Query-related structs and implementation.
use crate::BrokerItem;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::net::IpAddr;

/// QueryParams represents the query parameters to the backend API.
///
/// Example for constructing a QueryParams:
/// ```
/// use bgpkit_broker::QueryParams;
/// let mut params = QueryParams::new();
/// params = params.ts_start("1633046400");
/// params = params.ts_end("1633132800");
/// params = params.collector_id("rrc00");
/// params = params.project("riperis");
/// params = params.data_type("rib");
/// params = params.page(2);
/// params = params.page_size(20);
/// ```
/// The above example constructs a query that searches for BGP archive files that are:
/// - after 2021-10-01T00:00:00 UTC
/// - before 2021-10-02T00:00:00 UTC
/// - from collector `rrc00`
/// - from `riperis` collectors (already implied by collector=`rrc00` though)
/// - rib table dump files
/// - second page
/// - each page contains 20 items
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryParams {
    /// start unix timestamp: files with time after or equals to `ts_start` will match
    pub ts_start: Option<String>,
    /// end unix timestamp: files with time before or equals to `ts_end` will match
    pub ts_end: Option<String>,
    /// collector identifier, e.g. `rrc00` or `route-views2`
    pub collector_id: Option<String>,
    /// archive project name: `riperis` or `routeviews`
    pub project: Option<String>,
    /// archive data type: `rib` or `updates`
    pub data_type: Option<String>,
    /// page number to seek to, starting from 1, default to 1
    pub page: i64,
    /// number of items each page contains, default to 10, max to 100000
    pub page_size: i64,
    /// collector peer IP address (for listing peers info)
    pub peers_ip: Option<IpAddr>,
    /// collector peer ASN (for listing peers info)
    pub peers_asn: Option<u32>,
    /// collector peer full feed status (for listing peers info)
    pub peers_only_full_feed: bool,
}

/// Sorting order enum
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SortOrder {
    /// `ASC` -> sort by increasing on timestamp
    ASC,
    /// `DESC` -> sort by decreasing on timestamp
    DESC,
}

/// Default [QueryParams] values
impl Default for QueryParams {
    fn default() -> Self {
        QueryParams {
            ts_start: None,
            ts_end: None,
            collector_id: None,
            project: None,
            data_type: None,
            page: 1,
            page_size: 100,
            peers_ip: None,
            peers_asn: None,
            peers_only_full_feed: false,
        }
    }
}

impl Display for SortOrder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SortOrder::ASC => {
                write!(f, "asc")
            }
            SortOrder::DESC => {
                write!(f, "desc")
            }
        }
    }
}

impl Display for QueryParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut params_vec = vec![];
        if let Some(v) = &self.ts_start {
            params_vec.push(format!("ts_start={}", v));
        }
        if let Some(v) = &self.ts_end {
            params_vec.push(format!("ts_end={}", v));
        }
        if let Some(v) = &self.collector_id {
            params_vec.push(format!("collector_id={}", v));
        }
        if let Some(v) = &self.project {
            params_vec.push(format!("project={}", v));
        }
        if let Some(v) = &self.data_type {
            params_vec.push(format!("data_type={}", v));
        }
        params_vec.push(format!("page={}", self.page));
        params_vec.push(format!("page_size={}", self.page_size));

        if !params_vec.is_empty() {
            write!(f, "?{}", params_vec.join("&"))
        } else {
            write!(f, "")
        }
    }
}

impl QueryParams {
    pub fn new() -> QueryParams {
        QueryParams {
            ts_start: None,
            ts_end: None,
            collector_id: None,
            project: None,
            data_type: None,
            page: 1,
            page_size: 10,
            ..Default::default()
        }
    }

    /// set starting timestamp for the search and returns a new [QueryParams] object.
    ///
    /// ```
    /// use bgpkit_broker::QueryParams;
    /// let mut params = QueryParams::new();
    /// params = params.ts_start("1633046400");
    /// ```
    pub fn ts_start(self, ts_start: &str) -> Self {
        QueryParams {
            ts_start: Some(ts_start.to_string()),
            ..self
        }
    }

    /// set ending timestamp for the search and returns a new [QueryParams] object.
    ///
    /// ```
    /// use bgpkit_broker::QueryParams;
    /// let mut params = QueryParams::new();
    /// params = params.ts_end("1633046400");
    /// ```
    pub fn ts_end(self, ts_end: &str) -> Self {
        QueryParams {
            ts_end: Some(ts_end.to_string()),
            ..self
        }
    }

    /// set page number for the each for pagination. **the page number starts from 1**.
    ///
    /// ```
    /// use bgpkit_broker::QueryParams;
    /// let mut params = QueryParams::new();
    /// params = params.page(3);
    /// ```
    pub fn page(self, page: i64) -> Self {
        QueryParams { page, ..self }
    }

    /// set each page's size (number of items per page).
    ///
    /// ```
    /// use bgpkit_broker::QueryParams;
    /// let mut params = QueryParams::new();
    /// params = params.page_size(20);
    /// ```
    pub fn page_size(self, page_size: i64) -> Self {
        QueryParams { page_size, ..self }
    }

    /// set the type of data to search for:
    /// - `rib`: table dump files
    /// - `updates`: BGP updates files
    ///
    /// Without specifying a data type, it defaults to search for all types.
    ///
    /// ```
    /// use bgpkit_broker::QueryParams;
    /// let mut params = QueryParams::new();
    /// params = params.data_type("rib");
    /// ```
    pub fn data_type(self, data_type: &str) -> Self {
        QueryParams {
            data_type: Some(data_type.to_string()),
            ..self
        }
    }

    /// set searching for only data from specific project:
    /// - `routeviews`: RouteViews
    /// - `riperis`: RIPE RIS
    ///
    /// ```
    /// use bgpkit_broker::QueryParams;
    /// let mut params = QueryParams::new();
    /// params = params.project("routeviews");
    /// ```
    pub fn project(self, project: &str) -> Self {
        QueryParams {
            project: Some(project.to_string()),
            ..self
        }
    }

    /// set searching for only data from specific collector,
    /// examples: `rrc00`, `route-views2`
    ///
    /// ```
    /// use bgpkit_broker::QueryParams;
    /// let mut params = QueryParams::new();
    /// params = params.collector_id("rrc00");
    /// ```
    pub fn collector_id(self, collector_id: &str) -> Self {
        QueryParams {
            collector_id: Some(collector_id.to_string()),
            ..self
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "cli", derive(tabled::Tabled, utoipa::ToSchema))]
pub struct BrokerCollector {
    pub id: i64,
    pub name: String,
    pub url: String,
    pub project: String,
    pub updates_interval: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "cli", derive(tabled::Tabled, utoipa::ToSchema))]
pub struct BrokerItemType {
    pub id: i64,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CollectorLatestResult {
    /// total number of items
    pub count: u32,

    /// array of [BrokerItem]
    pub data: Vec<BrokerItem>,
}

/// Query result struct that contains data or error message
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct BrokerQueryResult {
    /// number of items returned in **current** call
    pub count: Option<i64>,
    /// the page number of the current call
    pub page: Option<i64>,
    /// the number of items per page
    pub page_size: Option<i64>,
    /// Error message
    pub error: Option<String>,
    /// the returning data [Item]s
    pub data: Vec<BrokerItem>,
}

impl Display for BrokerQueryResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_param_to_string() {
        let param = QueryParams {
            ts_start: Some("1".to_string()),
            ts_end: Some("2".to_string()),
            collector_id: None,
            project: Some("test_project".to_string()),
            data_type: None,
            page: 1,
            page_size: 20,
            ..Default::default()
        };

        assert_eq!(
            "?ts_start=1&ts_end=2&project=test_project&page=1&page_size=20".to_string(),
            param.to_string()
        );

        let param = QueryParams {
            ts_start: None,
            ts_end: None,
            collector_id: None,
            project: None,
            data_type: None,
            page: 1,
            page_size: 20,
            ..Default::default()
        };

        assert_eq!("?page=1&page_size=20".to_string(), param.to_string());
    }
}
