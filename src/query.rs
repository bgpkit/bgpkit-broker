//! Query-related structs and implementation.
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

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
    /// archive data type: `rib` or `update`
    pub data_type: Option<String>,
    /// page number to seek to, starting from 1, default to 1
    pub page: i64,
    /// number of items each page contains, default to 10, max to 100000
    pub page_size: i64,
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
    /// - `update`: BGP updates files
    /// without specifying data type, it defaults to search for all types
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

/// BGPKIT Broker data item.
///
/// The fields are:
/// - [ts_start][BrokerItem::ts_start]: the starting timestamp of the data file
/// - [ts_end][BrokerItem::ts_end]: the ending timestamp of the data file
/// - [collector_id][BrokerItem::collector_id]: the collector id of the item: e.g. `rrc00`
/// - [data_type][BrokerItem::data_type]: type of the data item: `rib` or `update`
/// - [url][BrokerItem::url]: the URL to the data item file
/// - [rough_size][BrokerItem::rough_size]: rough file size extracted from the collector webpage
/// - [exact_size][BrokerItem::exact_size]: exact file size extracted by crawling the file
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "cli", derive(tabled::Tabled, poem_openapi::Object))]
pub struct BrokerItem {
    /// start timestamp
    pub ts_start: chrono::NaiveDateTime,
    /// end timestamps
    pub ts_end: chrono::NaiveDateTime,
    /// the collector id of the item: e.g. `rrc00`
    pub collector_id: String,
    /// type of the data item: `rib` or `update`
    pub data_type: String,
    /// the URL to the data item file
    pub url: String,
    /// rough file size extracted from the hosting site page
    pub rough_size: i64,
    /// exact file size extracted by crawling the file
    pub exact_size: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CollectorLatestResult {
    /// total number of items
    pub count: u32,

    /// array of [CollectorLatestItem]
    pub data: Vec<CollectorLatestItem>,
}

/// BGPKIT Broker collector latest data item.
///
/// The fields are:
/// - [timestamp][CollectorLatestItem::timestamp]: the data timestamp of the file
/// - [delay][CollectorLatestItem::delay]: the delay in seconds between the time file available and the timestamp of the file
/// - [collector_id][CollectorLatestItem::collector_id]: the collector id of the item: e.g. `rrc00`
/// - [data_type][CollectorLatestItem::data_type]: type of the data item: `rib` or `update`
/// - [item_url][CollectorLatestItem::item_url]: the URL to the data item file
/// - [collector_url][CollectorLatestItem::collector_url]: the URL to the data item file
/// - [rough_size][CollectorLatestItem::rough_size]: rough file size extracted from the collector webpage
/// - [exact_size][CollectorLatestItem::exact_size]: exact file size extracted by crawling the file
#[derive(Debug, Serialize, Deserialize)]
pub struct CollectorLatestItem {
    /// timestamp of the file
    pub timestamp: chrono::NaiveDateTime,
    /// Delay in seconds between the time file available and the timestamp of the file
    pub delay: f64,
    /// the collector id of the item: e.g. `rrc00`
    pub collector_id: String,
    /// type of the data item: `rib` or `update`
    pub data_type: String,
    /// the URL to the data item file
    pub item_url: String,
    /// the URL to the route collector
    pub collector_url: String,
    /// rough file size extracted from the hosting site page
    pub rough_size: i64,
    /// exact file size extracted by crawling the file
    pub exact_size: i64,
}

/// Query result struct that contains data or error message
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct QueryResult {
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

impl Display for BrokerItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}

impl Display for CollectorLatestItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}

impl Display for QueryResult {
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
        };

        assert_eq!("?page=1&page_size=20".to_string(), param.to_string());
    }
}
