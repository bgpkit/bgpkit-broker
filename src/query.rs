//! Query-related structs and implementation.
use std::fmt::{Display, Formatter};
use serde::{Serialize, Deserialize};

/// QueryParams represents the query parameters to the backend API.
///
/// Example for constructing a QueryParams:
/// ```
/// use bgpkit_broker::QueryParams;
/// let mut params = QueryParams::new();
/// params = params.start_ts(1633046400);
/// params = params.end_ts(1633132800);
/// params = params.collector("rrc00");
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
    /// start unix timestamp: files with time after or equals to `start_ts` will match
    pub start_ts: Option<i64>,
    /// end unix timestamp: files with time before or equals to `end_ts` will match
    pub end_ts: Option<i64>,
    /// collector identifier, e.g. `rrc00` or `route-views2`
    pub collector: Option<String>,
    /// archive project name: `riperis` or `routeviews`
    pub project: Option<String>,
    /// archive data type: `rib` or `update`
    pub data_type: Option<String>,
    /// sort order by time: `desc` or `asc`, see [SortOrder]
    pub order: SortOrder,
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
    DESC
}

/// Default [QueryParams] values
impl Default for QueryParams {
    fn default() -> Self {
        QueryParams{
            start_ts: None,
            end_ts: None,
            collector: None,
            project: None,
            data_type: None,
            order: SortOrder::ASC,
            page: 1,
            page_size: 10
        }
    }
}

impl Display for SortOrder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SortOrder::ASC => {write!(f, "asc")}
            SortOrder::DESC => {write!(f, "desc")}
        }
    }
}

impl std::fmt::Display for QueryParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut params_vec = vec![];
        if let Some(v) = &self.start_ts {
            params_vec.push(format!("start_ts={}", v));
        }
        if let Some(v) = &self.end_ts {
            params_vec.push(format!("end_ts={}", v));
        }
        if let Some(v) = &self.collector {
            params_vec.push(format!("collector={}", v));
        }
        if let Some(v) = &self.project {
            params_vec.push(format!("project={}", v));
        }
        if let Some(v) = &self.data_type {
            params_vec.push(format!("data_type={}", v));
        }
        params_vec.push(format!("order={}", self.order));
        params_vec.push(format!("page={}", self.page));
        params_vec.push(format!("page_size={}", self.page_size));

        if params_vec.len()>0 {
            write!(f, "?{}", params_vec.join("&"))
        } else {
            write!(f, "")
        }
    }
}

impl QueryParams {
    pub fn new() -> QueryParams {
        QueryParams{
            start_ts: None,
            end_ts: None,
            collector: None,
            project: None,
            data_type: None,
            order: SortOrder::ASC,
            page: 1,
            page_size: 10
        }
    }

    /// set starting timestamp for the search and returns a new [QueryParams] object.
    ///
    /// ```
    /// use bgpkit_broker::QueryParams;
    /// let mut params = QueryParams::new();
    /// params = params.start_ts(1633046400);
    /// ```
    pub fn start_ts(self, start_ts:i64) -> Self {
        QueryParams{ start_ts: Some(start_ts), ..self}
    }

    /// set ending timestamp for the search and returns a new [QueryParams] object.
    ///
    /// ```
    /// use bgpkit_broker::QueryParams;
    /// let mut params = QueryParams::new();
    /// params = params.end_ts(1633046400);
    /// ```
    pub fn end_ts(self, end_ts:i64) -> Self {
        QueryParams{ end_ts: Some(end_ts), ..self}
    }

    /// set page number for the each for pagination. **the page number starts from 1**.
    ///
    /// ```
    /// use bgpkit_broker::QueryParams;
    /// let mut params = QueryParams::new();
    /// params = params.page(3);
    /// ```
    pub fn page(self, page:i64) -> Self {
        QueryParams{ page, ..self}
    }

    /// set each page's size (number of items per page).
    ///
    /// ```
    /// use bgpkit_broker::QueryParams;
    /// let mut params = QueryParams::new();
    /// params = params.page_size(20);
    /// ```
    pub fn page_size(self, page_size:i64) -> Self {
        QueryParams{ page_size, ..self}
    }

    /// set return objects ordering in terms of timestamps:
    /// - `asc` for timestamps increasing order (default)
    /// - `desc` for timestamps decreasing order
    ///
    /// ```
    /// use bgpkit_broker::{QueryParams, SortOrder};
    /// let mut params = QueryParams::new();
    /// params = params.order(SortOrder::DESC);
    /// ```
    pub fn order(self, order:SortOrder) -> Self {
        QueryParams{ order, ..self}
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
    pub fn data_type(self, data_type:&str) -> Self {
        QueryParams{ data_type: Some(data_type.to_string()), ..self}
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
    pub fn project(self, project:&str) -> Self {
        QueryParams{ project: Some(project.to_string()), ..self}
    }

    /// set searching for only data from specific collector,
    /// examples: `rrc00`, `route-views2`
    ///
    /// ```
    /// use bgpkit_broker::QueryParams;
    /// let mut params = QueryParams::new();
    /// params = params.collector("rrc00");
    /// ```
    pub fn collector(self, collector:&str) -> Self {
        QueryParams{ collector: Some(collector.to_string()), ..self}
    }
}

/// BGPKIT Broker data item.
///
/// The fields are:
/// - [collector_id][BrokerItem::collector_id]: the collector id of the item: e.g. `rrc00`
/// - [timestamp][BrokerItem::timestamp]: the unitimestamp timestamp of the data file
/// - [data_type][BrokerItem::data_type]: type of the data item: `rib` or `update`
/// - [url][BrokerItem::url]: the URL to the data item file
#[derive(Debug, Serialize, Deserialize)]
pub struct BrokerItem {
    /// the collector id of the item: e.g. `rrc00`
    pub collector_id: String,
    /// the unix timestamp of the data file
    pub timestamp: i64,
    /// type of the data item: `rib` or `update`
    pub data_type: String,
    /// the URL to the data item file
    pub url: String,
}

/// a wrapper struct of the returning values that include some meta information.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DataWrapper {
    /// the returning data [Item]s
    pub items: Vec<BrokerItem>,
    /// number of items returned in **current** call
    pub count: i64,
    /// the page number of the current call
    pub current_page: i64,
    /// the number of items per page
    pub page_size: i64,
    /// total number of pages
    pub total_pages: i64,
}

/// Query result struct that contains data or error message
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct QueryResult {
    /// Option that contains [DataWrapper] if the search call is successful
    pub data: Option<DataWrapper>,
    /// Option that contains an error message if the search call failed
    pub error: Option<String>
}

#[cfg(test)]
mod tests {
    use crate::SortOrder::ASC;
    use super::*;

    #[test]
    fn test_param_to_string() {
        let param = QueryParams{
            start_ts: Some(1),
            end_ts: Some(2),
            collector: None,
            project: Some("test_project".to_string()),
            data_type: None,
            order: ASC,
            page: 1,
            page_size: 20
        };

        assert_eq!("?start_ts=1&end_ts=2&project=test_project&order=asc&page=1&page_size=20".to_string(), param.to_string());

        let param = QueryParams{
            start_ts: None,
            end_ts: None,
            collector: None,
            project: None,
            data_type: None,
            order: ASC,
            page: 1,
            page_size: 20
        };

        assert_eq!("?order=asc&page=1&page_size=20".to_string(), param.to_string());
    }
}