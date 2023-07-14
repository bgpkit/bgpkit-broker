use crate::api::BrokerSearchResponse::BadRequestResponse;
use bgpkit_broker::{BrokerItem, LocalBrokerDb, DEFAULT_PAGE_SIZE};
use chrono::{Duration, NaiveDateTime};
use poem::listener::TcpListener;
use poem::middleware::AddData;
use poem::web::Data;
use poem::{handler, EndpointExt, Route, Server};
use poem_openapi::{param::Query, payload::Json, ApiResponse, Object, OpenApi, OpenApiService};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

struct BrokerAPI;

#[derive(Object, Serialize, Deserialize, Clone, Debug)]
struct BrokerSearchResult {
    count: usize,
    page: usize,
    page_size: usize,
    error: Option<String>,
    data: Vec<BrokerItem>,
}

#[derive(ApiResponse)]
enum BrokerSearchResponse {
    #[oai(status = 200)]
    SearchResponse(Json<BrokerSearchResult>),

    #[oai(status = 400)]
    BadRequestResponse(Json<BrokerSearchResult>),
}

#[OpenApi]
impl BrokerAPI {
    /// Search MRT files meta data from BGPKIT Broker database
    #[allow(clippy::too_many_arguments)]
    #[oai(path = "/search", method = "get")]
    async fn search(
        &self,
        /// Start timestamp
        ts_start: Query<Option<String>>,

        /// End timestamp
        ts_end: Query<Option<String>>,

        /// Human readable time duration string before `ts_end` or after `ts_start`, e.g. `1h`, `1d`, `1w`, `1m`, `1y`,
        duration: Query<Option<String>>,

        /// filter by route collector projects, i.e. `route-views` or `riperis`
        project: Query<Option<String>>,

        /// filter by collector IDs, e.g. `rrc00`, `route-views2`. use comma to separate multiple collectors
        collector_id: Query<Option<String>>,

        /// filter by data types, i.e. `update`, `rib`.
        data_type: Query<Option<String>>,

        /// page number, default to 1
        page: Query<Option<usize>>,

        /// page size
        page_size: Query<Option<usize>>,

        database: Data<&LocalBrokerDb>,
    ) -> BrokerSearchResponse {
        let (page, page_size) = (page.unwrap_or(1), page_size.unwrap_or(DEFAULT_PAGE_SIZE));
        if page == 0 {
            return BadRequestResponse(Json(BrokerSearchResult {
                count: 0,
                page,
                page_size,
                error: Some("page number start from 1".to_string()),
                data: vec![],
            }));
        }

        let mut ts_start = ts_start.0.map(|s| parse_time_str(s.as_str()).unwrap());
        let mut ts_end = ts_end.0.map(|s| parse_time_str(s.as_str()).unwrap());

        match (ts_start, ts_end) {
            (Some(start), None) => {
                if let Some(duration_str) = duration.0 {
                    match humantime::parse_duration(duration_str.as_str()) {
                        Ok(d) => {
                            ts_end = Some(start + Duration::from_std(d).unwrap());
                        }
                        Err(_) => {
                            return BadRequestResponse(Json(BrokerSearchResult {
                                count: 0,
                                page,
                                page_size,
                                error: Some(format!(
                                    "cannot parse time duration string: {}",
                                    duration_str
                                )),
                                data: vec![],
                            }))
                        }
                    }
                }
            }
            (None, Some(end)) => {
                if let Some(duration_str) = duration.0 {
                    match humantime::parse_duration(duration_str.as_str()) {
                        Ok(d) => {
                            ts_start = Some(end - Duration::from_std(d).unwrap());
                        }
                        Err(_) => {
                            return BadRequestResponse(Json(BrokerSearchResult {
                                count: 0,
                                page,
                                page_size,
                                error: Some(format!(
                                    "cannot parse time duration string: {}",
                                    duration_str
                                )),
                                data: vec![],
                            }))
                        }
                    }
                }
            }
            _ => {}
        };

        let collectors = collector_id
            .0
            .map(|s| s.split(',').map(|s| s.trim().to_string()).collect());

        let items = database
            .search_items(
                collectors,
                project.0,
                data_type.0,
                ts_start,
                ts_end,
                Some(page),
                Some(page_size),
            )
            .unwrap();

        BrokerSearchResponse::SearchResponse(Json(BrokerSearchResult {
            count: items.len(),
            page,
            page_size,
            error: None,
            data: items,
        }))
    }

    /// Get the latest MRT files meta information
    #[allow(clippy::too_many_arguments)]
    #[oai(path = "/latest", method = "get")]
    async fn latest(&self, database: Data<&LocalBrokerDb>) -> BrokerSearchResponse {
        let items = database.get_latest_items().unwrap();

        BrokerSearchResponse::SearchResponse(Json(BrokerSearchResult {
            count: items.len(),
            page: 0,
            page_size: items.len(),
            error: None,
            data: items,
        }))
    }
}

/// Parse timestamp string into NaiveDateTime
///
/// The timestamp string can be either unix timestamp or RFC3339 format string (e.g. 2020-01-01T00:00:00Z).
fn parse_time_str(ts_str: &str) -> Result<NaiveDateTime, String> {
    let ts = if let Ok(ts_end) = ts_str.parse::<i64>() {
        // it's unix timestamp
        NaiveDateTime::from_timestamp_opt(ts_end, 0).unwrap()
    } else {
        match NaiveDateTime::from_str(ts_str) {
            Ok(t) => t,
            Err(_) => {
                return Err(format!(
                    "Invalid timestamp format: {}, should be either unix timestamp or RFC3339",
                    ts_str
                ))
            }
        }
    };
    Ok(ts)
}

#[handler]
async fn search(database: Data<&LocalBrokerDb>) -> Json<serde_json::Value> {
    let ts = database.get_latest_timestamp().unwrap().unwrap();

    Json(serde_json::json!({ "ts": ts }))
}

pub async fn start_api_service(database: LocalBrokerDb, socket_addr: &str) -> std::io::Result<()> {
    let api_service = OpenApiService::new(BrokerAPI, "BGPKIT Broker", "3.0.0").server("/api");
    let ui = api_service.swagger_ui();

    let route = Route::new()
        .nest("/api", api_service)
        .nest("/", ui)
        .with(AddData::new(database));

    Server::new(TcpListener::bind(socket_addr)).run(route).await
}
