use crate::api::BrokerSearchResponse::BadRequestResponse;
use bgpkit_broker::{BrokerItem, LocalBrokerDb, DEFAULT_PAGE_SIZE};
use chrono::{DateTime, Duration, NaiveDateTime};
use clap::Args;
use poem::listener::TcpListener;
use poem::middleware::{AddData, CatchPanic, Cors, Tracing};
use poem::web::Data;
use poem::{EndpointExt, Route, Server};
use poem_openapi::payload::Response;
use poem_openapi::{param::Query, payload::Json, ApiResponse, Object, OpenApi, OpenApiService};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

struct BrokerAPI;

#[derive(Object, Args, Debug, Serialize, Deserialize)]
pub struct BrokerSearchQuery {
    /// Start timestamp
    #[clap(short = 't', long)]
    pub ts_start: Option<String>,

    /// End timestamp
    #[clap(short = 'T', long)]
    pub ts_end: Option<String>,

    /// filter by route collector projects, i.e. `route-views` or `riperis`
    #[clap(short, long)]
    pub project: Option<String>,

    /// filter by collector IDs, e.g. 'rrc00', 'route-views2. use comma to separate multiple collectors
    #[clap(short, long)]
    pub collector_id: Option<String>,

    /// filter by data types, i.e. 'update', 'rib'.
    #[clap(short, long)]
    pub data_type: Option<String>,

    /// page number
    #[clap(long)]
    pub page: Option<usize>,

    /// page size
    #[clap(long)]
    pub page_size: Option<usize>,
}

#[derive(Object, Serialize, Deserialize, Clone, Debug)]
pub struct BrokerSearchResult {
    pub count: usize,
    pub page: usize,
    pub page_size: usize,
    pub error: Option<String>,
    pub data: Vec<BrokerItem>,
    pub meta: Option<Meta>,
}

#[derive(Object, Serialize, Deserialize, Clone, Debug)]
pub struct Meta {
    pub latest_update_ts: NaiveDateTime,
    pub latest_update_duration: i32,
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
                meta: None,
            }));
        }
        if page_size > 1000 {
            return BadRequestResponse(Json(BrokerSearchResult {
                count: 0,
                page,
                page_size,
                error: Some("maximum page size is 1000".to_string()),
                data: vec![],
                meta: None,
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
                                meta: None,
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
                                meta: None,
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

        let meta = database
            .get_latest_updates_meta()
            .unwrap()
            .map(|data| Meta {
                latest_update_ts: data.update_ts,
                latest_update_duration: data.update_duration,
            });
        BrokerSearchResponse::SearchResponse(Json(BrokerSearchResult {
            count: items.len(),
            page,
            page_size,
            error: None,
            data: items,
            meta,
        }))
    }

    /// Get the latest MRT files meta information
    #[oai(path = "/latest", method = "get")]
    async fn latest(&self, database: Data<&LocalBrokerDb>) -> BrokerSearchResponse {
        let items = database.get_latest_items().unwrap();
        let meta = database
            .get_latest_updates_meta()
            .unwrap()
            .map(|data| Meta {
                latest_update_ts: data.update_ts,
                latest_update_duration: data.update_duration,
            });

        BrokerSearchResponse::SearchResponse(Json(BrokerSearchResult {
            count: items.len(),
            page: 0,
            page_size: items.len(),
            error: None,
            data: items,
            meta,
        }))
    }

    /// check API and database health
    #[oai(path = "/health", method = "get", hidden = true)]
    async fn health(&self, database: Data<&LocalBrokerDb>) -> Response<Json<Value>> {
        match database.get_latest_timestamp() {
            Ok(data) => match data {
                None => Response::new(Json(
                    json!({"status": "error", "message": "database not bootstrapped", "meta": {}}),
                ))
                .status(StatusCode::SERVICE_UNAVAILABLE),
                Some(ts) => {
                    // data is there, service is ok.
                    // this endpoint does not check for data freshness, as there are applications
                    // that does not require fresh data (e.g. historical analysis).
                    Response::new(Json(
                        json!({"status": "OK", "message": "database is healthy", "meta": {
                            "latest_file_ts": ts.timestamp(),
                        }}),
                    ))
                    .status(StatusCode::OK)
                }
            },
            Err(_) => Response::new(Json(
                json!({"status": "error", "message": "database connection error", "meta": {}}),
            ))
            .status(StatusCode::INTERNAL_SERVER_ERROR),
        }
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
        let ts_str = ts_str.trim_end_matches('Z').to_string() + "+00:00";
        match DateTime::parse_from_rfc3339(ts_str.as_str()) {
            Ok(t) => t.naive_utc(),
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

pub async fn start_api_service(
    database: LocalBrokerDb,
    host: String,
    port: u16,
    root: String,
) -> std::io::Result<()> {
    let api_service = OpenApiService::new(BrokerAPI, "BGPKIT Broker", "3.0.0").server(root);
    let ui = api_service.swagger_ui();

    let route = Route::new()
        .nest("/", api_service)
        .nest("/docs", ui)
        .with(Tracing)
        .with(Cors::new())
        .with(AddData::new(database))
        .with(CatchPanic::new());

    let socket_addr_str = format!("{}:{}", host, port);

    Server::new(TcpListener::bind(socket_addr_str))
        .run(route)
        .await
}
