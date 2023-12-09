use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use bgpkit_broker::{BrokerItem, LocalBrokerDb, DEFAULT_PAGE_SIZE};
use chrono::{DateTime, Duration, NaiveDateTime};
use clap::Args;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

struct AppState {
    database: LocalBrokerDb,
}

#[derive(IntoParams, Args, Debug, Serialize, Deserialize)]
pub struct BrokerSearchQuery {
    /// Start timestamp
    #[clap(short = 't', long)]
    pub ts_start: Option<String>,

    /// End timestamp
    #[clap(short = 'T', long)]
    pub ts_end: Option<String>,

    /// Duration string, e.g. 1 hour
    #[clap(short = 'd', long)]
    pub duration: Option<String>,

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

#[derive(ToSchema, Serialize, Deserialize, Clone, Debug)]
pub struct BrokerSearchResult {
    pub count: usize,
    pub page: usize,
    pub page_size: usize,
    pub error: Option<String>,
    pub data: Vec<BrokerItem>,
    pub meta: Option<Meta>,
}

#[derive(Serialize, Deserialize, ToSchema)]
enum BrokerApiError {
    #[schema(example = "database not bootstrap")]
    BrokerNotHealthy(String),
    #[schema(example = "page must start from 1")]
    SearchError(String),
}

#[derive(ToSchema, Serialize, Deserialize, Clone, Debug)]
pub struct Meta {
    pub latest_update_ts: NaiveDateTime,
    pub latest_update_duration: i32,
}

/// Search MRT files meta data from BGPKIT Broker database
#[utoipa::path(
    get,
    path = "/search",
    params(
        BrokerSearchQuery
    ),
    tag = "api",
    responses(
        (status = 200, description = "List matching todos by query", body = BrokerSearchResult),
        (status = 400, description = "Bad request", body = BrokerApiError, example = json!(BrokerApiError::SearchError("page must start from 1".to_string()))),
    )
)]
async fn search(
    query: Query<BrokerSearchQuery>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let (page, page_size) = (
        query.page.unwrap_or(1),
        query.page_size.unwrap_or(DEFAULT_PAGE_SIZE),
    );
    if page == 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(BrokerApiError::SearchError(
                "page number start from 1".to_string(),
            )),
        )
            .into_response();
    }
    if page_size > 1000 {
        return (
            StatusCode::BAD_REQUEST,
            Json(BrokerApiError::SearchError(
                "maximum page size is 1000".to_string(),
            )),
        )
            .into_response();
    }

    let mut ts_start = query
        .ts_start
        .as_ref()
        .map(|s| parse_time_str(s.as_str()).unwrap());
    let mut ts_end = query
        .ts_end
        .as_ref()
        .map(|s| parse_time_str(s.as_str()).unwrap());

    match (ts_start, ts_end) {
        (Some(start), None) => {
            if let Some(duration_str) = &query.duration {
                match humantime::parse_duration(duration_str.as_str()) {
                    Ok(d) => {
                        ts_end = Some(start + Duration::from_std(d).unwrap());
                    }
                    Err(_) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(BrokerApiError::SearchError(format!(
                                "cannot parse time duration string: {}",
                                duration_str
                            ))),
                        )
                            .into_response();
                    }
                }
            }
        }
        (None, Some(end)) => {
            if let Some(duration_str) = &query.duration {
                match humantime::parse_duration(duration_str.as_str()) {
                    Ok(d) => {
                        ts_start = Some(end - Duration::from_std(d).unwrap());
                    }
                    Err(_) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(BrokerApiError::SearchError(format!(
                                "cannot parse time duration string: {}",
                                duration_str
                            ))),
                        )
                            .into_response();
                    }
                }
            }
        }
        _ => {}
    };

    let collectors = query
        .collector_id
        .as_ref()
        .map(|s| s.split(',').map(|s| s.trim().to_string()).collect());

    let items = state
        .database
        .search(
            collectors,
            query.project.clone(),
            query.data_type.clone(),
            ts_start,
            ts_end,
            Some(page),
            Some(page_size),
        )
        .await
        .unwrap();

    let meta = state
        .database
        .get_latest_updates_meta()
        .await
        .unwrap()
        .map(|data| Meta {
            latest_update_ts: chrono::NaiveDateTime::from_timestamp_opt(data.update_ts, 0).unwrap(),
            latest_update_duration: data.update_duration,
        });

    Json(BrokerSearchResult {
        count: items.len(),
        page,
        page_size,
        error: None,
        data: items,
        meta,
    })
    .into_response()
}

/// Get the latest MRT files meta information
#[utoipa::path(
    get,
    path = "/latest",
    tag = "api",
    params(),
    responses(
        (status = 200, description = "Latest MRT files available for all collectors", body = BrokerSearchResult),
    )
)]
async fn latest(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let items = state.database.get_latest_files().await;
    let meta = state
        .database
        .get_latest_updates_meta()
        .await
        .unwrap()
        .map(|data| Meta {
            latest_update_ts: chrono::NaiveDateTime::from_timestamp_opt(data.update_ts, 0).unwrap(),
            latest_update_duration: data.update_duration,
        });

    Json(BrokerSearchResult {
        count: items.len(),
        page: 0,
        page_size: items.len(),
        error: None,
        data: items,
        meta,
    })
}

/// Return Broker API and database health
#[utoipa::path(
    get,
    path = "/health",
    tag = "metrics",
    params(),
    responses(
        (status = 200, description = "API and database is healthy"),
        (status = 503, description = "Database not available", body = BrokerApiError, example = json!(BrokerApiError::BrokerNotHealthy("database not bootstrap".to_string()))),
    )
)]
async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.database.get_latest_timestamp().await {
        Ok(data) => match data {
            None => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(BrokerApiError::BrokerNotHealthy(
                    "database not bootstrap".to_string(),
                )),
            )
                .into_response(),
            Some(ts) => {
                // data is there, service is ok.
                // this endpoint does not check for data freshness, as there are applications
                // that does not require fresh data (e.g. historical analysis).
                Json(
                    json!({"status": "OK", "message": "database is healthy", "meta": {
                        "latest_file_ts": ts.timestamp(),
                    }}),
                )
                .into_response()
            }
        },
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(BrokerApiError::BrokerNotHealthy(
                "database connection error".to_string(),
            )),
        )
            .into_response(),
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
    #[derive(OpenApi)]
    #[openapi(
        info(
            title = "BGPKIT Broker API",
            description = "BGPKIT Broker provides RESTful API for querying MRT files meta data across RouteViews and RIPE RIS collectors."
        ),
        paths(
            search,
            latest,
            health,
        ),
        components(
            schemas(BrokerSearchResult, BrokerItem, Meta, BrokerApiError)
        ),
        tags(
            (name = "api", description = "API for BGPKIT Broker"),
            (name = "metrics", description = "Metrics for BGPKIT Broker"),
        )
    )]
    struct ApiDoc;

    let database = Arc::new(AppState { database });
    let app = Router::new()
        .merge(SwaggerUi::new("/docs").url("/openapi.json", ApiDoc::openapi()))
        .route("/search", get(search))
        .route("/latest", get(latest))
        .route("/health", get(health))
        .with_state(database);
    let root_app = Router::new().nest(root.as_str(), app);

    let socket_str = format!("{}:{}", host, port);
    let listener = tokio::net::TcpListener::bind(socket_str).await?;
    tracing::info!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, root_app).await.unwrap();

    Ok(())
}
