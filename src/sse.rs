use crate::{build_async_client, BgpkitBroker, BrokerError, BrokerItem};
use futures_util::stream::{self, BoxStream};
use futures_util::{Stream, StreamExt};
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Clone, Debug, Default)]
pub struct SseSubscriptionOptions {
    project: Option<String>,
    collector_id: Option<String>,
    data_type: Option<String>,
}

impl SseSubscriptionOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn project<S: AsRef<str>>(mut self, project: S) -> Self {
        self.project = Some(project.as_ref().to_string());
        self
    }

    pub fn collector_id<S: AsRef<str>>(mut self, collector_id: S) -> Self {
        self.collector_id = Some(collector_id.as_ref().to_string());
        self
    }

    pub fn data_type<S: AsRef<str>>(mut self, data_type: S) -> Self {
        self.data_type = Some(data_type.as_ref().to_string());
        self
    }

    fn normalize(&self) -> Result<NormalizedSseFilters, BrokerError> {
        Ok(NormalizedSseFilters {
            project: self.project.as_deref().map(normalize_project).transpose()?,
            collector_ids: self
                .collector_id
                .as_deref()
                .map(normalize_collectors)
                .transpose()?,
            data_type: self
                .data_type
                .as_deref()
                .map(normalize_data_type)
                .transpose()?,
        })
    }
}

#[derive(Clone, Debug, Default)]
struct NormalizedSseFilters {
    project: Option<String>,
    collector_ids: Option<HashSet<String>>,
    data_type: Option<String>,
}

impl NormalizedSseFilters {
    fn matches(&self, item: &BrokerItem, collector_project_map: &HashMap<String, String>) -> bool {
        if let Some(project) = &self.project {
            let item_project = collector_project_map
                .get(&item.collector_id)
                .map(String::as_str)
                .unwrap_or_default();
            if item_project != project {
                return false;
            }
        }

        if let Some(data_type) = &self.data_type {
            match data_type.as_str() {
                "rib" if !item.is_rib() => return false,
                "updates" if item.is_rib() => return false,
                _ => {}
            }
        }

        if let Some(collector_ids) = &self.collector_ids {
            if !collector_ids.contains(item.collector_id.as_str()) {
                return false;
            }
        }

        true
    }
}

pub struct BrokerItemSubscription {
    inner: BoxStream<'static, Result<BrokerItem, BrokerError>>,
}

impl BrokerItemSubscription {
    fn new(inner: BoxStream<'static, Result<BrokerItem, BrokerError>>) -> Self {
        Self { inner }
    }
}

impl Stream for BrokerItemSubscription {
    type Item = Result<BrokerItem, BrokerError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next_unpin(cx)
    }
}

impl BgpkitBroker {
    /// Subscribe to live new-file notifications from the broker SSE endpoint.
    ///
    /// This endpoint is live-only and yields newly indexed files produced by the current broker
    /// process. Client-side filters are applied after events are received.
    ///
    /// ```no_run
    /// use bgpkit_broker::{BgpkitBroker, SseSubscriptionOptions};
    /// use futures_util::StreamExt;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let broker = BgpkitBroker::new().broker_url("http://127.0.0.1:40064/v3/broker");
    ///     let options = SseSubscriptionOptions::new()
    ///         .project("routeviews")
    ///         .collector_id("route-views2")
    ///         .data_type("updates");
    ///
    ///     let mut subscription = broker.subscribe_new_files(options).await?;
    ///
    ///     while let Some(item) = subscription.next().await {
    ///         println!("{}", item?.url);
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn subscribe_new_files(
        &self,
        options: SseSubscriptionOptions,
    ) -> Result<BrokerItemSubscription, BrokerError> {
        let filters = options.normalize()?;
        let client = build_async_client(self.accept_invalid_certs)?;
        let url = format!("{}/events", self.broker_url.trim_end_matches('/'));
        let response = client.get(url.as_str()).send().await.map_err(|e| {
            BrokerError::SseError(format!("Unable to connect to the URL ({url}): {e}"))
        })?;

        validate_sse_status(response.status(), url.as_str())?;

        let state = SubscriptionState {
            chunks: Box::pin(
                response
                    .bytes_stream()
                    .map(|result| result.map(|bytes| bytes.to_vec())),
            ),
            buffer: Vec::new(),
            filters,
            collector_project_map: self.collector_project_map.clone(),
            close_reported: false,
        };

        Ok(BrokerItemSubscription::new(Box::pin(stream::unfold(
            state,
            next_subscription_item,
        ))))
    }
}

type ByteStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>, reqwest::Error>> + Send>>;

struct SubscriptionState {
    chunks: ByteStream,
    buffer: Vec<u8>,
    filters: NormalizedSseFilters,
    collector_project_map: HashMap<String, String>,
    close_reported: bool,
}

async fn next_subscription_item(
    mut state: SubscriptionState,
) -> Option<(Result<BrokerItem, BrokerError>, SubscriptionState)> {
    if state.close_reported {
        return None;
    }

    loop {
        if let Some(event_bytes) = extract_next_event(&mut state.buffer) {
            match parse_event(&event_bytes) {
                Ok(Some(item)) => {
                    if state.filters.matches(&item, &state.collector_project_map) {
                        return Some((Ok(item), state));
                    }
                }
                Ok(None) => continue,
                Err(err) => {
                    state.close_reported = true;
                    return Some((Err(err), state));
                }
            }
        }

        match state.chunks.next().await {
            Some(Ok(chunk)) => state.buffer.extend_from_slice(&chunk),
            Some(Err(err)) => {
                state.close_reported = true;
                return Some((
                    Err(BrokerError::SseError(format!(
                        "Failed to read SSE response body: {}",
                        err
                    ))),
                    state,
                ));
            }
            None => {
                state.close_reported = true;
                return Some((
                    Err(BrokerError::SseError(
                        "SSE stream closed by remote peer".to_string(),
                    )),
                    state,
                ));
            }
        }
    }
}

fn validate_sse_status(status: reqwest::StatusCode, url: &str) -> Result<(), BrokerError> {
    if status.is_success() {
        Ok(())
    } else {
        Err(BrokerError::SseError(format!(
            "Unexpected SSE response status {} from {}",
            status, url
        )))
    }
}

fn extract_next_event(buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    let (event_end, delimiter_len) = find_event_boundary(buffer)?;
    let event = buffer[..event_end].to_vec();
    buffer.drain(..event_end + delimiter_len);
    Some(event)
}

fn find_event_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
    let mut idx = 0;
    while idx < buffer.len() {
        if buffer[idx..].starts_with(b"\r\n\r\n") {
            return Some((idx, 4));
        }
        if buffer[idx..].starts_with(b"\n\n") || buffer[idx..].starts_with(b"\r\r") {
            return Some((idx, 2));
        }
        idx += 1;
    }
    None
}

fn parse_event(event_bytes: &[u8]) -> Result<Option<BrokerItem>, BrokerError> {
    let event_text = std::str::from_utf8(event_bytes)
        .map_err(|err| BrokerError::SseError(format!("Invalid SSE payload encoding: {}", err)))?;

    let mut event_type = None;
    let mut event_id = None;
    let mut data_lines = Vec::new();
    let mut has_fields = false;

    for raw_line in event_text.lines() {
        let line = raw_line.trim_end_matches('\r');
        if line.is_empty() || line.starts_with(':') {
            continue;
        }

        has_fields = true;

        if let Some(rest) = line.strip_prefix("event:") {
            event_type = Some(rest.trim_start().to_string());
            continue;
        }
        if let Some(rest) = line.strip_prefix("id:") {
            event_id = Some(rest.trim_start().to_string());
            continue;
        }
        if let Some(rest) = line.strip_prefix("data:") {
            data_lines.push(rest.trim_start().to_string());
        }
    }

    if !has_fields || data_lines.is_empty() {
        return Ok(None);
    }

    let event_type = event_type
        .ok_or_else(|| BrokerError::SseError("SSE event missing event type".to_string()))?;
    if event_type != "new_file" {
        return Err(BrokerError::SseError(format!(
            "Unexpected SSE event type: {}",
            event_type
        )));
    }

    let item: BrokerItem = serde_json::from_str(&data_lines.join("\n")).map_err(|err| {
        BrokerError::SseError(format!("Failed to deserialize SSE event data: {}", err))
    })?;

    if let Some(id) = event_id {
        if id != item.url {
            return Err(BrokerError::SseError(
                "SSE event id does not match BrokerItem url".to_string(),
            ));
        }
    }

    Ok(Some(item))
}

fn normalize_collectors(collector_str: &str) -> Result<HashSet<String>, BrokerError> {
    let mut collectors = HashSet::new();
    for collector in collector_str
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        collectors.insert(collector.to_string());
    }

    if collectors.is_empty() {
        return Err(BrokerError::ConfigurationError(
            "Collector ID cannot be empty".to_string(),
        ));
    }

    Ok(collectors)
}

fn normalize_project(project: &str) -> Result<String, BrokerError> {
    match project.to_lowercase().as_str() {
        "rrc" | "riperis" | "ripe_ris" => Ok("riperis".to_string()),
        "routeviews" | "route_views" | "rv" => Ok("routeviews".to_string()),
        _ => Err(BrokerError::ConfigurationError(format!(
            "Invalid project '{}'. Valid projects are: 'riperis' (aliases: 'rrc', 'ripe_ris') or 'routeviews' (aliases: 'route_views', 'rv')",
            project
        ))),
    }
}

fn normalize_data_type(data_type: &str) -> Result<String, BrokerError> {
    match data_type.to_lowercase().as_str() {
        "rib" | "ribs" | "r" => Ok("rib".to_string()),
        "update" | "updates" => Ok("updates".to_string()),
        _ => Err(BrokerError::ConfigurationError(format!(
            "Invalid data type '{}'. Valid data types are: 'rib' (aliases: 'ribs', 'r') or 'updates' (alias: 'update')",
            data_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::DEFAULT_COLLECTORS_CONFIG;
    use chrono::DateTime;
    use futures_util::StreamExt;

    fn test_item(collector_id: &str, data_type: &str, url: &str) -> BrokerItem {
        BrokerItem {
            ts_start: DateTime::from_timestamp(1_710_000_000, 0)
                .unwrap()
                .naive_utc(),
            ts_end: DateTime::from_timestamp(1_710_000_300, 0)
                .unwrap()
                .naive_utc(),
            collector_id: collector_id.to_string(),
            data_type: data_type.to_string(),
            url: url.to_string(),
            rough_size: 100,
            exact_size: 100,
        }
    }

    fn test_subscription(
        chunks: Vec<String>,
        options: SseSubscriptionOptions,
    ) -> BrokerItemSubscription {
        let state = SubscriptionState {
            chunks: Box::pin(stream::iter(
                chunks
                    .into_iter()
                    .map(|chunk| Ok::<Vec<u8>, reqwest::Error>(chunk.into_bytes())),
            )),
            buffer: Vec::new(),
            filters: options.normalize().unwrap(),
            collector_project_map: DEFAULT_COLLECTORS_CONFIG.clone().to_project_map(),
            close_reported: false,
        };

        BrokerItemSubscription::new(Box::pin(stream::unfold(state, next_subscription_item)))
    }

    #[tokio::test]
    async fn test_subscribe_new_files_yields_items() {
        let item = test_item("route-views2", "updates", "https://example.com/updates");
        let payload = serde_json::to_string(&item).unwrap();
        let body = format!("event: new_file\nid: {}\ndata: {}\n\n", item.url, payload);
        let mut subscription = test_subscription(vec![body], SseSubscriptionOptions::new());

        let received = subscription.next().await.unwrap().unwrap();
        assert_eq!(received, item);
        let closed = subscription.next().await.unwrap();
        assert!(matches!(closed, Err(BrokerError::SseError(_))));
    }

    #[tokio::test]
    async fn test_subscribe_new_files_ignores_keepalive_comments() {
        let item = test_item("route-views2", "updates", "https://example.com/keepalive");
        let payload = serde_json::to_string(&item).unwrap();
        let mut subscription = test_subscription(
            vec![
                ": keepalive\n\n".to_string(),
                format!("event: new_file\nid: {}\ndata: {}\n\n", item.url, payload),
            ],
            SseSubscriptionOptions::new(),
        );

        let received = subscription.next().await.unwrap().unwrap();
        assert_eq!(received.url, item.url);
    }

    #[tokio::test]
    async fn test_subscribe_new_files_applies_client_side_filters() {
        let filtered_out = test_item("rrc00", "rib", "https://example.com/rib");
        let expected = test_item("route-views2", "updates", "https://example.com/updates");
        let mut subscription = test_subscription(
            vec![
                format!(
                    "event: new_file\nid: {}\ndata: {}\n\n",
                    filtered_out.url,
                    serde_json::to_string(&filtered_out).unwrap()
                ),
                format!(
                    "event: new_file\nid: {}\ndata: {}\n\n",
                    expected.url,
                    serde_json::to_string(&expected).unwrap()
                ),
            ],
            SseSubscriptionOptions::new()
                .project("routeviews")
                .collector_id("route-views2")
                .data_type("updates"),
        );

        let received = subscription.next().await.unwrap().unwrap();
        assert_eq!(received, expected);
    }

    #[tokio::test]
    async fn test_subscribe_new_files_rejects_invalid_json() {
        let mut subscription = test_subscription(
            vec!["event: new_file\nid: bad\ndata: {bad json}\n\n".to_string()],
            SseSubscriptionOptions::new(),
        );

        let err = subscription.next().await.unwrap().unwrap_err();
        assert!(matches!(err, BrokerError::SseError(_)));
    }

    #[tokio::test]
    async fn test_subscribe_new_files_rejects_wrong_event_type() {
        let item = test_item("route-views2", "updates", "https://example.com/wrong-type");
        let mut subscription = test_subscription(
            vec![format!(
                "event: snapshot\nid: {}\ndata: {}\n\n",
                item.url,
                serde_json::to_string(&item).unwrap()
            )],
            SseSubscriptionOptions::new(),
        );

        let err = subscription.next().await.unwrap().unwrap_err();
        assert!(matches!(err, BrokerError::SseError(_)));
    }

    #[test]
    fn test_subscribe_new_files_rejects_non_200_response() {
        let err = validate_sse_status(
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            "http://127.0.0.1:40064/v3/broker/events",
        )
        .unwrap_err();
        assert!(matches!(err, BrokerError::SseError(_)));
    }
}
