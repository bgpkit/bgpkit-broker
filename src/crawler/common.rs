use crate::BrokerError;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};
use regex::{Captures, Regex};
use scraper::{Html, Selector};
use std::time::Duration;
use tracing::{debug, warn};

/// Minimum valid timestamp for BGP data (1998-01-01).
/// The earliest collector (rrc00) started on 1999-10-01. 1998 provides a clear buffer
/// while distinguishing from Unix epoch (1970) and actual BGP data collection.
pub const MIN_VALID_TIMESTAMP: NaiveDateTime = match DateTime::from_timestamp(883612800, 0) {
    Some(dt) => dt.naive_utc(), // 1998-01-01 00:00:00 UTC
    None => panic!("Invalid MIN_VALID_TIMESTAMP"),
};

const SIZE_KB: u64 = u64::pow(1024, 1);
const SIZE_MB: u64 = u64::pow(1024, 2);
const SIZE_GB: u64 = u64::pow(1024, 3);

/// Get the maximum number of retry attempts for crawling.
/// Default is 3 attempts. Can be configured via BGPKIT_BROKER_CRAWLER_MAX_RETRIES.
pub(crate) fn get_crawler_max_retries() -> u32 {
    std::env::var("BGPKIT_BROKER_CRAWLER_MAX_RETRIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3)
}

/// Get the initial backoff duration in milliseconds.
/// Default is 1000ms (1 second). Can be configured via BGPKIT_BROKER_CRAWLER_BACKOFF_MS.
pub(crate) fn get_crawler_backoff_ms() -> u64 {
    std::env::var("BGPKIT_BROKER_CRAWLER_BACKOFF_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000)
}

/// Get the number of concurrent month crawls per collector.
///
/// This controls how many months are crawled in parallel for each collector.
/// Each month crawl involves fetching the month's directory listing and parsing
/// all MRT file entries.
///
/// **Note:** This setting is primarily relevant during bootstrap crawls when all
/// historical months need to be fetched. During regular updates, typically only
/// 1-2 months (current and possibly previous) are crawled, so this setting has
/// minimal impact.
///
/// Default is 2 concurrent months. Can be configured via BGPKIT_BROKER_CRAWLER_MONTH_CONCURRENCY.
///
/// Recommended values:
/// - 1-2: Conservative, suitable for avoiding rate limits
/// - 3-5: Balanced performance and server load
/// - 10+: Aggressive, may trigger rate limiting on some servers
pub(crate) fn get_crawler_month_concurrency() -> usize {
    std::env::var("BGPKIT_BROKER_CRAWLER_MONTH_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2)
}

fn size_str_to_bytes(size_str: &str, size_pattern: &Regex) -> Option<i64> {
    let cap: Captures = size_pattern.captures(size_str)?;
    let mut size = match cap[1].to_string().parse::<f64>() {
        Ok(x) => x,
        Err(_) => return None,
    };
    size *= match cap[2].to_ascii_lowercase().as_str() {
        "k" => SIZE_KB,
        "m" => SIZE_MB,
        "g" => SIZE_GB,
        "" => 1,
        other => panic!("unknown file size multiplier {}", other),
    } as f64;
    Some(size as i64)
}

/// Extract the MRT file links and file sizes from the body of a RouteViews or RIPE RIS page.
///
/// # Arguments
///
/// * `body`: RouteViews or RIPE RIS page body
///
/// returns: Vec<(String, i64)>
pub fn extract_link_size(body: &str) -> Vec<(String, i64)> {
    let mut res: Vec<(String, i64)> = vec![];

    if body.contains("table") {
        let size_pattern: Regex =
            Regex::new(r" *([\d.]+)([MKGmkg]*)").expect("invalid regex pattern");
        // table-based html pages, works with RouteViews and RIPE RIS old version
        let fragment = Html::parse_fragment(body);
        let row_selector = Selector::parse("tr").expect("invalid selector");
        let link_selector = Selector::parse("a").expect("invalid selector");
        for elem in fragment.select(&row_selector) {
            let text_arr = elem
                .text()
                .filter(|t| t.is_ascii() && !t.trim().is_empty())
                .collect::<Vec<_>>();
            let text = text_arr.join("");
            if text.is_empty() || text.contains("Name") || text.contains("Parent") {
                continue;
            }
            let href = match elem.select(&link_selector).next() {
                Some(e) => e.value().attr("href"),
                None => continue,
            };
            let size = match size_str_to_bytes(text_arr[2], &size_pattern) {
                None => continue,
                Some(v) => v,
            };
            if let Some(href_str) = href {
                res.push((href_str.to_string(), size));
            }
        }
    } else {
        let size_pattern: Regex =
            Regex::new(r" +([\d.]+)([MKGmkg]*)$").expect("invalid regex pattern");
        for line in body.lines() {
            let size = match size_str_to_bytes(line, &size_pattern) {
                Some(s) => s,
                None => continue,
            };

            let fragment = Html::parse_fragment(line);
            let link_selector = Selector::parse("a").expect("invalid selector");
            let mut link = "".to_string();
            if let Some(elem) = fragment.select(&link_selector).next() {
                if let Some(href) = elem.value().attr("href") {
                    link = href.to_string();
                }
            }
            if !link.is_empty() {
                res.push((link, size));
            }
        }
    }
    res
}

/// Base URL of the path-preserving fetch mirror, without a trailing slash.
///
/// Environment variable: `BGPKIT_BROKER_FETCH_PROXY` (for example
/// `https://m.bgpkit.com`). Unset means fetch origins directly.
pub(crate) fn get_fetch_proxy() -> Option<String> {
    let raw = std::env::var("BGPKIT_BROKER_FETCH_PROXY").ok()?;
    let base = raw.trim().trim_end_matches('/');
    if !(base.starts_with("http://") || base.starts_with("https://")) {
        // A set-but-malformed value would otherwise fall back to direct fetching without
        // a word, and the failures that follow look like origin trouble, not a bad setting.
        warn!("ignoring BGPKIT_BROKER_FETCH_PROXY: value is not an http(s) base URL");
        return None;
    }
    Some(base.to_string())
}

/// Origins the mirror may be used for.
///
/// Environment variable: `BGPKIT_BROKER_FETCH_PROXY_ORIGINS`, comma separated, default
/// `archive.routeviews.org`. Keep it to the origins whose listings are safe to fetch
/// through the mirror: agents serve a listing uncached (`X-Cache-Status: BYPASS` plus
/// `Cache-Control: no-store`), but an agent running an image older than that fix would
/// still cache the first copy for 30 days and freeze the listing.
pub(crate) fn get_fetch_proxy_origins() -> Vec<String> {
    match std::env::var("BGPKIT_BROKER_FETCH_PROXY_ORIGINS") {
        Ok(raw) => raw
            .split(',')
            .map(|part| part.trim().to_ascii_lowercase())
            .filter(|part| !part.is_empty())
            .collect(),
        Err(_) => vec!["archive.routeviews.org".to_string()],
    }
}

/// Rewrite an origin URL into the mirror's `<base>/<origin-host><path>` form.
///
/// Returns `None` for non-HTTP URLs, for URLs already pointing at the mirror, and for
/// origins outside the allowlist, so a caller can treat `None` as "fetch as-is".
pub(crate) fn mirror_url(base: &str, origins: &[String], url: &str) -> Option<String> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))?;
    // Already mirrored: do not nest the rewrite.
    let base_rest = base
        .strip_prefix("https://")
        .or_else(|| base.strip_prefix("http://"))?;
    let base_host = base_rest.split('/').next().unwrap_or_default();
    let (host, path) = match rest.find('/') {
        Some(idx) => rest.split_at(idx),
        None => (rest, ""),
    };
    if host.eq_ignore_ascii_case(base_host) {
        return None;
    }
    if !origins
        .iter()
        .any(|allowed| host.eq_ignore_ascii_case(allowed))
    {
        return None;
    }
    let base = base.trim_end_matches('/');
    Some(format!("{}/{}{}", base, host, path))
}

/// Fetch one URL with retry logic and exponential backoff.
async fn fetch_with_retries(
    client: &reqwest::Client,
    url: &str,
    max_retries: u32,
    initial_backoff_ms: u64,
) -> Result<String, BrokerError> {
    let mut last_error: Option<BrokerError> = None;

    for attempt in 0..max_retries {
        match client.get(url).send().await {
            Ok(response) => {
                let status = response.status();
                // A non-2xx body is not data: the mirror answers rejections (403 for
                // non-allowlisted origins, 502 for unavailable objects) with JSON, and parsing
                // that as a listing silently yields zero files. Fail so the caller can fall back.
                if !status.is_success() {
                    last_error = Some(BrokerError::BrokerError(format!(
                        "unexpected status {} for {}",
                        status, url
                    )));
                    if status.is_client_error() {
                        break;
                    }
                } else {
                    match response.text().await {
                        Ok(body) => return Ok(body),
                        Err(e) => {
                            last_error = Some(e.into());
                        }
                    }
                }
            }
            Err(e) => {
                last_error = Some(e.into());
            }
        }

        // If not the last attempt, wait with exponential backoff
        if attempt < max_retries - 1 {
            let backoff_ms = initial_backoff_ms * (1 << attempt); // 2^attempt * initial
            warn!(
                "fetch_body failed for {} (attempt {}/{}), retrying in {}ms: {:?}",
                url,
                attempt + 1,
                max_retries,
                backoff_ms,
                last_error
            );
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        }
    }

    // All retries exhausted
    debug!(
        "fetch_body failed for {} after {} attempts",
        url, max_retries
    );
    Err(last_error.unwrap_or_else(|| {
        BrokerError::BrokerError(format!(
            "fetch_body failed for {} after {} attempts",
            url, max_retries
        ))
    }))
}

/// Ordered fetch targets for one origin URL: mirror first when configured and the origin
/// is allowlisted, origin last. The flag marks the mirror candidate so it can be given a
/// smaller budget than the origin.
fn fetch_candidates(base: Option<&str>, origins: &[String], url: &str) -> Vec<(String, bool)> {
    let mut candidates = Vec::with_capacity(2);
    if let Some(mirrored) = base.and_then(|b| mirror_url(b, origins, url)) {
        candidates.push((mirrored, true));
    }
    candidates.push((url.to_string(), false));
    candidates
}

/// Fetch the body of a URL with retry logic and exponential backoff.
///
/// When `BGPKIT_BROKER_FETCH_PROXY` is set and the URL's origin is allowlisted, the mirror
/// is tried first and the origin second, so a blocked mirror degrades to a direct fetch
/// instead of stalling the crawl. The mirror gets one short attempt: it is an
/// optimisation, and a mirror that accepts a connection and then hangs must not consume
/// the origin's retry budget. The returned body is identical either way: the rewrite only
/// changes the fetch address, never the URL recorded in the database.
///
/// # Arguments
/// * `url` - The URL to fetch
///
/// # Environment Variables
/// * `BGPKIT_BROKER_CRAWLER_MAX_RETRIES` - Maximum number of retry attempts (default: 3)
/// * `BGPKIT_BROKER_CRAWLER_BACKOFF_MS` - Initial backoff duration in milliseconds (default: 1000)
/// * `BGPKIT_BROKER_FETCH_PROXY` - Mirror base URL; unset means direct fetching
/// * `BGPKIT_BROKER_FETCH_PROXY_ORIGINS` - Origins the mirror may serve (default: archive.routeviews.org)
///
/// # Returns
/// The body of the response as a string, or an error if all retries failed.
pub(crate) async fn fetch_body(url: &str) -> Result<String, BrokerError> {
    let max_retries = get_crawler_max_retries();
    let initial_backoff_ms = get_crawler_backoff_ms();

    let client = reqwest::ClientBuilder::new()
        .user_agent("bgpkit-broker/3")
        .pool_max_idle_per_host(0)
        .timeout(Duration::from_secs(30))
        .build()?;
    let mirror_client = reqwest::ClientBuilder::new()
        .user_agent("bgpkit-broker/3")
        .pool_max_idle_per_host(0)
        .timeout(Duration::from_secs(10))
        .build()?;

    let candidates = fetch_candidates(
        get_fetch_proxy().as_deref(),
        &get_fetch_proxy_origins(),
        url,
    );
    fetch_from_candidates(
        &client,
        &mirror_client,
        &candidates,
        url,
        max_retries,
        initial_backoff_ms,
    )
    .await
}

/// Try each candidate in order, giving the mirror one short attempt and the origin the
/// full retry budget. Split out from [`fetch_body`] so the fallback order and the status
/// handling can be tested against a local server instead of the live archives.
async fn fetch_from_candidates(
    origin_client: &reqwest::Client,
    mirror_client: &reqwest::Client,
    candidates: &[(String, bool)],
    url: &str,
    max_retries: u32,
    initial_backoff_ms: u64,
) -> Result<String, BrokerError> {
    let last_index = candidates.len() - 1;

    let mut last_error: Option<BrokerError> = None;
    for (idx, (target, is_mirror)) in candidates.iter().enumerate() {
        let (target_client, target_retries) = if *is_mirror {
            (mirror_client, 1)
        } else {
            (origin_client, max_retries)
        };
        match fetch_with_retries(target_client, target, target_retries, initial_backoff_ms).await {
            Ok(body) => return Ok(body),
            Err(e) => {
                if idx < last_index {
                    warn!(
                        "fetch_body mirror failed for {} ({}), retrying origin directly: {:?}",
                        url, target, e
                    );
                }
                last_error = Some(e);
            }
        }
    }

    Err(last_error
        .unwrap_or_else(|| BrokerError::BrokerError(format!("fetch_body failed for {}", url))))
}

/// Canonical directory URL: origins answer a bare directory path with a 301, and the
/// fetch mirror does not follow redirects, so directory listings must be requested with
/// the trailing slash.
pub(crate) fn directory_url(url: &str) -> String {
    if url.ends_with('/') {
        url.to_string()
    } else {
        format!("{}/", url)
    }
}

/// Remove trailing slash from a string.
///
/// # Arguments
///
/// * `s`:
///
/// returns: String
pub(crate) fn remove_trailing_slash(s: impl ToString) -> String {
    let mut s = s.to_string();
    if s.ends_with('/') {
        s.pop();
    }
    s
}

pub(crate) async fn crawl_months_list(
    collector_root_url: &str,
    from_month: Option<NaiveDate>,
) -> Result<Vec<NaiveDate>, BrokerError> {
    let rounded_month = from_month.and_then(|d| NaiveDate::from_ymd_opt(d.year(), d.month(), 1));

    let month_link_pattern: Regex =
        Regex::new(r#"<a href="(....\...)/">.*"#).expect("invalid regex pattern");
    let body = fetch_body(directory_url(collector_root_url).as_str()).await?;
    let mut res = vec![];
    for cap in month_link_pattern.captures_iter(body.as_str()) {
        let month = cap[1].to_owned();
        let parsed_month =
            NaiveDate::parse_from_str(format!("{}.01", month.as_str()).as_str(), "%Y.%m.%d")?;
        if let Some(rounded) = rounded_month {
            if let Some(new_month) = NaiveDate::from_ymd_opt(rounded.year(), rounded.month(), 1) {
                if parsed_month < new_month {
                    continue;
                }
            }
        }
        if parsed_month > Utc::now().naive_utc().date() {
            continue;
        }
        res.push(parsed_month);
    }
    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mirror_url() {
        let base = "https://m.bgpkit.com";
        let rv = vec!["archive.routeviews.org".to_string()];
        assert_eq!(
            mirror_url(
                base,
                &rv,
                "https://archive.routeviews.org/bgpdata/2026.11/RIBS/"
            )
            .as_deref(),
            Some("https://m.bgpkit.com/archive.routeviews.org/bgpdata/2026.11/RIBS/")
        );
        assert_eq!(
            mirror_url(base, &rv, "http://archive.routeviews.org/bgpdata").as_deref(),
            Some("https://m.bgpkit.com/archive.routeviews.org/bgpdata")
        );
        assert_eq!(
            mirror_url(
                "https://m.bgpkit.com/",
                &rv,
                "https://archive.routeviews.org/x"
            )
            .as_deref(),
            Some("https://m.bgpkit.com/archive.routeviews.org/x")
        );
        // Already mirrored, or not an HTTP source: fetch as-is.
        assert_eq!(
            mirror_url(base, &rv, "https://m.bgpkit.com/archive.routeviews.org/x"),
            None
        );
        assert_eq!(mirror_url(base, &rv, "s3://bucket/key"), None);
        // Outside the allowlist: fetch as-is. The mirror caches listings, so only the
        // origins named here may be routed through it.
        assert_eq!(
            mirror_url(base, &rv, "https://data.ris.ripe.net/rrc00/2026.11/"),
            None
        );
        assert_eq!(
            mirror_url(base, &rv, "https://archive.routeviews.org.evil.test/x"),
            None
        );
    }

    #[test]
    fn test_fetch_candidates() {
        let rv = vec!["archive.routeviews.org".to_string()];
        let url = "https://archive.routeviews.org/route-views2/bgpdata/";
        assert_eq!(
            fetch_candidates(Some("https://m.bgpkit.com"), &rv, url),
            vec![
                (
                    "https://m.bgpkit.com/archive.routeviews.org/route-views2/bgpdata/".to_string(),
                    true
                ),
                (url.to_string(), false),
            ]
        );
        // No mirror configured: origin only.
        assert_eq!(
            fetch_candidates(None, &rv, url),
            vec![(url.to_string(), false)]
        );
        // Origin outside the allowlist: origin only, even with a mirror configured.
        let ris = "https://data.ris.ripe.net/rrc00/2026.11/";
        assert_eq!(
            fetch_candidates(Some("https://m.bgpkit.com"), &rv, ris),
            vec![(ris.to_string(), false)]
        );
        // Already mirrored: do not nest.
        let mirrored = "https://m.bgpkit.com/archive.routeviews.org/route-views2/bgpdata/";
        assert_eq!(
            fetch_candidates(Some("https://m.bgpkit.com"), &rv, mirrored),
            vec![(mirrored.to_string(), false)]
        );
    }

    #[test]
    fn test_directory_url() {
        assert_eq!(
            directory_url("https://archive.routeviews.org/bgpdata"),
            "https://archive.routeviews.org/bgpdata/"
        );
        assert_eq!(
            directory_url("https://archive.routeviews.org/bgpdata/"),
            "https://archive.routeviews.org/bgpdata/"
        );
    }

    /// Minimal single-shot HTTP stub: answers the next connection with a fixed status and
    /// body. Returns its base URL and how many connections it served, so a test can assert
    /// the mirror was tried before the origin.
    fn spawn_stub(
        status: &'static str,
        body: &'static str,
        expected_requests: usize,
    ) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        use std::io::{Read, Write};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind stub");
        let addr = listener.local_addr().expect("stub addr");
        let served = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&served);
        std::thread::spawn(move || {
            for _ in 0..expected_requests {
                let Ok((mut stream, _)) = listener.accept() else {
                    return;
                };
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });
        (format!("http://{addr}"), served)
    }

    fn test_client() -> reqwest::Client {
        reqwest::ClientBuilder::new()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("test client")
    }

    #[tokio::test]
    async fn test_fetch_falls_back_to_origin_on_mirror_4xx() {
        // The mirror answers a rejection with a JSON body. Before the status check that
        // body was returned as the listing and the crawl recorded zero files.
        let (mirror, mirror_hits) =
            spawn_stub("403 Forbidden", r#"{"error":"origin_not_allowed"}"#, 1);
        let (origin, origin_hits) = spawn_stub("200 OK", "<a href=\"2026.11/\">2026.11/</a>", 1);
        let candidates = vec![
            (format!("{mirror}/archive.routeviews.org/x/"), true),
            (format!("{origin}/archive.routeviews.org/x/"), false),
        ];
        let body = fetch_from_candidates(
            &test_client(),
            &test_client(),
            &candidates,
            "https://archive.routeviews.org/x/",
            1,
            1,
        )
        .await
        .expect("origin fallback should succeed");
        assert!(
            body.contains("2026.11"),
            "expected the origin listing, got {body}"
        );
        assert_eq!(mirror_hits.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(origin_hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_fetch_prefers_mirror_on_2xx() {
        let (mirror, _mirror_hits) = spawn_stub("200 OK", "mirror-listing", 1);
        let (origin, origin_hits) = spawn_stub("200 OK", "origin-listing", 1);
        let candidates = vec![
            (format!("{mirror}/route-views2/bgpdata/"), true),
            (format!("{origin}/route-views2/bgpdata/"), false),
        ];
        let body = fetch_from_candidates(
            &test_client(),
            &test_client(),
            &candidates,
            "https://archive.routeviews.org/route-views2/bgpdata/",
            1,
            1,
        )
        .await
        .expect("mirror should answer");
        assert_eq!(body, "mirror-listing");
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(
            origin_hits.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the origin must not be contacted when the mirror answers"
        );
    }

    #[tokio::test]
    async fn test_fetch_reports_error_when_every_candidate_fails() {
        let (mirror, _) = spawn_stub("502 Bad Gateway", r#"{"error":"no agent"}"#, 1);
        let (origin, _) = spawn_stub("404 Not Found", "not here", 1);
        let candidates = vec![
            (format!("{mirror}/x/"), true),
            (format!("{origin}/x/"), false),
        ];
        let err = fetch_from_candidates(
            &test_client(),
            &test_client(),
            &candidates,
            "https://archive.routeviews.org/x/",
            1,
            1,
        )
        .await
        .expect_err("both candidates reject the request");
        assert!(
            format!("{err:?}").contains("404"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_extract_link_size() {
        const RIPE_OLD: &str = r#"<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
 <head>
  <title>Index of /rrc00/2022.11</title>
 </head>
 <body>
<h1>Index of /rrc00/2022.11</h1>
  <table>
   <tr><th valign="top">&nbsp;</th><th><a href="?C=N;O=A">Name</a></th><th><a href="?C=M;O=A">Last modified</a></th><th><a href="?C=S;O=A">Size</a></th><th><a href="?C=D;O=A">Description</a></th></tr>
   <tr><th colspan="5"><hr></th></tr>
<tr><td valign="top">&nbsp;</td><td><a href="/rrc00/">Parent Directory</a></td><td>&nbsp;</td><td align="right">  - </td><td>&nbsp;</td></tr>
<tr><td valign="top">&nbsp;</td><td><a href="updates.20221128.2220.gz">updates.20221128.2220.gz</a></td><td align="right">2022-11-28 22:25  </td><td align="right">6.4M</td><td>&nbsp;</td></tr>
<tr><td valign="top">&nbsp;</td><td><a href="updates.20221128.2215.gz">updates.20221128.2215.gz</a></td><td align="right">2022-11-28 22:20  </td><td align="right">3.8M</td><td>&nbsp;</td></tr>
<tr><td valign="top">&nbsp;</td><td><a href="bview.20221102.0800.gz">bview.20221102.0800.gz</a></td><td align="right">2022-11-02 10:14  </td><td align="right">1.5G</td><td>&nbsp;</td></tr>
<tr><td valign="top">&nbsp;</td><td><a href="bview.20221102.0000.gz">bview.20221102.0000.gz</a></td><td align="right">2022-11-02 02:13  </td><td align="right">1.5G</td><td>&nbsp;</td></tr>
   <tr><th colspan="5"><hr></th></tr>
</table>
</body></html>
"#;

        const RIPE_NEW: &str = r#"<html>
<head><title>Index of /rrc00/2001.01/</title></head>
<body bgcolor="white">
<h1>Index of /rrc00/2001.01/</h1><hr><pre><a href="../">../</a>
<a href="bview.20010101.0609.gz">bview.20010101.0609.gz</a>                             01-Jan-2001 06:09     12M
<a href="bview.20010101.1410.gz">bview.20010101.1410.gz</a>                             01-Jan-2001 14:10     12M
<a href="updates.20010131.2236.gz">updates.20010131.2236.gz</a>                           31-Jan-2001 22:36     98K
<a href="updates.20010131.2251.gz">updates.20010131.2251.gz</a>                           31-Jan-2001 22:51     97K
</pre><hr></body>
</html>
"#;
        let res = extract_link_size(RIPE_NEW);
        assert_eq!(res.len(), 4);

        const ROUTEVIEWS: &str = r#"<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
 <head>
  <title>Index of /route-views.bdix/bgpdata/2022.10/UPDATES</title>
 </head>
 <body>
<h1>Index of /route-views.bdix/bgpdata/2022.10/UPDATES</h1>
  <table>
   <tr><th valign="top"><img src="/icons/blank.gif" alt="[ICO]"></th><th><a href="?C=N;O=D">Name</a></th><th><a href="?C=M;O=A">Last modified</a></th><th><a href="?C=S;O=A">Size</a></th><th><a href="?C=D;O=A">Description</a></th></tr>
   <tr><th colspan="5"><hr></th></tr>
<tr><td valign="top"><img src="/icons/back.gif" alt="[PARENTDIR]"></td><td><a href="/route-views.bdix/bgpdata/2022.10/">Parent Directory</a>       </td><td>&nbsp;</td><td align="right">  - </td><td>&nbsp;</td></tr>
<tr><td valign="top"><img src="/icons/unknown.gif" alt="[   ]"></td><td><a href="updates.20221001.0000.bz2">updates.20221001.000..&gt;</a></td><td align="right">2022-10-01 00:00  </td><td align="right"> 14 </td><td>&nbsp;</td></tr>
<tr><td valign="top"><img src="/icons/unknown.gif" alt="[   ]"></td><td><a href="updates.20221001.0015.bz2">updates.20221001.001..&gt;</a></td><td align="right">2022-10-01 00:15  </td><td align="right"> 14 </td><td>&nbsp;</td></tr>
<tr><td valign="top"><img src="/icons/unknown.gif" alt="[   ]"></td><td><a href="updates.20221026.1545.bz2">updates.20221026.154..&gt;</a></td><td align="right">2022-10-26 15:45  </td><td align="right"> 14 </td><td>&nbsp;</td></tr>
<tr><td valign="top"><img src="/icons/unknown.gif" alt="[   ]"></td><td><a href="updates.20221026.1600.bz2">updates.20221026.160..&gt;</a></td><td align="right">2022-10-26 16:00  </td><td align="right"> 14 </td><td>&nbsp;</td></tr>
   <tr><th colspan="5"><hr></th></tr>
</table>
</body></html>
"#;

        let res = extract_link_size(RIPE_OLD);
        assert_eq!(res.len(), 4);
        let res = extract_link_size(ROUTEVIEWS);
        assert_eq!(res.len(), 4);
    }

    #[test]
    fn test_min_valid_timestamp() {
        use chrono::NaiveDate;

        // Verify MIN_VALID_TIMESTAMP is 1998-01-01
        let expected = NaiveDate::from_ymd_opt(1998, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        assert_eq!(MIN_VALID_TIMESTAMP, expected);

        // Verify it correctly rejects Unix epoch (1970-01-01)
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        assert!(epoch < MIN_VALID_TIMESTAMP);

        // Verify it accepts valid early dates (e.g., 1999-10-01 when rrc00 started)
        let valid_early = NaiveDate::from_ymd_opt(1999, 10, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        assert!(valid_early >= MIN_VALID_TIMESTAMP);
    }
}
