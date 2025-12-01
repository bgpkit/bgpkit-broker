use crate::crawler::common::{
    crawl_months_list, extract_link_size, fetch_body, remove_trailing_slash,
};
use crate::crawler::Collector;
use crate::{BrokerError, BrokerItem};
use chrono::{NaiveDate, NaiveDateTime};
use futures::stream::StreamExt;
use regex::Regex;
use tracing::debug;

/// Crawl RouteViews MRT data dump for a given collector.
///
/// Example: <https://routeviews.org/bgpdata/>.
/// A few things to note:
/// - at the root level, there are one directory per month, e.g. `2001.01/`
///     - this means a single crawl of the root page will give us all the months available
/// - each month directory contains two subdirectories, `UPDATES/` and `RIBS/`
/// - each subdirectory contains a list of files, e.g. `updates.20010101.0000.bz2` or `rib.20010101.0000.bz2`
///
/// # Arguments
///
/// * `collector`: the [Collector] to crawl
/// * `from_ts`: optional start date for the crawl to start from, provide None for bootstrap
///
/// returns: Result<Vec<Item, Global>, Error>
pub async fn crawl_routeviews(
    collector: &Collector,
    from_ts: Option<NaiveDate>,
) -> Result<Vec<BrokerItem>, BrokerError> {
    let collector_url = remove_trailing_slash(collector.url.as_str());

    let months_to_crawl = crawl_months_list(collector_url.as_str(), from_ts).await?;
    let mut stream = futures::stream::iter(months_to_crawl.into_iter().map(|month| {
        let url = format!("{}/{}", collector_url.as_str(), month.format("%Y.%m/"));
        crawl_month(url, collector.id.clone())
    }))
    .buffer_unordered(10);

    let mut res = vec![];
    while let Some(result) = stream.next().await {
        let items = result?;
        res.extend(items);
    }
    Ok(res)
}

async fn crawl_month(url: String, collector_id: String) -> Result<Vec<BrokerItem>, BrokerError> {
    let root_url = remove_trailing_slash(url.as_str());
    debug!("crawling data for {} ...", root_url.as_str());

    let mut all_items = vec![];

    // RIBS
    for subdir in ["RIBS", "UPDATES"] {
        let url = format!("{}/{}", &root_url, subdir);
        let body = fetch_body(url.as_str()).await?;
        let collector_id_clone = collector_id.clone();
        let data_items: Vec<BrokerItem> = tokio::task::spawn_blocking(move || {
            let items = extract_link_size(body.as_str());
            items
                .iter()
                .filter_map(|(link, size)| {
                    let url = format!("{}/{}", &url, link);
                    #[allow(clippy::regex_creation_in_loops)]
                    let link_time_pattern: Regex =
                        Regex::new(r".*(........\.....)\.bz2.*").expect("invalid regex pattern");
                    let time_str = link_time_pattern.captures(&url)?.get(1)?.as_str();
                    let unix_time = NaiveDateTime::parse_from_str(time_str, "%Y%m%d.%H%M").ok()?;
                    match link.contains("update") {
                        true => Some(BrokerItem {
                            ts_start: unix_time,
                            ts_end: unix_time + chrono::Duration::seconds(15 * 60),
                            url: url.clone(),
                            rough_size: *size,
                            collector_id: collector_id_clone.clone(),
                            data_type: "updates".to_string(),
                            exact_size: 0,
                        }),
                        false => Some(BrokerItem {
                            ts_start: unix_time,
                            ts_end: unix_time,
                            url: url.clone(),
                            rough_size: *size,
                            collector_id: collector_id_clone.clone(),
                            data_type: "rib".to_string(),
                            exact_size: 0,
                        }),
                    }
                })
                .collect()
        })
        .await
        .expect("blocking task panicked");
        all_items.extend(data_items);
    }

    debug!("crawling data for {} ... finished", &root_url);
    Ok(all_items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[tokio::test]
    async fn test_crawl_routeviews() {
        let collector = Collector {
            id: "route-views2".to_string(),
            project: "routeviews".to_string(),
            url: "https://routeviews.org/bgpdata/".to_string(),
        };

        let two_months_ago = Utc::now().date_naive() - chrono::Duration::days(60);
        let items = crawl_routeviews(&collector, Some(two_months_ago))
            .await
            .unwrap();
        dbg!(items);
    }

    #[tokio::test]
    async fn test_crawl_months() {
        let root_url = "https://routeviews.org/bgpdata/";
        let months = crawl_months_list(root_url, None).await.unwrap();
        dbg!(months);
        let current_month = crawl_months_list(root_url, Some(Utc::now().date_naive()))
            .await
            .unwrap();
        assert!(!current_month.is_empty());
    }

    #[tokio::test]
    async fn test_crawl_month() {
        let items = crawl_month(
            "https://routeviews.org/bgpdata/2016.11/".to_string(),
            "route-views2".to_string(),
        )
        .await
        .unwrap();
        for item in items {
            println!("{}", item.url);
        }
    }
}
