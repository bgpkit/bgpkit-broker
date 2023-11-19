use crate::crawler::common::{
    crawl_months_list, extract_link_size, fetch_body, remove_trailing_slash,
};
use crate::crawler::Collector;
use crate::{BrokerError, BrokerItem};
use chrono::{NaiveDate, NaiveDateTime};
use futures::stream::StreamExt;
use log::debug;
use regex::Regex;

/// Crawl RIPE RIS MRT data dump for a given collector.
///
/// Example: <https://data.ris.ripe.net/rrc00/>.
/// A few things to note:
/// - at the root level, there are one directory per month, e.g. `2001.01/`
///     - this means a single crawl of the root page will give us all the months available
/// - each month directory contains a list of files, e.g. `updates.20010101.0000.gz` or `bview.20010101.0000.gz` (the latter is a full dump, the former is an incremental update)
///
/// # Arguments
///
/// * `collector`: the [Collector] to crawl
/// * `from_ts`: optional start date for the crawl to start from, provide None for bootstrap
///
/// returns: Result<Vec<Item, Global>, Error>
pub async fn crawl_ripe_ris(
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
    let url = remove_trailing_slash(url.as_str());
    debug!("crawling data for {} ...", url.as_str());
    let body = fetch_body(url.as_str()).await?;
    debug!("    download for {} finished ", url.as_str());

    let new_url = url.to_string();

    let data_items: Vec<BrokerItem> = tokio::task::spawn_blocking(move || {
        let items = extract_link_size(body.as_str());
        items
            .iter()
            .map(|(link, size)| {
                let url = match url.as_str().contains("https") {
                    true => format!("{}/{}", url, link),
                    false => format!("{}/{}", url, link).replace("http", "https"),
                };
                let updates_link_pattern: Regex = Regex::new(r".*(........\.....)\.gz.*").unwrap();
                let time_str = updates_link_pattern
                    .captures(&url)
                    .unwrap()
                    .get(1)
                    .unwrap()
                    .as_str();
                let unix_time = NaiveDateTime::parse_from_str(time_str, "%Y%m%d.%H%M").unwrap();
                match link.contains("update") {
                    true => BrokerItem {
                        ts_start: unix_time,
                        ts_end: unix_time + chrono::Duration::seconds(5 * 60),
                        url: url.clone(),
                        rough_size: *size,
                        collector_id: collector_id.clone(),
                        data_type: "updates".to_string(),
                        exact_size: 0,
                    },
                    false => BrokerItem {
                        ts_start: unix_time,
                        ts_end: unix_time,
                        url: url.clone(),
                        rough_size: *size,
                        collector_id: collector_id.clone(),
                        data_type: "rib".to_string(),
                        exact_size: 0,
                    },
                }
            })
            .collect()
    })
    .await
    .unwrap();

    debug!("crawling data for {} ... finished", &new_url);
    Ok(data_items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[tokio::test]
    async fn test_crawl_ripe_ris() {
        tracing_subscriber::fmt::init();
        let collector = Collector {
            id: "rrc00".to_string(),
            project: "riperis".to_string(),
            url: "https://data.ris.ripe.net/rrc00/".to_string(),
        };

        let two_months_ago = Utc::now().date_naive() - chrono::Duration::days(60);
        let _items = crawl_ripe_ris(&collector, Some(two_months_ago))
            .await
            .unwrap();
        let _after_date = NaiveDate::from_ymd_opt(2023, 5, 3)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
    }

    #[tokio::test]
    async fn test_crawl_months() {
        let months = crawl_months_list("https://data.ris.ripe.net/rrc00/", None)
            .await
            .unwrap();
        dbg!(months);
        let current_month = crawl_months_list(
            "https://data.ris.ripe.net/rrc00/",
            Some(Utc::now().date_naive()),
        )
        .await
        .unwrap();

        assert_eq!(current_month.len(), 1);
    }

    #[tokio::test]
    async fn test_crawl_month() {
        let items = crawl_month(
            "https://data.ris.ripe.net/rrc00/2008.09/".to_string(),
            "rrc00".to_string(),
        )
        .await
        .unwrap();
        for item in items {
            println!("{}", item.url);
        }
    }
}
