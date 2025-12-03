mod common;
mod riperis;
mod routeviews;

use chrono::NaiveDate;
use tracing::{debug, info};

// public interface
use crate::{BrokerError, BrokerItem};
pub use common::{
    get_crawler_backoff_ms, get_crawler_collector_concurrency, get_crawler_max_retries,
    get_crawler_month_concurrency,
};
use riperis::crawl_ripe_ris;
use routeviews::crawl_routeviews;

use crate::Collector;

pub async fn crawl_collector(
    collector: &Collector,
    from_ts: Option<NaiveDate>,
) -> Result<Vec<BrokerItem>, BrokerError> {
    debug!("crawl collector {} from {:?}", &collector.id, from_ts);
    if from_ts.is_none() {
        info!("bootstrap crawl for collector {}", &collector.id);
    }

    let items = match collector.project.as_str() {
        "riperis" => crawl_ripe_ris(collector, from_ts).await,
        "routeviews" => crawl_routeviews(collector, from_ts).await,
        _ => panic!("unknown project {}", collector.project),
    };
    debug!(
        "crawl collector {} from {:?}... done",
        &collector.id, from_ts
    );
    items
}
