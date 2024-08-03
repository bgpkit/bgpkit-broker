mod collector;
mod common;
mod riperis;
mod routeviews;

use chrono::NaiveDate;
use log::info;
use tracing::debug;

// public interface
use crate::{BrokerError, BrokerItem};
use riperis::crawl_ripe_ris;
use routeviews::crawl_routeviews;

pub use collector::{load_collectors, Collector};

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
