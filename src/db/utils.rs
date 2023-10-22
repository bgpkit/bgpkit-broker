use crate::query::BrokerCollector;
use chrono::{Datelike, Duration, NaiveDateTime, Timelike};

pub(crate) fn infer_url(
    collector: &BrokerCollector,
    ts_start: &NaiveDateTime,
    is_rib: bool,
) -> (String, NaiveDateTime) {
    let project = collector.project.as_str();
    let collector_url = collector.url.as_str();
    let updates_interval = collector.updates_interval;

    let (url, ts_end) = match project {
        "route-views" => match is_rib {
            true => (
                format!(
                    "{}/bgpdata/{}.{:02}/RIBS/rib.{}{:02}{:02}.{:02}{:02}.bz2",
                    collector_url,
                    ts_start.year(),
                    ts_start.month(),
                    ts_start.year(),
                    ts_start.month(),
                    ts_start.day(),
                    ts_start.hour(),
                    ts_start.minute(),
                ),
                *ts_start,
            ),
            false => (
                format!(
                    "{}/bgpdata/{}.{:02}/UPDATES/updates.{}{:02}{:02}.{:02}{:02}.bz2",
                    collector_url,
                    ts_start.year(),
                    ts_start.month(),
                    ts_start.year(),
                    ts_start.month(),
                    ts_start.day(),
                    ts_start.hour(),
                    ts_start.minute(),
                ),
                *ts_start + Duration::seconds(updates_interval),
            ),
        },
        "ripe-ris" => match is_rib {
            true => (
                format!(
                    "{}/{}.{:02}/bview.{}{:02}{:02}.{:02}{:02}.gz",
                    collector_url,
                    ts_start.year(),
                    ts_start.month(),
                    ts_start.year(),
                    ts_start.month(),
                    ts_start.day(),
                    ts_start.hour(),
                    ts_start.minute(),
                ),
                *ts_start,
            ),
            false => (
                format!(
                    "{}/{}.{:02}/updates.{}{:02}{:02}.{:02}{:02}.gz",
                    collector_url,
                    ts_start.year(),
                    ts_start.month(),
                    ts_start.year(),
                    ts_start.month(),
                    ts_start.day(),
                    ts_start.hour(),
                    ts_start.minute(),
                ),
                *ts_start + Duration::seconds(updates_interval),
            ),
        },
        _ => {
            todo!()
        }
    };
    (url, ts_end)
}
