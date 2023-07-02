use crate::{BrokerItem, LocalBrokerDb};
use chrono::{Duration, NaiveDateTime};
use clap::Args;
use std::str::FromStr;

#[derive(Args, Debug)]
pub struct BrokerSearchQuery {
    /// Start timestamp
    #[clap(short, long)]
    ts_start: Option<String>,

    /// End timestamp
    #[clap(short, long)]
    ts_end: Option<String>,

    /// duration before `ts_end` or after `ts_start`
    #[clap(short, long)]
    duration: Option<String>,

    /// filter by route collector projects, i.e. `route-views` or `riperis`
    #[clap(short, long)]
    project: Option<String>,

    /// filter by collector IDs, e.g. 'rrc00', 'route-views2. use comma to separate multiple collectors
    #[clap(short, long)]
    collectors: Option<String>,

    /// filter by data types, i.e. 'update', 'rib'.
    #[clap(short, long)]
    data_type: Option<String>,

    /// page number
    #[clap(long)]
    page: Option<usize>,

    /// page size
    #[clap(long)]
    page_size: Option<usize>,
}

/// Parse timestamp string into NaiveDateTime
///
/// The timestamp string can be either unix timestamp or RFC3339 format string (e.g. 2020-01-01T00:00:00Z).
pub fn parse_time_str(ts_str: &str) -> Result<NaiveDateTime, String> {
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

pub fn process_search_query(
    query: BrokerSearchQuery,
    db: &LocalBrokerDb,
) -> Result<Vec<BrokerItem>, String> {
    let mut ts_start = query.ts_start.map(|s| parse_time_str(s.as_str()).unwrap());
    let mut ts_end = query.ts_end.map(|s| parse_time_str(s.as_str()).unwrap());

    match (ts_start, ts_end) {
        (Some(start), None) => {
            if let Some(duration_str) = &query.duration {
                match humantime::parse_duration(duration_str.as_str()) {
                    Ok(d) => {
                        ts_end = Some(start + Duration::from_std(d).unwrap());
                    }
                    Err(_) => {
                        return Err(format!(
                            "cannot parse time duration string: {}",
                            duration_str
                        ))
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
                        return Err(format!(
                            "cannot parse time duration string: {}",
                            duration_str
                        ))
                    }
                }
            }
        }
        _ => {}
    };

    let collectors = query
        .collectors
        .map(|s| s.split(",").map(|s| s.trim().to_string()).collect());

    let items = db
        .search_items(
            collectors,
            query.project,
            query.data_type,
            ts_start,
            ts_end,
            query.page,
            query.page_size,
        )
        .unwrap();

    Ok(items)
}
