use bgpkit_broker::BrokerItem;
use bgpkit_commons::mrt_collectors::MrtCollector;
use chrono::NaiveDateTime;
use itertools::Itertools;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;

#[derive(Serialize, tabled::Tabled)]
pub struct CollectorInfo {
    pub project: String,
    pub name: String,
    pub country: String,
    pub activated_on: NaiveDateTime,
    pub data_url: String,
}

pub fn get_missing_collectors(latest_items: &[BrokerItem]) -> Vec<CollectorInfo> {
    let latest_collectors: HashSet<String> = latest_items
        .iter()
        .map(|i| i.collector_id.clone())
        .collect();

    let all_collectors = match bgpkit_commons::mrt_collectors::get_all_collectors() {
        Ok(collectors) => collectors,
        Err(_) => return Vec::new(),
    };

    let all_collectors_map: HashMap<String, MrtCollector> = all_collectors
        .into_iter()
        .map(|c| (c.name.clone(), c))
        .collect();

    let all_collector_names: HashSet<String> = all_collectors_map
        .values()
        .map(|c| c.name.clone())
        .collect();

    let country_map = match bgpkit_commons::countries::Countries::new() {
        Ok(map) => map,
        Err(_) => return Vec::new(),
    };

    // get the difference between the two sets
    let missing_collectors: Vec<CollectorInfo> = all_collector_names
        .difference(&latest_collectors)
        .filter_map(|c| {
            // convert to CollectorInfo
            let collector = all_collectors_map.get(c)?;
            let country_name = country_map
                .lookup_by_code(&collector.country)
                .map(|c| c.name.clone())
                .unwrap_or_else(|| collector.country.clone());
            Some(CollectorInfo {
                project: collector.project.to_string(),
                name: collector.name.clone(),
                country: country_name,
                activated_on: collector.activated_on,
                data_url: collector.data_url.clone(),
            })
        })
        .sorted_by(|a, b| a.name.cmp(&b.name))
        .collect();

    missing_collectors
}

pub fn is_local_path(path: &str) -> bool {
    if path.contains("://") {
        return false;
    }
    let path = Path::new(path);
    path.is_absolute() || path.is_relative()
}

pub fn parse_s3_path(path: &str) -> Option<(String, String)> {
    // split a path like s3://bucket/path/to/file into (bucket, path/to/file)
    let parts = path.split("://").collect::<Vec<&str>>();
    if parts.len() != 2 || parts[0] != "s3" {
        return None;
    }
    let parts = parts[1].split('/').collect::<Vec<&str>>();
    let bucket = parts[0].to_string();
    // join the rest delimited by `/`
    let path = format!("/{}", parts[1..].join("/"));
    if parts.ends_with(&["/"]) {
        return None;
    }
    Some((bucket, path))
}
