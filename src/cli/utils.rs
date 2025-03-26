use bgpkit_broker::BrokerItem;
use bgpkit_commons::collectors::MrtCollector;
use chrono::NaiveDateTime;
use itertools::Itertools;
use serde::Serialize;
use std::collections::{HashMap, HashSet};

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
    let all_collectors_map: HashMap<String, MrtCollector> =
        bgpkit_commons::collectors::get_all_collectors()
            .unwrap()
            .into_iter()
            .map(|c| (c.name.clone(), c))
            .collect();

    let all_collector_names: HashSet<String> = all_collectors_map
        .values()
        .map(|c| c.name.clone())
        .collect();

    // get the difference between the two sets
    let missing_collectors: Vec<CollectorInfo> = all_collector_names
        .difference(&latest_collectors)
        .map(|c| {
            // convert to CollectorInfo
            let collector = all_collectors_map.get(c).unwrap();
            let country_map = bgpkit_commons::countries::Countries::new().unwrap();
            CollectorInfo {
                project: collector.project.to_string(),
                name: collector.name.clone(),
                country: country_map
                    .lookup_by_code(&collector.country)
                    .unwrap()
                    .name
                    .clone(),
                activated_on: collector.activated_on,
                data_url: collector.data_url.clone(),
            }
        })
        .sorted_by(|a, b| a.name.cmp(&b.name))
        .collect();

    missing_collectors
}
