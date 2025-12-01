//! BrokerItem module define the broker search results
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

/// BGPKIT Broker data item.
///
/// The fields are:
/// - [ts_start][BrokerItem::ts_start]: the starting timestamp of the data file
/// - [ts_end][BrokerItem::ts_end]: the ending timestamp of the data file
/// - [collector_id][BrokerItem::collector_id]: the collector id of the item: e.g. `rrc00`
/// - [data_type][BrokerItem::data_type]: type of the data item: `rib` or `updates`
/// - [url][BrokerItem::url]: the URL to the data item file
/// - [rough_size][BrokerItem::rough_size]: rough file size extracted from the collector webpage
/// - [exact_size][BrokerItem::exact_size]: exact file size extracted by crawling the file
///
/// An array of [BrokerItem]s can be sorted with the following order:
/// 1. smaller timestamp before larger timestamp
/// 2. RIB before updates
/// 3. then alphabetical order on collector ID (route-views before rrc)
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "cli", derive(tabled::Tabled))]
pub struct BrokerItem {
    /// start timestamp
    pub ts_start: chrono::NaiveDateTime,
    /// end timestamps
    pub ts_end: chrono::NaiveDateTime,
    /// the collector id of the item: e.g. `rrc00`
    pub collector_id: String,
    /// type of the data item: `rib` or `updates`
    pub data_type: String,
    /// the URL to the data item file
    pub url: String,
    /// rough file size extracted from the hosting site page
    pub rough_size: i64,
    /// exact file size extracted by crawling the file
    pub exact_size: i64,
}

impl BrokerItem {
    /// Checks if the data type is "rib" (i.e. RIB dump).
    ///
    /// # Return
    /// Returns `true` if the data type is "rib", otherwise `false`.
    pub fn is_rib(&self) -> bool {
        self.data_type.as_str() == "rib"
    }
}

#[allow(clippy::unwrap_used)]
impl Display for BrokerItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}

impl PartialOrd for BrokerItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BrokerItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // compare BrokerItems with the following sequence
        // 1. ts_start
        // 2. data_type
        // 3. collector_id
        self.ts_start
            .cmp(&other.ts_start) // smaller timestamp comes earlier
            .then(self.data_type.cmp(&other.data_type)) // RIB before updates on the same timestamp
            .then(self.collector_id.cmp(&other.collector_id)) // route-viewsX before rrcX
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;

    #[test]
    fn test_sorting() {
        let mut items = vec![
            BrokerItem {
                ts_start: DateTime::from_timestamp(10, 0).unwrap().naive_utc(),
                ts_end: Default::default(),
                collector_id: "rrc00".to_string(),
                data_type: "updates".to_string(),
                url: "".to_string(),
                rough_size: 0,
                exact_size: 0,
            },
            BrokerItem {
                ts_start: DateTime::from_timestamp(9, 0).unwrap().naive_utc(),
                ts_end: Default::default(),
                collector_id: "rrc00".to_string(),
                data_type: "updates".to_string(),
                url: "".to_string(),
                rough_size: 0,
                exact_size: 0,
            },
            BrokerItem {
                ts_start: DateTime::from_timestamp(10, 0).unwrap().naive_utc(),
                ts_end: Default::default(),
                collector_id: "rrc00".to_string(),
                data_type: "rib".to_string(),
                url: "".to_string(),
                rough_size: 0,
                exact_size: 0,
            },
            BrokerItem {
                ts_start: DateTime::from_timestamp(10, 0).unwrap().naive_utc(),
                ts_end: Default::default(),
                collector_id: "route-views2".to_string(),
                data_type: "rib".to_string(),
                url: "".to_string(),
                rough_size: 0,
                exact_size: 0,
            },
        ];
        let correct_items = vec![
            BrokerItem {
                ts_start: DateTime::from_timestamp(9, 0).unwrap().naive_utc(),
                ts_end: Default::default(),
                collector_id: "rrc00".to_string(),
                data_type: "updates".to_string(),
                url: "".to_string(),
                rough_size: 0,
                exact_size: 0,
            },
            BrokerItem {
                ts_start: DateTime::from_timestamp(10, 0).unwrap().naive_utc(),
                ts_end: Default::default(),
                collector_id: "route-views2".to_string(),
                data_type: "rib".to_string(),
                url: "".to_string(),
                rough_size: 0,
                exact_size: 0,
            },
            BrokerItem {
                ts_start: DateTime::from_timestamp(10, 0).unwrap().naive_utc(),
                ts_end: Default::default(),
                collector_id: "rrc00".to_string(),
                data_type: "rib".to_string(),
                url: "".to_string(),
                rough_size: 0,
                exact_size: 0,
            },
            BrokerItem {
                ts_start: DateTime::from_timestamp(10, 0).unwrap().naive_utc(),
                ts_end: Default::default(),
                collector_id: "rrc00".to_string(),
                data_type: "updates".to_string(),
                url: "".to_string(),
                rough_size: 0,
                exact_size: 0,
            },
        ];

        assert_ne!(items, correct_items);
        items.sort();
        assert_eq!(items, correct_items);
    }
}
