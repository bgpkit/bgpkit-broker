use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// MRT collector peer information
///
/// Represents the information of an MRT collector peer.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct BrokerPeer {
    /// The date of the latest available data.
    pub date: NaiveDate,
    /// The IP address of the collector peer.
    pub ip: IpAddr,
    /// The ASN (Autonomous System Number) of the collector peer.
    pub asn: u32,
    /// The name of the collector.
    pub collector: String,
    /// The number of IPv4 prefixes.
    pub num_v4_pfxs: u32,
    /// The number of IPv6 prefixes.
    pub num_v6_pfxs: u32,
    /// The number of connected ASNs.
    pub num_connected_asns: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BrokerPeersResult {
    pub count: u32,
    pub data: Vec<BrokerPeer>,
}
