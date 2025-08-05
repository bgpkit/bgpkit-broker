//! This example retrieves a list of full-feed MRT collector peers from route-views.amsix and print
//! out the top 10 peers with the most connected ASNs.
//!
//! Example output
//! ```text
//! 2024-10-31,route-views.amsix,58511,80.249.212.104,2567,960791,0
//! 2024-10-31,route-views.amsix,267613,80.249.213.223,2268,965321,0
//! 2024-10-31,route-views.amsix,267613,2001:7f8:1:0:a500:26:7613:1,2011,0,206667
//! 2024-10-31,route-views.amsix,12779,80.249.209.17,1932,951788,0
//! 2024-10-31,route-views.amsix,9002,2001:7f8:1::a500:9002:1,1896,0,202069
//! 2024-10-31,route-views.amsix,38880,80.249.212.75,1883,992214,0
//! 2024-10-31,route-views.amsix,58511,2001:7f8:1::a505:8511:1,1853,0,216981
//! 2024-10-31,route-views.amsix,9002,80.249.209.216,1318,956345,0
//! 2024-10-31,route-views.amsix,42541,80.249.212.84,1302,952091,0
//! 2024-10-31,route-views.amsix,12779,2001:7f8:1::a501:2779:1,1247,0,201726
//! ```

fn main() {
    let broker = bgpkit_broker::BgpkitBroker::new()
        .collector_id("route-views.amsix")
        .unwrap()
        .peers_only_full_feed(true);
    let mut peers = broker.get_peers().unwrap();
    peers.sort_by(|a, b| b.num_connected_asns.cmp(&a.num_connected_asns));
    for peer in peers.iter().take(10) {
        println!(
            "{},{},{},{},{},{},{}",
            peer.date,
            peer.collector,
            peer.asn,
            peer.ip,
            peer.num_connected_asns,
            peer.num_v4_pfxs,
            peer.num_v6_pfxs,
        );
    }
}
