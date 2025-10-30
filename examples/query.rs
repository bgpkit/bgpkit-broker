use bgpkit_broker::{BgpkitBroker, BrokerItem};

pub fn main() {
    let broker = BgpkitBroker::new()
        .broker_url("https://api.bgpkit.com/v3/broker")
        .ts_start("1634693400")
        .ts_end("1634693400")
        .collector_id("rrc00,route-views2");

    // method 1: create iterator from reference (so that you can reuse the broker object)
    // same as `&broker.into_iter()`
    for item in &broker {
        println!("{}", item);
    }

    let broker = BgpkitBroker::new()
        .ts_start("1634693400")
        .ts_end("1634693400");

    // method 2: create iterator from the broker object (taking ownership)
    let items = broker.into_iter().collect::<Vec<BrokerItem>>();
    assert!(items.len() >= 53);

    // count total number of items
    let broker = BgpkitBroker::new()
        .ts_start("2024-01-01")
        .ts_end("2024-01-02")
        .collector_id("route-views2");
    match broker.query_total_count() {
        Ok(total) => println!("total: {}", total),
        Err(err) => println!("error: {}", err),
    }
}
