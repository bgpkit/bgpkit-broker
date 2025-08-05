use bgpkit_broker::{BgpkitBroker, BrokerItem};

pub fn main() {
    let broker = BgpkitBroker::new()
        .broker_url("https://api.bgpkit.com/v3/broker")
        .ts_start("1634693400").unwrap()
        .ts_end("1634693400").unwrap()
        .collector_id("rrc00,route-views2").unwrap();

    // method 1: create iterator from reference (so that you can reuse the broker object)
    // same as `&broker.into_iter()`
    for item in &broker {
        println!("{}", item);
    }

    let broker = BgpkitBroker::new()
        .ts_start("1634693400").unwrap()
        .ts_end("1634693400").unwrap();
    // method 2: create iterator from the broker object (taking ownership)
    let items = broker.into_iter().collect::<Vec<BrokerItem>>();

    assert_eq!(items.len(), 53);
}
