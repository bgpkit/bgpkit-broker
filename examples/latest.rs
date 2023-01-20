use bgpkit_broker::{BgpkitBroker};

pub fn main() {
    let broker = BgpkitBroker::new();

    // method 1: create iterator from reference (so that you can reuse the broker object)
    // same as `&broker.into_iter()`
    for item in broker.latest().unwrap() {
        println!("{}", item);
    }
}
