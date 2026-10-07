//! `BGPKIT_BROKER_URL` must be reflected by `BgpkitBroker::default()`.
//!
//! This lives in its own integration-test binary on purpose: it mutates the
//! process-global environment, which would race the unit tests that construct
//! brokers in parallel if it ran in the same binary.

use bgpkit_broker::BgpkitBroker;

#[test]
fn default_reads_and_normalizes_bgpkit_broker_url() {
    let previous = std::env::var("BGPKIT_BROKER_URL").ok();

    std::env::set_var("BGPKIT_BROKER_URL", " https://broker.example.com/v9/broker/ ");
    let broker = BgpkitBroker::default();
    assert_eq!(broker.broker_url, "https://broker.example.com/v9/broker");

    match previous {
        Some(value) => std::env::set_var("BGPKIT_BROKER_URL", value),
        None => std::env::remove_var("BGPKIT_BROKER_URL"),
    }
}
