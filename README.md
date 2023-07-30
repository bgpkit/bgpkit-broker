# BGPKIT Broker

[![Rust](https://github.com/bgpkit/bgpkit-broker/actions/workflows/rust.yml/badge.svg)](https://github.com/bgpkit/bgpkit-broker/actions/workflows/rust.yml)
[![Crates.io][crates-badge]][crates-url]
[![MIT licensed][mit-badge]][mit-url]
[![Better Uptime Badge](https://betteruptime.com/status-badges/v1/monitor/mfwr.svg)](https://status.bgpkit.com)
[![Twitter][twitter-badge]][twitter-url]
[![Mastodon][mastodon-badge]][mastodon-url]


[crates-badge]: https://img.shields.io/crates/v/bgpkit-broker.svg
[crates-url]: https://crates.io/crates/bgpkit-broker
[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: https://github.com/bgpkit/bgpkit-broker/blob/main/LICENSE
[twitter-badge]: https://shields.io/badge/Follow-lightgrey?logo=twitter&style=social
[twitter-url]: https://twitter.com/bgpkit
[mastodon-url]: https://infosec.exchange/@bgpkit
[mastodon-badge]: https://img.shields.io/mastodon/follow/109852506691103147?domain=https%3A%2F%2Finfosec.exchange&style=social

[BGPKIT Broker](https://bgpkit.com/broker) is an online data API service that allows users to search for publicly available BGP archive files by time, collector, project, or data type. The service indexes the archives in close to real-time (delay is less than 5 minutes). Currently, we are indexing BGP table dump and updates files from [RIPE RIS][ripe-ris] and [RouteViews][route-views].

[ripe-ris]: https://www.ripe.net/analyse/internet-measurements/routing-information-service-ris/ris-data-access/mrt-files-store
[route-views]: http://archive.routeviews.org/

This Rust library provides SDK access to the BGPKIT Broker API as well as a command-line tool to start a self-hosted broker instance. 

Currently deployed BGPKIT Broker API is available at <https://api.bgpkit.com/docs>.


## Broker Rust SDK

### Usage

Add the following dependency line to your project's `Cargo.toml` file:
```yaml
bgpkit-broker = "0.7.0-alpha.1"
```

### Example

You can run the follow example with `cargo run --example query` ([source code](./examples/query.rs)).

```rust
use bgpkit_broker::{BgpkitBroker, BrokerItem};

pub fn main() {
    let broker = BgpkitBroker::new()
        .ts_start("1634693400")
        .ts_end("1634693400");

    // method 1: create iterator from reference (so that you can reuse the broker object)
    // same as `&broker.into_iter()`
    for item in &broker {
        println!("{}", item);
    }

    // method 2: create iterator from the broker object (taking ownership)
    let items = broker.into_iter().collect::<Vec<BrokerItem>>();

    assert_eq!(items.len(), 106);
}
```

## `bgpkit-broker` CLI Tool


## Contribution

### Issues and Pull Requests

If you found any issues of this Rust library or would like to contribute to the code base, please feel free to open an 
issue or pull request. Code or documentation issues/PRs are both welcome.

### Data Provider

If you have publicly available data and want to be indexed BGPKIT Broker service, please send us an email at
data@bgpkit.com. Our back-end service is designed to be flexible and should be able to adapt to most data archiving
approaches.

## On-premise Deployment

We provide service to allow companies to host their own BGP Broker backend on-premise to allow maximum
performance and customization. If you are interested in deploying one, please contact us at contact@bgpkit.com.

## Built with ❤️ by BGPKIT Team

<a href="https://bgpkit.com"><img src="https://bgpkit.com/Original%20Logo%20Cropped.png" alt="https://bgpkit.com/favicon.ico" width="200"/></a>
