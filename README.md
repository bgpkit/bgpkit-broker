# BGPKIT Broker

[![Crates.io][crates-badge]][crates-url]
[![MIT licensed][mit-badge]][mit-url]
[![Twitter][twitter-badge]][twitter-url]


[crates-badge]: https://img.shields.io/crates/v/bgpkit-broker.svg
[crates-url]: https://crates.io/crates/bgpkit-broker
[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: https://github.com/bgpkit/bgpkit-broker/blob/main/LICENSE
[twitter-badge]: https://shields.io/badge/Follow-lightgrey?logo=twitter&style=social
[twitter-url]: https://twitter.com/bgpkit

[BGPKIT](https://bgpkit.com) Broker is a online data API service that allows users to search for publicly available BGP archive
files by time, collector, project, or data type. The service indexes the archives in close to real-time (delay is
less than 5 minutes). Currently, we are indexing BGP table dump and updates files from RIPE RIS and RouteViews.

This Rust library provides access to the BGPKIT Broker API with the capability to search and paginate results. 

## Example

```rust
use bgpkit_broker::{BgpkitBroker, BrokerItem, QueryParams};

let mut params = QueryParams::new();
params = params.start_ts(1634693400);
params = params.end_ts(1634693400);
params = params.page_size(10);
params = params.page(2);

let mut broker = BgpkitBroker::new("https://api.broker.bgpkit.com/v1");
broker.set_params(&params);

// method 1: create iterator from reference (so that you can reuse the broker object)
// same as `&broker.into_iter()`
for item in &broker {
println!("{:?}", item);
}

// method 2: create iterator from the broker object (taking ownership)
let items = broker.into_iter().collect::<Vec<BrokerItem>>();

assert_eq!(items.len(), 48);
```


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

BGPKIT is a small-team start-up that focus on building the best tooling for BGP data in Rust. We have 10 years of 
experience working with BGP data and believe that our work can enable more companies to start keeping tracks of BGP data
on their own turf. Learn more about what services we provide at https://bgpkit.com.

<a href="https://bgpkit.com"><img src="https://bgpkit.com/Original%20Logo%20Cropped.png" alt="https://bgpkit.com/favicon.ico" width="200"/></a>
