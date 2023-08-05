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
Current BGPKIT Broker API is available at <https://api.bgpkit.com/docs>.

BGPKIT Broker is used in production at [Cloudflare Radar][radar] powering its [routing page][routing] and projects like [BGP hijack detection]() and [route leak detection](https://blog.cloudflare.com/route-leak-detection-with-cloudflare-radar/).

[radar]: https://radar.cloudflare.com/
[route-leak]: https://blog.cloudflare.com/route-leak-detection-with-cloudflare-radar/
[hijack]: https://blog.cloudflare.com/bgp-hijack-detection/
[routing]: https://blog.cloudflare.com/radar-routing/

## Broker Rust SDK

### Usage

Add the following dependency line to your project's `Cargo.toml` file:
```yaml
bgpkit-broker = "0.7.0-alpha.3"
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

`bgpkit-broker` is a command-line application that packages many functionalities to allow users to self-host a BGPKIT Broker instance with ease.

Install with `cargo install bgpkit-broker@0.7.0-alpha.3 --features cli` or check out the main branch and run `cargo install --path . --features cli`.

`bgpkit-broker` has the following subcommands:

```text
A library and command-line to provide indexing and searching functionalities for public BGP data archive files over time.


Usage: bgpkit-broker [OPTIONS] <COMMAND>

Commands:
  serve      Serve the Broker content via RESTful API
  update     Update the Broker database
  config     Print out current configuration
  bootstrap  Bootstrap the Broker database
  backup     Export broker database to parquet file
  search     Search MRT files in Broker db
  help       Print this message or the help of the given subcommand(s)

Options:
      --no-log             disable logging
      --bootstrap-parquet  bootstrap from parquet file instead of DuckDB file
  -h, --help               Print help
  -V, --version            Print version
```

### `serve`
`bgpkit-broker serve` is the main command to start the BGPKIT Broker service. It will start a web server that serves the API endpoints. It will also periodically update the local database unless the `--no-update` flag is set.

```text
Serve the Broker content via RESTful API

Usage: bgpkit-broker serve [OPTIONS]

Options:
  -i, --update-interval <UPDATE_INTERVAL>  update interval in seconds [default: 300]
      --no-log                             disable logging
      --bootstrap-parquet                  bootstrap from parquet file instead of DuckDB file
  -h, --host <HOST>                        host address [default: 0.0.0.0]
  -p, --port <PORT>                        port number [default: 40064]
  -r, --root <ROOT>                        root path, useful for configuring docs UI [default: /]
      --no-update                          disable updater service
      --no-api                             disable API service
      --full-bootstrap                     do a full database bootstrap from duckdb or parquet file
  -h, --help                               Print help
  -V, --version                            Print version
```

### `update`
`bgpkit-broker update` triggers a local database update manually. This command **cannot** be run at the same time as `serve` because the active API will lock the database file.

```text
Update the Broker database

Usage: bgpkit-broker update [OPTIONS]

Options:
      --no-log             disable logging
      --bootstrap-parquet  bootstrap from parquet file instead of DuckDB file
  -h, --help               Print help
  -V, --version            Print version
```

### `config`
`bgpkit-broker config` displays current configuration, e.g. local database path, update interval, etc.

```text
Print out current configuration

Usage: bgpkit-broker config [OPTIONS]

Options:
      --no-log             disable logging
      --bootstrap-parquet  bootstrap from parquet file instead of DuckDB file
  -h, --help               Print help
  -V, --version            Print version
```

### `backup` 
`bgpkit-broker update` runs a database backup and export the database to a duckdb file and a parquet file. This *can* be run while `serve` is running.

```text
Export broker database to parquet file

Usage: bgpkit-broker backup [OPTIONS]

Options:
      --no-log             disable logging
      --bootstrap-parquet  bootstrap from parquet file instead of DuckDB file
  -h, --help               Print help
  -V, --version            Print version
```

### `search` 
`bgpkit-broker search` queries for MRT files using the default production API unless specified otherwise.

```text
Search MRT files in Broker db

Usage: bgpkit-broker search [OPTIONS]

Options:
      --no-log                       disable logging
  -t, --ts-start <TS_START>          Start timestamp
      --bootstrap-parquet            bootstrap from parquet file instead of DuckDB file
  -T, --ts-end <TS_END>              End timestamp
  -p, --project <PROJECT>            filter by route collector projects, i.e. `route-views` or `riperis`
  -c, --collector-id <COLLECTOR_ID>  filter by collector IDs, e.g. 'rrc00', 'route-views2. use comma to separate multiple collectors
  -d, --data-type <DATA_TYPE>        filter by data types, i.e. 'update', 'rib'
      --page <PAGE>                  page number
      --page-size <PAGE_SIZE>        page size
  -u, --url <URL>                    
  -j, --json                         print out search results in JSON format instead of Markdown table
  -h, --help                         Print help
  -V, --version                      Print version
```

## Data Provider

If you have publicly available data and want to be indexed BGPKIT Broker service, please send us an email at
data@bgpkit.com. Our back-end service is designed to be flexible and should be able to adapt to most data archiving
approaches.

<a href="https://bgpkit.com"><img src="https://bgpkit.com/Original%20Logo%20Cropped.png" alt="https://bgpkit.com/favicon.ico" width="200"/></a>
