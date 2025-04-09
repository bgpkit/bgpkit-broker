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

[BGPKIT Broker](https://bgpkit.com/broker) is an online data API service that allows users to search for publicly
available BGP archive files by time, collector, project, or data type. The service indexes the archives in close to
real-time (delay is less than 5 minutes). Currently, we are indexing BGP table dump and updates files
from [RIPE RIS][ripe-ris] and [RouteViews][route-views].

[ripe-ris]: https://www.ripe.net/analyse/internet-measurements/routing-information-service-ris/ris-data-access/mrt-files-store

[route-views]: http://archive.routeviews.org/

This Rust library provides SDK access to the BGPKIT Broker API as well as a command-line tool to start a self-hosted
broker instance.
The current BGPKIT Broker API is available at <https://api.bgpkit.com/docs>.

BGPKIT Broker is used in production at [Cloudflare Radar][radar] powering its [routing page][routing] and projects
like [BGP hijack detection]()
and [route leak detection](https://blog.cloudflare.com/route-leak-detection-with-cloudflare-radar/).

[radar]: https://radar.cloudflare.com/

[route-leak]: https://blog.cloudflare.com/route-leak-detection-with-cloudflare-radar/

[hijack]: https://blog.cloudflare.com/bgp-hijack-detection/

[routing]: https://blog.cloudflare.com/radar-routing/

## Broker Rust SDK

### Usage

Add the following dependency line to your project's `Cargo.toml` file:

```yaml
bgpkit-broker = "0.7"
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

`bgpkit-broker` is a command-line application that packages many functionalities to allow users to self-host a BGPKIT
Broker instance with ease.

### Install

Install with `cargo install bgpkit-broker@^0.7 --features cli` or check out the main branch and
run `cargo install --path . --features cli`.

If you are in a macOS environment, you can also use homebrew to install the pre-compiled binary (universal):

```
brew install bgpkit/tap/bgpkit-broker
```

### Usage

`bgpkit-broker` has the following subcommands:

```text
  A library and command-line to provide indexing and searching functionalities for public BGP data archive files over time.


Usage: bgpkit-broker [OPTIONS] <COMMAND>

Commands:
  serve      Serve the Broker content via RESTful API
  update     Update the Broker database
  bootstrap  Bootstrap the broker database
  backup     Backup Broker database
  search     Search MRT files in Broker db
  latest     Display latest MRT files indexed
  peers      List public BGP collector peers
  live       Streaming live from a broker NATS server
  doctor     Check broker instance health and missing collectors
  help       Print this message or the help of the given subcommand(s)

Options:
      --no-log     disable logging
      --env <ENV>  
  -h, --help       Print help
  -V, --version    Print version
```

#### `serve`

`bgpkit-broker serve` is the main command to start the BGPKIT Broker service. It will start a web server that serves the
API endpoints. It will also periodically update the local database unless the `--no-update` flag is set.

```text
  Serve the Broker content via RESTful API

Usage: bgpkit-broker serve [OPTIONS] <DB_PATH>

Arguments:
  <DB_PATH>  broker db file location

Options:
  -i, --update-interval <UPDATE_INTERVAL>  update interval in seconds [default: 300]
      --no-log                             disable logging
  -b, --bootstrap                          bootstrap the database if it does not exist
      --env <ENV>                          
  -s, --silent                             disable bootstrap progress bar
  -h, --host <HOST>                        host address [default: 0.0.0.0]
  -p, --port <PORT>                        port number [default: 40064]
  -r, --root <ROOT>                        root path, useful for configuring docs UI [default: /]
      --no-update                          disable updater service
      --no-api                             disable API service
  -h, --help                               Print help
  -V, --version                            Print version
```

For sending NATS notifications, there are three environment variables that need to be set:

* `BGPKIT_BROKER_NATS_URL`: the URL for the NATS server, such as `nats.broker.bgpkit.com`
* `BGPKIT_BROKER_NATS_USER`: NATS server user name
* `BGPKIT_BROKER_NATS_PASSWORD`: NATS server password
* `BGPKIT_BROKER_NATS_ROOT_SUBJECT`: NATS server root subject, such as `public.broker`

#### `update`

`bgpkit-broker update` triggers a local database update manually. This command **cannot** be run at the same time
as `serve` because the active API will lock the database file.

```text
Update the Broker database

Usage: bgpkit-broker update [OPTIONS] <DB_PATH>

Arguments:
  <DB_PATH>  broker db file location

Options:
  -d, --days <DAYS>  force number of days to look back. by default resume from the latest available data time
      --no-log       disable logging
      --env <ENV>    
  -h, --help         Print help
  -V, --version      Print version
```

#### `backup`

`bgpkit-broker update` runs a database backup and export the database to a duckdb file and a parquet file. This *can* be
run while `serve` is running.

```text
  Backup Broker database

Usage: bgpkit-broker backup [OPTIONS] <FROM> <TO>

Arguments:
  <FROM>  source database location
  <TO>    remote database location

Options:
  -f, --force                              force writing backup file to existing file if specified
      --no-log                             disable logging
      --env <ENV>                          
  -s, --sqlite-cmd-path <SQLITE_CMD_PATH>  specify sqlite3 command path
  -h, --help                               Print help
  -V, --version                            Print version
```

#### `search`

`bgpkit-broker search` queries for MRT files using the default production API unless specified otherwise.

```text
  Search MRT files in Broker db

Usage: bgpkit-broker search [OPTIONS]

Options:
      --no-log                       disable logging
  -t, --ts-start <TS_START>          Start timestamp
      --env <ENV>                    
  -T, --ts-end <TS_END>              End timestamp
  -d, --duration <DURATION>          Duration string, e.g. 1 hour
  -p, --project <PROJECT>            filter by route collector projects, i.e. `route-views` or `riperis`
  -c, --collector-id <COLLECTOR_ID>  filter by collector IDs, e.g. 'rrc00', 'route-views2. use comma to separate multiple collectors
  -d, --data-type <DATA_TYPE>        filter by data types, i.e. 'updates', 'rib'
      --page <PAGE>                  page number
      --page-size <PAGE_SIZE>        page size
  -u, --url <URL>                    Specify broker endpoint
  -j, --json                         Print out search results in JSON format instead of Markdown table
  -h, --help                         Print help
  -V, --version                      Print version
```

#### `latest`

`bgpkit-broker latest` queries for the latest MRT files of each route collector from RouteViews and RIPE RIS.

- use `--collector COLLECTOR` to narrow down the display of the collector.
- use `--outdated` flag to toggle showing only the files from collectors that have not been generating data timely
- use `--json` flag to output to a JSON file instead of a Markdown table

```text
  Display latest MRT files indexed

Usage: bgpkit-broker latest [OPTIONS]

Options:
  -c, --collector <COLLECTOR>  filter by collector ID
      --no-log                 disable logging
      --env <ENV>              
  -u, --url <URL>              Specify broker endpoint
  -o, --outdated               Showing only latest items that are outdated
  -j, --json                   Print out search results in JSON format instead of Markdown table
  -h, --help                   Print help
  -V, --version                Print version
```

#### `live`

Streaming live from a broker NATS server.

```text
Streaming live from a broker NATS server

Usage: bgpkit-broker live [OPTIONS]

Options:
      --no-log             disable logging
  -u, --url <URL>          URL to NATS server, e.g. nats://localhost:4222. If not specified, will try to read from BGPKIT_BROKER_NATS_URL env variable
      --env <ENV>          
  -s, --subject <SUBJECT>  Subject to subscribe to, default to public.broker.>
  -p, --pretty             Pretty print JSON output
  -h, --help               Print help
  -V, --version            Print version
```

#### `peers`

List public BGP collector peer information.

```text
List public BGP collector peers

Usage: bgpkit-broker peers [OPTIONS]

Options:
  -c, --collector <COLLECTOR>  filter by collector ID
      --no-log                 disable logging
  -a, --peer-asn <PEER_ASN>    filter by peer AS number
      --env <ENV>              
  -i, --peer-ip <PEER_IP>      filter by peer IP address
  -f, --full-feed-only         show only full-feed peers
  -j, --json                   Print out search results in JSON format instead of Markdown table
  -h, --help                   Print help
  -V, --version                Print version
```

For example, the command `bgpkit-broker peers --collector rrc00 --full-feed-only` lists all full-feed peers from
collector `rrc00`:

```text
bgpkit-broker peers --collector rrc00 --full-feed-only
| date       | ip                                   | asn    | collector | num_v4_pfxs | num_v6_pfxs | num_connected_asns |
|------------|--------------------------------------|--------|-----------|-------------|-------------|--------------------|
| 2024-11-01 | 103.102.5.1                          | 131477 | rrc00     | 964311      | 0           | 3                  |
| 2024-11-01 | 12.0.1.63                            | 7018   | rrc00     | 950205      | 0           | 2316               |
| 2024-11-01 | 154.11.12.212                        | 852    | rrc00     | 952822      | 0           | 482                |
| 2024-11-01 | 161.129.152.2                        | 13830  | rrc00     | 948244      | 0           | 120                |
| 2024-11-01 | 165.16.221.66                        | 37721  | rrc00     | 833893      | 214125      | 4515               |
| 2024-11-01 | 165.254.255.2                        | 15562  | rrc00     | 951002      | 0           | 2                  |
| 2024-11-01 | 176.12.110.8                         | 50300  | rrc00     | 955141      | 0           | 1046               |
...
```

#### `doctor`

Check broker instance health and missing collectors.

```text
Check broker instance health and missing collectors

Usage: bgpkit-broker doctor [OPTIONS]

Options:
      --no-log     disable logging
      --env <ENV>  
  -h, --help       Print help
  -V, --version    Print version
```

Example output (the data for the shown collectors are now available):

```text
checking broker instance health...
        broker instance at https://api.broker.bgpkit.com/v3 is healthy

checking for missing collectors...
missing the following collectors:
| project    | name         | country         | activated_on        | data_url                                           |
|------------|--------------|-----------------|---------------------|----------------------------------------------------|
| routeviews | decix.jhb    | Malaysia        | 2022-12-20 12:00:00 | http://archive.routeviews.org/decix.jhb/bgpdata    |
| routeviews | pacwave.lax  | United States   | 2023-03-30 12:00:00 | http://archive.routeviews.org/pacwave.lax/bgpdata  |
| routeviews | pit.scl      | Chile           | 2023-08-31 23:45:00 | http://archive.routeviews.org/pit.scl/bgpdata      |
| routeviews | amsix.ams    | The Netherlands | 2024-02-22 23:20:00 | http://archive.routeviews.org/amsix.ams/bgpdata    |
| routeviews | pitmx.qro    | Mexico          | 2024-02-23 22:15:00 | http://archive.routeviews.org/pitmx.qro/bgpdata    |
| routeviews | iraq-ixp.bgw | Iraq            | 2024-04-13 00:01:00 | http://archive.routeviews.org/iraq-ixp.bgw/bgpdata |
```

## Deployment

### Docker

You can deploy the BGPKIT Broker service using the provided Docker image. The image is available on Docker Hub
at [bgpkit/broker](https://hub.docker.com/r/bgpkit/bgpkit-broker).

To run in deattached mode (as a service):

```bash
docker run -d -p 40064:40064 bgpkit/bgpkit-broker:latest
```

To run as a service using `docker-compose`:

``` bash
docker-compose up -d
```

You can also build the Docker image from the source code:

```bash
docker build -t bgpkit/bgpkit-broker:latest .
```

### On-premises CLI

You can also start a BGPKIT Broker instance on your own server using the `bgpkit-broker` CLI tool with the following
command:

```bash
bgpkit-broker serve YOUR_SQLITE_3_FILE_PATH.sqlite3 --bootstrap --silent
```

* `YOUR_SQLITE_3_FILE_PATH.sqlite3` is the path to the SQLite3 database file.
* `--bootstrap` flag is used to bootstrap the database content from the provided daily backup database.
* `--silent` flag is used to disable the bootstrap download progress bar.

On a systemd managed OS like Debian or Ubuntu, you can also use the following service file to manage the BGPKIT Broker
service:

```ini
[Unit]
Description=BGPKIT Broker Service
After=network.target

[Service]
ExecStart=/usr/local/bin/bgpkit-broker serve /var/lib/bgpkit/broker.sqlite3
Restart=on-failure
User=root

[Install]
WantedBy=multi-user.target
```

Put this file at `/etc/systemd/system/bgpkit-broker.service` and run `systemctl daemon-reload` to reload the service
list, and then you can start the service with `systemctl start bgpkit-broker`.
To enable the service to start on boot, run `systemctl enable bgpkit-broker`.

## Data Provider

If you have publicly available data and want to be indexed BGPKIT Broker service, please email us at
data@bgpkit.com. Our back-end service is designed to be flexible and should be able to adapt to most data archiving
approaches.

<a href="https://bgpkit.com"><img src="https://bgpkit.com/Original%20Logo%20Cropped.png" alt="https://bgpkit.com/favicon.ico" width="200"/></a>
