# Changelog

All notable changes to this project will be documented in this file.

## Unreleased changes

### Breaking Changes

None - This release maintains API compatibility with previous versions.

### Features

* **Shortcuts module**: Added convenience methods for common BGP data queries
    * `daily_ribs()` - Filter RIB files captured at midnight (00:00:00) for daily snapshots
    * `recent_updates(hours)` - Get BGP update files from the last N hours 
    * `most_diverse_collectors(n, project)` - Find collectors with maximum ASN diversity using greedy algorithm
    * All shortcuts integrate seamlessly with existing filtering methods and support method chaining
    * Enhanced diversity algorithm selects collectors based on unique ASN coverage from full-feed peers
    * Project filtering supported for targeted RouteViews or RIPE RIS analysis

### Code improvements

* **Configuration validation**: Restructured parameter validation for better error handling
    * Moved validation from configuration time to query execution time
    * Added `validate_configuration()` method with comprehensive parameter checking
    * Validation includes timestamps, collectors, projects, data types, page numbers, and page sizes
    * Provides detailed error messages with valid options for invalid parameters
    * Maintains method chaining simplicity while ensuring data correctness at query time

* **Timestamp parsing**: Enhanced timestamp parsing with timezone support and better validation
    * Added support for RFC3339 timestamps with timezone offsets (e.g., `2022-01-01T00:00:00+00:00`, `2022-01-01T05:00:00-05:00`)
    * Support for pure dates (e.g., `2022-01-01`), Unix timestamps, RFC3339 formats, and various date separators
    * Internal `parse_timestamp` function now returns `DateTime<Utc>` with proper timezone conversion
    * Validation occurs at query time with helpful error messages for invalid timestamp formats
    * Pure dates automatically converted to start-of-day UTC timestamps

* **Database tests**: Updated database tests to use temporary files with proper cleanup
    * Replace hardcoded test database paths with unique temporary file paths using system temp directory
    * Add automatic cleanup of SQLite database files including WAL and SHM files
    * Improve test isolation and prevent interference between test runs
    * Tests now suitable for CI/CD environments without leaving leftover files

## v0.7.11 - 2025-04-08

### Highlights

* NATS notifier is now configured via the following env variables
    * `BGPKIT_BROKER_NATS_URL`: the URL for the NATS server, such as `nats.broker.bgpkit.com`
    * `BGPKIT_BROKER_NATS_USER`: NATS server user name
    * `BGPKIT_BROKER_NATS_PASSWORD`: NATS server password
    * `BGPKIT_BROKER_NATS_ROOT_SUBJECT`: NATS server root subject, such as `public.broker`

## v0.7.10 - 2025-03-26

### Highlights

* add `route-views8` collector
* add `/missing_collectors` endpoint to check for collectors that have not been added yet
* remove `/docs` and `utopia` dependency to remove clutter
* freshen up dependencies

### Bug fixes

* fixed an issue where incorrectly formatted timestring may cause the API to panic

## v0.7.9 - 2025-03-24

### Highlights

* `bgpkit-broker serve` and `bgpkit-broker backup` commands now runs SQLite `ANALYZE` command to once to ensure the
  performance is up to date to all the recently inserted data.

## v0.7.8 - 2025-03-20

### Highlights

* `bgpkit-broker backup` command now supports a bootstrapping source database
    * this is useful to set up independent backup executions separate from the running API services

## v0.7.7 - 2025-03-07

### Highlights

* Fix installation instruction for cargo
    * Change `@0.7` to `@^0.7` to correctly use the latest `v0.7.x` version.
* Add recently added RouteViews collectors
    * new collectors are `interlan.otp` (Romania),`kinx.icn` (South Korea), and `namex.fco` (Italy)
    * users update the version to `v0.7.7` can run the same `bgpkit-broker update` command to automatically bootstrap
      data for these collectors

## v0.7.6 - 2024-10-31

### Highlights

* migrate default broker API endpoint to `https://api.bgpkit.com/v3/broker`
    * Full API docs is available at `https://api.bgpkit.com/docs`
* add `get_peers` to `BgpkitBroker` struct
    * fetches the list of peers for a given collector
    * allow specifying filters the same way as querying MRT files
    * available filter functions include:
        * `.peers_asn(ASN)`
        * `.peers_ip(IP)`
        * `.collector_id(COLLECTOR_ID)`
        * `.peers_only_full_feed(TRUE/FALSE)`
    * returns `Vec<BrokerPeer>`

```rust
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
```

## v0.7.5 - 2024-08-23

### [NEW] deploy at fly.io

* add a deployment config file for fly.io

### Bugfix

* fix an issue where the API returns URL with additional `/bgpdata` for some new route-views collectors.

### Documentation

* improved documentation on deploying bgpkit-broker services.

## v0.7.4 - 2024-08-22

### Highlights

* add a new 30s timeout for fetching web page content for the crawler
    * the async `reqwest::get` function by default does not have a timeout and thus we could potentially stuck waiting
      forever.
* add new `max_delay_secs` parameter to the `/health` endpoint to allow user specify a maximum delay in seconds and
  the API will return error code 503 if the delay for the latest file timestamp (any file) is greater than the specified
  value.
    * this allows better monitoring for the API's health without sending heartbeats.

## v0.7.3 - 2024-08-14

### Hotfix

* fix an issue where the main thread waits for updater thread and never starts the API thread

## v0.7.2 - 2024-08-13

### Highlights

* panic if the cli updater thread failed for some reason
    * previously, the failed thread goes silently, and the main process continues running
* reduce logging if not configuring NATS notifier
    * previously, the missing notifier message appear at every fetch

## v0.7.1 - 2024-08-05

### Highlights

* updated the `bgpkit-broker update` command to allow bootstrapping new collectors on demand
* a number of new RouteViews collectors have been added
    * `amsix.ams` `cix.atl` `decix.jhb` `iraq-ixp.bgw` `pacwave.lax` `pit.scl` `pitmx.qro` `route-views7`
* added a new `allow_invalid_cert` function to the constructor and deprecated the `disable_ssl_check` function
    * they work the same way, but the new function name is more accurate
* constructor also checks for `ONEIO_ACCEPT_INVALID_CERTS=true` environment variable to allow invalid certs (not
  recommended, use at your own risk)
* improved project filter. it now matches the name of the collector to project names
    * this works with the newly added RouteViews collectors whose names do not prefix with `route-views`

## v0.7.0 - 2024-06-18

### [NEW] NATS notification

Added new-file notification by publishing newly indexed BGP MRT file meta information in JSON format to user-specified
NATS server.

The NATS URL and credentials is configured via environment variables:

* `BGPKIT_BROKER_NATS_URL` like `nats://nats.bgpkit.com:4222` (default)
* `BGPKIT_BROKER_NATS_ROOT_SUBJECT` such as `public.broker.` (default)

The notification is published to `public.broker.{PROJECT}.{COLLECTOR}.{DATA_TYPE}` where

* `PROJECT` is `riperis` or `route-views`
* `COLLECTOR` is the route collector IDs like `rrc00` or `route-views2`
* `DATA_TYPE` is `rib` or `updates`

Example of what a subscriber can be notified for:

* `public.broker.>`: any new MRT files
* `public.broker.route-views.>`: new RouteViews updates files
* `public.broker.rrc00.rib`: new RIPE RIS rrc00 RIB dump files
* `public.broker.*.rib`: any new RIB dump files

This PR also adds a new subcommand `bgpkit-broker live` that listens to the specified NATS server for new file
notification.

```
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

### [NEW] `bgpkit-broker doctor` subcommand

Added `bgpkit-broker doctor` subcommand that checks the broker instance status and missing collectors.

```text
Check broker instance health and missing collectors

Usage: bgpkit-broker doctor [OPTIONS]

Options:
      --no-log     disable logging
      --env <ENV>  
  -h, --help       Print help
  -V, --version    Print version
```

Example output:

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

### [NEW] Heartbeat URL support

If `BGPKIT_BROKER_HEARTBEAT_URL` environment is set, when running the `bgpkit-broker serve` subcommand, the instance
will periodically send a GET request to the configured heartbeat URL.

### [NEW] `bgpkit-broker latest` subcommand

Added `latest` subcommand to CLI to display latest MRT files for all collectors.

### Developer experience improvements

- add `.is_rib()` to `BrokerItem` struct
- add strict ordering definition for `BrokerItem` struct

An array of `BrokerItem`s can be sorted with the following order:

1. smaller timestamp before larger timestamp
2. RIB before updates
3. then alphabetical order on collector ID (route-views before rrc)

### Breaking changes

- switch to `rustls` as the default TLS backend

## V0.6.1

### What's Changed

* switch http lib to reqwest from ureq by @digizeph in https://github.com/bgpkit/bgpkit-broker/pull/20

**Full Changelog**: https://github.com/bgpkit/bgpkit-broker/compare/v0.6.0...v0.6.1