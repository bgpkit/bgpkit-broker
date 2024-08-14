# Changelog

All notable changes to this project will be documented in this file.

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