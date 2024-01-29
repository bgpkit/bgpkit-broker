# Changelog

All notable changes to this project will be documented in this file.

## v0.7.0-beta.5 - 2024-01-29

### Breaking changes

- switch to `rustls` as the default TLS backend

## v0.7.0-beta.4 - 2024-01-18

### CLI addtion

- added `latest` subcommand to CLI to display latest MRT files for all collectors

### Developer experience improvements

- add `.is_rib()` to `BrokerItem` struct
- add strict ordering definition for `BrokerItem` struct

An array of `BrokerItem`s can be sorted with the following order:
1. smaller timestamp before larger timestamp
2. RIB before updates
3. then alphabetical order on collector ID (route-views before rrc)

## V0.6.1

### What's Changed
* switch http lib to reqwest from ureq by @digizeph in https://github.com/bgpkit/bgpkit-broker/pull/20


**Full Changelog**: https://github.com/bgpkit/bgpkit-broker/compare/v0.6.0...v0.6.1