## V0.7.0

### Major Changes

- V0.7.0 introduces new CLI deployment solution, `bgpkit-broker serve`, which is a standalone HTTP server that serves the Broker content via RESTful API.

### API Changes

- New API now deployed at `https://api.broker.bgpkit.com/v3`

### SDK Changes

- set `BGPKIT_BROKER_URL` environment variable to change broker instance