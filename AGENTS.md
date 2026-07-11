# PROJECT KNOWLEDGE BASE

## Overview

BGPKIT Broker is a Rust SDK plus a feature-gated self-hosted broker server.
Default builds cover the SDK only; the binary and local DB/crawler/API paths require the `cli` feature.

## Structure

```text
.
├── src/lib.rs            # SDK entry point: BgpkitBroker builder, BrokerItemIterator
├── src/query.rs          # QueryParams, SortOrder, BrokerCollector, BrokerItemType
├── src/shortcuts.rs      # Daily-RIB and snapshot shortcut helpers
├── src/sse.rs            # Live SSE subscription client (feature: sse)
├── src/item.rs           # BrokerItem struct
├── src/peer.rs           # BrokerPeer struct
├── src/error.rs          # BrokerError enum
├── src/collector.rs      # Embedded default collector inventory (JSON config)
├── src/config.rs         # BrokerConfig: crawler/backup/heartbeat/database settings
├── src/cli/              # Binary entry, Axum API, bootstrap, backup, CLI commands
│   ├── main.rs           # clap Commands enum: serve/update/bootstrap/backup/search/latest/peers/live/doctor
│   ├── api.rs            # Axum routes: /search /latest /health /missing_collectors /events /metrics
│   ├── bootstrap.rs      # Download prebuilt SQLite DB from spaces.bgpkit.org
│   ├── backup.rs         # S3/sqlite3 backup and restore
│   └── utils.rs          # Missing-collector detection helpers
├── src/db/               # SQLite layer (feature: backend)
│   ├── mod.rs            # Schema init, search query, insert_items, insert_collector, files_view
│   ├── latest_files.rs   # Latest-per-collector tracking, bootstrap, staleness checks
│   ├── meta.rs           # Update metadata, entry count, cleanup
│   └── utils.rs          # infer_url — reconstructs MRT file URLs from collector + timestamp
├── src/crawler/          # RouteViews and RIPE RIS HTML directory crawlers
│   ├── common.rs         # Shared month-list parsing, link extraction, concurrency
│   ├── routeviews.rs     # RV crawler (15-min update interval)
│   ├── riperis.rs        # RIPE RIS crawler (5-min update interval)
│   └── mod.rs            # crawl_collector dispatcher
├── examples/             # SDK usage and operational examples
├── migration/            # One-off SQLite→Postgres migration scripts (not part of crate)
├── deployment/           # Nomad job specs for API and backup
├── Dockerfile            # Multi-stage build with --all-features
├── docker-compose.yaml   # Single-container deployment
└── .github/workflows/    # CI (rust.yml) and release (release.yml)
```

## Where To Look

| Task | Location | Notes |
|------|----------|-------|
| SDK query behavior | `src/lib.rs` | Builder methods are cheap; validation happens at iteration/query time |
| URL reconstruction | `src/db/utils.rs` | `infer_url` strips `bgpdata` suffix and rebuilds path — URL stored in DB is never used directly |
| CLI/server startup | `src/cli/main.rs` | Binary lives here, not `src/main.rs` |
| HTTP routes | `src/cli/api.rs` | Default routes are root-based unless `--root` is set |
| DB schema and inserts | `src/db/mod.rs` | `CREATE TABLE IF NOT EXISTS` only — no schema migration on boot |
| Collector insert | `src/db/mod.rs` | `insert_collector` is a no-op if collector name already exists (won't update URL) |
| Latest/meta queries | `src/db/latest_files.rs`, `src/db/meta.rs` | Watch empty-DB edge cases |
| Collector inventory | `src/collector.rs` | Embedded JSON; `load_collectors()` returns the full list |
| Crawl timing rules | `src/crawler/routeviews.rs`, `src/crawler/riperis.rs` | RV updates 15m, RIS updates 5m |
| Runtime configuration | `src/config.rs` | `BrokerConfig::from_env()` — all env-var reads centralized here |
| Deprecated collectors | `src/cli/main.rs` | `DEPRECATED_COLLECTORS` array filtered in `doctor` command |

## Conventions

- Default feature set is empty; test both SDK-only and `cli`/all-feature paths when touching shared code.
- The `bgpkit-broker` binary is feature-gated in `Cargo.toml`; `cargo build` alone does not exercise server code.
- CLI `search`, `latest`, and `peers` are remote API clients; they do not read the local SQLite DB.
- Collector validation is intentionally permissive; unknown collector IDs should not be rejected eagerly.
- All runtime configuration flows through `BrokerConfig` (`src/config.rs`); do not scatter env-var reads across modules.
- Prefer factual changelog and commit language; avoid promotional wording.
- When updating the supported RFC or collector list, update `src/lib.rs` crate-level doc comments and regenerate `README.md` with `cargo readme > README.md` if applicable.

## CI Requirements

**All CI checks must pass locally before pushing or creating a PR.**

The CI workflow (`.github/workflows/rust.yml`) runs four jobs — run all of them locally before pushing:

```bash
cargo build --verbose                    # SDK-only build
cargo build --features cli --verbose     # Full build with CLI/server
cargo test --no-default-features --verbose  # SDK tests (no network calls in CI)
cargo clippy --all-features -- -D warnings  # Lint with warnings as errors
```

Key points:
- `clippy` uses `-D warnings` — any warning fails CI. The crate also denies `unwrap_used` in production paths via `[lints.clippy]` in `Cargo.toml`.
- Tests run with `--no-default-features` (SDK only). CLI/server tests require the `cli` feature and are not run in CI.
- No pre-push hooks are configured; the developer is responsible for running these checks.

## Anti-Patterns

- Do not assume all RouteViews collectors start with `route-views` (e.g. `amsix.ams`, `decix.fra`).
- Do not use `disable_ssl_check`; use `accept_invalid_certs` instead (the former is deprecated since 0.7.1).
- Do not reintroduce `unwrap()` in production paths; clippy denies it (`unwrap_used = "deny"` in `Cargo.toml`).
- Do not scatter new env-var reads across modules when config can be threaded through `BrokerConfig`.
- Do not document `/v3/...` as the default local route prefix unless `--root` is actually set.
- Do not assume changing `collector.rs` updates existing databases — `insert_collector` is a no-op for existing collectors, and there is no schema migration on boot.

## Commands

```bash
# CI-equivalent checks (run all before pushing)
cargo build --verbose
cargo build --features cli --verbose
cargo test --no-default-features --verbose
cargo clippy --all-features -- -D warnings

# Full feature build (for local server testing)
cargo build --features cli --verbose

# Run server locally
cargo run --features cli -- serve broker.sqlite3 --bootstrap

# Regenerate README from crate-level docs
cargo readme > README.md
```

## Notes

- The actual CI file is `.github/workflows/rust.yml`; release is `.github/workflows/release.yml`.
- `serve` uses Axum and may wait on updater readiness when updates are enabled.
- `/health` only checks DB usability by default; freshness checks require `max_delay_secs`.
- Backup/restore relies on the external `sqlite3` CLI binary.
- Bootstrap downloads a prebuilt ~3GB SQLite DB from `https://spaces.bgpkit.org/broker/bgpkit_broker.sqlite3`.
- The `files` table does not store URLs — they are always reconstructed via `infer_url()` at query time from the collector's base URL.
- Deprecated collectors: `rrc02`, `rrc08`, `rrc09`, `route-views.jinx`, `route-views.siex` (filtered in `doctor` command).
- `migration/` contains a one-off SQLite→Postgres migration with a large DB file; not part of the crate build.
