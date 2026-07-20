# Broker SQLite → PostgreSQL bootstrap

This utility imports a Broker SQLite catalog into PostgreSQL and creates a Broker-compatible catalog:

- `mrt`: RouteViews/RIPE RIS collectors, archive-file observations, latest-file read model, and crawler update metadata;
- `api`: Broker-compatible search/latest views.

It indexes archive metadata only. Raw MRT payloads stay in archive/object storage. `mrt.file` is an observed catalog entry, not a reachability or routing-forwarding claim.

## Data model

The PostgreSQL schema deliberately mirrors Broker’s operational model rather than adding a separate ingestion-provenance layer:

- `mrt.file` holds one catalog entry per `(timestamp, collector, type)`;
- `mrt.latest_file` is the newest entry per collector/type and is refreshed after a bootstrap or incrementally maintained by the crawler;
- `mrt.update_meta` mirrors SQLite’s `meta` table: update timestamp, crawler duration, and inserted-row count.

The importer preserves zero sizes (`0` remains `0`); it does not reinterpret zero as unknown.

The compatibility view is `api.mrt_file_search`:

| SQLite `files_view` | PostgreSQL view |
|---|---|
| `timestamp` | `timestamp` (`timestamptz`) |
| `type` | `type` |
| `collector_name`, `collector_url`, `project_name` | same names |
| `rough_size`, `exact_size`, `updates_interval` | same names |

## Bootstrap

```bash
python3 migration/postgres_bootstrap/bootstrap.py \
  /var/lib/bgpkit/broker/bgpkit_broker.sqlite3 \
  --database-url "$POSTGRES_URL" \
  --batch-size 100000 --reset
```

`--reset` drops and recreates only the `api` and `mrt` schemas. Use it for a fresh catalog bootstrap, not an existing database whose catalog contents must be retained.

The importer exits non-zero if SQLite/PostgreSQL file or collector counts differ. It creates query indexes and runs `ANALYZE` after import.

## Runtime backend selection

SQLite remains the default:

```bash
bgpkit-broker serve /var/lib/bgpkit/broker/bgpkit_broker.sqlite3
```

PostgreSQL is an explicit opt-in. A single URL is the only runtime connection setting; the same configuration supports an API-only process (`--no-update`) or an API+crawler process (default):

```bash
# Prefer an environment variable or secret manager for the URL.
# Combined crawler + API service using the environment selector.
BGPKIT_BROKER_POSTGRES_URL="$POSTGRES_URL" bgpkit-broker serve

# Or pass a PostgreSQL URL as the database target.
bgpkit-broker serve "pg://broker@db.example/broker"

# API-only deployment: disable the updater.
BGPKIT_BROKER_POSTGRES_URL="$POSTGRES_URL" bgpkit-broker serve --no-update
```

The runtime dispatches through `DatabaseBackend`: `LocalBrokerDb` for SQLite and `PostgresDb` for PostgreSQL. SQLite file backups are intentionally skipped for PostgreSQL deployments; use PostgreSQL-native backup/replication instead. No credential is stored in this repository.

## Scope boundary

The schema/catalogue indexes archive metadata only. Raw MRT payloads remain in archive/object storage.
