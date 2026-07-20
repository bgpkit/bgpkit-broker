#!/usr/bin/env python3
"""Bootstrap the PostgreSQL Broker catalog from SQLite."""

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import sys
from typing import Iterator


BATCH_SIZE = 100_000
CODE_VERSION = "postgres-bootstrap-poc-v1"


def project_name(value: str) -> str:
    normalized = value.lower().replace("_", "-")
    aliases = {"riperis": "ripe-ris", "ripe-ris": "ripe-ris", "routeviews": "route-views", "route-views": "route-views"}
    try:
        return aliases[normalized]
    except KeyError as error:
        raise ValueError(f"unknown legacy Broker project: {value!r}") from error


def copy_row(row: tuple, collector_ids: dict[int, int], types: dict[int, str]) -> tuple:
    ts_epoch, old_collector_id, old_type_id, rough_size, exact_size = row
    try:
        collector_id = collector_ids[old_collector_id]
        data_type = types[old_type_id]
    except KeyError as error:
        raise ValueError(f"unmapped legacy foreign key: {error.args[0]}") from error
    if data_type not in {"rib", "updates"}:
        raise ValueError(f"unknown legacy Broker type: {data_type!r}")
    return ts_epoch, collector_id, data_type, rough_size, exact_size


def sqlite_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.execute("PRAGMA query_only = ON")
    return connection


def ensure_sqlite_shape(connection: sqlite3.Connection) -> None:
    required = {"collectors", "types", "files", "meta"}
    actual = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    missing = required - actual
    if missing:
        raise ValueError(f"SQLite source is not a Broker database; missing tables: {', '.join(sorted(missing))}")


def schema_sql() -> str:
    return (Path(__file__).parent / "schema.sql").read_text(encoding="utf-8")


def execute_schema(connection, reset: bool) -> None:
    with connection.cursor() as cursor:
        if reset:
            cursor.execute("DROP SCHEMA IF EXISTS api CASCADE; DROP SCHEMA IF EXISTS mrt CASCADE;")
        cursor.execute(schema_sql())
    connection.commit()


def import_collectors(sqlite_db: sqlite3.Connection, pg) -> tuple[dict[int, int], dict[int, str]]:
    with pg.cursor() as cursor:
        for name in ("ripe-ris", "route-views"):
            cursor.execute("INSERT INTO mrt.project (name) VALUES (%s) ON CONFLICT DO NOTHING", (name,))

        project_ids = dict(cursor.execute("SELECT name, project_id FROM mrt.project").fetchall())
        collector_ids: dict[int, int] = {}
        for legacy_id, name, url, project, interval in sqlite_db.execute(
            "SELECT id, name, url, project, updates_interval FROM collectors WHERE name IS NOT NULL ORDER BY id"
        ):
            normalized_project = project_name(project)
            cursor.execute(
                """
                INSERT INTO mrt.collector (project_id, name, base_uri, updates_interval_seconds)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (project_id, name) DO UPDATE
                    SET base_uri = EXCLUDED.base_uri,
                        updates_interval_seconds = EXCLUDED.updates_interval_seconds
                RETURNING collector_id
                """,
                (project_ids[normalized_project], name, url, interval),
            )
            collector_ids[legacy_id] = cursor.fetchone()[0]
        types = dict(sqlite_db.execute("SELECT id, name FROM types").fetchall())
    pg.commit()
    return collector_ids, types


def batches(cursor: sqlite3.Cursor, batch_size: int) -> Iterator[list[tuple]]:
    while rows := cursor.fetchmany(batch_size):
        yield rows


def copy_files(sqlite_db: sqlite3.Connection, pg, collector_ids: dict[int, int], types: dict[int, str], batch_size: int) -> tuple[int, int]:
    source = sqlite_db.cursor()
    source.execute("SELECT timestamp, collector_id, type_id, rough_size, exact_size FROM files ORDER BY timestamp, collector_id, type_id")
    seen = inserted = 0
    for batch in batches(source, batch_size):
        converted = [copy_row(row, collector_ids, types) for row in batch]
        with pg.cursor() as cursor:
            cursor.execute("CREATE TEMP TABLE broker_file_stage (ts_epoch BIGINT, collector_id BIGINT, data_type TEXT, rough_size BIGINT, exact_size BIGINT) ON COMMIT DROP")
            with cursor.copy("COPY broker_file_stage (ts_epoch, collector_id, data_type, rough_size, exact_size) FROM STDIN") as copy:
                for row in converted:
                    copy.write_row(row)
            cursor.execute(
                """
                INSERT INTO mrt.file (ts_start, collector_id, data_type, rough_size, exact_size)
                SELECT to_timestamp(ts_epoch), collector_id, data_type, rough_size, exact_size
                FROM broker_file_stage
                ON CONFLICT (ts_start, collector_id, data_type) DO NOTHING
                """
            )
            inserted += cursor.rowcount
        pg.commit()
        seen += len(converted)
        print(f"  files: {seen:,} seen / {inserted:,} inserted", flush=True)
    return seen, inserted


def refresh_latest(pg) -> int:
    with pg.cursor() as cursor:
        cursor.execute("TRUNCATE mrt.latest_file")
        cursor.execute(
            """
            INSERT INTO mrt.latest_file
                (collector_id, data_type, ts_start, rough_size, exact_size)
            SELECT DISTINCT ON (collector_id, data_type)
                collector_id, data_type, ts_start, rough_size, exact_size
            FROM mrt.file
            ORDER BY collector_id, data_type, ts_start DESC
            """
        )
        count = cursor.rowcount
    pg.commit()
    return count


def import_update_meta(sqlite_db: sqlite3.Connection, pg) -> int:
    rows = list(sqlite_db.execute("SELECT update_ts, update_duration, insert_count FROM meta"))
    if not rows:
        return 0
    with pg.cursor() as cursor:
        cursor.execute("TRUNCATE mrt.update_meta")
        with cursor.copy(
            "COPY mrt.update_meta (update_ts, update_duration_seconds, insert_count) FROM STDIN"
        ) as copy:
            for update_ts, duration, inserts in rows:
                copy.write_row((datetime.fromtimestamp(update_ts, timezone.utc), duration, inserts))
    pg.commit()
    return len(rows)


def create_indexes(pg) -> None:
    with pg.cursor() as cursor:
        cursor.execute("CREATE INDEX IF NOT EXISTS mrt_file_collector_type_ts_idx ON mrt.file (collector_id, data_type, ts_start)")
        cursor.execute("CREATE INDEX IF NOT EXISTS mrt_file_type_ts_idx ON mrt.file (data_type, ts_start)")
        cursor.execute("CREATE INDEX IF NOT EXISTS mrt_update_meta_ts_idx ON mrt.update_meta (update_ts DESC)")
        cursor.execute("ANALYZE mrt.file; ANALYZE mrt.collector; ANALYZE mrt.latest_file")
    pg.commit()


def verify(sqlite_db: sqlite3.Connection, pg) -> None:
    sqlite_files = sqlite_db.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    sqlite_collectors = sqlite_db.execute("SELECT COUNT(*) FROM collectors WHERE name IS NOT NULL").fetchone()[0]
    with pg.cursor() as cursor:
        cursor.execute("SELECT COUNT(*), min(ts_start), max(ts_start) FROM mrt.file")
        pg_files, minimum, maximum = cursor.fetchone()
        cursor.execute("SELECT COUNT(*) FROM mrt.collector")
        pg_collectors = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM mrt.latest_file")
        latest = cursor.fetchone()[0]
    if sqlite_files != pg_files or sqlite_collectors != pg_collectors:
        raise RuntimeError(f"verification failed: SQLite files/collectors={sqlite_files}/{sqlite_collectors}; PostgreSQL={pg_files}/{pg_collectors}")
    print(f"verified: files={pg_files:,}, collectors={pg_collectors}, latest={latest}, time_range={minimum}..{maximum}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sqlite_path", type=Path)
    parser.add_argument("--database-url", required=True, help="libpq URL, normally postgresql:///bgpkit_platform?host=/var/run/postgresql")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--reset", action="store_true", help="drop only api and mrt schemas before import")
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")

    try:
        import psycopg
        sqlite_db = sqlite_connection(args.sqlite_path)
        ensure_sqlite_shape(sqlite_db)
        with psycopg.connect(args.database_url) as pg:
            execute_schema(pg, args.reset)
            collector_ids, types = import_collectors(sqlite_db, pg)
            seen, inserted = copy_files(sqlite_db, pg, collector_ids, types, args.batch_size)
            latest = refresh_latest(pg)
            imported_meta = import_update_meta(sqlite_db, pg)
            create_indexes(pg)
            verify(sqlite_db, pg)
            print(f"completed: latest={latest}, update_meta={imported_meta}, files_seen={seen}, files_inserted={inserted}")
    except Exception as error:
        print(f"bootstrap failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
