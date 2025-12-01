#!/usr/bin/env python3
"""
SQLite to PostgreSQL Migration Script for BGPKIT Broker

This script migrates data from an SQLite database to a PostgreSQL database.
It handles:
- Schema creation (via schema.sql)
- Data migration for all tables
- Batch processing for large tables
- Progress reporting with time estimates
- Timestamp conversion (epoch -> timestamptz)

Usage:
    python migrate_to_postgres.py <sqlite_db_path>

Example:
    # Set environment variables
    export BROKER_DATABASE_HOST=localhost
    export BROKER_DATABASE_PORT=5432
    export BROKER_DATABASE_USERNAME=postgres
    export BROKER_DATABASE_PASSWORD=secret
    export BROKER_DATABASE=bgpkit_broker
    
    # Migrate last 30 days (default)
    python migrate_to_postgres.py ./bgpkit_broker.sqlite3
    
    # Migrate all data
    python migrate_to_postgres.py --all ./bgpkit_broker.sqlite3
    
    # Migrate last 7 days with custom schema
    python migrate_to_postgres.py --days 7 --schema bgpkit ./bgpkit_broker.sqlite3

Requirements:
    pip install psycopg2-binary tqdm

Environment Variables:
    BROKER_DATABASE_HOST: PostgreSQL host (default: localhost)
    BROKER_DATABASE_PORT: PostgreSQL port (default: 5432)
    BROKER_DATABASE_USERNAME: PostgreSQL username (required)
    BROKER_DATABASE_PASSWORD: PostgreSQL password (required)
    BROKER_DATABASE: PostgreSQL database name (required)
    SQLITE_PATH: Alternative way to specify SQLite path
"""

import argparse
import os
import sqlite3
import sys
import time
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

load_dotenv()

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("Error: psycopg2 is required. Install with: pip install psycopg2-binary")
    sys.exit(1)

try:
    from tqdm import tqdm

    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("Warning: tqdm not installed. Using basic progress reporting.")
    print("Install with: pip install tqdm")


# Batch size for data migration (adjust based on memory constraints)
BATCH_SIZE = 10000

# Default number of days to migrate
DEFAULT_DAYS = 30


def get_schema_sql() -> str:
    """Read the PostgreSQL schema from schema.sql file."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r") as f:
        return f.read()


def connect_sqlite(db_path: str) -> sqlite3.Connection:
    """Connect to SQLite database."""
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"SQLite database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def connect_postgres(host: str, port: int, database: str, user: str, password: str):
    """Connect to PostgreSQL database using individual parameters."""
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    conn.autocommit = False
    return conn


def epoch_to_timestamp(epoch: int) -> datetime:
    """Convert Unix epoch to datetime with UTC timezone."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def create_schema(pg_conn, schema_name: str = None) -> None:
    """Create PostgreSQL schema from schema.sql.
    
    Args:
        pg_conn: PostgreSQL connection
        schema_name: Optional schema name (e.g., 'bgpkit'). If provided,
                     creates the schema and sets search_path.
    """
    print("Creating PostgreSQL schema...")
    
    with pg_conn.cursor() as cur:
        # Create schema if specified
        if schema_name:
            print(f"  Using schema: {schema_name}")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            cur.execute(f"SET search_path TO {schema_name}")
        
        # Execute schema SQL
        schema_sql = get_schema_sql()
        cur.execute(schema_sql)
    
    pg_conn.commit()
    print("Schema created successfully.")


def set_schema(pg_conn, schema_name: str) -> None:
    """Set the search_path to the specified schema."""
    with pg_conn.cursor() as cur:
        cur.execute(f"SET search_path TO {schema_name}")
    pg_conn.commit()


def migrate_collectors(sqlite_conn: sqlite3.Connection, pg_conn) -> dict:
    """Migrate collectors table and return old_id -> new_id mapping."""
    print("Migrating collectors table...")

    cursor = sqlite_conn.cursor()
    cursor.execute(
        """
        SELECT id, name, url, project, updates_interval 
        FROM collectors 
        WHERE name IS NOT NULL
        ORDER BY id
    """
    )
    rows = cursor.fetchall()

    if not rows:
        print("No collectors to migrate.")
        return {}

    # Map old SQLite IDs to new PostgreSQL IDs
    old_to_new_id = {}

    with pg_conn.cursor() as pg_cur:
        for row in rows:
            old_id = row["id"]
            project = row["project"]
            name = row["name"]
            url = row["url"]
            updates_interval = row["updates_interval"]

            # Insert and get new ID
            pg_cur.execute(
                """
                INSERT INTO collectors (project, name, url, updates_interval)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """,
                (project, name, url, updates_interval),
            )

            new_id = pg_cur.fetchone()[0]
            old_to_new_id[old_id] = new_id

    pg_conn.commit()
    print(f"Migrated {len(rows)} collectors.")
    return old_to_new_id


def get_type_mapping(sqlite_conn: sqlite3.Connection) -> dict:
    """Get type_id -> type_name mapping from SQLite."""
    cursor = sqlite_conn.cursor()
    cursor.execute("SELECT id, name FROM types")
    rows = cursor.fetchall()
    return {row["id"]: row["name"] for row in rows}


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def format_rate(count: int, seconds: float) -> str:
    """Format rate as rows/second."""
    if seconds == 0:
        return "N/A"
    rate = count / seconds
    if rate >= 1000:
        return f"{rate/1000:.1f}K/s"
    return f"{rate:.0f}/s"


class ProgressTracker:
    """Track migration progress with time estimates."""

    def __init__(self, total: int, desc: str = "Processing"):
        self.total = total
        self.desc = desc
        self.processed = 0
        self.start_time = time.time()
        self.last_report_time = self.start_time
        self.last_report_count = 0

        if TQDM_AVAILABLE:
            self.pbar = tqdm(
                total=total,
                desc=desc,
                unit="rows",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
            )
        else:
            self.pbar = None
            print(f"{desc}: 0/{total:,} (0%)")

    def update(self, count: int):
        """Update progress by count."""
        self.processed += count

        if self.pbar:
            self.pbar.update(count)
        else:
            # Report every 5 seconds or every 100K rows
            now = time.time()
            if (
                now - self.last_report_time >= 5
                or self.processed - self.last_report_count >= 100000
            ):
                elapsed = now - self.start_time
                percent = (self.processed / self.total * 100) if self.total > 0 else 0
                rate = format_rate(self.processed, elapsed)

                # Estimate remaining time
                if self.processed > 0:
                    remaining = (self.total - self.processed) * elapsed / self.processed
                    eta = format_duration(remaining)
                else:
                    eta = "?"

                print(
                    f"\r{self.desc}: {self.processed:,}/{self.total:,} ({percent:.1f}%) "
                    f"[{format_duration(elapsed)}<{eta}, {rate}]",
                    end="",
                    flush=True,
                )

                self.last_report_time = now
                self.last_report_count = self.processed

    def close(self):
        """Close progress tracker and print final stats."""
        elapsed = time.time() - self.start_time

        if self.pbar:
            self.pbar.close()
        else:
            print()  # New line after progress

        rate = format_rate(self.processed, elapsed)
        print(
            f"  Completed: {self.processed:,} rows in {format_duration(elapsed)} ({rate})"
        )


def migrate_files(
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    collector_id_map: dict,
    type_map: dict,
    batch_size: int = BATCH_SIZE,
    days: int = None,
    migrate_all: bool = False,
) -> None:
    """Migrate files table with batching for large datasets.

    Args:
        sqlite_conn: SQLite connection
        pg_conn: PostgreSQL connection
        collector_id_map: Mapping from old to new collector IDs
        type_map: Mapping from type_id to type name
        batch_size: Number of rows per batch
        days: Number of days to migrate (None = use default)
        migrate_all: If True, migrate all data regardless of days

    Converts:
    - timestamp (epoch) -> ts (timestamptz)
    - collector_id (old) -> collector_id (new)
    - type_id -> data_type (enum)
    - 0 sizes -> NULL
    """
    print("Migrating files table...")

    cursor = sqlite_conn.cursor()

    # Determine time filter
    if migrate_all:
        time_filter = ""
        filter_desc = "all data"
    else:
        days_to_migrate = days if days is not None else DEFAULT_DAYS
        cutoff_ts = int(
            (datetime.now(timezone.utc) - timedelta(days=days_to_migrate)).timestamp()
        )
        time_filter = f"WHERE timestamp >= {cutoff_ts}"
        cutoff_date = datetime.fromtimestamp(cutoff_ts, tz=timezone.utc).strftime(
            "%Y-%m-%d"
        )
        filter_desc = f"last {days_to_migrate} days (since {cutoff_date})"

    print(f"  Filter: {filter_desc}")

    # Get total count for progress bar
    cursor.execute(f"SELECT COUNT(*) FROM files {time_filter}")
    total_count = cursor.fetchone()[0]
    print(f"  Files to migrate: {total_count:,}")

    if total_count == 0:
        print("  No files to migrate.")
        return

    # Get time range
    cursor.execute(f"SELECT MIN(timestamp), MAX(timestamp) FROM files {time_filter}")
    min_ts, max_ts = cursor.fetchone()
    if min_ts and max_ts:
        min_date = datetime.fromtimestamp(min_ts, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M"
        )
        max_date = datetime.fromtimestamp(max_ts, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M"
        )
        print(f"  Time range: {min_date} to {max_date}")

    # Use a separate cursor for iteration
    cursor.execute(
        f"""
        SELECT timestamp, collector_id, type_id, rough_size, exact_size 
        FROM files 
        {time_filter}
        ORDER BY timestamp
    """
    )

    progress = ProgressTracker(total_count, "Migrating files")

    with pg_conn.cursor() as pg_cur:
        batch = []
        migrated = 0
        skipped = 0

        for row in cursor:
            old_collector_id = row["collector_id"]

            # Skip if collector not found (invalid data)
            if old_collector_id not in collector_id_map:
                skipped += 1
                progress.update(1)
                continue

            # Convert epoch to timestamptz
            ts = epoch_to_timestamp(row["timestamp"])

            # Map collector ID
            new_collector_id = collector_id_map[old_collector_id]

            # Map type_id to enum value
            type_id = row["type_id"]
            data_type = type_map.get(type_id, "updates")

            # Convert 0 to NULL for sizes
            rough_size = row["rough_size"] if row["rough_size"] != 0 else None
            exact_size = row["exact_size"] if row["exact_size"] != 0 else None

            batch.append((ts, new_collector_id, data_type, rough_size, exact_size))

            if len(batch) >= batch_size:
                execute_values(
                    pg_cur,
                    """INSERT INTO files (ts, collector_id, data_type, rough_size, exact_size) 
                       VALUES %s ON CONFLICT DO NOTHING""",
                    batch,
                    template="(%s, %s, %s, %s, %s)",
                )
                pg_conn.commit()
                migrated += len(batch)
                progress.update(len(batch))
                batch = []

        # Insert remaining records
        if batch:
            execute_values(
                pg_cur,
                """INSERT INTO files (ts, collector_id, data_type, rough_size, exact_size) 
                   VALUES %s ON CONFLICT DO NOTHING""",
                batch,
                template="(%s, %s, %s, %s, %s)",
            )
            pg_conn.commit()
            migrated += len(batch)
            progress.update(len(batch))

    progress.close()
    print(f"  Migrated: {migrated:,} files")
    if skipped > 0:
        print(f"  Skipped: {skipped:,} files with invalid collector_id")


def bootstrap_latest(pg_conn) -> None:
    """Bootstrap the latest table from files data."""
    print("Bootstrapping latest table...")

    with pg_conn.cursor() as pg_cur:
        # Set a longer statement timeout for this operation
        pg_cur.execute("SET statement_timeout = '30min'")
        pg_cur.execute("SELECT bootstrap_latest()")
        result = pg_cur.fetchone()
        count = result[0] if result else 0

    pg_conn.commit()
    print(f"Latest table bootstrapped with {count} entries.")


def migrate_meta(sqlite_conn: sqlite3.Connection, pg_conn) -> None:
    """Migrate meta table with timestamp conversion."""
    print("Migrating meta table...")

    cursor = sqlite_conn.cursor()
    cursor.execute("SELECT update_ts, update_duration, insert_count FROM meta")
    rows = cursor.fetchall()

    if not rows:
        print("No meta entries to migrate.")
        return

    with pg_conn.cursor() as pg_cur:
        values = []
        for row in rows:
            update_ts = epoch_to_timestamp(row["update_ts"])
            values.append((update_ts, row["update_duration"], row["insert_count"]))

        execute_values(
            pg_cur,
            "INSERT INTO meta (update_ts, update_duration, insert_count) VALUES %s",
            values,
            template="(%s, %s, %s)",
        )

    pg_conn.commit()
    print(f"Migrated {len(rows)} meta entries.")


def run_analyze(pg_conn) -> None:
    """Run ANALYZE on all tables to update statistics."""
    print("Running ANALYZE on all tables...")

    # ANALYZE requires autocommit
    old_autocommit = pg_conn.autocommit
    pg_conn.autocommit = True

    with pg_conn.cursor() as pg_cur:
        for table in ["collectors", "files", "meta"]:
            print(f"  Analyzing {table}...")
            pg_cur.execute(f"ANALYZE {table}")

    pg_conn.autocommit = old_autocommit
    print("ANALYZE completed.")


def verify_migration(
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    days: int = None,
    migrate_all: bool = False,
) -> bool:
    """Verify that migration completed successfully by comparing row counts."""
    print("\nVerifying migration...")

    all_match = True
    sqlite_cursor = sqlite_conn.cursor()

    # Determine time filter for files comparison
    if migrate_all:
        time_filter = ""
    else:
        days_to_migrate = days if days is not None else DEFAULT_DAYS
        cutoff_ts = int(
            (datetime.now(timezone.utc) - timedelta(days=days_to_migrate)).timestamp()
        )
        time_filter = f"WHERE timestamp >= {cutoff_ts}"

    with pg_conn.cursor() as pg_cur:
        # Check collectors
        sqlite_cursor.execute("SELECT COUNT(*) FROM collectors WHERE name IS NOT NULL")
        sqlite_count = sqlite_cursor.fetchone()[0]
        pg_cur.execute("SELECT COUNT(*) FROM collectors")
        pg_count = pg_cur.fetchone()[0]
        status = "✓" if sqlite_count == pg_count else "✗"
        print(
            f"  collectors: SQLite={sqlite_count:,}, PostgreSQL={pg_count:,} {status}"
        )
        if sqlite_count != pg_count:
            all_match = False

        # Check files (with time filter if applicable)
        sqlite_cursor.execute(f"SELECT COUNT(*) FROM files {time_filter}")
        sqlite_count = sqlite_cursor.fetchone()[0]
        pg_cur.execute("SELECT COUNT(*) FROM files")
        pg_count = pg_cur.fetchone()[0]
        status = "✓" if sqlite_count == pg_count else "~"
        filter_note = "" if migrate_all else f" (filtered)"
        print(
            f"  files: SQLite={sqlite_count:,}{filter_note}, PostgreSQL={pg_count:,} {status}"
        )

        # Check latest (materialized view)
        pg_cur.execute("SELECT COUNT(*) FROM latest")
        pg_count = pg_cur.fetchone()[0]
        print(f"  latest: PostgreSQL={pg_count:,} (materialized view)")

        # Check meta
        sqlite_cursor.execute("SELECT COUNT(*) FROM meta")
        sqlite_count = sqlite_cursor.fetchone()[0]
        pg_cur.execute("SELECT COUNT(*) FROM meta")
        pg_count = pg_cur.fetchone()[0]
        status = "✓" if sqlite_count == pg_count else "✗"
        print(f"  meta: SQLite={sqlite_count:,}, PostgreSQL={pg_count:,} {status}")
        if sqlite_count != pg_count:
            all_match = False

        # Verify timestamp conversion
        pg_cur.execute("SELECT MIN(ts), MAX(ts) FROM files")
        min_ts, max_ts = pg_cur.fetchone()
        if min_ts and max_ts:
            print(f"\n  PostgreSQL time range: {min_ts} to {max_ts}")

    return all_match


def main():
    parser = argparse.ArgumentParser(
        description="Migrate BGPKIT Broker data from SQLite to PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Set environment variables first
  export BROKER_DATABASE_HOST=localhost
  export BROKER_DATABASE_PORT=5432
  export BROKER_DATABASE_USERNAME=postgres
  export BROKER_DATABASE_PASSWORD=secret
  export BROKER_DATABASE=bgpkit_broker
  
  # Migrate last 30 days (default)
  python migrate_to_postgres.py ./db.sqlite3
  
  # Migrate all data
  python migrate_to_postgres.py --all ./db.sqlite3
  
  # Migrate last 7 days
  python migrate_to_postgres.py --days 7 ./db.sqlite3
  
  # Use a custom schema (for managed PostgreSQL like Neon, Supabase)
  python migrate_to_postgres.py --schema bgpkit ./db.sqlite3
""",
    )
    parser.add_argument("sqlite_path", nargs="?", help="Path to SQLite database file")
    parser.add_argument(
        "--all",
        action="store_true",
        dest="migrate_all",
        help="Migrate all data (default: last 30 days only)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help=f"Number of days of data to migrate (default: {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--schema",
        type=str,
        default=None,
        help="PostgreSQL schema name (e.g., 'bgpkit'). Creates schema if needed.",
    )
    parser.add_argument(
        "--skip-schema",
        action="store_true",
        help="Skip schema creation (use if schema already exists)",
    )
    parser.add_argument(
        "--skip-files",
        action="store_true",
        help="Skip files table migration (useful for testing)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"Batch size for file migration (default: {BATCH_SIZE})",
    )
    parser.add_argument(
        "--skip-refresh",
        action="store_true",
        help="Skip materialized view refresh (can be done later with refresh_views.py)",
    )

    args = parser.parse_args()

    # Get SQLite path from args or environment
    sqlite_path = args.sqlite_path or os.environ.get("SQLITE_PATH")
    
    if not sqlite_path:
        print("Error: SQLite path is required.")
        print("Provide as argument or set SQLITE_PATH environment variable.")
        sys.exit(1)

    # Get PostgreSQL connection parameters from environment
    db_host = os.environ.get("BROKER_DATABASE_HOST", "localhost")
    db_port = int(os.environ.get("BROKER_DATABASE_PORT", "5432"))
    db_user = os.environ.get("BROKER_DATABASE_USERNAME")
    db_password = os.environ.get("BROKER_DATABASE_PASSWORD")
    db_name = os.environ.get("BROKER_DATABASE")

    # Validate required PostgreSQL parameters
    missing_vars = []
    if not db_user:
        missing_vars.append("BROKER_DATABASE_USERNAME")
    if not db_password:
        missing_vars.append("BROKER_DATABASE_PASSWORD")
    if not db_name:
        missing_vars.append("BROKER_DATABASE")
    
    if missing_vars:
        print("Error: Missing required environment variables:")
        for var in missing_vars:
            print(f"  - {var}")
        print("\nRequired environment variables:")
        print("  BROKER_DATABASE_HOST     - PostgreSQL host (default: localhost)")
        print("  BROKER_DATABASE_PORT     - PostgreSQL port (default: 5432)")
        print("  BROKER_DATABASE_USERNAME - PostgreSQL username (required)")
        print("  BROKER_DATABASE_PASSWORD - PostgreSQL password (required)")
        print("  BROKER_DATABASE          - PostgreSQL database name (required)")
        sys.exit(1)

    batch_size = args.batch_size
    days = args.days
    migrate_all = args.migrate_all
    schema_name = args.schema

    # Determine what we're migrating
    if migrate_all:
        data_range = "all data"
    elif days is not None:
        data_range = f"last {days} days"
    else:
        data_range = f"last {DEFAULT_DAYS} days (default)"

    print("=" * 60)
    print("BGPKIT Broker: SQLite to PostgreSQL Migration")
    print("=" * 60)
    print(f"SQLite source: {sqlite_path}")
    print(f"PostgreSQL target: {db_host}:{db_port}/{db_name}")
    if schema_name:
        print(f"Schema: {schema_name}")
    print(f"Data range: {data_range}")
    print(f"Batch size: {batch_size:,}")
    print()

    migration_start = time.time()

    try:
        # Connect to databases
        print("Connecting to databases...")
        sqlite_conn = connect_sqlite(sqlite_path)
        pg_conn = connect_postgres(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        print("Connected successfully.")
        print()

        # Create schema or set search_path
        if not args.skip_schema:
            create_schema(pg_conn, schema_name=schema_name)
            print()
        elif schema_name:
            # If skipping schema creation but schema is specified, just set search_path
            set_schema(pg_conn, schema_name)
            print(f"Using existing schema: {schema_name}")
            print()

        # Get type mapping from SQLite
        type_map = get_type_mapping(sqlite_conn)
        print(f"Type mapping: {type_map}")
        print()

        # Migrate collectors (returns ID mapping)
        collector_id_map = migrate_collectors(sqlite_conn, pg_conn)
        print()

        if not args.skip_files:
            migrate_files(
                sqlite_conn,
                pg_conn,
                collector_id_map,
                type_map,
                batch_size=batch_size,
                days=days,
                migrate_all=migrate_all,
            )
            print()

        # Bootstrap latest table
        if not args.skip_refresh:
            bootstrap_latest(pg_conn)
            print()
        else:
            print("Skipping latest table bootstrap (run SELECT bootstrap_latest() manually)")
            print()

        # Migrate meta
        migrate_meta(sqlite_conn, pg_conn)
        print()

        # Run ANALYZE
        run_analyze(pg_conn)
        print()

        # Verify migration
        verify_migration(sqlite_conn, pg_conn, days=days, migrate_all=migrate_all)

        # Print total time
        total_time = time.time() - migration_start
        print(f"\n{'=' * 60}")
        print(f"✓ Migration completed in {format_duration(total_time)}")
        print(f"{'=' * 60}")

    except Exception as e:
        print(f"\nError during migration: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

    finally:
        if "sqlite_conn" in locals():
            sqlite_conn.close()
        if "pg_conn" in locals():
            pg_conn.close()


if __name__ == "__main__":
    main()
