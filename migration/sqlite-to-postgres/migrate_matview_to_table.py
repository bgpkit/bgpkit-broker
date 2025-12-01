#!/usr/bin/env python3
"""
Migrate from materialized view 'latest' to table-based 'latest'.

This script:
1. Drops the old materialized view if it exists
2. Creates the new 'latest' table
3. Creates helper functions (upsert_latest, bootstrap_latest)
4. Bootstraps the latest table from files data

Run this after migrating data if you have the old materialized view schema.
"""

import os
import sys
import time
import psycopg2

from dotenv import load_dotenv
load_dotenv()

def get_pg_connection():
    """Create PostgreSQL connection from environment variables."""
    host = os.environ.get('BROKER_DATABASE_HOST', 'localhost')
    port = os.environ.get('BROKER_DATABASE_PORT', '5432')
    user = os.environ.get('BROKER_DATABASE_USERNAME', 'postgres')
    password = os.environ.get('BROKER_DATABASE_PASSWORD', '')
    database = os.environ.get('BROKER_DATABASE', 'bgpkit_broker')
    
    print(f"Connecting to PostgreSQL at {host}:{port}/{database}...")
    
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=database,
        connect_timeout=30,
        # 30 minutes timeout for long operations
        options='-c statement_timeout=1800000'
    )
    return conn


def check_current_schema(conn) -> dict:
    """Check what currently exists in the database."""
    with conn.cursor() as cur:
        # Check for materialized view
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM pg_matviews 
                WHERE matviewname = 'latest'
            )
        """)
        has_matview = cur.fetchone()[0]
        
        # Check for regular table
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'latest' AND table_type = 'BASE TABLE'
            )
        """)
        has_table = cur.fetchone()[0]
        
        # Check for bootstrap function
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM pg_proc 
                WHERE proname = 'bootstrap_latest'
            )
        """)
        has_bootstrap_func = cur.fetchone()[0]
        
        # Check for upsert function
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM pg_proc 
                WHERE proname = 'upsert_latest'
            )
        """)
        has_upsert_func = cur.fetchone()[0]
        
        return {
            'has_matview': has_matview,
            'has_table': has_table,
            'has_bootstrap_func': has_bootstrap_func,
            'has_upsert_func': has_upsert_func
        }


def drop_materialized_view(conn):
    """Drop the materialized view if it exists."""
    print("Dropping materialized view 'latest'...")
    with conn.cursor() as cur:
        cur.execute("DROP MATERIALIZED VIEW IF EXISTS latest CASCADE")
    conn.commit()
    print("  Done.")


def create_latest_table(conn):
    """Create the latest table."""
    print("Creating 'latest' table...")
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS latest (
                collector_name TEXT NOT NULL,
                type TEXT NOT NULL,
                ts TIMESTAMPTZ NOT NULL,
                rough_size BIGINT,
                exact_size BIGINT,
                PRIMARY KEY (collector_name, type)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_latest_collector ON latest (collector_name)")
    conn.commit()
    print("  Done.")


def create_latest_view(conn):
    """Create the latest_view that matches expected output format."""
    print("Creating 'latest_view'...")
    with conn.cursor() as cur:
        cur.execute("""
            CREATE OR REPLACE VIEW latest_view AS
            SELECT 
                l.ts,
                l.rough_size,
                l.exact_size,
                l.type,
                l.collector_name,
                c.url AS collector_url,
                c.project::text AS project_name,
                c.updates_interval
            FROM latest l
            JOIN collectors c ON c.name = l.collector_name
        """)
    conn.commit()
    print("  Done.")


def create_upsert_function(conn):
    """Create the upsert_latest function."""
    print("Creating 'upsert_latest' function...")
    with conn.cursor() as cur:
        cur.execute("""
            CREATE OR REPLACE FUNCTION upsert_latest(
                p_collector_name TEXT,
                p_type TEXT,
                p_ts TIMESTAMPTZ,
                p_rough_size BIGINT,
                p_exact_size BIGINT
            ) RETURNS VOID AS $$
            BEGIN
                INSERT INTO latest (collector_name, type, ts, rough_size, exact_size)
                VALUES (p_collector_name, p_type, p_ts, p_rough_size, p_exact_size)
                ON CONFLICT (collector_name, type) 
                DO UPDATE SET
                    ts = EXCLUDED.ts,
                    rough_size = EXCLUDED.rough_size,
                    exact_size = EXCLUDED.exact_size
                WHERE EXCLUDED.ts > latest.ts;
            END;
            $$ LANGUAGE plpgsql
        """)
    conn.commit()
    print("  Done.")


def create_bootstrap_function(conn):
    """Create the bootstrap_latest function."""
    print("Creating 'bootstrap_latest' function...")
    with conn.cursor() as cur:
        cur.execute("""
            CREATE OR REPLACE FUNCTION bootstrap_latest()
            RETURNS INTEGER AS $$
            DECLARE
                row_count INTEGER;
            BEGIN
                -- Clear existing data
                DELETE FROM latest;
                
                -- Insert the latest file for each collector/type combination
                INSERT INTO latest (collector_name, type, ts, rough_size, exact_size)
                SELECT DISTINCT ON (c.name, f.data_type)
                    c.name,
                    f.data_type::text,
                    f.ts,
                    f.rough_size,
                    f.exact_size
                FROM files f
                JOIN collectors c ON c.id = f.collector_id
                ORDER BY c.name, f.data_type, f.ts DESC;
                
                GET DIAGNOSTICS row_count = ROW_COUNT;
                RETURN row_count;
            END;
            $$ LANGUAGE plpgsql
        """)
    conn.commit()
    print("  Done.")


def bootstrap_latest(conn) -> int:
    """Run the bootstrap_latest function."""
    print("Bootstrapping 'latest' table from files...")
    print("  This may take a few minutes for large datasets...")
    
    start_time = time.time()
    with conn.cursor() as cur:
        cur.execute("SELECT bootstrap_latest()")
        result = cur.fetchone()
        count = result[0] if result else 0
    conn.commit()
    elapsed = time.time() - start_time
    
    print(f"  Completed in {elapsed:.1f} seconds")
    print(f"  Populated {count} entries")
    return count


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description='Migrate from materialized view to table-based latest tracking'
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='Only check current schema, do not make changes')
    parser.add_argument('--skip-bootstrap', action='store_true',
                        help='Skip the bootstrap step (useful if you want to run it separately)')
    args = parser.parse_args()
    
    print("=" * 60)
    print("BGPKIT Broker: Migrate 'latest' from Materialized View to Table")
    print("=" * 60)
    
    try:
        conn = get_pg_connection()
        print("Connected successfully.\n")
    except Exception as e:
        print(f"Failed to connect: {e}")
        print("\nMake sure these environment variables are set:")
        print("  BROKER_DATABASE_HOST")
        print("  BROKER_DATABASE_PORT")
        print("  BROKER_DATABASE_USERNAME")
        print("  BROKER_DATABASE_PASSWORD")
        print("  BROKER_DATABASE")
        sys.exit(1)
    
    # Check current schema
    print("Checking current schema...")
    status = check_current_schema(conn)
    print(f"  Materialized view 'latest': {'EXISTS' if status['has_matview'] else 'not found'}")
    print(f"  Table 'latest': {'EXISTS' if status['has_table'] else 'not found'}")
    print(f"  Function 'bootstrap_latest': {'EXISTS' if status['has_bootstrap_func'] else 'not found'}")
    print(f"  Function 'upsert_latest': {'EXISTS' if status['has_upsert_func'] else 'not found'}")
    print()
    
    if args.dry_run:
        print("Dry run mode - no changes made.")
        conn.close()
        return
    
    # Determine what needs to be done
    if status['has_table'] and status['has_bootstrap_func'] and status['has_upsert_func']:
        print("Schema already migrated to table-based 'latest'.")
        if not args.skip_bootstrap:
            response = input("Re-bootstrap the latest table? [y/N]: ")
            if response.lower() == 'y':
                bootstrap_latest(conn)
        conn.close()
        return
    
    # Perform migration
    print("Starting migration...\n")
    
    # Step 1: Drop materialized view if exists
    if status['has_matview']:
        drop_materialized_view(conn)
    
    # Step 2: Create latest table
    if not status['has_table']:
        create_latest_table(conn)
    
    # Step 3: Create helper functions
    create_upsert_function(conn)
    create_bootstrap_function(conn)
    
    # Step 4: Create latest_view
    create_latest_view(conn)
    
    # Step 5: Bootstrap the table
    if not args.skip_bootstrap:
        print()
        bootstrap_latest(conn)
    else:
        print("\nSkipping bootstrap (run 'SELECT bootstrap_latest()' manually)")
    
    # Verify
    print("\nVerifying migration...")
    status = check_current_schema(conn)
    if status['has_table'] and status['has_bootstrap_func'] and status['has_upsert_func']:
        print("  Migration completed successfully!")
    else:
        print("  Warning: Some components may be missing")
    
    conn.close()
    print("\nDone!")


if __name__ == '__main__':
    main()
