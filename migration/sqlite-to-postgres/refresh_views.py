#!/usr/bin/env python3
"""
Bootstrap/refresh the latest table in PostgreSQL.

This script populates the latest table from files data.
Used after migration or to recover from data inconsistencies.
"""

import os
import sys
import time
import psycopg2

def get_pg_connection():
    """Create PostgreSQL connection from environment variables."""
    host = os.environ.get('BROKER_DATABASE_HOST', 'localhost')
    port = os.environ.get('BROKER_DATABASE_PORT', '5432')
    user = os.environ.get('BROKER_DATABASE_USERNAME', 'postgres')
    password = os.environ.get('BROKER_DATABASE_PASSWORD', '')
    database = os.environ.get('BROKER_DATABASE', 'bgpkit_broker')
    
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=database,
        connect_timeout=30,
        options='-c statement_timeout=1800000'  # 30 minutes in milliseconds
    )
    return conn

def check_latest_status(conn) -> dict:
    """Check the status of the latest table."""
    with conn.cursor() as cur:
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'latest'
            )
        """)
        exists = cur.fetchone()[0]
        
        if not exists:
            return {'exists': False}
        
        # Get row count and some stats
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT collector_name) as collectors,
                COUNT(DISTINCT type) as types,
                MIN(ts) as oldest,
                MAX(ts) as newest
            FROM latest
        """)
        row = cur.fetchone()
        
        return {
            'exists': True,
            'row_count': row[0],
            'collectors': row[1],
            'types': row[2],
            'oldest_ts': row[3],
            'newest_ts': row[4]
        }

def bootstrap_latest(conn) -> int:
    """Bootstrap the latest table from files data."""
    with conn.cursor() as cur:
        cur.execute("SELECT bootstrap_latest()")
        result = cur.fetchone()
        count = result[0] if result else 0
    conn.commit()
    return count

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Bootstrap/refresh the latest table')
    parser.add_argument('--status-only', action='store_true',
                        help='Only show table status, do not bootstrap')
    args = parser.parse_args()
    
    print("Connecting to PostgreSQL...")
    try:
        conn = get_pg_connection()
        print(f"Connected to {os.environ.get('BROKER_DATABASE_HOST', 'localhost')}")
    except Exception as e:
        print(f"Failed to connect: {e}")
        sys.exit(1)
    
    # Check current status
    print("\nChecking status of 'latest' table...")
    status = check_latest_status(conn)
    
    if not status['exists']:
        print("Table 'latest' does not exist!")
        sys.exit(1)
    
    print(f"  Row count: {status['row_count']:,}")
    print(f"  Collectors: {status['collectors']}")
    print(f"  Types: {status['types']}")
    if status['oldest_ts']:
        print(f"  Oldest entry: {status['oldest_ts']}")
        print(f"  Newest entry: {status['newest_ts']}")
    
    if args.status_only:
        conn.close()
        return
    
    # Bootstrap the table
    print("\nBootstrapping 'latest' table from files...")
    print("  This scans the files table to find latest entry per collector/type")
    
    start_time = time.time()
    try:
        count = bootstrap_latest(conn)
        elapsed = time.time() - start_time
        print(f"  Bootstrap completed in {elapsed:.1f} seconds")
        print(f"  Populated {count} entries")
    except Exception as e:
        print(f"  Bootstrap failed: {e}")
        sys.exit(1)
    
    # Show updated status
    print("\nUpdated status of 'latest' table:")
    status = check_latest_status(conn)
    print(f"  Row count: {status['row_count']:,}")
    if status['newest_ts']:
        print(f"  Newest entry: {status['newest_ts']}")
    
    conn.close()
    print("\nDone!")

if __name__ == '__main__':
    main()
