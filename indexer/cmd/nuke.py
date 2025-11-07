#!/usr/bin/env python3
"""
Nuke all schema objects created by the indexer (tables, views, functions, etc.)
"""

import psycopg2

from indexer.db import get_db_cursor

def nuke_schema():
    """Drop all schema objects: materialized views, tables, and functions."""
    with get_db_cursor() as cur:
        # Drop view/materialized view first (depends on tables)
        # Handle both regular VIEW and MATERIALIZED VIEW cases
        print("Dropping view/materialized view: address_status")
        try:
            cur.execute("DROP MATERIALIZED VIEW IF EXISTS address_status CASCADE;")
        except psycopg2.errors.WrongObjectType:
            # If it's a regular view (not a materialized view), drop it as a view
            cur.execute("DROP VIEW IF EXISTS address_status CASCADE;")
        # If it doesn't exist, IF EXISTS handles it gracefully (no exception)
        
        print("Dropping materialized view: utxo_stats")
        cur.execute("DROP MATERIALIZED VIEW IF EXISTS utxo_stats CASCADE;")
        
        print("Dropping materialized view: address_stats")
        cur.execute("DROP MATERIALIZED VIEW IF EXISTS address_stats CASCADE;")
        
        print("Dropping materialized view: block_stats")
        cur.execute("DROP MATERIALIZED VIEW IF EXISTS block_stats CASCADE;")
        
        # Drop all tables
        tables = [
            "addresses",
            "block_log",
            "utxos",
            "schema_initialized"
        ]
        for table in tables:
            print(f"Dropping table: {table}")
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        
        # Drop custom functions and procedures
        print("Dropping function: refresh_all_materialized_views()")
        cur.execute("DROP FUNCTION IF EXISTS refresh_all_materialized_views() CASCADE;")
        
        print("Dropping function: refresh_block_stats()")
        cur.execute("DROP FUNCTION IF EXISTS refresh_block_stats() CASCADE;")
        
        print("Dropping function: refresh_address_stats()")
        cur.execute("DROP FUNCTION IF EXISTS refresh_address_stats() CASCADE;")
        
        print("Dropping function: refresh_utxo_stats()")
        cur.execute("DROP FUNCTION IF EXISTS refresh_utxo_stats() CASCADE;")
        
        print("Dropping function: refresh_address_status()")
        cur.execute("DROP FUNCTION IF EXISTS refresh_address_status() CASCADE;")
        
        print("Dropping function: current_nano()")
        cur.execute("DROP FUNCTION IF EXISTS current_nano() CASCADE;")
        
        print("Dropping procedure: setup_hypertable(regclass)")
        cur.execute("DROP PROCEDURE IF EXISTS setup_hypertable(regclass) CASCADE;")
        
        # Note: timescaledb extension is NOT dropped as it may be used by other schemas
    
    print("All schema objects dropped.")

if __name__ == "__main__":
    nuke_schema()