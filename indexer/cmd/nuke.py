#!/usr/bin/env python3
"""
Nuke all quantum-at-risk tables in the PostgreSQL database.
"""

from indexer.db import get_db_cursor

TABLES = [
    "block_log",
    "utxos"
]

def nuke_tables():
    with get_db_cursor() as cur:
        for table in TABLES:
            print(f"Dropping table: {table}")
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
    print("All tables dropped.")

if __name__ == "__main__":
    nuke_tables()