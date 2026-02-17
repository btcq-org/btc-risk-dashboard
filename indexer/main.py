#!/usr/bin/env python3
"""
Bitcoin UTXO indexer - extracts VOUTs (UTXOs) from Bitcoin blocks
and stores them in PostgreSQL for efficient querying.
"""
from bitcoin import SelectParams

import json
import os
import signal
import sys
import time
import csv

import requests

from collections import defaultdict
from typing import Dict, List
from psycopg2.extras import execute_values

from . import config
from . import db
from .utils import detect_script_type, address_from_vout, get_script_type

# ========================
# CONFIGURATION (from indexer.config)
# ========================
NETWORK = config.NETWORK
RPC_USER = config.RPC_USER
RPC_PASSWORD = config.RPC_PASSWORD
RPC_HOST = config.RPC_HOST
RPC_PORT = config.RPC_PORT
RPC_URL = config.RPC_URL

SelectParams(NETWORK)

POLL_INTERVAL = config.POLL_INTERVAL
RECONNECT_DELAY = config.RECONNECT_DELAY
CHUNK_SIZE = config.CHUNK_SIZE
MAX_RETRIES = config.MAX_RETRIES
DB_PAGE_ROWS = config.DB_PAGE_ROWS
IS_UTXO = config.IS_UTXO
IS_RECALC = config.IS_RECALC

# ========================
# RPC UTILS
# ========================
def test_rpc_connection():
    """Test RPC connection and provide helpful error messages."""
    try:
        result = rpc_call("getblockcount")
        print(f"✓ RPC connection successful. Current block height: {result}")
        return True
    except requests.exceptions.ConnectionError as e:
        print(f"✗ Failed to connect to Bitcoin RPC at {RPC_URL}")
        print(f"  Error: {e}")
        print(f"\n  Troubleshooting steps:")
        print(f"  1. Ensure Bitcoin Core is running")
        print(f"  2. Check that RPC is enabled in bitcoin.conf:")
        print(f"     - rpcuser={RPC_USER}")
        print(f"     - rpcpassword={RPC_PASSWORD}")
        print(f"     - rpcport={RPC_PORT}")
        print(f"     - rpcallowip=127.0.0.1 (or 0.0.0.0/0 for all)")
        print(f"     - rpcbind=127.0.0.1 (or 0.0.0.0 for all)")
        print(f"  3. If using Docker, ensure the container is running:")
        print(f"     docker ps | grep bitcoind")
        print(f"  4. Verify the port is correct (18332 for testnet, 8332 for mainnet)")
        print(f"  5. Check firewall settings")
        return False
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print(f"✗ Authentication failed. Check RPC_USER and RPC_PASSWORD.")
            print(f"  Current settings: user={RPC_USER}, password={'*' * len(RPC_PASSWORD)}")
        else:
            print(f"✗ HTTP error: {e}")
        return False
    except Exception as e:
        print(f"✗ RPC connection test failed: {e}")
        return False

def rpc_call(method, params=None):
    payload = json.dumps({
        "jsonrpc": "1.0",
        "id": "quantum-check",
        "method": method,
        "params": params or []
    })
    try:
        resp = requests.post(RPC_URL, auth=(RPC_USER, RPC_PASSWORD),
                             headers={"content-type": "text/plain"},
                             data=payload,
                             timeout=120)
        resp.raise_for_status()
        r = resp.json()
        if r.get("error"):
            raise Exception(r["error"])
        return r["result"]
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Cannot connect to Bitcoin RPC at {RPC_URL}. "
                            f"Is Bitcoin Core running? Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"RPC request to {RPC_URL} timed out. Error: {e}")

class RPCBatchError(RuntimeError):
    def __init__(self, message, pending=None, last_error=None):
        super().__init__(message)
        self.pending = pending or []
        self.last_error = last_error


def rpc_batch_call(rpc_requests, description="RPC batch request"):
    """
    Send multiple RPC requests with internal retry handling.
    rpc_requests: list of tuples (method, params, identifier)
    Returns: dict mapping identifier to result.
    Raises RPCBatchError if requests cannot be fulfilled within MAX_RETRIES.
    """
    if not rpc_requests:
        return {}

    pending = list(rpc_requests)
    results = {}
    attempts = 0
    last_error = None

    while pending and attempts < MAX_RETRIES and not shutdown_requested:
        payload = []
        id_to_index = {}

        for idx, (method, params, identifier) in enumerate(pending):
            request_id = f"batch-{method}-{identifier}-{attempts}-{idx}"
            id_to_index[request_id] = idx
            payload.append({
                "jsonrpc": "1.0",
                "id": request_id,
                "method": method,
                "params": params or []
            })

        try:
            resp = requests.post(
                RPC_URL,
                auth=(RPC_USER, RPC_PASSWORD),
                headers={"content-type": "text/plain"},
                data=json.dumps(payload),
                timeout=120
            )
            resp.raise_for_status()
            response_payload = resp.json()
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.HTTPError) as e:
            last_error = e
        except Exception as e:
            last_error = e
        else:
            responses = response_payload if isinstance(response_payload, list) else [response_payload]
            for entry in responses:
                request_id = entry.get("id")
                if request_id not in id_to_index:
                    continue
                original_idx = id_to_index[request_id]
                _, _, identifier = pending[original_idx]
                if entry.get("error"):
                    last_error = entry["error"]
                    continue
                results[identifier] = entry.get("result")

        pending = [req for req in pending if req[2] not in results]

        if pending and not shutdown_requested:
            attempts += 1
            if attempts < MAX_RETRIES:
                pending_identifiers = [req[2] for req in pending]
                print(f"Retrying {len(pending)} requests for {description} "
                      f"(attempt {attempts + 1}/{MAX_RETRIES}); pending identifiers: {pending_identifiers}")
                time.sleep(RECONNECT_DELAY)

    if shutdown_requested:
        raise RPCBatchError(f"{description} interrupted by shutdown request.", last_error="shutdown")

    if pending:
        pending_identifiers = [req[2] for req in pending]
        raise RPCBatchError(
            f"{description} failed after {MAX_RETRIES} retries; pending identifiers: {pending_identifiers}",
            pending=pending_identifiers,
            last_error=last_error
        )

    return results

# ========================
# DB
# ========================
def update_address_status_with_vout_balance(cur, vout_rows):
    """
    Updates the address_status table to increment balances for the vout addresses.
    For new addresses, inserts all required fields: script_pub_type, reused, created_block, created_block_timestamp.
    For existing addresses, only updates balance_sat.
    vout_rows: list of tuples (txid, vout_idx, address, value_sat, script_pub_key_hex, script_type, height, block_ts)
    """
    # Collect address data with all required fields for new addresses
    # For addresses that appear multiple times, use the first occurrence's metadata
    address_data = {}  # address -> (value_sat_sum, script_type, height, block_ts)
    for row in vout_rows:
        address = row[2]  # address is the 3rd field in vout_rows tuple
        value_sat = row[3]
        script_type = row[5]  # script_pub_type
        height = row[6]  # created_block
        block_ts = row[7]  # created_block_timestamp
        if address:
            if address not in address_data:
                address_data[address] = (value_sat, script_type, height, block_ts)
            else:
                # Sum the balance, keep first occurrence's metadata
                old_sum, script_type, height, block_ts = address_data[address]
                address_data[address] = (old_sum + value_sat, script_type, height, block_ts)

    # Upsert addresses with all required fields for new addresses
    if address_data:
        rows_to_upsert = [
            (address, script_type, False, height, block_ts, value_sat)
            for address, (value_sat, script_type, height, block_ts) in address_data.items()
        ]
        sql = """
            INSERT INTO address_status (address, script_pub_type, reused, created_block, created_block_timestamp, balance_sat)
            VALUES %s
            ON CONFLICT (address) DO UPDATE
                SET balance_sat = address_status.balance_sat + EXCLUDED.balance_sat
        """
        execute_values(cur, sql, rows_to_upsert)

def update_address_status_with_vin_balance(cur, deleted_utxos, height=None, block_ts=None):
    """
    Updates the address_status table to decrement balances and mark as reused for addresses whose UTXOs were spent.
    Seeing an address in a VIN means it's reused, so set reused = TRUE.
    If an address doesn't exist, it will be created with reused=TRUE and the appropriate balance.
    deleted_utxos: list of tuples (address, value_sat, script_pub_type) from bulk_delete_utxos
    height: optional block height for new addresses (used as created_block)
    block_ts: optional block timestamp for new addresses (used as created_block_timestamp)
    """
    if not deleted_utxos:
        return

    # Collect address data: balance delta and script_type (use first occurrence)
    address_data = {}  # address -> (balance_delta, script_type)
    for address, value_sat, script_type in deleted_utxos:
        if address:
            if address not in address_data:
                address_data[address] = (-value_sat, script_type)
            else:
                # Sum the balance delta, keep first occurrence's script_type
                old_delta, script_type = address_data[address]
                address_data[address] = (old_delta - value_sat, script_type)

    # Upsert addresses: create new ones if they don't exist, update existing ones
    if address_data:
        # Prepare rows for upsert
        rows_to_upsert = []
        for address, (balance_delta, script_type) in address_data.items():
            # For new addresses, use provided height/block_ts or NULL
            rows_to_upsert.append((
                address,
                script_type,
                True,  # reused = TRUE (address is being spent)
                height if height is not None else None,  # created_block
                block_ts if block_ts is not None else None,  # created_block_timestamp
                balance_delta  # balance_sat (negative value for decrement)
            ))

        sql = """
            INSERT INTO address_status (address, script_pub_type, reused, created_block, created_block_timestamp, balance_sat)
            VALUES %s
            ON CONFLICT (address) DO UPDATE
                SET balance_sat = address_status.balance_sat + EXCLUDED.balance_sat,
                    reused = TRUE
        """
        execute_values(cur, sql, rows_to_upsert)

def update_address_stats_incremental(cur, vout_rows, deleted_utxos):
    """
    Update address_stats table based on balance changes from vouts and vins.
    Recalculates stats for affected script types from address_status table.
    vout_rows: list of tuples with new UTXOs (includes address, value_sat, script_type)
    deleted_utxos: list of tuples (address, value_sat, script_pub_type) for deleted UTXOs
    """
    # Collect all script types that were affected
    affected_script_types = set()

    # Process vouts (additions)
    for row in vout_rows:
        script_type = row[5]  # script_pub_type is the 6th field
        if script_type:
            affected_script_types.add(script_type)

    # Process deleted UTXOs (subtractions)
    for address, value_sat, script_type in deleted_utxos:
        if script_type:
            affected_script_types.add(script_type)

    if not affected_script_types:
        return

    # Recalculate address_stats for affected script types from address_status
    # This ensures accuracy since address_status has been updated with balance changes
    cur.execute("""
        INSERT INTO address_stats (
            script_pub_type,
            reused_sat,
            total_sat,
            reused_count,
            count
        )
        SELECT
            script_pub_type,
            SUM(CASE WHEN reused THEN balance_sat ELSE 0 END) AS reused_sat,
            SUM(balance_sat) AS total_sat,
            COUNT(*) FILTER (WHERE reused) AS reused_count,
            COUNT(*) AS count
        FROM address_status
        WHERE script_pub_type IN %s
        GROUP BY script_pub_type
        ON CONFLICT (script_pub_type) DO UPDATE SET
            reused_sat = EXCLUDED.reused_sat,
            total_sat = EXCLUDED.total_sat,
            reused_count = EXCLUDED.reused_count,
            count = EXCLUDED.count
    """, (tuple(affected_script_types),))

def bulk_insert_utxos_copy(cur, utxo_rows):
    """
    Insert UTXOs into the main table using execute_values.
    """
    if not utxo_rows:
        return

    insert_sql = """
        INSERT INTO utxos (
            txid,
            vout,
            address,
            value_sat,
            script_pub_key_hex,
            script_pub_type,
            created_block,
            created_block_timestamp
        )
        VALUES %s
    """

    execute_values(cur, insert_sql, utxo_rows, page_size=DB_PAGE_ROWS)

def bulk_insert_block_log_copy(cur, block_rows):
    """Insert block_log entries using execute_values."""
    if not block_rows:
        return

    insert_sql = """
        INSERT INTO block_log (
            block_height,
            block_hash,
            block_timestamp
        )
        VALUES %s
    """

    execute_values(cur, insert_sql, block_rows, page_size=DB_PAGE_ROWS)

def bulk_delete_utxos(cur, spend_rows):
    """
    Delete UTXOs that have been spent and return their addresses, values and script types.
    spend_rows: list of tuples (spent_by_txid, spent_vin, spent_block, spent_block_timestamp, txid, vout)
    Only txid and vout are used for deletion.
    Returns: list of tuples (address, value_sat, script_pub_type) for deleted UTXOs
    """
    if not spend_rows:
        return []

    # Extract only txid and vout for deletion
    delete_rows = [(txid, vout) for _, _, _, _, txid, vout in spend_rows]

    # Use DELETE ... RETURNING to get address, values and script types before deletion
    # We need to use execute_values pattern but with RETURNING, so we'll do it in batches
    deleted_utxos = []

    for page in [delete_rows[i:i+DB_PAGE_ROWS] for i in range(0, len(delete_rows), DB_PAGE_ROWS)]:
        # Build the VALUES clause manually for this page
        placeholders = ','.join(['(%s, %s)'] * len(page))
        delete_sql = f"""
            DELETE FROM utxos
            WHERE (txid, vout) IN (VALUES {placeholders})
            RETURNING address, value_sat, script_pub_type
        """
        # Flatten the page for parameter binding
        params = [item for pair in page for item in pair]
        cur.execute(delete_sql, params)
        deleted_utxos.extend(cur.fetchall())

    return deleted_utxos

def prepare_stats_batch(total_utxo: int, type_counts: Dict[str, int], total_balance_sat: int = 0, type_balances: Dict[str, int] = None) -> List[tuple]:
    """
    Prepare stats records for insertion/upsert.
    total_utxo: change in UTXO count (positive for additions, negative for deletions)
    type_counts: dict of script_type -> count change
    total_balance_sat: change in total balance in satoshis (positive for additions, negative for deletions)
    type_balances: dict of script_type -> balance change in satoshis
    """
    stats_batch = []
    if total_utxo:
        stats_batch.append(('total_utxo', total_utxo))
    if total_balance_sat:
        stats_batch.append(('total_balance_sat', total_balance_sat))
    for script_type, count in type_counts.items():
        stats_batch.append((f"{script_type}_count", count))
    if type_balances:
        for script_type, balance in type_balances.items():
            stats_batch.append((f"{script_type}_balance_sat", balance))
    return stats_batch

def upsert_stats(cur, stats_batch: List[tuple], *, absolute: bool = False):
    """
    Upsert stats into the stats table.
    When absolute=True, values overwrite existing entries.
    Otherwise, values are incremented.
    """
    if not stats_batch:
        return

    if absolute:
        stats_upsert_sql = """
            INSERT INTO stats (key, value) VALUES %s
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """
    else:
        stats_upsert_sql = """
            INSERT INTO stats (key, value) VALUES %s
            ON CONFLICT (key) DO UPDATE SET value = stats.value + EXCLUDED.value
        """

    execute_values(cur, stats_upsert_sql, stats_batch)

def recalculate_stats_from_db(cur):
    """
    Recalculate all stats from the current state of the utxos table using SQL.
    This function directly queries the database and updates the stats table with
    the current counts and balances.
    """
    # Recalculate total UTXO count and total balance
    cur.execute("""
        INSERT INTO stats (key, value)
        SELECT 'total_utxo', COUNT(*)::bigint
        FROM utxos
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """)

    cur.execute("""
        INSERT INTO stats (key, value)
        SELECT 'total_balance_sat', COALESCE(SUM(value_sat), 0)::bigint
        FROM utxos
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """)

    # Recalculate counts and balances per script type
    cur.execute("""
        INSERT INTO stats (key, value)
        SELECT
            script_pub_type || '_count' AS key,
            COUNT(*)::bigint AS value
        FROM utxos
        WHERE script_pub_type IS NOT NULL
        GROUP BY script_pub_type
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """)

    cur.execute("""
        INSERT INTO stats (key, value)
        SELECT
            script_pub_type || '_balance_sat' AS key,
            COALESCE(SUM(value_sat), 0)::bigint AS value
        FROM utxos
        WHERE script_pub_type IS NOT NULL
        GROUP BY script_pub_type
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """)

    # Remove stats for script types that no longer exist in the database
    cur.execute("""
        DELETE FROM stats
        WHERE (key LIKE '%_count' OR key LIKE '%_balance_sat')
        AND key NOT IN (
            SELECT script_pub_type || '_count' FROM utxos WHERE script_pub_type IS NOT NULL
            UNION
            SELECT script_pub_type || '_balance_sat' FROM utxos WHERE script_pub_type IS NOT NULL
        )
        AND key NOT IN ('total_utxo', 'total_balance_sat')
    """)

def schema_init():
    schema_path = os.path.join(os.path.dirname(__file__), "db", "schema.sql")
    db.init_pool()
    with db.get_db_cursor() as cur:
        # Check if block_log table exists (indicates schema is already initialized)
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'block_log'
            )
        """)
        table_exists = cur.fetchone()[0]

        if table_exists:
            print("Schema already initialized (block_log table exists), skipping initialization.")
            return

        # Schema not initialized, run the SQL
        print("Initializing database schema...")
        with open(schema_path, "r") as sf:
            schema_sql = sf.read()
        cur.execute(schema_sql)
        print("Schema initialization completed.")

# ========================
# Global state for graceful shutdown
# ========================
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle SIGINT/SIGTERM for graceful shutdown."""
    global shutdown_requested
    print(f"\nReceived signal {signum}, shutting down gracefully...")
    shutdown_requested = True

# ========================
# Range processing
# ========================
def _extract_block_data(block, height, blockhash):
    """
    Extract VOUTs (UTXOs) and VINs (spends) from a block.
    Returns: tuple of (vout_rows, spend_rows, stats_dict) or None if shutdown requested.
    """
    if shutdown_requested:
        return None

    block_time = block.get('time', 0)
    block_ts = int(block_time * 1_000_000_000)

    vout_rows = []
    spend_rows = []
    type_counts: Dict[str, int] = defaultdict(int)
    type_balances: Dict[str, int] = defaultdict(int)
    total_utxo = 0
    total_balance_sat = 0

    for tx in block.get("tx", []):
        txid = tx.get("txid", "")
        if shutdown_requested:
            return None

        # Process vins (transaction inputs) to identify spent UTXOs for deletion
        for vin_idx, vin in enumerate(tx.get("vin", [])):
            # Skip coinbase transactions (they don't have valid txid/vout references)
            if "coinbase" in vin:
                continue

            # Extract the UTXO being spent
            spent_txid = vin.get("txid")
            spent_vout = vin.get("vout")

            # Only process if we have valid references
            if spent_txid and spent_vout is not None:
                spend_rows.append((
                    txid,  # spent_by_txid
                    vin_idx,  # spent_vin
                    height,  # spent_block
                    block_ts,  # spent_block_timestamp
                    spent_txid,  # txid (the UTXO being spent)
                    spent_vout  # vout (the UTXO being spent)
                ))

        # Process vouts (transaction outputs) to create new UTXOs
        for idx, v in enumerate(tx.get("vout", [])):
            spk = v.get("scriptPubKey", {})
            address = address_from_vout(v)
            value_btc = float(v.get("value", 0))
            value_sat = int(value_btc * 100_000_000)
            script_type = detect_script_type(spk)
            vout_rows.append((
                txid,
                idx,
                address or None,
                value_sat,
                spk.get("hex", ""),
                script_type,
                height,
                block_ts
            ))
            type_counts[script_type] += 1
            type_balances[script_type] += value_sat
            total_utxo += 1
            total_balance_sat += value_sat

    return {
        'vout_rows': vout_rows,
        'spend_rows': spend_rows,
        'block_info': {
            "block_height": height,
            "block_hash": blockhash,
            "block_timestamp": block_ts
        },
        'stats': {
            'total_utxo': total_utxo,
            'total_balance_sat': total_balance_sat,
            'type_counts': dict(type_counts),
            'type_balances': dict(type_balances)
        }
    }

def get_last_processed_height():
    try:
        with db.get_db_cursor() as cur:
            cur.execute("SELECT MAX(block_height) FROM block_log")
            row = cur.fetchone()
            return row[0] if row and row[0] is not None else 0
    except Exception as e:
        print(f"Error getting last processed height: {e}")
        return 0

def process_single_block(height):
    """Process a single block and extract VOUTs (UTXOs) to database."""
    if shutdown_requested:
        return False

    try:
        blockhash = rpc_call("getblockhash", [height])
        block = rpc_call("getblock", [blockhash, 3])
    except Exception as e:
        # Handle case where block is not yet available during sync
        error_msg = str(e)
        if "not found" in error_msg.lower() or "invalid" in error_msg.lower():
            print(f"Block {height} not yet available (bitcoind may still be syncing)")
            return False
        print(f"Error reading block {height}: {e}")
        return False

    block_data = _extract_block_data(block, height, blockhash)
    if block_data is None:
        print(f"Shutdown requested while processing block {height}")
        return False

    vout_rows = block_data['vout_rows']
    spend_rows = block_data['spend_rows']
    block_ts = block_data['block_info']['block_timestamp']
    stats = block_data['stats']
    total_utxo = stats['total_utxo']
    total_balance_sat = stats['total_balance_sat']
    type_counts = stats['type_counts']
    type_balances = stats['type_balances']

    try:
        db_start = time.time()
        print(f"[Block {height}] Starting database write...")
        with db.get_db_cursor() as cur:
            if shutdown_requested:
                print(f"Shutdown requested before writing block {height} to database")
                return False
            # Performance settings for single block inserts
            cur.execute("SET LOCAL synchronous_commit = OFF")
            if vout_rows:
                vout_start = time.time()
                vout_count = len(vout_rows)
                print(f"[Block {height}] Inserting {vout_count} UTXOs using COPY...")
                # Use COPY FROM for fastest bulk inserts
                bulk_insert_utxos_copy(cur, vout_rows)
                update_address_status_with_vout_balance(cur, vout_rows)
                vout_time = time.time() - vout_start
                if vout_time > 0:
                    rows_per_sec = vout_count / vout_time
                    print(f"[Block {height}] Inserted {vout_count} UTXOs in {vout_time:.3f}s ({rows_per_sec:.0f} rows/sec)")

            # Delete UTXOs that have been spent
            deleted_balance_sat = 0
            deleted_type_balances: Dict[str, int] = defaultdict(int)
            deleted_type_counts: Dict[str, int] = defaultdict(int)
            deleted_utxos = []

            if spend_rows:
                spend_start = time.time()
                spend_count = len(spend_rows)
                print(f"[Block {height}] Deleting {spend_count} spent UTXOs...")
                deleted_utxos = bulk_delete_utxos(cur, spend_rows)
                # Update address_status to decrement balances for spent UTXOs
                update_address_status_with_vin_balance(cur, deleted_utxos, height=height, block_ts=block_ts)

                # Track balance and counts for deleted UTXOs
                for address, value_sat, script_type in deleted_utxos:
                    deleted_balance_sat += value_sat
                    deleted_type_balances[script_type] += value_sat
                    deleted_type_counts[script_type] += 1
                spend_time = time.time() - spend_start
                if spend_time > 0:
                    rows_per_sec = spend_count / spend_time
                    print(f"[Block {height}] Deleted {spend_count} spent UTXOs in {spend_time:.3f}s ({rows_per_sec:.0f} rows/sec)")

            # Insert block log entry
            bulk_insert_block_log_copy(cur, [(height, blockhash, block_ts)])

            # Calculate net changes for stats (additions - deletions)
            net_total_utxo = total_utxo - sum(deleted_type_counts.values())
            net_total_balance_sat = total_balance_sat - deleted_balance_sat
            all_type_keys = set(type_counts.keys()) | set(deleted_type_counts.keys())
            net_type_counts = {k: type_counts.get(k, 0) - deleted_type_counts.get(k, 0) for k in all_type_keys}
            all_balance_keys = set(type_balances.keys()) | set(deleted_type_balances.keys())
            net_type_balances = {k: type_balances.get(k, 0) - deleted_type_balances.get(k, 0) for k in all_balance_keys}
            # Remove zero values
            net_type_counts = {k: v for k, v in net_type_counts.items() if v != 0}
            net_type_balances = {k: v for k, v in net_type_balances.items() if v != 0}

            stats_batch = prepare_stats_batch(net_total_utxo, net_type_counts, net_total_balance_sat, net_type_balances)
            upsert_stats(cur, stats_batch)

            # Update address_stats table for affected script types
            update_address_stats_incremental(cur, vout_rows, deleted_utxos)
        db_time = time.time() - db_start
        print(f"[Block {height}] Database write completed in {db_time:.3f}s")
        print(f"Processed block {height} ({blockhash[:16]}...)")
        return True
    except Exception as e:
        print(f"Error writing block {height} to PostgreSQL: {e}")
        return False

def process_range_legacy(start_height, end_height, chunk_size=CHUNK_SIZE):
    if start_height > end_height:
        return

    print(f"Processing blocks {start_height} → {end_height} in chunks of {chunk_size}")

    def process_block_data(block, height, blockhash):
        """Process a single block's data and extract VOUTs (UTXOs) and VINs (spends)."""
        block_data = _extract_block_data(block, height, blockhash)
        if block_data is None:
            print(f"Shutdown requested while preparing chunk data for block {height}")
            return {
                'vout_rows': [],
                'spend_rows': [],
                'block_info': None
            }
        return block_data

    for chunk_start in range(start_height, end_height + 1, chunk_size):
        if shutdown_requested:
            print("Shutdown requested during catch-up")
            break

        chunk_end = min(chunk_start + chunk_size - 1, end_height)
        print(f"Processing chunk {chunk_start} → {chunk_end}")

        out_lists = {'vout_rows': [], 'spend_rows': [], 'scanned': []}
        chunk_heights = list(range(chunk_start, chunk_end + 1))
        chunk_type_counts: Dict[str, int] = defaultdict(int)
        chunk_type_balances: Dict[str, int] = defaultdict(int)
        chunk_total_utxo = 0
        chunk_total_balance_sat = 0

        if not chunk_heights:
            continue

        fetch_start = time.time()

        try:
            hash_batch_requests = [("getblockhash", [height], height) for height in chunk_heights]
            hash_results = rpc_batch_call(hash_batch_requests, description=f"getblockhash {chunk_start}-{chunk_end}")

            block_batch_requests = [("getblock", [hash_results[height], 3], height) for height in chunk_heights]
            block_results = rpc_batch_call(block_batch_requests, description=f"getblock {chunk_start}-{chunk_end}")
        except RPCBatchError as e:
            print(f"Fatal RPC failure while fetching chunk {chunk_start}-{chunk_end}: {e}")
            raise

        for height in chunk_heights:
            if shutdown_requested:
                print(f"Shutdown requested while processing chunk {chunk_start}-{chunk_end}, stopping block accumulation")
                break

            blockhash = hash_results.get(height)
            block = block_results.get(height)

            if blockhash is None or block is None:
                raise RPCBatchError(f"Missing RPC data for block {height}", pending=[height], last_error="missing data")

            try:
                block_data = process_block_data(block, height, blockhash)
            except Exception as e:
                raise RuntimeError(f"Failed to process block {height}: {e}") from e

            out_lists['vout_rows'].extend(block_data['vout_rows'])
            out_lists['spend_rows'].extend(block_data.get('spend_rows', []))
            if block_data['block_info']:
                out_lists['scanned'].append(block_data['block_info'])
            block_stats = block_data.get('stats', {})
            chunk_total_utxo += block_stats.get('total_utxo', 0)
            chunk_total_balance_sat += block_stats.get('total_balance_sat', 0)
            for script_type, count in block_stats.get('type_counts', {}).items():
                chunk_type_counts[script_type] += count
            for script_type, balance in block_stats.get('type_balances', {}).items():
                chunk_type_balances[script_type] += balance

        if shutdown_requested:
            break

        fetch_time = time.time() - fetch_start
        print(f"[Chunk {chunk_start}-{chunk_end}] Fetched {chunk_end - chunk_start + 1} blocks in {fetch_time:.3f}s")

        if shutdown_requested:
            print(f"Skipping database write for chunk {chunk_start}-{chunk_end} due to shutdown request")
            break

        try:
            db_start = time.time()
            print(f"[Chunk {chunk_start}-{chunk_end}] Starting database write...")
            with db.get_db_cursor() as cur:
                if shutdown_requested:
                    print(f"Shutdown requested before writing chunk {chunk_start}-{chunk_end} to database")
                    break

                if out_lists['vout_rows']:
                    vout_start = time.time()
                    vout_count = len(out_lists['vout_rows'])
                    print(f"[Chunk {chunk_start}-{chunk_end}] Inserting {vout_count} UTXOs ...")
                    bulk_insert_utxos_copy(cur, out_lists['vout_rows'])
                    update_address_status_with_vout_balance(cur, out_lists['vout_rows'])
                    vout_time = time.time() - vout_start
                    if vout_time > 0:
                        rows_per_sec = vout_count / vout_time
                        print(f"[Chunk {chunk_start}-{chunk_end}] Inserted {vout_count} UTXOs in {vout_time:.3f}s ({rows_per_sec:.0f} rows/sec)")

                # Delete UTXOs that have been spent
                deleted_balance_sat = 0
                deleted_type_balances: Dict[str, int] = defaultdict(int)
                deleted_type_counts: Dict[str, int] = defaultdict(int)

                if out_lists['spend_rows']:
                    spend_start = time.time()
                    spend_count = len(out_lists['spend_rows'])
                    print(f"[Chunk {chunk_start}-{chunk_end}] Deleting {spend_count} spent UTXOs...")
                    deleted_utxos = bulk_delete_utxos(cur, out_lists['spend_rows'])

                    # Extract max block height and timestamp from spend_rows for new address creation
                    # spend_rows structure: (spent_by_txid, spent_vin, spent_block, spent_block_timestamp, txid, vout)
                    max_height = max(row[2] for row in out_lists['spend_rows']) if out_lists['spend_rows'] else None
                    max_block_ts = max(row[3] for row in out_lists['spend_rows']) if out_lists['spend_rows'] else None

                    # Update address_status to decrement balances for spent UTXOs
                    update_address_status_with_vin_balance(cur, deleted_utxos, height=max_height, block_ts=max_block_ts)

                    # Track balance and counts for deleted UTXOs
                    for address, value_sat, script_type in deleted_utxos:
                        deleted_balance_sat += value_sat
                        deleted_type_balances[script_type] += value_sat
                        deleted_type_counts[script_type] += 1
                    spend_time = time.time() - spend_start
                    if spend_time > 0:
                        rows_per_sec = spend_count / spend_time
                        print(f"[Chunk {chunk_start}-{chunk_end}] Deleted {spend_count} spent UTXOs in {spend_time:.3f}s ({rows_per_sec:.0f} rows/sec)")

                if out_lists['scanned']:
                    block_log_start = time.time()
                    block_count = len(out_lists['scanned'])
                    block_vals = [(b['block_height'], b['block_hash'], b['block_timestamp']) for b in out_lists['scanned']]
                    print(f"[Chunk {chunk_start}-{chunk_end}] Inserting {block_count} block_log entries using COPY...")
                    bulk_insert_block_log_copy(cur, block_vals)
                    block_log_time = time.time() - block_log_start
                    print(f"[Chunk {chunk_start}-{chunk_end}] Inserted {block_count} block_log entries in {block_log_time:.3f}s")

                # Calculate net changes for stats (additions - deletions)
                net_total_utxo = chunk_total_utxo - sum(deleted_type_counts.values())
                net_total_balance_sat = chunk_total_balance_sat - deleted_balance_sat
                all_type_keys = set(chunk_type_counts.keys()) | set(deleted_type_counts.keys())
                net_type_counts = {k: chunk_type_counts.get(k, 0) - deleted_type_counts.get(k, 0) for k in all_type_keys}
                all_balance_keys = set(chunk_type_balances.keys()) | set(deleted_type_balances.keys())
                net_type_balances = {k: chunk_type_balances.get(k, 0) - deleted_type_balances.get(k, 0) for k in all_balance_keys}
                # Remove zero values
                net_type_counts = {k: v for k, v in net_type_counts.items() if v != 0}
                net_type_balances = {k: v for k, v in net_type_balances.items() if v != 0}

                stats_batch = prepare_stats_batch(net_total_utxo, net_type_counts, net_total_balance_sat, net_type_balances)
                upsert_stats(cur, stats_batch)

            db_time = time.time() - db_start
            print(f"[Chunk {chunk_start}-{chunk_end}] Database write completed in {db_time:.3f}s")
        except Exception as e:
            print(f"Error writing chunk {chunk_start}-{chunk_end} to PostgreSQL: {e}")
        print(f"Saved and flushed chunk {chunk_start}–{chunk_end}")

def read_utxo(height: int = 840_000, batch_size: int = 1_000_000):
    batch: List[tuple] = []
    type_counts: Dict[str, int] = defaultdict(int)
    total_utxo = 0

    insert_sql = """
        INSERT INTO utxos (
            txid, vout, address, value_sat, script_pub_key_hex, script_pub_type,
            created_block, created_block_timestamp
        ) VALUES %s
    """

    with db.get_db_cursor() as cur:
        cur.execute("ALTER TABLE utxos DROP CONSTRAINT utxos_pkey;")
        cur.connection.commit()

    with open('./data/utxo.csv', 'r') as f:
        reader = csv.reader(f, delimiter=',')
        read_start = time.time()
        for line_num, fields in enumerate(reader, start=1):
            if len(fields) != 6:
                print(f"Warning: Skipping malformed line {line_num} (expected 6 fields, got {len(fields)})", file=sys.stderr)
                continue

            try:
                outpoint_parts = fields[0].split(':', 1)
                txid = outpoint_parts[0].strip()
                vout = int(outpoint_parts[1].strip())

                block_height = int(fields[2].strip())
                amount = int(fields[3].strip())
                script_pub_key = fields[4].strip()
                address = fields[5].strip()
                script_type = get_script_type(script_pub_key, address)
                type_counts[script_type] += 1

                if address == '':
                    continue

                row_tuple = (
                    txid, vout, address, amount, script_pub_key,
                    script_type, block_height, 0
                )

                batch.append(row_tuple)
                total_utxo += 1

                if len(batch) >= batch_size:
                    print(f"Reading batch of {len(batch)} rows..., Read CSV batch in {time.time() - read_start}s", file=sys.stderr)
                    if shutdown_requested:
                        print(f"Shutdown requested, stopping UTXO import", file=sys.stderr)
                        break
                    print(f"Inserting batch of {len(batch)} rows...", file=sys.stderr)
                    insert_start = time.time()
                    with db.get_db_cursor() as cur:
                            execute_values(cur, insert_sql, batch, page_size=1000)
                            cur.connection.commit()
                    insert_time = time.time() - insert_start
                    rows_per_sec = len(batch) / insert_time
                    print(f"Inserted batch of {len(batch)} rows in {insert_time:.3f}s ({rows_per_sec:.0f} rows/sec). Total rows: {total_utxo}", file=sys.stderr)
                    batch = []
                    read_start = time.time()

            except (ValueError, IndexError) as e:
                print(f"Warning: Skipping line {line_num} due to parse error: {e}", file=sys.stderr)
                continue

    stats_batch = prepare_stats_batch(total_utxo, dict(type_counts))

    final_insert_start = time.time()
    print(f"Inserting final batch of {len(batch)} rows...", file=sys.stderr)
    with db.get_db_cursor() as cur:
        if len(batch) > 0:
            final_batch_start = time.time()
            execute_values(cur, insert_sql, batch, page_size=1000)
            final_batch_time = time.time() - final_batch_start
            if final_batch_time > 0:
                rows_per_sec = len(batch) / final_batch_time
                print(f"Inserted final batch of {len(batch)} rows in {final_batch_time:.3f}s ({rows_per_sec:.0f} rows/sec)", file=sys.stderr)
        upsert_stats(cur, stats_batch, absolute=True)

        index_start = time.time()
        # Check if primary key exists before adding it
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'utxos_pkey'
                    AND conrelid = 'utxos'::regclass
                ) THEN
                    ALTER TABLE utxos ADD PRIMARY KEY (txid, vout);
                END IF;
            END $$;
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS utxos_address_idx ON utxos (address);")
        cur.execute("INSERT INTO block_log (block_height, block_hash, block_timestamp) VALUES (%s, %s, 0) ON CONFLICT DO NOTHING;", (height, '0' * 64))
        index_time = time.time() - index_start
        print(f"Created indexes and updated block_log in {index_time:.3f}s", file=sys.stderr)

        cur.connection.commit()

    final_insert_time = time.time() - final_insert_start
    print(f"Final batch insertion complete in {final_insert_time:.3f}s. ", file=sys.stderr)
    print(f"UTXO import complete. Total rows inserted: {total_utxo}", file=sys.stderr)

    return

def main():
    global shutdown_requested

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Initialize database pool once at startup
    db.init_pool()

    schema_init()
    last = get_last_processed_height()

    # Initial catch-up: process from last_processed to current tip
    print(f"Starting indexer. Last processed: {last}")
    print(f"RPC Configuration: {RPC_HOST}:{RPC_PORT} (user: {RPC_USER})")

    # Test RPC connection before proceeding
    print("\nTesting Bitcoin RPC connection...")
    if not test_rpc_connection():
        print("\nFatal: Cannot connect to Bitcoin RPC. Please fix the connection and try again.")
        db.shutdown_pool()
        sys.exit(1)

    try:
        tip = rpc_call("getblockcount")
        start = max(0, last + 1)

        print(f"Tippity top: {tip} | Starting from: {start}")

        if IS_RECALC:
            print("Recalculating stats from database...")
            with db.get_db_cursor() as cur:
                recalculate_stats_from_db(cur)
                cur.connection.commit()
            print("Recalculation complete.")
            db.shutdown_pool()
            return

        if IS_UTXO:
            print("UTXO mode enabled - skipping historical block processing")
            read_utxo(height=923867)
            db.shutdown_pool()
            return

        epsilon = 100
        if start + epsilon <= tip:
            print(f"Catching up: processing blocks {start} → {tip}")
            
            # Choose block reader based on configuration
            from .block_reader import RPCBlockReader, BLKFileReader
            from .range_processor import process_range
            
            BLOCK_SOURCE = config.BLOCK_SOURCE
            if BLOCK_SOURCE == 'blk':
                blocks_dir = config.BLOCKS_DIR
                block_reader = BLKFileReader(
                    blocks_dir=blocks_dir,
                    rpc_call=rpc_call,
                    rpc_batch_call=rpc_batch_call
                )
                print("Using BLK file reader for block fetching")
            else:
                # Use RPC reader (default)
                block_reader = RPCBlockReader(
                    rpc_batch_call=rpc_batch_call,
                    rpc_call=rpc_call
                )
                print("Using RPC reader for block fetching")
            
            # Process range with selected block reader
            process_range(start, tip, block_reader, chunk_size=CHUNK_SIZE, shutdown_check=lambda: shutdown_requested)

        if shutdown_requested:
            print("Shutdown requested during catch-up, exiting...")
            db.shutdown_pool()
            sys.exit(0)

        # Continuous polling loop
        print("Catch-up complete. Entering continuous sync mode...")

        # Recreate address index if missing
        with db.get_db_cursor() as cur:
            cur.execute("CREATE INDEX IF NOT EXISTS utxos_address_idx ON utxos (address);")
            # Check if primary key exists before adding it
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'utxos_pkey'
                        AND conrelid = 'utxos'::regclass
                    ) THEN
                        ALTER TABLE utxos ADD PRIMARY KEY (txid, vout);
                    END IF;
                END $$;
            """)
            cur.connection.commit()

        while not shutdown_requested:
            try:
                current_tip = rpc_call("getblockcount")
                last_processed = get_last_processed_height()

                if last_processed < current_tip:
                    # Process new blocks one at a time
                    for height in range(last_processed + 1, current_tip + 1):
                        if shutdown_requested:
                            break
                        # Retry logic for individual blocks
                        retries = 0
                        while retries < MAX_RETRIES:
                            success = process_single_block(height)
                            if success:
                                break
                            retries += 1
                            if retries < MAX_RETRIES:
                                print(f"Retrying block {height} (attempt {retries + 1}/{MAX_RETRIES})...")
                                time.sleep(1)
                            else:
                                print(f"Failed to process block {height} after {MAX_RETRIES} retries")

                # Wait before next poll
                time.sleep(POLL_INTERVAL)

            except requests.exceptions.RequestException as e:
                print(f"RPC connection error: {e}. Retrying in {RECONNECT_DELAY} seconds...")
                time.sleep(RECONNECT_DELAY)
            except Exception as e:
                print(f"Unexpected error in polling loop: {e}")
                time.sleep(RECONNECT_DELAY)

        print("Shutdown complete. Cleaning up...")
        db.shutdown_pool()

    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        db.shutdown_pool()
        sys.exit(1)

if __name__ == "__main__":
    main()