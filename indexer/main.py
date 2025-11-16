#!/usr/bin/env python3
"""
Bitcoin UTXO indexer - extracts VOUTs (UTXOs) from Bitcoin blocks
and stores them in PostgreSQL for efficient querying.
"""
from bitcoin import SelectParams

import requests, json
import time
import signal
import sys
import os

from typing import Generator, Dict, Any, List
from collections import defaultdict
from psycopg2.extras import execute_batch, execute_values
from . import db
from .utils import detect_script_type, address_from_vout, get_script_type

import csv

# ========================
# CONFIGURATION
# ========================
NETWORK = 'testnet'
RPC_USER = os.getenv('RPC_USER', 'admin')
RPC_PASSWORD = os.getenv('RPC_PASSWORD', 'pass')
RPC_HOST = os.getenv('RPC_HOST', '127.0.0.1')
RPC_PORT = int(os.getenv('RPC_PORT', '18332'))
RPC_URL = f"http://{RPC_HOST}:{RPC_PORT}/"

SelectParams(NETWORK)

POLL_INTERVAL = 1.0                       # seconds between checking for new blocks
RECONNECT_DELAY = 5.0                     # seconds to wait after connection error

# Tuning knobs (can be overridden by env)
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', '2'))                    # blocks per catch-up chunk
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '5'))                    # max retries for failed RPC
DB_PAGE_ROWS = int(os.getenv('DB_PAGE_ROWS', '1000'))              # rows per DB insert page
IS_UTXO = bool(int(os.getenv('IS_UTXO', '0')))                      # whether indexing UTXOs (True) or TXOs (False)

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

def bulk_update_utxo_spends(cur, spend_rows):
    """
    Update UTXOs with spend information.
    spend_rows: list of tuples (spent_by_txid, spent_vin, spent_block, spent_block_timestamp, txid, vout)
    """
    if not spend_rows:
        return
    
    update_sql = """
        UPDATE utxos
        SET 
            spent = TRUE,
            spent_by_txid = data.spent_by_txid,
            spent_vin = data.spent_vin,
            spent_block = data.spent_block,
            spent_block_timestamp = data.spent_block_timestamp
        FROM (VALUES %s) AS data(spent_by_txid, spent_vin, spent_block, spent_block_timestamp, txid, vout)
        WHERE utxos.txid = data.txid AND utxos.vout = data.vout
    """
    
    execute_values(cur, update_sql, spend_rows, page_size=DB_PAGE_ROWS)

def prepare_stats_batch(total_utxo: int, type_counts: Dict[str, int]) -> List[tuple]:
    """
    Prepare stats records for insertion/upsert.
    """
    stats_batch = []
    if total_utxo:
        stats_batch.append(('total_utxo', total_utxo))
    for script_type, count in type_counts.items():
        stats_batch.append((f"{script_type}_count", count))
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

def schema_init():
    import os
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
    
    block_time = block.get('time', 0)
    block_ts = int(block_time * 1_000_000_000)
    
    vout_rows = []
    spend_rows = []
    type_counts: Dict[str, int] = defaultdict(int)
    total_utxo = 0
    
    for tx in block.get("tx", []):
        txid = tx.get("txid", "")
        if shutdown_requested:
            print(f"Shutdown requested while scanning transactions for block {height}")
            return False
        
        # Process vins (transaction inputs) to mark UTXOs as spent
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
            total_utxo += 1
    
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
                vout_time = time.time() - vout_start
                if vout_time > 0:
                    rows_per_sec = vout_count / vout_time
                    print(f"[Block {height}] Inserted {vout_count} UTXOs in {vout_time:.3f}s ({rows_per_sec:.0f} rows/sec)")
            
            # Update UTXOs with spend information
            if spend_rows:
                spend_start = time.time()
                spend_count = len(spend_rows)
                print(f"[Block {height}] Updating {spend_count} UTXO spends...")
                bulk_update_utxo_spends(cur, spend_rows)
                spend_time = time.time() - spend_start
                if spend_time > 0:
                    rows_per_sec = spend_count / spend_time
                    print(f"[Block {height}] Updated {spend_count} UTXO spends in {spend_time:.3f}s ({rows_per_sec:.0f} rows/sec)")
            
            # Insert block log entry
            bulk_insert_block_log_copy(cur, [(height, blockhash, block_ts)])
            stats_batch = prepare_stats_batch(total_utxo, dict(type_counts))
            upsert_stats(cur, stats_batch)
        db_time = time.time() - db_start
        print(f"[Block {height}] Database write completed in {db_time:.3f}s")
        print(f"Processed block {height} ({blockhash[:16]}...)")
        return True
    except Exception as e:
        print(f"Error writing block {height} to PostgreSQL: {e}")
        return False

def process_range(start_height, end_height, chunk_size=CHUNK_SIZE):
    if start_height > end_height:
        return

    print(f"Processing blocks {start_height} → {end_height} in chunks of {chunk_size}")

    def process_block_data(block, height, blockhash):
        """Process a single block's data and extract VOUTs (UTXOs) and VINs (spends)."""
        block_time = block.get('time', 0)
        block_ts = int(block_time * 1_000_000_000)

        vout_rows = []
        spend_rows = []
        type_counts: Dict[str, int] = defaultdict(int)
        total_utxo = 0

        for tx in block.get("tx", []):
            txid = tx.get("txid", "")
            if shutdown_requested:
                print(f"Shutdown requested while preparing chunk data for block {height}")
                return {
                    'vout_rows': [],
                    'spend_rows': [],
                    'block_info': None
                }
            
            # Process vins (transaction inputs) to mark UTXOs as spent
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
                total_utxo += 1

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
                'type_counts': dict(type_counts)
            }
        }

    for chunk_start in range(start_height, end_height + 1, chunk_size):
        if shutdown_requested:
            print("Shutdown requested during catch-up")
            break
        
        chunk_end = min(chunk_start + chunk_size - 1, end_height)
        print(f"Processing chunk {chunk_start} → {chunk_end}")

        out_lists = {'vout_rows': [], 'spend_rows': [], 'scanned': []}
        chunk_heights = list(range(chunk_start, chunk_end + 1))
        chunk_type_counts: Dict[str, int] = defaultdict(int)
        chunk_total_utxo = 0

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
            for script_type, count in block_stats.get('type_counts', {}).items():
                chunk_type_counts[script_type] += count

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
                    vout_time = time.time() - vout_start
                    if vout_time > 0:
                        rows_per_sec = vout_count / vout_time
                        print(f"[Chunk {chunk_start}-{chunk_end}] Inserted {vout_count} UTXOs in {vout_time:.3f}s ({rows_per_sec:.0f} rows/sec)")

                # Update UTXOs with spend information
                if out_lists['spend_rows']:
                    spend_start = time.time()
                    spend_count = len(out_lists['spend_rows'])
                    print(f"[Chunk {chunk_start}-{chunk_end}] Updating {spend_count} UTXO spends...")
                    bulk_update_utxo_spends(cur, out_lists['spend_rows'])
                    spend_time = time.time() - spend_start
                    if spend_time > 0:
                        rows_per_sec = spend_count / spend_time
                        print(f"[Chunk {chunk_start}-{chunk_end}] Updated {spend_count} UTXO spends in {spend_time:.3f}s ({rows_per_sec:.0f} rows/sec)")

                if out_lists['scanned']:
                    block_log_start = time.time()
                    block_count = len(out_lists['scanned'])
                    block_vals = [(b['block_height'], b['block_hash'], b['block_timestamp']) for b in out_lists['scanned']]
                    print(f"[Chunk {chunk_start}-{chunk_end}] Inserting {block_count} block_log entries using COPY...")
                    bulk_insert_block_log_copy(cur, block_vals)
                    block_log_time = time.time() - block_log_start
                    print(f"[Chunk {chunk_start}-{chunk_end}] Inserted {block_count} block_log entries in {block_log_time:.3f}s")
                
                stats_batch = prepare_stats_batch(chunk_total_utxo, dict(chunk_type_counts))
                upsert_stats(cur, stats_batch)

            db_time = time.time() - db_start
            print(f"[Chunk {chunk_start}-{chunk_end}] Database write completed in {db_time:.3f}s")
        except Exception as e:
            print(f"Error writing chunk {chunk_start}-{chunk_end} to PostgreSQL: {e}")
        print(f"Saved and flushed chunk {chunk_start}–{chunk_end}")

def read_utxo(height: int = 840_000,batch_size: int = 1_000_000):
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
        upsert_stats(cur, stats_batch, absolute=True)
        final_batch_time = time.time() - final_batch_start
        if final_batch_time > 0:
            rows_per_sec = len(batch) / final_batch_time
            print(f"Inserted final batch of {len(batch)} rows in {final_batch_time:.3f}s ({rows_per_sec:.0f} rows/sec)", file=sys.stderr)

        index_start = time.time()
        cur.execute("ALTER TABLE utxos ADD PRIMARY KEY (txid, vout);")
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

        if IS_UTXO:
            print("UTXO mode enabled - skipping historical block processing")
            read_utxo(height=923867)
            return

        if start <= tip:
            print(f"Catching up: processing blocks {start} → {tip}")
            process_range(start, tip)

        if shutdown_requested:
            print("Shutdown requested during catch-up, exiting...")
            db.shutdown_pool()
            sys.exit(0)
        
        # Continuous polling loop
        print("Catch-up complete. Entering continuous sync mode...")

        # Recreate address index if missing
        with db.get_db_cursor() as cur:
            cur.execute("CREATE INDEX IF NOT EXISTS utxos_address_idx ON utxos (address);")
            cur.execute("ALTER TABLE utxos ADD PRIMARY KEY (txid, vout);")
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