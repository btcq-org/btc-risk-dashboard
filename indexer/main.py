#!/usr/bin/env python3
"""
Find "quantum-at-risk" pubkeys (revealed pubkeys in inputs or raw pubkeys in P2PK outputs)
in Bitcoin *testnet* blocks using your local bitcoind node via RPC.
"""
from bitcoin.wallet import CBitcoinAddress, P2PKHBitcoinAddress, P2WPKHBitcoinAddress
from bitcoin.core import x
from bitcoin.core.script import CScript
from bitcoin.core.key import CPubKey
from bitcoin import SelectParams

import requests, json, concurrent.futures
from threading import Lock
import time
import signal
import sys
import os

import psycopg2
from psycopg2.extras import execute_batch, execute_values
from . import db

from pycoin.symbols.btc import network

# ========================
# CONFIGURATION
# ========================
NETWORK = 'testnet'
RPC_USER = "admin"
RPC_PASSWORD = "pass"
RPC_PORT = 18332
RPC_URL = f"http://127.0.0.1:{RPC_PORT}/"

SelectParams(NETWORK)

THREADS = 8              # number of parallel workers
INITIAL_BLOCKS = 100     # number of historical blocks to scan on startup
POLL_INTERVAL = 1.0      # seconds between checking for new blocks
RECONNECT_DELAY = 5.0    # seconds to wait after connection error

CHUNK_SIZE = 1000        # number of blocks to process per DB bulk write
MAX_RETRIES = 5          # max retries for failed block fetches per chunk
REFRESH_INTERVAL = 10    # refresh address_status every N blocks (0 = every block, -1 = never)
VACUUM_INTERVAL = 100    # run VACUUM ANALYZE every N chunks (0 = every chunk, -1 = never)

# ========================
# RPC UTILS
# ========================
def rpc_call(method, params=None):
    payload = json.dumps({
        "jsonrpc": "1.0",
        "id": "quantum-check",
        "method": method,
        "params": params or []
    })
    resp = requests.post(RPC_URL, auth=(RPC_USER, RPC_PASSWORD),
                         headers={"content-type": "text/plain"},
                         data=payload)
    resp.raise_for_status()
    r = resp.json()
    if r.get("error"):
        raise Exception(r["error"])
    return r["result"]

# ========================
# Utils
# =======================
def detect_script_type(spk):
        """Detect scriptPubKey type for common and rare types."""
        if not spk:
            return None
        
        script_type_map = {
            "pubkey": "P2PK",
            "pubkeyhash": "P2PKH", 
            "scripthash": "P2SH",
            "multisig": "P2MS",
            "witness_v0_keyhash": "P2WPKH",
            "witness_v0_scripthash": "P2WSH",
            "v1_p2tr": "P2TR",
            "witness_v1_taproot": "P2TR",
            "nulldata": "OP_RETURN",
            "op_return": "OP_RETURN",
            "nonstandard": "nonstandard"
        }
        
        return script_type_map.get(spk.get("type"), "nonstandard")

def address_from_vout(v):
    """Extract address from vout's scriptPubKey.addresses array (Bitcoin Core RPC format)."""
    if not v:
        return None
    spk = v.get("scriptPubKey", {})
    if not spk:
        return None
    addresses = spk.get("addresses", [])
    if addresses and len(addresses) > 0:
        return addresses[0]
    # Fallback: try address field directly (some RPC versions use this)
    address = spk.get("address")
    if address:
        return address
    
    script_hex = v.get("scriptPubKey", {}).get("hex")
    if script_hex:
        script_bytes = bytes.fromhex(script_hex)
        address = network.address.for_script(script_bytes)
        if address:
            return str(address)

    return None

# ========================
# DB
# ========================
def get_db_conn():
    host = os.getenv('PGHOST', '127.0.0.1')
    port = int(os.getenv('PGPORT', '5432'))
    db = os.getenv('PGDATABASE', 'postgres')
    user = os.getenv('PGUSER', 'indexer')
    password = os.getenv('PGPASSWORD', 'password')
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)

# ========================
def schema_init():
    import os
    schema_path = os.path.join(os.path.dirname(__file__), "db", "schema.sql")
    db.init_pool()
    try:
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
            try:
                with open(schema_path, "r") as sf:
                    schema_sql = sf.read()
                cur.execute(schema_sql)
                print("Schema initialization completed.")
            except FileNotFoundError:
                raise
    finally:
        db.shutdown_pool()

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

# Track blocks since last refresh for rate limiting
_blocks_since_refresh = 0
_chunks_processed = 0  # Track chunks for VACUUM maintenance

def process_single_block(height):
    """Process a single block and save to database immediately."""
    global _blocks_since_refresh
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
    vin_rows = []
    
    for tx in block.get("tx", []):
        txid = tx.get("txid", "")
        for idx, v in enumerate(tx.get("vout", [])):
            spk = v.get("scriptPubKey", {})
            address = address_from_vout(v)
            value_btc = float(v.get("value", 0))
            value_sat = int(value_btc * 100_000_000)
            vout_rows.append((
                txid,
                idx,
                address or None,
                value_sat,
                spk.get("hex", ""),
                detect_script_type(spk),
                height,
                block_ts
            ))
        for vin_idx, vin in enumerate(tx.get("vin", [])):
            if "txid" not in vin:
                continue
            prev_txid = vin.get("txid")
            prev_vout = int(vin.get("vout", 0))
            vin_rows.append((
                True,
                txid,
                vin_idx,
                height,
                block_ts,
                prev_txid,
                prev_vout
            ))
    
    try:
        db_start = time.time()
        print(f"[Block {height}] Starting database write...")
        with db.get_db_cursor() as cur:
            if vout_rows:
                vout_start = time.time()
                vout_count = len(vout_rows)
                print(f"[Block {height}] Inserting {vout_count} UTXOs...")
                # Use execute_values for faster bulk inserts (uses COPY internally)
                execute_values(cur,
                    """
                    INSERT INTO utxos (txid, vout, address, value_sat, script_pub_key_hex, script_pub_type, created_block, created_block_timestamp)
                    VALUES %s
                    ON CONFLICT (txid, vout) DO UPDATE SET
                        address = EXCLUDED.address,
                        value_sat = EXCLUDED.value_sat,
                        script_pub_key_hex = EXCLUDED.script_pub_key_hex,
                        script_pub_type = EXCLUDED.script_pub_type,
                        created_block = EXCLUDED.created_block,
                        created_block_timestamp = EXCLUDED.created_block_timestamp
                    """,
                    vout_rows, page_size=1000, template=None
                )
                vout_time = time.time() - vout_start
                print(f"[Block {height}] Inserted {vout_count} UTXOs in {vout_time:.3f}s")
            
            if vin_rows:
                vin_start = time.time()
                vin_count = len(vin_rows)
                print(f"[Block {height}] Updating {vin_count} spent UTXOs...")
                # Use execute_batch for UPDATEs (execute_values doesn't support UPDATE well)
                execute_batch(cur,
                    """
                    UPDATE utxos
                    SET spent = %s,
                        spent_by_txid = %s,
                        spent_vin = %s,
                        spent_block = %s,
                        spent_block_timestamp = %s
                    WHERE txid = %s AND vout = %s AND (spent IS DISTINCT FROM TRUE)
                    """,
                    vin_rows, page_size=1000
                )
                vin_time = time.time() - vin_start
                print(f"[Block {height}] Updated {vin_count} spent UTXOs in {vin_time:.3f}s")
            
            # Insert block log entry
            block_log_start = time.time()
            cur.execute(
                """
                INSERT INTO block_log (block_height, block_hash, block_timestamp)
                VALUES (%s, %s, %s)
                """,
                (height, blockhash, block_ts)
            )
            block_log_time = time.time() - block_log_start
            print(f"[Block {height}] Inserted block_log in {block_log_time:.3f}s")
            
            # Refresh address_status materialized view periodically
            # REFRESH_INTERVAL > 0: refresh every N blocks
            # REFRESH_INTERVAL = 0: refresh every block (expensive)
            # REFRESH_INTERVAL < 0: never refresh automatically
            _blocks_since_refresh += 1
            if REFRESH_INTERVAL > 0:
                if _blocks_since_refresh >= REFRESH_INTERVAL:
                    cur.execute("SELECT refresh_address_status()")
                    _blocks_since_refresh = 0
            elif REFRESH_INTERVAL == 0:
                cur.execute("SELECT refresh_address_status()")
                _blocks_since_refresh = 0
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

    lock = Lock()

    def process_block(height, out_lists, failed_blocks):
        blockhash = ""
        try:
            blockhash = rpc_call("getblockhash", [height])
            block = rpc_call("getblock", [blockhash, 3])
        except Exception as e:
            print(f"Error reading block {height}: {e}")
            failed_blocks.append(height)
            return

        block_time = block.get('time', 0)
        block_ts = int(block_time * 1_000_000_000)

        local_vout_rows = []
        local_vin_rows = []

        for tx in block.get("tx", []):
            txid = tx.get("txid", "")
            for idx, v in enumerate(tx.get("vout", [])):
                spk = v.get("scriptPubKey", {})
                address = address_from_vout(v)
                value_btc = float(v.get("value", 0))
                value_sat = int(value_btc * 100_000_000)
                local_vout_rows.append((
                    txid,
                    idx,
                    address or None,
                    value_sat,
                    spk.get("hex", ""),
                    detect_script_type(spk),
                    height,
                    block_ts
                ))
            for vin_idx, vin in enumerate(tx.get("vin", [])):
                if "txid" not in vin:
                    continue
                prev_txid = vin.get("txid")
                prev_vout = int(vin.get("vout", 0))
                local_vin_rows.append((
                    True,
                    txid,
                    vin_idx,
                    height,
                    block_ts,
                    prev_txid,
                    prev_vout
                ))

        with lock:
            out_lists['vout_rows'].extend(local_vout_rows)
            out_lists['vin_rows'].extend(local_vin_rows)
            out_lists['scanned'].append({
                "block_height": height,
                "block_hash": blockhash,
                "block_timestamp": block_ts
            })

    for chunk_start in range(start_height, end_height + 1, chunk_size):
        if shutdown_requested:
            print("Shutdown requested during catch-up")
            break
        
        chunk_end = min(chunk_start + chunk_size - 1, end_height)
        print(f"Processing chunk {chunk_start} → {chunk_end}")

        out_lists = { 'vout_rows': [], 'vin_rows': [], 'scanned': [] }
        to_process = list(range(chunk_start, chunk_end + 1))
        failed_blocks = []
        retries = 0
        
        fetch_start = time.time()
        while to_process and retries < MAX_RETRIES and not shutdown_requested:
            failed_blocks.clear()
            with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
                futures = [executor.submit(process_block, h, out_lists, failed_blocks) for h in to_process]
                for f in concurrent.futures.as_completed(futures):
                    try:
                        f.result()
                    except Exception as e:
                        print(f"Worker error: {e}")
            if failed_blocks:
                print(f"Retrying {len(failed_blocks)} failed blocks in chunk {chunk_start}–{chunk_end} (attempt {retries+2}/{MAX_RETRIES})...")
            to_process = failed_blocks[:]
            retries += 1
        fetch_time = time.time() - fetch_start
        if to_process:
            print(f"Failed to process blocks after {MAX_RETRIES} retries in chunk {chunk_start}–{chunk_end}: {to_process}")
        else:
            print(f"[Chunk {chunk_start}-{chunk_end}] Fetched {chunk_end - chunk_start + 1} blocks in {fetch_time:.3f}s")

        try:
            db_start = time.time()
            print(f"[Chunk {chunk_start}-{chunk_end}] Starting database write...")
            with db.get_db_cursor() as cur:
                if out_lists['vout_rows']:
                    vout_start = time.time()
                    vout_count = len(out_lists['vout_rows'])
                    print(f"[Chunk {chunk_start}-{chunk_end}] Inserting {vout_count} UTXOs...")
                    # Use execute_values for faster bulk inserts (uses COPY internally)
                    execute_values(cur,
                        """
                        INSERT INTO utxos (txid, vout, address, value_sat, script_pub_key_hex, script_pub_type, created_block, created_block_timestamp)
                        VALUES %s
                        ON CONFLICT (txid, vout) DO UPDATE SET
                            address = EXCLUDED.address,
                            value_sat = EXCLUDED.value_sat,
                            script_pub_key_hex = EXCLUDED.script_pub_key_hex,
                            script_pub_type = EXCLUDED.script_pub_type,
                            created_block = EXCLUDED.created_block,
                            created_block_timestamp = EXCLUDED.created_block_timestamp
                        """,
                        out_lists['vout_rows'], page_size=1000, template=None
                    )
                    vout_time = time.time() - vout_start
                    print(f"[Chunk {chunk_start}-{chunk_end}] Inserted {vout_count} UTXOs in {vout_time:.3f}s")

                if out_lists['vin_rows']:
                    vin_start = time.time()
                    vin_count = len(out_lists['vin_rows'])
                    print(f"[Chunk {chunk_start}-{chunk_end}] Updating {vin_count} spent UTXOs...")
                    # Use execute_batch for UPDATEs (execute_values doesn't support UPDATE well)
                    execute_batch(cur,
                        """
                        UPDATE utxos
                        SET spent = %s,
                            spent_by_txid = %s,
                            spent_vin = %s,
                            spent_block = %s,
                            spent_block_timestamp = %s
                        WHERE txid = %s AND vout = %s AND (spent IS DISTINCT FROM TRUE)
                        """,
                        out_lists['vin_rows'], page_size=1000
                    )
                    vin_time = time.time() - vin_start
                    print(f"[Chunk {chunk_start}-{chunk_end}] Updated {vin_count} spent UTXOs in {vin_time:.3f}s")

                if out_lists['scanned']:
                    block_log_start = time.time()
                    block_count = len(out_lists['scanned'])
                    block_vals = [(b['block_height'], b['block_hash'], b['block_timestamp']) for b in out_lists['scanned']]
                    print(f"[Chunk {chunk_start}-{chunk_end}] Inserting {block_count} block_log entries...")
                    db.execute_values(cur,
                        """
                        INSERT INTO block_log (block_height, block_hash, block_timestamp) VALUES %s
                        """,
                        block_vals
                    )
                    block_log_time = time.time() - block_log_start
                    print(f"[Chunk {chunk_start}-{chunk_end}] Inserted {block_count} block_log entries in {block_log_time:.3f}s")
                
                # Refresh address_status materialized view after each chunk during catch-up
                # (chunks are large so refresh overhead is acceptable)
                if REFRESH_INTERVAL >= 0:
                    cur.execute("SELECT refresh_address_status()")
            db_time = time.time() - db_start
            print(f"[Chunk {chunk_start}-{chunk_end}] Database write completed in {db_time:.3f}s")
            
            # Periodic VACUUM ANALYZE for table maintenance (prevents bloat and keeps stats fresh)
            global _chunks_processed
            _chunks_processed += 1
            if VACUUM_INTERVAL > 0 and _chunks_processed >= VACUUM_INTERVAL:
                print(f"Running VACUUM ANALYZE (after {_chunks_processed} chunks)...")
                vacuum_start = time.time()
                with db.get_db_cursor() as vac_cur:
                    vac_cur.execute("VACUUM ANALYZE utxos")
                    vac_cur.execute("VACUUM ANALYZE block_log")
                vacuum_time = time.time() - vacuum_start
                print(f"VACUUM ANALYZE completed in {vacuum_time:.3f}s")
                _chunks_processed = 0
            elif VACUUM_INTERVAL == 0:
                # Every chunk (only for testing - expensive!)
                print("Running VACUUM ANALYZE after chunk...")
                with db.get_db_cursor() as vac_cur:
                    vac_cur.execute("VACUUM ANALYZE utxos")
        except Exception as e:
            print(f"Error writing chunk {chunk_start}-{chunk_end} to PostgreSQL: {e}")
        print(f"Saved and flushed chunk {chunk_start}–{chunk_end}")

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
    
    try:
        tip = rpc_call("getblockcount")
        start = max(0, last + 1)
        
        if start <= tip:
            print(f"Catching up: processing blocks {start} → {tip}")
            process_range(start, tip)
        
        # Continuous polling loop
        print("Catch-up complete. Entering continuous sync mode...")
        
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