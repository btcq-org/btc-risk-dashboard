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

import requests, json
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
RPC_USER = os.getenv('RPC_USER', 'admin')
RPC_PASSWORD = os.getenv('RPC_PASSWORD', 'pass')
RPC_HOST = os.getenv('RPC_HOST', '127.0.0.1')
RPC_PORT = int(os.getenv('RPC_PORT', '18332'))
RPC_URL = f"http://{RPC_HOST}:{RPC_PORT}/"

SelectParams(NETWORK)

THREADS = 8                               # number of parallel workers
INITIAL_BLOCKS = 100                      # number of historical blocks to scan on startup
POLL_INTERVAL = 1.0                       # seconds between checking for new blocks
RECONNECT_DELAY = 5.0                     # seconds to wait after connection error

# Tuning knobs (can be overridden by env)
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', '200'))                    # blocks per catch-up chunk
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '5'))                    # max retries for failed RPC
REFRESH_INTERVAL = int(os.getenv('REFRESH_INTERVAL', '10'))         # MV refresh cadence
VACUUM_INTERVAL = int(os.getenv('VACUUM_INTERVAL', '100'))          # VACUUM cadence in chunks
MAX_RPC_BATCH = int(os.getenv('MAX_RPC_BATCH', '100'))              # cap per JSON-RPC batch
DB_PAGE_ROWS = int(os.getenv('DB_PAGE_ROWS', '2000'))               # rows per DB insert/update page

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
                             timeout=30)
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

def rpc_batch_call(rpc_requests):
    """
    Send multiple RPC requests in a single batch call using Bitcoin Core's native batching.
    Bitcoin Core supports JSON-RPC batch requests by sending an array of request objects.
    rpc_requests: list of tuples (method, params, height)
    Returns: dict mapping height to result (or {"error": ...} for errors)
    """
    if not rpc_requests:
        return {}
    
    payload = []
    id_to_height = {}
    for idx, (method, params, height) in enumerate(rpc_requests):
        request_id = f"batch-{height}-{idx}"
        id_to_height[request_id] = height
        # Bitcoin Core accepts batch requests with jsonrpc 1.0 or 2.0
        payload.append({
            "jsonrpc": "1.0",
            "id": request_id,
            "method": method,
            "params": params or []
        })
    
    try:
        resp = requests.post(RPC_URL, auth=(RPC_USER, RPC_PASSWORD),
                             headers={"content-type": "text/plain"},
                             data=json.dumps(payload),
                             timeout=60)
        resp.raise_for_status()
        results = resp.json()
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Cannot connect to Bitcoin RPC at {RPC_URL}. "
                            f"Is Bitcoin Core running? Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"RPC batch request to {RPC_URL} timed out. Error: {e}")
    
    # Handle both single response (if only one request) and array response
    if not isinstance(results, list):
        results = [results]
    
    # Map results back to heights, handling errors
    result_map = {}
    for r in results:
        request_id = r.get("id", "")
        if r.get("error"):
            height = id_to_height.get(request_id, "unknown")
            result_map[height] = {"error": r["error"]}
        else:
            height = id_to_height.get(request_id, "unknown")
            result_map[height] = r.get("result")
    
    return result_map

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
        block = rpc_call("getblock", [blockhash, 2])
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
        if shutdown_requested:
            print(f"Shutdown requested while scanning transactions for block {height}")
            return False
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

            if shutdown_requested:
                print(f"Shutdown requested during processing of block {height}, aborting before database write")
                return False
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
            if shutdown_requested:
                print(f"Shutdown requested before writing block {height} to database")
                return False
            if vout_rows:
                vout_start = time.time()
                vout_count = len(vout_rows)
                print(f"[Block {height}] Inserting {vout_count} UTXOs...")
                # Use execute_values for faster bulk inserts (uses COPY internally)
                # Increased page_size for better performance with large datasets
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
                    vout_rows, page_size=10000, template=None
                )
                vout_time = time.time() - vout_start
                print(f"[Block {height}] Inserted {vout_count} UTXOs in {vout_time:.3f}s")
            
            if vin_rows:
                vin_start = time.time()
                vin_count = len(vin_rows)
                print(f"[Block {height}] Updating {vin_count} spent UTXOs...")
                # Use execute_batch for UPDATEs (execute_values doesn't support UPDATE well)
                # Increased page_size for better performance with large datasets
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
                    vin_rows, page_size=10000
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
            
            # Refresh materialized views periodically
            # REFRESH_INTERVAL > 0: refresh every N blocks
            # REFRESH_INTERVAL = 0: refresh every block (expensive)
            # REFRESH_INTERVAL < 0: never refresh automatically
            _blocks_since_refresh += 1
            if REFRESH_INTERVAL > 0:
                if _blocks_since_refresh >= REFRESH_INTERVAL:
                    cur.execute("SELECT refresh_all_materialized_views()")
                    _blocks_since_refresh = 0
            elif REFRESH_INTERVAL == 0:
                cur.execute("SELECT refresh_all_materialized_views()")
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

    def process_block_data(block, height, blockhash):
        """Process a single block's data and return rows."""
        block_time = block.get('time', 0)
        block_ts = int(block_time * 1_000_000_000)

        vout_rows = []
        vin_rows = []

        for tx in block.get("tx", []):
            txid = tx.get("txid", "")
            if shutdown_requested:
                print(f"Shutdown requested while preparing chunk data for block {height}")
                return {
                    'vout_rows': [],
                    'vin_rows': [],
                    'block_info': None
                }
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

        return {
            'vout_rows': vout_rows,
            'vin_rows': vin_rows,
            'block_info': {
                "block_height": height,
                "block_hash": blockhash,
                "block_timestamp": block_ts
            }
        }

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
            
            # Batch fetch all block hashes
            hash_batch_requests = [("getblockhash", [height], height) for height in to_process]
            try:
                hash_results = rpc_batch_call(hash_batch_requests)
            except (ConnectionError, TimeoutError) as e:
                print(f"Error in batch getblockhash call: {e}")
                print(f"Waiting {RECONNECT_DELAY} seconds before retry...")
                time.sleep(RECONNECT_DELAY)
                failed_blocks = to_process[:]
                to_process = failed_blocks[:]
                retries += 1
                continue
            except Exception as e:
                print(f"Error in batch getblockhash call: {e}")
                failed_blocks = to_process[:]
                to_process = failed_blocks[:]
                retries += 1
                continue
            
            # Build block fetch requests for successful hashes
            block_batch_requests = []
            height_to_hash = {}
            # Limit batch of blocks to avoid overloading bitcoind memory
            limited_heights = to_process[:MAX_RPC_BATCH]
            remaining_heights = to_process[MAX_RPC_BATCH:]
            for height in limited_heights:
                if height in hash_results:
                    result = hash_results[height]
                    if isinstance(result, dict) and "error" in result:
                        failed_blocks.append(height)
                        continue
                    blockhash = result
                    if blockhash:
                        height_to_hash[height] = blockhash
                        block_batch_requests.append(("getblock", [blockhash, 3], height))
                else:
                    failed_blocks.append(height)
            
            # Batch fetch all blocks
            if block_batch_requests:
                try:
                    block_results = rpc_batch_call(block_batch_requests)
                except (ConnectionError, TimeoutError) as e:
                    print(f"Error in batch getblock call: {e}")
                    print(f"Waiting {RECONNECT_DELAY} seconds before retry...")
                    time.sleep(RECONNECT_DELAY)
                    failed_blocks.extend([h for h in to_process if h not in failed_blocks])
                    to_process = failed_blocks[:]
                    retries += 1
                    continue
                except Exception as e:
                    print(f"Error in batch getblock call: {e}")
                    failed_blocks.extend([h for h in to_process if h not in failed_blocks])
                    to_process = failed_blocks[:]
                    retries += 1
                    continue
                
                # Process all fetched blocks
                for height in limited_heights:
                    if height in failed_blocks:
                        continue
                    if height not in block_results:
                        failed_blocks.append(height)
                        continue
                    
                    result = block_results[height]
                    if isinstance(result, dict) and "error" in result:
                        failed_blocks.append(height)
                        continue
                    
                    blockhash = height_to_hash.get(height, "")
                    block = result
                    if not block:
                        failed_blocks.append(height)
                        continue
                    
                    try:
                        block_data = process_block_data(block, height, blockhash)
                        if shutdown_requested:
                            print(f"Shutdown requested while processing chunk {chunk_start}-{chunk_end}, stopping block accumulation")
                            failed_blocks.clear()
                            to_process.clear()
                            break
                        out_lists['vout_rows'].extend(block_data['vout_rows'])
                        out_lists['vin_rows'].extend(block_data['vin_rows'])
                        if block_data['block_info']:
                            out_lists['scanned'].append(block_data['block_info'])
                    except Exception as e:
                        print(f"Error processing block {height}: {e}")
                        failed_blocks.append(height)
            else:
                # All blocks failed at hash stage
                pass
            
            if failed_blocks:
                print(f"Retrying {len(failed_blocks)} failed blocks in chunk {chunk_start}–{chunk_end} (attempt {retries+2}/{MAX_RETRIES})...")
            # Re-queue failed first, then remaining heights of this chunk
            to_process = failed_blocks[:] + remaining_heights[:]
            retries += 1
            if shutdown_requested:
                break
        
        fetch_time = time.time() - fetch_start
        if to_process:
            print(f"Failed to process blocks after {MAX_RETRIES} retries in chunk {chunk_start}–{chunk_end}: {to_process}")
        else:
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
                # Speed up bulk load locally
                cur.execute("SET LOCAL synchronous_commit = OFF")
                cur.execute("SET LOCAL statement_timeout = 0")
                if out_lists['vout_rows']:
                    vout_start = time.time()
                    vout_count = len(out_lists['vout_rows'])
                    print(f"[Chunk {chunk_start}-{chunk_end}] Staging {vout_count} UTXOs into temp table...")
                    # Create staging table
                    cur.execute("""
                        CREATE TEMP TABLE temp_utxos (
                            txid text,
                            vout integer,
                            address text,
                            value_sat bigint,
                            script_pub_key_hex text,
                            script_pub_type text,
                            created_block integer,
                            created_block_timestamp bigint
                        ) ON COMMIT DROP
                    """)
                    # Load into staging in pages
                    for i in range(0, vout_count, DB_PAGE_ROWS):
                        if shutdown_requested:
                            print(f"Shutdown requested during staging UTXO insert")
                            break
                        slice_rows = out_lists['vout_rows'][i:i+DB_PAGE_ROWS]
                        execute_values(cur, """
                            INSERT INTO temp_utxos (txid, vout, address, value_sat, script_pub_key_hex, script_pub_type, created_block, created_block_timestamp)
                            VALUES %s
                        """, slice_rows, page_size=min(2000, DB_PAGE_ROWS), template=None)
                    if not shutdown_requested:
                        # Single upsert from staging (ordered for index locality)
                        print(f"[Chunk {chunk_start}-{chunk_end}] Upserting staged UTXOs into main table...")
                        cur.execute("""
                            INSERT INTO utxos (txid, vout, address, value_sat, script_pub_key_hex, script_pub_type, created_block, created_block_timestamp)
                            SELECT txid, vout, address, value_sat, script_pub_key_hex, script_pub_type, created_block, created_block_timestamp
                            FROM temp_utxos
                            ORDER BY txid, vout
                            ON CONFLICT (txid, vout) DO UPDATE SET
                                address = EXCLUDED.address,
                                value_sat = EXCLUDED.value_sat,
                                script_pub_key_hex = EXCLUDED.script_pub_key_hex,
                                script_pub_type = EXCLUDED.script_pub_type,
                                created_block = EXCLUDED.created_block,
                                created_block_timestamp = EXCLUDED.created_block_timestamp
                        """)
                    vout_time = time.time() - vout_start
                    print(f"[Chunk {chunk_start}-{chunk_end}] Inserted {vout_count} UTXOs in {vout_time:.3f}s")
                    if shutdown_requested:
                        print(f"Shutdown requested after UTXO insert for chunk {chunk_start}-{chunk_end}, aborting remaining work")
                        break

                if out_lists['vin_rows']:
                    vin_start = time.time()
                    vin_count = len(out_lists['vin_rows'])
                    print(f"[Chunk {chunk_start}-{chunk_end}] Staging {vin_count} spend updates into temp table...")
                    # Create staging table for updates
                    cur.execute("""
                        CREATE TEMP TABLE temp_spends (
                            spent boolean,
                            spent_by_txid text,
                            spent_vin integer,
                            spent_block integer,
                            spent_block_timestamp bigint,
                            prev_txid text,
                            prev_vout integer
                        ) ON COMMIT DROP
                    """)
                    for i in range(0, vin_count, DB_PAGE_ROWS):
                        if shutdown_requested:
                            print(f"Shutdown requested during staging spent updates")
                            break
                        slice_rows = out_lists['vin_rows'][i:i+DB_PAGE_ROWS]
                        execute_values(cur, """
                            INSERT INTO temp_spends (spent, spent_by_txid, spent_vin, spent_block, spent_block_timestamp, prev_txid, prev_vout)
                            VALUES %s
                        """, slice_rows, page_size=min(2000, DB_PAGE_ROWS), template=None)
                    if not shutdown_requested:
                        print(f"[Chunk {chunk_start}-{chunk_end}] Applying spend updates via join...")
                        cur.execute("""
                            UPDATE utxos u
                            SET spent = s.spent,
                                spent_by_txid = s.spent_by_txid,
                                spent_vin = s.spent_vin,
                                spent_block = s.spent_block,
                                spent_block_timestamp = s.spent_block_timestamp
                            FROM temp_spends s
                            WHERE u.txid = s.prev_txid
                              AND u.vout = s.prev_vout
                              AND (u.spent IS DISTINCT FROM TRUE)
                        """)
                    vin_time = time.time() - vin_start
                    print(f"[Chunk {chunk_start}-{chunk_end}] Updated {vin_count} spent UTXOs in {vin_time:.3f}s")
                    if shutdown_requested:
                        print(f"Shutdown requested after spent UTXO update for chunk {chunk_start}-{chunk_end}, aborting remaining work")
                        break

                if out_lists['scanned']:
                    block_log_start = time.time()
                    block_count = len(out_lists['scanned'])
                    block_vals = [(b['block_height'], b['block_hash'], b['block_timestamp']) for b in out_lists['scanned']]
                    print(f"[Chunk {chunk_start}-{chunk_end}] Inserting {block_count} block_log entries in pages of {DB_PAGE_ROWS}...")
                    for i in range(0, block_count, DB_PAGE_ROWS):
                        if shutdown_requested:
                            print(f"Shutdown requested during block_log paging insert")
                            break
                        slice_rows = block_vals[i:i+DB_PAGE_ROWS]
                        execute_values(cur,
                            """
                            INSERT INTO block_log (block_height, block_hash, block_timestamp) VALUES %s
                            """,
                            slice_rows, page_size=min(1000, DB_PAGE_ROWS), template=None
                        )
                    if shutdown_requested:
                        print(f"Shutdown requested after block_log insert for chunk {chunk_start}-{chunk_end}")
                    block_log_time = time.time() - block_log_start
                    print(f"[Chunk {chunk_start}-{chunk_end}] Inserted {block_count} block_log entries in {block_log_time:.3f}s")
                
                # Refresh materialized views after each chunk during catch-up
                # (chunks are large so refresh overhead is acceptable)
                if REFRESH_INTERVAL >= 0:
                    if shutdown_requested:
                        print(f"Skipping materialized view refresh due to shutdown request")
                    else:
                        cur.execute("SELECT refresh_all_materialized_views()")
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
    print(f"RPC Configuration: {RPC_HOST}:{RPC_PORT} (user: {RPC_USER})")
    
    # Test RPC connection before proceeding
    print("\nTesting Bitcoin RPC connection...")
    if not test_rpc_connection():
        print("\nFatal: Cannot connect to Bitcoin RPC. Please fix the connection and try again.")
        db.shutdown_pool()
        sys.exit(1)
    print()
    
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