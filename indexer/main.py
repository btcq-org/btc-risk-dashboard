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

import psycopg2
from psycopg2.extras import execute_batch, execute_values
from . import db

import csv
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

POLL_INTERVAL = 1.0                       # seconds between checking for new blocks
RECONNECT_DELAY = 5.0                     # seconds to wait after connection error

# Tuning knobs (can be overridden by env)
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', '50'))                    # blocks per catch-up chunk
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
            
            # Insert block log entry
            bulk_insert_block_log_copy(cur, [(height, blockhash, block_ts)])
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
        """Process a single block's data and extract VOUTs (UTXOs)."""
        block_time = block.get('time', 0)
        block_ts = int(block_time * 1_000_000_000)

        vout_rows = []

        for tx in block.get("tx", []):
            txid = tx.get("txid", "")
            if shutdown_requested:
                print(f"Shutdown requested while preparing chunk data for block {height}")
                return {
                    'vout_rows': [],
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

        return {
            'vout_rows': vout_rows,
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

        out_lists = {'vout_rows': [], 'scanned': []}
        chunk_heights = list(range(chunk_start, chunk_end + 1))

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
            if block_data['block_info']:
                out_lists['scanned'].append(block_data['block_info'])

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
                    # Use COPY FROM for fastest bulk inserts
                    bulk_insert_utxos_copy(cur, out_lists['vout_rows'])
                    vout_time = time.time() - vout_start
                    if vout_time > 0:
                        rows_per_sec = vout_count / vout_time
                        print(f"[Chunk {chunk_start}-{chunk_end}] Inserted {vout_count} UTXOs in {vout_time:.3f}s ({rows_per_sec:.0f} rows/sec)")
                        if rows_per_sec < 5000:
                            print(f"  Warning: Insert speed is low. Consider setting DROP_ADDRESS_INDEX=true or checking database configuration.")

                if out_lists['scanned']:
                    block_log_start = time.time()
                    block_count = len(out_lists['scanned'])
                    block_vals = [(b['block_height'], b['block_hash'], b['block_timestamp']) for b in out_lists['scanned']]
                    print(f"[Chunk {chunk_start}-{chunk_end}] Inserting {block_count} block_log entries using COPY...")
                    bulk_insert_block_log_copy(cur, block_vals)
                    block_log_time = time.time() - block_log_start
                    print(f"[Chunk {chunk_start}-{chunk_end}] Inserted {block_count} block_log entries in {block_log_time:.3f}s")

            db_time = time.time() - db_start
            print(f"[Chunk {chunk_start}-{chunk_end}] Database write completed in {db_time:.3f}s")
        except Exception as e:
            print(f"Error writing chunk {chunk_start}-{chunk_end} to PostgreSQL: {e}")
        print(f"Saved and flushed chunk {chunk_start}–{chunk_end}")


def get_script_type(script_hex: str, address: str) -> str:
    """
    Classifies scriptPubKey hex into one of: P2PK, P2PKH, P2SH, P2MS, P2WPKH, P2WSH, P2TR.
    
    Args:
        script_hex: Hex string of scriptPubKey.
    
    Returns:
        str: The type (e.g., 'P2PKH') or 'unknown'.
    """
    script = bytes.fromhex(script_hex)
    
    # Helper: Simple disassembler for pattern matching
    def disassemble():
        opcodes = {
            0x76: 'OP_DUP', 0xa9: 'OP_HASH160', 0x88: 'OP_EQUALVERIFY', 0xac: 'OP_CHECKSIG',
            0x87: 'OP_EQUAL', 0x51: 'OP_1', 0x52: 'OP_2', 0x53: 'OP_3', 0xae: 'OP_CHECKMULTISIG',
            0x00: 'OP_0', 0x01: 'OP_1'  # OP_1 for P2TR
        }
        disasm = []
        i = 0
        while i < len(script):
            op = script[i]
            i += 1
            if 1 <= op <= 75:  # OP_PUSHBYTES_N + data
                data_len = op
                if i + data_len > len(script):
                    return ['ERROR']
                data_hex = script[i:i + data_len].hex()
                disasm.append(f'PUSH_{data_len}:{data_hex}')
                i += data_len
            else:
                name = opcodes.get(op, f'OP_{op:02x}')
                disasm.append(name)
        return disasm
    
    disasm = disassemble()
    
    # P2PKH: OP_DUP OP_HASH160 PUSH_20 OP_EQUALVERIFY OP_CHECKSIG
    if (len(disasm) == 5 and
        disasm[0] == 'OP_DUP' and
        disasm[1] == 'OP_HASH160' and
        disasm[2].startswith('PUSH_20:') and
        disasm[3] == 'OP_EQUALVERIFY' and
        disasm[4] == 'OP_CHECKSIG'):
        return 'P2PKH'
    
    # P2SH: OP_HASH160 PUSH_20 OP_EQUAL
    if (len(disasm) == 3 and
        disasm[0] == 'OP_HASH160' and
        disasm[1].startswith('PUSH_20:') and
        disasm[2] == 'OP_EQUAL'):
        return 'P2SH'
    
    # P2WPKH: OP_0 PUSH_20
    if (len(disasm) == 2 and
        disasm[0] == 'OP_0' and
        disasm[1].startswith('PUSH_20:')):
        return 'P2WPKH'
    
    # P2WSH: OP_0 PUSH_32
    if (len(disasm) == 2 and
        disasm[0] == 'OP_0' and
        disasm[1].startswith('PUSH_32:')):
        return 'P2WSH'
    
    # P2TR: OP_1 PUSH_32
    if (len(disasm) == 2 and
        disasm[0] == 'OP_1' and
        disasm[1].startswith('PUSH_32:')):
        return 'P2TR'
    
    # P2PK: PUSH_33/65 OP_CHECKSIG
    if (len(disasm) == 2 and disasm[1] == 'OP_CHECKSIG' and
        (disasm[0].startswith('PUSH_33:') or disasm[0].startswith('PUSH_65:'))):
        return 'P2PK'
    
    # P2MS: OP_N + >=2 PUSH_33/65 + OP_M + OP_CHECKMULTISIG (heuristic)
    if (len(disasm) >= 4 and disasm[-1] == 'OP_CHECKMULTISIG' and
        disasm[-2] in ['OP_1', 'OP_2', 'OP_3'] and disasm[0] in ['OP_1', 'OP_2', 'OP_3']):
        pubkey_pushes = [d for d in disasm[1:-2] if d.startswith('PUSH_33:') or d.startswith('PUSH_65:')]
        if len(pubkey_pushes) >= 2:
            return 'P2MS'
    
    if address.startswith('bc1p'):
        return 'Bech32'
    
    return 'unknown'

from typing import Generator, Dict, Any, List
from collections import defaultdict

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
                total_utxo += 1

                if address == '':
                    continue
                
                row_tuple = (
                    txid, vout, address, amount, script_pub_key,
                    script_type, block_height, 0
                )
                
                batch.append(row_tuple)

                if len(batch) >= batch_size:
                    print(f"Inserting batch of {len(batch)} rows...", file=sys.stderr)
                    start = time.time()
                    with db.get_db_cursor() as cur:
                            execute_values(cur, insert_sql, batch, page_size=1000)
                            cur.connection.commit()
                    print(f"Inserted batch: {total_utxo} total rows", file=sys.stderr)
                    batch = []
            
            except (ValueError, IndexError) as e:
                print(f"Warning: Skipping line {line_num} due to parse error: {e}", file=sys.stderr)
                continue

    stats_upsert_sql = """
        INSERT INTO stats (key, value) VALUES %s
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """

    stats_batch = [('total_utxo', total_utxo)]
    for script_type, count in type_counts.items():
        key = f"{script_type}_count"
        stats_batch.append((key, count))

    start = time.time()
    print(f"Inserting final batch of {len(batch)} rows...", file=sys.stderr)
    with db.get_db_cursor() as cur:
        if len(batch) > 0:
            execute_values(cur, insert_sql, batch, page_size=1000)
        execute_values(cur, stats_upsert_sql, stats_batch)
        cur.execute("ALTER TABLE utxos ADD PRIMARY KEY (txid, vout);")
        cur.execute("CREATE INDEX IF NOT EXISTS utxos_address_idx ON utxos (address);")
        cur.execute("INSERT INTO block_log (block_height, block_hash, block_timestamp) VALUES (0, '0'*64, 0) ON CONFLICT DO NOTHING;")
        cur.connection.commit()
        print(f"Inserted stats and created indexes in {time.time() - start}", file=sys.stderr)
    
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
            read_utxo()

        if start <= tip:
            print(f"Catching up: processing blocks {start} → {tip}")
            process_range(start, tip)
        
        # Continuous polling loop
        print("Catch-up complete. Entering continuous sync mode...")

        # Recreate address index if missing
        with db.get_db_cursor() as cur:
            cur.execute("CREATE INDEX IF NOT EXISTS utxos_address_idx ON utxos (address);")
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