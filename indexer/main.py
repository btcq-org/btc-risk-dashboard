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
from psycopg2.extras import execute_batch
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
    """Extract address from vout entry if possible"""
    spk = v.get("scriptPubKey", {})
    stype = spk.get("type", {})
    script_hex = spk.get("hex", "")
    
    if stype == "pubkey" and script_hex:
        try:
            script_bytes = bytes.fromhex(script_hex)
            address = network.address.for_script(script_bytes)
            return address
        except Exception as e:
            return ""
    
    return spk.get("address", "")

def pubkey_to_address(pubkey_hex):
    """Convert pubkey hex to Bitcoin address."""
    try:
        pubkey_bytes = x(pubkey_hex)
        pubkey = CPubKey(pubkey_bytes)
        
        if len(pubkey_bytes) == 33:
            try:
                return str(P2WPKHBitcoinAddress.from_pubkey(pubkey))
            except Exception:
                return str(P2PKHBitcoinAddress.from_pubkey(pubkey))
        else:
            return str(P2PKHBitcoinAddress.from_pubkey(pubkey))
    except Exception as e:
        print(f"Error converting pubkey to address: {e}")
        return ""

def is_pubkey_hex(s):
    """Roughly detect compressed/uncompressed pubkeys."""
    return ((s.startswith("02") or s.startswith("03")) and len(s) == 66) or \
           (s.startswith("04") and len(s) == 130)

def address_from_vin(vin):
    """Extract address from vin entry if possible"""
    scriptsig_asm = vin.get("scriptSig", {}).get("asm", "")
    scriptsig_type = vin.get("scriptSig", {}).get("type", "")

    prevout = vin.get("prevout", {})
    prev_addr = prevout.get("scriptPubKey", {}).get("address", "")
    
    if prev_addr:
        return prev_addr
    
    spk = prevout.get("scriptPubKey", {})
    script_hex = spk.get("hex", "")

    if script_hex:
        script_bytes = bytes.fromhex(script_hex)
        return network.address.for_script(script_bytes)

    if scriptsig_asm:
        for item in scriptsig_asm.split():
            if is_pubkey_hex(item):
                address = pubkey_to_address(item)
                return address

    return spk.get("address", "")

# ========================
# TX PARSING
# ========================
def parse_single_tx(tx):
    """Parse a single TX with bitcoin library"""
    result = {
        'txid': tx.get('txid', ''),
        'vin_addresses': [],
        'vout_addresses': [],
        'balance_changes': {}
    }

    # Process inputs (vin) - addresses spending BTC
    for vin in tx.get("vin", []):
        if "txid" not in vin:
            if "coinbase" in vin:
                result['vin_addresses'].append({
                    'address': 'coinbase',
                    'value_btc': 0,
                    'value_sat': 0
                })
            continue
        
        # Get address from prevout
        prevout = vin.get("prevout", {})
        spk = prevout.get("scriptPubKey", {})
        script_hex = spk.get("hex", "")
        value_btc = float(prevout.get("value", 0))
        value_sat = int(value_btc * 100_000_000)
        address = address_from_vin(vin)
        
        if address:
            result['vin_addresses'].append({
                'address': address,
                'value_btc': value_btc,
                'value_sat': value_sat
            })
            # Track negative balance change (spending BTC)
            result['balance_changes'][address] = result['balance_changes'].get(address, 0) - value_sat

        if not address:
            result['vin_addresses'].append({
                'address': 'unknown',
                'value_btc': value_btc,
                'value_sat': value_sat,
                'vin_hash': vin.get("txid", ""),
                'vin_vout': vin.get("vout", 0),
                'script_hex': vin.get("scriptSig", {}).get("hex", ""),
            })
    
    # Process outputs (vout) - addresses receiving BTC
    for vout in tx.get("vout", []):
        spk = vout.get("scriptPubKey", {})
        script_hex = spk.get("hex", "")
        value_btc = float(vout.get("value", 0))
        value_sat = int(value_btc * 100_000_000)
        address = address_from_vout(vout)
        atype = detect_script_type(spk)
        
        if address:
            result['vout_addresses'].append({
                'hash': script_hex,
                'address': address,
                'value_btc': value_btc,
                'value_sat': value_sat,
                'type': atype
            })
            # Track positive balance change (receiving BTC)
            result['balance_changes'][address] = result['balance_changes'].get(address, 0) + value_sat

    return result


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

def upsert_vouts(cur, txid, vouts, block_height, block_time):
    rows = []
    for idx, v in enumerate(vouts):
        spk = v.get("scriptPubKey", {})
        address = address_from_vout(v)
        value_btc = float(v.get("value", 0))
        value_sat = int(value_btc * 100_000_000)
        rows.append((
            txid,
            idx,
            address or None,
            value_sat,
            spk.get("hex", ""),
            detect_script_type(spk),
            block_height,
            int(block_time) * 1_000_000_000
        ))
    if not rows:
        return
    execute_batch(cur,
        """
        INSERT INTO utxos (txid, vout, address, value_sat, script_pub_key_hex, script_pub_type, created_block, created_block_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (txid, vout) DO UPDATE SET
            address = EXCLUDED.address,
            value_sat = EXCLUDED.value_sat,
            script_pub_key_hex = EXCLUDED.script_pub_key_hex,
            script_pub_type = EXCLUDED.script_pub_type,
            created_block = EXCLUDED.created_block,
            created_block_timestamp = EXCLUDED.created_block_timestamp
        """,
        rows,
        page_size=200
    )

def mark_spent_inputs(cur, txid, vins, block_height, block_time):
    rows = []
    for vin_idx, vin in enumerate(vins):
        if "txid" not in vin:
            continue  # coinbase
        prev_txid = vin.get("txid")
        prev_vout = int(vin.get("vout", 0))
        rows.append((
            True,
            txid,
            vin_idx,
            block_height,
            int(block_time) * 1_000_000_000,
            prev_txid,
            prev_vout
        ))
    if not rows:
        return
    execute_batch(cur,
        """
        UPDATE utxos u
        SET spent = %s,
            spent_by_txid = %s,
            spent_vin = %s,
            spent_block = %s,
            spent_block_timestamp = %s
        WHERE u.txid = %s AND u.vout = %s AND (u.spent IS DISTINCT FROM TRUE)
        """,
        rows,
        page_size=200
    )

# ========================
def schema_init():
    import os
    schema_path = os.path.join(os.path.dirname(__file__), "db", "schema.sql")
    db.init_pool()
    try:
        with db.get_db_cursor() as cur:
            try:
                with open(schema_path, "r") as sf:
                    schema_sql = sf.read()
                cur.execute(schema_sql)
            except FileNotFoundError:
                raise
    finally:
        db.shutdown_pool()

# ========================
# Range processing (mirrors indexer.py style)
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
        db.init_pool()
        chunk_end = min(chunk_start + chunk_size - 1, end_height)
        print(f"Processing chunk {chunk_start} → {chunk_end}")

        out_lists = { 'vout_rows': [], 'vin_rows': [], 'scanned': [] }
        to_process = list(range(chunk_start, chunk_end + 1))
        failed_blocks = []
        retries = 0
        while to_process and retries < MAX_RETRIES:
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
        if to_process:
            print(f"Failed to process blocks after {MAX_RETRIES} retries in chunk {chunk_start}–{chunk_end}: {to_process}")

        try:
            with db.get_db_cursor() as cur:
                if out_lists['vout_rows']:
                    execute_batch(cur,
                        """
                        INSERT INTO utxos (txid, vout, address, value_sat, script_pub_key_hex, script_pub_type, created_block, created_block_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (txid, vout) DO UPDATE SET
                            address = EXCLUDED.address,
                            value_sat = EXCLUDED.value_sat,
                            script_pub_key_hex = EXCLUDED.script_pub_key_hex,
                            script_pub_type = EXCLUDED.script_pub_type,
                            created_block = EXCLUDED.created_block,
                            created_block_timestamp = EXCLUDED.created_block_timestamp
                        """,
                        out_lists['vout_rows'], page_size=500
                    )

                if out_lists['vin_rows']:
                    execute_batch(cur,
                        """
                        UPDATE utxos u
                        SET spent = %s,
                            spent_by_txid = %s,
                            spent_vin = %s,
                            spent_block = %s,
                            spent_block_timestamp = %s
                        WHERE u.txid = %s AND u.vout = %s AND (u.spent IS DISTINCT FROM TRUE)
                        """,
                        out_lists['vin_rows'], page_size=500
                    )

                if out_lists['scanned']:
                    block_vals = [(b['block_height'], b['block_hash'], b['block_timestamp']) for b in out_lists['scanned']]
                    db.execute_values(cur,
                        """
                        INSERT INTO block_log (block_height, block_hash, block_timestamp) VALUES %s
                        """,
                        block_vals
                    )
        except Exception as e:
            print(f"Error writing chunk {chunk_start}-{chunk_end} to PostgreSQL: {e}")
        db.shutdown_pool()
        print(f"Saved and flushed chunk {chunk_start}–{chunk_end}")

def main():
    schema_init()
    last = get_last_processed_height()
    tip = rpc_call("getblockcount")
    start = max(0, last + 1)
    if start <= tip:
        process_range(start, tip)

if __name__ == "__main__":
    main()