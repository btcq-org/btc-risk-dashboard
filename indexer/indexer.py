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
from . import db

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

def is_pubkey_hex(s):
    return ((s.startswith("02") or s.startswith("03")) and len(s) == 66) or \
           (s.startswith("04") and len(s) == 130)

def get_last_processed_height():
    """Get the highest block we've processed from block_log."""
    try:
        with db.get_db_cursor() as cur:
            cur.execute("SELECT MAX(block_height) FROM block_log")
            row = cur.fetchone()
            return row[0] if row and row[0] is not None else 0
    except Exception as e:
        print(f"Error getting last processed height: {e}")
        return 0

def process_range(start_height, end_height, chunk_size=CHUNK_SIZE):
    """Process a range of blocks and store results in chunked bulks.

    The function will process blocks in chunks of `chunk_size` and write each
    chunk's results to the database before proceeding to the next chunk.
    """
    if start_height > end_height:
        return

    print(f"Processing blocks {start_height} → {end_height} in chunks of {chunk_size}")

    lock = Lock()

    def process_block(height, out_lists, failed_blocks):
        blockhash = ""
        try:
            blockhash = rpc_call("getblockhash", [height])
            block = rpc_call("getblock", [blockhash, 2])
        except Exception as e:
            print(f"Error reading block {height}: {e}")
            failed_blocks.append(height)
            return
        # Compute block timestamp in nanoseconds for hypertable
        block_time = block.get('time', 0)
        block_ts = int(block_time * 1_000_000_000)
        local_addr_updates = {}
        
        for tx in block.get("tx", []):
            addr_meta = analyze_tx(tx, height, block_ts)
            # Merge address metadata updates
            for addr, meta in addr_meta.items():
                if addr not in local_addr_updates:
                    local_addr_updates[addr] = meta.copy()
                else:
                    existing = local_addr_updates[addr]
                    existing["first_seen_block"] = min(existing["first_seen_block"], meta["first_seen_block"])
                    existing["last_seen_block"] = max(existing["last_seen_block"], meta["last_seen_block"])
                    existing["tx_count"] += meta["tx_count"]
                    existing["balance_sat"] += meta["balance_sat"]
                    if meta["script_pub_type"]:
                        existing["script_pub_type"] = meta["script_pub_type"]
                    if meta["script_sig_type"]:
                        existing["script_sig_type"] = meta["script_sig_type"]

        with lock:
            # Merge block's address updates into global list
            out_lists['address_updates'] = {}
            for addr, meta in local_addr_updates.items():
                if addr not in out_lists['address_updates']:
                    out_lists['address_updates'][addr] = meta.copy()
                else:
                    existing = out_lists['address_updates'][addr]
                    existing["first_seen_block"] = min(existing["first_seen_block"], meta["first_seen_block"])
                    existing["last_seen_block"] = max(existing["last_seen_block"], meta["last_seen_block"])
                    existing["tx_count"] += meta["tx_count"]
                    existing["balance_sat"] += meta["balance_sat"]
                    if meta["script_pub_type"]:
                        existing["script_pub_type"] = meta["script_pub_type"]
                    if meta["script_sig_type"]:
                        existing["script_sig_type"] = meta["script_sig_type"]
            
            out_lists['scanned'].append({
                "block_height": height,
                "block_hash": blockhash,
                "block_timestamp": block_ts
            })

    # iterate chunks
    for chunk_start in range(start_height, end_height + 1, chunk_size):
        # Ensure pool is initialized for each chunk
        db.init_pool()
        chunk_end = min(chunk_start + chunk_size - 1, end_height)
        print(f"Processing chunk {chunk_start} → {chunk_end}")


        out_lists = {'exposed': [], 'revealed': [], 'scanned': []}
        to_process = list(range(chunk_start, chunk_end + 1))
        failed_blocks = []
        retries = 0
        while to_process and retries < MAX_RETRIES:
            failed_blocks.clear()
            with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
                # submit tasks
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
            print(f"❌ Failed to process blocks after {MAX_RETRIES} retries in chunk {chunk_start}–{chunk_end}: {to_process}")

        # Gather per-chunk data
        address_updates = out_lists.get('address_updates', {})
        block_logs = out_lists.get('scanned', [])

        # Write this chunk's results to DB
        try:
            with db.get_db_cursor() as cur:
                # Batch insert block log
                if block_logs:
                    block_vals = [(b['block_height'], b['block_hash'], b.get('block_timestamp', 0))
                                 for b in block_logs]
                    db.execute_values(cur,
                        """
                        INSERT INTO block_log (block_height, block_hash, block_timestamp) VALUES %s
                        ON CONFLICT (block_height, block_timestamp) DO UPDATE SET
                            block_hash = EXCLUDED.block_hash,
                            scanned_at = DEFAULT
                        """,
                        block_vals)

                # Batch insert/update address metadata
                if address_updates:
                    addr_vals = [(
                        addr,  # address
                        meta.get('first_seen_block'),  # first_seen_block
                        meta.get('last_seen_block'),   # last_seen_block
                        meta.get('tx_count', 1),       # tx_count
                        meta.get('script_pub_type'),   # script_pub_type
                        meta.get('script_sig_type'),   # script_sig_type
                        meta.get('balance_sat', 0),    # balance_sat
                        meta.get('block_timestamp', 0) # block_timestamp (ns)
                    ) for addr, meta in address_updates.items()]
                    db.execute_values(cur,
                        """
                        INSERT INTO addresses 
                        (address, first_seen_block, last_seen_block, tx_count, 
                         script_pub_type, script_sig_type, balance_sat, block_timestamp)
                        VALUES %s
                        ON CONFLICT (address, block_timestamp) DO UPDATE SET
                            first_seen_block = LEAST(addresses.first_seen_block, EXCLUDED.first_seen_block),
                            last_seen_block = GREATEST(addresses.last_seen_block, EXCLUDED.last_seen_block),
                            tx_count = addresses.tx_count + EXCLUDED.tx_count,
                            script_pub_type = COALESCE(EXCLUDED.script_pub_type, addresses.script_pub_type),
                            script_sig_type = COALESCE(EXCLUDED.script_sig_type, addresses.script_sig_type),
                            balance_sat = addresses.balance_sat + EXCLUDED.balance_sat
                        """,
                        addr_vals)

        except Exception as e:
            print(f"Error writing chunk {chunk_start}-{chunk_end} to PostgreSQL: {e}")
        # Shutdown pool to flush all connections and data to disk
        db.shutdown_pool()
        # Print summary for this chunk
        print(f"✅ Saved and flushed chunk {chunk_start}–{chunk_end}")

# ========================
# ANALYSIS
# ========================
def is_pubkey_hex(s):
    """Roughly detect compressed/uncompressed pubkeys."""
    return ((s.startswith("02") or s.startswith("03")) and len(s) == 66) or \
           (s.startswith("04") and len(s) == 130)


def analyze_tx(tx, height, block_ts):
    """Analyze a transaction JSON object from getblock(...,2).
    
    Returns:
        tuple: (address metadata updates)
        where address metadata includes script types, balance changes, etc.
    """
    address_updates = {}  # address -> {first_seen, last_seen, script types, balance change}

    # Check outputs (exposed pubkeys and track balances)
    def detect_script_type(spk):
        """
        Detect scriptPubKey type for common and rare types.
        Covers: P2PK, P2PKH, P2SH, P2MS, P2WPKH, P2WSH, P2TR, OP_RETURN, NULLDATA, nonstandard, witness_v1_taproot, etc.
        """
        if not spk:
            return None
        t = spk.get("type")
        # Common types
        if t == "pubkey":
            return "P2PK"
        if t == "pubkeyhash":
            return "P2PKH"
        if t == "scripthash":
            return "P2SH"
        if t == "multisig":
            return "P2MS"
        if t == "witness_v0_keyhash":
            return "P2WPKH"
        if t == "witness_v0_scripthash":
            return "P2WSH"
        if t == "v1_p2tr" or t == "witness_v1_taproot":
            return "P2TR"
        # Other types
        if t == "nulldata" or t == "op_return":
            return "OP_RETURN"
        if t == "nonstandard":
            return "nonstandard"
        # Add more as needed
        return t

    address = ""
    for vout in tx.get("vout", []):
        spk = vout.get("scriptPubKey", {})
        address = spk.get("address", "")
        value_sat = int(float(vout.get("value", 0)) * 100_000_000)

        script_type = detect_script_type(spk)

        # Try to decode address if missing
        if not address and "hex" in spk:
            try:
                script = CScript(x(spk["hex"]))
                # try generic decoder first (P2SH/P2WPKH etc.)
                try:
                    address = str(CBitcoinAddress.from_scriptPubKey(script))
                except Exception:
                    address = ""
                # If still missing and it's a raw pubkey output, extract pubkey and derive address
                if not address and script_type == "P2PK":
                    for item in script:
                        # script elements that are bytes may be pubkey
                        if isinstance(item, (bytes, bytearray)) and len(item) in (33, 65):
                            try:
                                pubkey = CPubKey(item)
                                # prefer native segwit address for compressed pubkeys
                                if len(item) == 33:
                                    try:
                                        address = str(P2WPKHBitcoinAddress.from_pubkey(pubkey))
                                    except Exception:
                                        address = str(P2PKHBitcoinAddress.from_pubkey(pubkey))
                                else:
                                    address = str(P2PKHBitcoinAddress.from_pubkey(pubkey))
                                break
                            except Exception:
                                continue
            except Exception:
                address = ""

        # Update address metadata if we have an address
        if address:
            addr_meta = address_updates.setdefault(address, {
                "first_seen_block": height,
                "last_seen_block": height,
                "tx_count": 1,
                "script_pub_type": script_type,
                "script_sig_type": None,
                "balance_sat": 0,
                "block_timestamp": block_ts
            })
            addr_meta["last_seen_block"] = max(height, addr_meta["last_seen_block"])
            addr_meta["balance_sat"] += value_sat
            # Keep most recent scriptPubKey type
            if script_type:
                addr_meta["script_pub_type"] = script_type

    # Check inputs (revealed pubkeys and track spends)
    for vin in tx.get("vin", []):
        if "txid" not in vin:
            continue

        scriptsig = vin.get("scriptSig", {})
        scriptsig_asm = scriptsig.get("asm", "")
        scriptsig_type = scriptsig.get("type", "")
        witness = vin.get("txinwitness", [])
        revealed_keys = []
        prev_addr = vin.get("prevout", {}).get("scriptPubKey", {}).get("address", "")
        value_sat = int(float(vin.get("prevout", {}).get("value", 0)) * 100_000_000)

        # Look for revealed pubkeys in scriptSig and witness
        # Reused addresses
        if any(is_pubkey_hex(x) for x in scriptsig_asm.split()):
            revealed_keys.extend([x for x in scriptsig_asm.split() if is_pubkey_hex(x)])
        if any(is_pubkey_hex(x) for x in witness):
            revealed_keys.extend([x for x in witness if is_pubkey_hex(x)])

        # Process revealed keys and track metadata
        if revealed_keys:
            for pubkey_hex in revealed_keys:
                try:
                    pubkey_bytes = x(pubkey_hex)
                    pubkey = CPubKey(pubkey_bytes)
                    # For compressed pubkeys prefer P2WPKH (bech32) if possible
                    if len(pubkey_bytes) == 33:
                        try:
                            address = str(P2WPKHBitcoinAddress.from_pubkey(pubkey))
                        except Exception:
                            address = str(P2PKHBitcoinAddress.from_pubkey(pubkey))
                    else:
                        address = str(P2PKHBitcoinAddress.from_pubkey(pubkey))

                    # Update address metadata for the revealed key
                    addr_meta = address_updates.setdefault(address, {
                        "first_seen_block": height,
                        "last_seen_block": height,
                        "tx_count": 1,
                        "script_pub_type": None,
                        "script_sig_type": scriptsig_type,
                        "balance_sat": 0,
                        "block_timestamp": block_ts
                    })
                    addr_meta["last_seen_block"] = max(height, addr_meta["last_seen_block"])
                    addr_meta["tx_count"] += 1
                    if scriptsig_type:
                        addr_meta["script_sig_type"] = scriptsig_type

                except Exception as e:
                    print(f"Error converting pubkey to address: {e}")

        # Update balance for spent outputs if we have the previous address
        if prev_addr:
            addr_meta = address_updates.setdefault(prev_addr, {
                "first_seen_block": height,
                "last_seen_block": height,
                "tx_count": 1,
                "script_pub_type": None,
                "script_sig_type": None,
                "balance_sat": 0,
                "block_timestamp": block_ts
            })
            addr_meta["last_seen_block"] = max(height, addr_meta["last_seen_block"])
            addr_meta["balance_sat"] -= value_sat
            addr_meta["tx_count"] += 1

    return address_updates


# ========================
# MAIN
# ========================
def monitor_blocks():
    """Monitor for new blocks continuously."""
    running = True
    def handle_stop(signum, frame):
        nonlocal running
        print("\nShutting down gracefully...")
        running = False
    
    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    # Get current chain tip and last processed height
    try:
        # chain_tip = rpc_call("getblockcount")
        chain_tip = 2_000
        last_processed = get_last_processed_height()

        # Get last indexed block hash and check it matches bitcoind
        if last_processed > 0:
            try:
                with db.get_db_cursor() as cur:
                    cur.execute("SELECT block_hash FROM block_log WHERE block_height = %s", (last_processed,))
                    row = cur.fetchone()
                    if row and row[0]:
                        indexed_hash = row[0]
                        chain_hash = rpc_call("getblockhash", [last_processed])
                        if indexed_hash != chain_hash:
                            print(f"⚠️  Chain reorganization detected at height {last_processed}")
                            # On reorg, rollback one block and rescan
                            last_processed -= 1
            except Exception as e:
                print(f"Error verifying last block: {e}")

        # Initial scan of last INITIAL_BLOCKS if we haven't processed any
        if last_processed == 0:
            start = max(0, chain_tip)
            process_range(0, chain_tip)
            last_processed = chain_tip
        # Otherwise catch up from our last processed to current tip
        # elif last_processed < chain_tip:
        #     process_range(last_processed + 1, chain_tip)
        #     last_processed = chain_tip

        print(f"\n✅ Initial sync complete. Monitoring for new blocks from height {last_processed}...")

        # Monitor for new blocks
        while running:
            try:
                chain_tip = rpc_call("getblockcount")
                
                # First verify our last processed block is still valid
                if last_processed > 0:
                    with db.get_db_cursor() as cur:
                        cur.execute("SELECT block_hash FROM block_log WHERE block_height = %s", (last_processed,))
                        row = cur.fetchone()
                        if row and row[0]:
                            indexed_hash = row[0]
                            chain_hash = rpc_call("getblockhash", [last_processed])
                            if indexed_hash != chain_hash:
                                print(f"⚠️  Chain reorganization detected at height {last_processed}")
                                # On reorg, rollback one block and rescan
                                last_processed -= 1
                                continue  # restart loop to handle reorg
                
                # Process new blocks if we're behind
                if chain_tip > last_processed:
                    process_range(last_processed + 1, chain_tip)
                    last_processed = chain_tip
                    print(f"Chain tip: {chain_tip}")
                time.sleep(POLL_INTERVAL)
            except Exception as e:
                print(f"Error polling for new blocks: {e}")
                if running:  # only sleep on error if we're still meant to be running
                    time.sleep(RECONNECT_DELAY)

    except Exception as e:
        print(f"Fatal error in monitor_blocks: {e}")
        sys.exit(1)

    print("Shutdown complete.")
    db.shutdown_pool()

# Create schema once at startup
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

def main():
    schema_init()
    monitor_blocks()

if __name__ == "__main__":
    main()