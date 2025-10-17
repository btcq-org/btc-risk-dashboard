#!/usr/bin/env python3
"""
Find "quantum-at-risk" pubkeys (revealed pubkeys in inputs or raw pubkeys in P2PK outputs)
in the first 100 Bitcoin *testnet* blocks using your local bitcoind node via RPC.
"""

import requests, json, csv, concurrent.futures
from threading import Lock

RPC_USER = "admin"
RPC_PASSWORD = "pass"
RPC_PORT = 30032
RPC_URL = f"http://127.0.0.1:{RPC_PORT}/"

THREADS = 8               # number of parallel workers
BLOCK_COUNT = 50          # number of blocks to scan (from tip backwards)

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

# ========================
# ANALYSIS
# ========================
def is_pubkey_hex(s):
    """Roughly detect compressed/uncompressed pubkeys."""
    return ((s.startswith("02") or s.startswith("03")) and len(s) == 66) or \
           (s.startswith("04") and len(s) == 130)

def analyze_tx(tx, height):
    """Analyze a transaction JSON object from getblock(...,2)."""
    exposed, revealed = [], []

    # Check outputs (exposed pubkeys)
    for vout in tx.get("vout", []):
        spk = vout.get("scriptPubKey", {})
        stype = spk.get("type")
        if stype in ("pubkey", "v1_p2tr"):
            exposed.append({
                "block_height": height,
                "txid": tx["txid"],
                "vout": vout["n"],
                "type": stype,
                "address": spk.get("address", ""),
                "value": vout.get("value", 0)
            })

    # Check inputs (revealed pubkeys)
    for vin in tx.get("vin", []):
        if "txid" not in vin:
            continue
        scriptsig = vin.get("scriptSig", {}).get("asm", "")
        witness = vin.get("txinwitness", [])
        revealed_keys = []

        if any(is_pubkey_hex(x) for x in scriptsig.split()):
            revealed_keys.extend([x for x in scriptsig.split() if is_pubkey_hex(x)])
        if any(is_pubkey_hex(x) for x in witness):
            revealed_keys.extend([x for x in witness if is_pubkey_hex(x)])

        if revealed_keys:
            revealed.append({
                "block_height": height,
                "txid": tx["txid"],
                "spent_from": f"{vin['txid']}:{vin['vout']}",
                "revealed_keys": ";".join(revealed_keys)
            })

    return exposed, revealed


# ========================
# MAIN
# ========================
def main():
    best = rpc_call("getblockcount")
    start = max(0, best - BLOCK_COUNT)
    print(f"Scanning last {BLOCK_COUNT} blocks ({start} → {best}) with {THREADS} threads...\n")

    exposed_rows = []
    revealed_rows = []
    lock = Lock()

    def process_block(height):
        try:
            blockhash = rpc_call("getblockhash", [height])
            block = rpc_call("getblock", [blockhash, 2])
        except Exception as e:
            print(f"Error reading block {height}: {e}")
            return

        local_exposed, local_revealed = [], []
        for tx in block.get("tx", []):
            exp, rev = analyze_tx(tx, height)
            local_exposed.extend(exp)
            local_revealed.extend(rev)

        with lock:
            exposed_rows.extend(local_exposed)
            revealed_rows.extend(local_revealed)
            if local_exposed or local_revealed:
                print(f"Block {height}: +{len(local_exposed)} exposed, +{len(local_revealed)} revealed")

    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
        list(executor.map(process_block, range(start, best + 1)))

    # Write CSVs
    with open("data/quantum_exposed.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["block_height", "txid", "vout", "type", "address", "value"])
        writer.writeheader()
        writer.writerows(exposed_rows)

    with open("data/quantum_revealed.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["block_height", "txid", "spent_from", "revealed_keys"])
        writer.writeheader()
        writer.writerows(revealed_rows)

    print("\n✅ Done.")
    print(f"Exposed outputs: {len(exposed_rows)} → quantum_exposed.csv")
    print(f"Revealed pubkeys: {len(revealed_rows)} → quantum_revealed.csv")

if __name__ == "__main__":
    main()