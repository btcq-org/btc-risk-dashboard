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
def main():
    height = 100001
    blockhash = rpc_call("getblockhash", [height])
    block = rpc_call("getblock", [blockhash, 3])

    for tx in block.get("tx", []):
        parsed = parse_single_tx(tx)
        print(json.dumps(parsed, indent=2))

if __name__ == "__main__":
    main()