#!/usr/bin/env python3
"""
Verify we can read the first block of blk*.dat files and that the block hash is correct.
Useful to check if the first block of e.g. blk00001.dat is readable and whether
height 9444's hash could be lost due to an off-by-one or first-block edge case.

Usage:
  python scripts/verify_blk_first_block.py [--blocks-dir /path/to/blocks] [--file blk00001.dat]
  BLOCKS_DIR=/root/data-bitcoin/blocks python scripts/verify_blk_first_block.py --file blk00001.dat
"""
import os
import sys
import struct
import hashlib
import argparse

# Run from repo root so indexer is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from indexer.test_blk import read_blk_file, read_xor_key_from_file
from indexer.block_reader import calculate_block_hash


def block_hash_from_blk_block(blk_block: dict) -> str:
    """Compute Bitcoin block hash (display order) from our BLK-format block dict."""
    header = blk_block.get("header", {})
    version = header.get("version", 0)
    prev_block_hash = bytes.fromhex(header.get("prev_block_hash", "0" * 64))[::-1]
    merkle_root = bytes.fromhex(header.get("merkle_root", "0" * 64))[::-1]
    timestamp_int = header.get("timestamp", 0)
    bits = header.get("bits", 0)
    nonce = header.get("nonce", 0)
    header_bytes = (
        struct.pack("<I", version)
        + prev_block_hash
        + merkle_root
        + struct.pack("<I", timestamp_int)
        + struct.pack("<I", bits)
        + struct.pack("<I", nonce)
    )
    if len(header_bytes) != 80:
        raise ValueError(f"Header is {len(header_bytes)} bytes, expected 80")
    return calculate_block_hash(header_bytes)


def main():
    parser = argparse.ArgumentParser(description="Verify first block of blk*.dat and hashes")
    parser.add_argument(
        "--blocks-dir",
        default=os.path.expanduser(os.environ.get("BLOCKS_DIR", "~/data-bitcoin/blocks")),
        help="Path to blocks directory (default: BLOCKS_DIR or ~/data-bitcoin/blocks)",
    )
    parser.add_argument(
        "--file",
        metavar="blkNNNNN.dat",
        help="Specific file to check (e.g. blk00001.dat). If omitted, check blk00000 and blk00001.",
    )
    parser.add_argument(
        "--height",
        type=int,
        metavar="N",
        help="If set, fetch getblockhash(N) from RPC and report expected hash (requires RPC).",
    )
    args = parser.parse_args()

    blocks_dir = os.path.abspath(args.blocks_dir)
    if not os.path.isdir(blocks_dir):
        print(f"Error: blocks dir not found: {blocks_dir}")
        sys.exit(1)

    xor_key = read_xor_key_from_file(blocks_dir)
    if xor_key:
        print(f"Using XOR key from xor.dat ({len(xor_key)} bytes)")
    else:
        print("No xor.dat; reading without deobfuscation")

    if args.file:
        files_to_check = [os.path.join(blocks_dir, args.file)]
        if not os.path.isfile(files_to_check[0]):
            print(f"Error: file not found: {files_to_check[0]}")
            sys.exit(1)
    else:
        files_to_check = [
            os.path.join(blocks_dir, "blk00000.dat"),
            os.path.join(blocks_dir, "blk00001.dat"),
        ]
        files_to_check = [f for f in files_to_check if os.path.isfile(f)]
        if not files_to_check:
            print(f"No blk00000.dat or blk00001.dat in {blocks_dir}")
            sys.exit(1)

    # Known genesis hash (mainnet)
    genesis_hash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"

    for blk_path in files_to_check:
        name = os.path.basename(blk_path)
        print(f"\n--- {name} ---")
        try:
            blocks, _ = read_blk_file(
                blk_path,
                max_blocks=1,
                xor_key=xor_key,
                start_block_index=0,
                target_height=None,
            )
        except Exception as e:
            print(f"  Error reading first block: {e}")
            continue

        if not blocks:
            print("  No block read (read_blk_file returned empty list)")
            continue

        first = blocks[0]
        try:
            computed_hash = block_hash_from_blk_block(first)
        except Exception as e:
            print(f"  Error computing hash: {e}")
            continue

        tx_count = first.get("tx_count", 0)
        block_size = first.get("block_size", 0)
        print(f"  First block hash (computed): {computed_hash}")
        print(f"  Block size: {block_size} bytes, tx_count: {tx_count}")

        # Resolve height: RPC getblock(hash, 1) or Core block index
        first_block_height = None
        try:
            import requests
            from indexer import config
            url = config.RPC_URL
            auth = (config.RPC_USER, config.RPC_PASSWORD)
            payload = {"jsonrpc": "1.0", "id": "verify", "method": "getblock", "params": [computed_hash, 1]}
            r = requests.post(url, json=payload, auth=auth, timeout=10)
            r.raise_for_status()
            data = r.json()
            if data.get("result"):
                first_block_height = data["result"].get("height")
        except Exception:
            pass
        if first_block_height is None:
            try:
                from indexer.core_block_index import get_block_height
                index_path = os.path.join(blocks_dir, "index")
                first_block_height = get_block_height(index_path, computed_hash)
            except Exception:
                pass
        if first_block_height is not None:
            print(f"  First block of {name} → height: {first_block_height}")
        else:
            print("  First block height: (could not resolve; RPC and Core index unavailable)")

        if name == "blk00000.dat" and computed_hash.lower() == genesis_hash.lower():
            print("  OK: matches known genesis block hash")
        elif name == "blk00000.dat":
            print(f"  WARN: genesis expected {genesis_hash}")

    if args.height is not None:
        print(f"\n--- RPC getblockhash({args.height}) ---")
        try:
            import json
            import requests
            from indexer import config
            url = config.RPC_URL
            auth = (config.RPC_USER, config.RPC_PASSWORD)
            payload = {"jsonrpc": "1.0", "id": "verify", "method": "getblockhash", "params": [args.height]}
            r = requests.post(url, json=payload, auth=auth, timeout=10)
            r.raise_for_status()
            data = r.json()
            expected_hash = data.get("result")
            if expected_hash:
                print(f"  Expected hash for height {args.height}: {expected_hash}")
                print("  (Indexer scans blk files until it finds a block with this hash.)")
            else:
                print(f"  RPC error: {data}")
        except Exception as e:
            print(f"  RPC not available or failed: {e}")

    print()


if __name__ == "__main__":
    main()
