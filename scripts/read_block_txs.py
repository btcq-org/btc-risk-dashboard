#!/usr/bin/env python3
"""
Read transactions for a block at a specific height from blk*.dat files.
Uses RPC to get the block hash, the block index for file/offset, then reads
the block and lists its transactions (inputs/outputs summary).

Usage:
  python scripts/read_block_txs.py --height 9444
  python scripts/read_block_txs.py --height 9444 --blocks-dir /path/to/blocks --index-dir ./data/index
  python scripts/read_block_txs.py --height 9444 --block-hash 00000000...
"""
import os
import sys
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def get_block_hash_rpc(height: int):
    """Return block hash (hex) for height via RPC, or None."""
    try:
        import requests
        from indexer import config
        url = config.RPC_URL
        auth = (config.RPC_USER, config.RPC_PASSWORD)
        payload = {"jsonrpc": "1.0", "id": "read_txs", "method": "getblockhash", "params": [height]}
        r = requests.post(url, json=payload, auth=auth, timeout=10)
        r.raise_for_status()
        data = r.json()
        return data.get("result")
    except Exception as e:
        print(f"RPC getblockhash({height}) failed: {e}", file=sys.stderr)
        return None


def main():
    parser = argparse.ArgumentParser(description="Read transactions for a block at a given height from blk files")
    parser.add_argument(
        "--height",
        type=int,
        required=True,
        metavar="N",
        help="Block height",
    )
    parser.add_argument(
        "--blocks-dir",
        default=os.path.expanduser(os.environ.get("BLOCKS_DIR", "~/data-bitcoin/blocks")),
        help="Path to blocks directory",
    )
    parser.add_argument(
        "--index-dir",
        default=os.environ.get("BLOCK_INDEX_DIR", "").strip() or None,
        help="Path to block index (LevelDB). Default: blocks-dir/index.",
    )
    parser.add_argument(
        "--block-hash",
        metavar="HEX",
        help="Block hash (64 hex chars). If set, skip RPC.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print input prev_out and full script types",
    )
    args = parser.parse_args()

    blocks_dir = os.path.abspath(args.blocks_dir)
    if not os.path.isdir(blocks_dir):
        print("Error: blocks dir not found:", blocks_dir, file=sys.stderr)
        sys.exit(1)

    index_path = args.index_dir
    if not index_path:
        index_path = os.path.join(blocks_dir, "index")
    else:
        index_path = os.path.abspath(os.path.expanduser(index_path))
    if not os.path.isdir(index_path):
        print("Error: index path not found:", index_path, file=sys.stderr)
        sys.exit(1)

    height = args.height

    # Block hash
    if args.block_hash:
        block_hash = args.block_hash.strip().lower()
        if len(block_hash) != 64 or not all(c in "0123456789abcdef" for c in block_hash):
            print("Error: --block-hash must be 64 hex characters.", file=sys.stderr)
            sys.exit(1)
    else:
        block_hash = get_block_hash_rpc(height)
        if not block_hash:
            print("Cannot get block hash without RPC. Set RPC_* or use --block-hash.", file=sys.stderr)
            sys.exit(1)

    # Block location from index
    from indexer.core_block_index import diagnose_block_index, blk_path_from_file_number
    loc_result, diag = diagnose_block_index(index_path, block_hash)
    if loc_result is None:
        print("Block index lookup failed:", diag, file=sys.stderr)
        sys.exit(1)
    n_file, n_data_pos, _ = loc_result
    blk_path = blk_path_from_file_number(blocks_dir, n_file)
    if not os.path.isfile(blk_path):
        print("Error: blk file not found:", blk_path, file=sys.stderr)
        sys.exit(1)

    # Read block
    from indexer.test_blk import read_block, read_xor_key_from_file
    xor_key = read_xor_key_from_file(blocks_dir)
    with open(blk_path, "rb") as f:
        f.seek(n_data_pos)
        block = read_block(f, xor_key, None)
    if not block:
        print("Error: failed to read block at offset", n_data_pos, file=sys.stderr)
        sys.exit(1)

    # Block summary
    h = block["header"]
    print("Block height:", height)
    print("Block hash: ", block_hash)
    print("Prev block: ", h.get("prev_block_hash", ""))
    print("Merkle root:", h.get("merkle_root", ""))
    print("Timestamp:  ", h.get("timestamp"))
    print("Tx count:   ", block["tx_count"])
    print()

    for i, tx in enumerate(block["transactions"]):
        n_in = len(tx.get("inputs", []))
        n_out = len(tx.get("outputs", []))
        print(f"--- Tx #{i} (inputs={n_in}, outputs={n_out}) ---")
        if args.verbose:
            for j, inp in enumerate(tx.get("inputs", [])):
                print(f"  In[{j}]:  {inp.get('prev_tx_hash', '')[:16]}...:{inp.get('prev_output_index')}")
        total_out = 0
        for j, out in enumerate(tx.get("outputs", [])):
            val = out.get("value", 0)
            total_out += val
            addr = out.get("address") or out.get("script_type") or "?"
            print(f"  Out[{j}]: {val:>12} sat  {addr}")
        if n_out:
            print(f"  Total out: {total_out} sat")
        print()

    print(f"# Block had {block['tx_count']} transaction(s)")


if __name__ == "__main__":
    main()
