#!/usr/bin/env python3
"""
Find which blk*.dat file contains the block at a given height.
Uses RPC getblockhash(height) to get the block hash, then Core's block index
(LevelDB) to get file number and offset. Requires plyvel and blocks/index (or
a copy via --index-dir).

Usage:
  python scripts/find_blk_by_height.py --height 9444
  python scripts/find_blk_by_height.py --height 9444 --blocks-dir /path/to/blocks
  BLOCK_INDEX_DIR=/path/to/index-copy python scripts/find_blk_by_height.py --height 9444
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
        payload = {"jsonrpc": "1.0", "id": "find", "method": "getblockhash", "params": [height]}
        r = requests.post(url, json=payload, auth=auth, timeout=10)
        r.raise_for_status()
        data = r.json()
        return data.get("result")
    except Exception as e:
        print(f"RPC getblockhash({height}) failed: {e}", file=sys.stderr)
        return None


def main():
    parser = argparse.ArgumentParser(description="Find which blk*.dat file contains a block at the given height")
    parser.add_argument(
        "--height",
        type=int,
        default=9445,
        metavar="N",
        help="Block height to find (default: 9444)",
    )
    parser.add_argument(
        "--blocks-dir",
        default=os.path.expanduser(os.environ.get("BLOCKS_DIR", "~/data-bitcoin/blocks")),
        help="Path to blocks directory",
    )
    parser.add_argument(
        "--index-dir",
        default=os.environ.get("BLOCK_INDEX_DIR", "").strip() or None,
        help="Path to block index (LevelDB). Default: blocks-dir/index. Use a copy if Core holds the lock.",
    )
    parser.add_argument(
        "--block-hash",
        metavar="HEX",
        help="Block hash (hex, 64 chars). If set, skip RPC and use this hash for index lookup.",
    )
    args = parser.parse_args()

    blocks_dir = os.path.abspath(args.blocks_dir)
    index_path = args.index_dir
    if not index_path:
        index_path = os.path.join(blocks_dir, "index")

    height = args.height

    # 1) Get block hash (RPC or --block-hash)
    if args.block_hash:
        block_hash = args.block_hash.strip()
        if len(block_hash) != 64 or not all(c in "0123456789abcdefABCDEF" for c in block_hash):
            print("Error: --block-hash must be 64 hex characters.")
            sys.exit(1)
        block_hash = block_hash.lower()
        print(f"Using block hash: {block_hash} (height {height} from arg)")
    else:
        block_hash = get_block_hash_rpc(height)
        if not block_hash:
            print("Cannot get block hash without RPC. Set RPC_* env or run bitcoind with RPC enabled.")
            sys.exit(1)
        print(f"Block height {height} → hash: {block_hash}")

    # 2) Look up in block index (with diagnostics)
    from indexer.core_block_index import diagnose_block_index, blk_path_from_file_number

    loc_result, diag = diagnose_block_index(index_path, block_hash)
    if loc_result is None:
        print("Block index lookup failed.")
        print(diag)
        sys.exit(1)
    n_file, n_data_pos, _ = loc_result
    loc = (n_file, n_data_pos)

    n_file, n_data_pos = loc
    blk_name = f"blk{n_file:05d}.dat"
    blk_path = blk_path_from_file_number(blocks_dir, n_file)
    print(f"Block height {height} is in: {blk_name} at file offset {n_data_pos} (0x{n_data_pos:x})")
    print(f"Full path: {blk_path}")
    if not os.path.isfile(blk_path):
        print(f"Warning: file not found at {blk_path}")
    print()


if __name__ == "__main__":
    main()
