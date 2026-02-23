#!/usr/bin/env python3
"""
Read blocks from blk*.dat in ascending height order and print height and size.
Uses the block index (LevelDB) to get (height, file, offset) for every block,
then reads the 4-byte block size at each offset without parsing the full block.

Usage:
  python scripts/read_blocks_by_height.py [--blocks-dir DIR] [--index-dir DIR] [--max N] [--csv]
  BLOCK_INDEX_DIR=./data/index python scripts/read_blocks_by_height.py --max 100

Requires plyvel and a block index (use BLOCK_INDEX_DIR or --index-dir if Core holds the lock).
"""
import os
import sys
import struct
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from indexer.test_blk import read_xor_key_from_file, apply_xor
from indexer.core_block_index import iter_block_index_by_height, blk_path_from_file_number, BLOCK_FILE_HEADER_SIZE


def read_block_size_at(blocks_dir: str, n_file: int, n_data_pos: int, xor_key: bytes) -> int:
    """Read the 4-byte block size. n_data_pos is offset of block payload; size is at n_data_pos - 4."""
    path = blk_path_from_file_number(blocks_dir, n_file)
    if not os.path.isfile(path):
        return -1
    size_offset = n_data_pos - 4  # 4 bytes before payload = size field
    with open(path, "rb") as f:
        f.seek(size_offset)
        raw = f.read(4)
    if len(raw) < 4:
        return -1
    if xor_key:
        raw = apply_xor(raw, xor_key, offset=size_offset)
    return struct.unpack("<I", raw)[0]


def main():
    parser = argparse.ArgumentParser(description="Read blocks from blk files by height, print height and size")
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
        "--max",
        type=int,
        default=None,
        metavar="N",
        help="Stop after N blocks (default: all)",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Output as CSV (height,size_bytes)",
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
        print("Use a copy of blocks/index if Bitcoin Core is running (e.g. BLOCK_INDEX_DIR=./data/index)", file=sys.stderr)
        sys.exit(1)

    xor_key = read_xor_key_from_file(blocks_dir)
    if not xor_key:
        print("Warning: no xor.dat; block sizes may be wrong if files are obfuscated", file=sys.stderr)

    try:
        import plyvel
    except ImportError:
        print("Error: plyvel required. pip install plyvel", file=sys.stderr)
        sys.exit(1)

    if args.csv:
        print("height,size_bytes")
    else:
        print("height\tsize_bytes")

    n = 0
    for height, n_file, n_data_pos in iter_block_index_by_height(index_path):
        size = read_block_size_at(blocks_dir, n_file, n_data_pos, xor_key)
        if args.csv:
            print(f"{height},{size}")
        else:
            print(f"{height}\t{size}")
        n += 1
        if args.max is not None and n >= args.max:
            break

    if not args.csv:
        print(f"# total blocks: {n}", file=sys.stderr)


if __name__ == "__main__":
    main()
