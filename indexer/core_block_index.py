"""
Read Bitcoin Core's block index (LevelDB) to get blk*.dat file number and offset for each block.
This allows direct seeks instead of scanning files. Requires plyvel and Core's blocks/index.

Index path: BLOCKS_DIR/index (e.g. /path/to/data-bitcoin/blocks/index).
Note: LevelDB does not allow concurrent write access. If Bitcoin Core is running it may hold
the index open; we open read-only so it may work on some systems, otherwise we fall back to
scanning blk*.dat files.

Key: 'b' + 32-byte block hash (little-endian = RPC hash hex decoded then reversed).
Value: CDiskBlockIndex serialization (varints + 80-byte header).
"""
import os
import struct
from typing import Optional, Tuple

# Status flags from Bitcoin Core chain.h
BLOCK_HAVE_DATA = 8   # full block available in blk*.dat
BLOCK_HAVE_UNDO = 16  # undo data in rev*.dat


def _decode_varint(data: bytes, offset: int) -> Tuple[int, int]:
    """Decode Bitcoin Core varint (MSB base-128). Returns (value, new_offset)."""
    n = 0
    while offset < len(data):
        ch = data[offset]
        offset += 1
        n = (n << 7) | (ch & 0x7F)
        if ch & 0x80:
            n += 1
        else:
            return n, offset
    return n, offset


def get_block_location(index_path: str, block_hash_hex: str) -> Optional[Tuple[int, int]]:
    """
    Look up block in Core's block index. Returns (n_file, n_data_pos) or None.
    
    Args:
        index_path: Path to blocks/index directory (LevelDB).
        block_hash_hex: Block hash as hex string (RPC order, 64 chars).
    
    Returns:
        (file_number, byte_offset) for blk*.dat, or None if not found / no index.
    """
    loc = _get_block_index_record(index_path, block_hash_hex)
    return (loc[0], loc[1]) if loc else None


def get_block_height(index_path: str, block_hash_hex: str) -> Optional[int]:
    """
    Look up block height in Core's block index. Returns height or None.
    
    Args:
        index_path: Path to blocks/index directory (LevelDB).
        block_hash_hex: Block hash as hex string (RPC order, 64 chars).
    
    Returns:
        Block height, or None if not found / no index.
    """
    loc = _get_block_index_record(index_path, block_hash_hex)
    return loc[2] if loc else None


def _get_block_index_record(index_path: str, block_hash_hex: str) -> Optional[Tuple[int, int, int]]:
    """
    Internal: look up block index record. Returns (n_file, n_data_pos, n_height) or None.
    """
    try:
        import plyvel
    except ImportError:
        return None

    if not os.path.isdir(index_path):
        return None

    try:
        hash_bytes = bytes.fromhex(block_hash_hex)
    except Exception:
        return None
    if len(hash_bytes) != 32:
        return None
    key = b'b' + hash_bytes[::-1]

    try:
        db = plyvel.DB(index_path, create_if_missing=False)
    except Exception:
        return None

    try:
        value = db.get(key)
    except Exception:
        value = None
    finally:
        db.close()

    if not value or len(value) < 80:
        return None

    offset = 0
    _, offset = _decode_varint(value, offset)
    n_height, offset = _decode_varint(value, offset)
    n_status, offset = _decode_varint(value, offset)
    _, offset = _decode_varint(value, offset)

    if not (n_status & BLOCK_HAVE_DATA):
        return None

    n_file, offset = _decode_varint(value, offset)
    n_data_pos, offset = _decode_varint(value, offset)

    return (n_file, n_data_pos, n_height)


def blk_path_from_file_number(blocks_dir: str, n_file: int) -> str:
    """Return path to blkNNNNN.dat for given file number."""
    return os.path.join(blocks_dir, f"blk{n_file:05d}.dat")
