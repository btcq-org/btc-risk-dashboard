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


def _parse_block_index_value(value: bytes) -> Optional[Tuple[int, int, int]]:
    """Parse CDiskBlockIndex value. Returns (n_file, n_data_pos, n_height) or None."""
    if not value or len(value) < 80:
        return None
    offset = 0
    try:
        _, offset = _decode_varint(value, offset)
        n_height, offset = _decode_varint(value, offset)
        n_status, offset = _decode_varint(value, offset)
        _, offset = _decode_varint(value, offset)
        if not (n_status & BLOCK_HAVE_DATA):
            return None
        n_file, offset = _decode_varint(value, offset)
        n_data_pos, offset = _decode_varint(value, offset)
        return (n_file, n_data_pos, n_height)
    except Exception:
        return None


def get_block_location_from_db(db, block_hash_hex: str) -> Optional[Tuple[int, int]]:
    """
    Look up block using an already-open LevelDB instance. Returns (n_file, n_data_pos) or None.
    Caller must pass a plyvel DB object (e.g. from plyvel.DB(path, create_if_missing=False)).
    """
    try:
        hash_bytes = bytes.fromhex(block_hash_hex)
    except Exception:
        return None
    if len(hash_bytes) != 32:
        return None
    key = b'b' + hash_bytes[::-1]
    try:
        value = db.get(key)
    except Exception:
        return None
    rec = _parse_block_index_value(value) if value else None
    return (rec[0], rec[1]) if rec else None


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


def diagnose_block_index(index_path: str, block_hash_hex: str) -> Tuple[Optional[Tuple[int, int, int]], str]:
    """
    Run block index lookup step-by-step and return (result, diagnostic_message).
    Use this to see exactly where lookup fails (plyvel, open, get, parse).
    """
    try:
        import plyvel
    except ImportError as e:
        return None, f"plyvel not installed: {e}. Install with: pip install plyvel"

    if not os.path.isdir(index_path):
        return None, f"index path is not a directory: {index_path}"

    try:
        hash_bytes = bytes.fromhex(block_hash_hex)
    except Exception as e:
        return None, f"invalid block hash hex: {e}"
    if len(hash_bytes) != 32:
        return None, f"block hash must be 32 bytes (64 hex chars), got {len(hash_bytes)}"

    key = b'b' + hash_bytes[::-1]

    try:
        db = plyvel.DB(index_path, create_if_missing=False)
    except Exception as e:
        err = str(e).strip()
        if "LOCK" in err or "lock" in err.lower() or "Resource temporarily unavailable" in err:
            return None, f"LevelDB locked (Bitcoin Core may be running): {e}. Use a copy: stop Core, cp -r blocks/index /path/to/copy, set BLOCK_INDEX_DIR"
        return None, f"LevelDB open failed: {e}"

    try:
        value = db.get(key)
    except Exception as e:
        db.close()
        return None, f"db.get(key) failed: {e}"
    finally:
        db.close()

    if value is None:
        return None, "db.get(key) returned None (key not found in index; wrong hash or index from different chain?)"
    if len(value) < 80:
        return None, f"value too short: {len(value)} bytes (expected varints + 80-byte header)"

    offset = 0
    try:
        _, offset = _decode_varint(value, offset)
        n_height, offset = _decode_varint(value, offset)
        n_status, offset = _decode_varint(value, offset)
        _, offset = _decode_varint(value, offset)
    except Exception as e:
        return None, f"varint decode failed at offset {offset}: {e}"

    if not (n_status & BLOCK_HAVE_DATA):
        return None, f"block index entry has no data (n_status=0x{n_status:x}); block may be pruned or index from different node"

    try:
        n_file, offset = _decode_varint(value, offset)
        n_data_pos, offset = _decode_varint(value, offset)
    except Exception as e:
        return None, f"varint decode (n_file/n_data_pos) failed: {e}"

    return (n_file, n_data_pos, n_height), "ok"


def _get_block_index_record(index_path: str, block_hash_hex: str) -> Optional[Tuple[int, int, int]]:
    """
    Internal: look up block index record. Returns (n_file, n_data_pos, n_height) or None.
    """
    result, _ = diagnose_block_index(index_path, block_hash_hex)
    return result


def blk_path_from_file_number(blocks_dir: str, n_file: int) -> str:
    """Return path to blkNNNNN.dat for given file number."""
    return os.path.join(blocks_dir, f"blk{n_file:05d}.dat")


def iter_block_index_by_height(index_path: str):
    """
    Iterate the block index by ascending height. Yields (height, n_file, n_data_pos).
    Requires plyvel and an openable index (use a copy if Core is running).
    """
    try:
        import plyvel
    except ImportError:
        return
    if not os.path.isdir(index_path):
        return
    db = plyvel.DB(index_path, create_if_missing=False)
    try:
        entries = []
        for key, value in db.iterator(prefix=b'b'):
            if len(key) != 33:
                continue
            rec = _parse_block_index_value(value)
            if rec:
                n_file, n_data_pos, n_height = rec
                entries.append((n_height, n_file, n_data_pos))
        entries.sort(key=lambda x: x[0])
        for n_height, n_file, n_data_pos in entries:
            yield n_height, n_file, n_data_pos
    finally:
        db.close()
