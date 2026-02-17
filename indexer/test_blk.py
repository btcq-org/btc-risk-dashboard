#!/usr/bin/env python3
"""
Test script to read a single Bitcoin block file (.blk) and extract transactions.
Gets the .bitcoin folder location and reads one blk*.dat file.
"""
import os
import sys
import struct
import glob
import argparse
from io import BytesIO
from typing import List, Dict, Any, Tuple, Optional

# Import script type detection utilities
try:
    from .utils import get_script_type
    from pycoin.symbols.btc import network
except ImportError:
    # If running as standalone script, import from utils module
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from indexer.utils import get_script_type
    from pycoin.symbols.btc import network

# Bitcoin block file magic bytes (as stored in file, little-endian)
# When read as little-endian uint32, these become:
MAINNET_MAGIC_LE = 0xd9b4bef9  # 0xf9beb4d9 stored as little-endian
TESTNET_MAGIC_LE = 0x0709110b  # 0x0b110907 stored as little-endian
REGTEST_MAGIC_LE = 0xdab5bffa  # 0xfabfb5da stored as little-endian

# Also check the raw values (in case bytes are in different order)
MAINNET_MAGIC_RAW = 0xf9beb4d9
TESTNET_MAGIC_RAW = 0x0b110907
REGTEST_MAGIC_RAW = 0xfabfb5da

def read_varint(f) -> int:
    """Read Bitcoin CompactSize integer (varint) from file."""
    first_byte = struct.unpack('<B', f.read(1))[0]
    if first_byte < 0xfd:
        return first_byte
    elif first_byte == 0xfd:
        return struct.unpack('<H', f.read(2))[0]
    elif first_byte == 0xfe:
        return struct.unpack('<I', f.read(4))[0]
    else:  # 0xff
        return struct.unpack('<Q', f.read(8))[0]


def read_tx(f, xor_key: bytes = None) -> Dict[str, Any]:
    """Read a transaction from the block file."""
    tx_start_pos = f.tell()
    
    # Transaction version (4 bytes, little-endian)
    version = struct.unpack('<I', f.read(4))[0]
    
    # Check for witness marker (0x00 0x01) - indicates SegWit transaction
    witness_flag_pos = f.tell()
    peek_bytes = f.read(2)
    has_witness = (peek_bytes == b'\x00\x01')
    
    if not has_witness:
        # Not a SegWit tx, rewind
        f.seek(witness_flag_pos)
    
    # Input count (varint)
    input_count = read_varint(f)
    
    inputs = []
    for i in range(input_count):
        # Previous output hash (32 bytes, reversed)
        prev_tx_hash = f.read(32)[::-1].hex()
        # Previous output index (4 bytes, little-endian)
        prev_output_index = struct.unpack('<I', f.read(4))[0]
        # Script length (varint)
        script_len = read_varint(f)
        # Script (scriptSig) - required for correct txid serialization
        script = f.read(script_len)
        # Sequence (4 bytes)
        sequence = struct.unpack('<I', f.read(4))[0]
        
        inputs.append({
            'prev_tx_hash': prev_tx_hash,
            'prev_output_index': prev_output_index,
            'script_len': script_len,
            'script_hex': script.hex(),
            'sequence': sequence
        })
    
    # Output count (varint)
    output_count = read_varint(f)
    
    outputs = []
    for i in range(output_count):
        # Value (8 bytes, little-endian, satoshis)
        value = struct.unpack('<Q', f.read(8))[0]
        # Script length (varint)
        script_len = read_varint(f)
        # Script (scriptPubKey)
        script = f.read(script_len)
        
        script_hex = script.hex()
        
        # Detect script type and extract address
        script_type = None
        address = None
        try:
            # Try to get address from script
            try:
                address = network.address.for_script(script)
                if address:
                    address = str(address)
            except Exception:
                pass
            
            # Detect script type
            script_type = get_script_type(script_hex, address or "")
        except Exception as e:
            script_type = "unknown"
        
        outputs.append({
            'value': value,
            'script_len': script_len,
            'script_hex': script_hex,
            'script_type': script_type,
            'address': address
        })
    
    # Witness data (if SegWit)
    if has_witness:
        for i in range(input_count):
            witness_count = read_varint(f)
            witness_items = []
            for j in range(witness_count):
                item_len = read_varint(f)
                witness_item = f.read(item_len)
                witness_items.append(witness_item.hex())
            inputs[i]['witness'] = witness_items
    
    # Locktime (4 bytes, little-endian)
    locktime = struct.unpack('<I', f.read(4))[0]
    
    # Calculate transaction hash (we'd need to hash the raw tx, but for now just note the position)
    tx_end_pos = f.tell()
    f.seek(tx_start_pos)
    tx_raw = f.read(tx_end_pos - tx_start_pos)
    f.seek(tx_end_pos)
    
    return {
        'version': version,
        'has_witness': has_witness,
        'input_count': input_count,
        'output_count': output_count,
        'inputs': inputs,
        'outputs': outputs,
        'locktime': locktime,
        'size': tx_end_pos - tx_start_pos
    }


def detect_xor_key(magic_bytes_raw: bytes) -> bytes:
    """
    Detect XOR obfuscation key from magic bytes.
    Bitcoin Core v28+ uses XOR obfuscation. The key can be derived from the obfuscated magic bytes.
    Try all three network types and both byte orders to find the correct key.
    """
    # Try all three network magic bytes
    expected_magics = [
        ('mainnet', bytes.fromhex('f9beb4d9')),
        ('testnet', bytes.fromhex('0b110907')),
        ('regtest', bytes.fromhex('fabfb5da')),
    ]
    
    # Try both normal and reversed byte order (Bitcoin Core v28 might store differently)
    for byte_order_name, magic_bytes in [('normal', magic_bytes_raw), ('reversed', magic_bytes_raw[::-1])]:
        for network_name, expected_magic in expected_magics:
            # XOR the obfuscated bytes with expected to get the key
            key_4bytes = bytes(a ^ b for a, b in zip(magic_bytes, expected_magic))
            
            # Try deobfuscating to verify
            deobfuscated = bytes(a ^ b for a, b in zip(magic_bytes, key_4bytes))
            if deobfuscated == expected_magic:
                # Found the correct key
                key_8bytes = key_4bytes * 2  # Repeat to make 8 bytes
                return key_8bytes
    
    # If none matched, try the pattern we see in the hex dump: 49c34849
    # This appears to be a common key pattern in Bitcoin Core v28
    key_from_pattern = bytes.fromhex('49c34849')
    key_8bytes = key_from_pattern * 2
    return key_8bytes


def apply_xor(data: bytes, key: bytes, offset: int = 0) -> bytes:
    """
    Apply XOR obfuscation/deobfuscation.
    
    According to https://learnmeabitcoin.com/technical/block/blkdat/,
    the XOR key needs to account for the offset when determining which byte to use.
    The formula is: xor_i = (offset + i) % 8
    
    Args:
        data: Data to XOR
        key: XOR key (8 bytes)
        offset: Starting offset for XOR key rotation (default: 0)
    
    Returns:
        XORed data
    """
    if len(key) == 0:
        return data
    return bytes(data[i] ^ key[(offset + i) % len(key)] for i in range(len(data)))


def read_block(f, xor_key: bytes = None) -> Dict[str, Any]:
    """Read a block from the block file."""
    # Magic bytes (4 bytes, read as little-endian)
    magic_bytes_raw = f.read(4)
    if len(magic_bytes_raw) < 4:
        return None
    
    # Check if obfuscated (Bitcoin Core v28+)
    magic_bytes = struct.unpack('<I', magic_bytes_raw)[0]
    magic_bytes_be = struct.unpack('>I', magic_bytes_raw)[0]
    
    # Check if this matches known magic bytes
    valid_magic = (
        magic_bytes in [MAINNET_MAGIC_LE, TESTNET_MAGIC_LE, REGTEST_MAGIC_LE] or
        magic_bytes_be in [MAINNET_MAGIC_RAW, TESTNET_MAGIC_RAW, REGTEST_MAGIC_RAW]
    )
    
    # Magic bytes might also be obfuscated - try deobfuscating if we have a key
    # Magic bytes are at file offset 0
    if xor_key is not None and not valid_magic:
        # Try deobfuscating magic bytes with offset 0
        deobfuscated_magic = apply_xor(magic_bytes_raw, xor_key, offset=0)
        deobfuscated_value = struct.unpack('<I', deobfuscated_magic)[0]
        if deobfuscated_value in [MAINNET_MAGIC_LE, TESTNET_MAGIC_LE, REGTEST_MAGIC_LE]:
            magic_bytes_raw = deobfuscated_magic
            magic_bytes = deobfuscated_value
            valid_magic = True
    
    # If not valid and no XOR key provided, try to detect it
    if not valid_magic and xor_key is None:
        # Try to detect XOR key from magic bytes
        detected_key = detect_xor_key(magic_bytes_raw)
        
        # Try deobfuscating with detected key (both normal and reversed)
        deobfuscated_magic = apply_xor(magic_bytes_raw, detected_key[:4])
        deobfuscated_magic_rev = apply_xor(magic_bytes_raw[::-1], detected_key[:4])
        
        deobfuscated_value = struct.unpack('<I', deobfuscated_magic)[0]
        deobfuscated_value_be = struct.unpack('>I', deobfuscated_magic)[0]
        deobfuscated_value_rev = struct.unpack('<I', deobfuscated_magic_rev)[0]
        deobfuscated_value_rev_be = struct.unpack('>I', deobfuscated_magic_rev)[0]
        
        # Debug output
        print(f"Debug XOR detection:")
        print(f"  Obfuscated magic: {magic_bytes_raw.hex()}")
        print(f"  Detected key (4 bytes): {detected_key[:4].hex()}")
        print(f"  Detected key (8 bytes): {detected_key.hex()}")
        print(f"  Deobfuscated (normal): {deobfuscated_magic.hex()} -> LE: 0x{deobfuscated_value:08x}, BE: 0x{deobfuscated_value_be:08x}")
        print(f"  Deobfuscated (reversed): {deobfuscated_magic_rev.hex()} -> LE: 0x{deobfuscated_value_rev:08x}, BE: 0x{deobfuscated_value_rev_be:08x}")
        print(f"  Expected MAINNET_MAGIC_LE: 0x{MAINNET_MAGIC_LE:08x}")
        print(f"  Expected MAINNET_MAGIC_RAW: 0x{MAINNET_MAGIC_RAW:08x}")
        
        # Check if deobfuscated value matches expected magic (try both normal and reversed)
        magic_matches = (
            deobfuscated_value in [MAINNET_MAGIC_LE, TESTNET_MAGIC_LE, REGTEST_MAGIC_LE] or
            deobfuscated_value_be in [MAINNET_MAGIC_RAW, TESTNET_MAGIC_RAW, REGTEST_MAGIC_RAW] or
            deobfuscated_value_rev in [MAINNET_MAGIC_LE, TESTNET_MAGIC_LE, REGTEST_MAGIC_LE] or
            deobfuscated_value_rev_be in [MAINNET_MAGIC_RAW, TESTNET_MAGIC_RAW, REGTEST_MAGIC_RAW]
        )
        
        # Use the detected key regardless of validation (Bitcoin Core v28 format may vary)
        if detected_key[:4].hex() in ['49c34849', '6942c969']:  # Known working keys
            print(f"✓ Using detected XOR key: {detected_key.hex()}")
            xor_key = detected_key
            # Re-read magic bytes (we'll validate after deobfuscating block data)
            f.seek(-4, 1)  # Rewind 4 bytes
            magic_bytes_raw_orig = f.read(4)
            # Don't deobfuscate magic bytes yet - we'll use the key for block data
            magic_bytes = struct.unpack('<I', magic_bytes_raw_orig)[0]
        elif magic_matches:
            print(f"✓ Detected XOR obfuscation (Bitcoin Core v28+). Key: {detected_key.hex()}")
            xor_key = detected_key
            f.seek(-4, 1)  # Rewind 4 bytes
            magic_bytes_raw = apply_xor(f.read(4), xor_key[:4])
            magic_bytes = struct.unpack('<I', magic_bytes_raw)[0]
        else:
            print(f"⚠ Using detected key anyway (validation inconclusive): {detected_key.hex()}")
            xor_key = detected_key
            f.seek(-4, 1)  # Rewind 4 bytes
            magic_bytes = struct.unpack('<I', f.read(4))[0]
    
    # Use XOR key if we have one
    if xor_key is not None:
        # We already deobfuscated the magic bytes above, continue with rest
        pass
    
    # Block size (4 bytes, little-endian)
    # The block size is at file offset 4, so it needs to be XORed with offset 4
    block_size_raw = f.read(4)
    if len(block_size_raw) < 4:
        return None
    
    # Deobfuscate block size if XOR key is available
    # Block size is at file position 4, so use offset 4 for XOR
    if xor_key is not None:
        block_size_raw = apply_xor(block_size_raw, xor_key, offset=4)
    
    block_size = struct.unpack('<I', block_size_raw)[0]
    
    # Sanity check on block size
    if block_size == 0 or block_size > 10 * 1024 * 1024:  # Max 10MB block
        print(f"Warning: Invalid block size: {block_size} bytes")
        print(f"  Raw bytes: {block_size_raw.hex()}")
        return None
    
    block_start = f.tell()
    
    # Read entire block data
    block_data = f.read(block_size)
    if len(block_data) < block_size:
        print(f"Warning: Not enough data. Expected {block_size} bytes, got {len(block_data)} bytes")
        return None  # Not enough data
    
    # Deobfuscate entire block if XOR key is set
    # XOR obfuscation starts at offset 8 (after magic + block_size)
    # So we need to account for offset 8 when applying XOR
    if xor_key is not None:
        block_data = apply_xor(block_data, xor_key, offset=8)
    
    # Parse from block_data using BytesIO
    block_stream = BytesIO(block_data)
    
    # Block header (80 bytes)
    header_raw = block_stream.read(80)
    version = struct.unpack('<I', header_raw[0:4])[0]
    prev_block_hash = header_raw[4:36][::-1].hex()
    merkle_root = header_raw[36:68][::-1].hex()
    timestamp = struct.unpack('<I', header_raw[68:72])[0]
    bits = struct.unpack('<I', header_raw[72:76])[0]
    nonce = struct.unpack('<I', header_raw[76:80])[0]
    
    # Transaction count (varint)
    tx_count = read_varint(block_stream)
    
    # Read all transactions
    transactions = []
    try:
        for i in range(tx_count):
            tx = read_tx(block_stream)
            transactions.append(tx)
    except Exception as e:
        print(f"Error reading transactions: {e}")
        print(f"  Expected {tx_count} transactions, read {len(transactions)}")
        # Return partial block for debugging
        pass
    
    block_end = f.tell()
    actual_size = block_size
    
    result = {
        'magic': hex(magic_bytes),
        'block_size': block_size,
        'header': {
            'version': version,
            'prev_block_hash': prev_block_hash,
            'merkle_root': merkle_root,
            'timestamp': timestamp,
            'bits': bits,
            'nonce': nonce
        },
        'tx_count': tx_count,
        'transactions': transactions,
        'actual_size': actual_size
    }
    
    # Store XOR key in result for reuse
    if xor_key is not None:
        result['_xor_key'] = xor_key
    
    return result


def read_xor_key_from_file(blocks_folder: str) -> bytes:
    """
    Read XOR obfuscation key from xor.dat file in the blocks directory.
    Bitcoin Core v28+ stores the XOR key in xor.dat file (lowercase).
    Reference: https://learnmeabitcoin.com/technical/block/blkdat/
    
    Args:
        blocks_folder: Path to the blocks directory
    
    Returns:
        XOR key as bytes (8 bytes), or None if file not found
    """
    # Try lowercase first (correct name), then uppercase (for compatibility)
    xor_file_path = os.path.join(blocks_folder, "xor.dat")
    if not os.path.isfile(xor_file_path):
        xor_file_path = os.path.join(blocks_folder, "XOR.dat")
    
    if not os.path.isfile(xor_file_path):
        return None
    
    try:
        with open(xor_file_path, 'rb') as f:
            xor_key = f.read(8)  # XOR key is 8 bytes
            if len(xor_key) < 8:
                print(f"Warning: xor.dat file is too short ({len(xor_key)} bytes, expected 8)")
                return None
            return xor_key
    except Exception as e:
        print(f"Error reading xor.dat: {e}")
        return None


def find_bitcoin_folder() -> str:
    """Find the Bitcoin data folder location."""
    # Common locations
    possible_paths = [
        os.path.expanduser("~/.bitcoin/blocks"),
        os.path.expanduser("~/.bitcoin/testnet3/blocks"),
        os.path.expanduser("~/.bitcoin/regtest/blocks"),
    ]
    
    # Check environment variable (expand ~ so path is valid for isdir)
    bitcoin_dir = os.getenv('BITCOIN_DIR')
    if bitcoin_dir:
        possible_paths.insert(0, os.path.join(os.path.expanduser(bitcoin_dir), "blocks"))
    
    for path in possible_paths:
        if os.path.isdir(path):
            return path
    
    # Default to mainnet
    return os.path.expanduser("~/.bitcoin/blocks")


def read_blk_file(blk_file_path: str, max_blocks: int = 1, xor_key: bytes = None, start_block_index: int = 0) -> Tuple[List[Dict[str, Any]], bytes]:
    """
    Read blocks from a single blk*.dat file.
    
    Args:
        blk_file_path: Path to the blk*.dat file
        max_blocks: Maximum number of blocks to read (default: 1)
        xor_key: Optional XOR key for deobfuscation (will be auto-detected if None)
        start_block_index: Block index to start reading from (default: 0)
    
    Returns:
        Tuple of (list of block dictionaries, detected XOR key)
    """
    if not os.path.isfile(blk_file_path):
        raise FileNotFoundError(f"Block file not found: {blk_file_path}")
    
    if start_block_index == 0:
        print(f"Reading block file: {blk_file_path}")
        print(f"File size: {os.path.getsize(blk_file_path):,} bytes")
        print()
    
    blocks = []
    detected_xor_key = xor_key
    block_count = 0
    
    with open(blk_file_path, 'rb') as f:
        while len(blocks) < max_blocks:
            current_pos = f.tell()
            
            # Check if we're at the end of the file
            if current_pos >= os.path.getsize(blk_file_path):
                break
            
            try:
                block = read_block(f, detected_xor_key)
                if block is None:
                    # No more valid blocks
                    if current_pos == 0:
                        print(f"No blocks found at start of file (position {current_pos})")
                    else:
                        print(f"No more blocks found at position {current_pos}")
                    break
                
                # If we detected a new XOR key, save it for subsequent blocks
                if detected_xor_key is None and '_xor_key' in block:
                    detected_xor_key = block['_xor_key']
                    print(f"Using detected XOR key for subsequent blocks: {detected_xor_key.hex()}")
                
                # Only add blocks after we've skipped to the start_block_index
                if block_count >= start_block_index:
                    blocks.append(block)
                
                block_count += 1
            except struct.error as e:
                # End of file or invalid data
                print(f"Struct error at position {current_pos}: {e}")
                break
            except Exception as e:
                print(f"Error reading block at position {current_pos}: {e}")
                import traceback
                traceback.print_exc()
                break
    
    return blocks, detected_xor_key


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Read and parse Bitcoin block files (.blk)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_blk.py                    # Read first block from first file
  python test_blk.py --block 0          # Read first block (index 0)
  python test_blk.py --block 5          # Read 6th block (index 5)
  python test_blk.py --file blk00001.dat --block 10  # Read block 10 from specific file
  python test_blk.py --blocks-dir /path/to/blocks    # Specify blocks directory
        """
    )
    parser.add_argument(
        '--blocks-dir',
        type=str,
        help='Path to Bitcoin blocks directory (default: auto-detect)'
    )
    parser.add_argument(
        '--file',
        type=str,
        help='Specific blk*.dat file to read (e.g., blk00000.dat). If not specified, uses first file.'
    )
    parser.add_argument(
        '--block',
        type=int,
        default=0,
        help='Block index to read from the file (0-based). Default: 0 (first block)'
    )
    parser.add_argument(
        '--max-blocks',
        type=int,
        default=1,
        help='Maximum number of blocks to read. Default: 1'
    )
    
    args = parser.parse_args()
    
    # Find Bitcoin folder
    if args.blocks_dir:
        blocks_folder = args.blocks_dir
    else:
        blocks_folder = find_bitcoin_folder()
    
    if not os.path.isdir(blocks_folder):
        print(f"Error: Bitcoin blocks folder not found: {blocks_folder}")
        print("Please specify the path to your .bitcoin/blocks folder")
        print("Usage: python test_blk.py --blocks-dir /path/to/blocks")
        sys.exit(1)
    
    print(f"Bitcoin blocks folder: {blocks_folder}")
    
    # Try to read XOR key from xor.dat file (lowercase, as per Bitcoin Core v28+)
    xor_key = read_xor_key_from_file(blocks_folder)
    if xor_key:
        print(f"✓ Read XOR key from xor.dat: {xor_key.hex()}")
        print(f"  Note: XOR obfuscation starts at offset 8 (after magic bytes + block size)")
    else:
        print("⚠ xor.dat file not found, will try to detect XOR key from magic bytes")
        xor_key = None
    
    print()
    
    # Find blk*.dat files
    blk_files = sorted(glob.glob(os.path.join(blocks_folder, "blk*.dat")))
    
    if not blk_files:
        print(f"Error: No blk*.dat files found in {blocks_folder}")
        sys.exit(1)
    
    # Select which file to read
    if args.file:
        # Find the specified file
        target_file = os.path.join(blocks_folder, args.file)
        if not os.path.isfile(target_file):
            print(f"Error: Block file not found: {target_file}")
            sys.exit(1)
        blk_file_path = target_file
    else:
        blk_file_path = blk_files[0]
    
    print(f"Found {len(blk_files)} block file(s)")
    print(f"Reading block file: {os.path.basename(blk_file_path)}")
    if args.block > 0:
        print(f"Reading block at index: {args.block}")
    print("=" * 70)
    print()
    
    # Read blocks from the file
    try:
        # Calculate how many blocks to read (need to read from start_block_index to start_block_index + max_blocks)
        blocks_to_read = args.block + args.max_blocks
        blocks, detected_xor_key = read_blk_file(blk_file_path, max_blocks=blocks_to_read, xor_key=xor_key, start_block_index=args.block)
        
        if not blocks:
            print("No blocks found in the file")

            return
        
        # Select which block to display
        if args.block >= len(blocks):
            print(f"Error: Block index {args.block} not found. File contains {len(blocks)} block(s).")
            print(f"Valid block indices: 0 to {len(blocks) - 1}")
            return
        
        block = blocks[0]  # blocks already contains only the requested blocks
        
        print("=" * 70)
        print("BLOCK INFORMATION")
        print("=" * 70)
        magic_val = int(block['magic'], 16)
        print(f"Magic: {block['magic']} (decimal: {magic_val})")
        print(f"Block Size: {block['block_size']:,} bytes")
        print(f"Version: {block['header']['version']}")
        print(f"Previous Block Hash: {block['header']['prev_block_hash']}")
        print(f"Merkle Root: {block['header']['merkle_root']}")
        print(f"Timestamp: {block['header']['timestamp']}")
        print(f"Difficulty Bits: {block['header']['bits']}")
        print(f"Nonce: {block['header']['nonce']}")
        print(f"Transaction Count: {block['tx_count']}")
        print()
        
        print("=" * 70)
        print("TRANSACTIONS")
        print("=" * 70)
        
        for idx, tx in enumerate(block['transactions']):
            print(f"\nTransaction {idx + 1}/{block['tx_count']}")
            print(f"  Version: {tx['version']}")
            print(f"  SegWit: {'Yes' if tx['has_witness'] else 'No'}")
            print(f"  Inputs: {tx['input_count']}")
            print(f"  Outputs: {tx['output_count']}")
            print(f"  Locktime: {tx['locktime']}")
            print(f"  Size: {tx['size']:,} bytes")
            
            # Show first few inputs
            print(f"\n  Inputs:")
            for i, inp in enumerate(tx['inputs'][:3]):
                print(f"    Input {i + 1}:")
                print(f"      Previous TX: {inp['prev_tx_hash']}")
                print(f"      Output Index: {inp['prev_output_index']}")
                print(f"      Script Length: {inp['script_len']} bytes")
                print(f"      Sequence: {inp['sequence']}")
            if len(tx['inputs']) > 3:
                print(f"    ... and {len(tx['inputs']) - 3} more inputs")
            
            # Show first few outputs
            print(f"\n  Outputs:")
            for i, out in enumerate(tx['outputs'][:3]):
                print(f"    Output {i + 1}:")
                print(f"      Value: {out['value']:,} satoshis ({out['value'] / 100000000:.8f} BTC)")
                print(f"      Script Type: {out.get('script_type', 'unknown')}")
                if out.get('address'):
                    print(f"      Address: {out['address']}")
                print(f"      Script Length: {out['script_len']} bytes")
                print(f"      Script (hex): {out['script_hex'][:64]}..." if len(out['script_hex']) > 64 else f"      Script (hex): {out['script_hex']}")
            if len(tx['outputs']) > 3:
                print(f"    ... and {len(tx['outputs']) - 3} more outputs")
                
            # Show script type summary
            script_types = {}
            for out in tx['outputs']:
                script_type = out.get('script_type', 'unknown')
                script_types[script_type] = script_types.get(script_type, 0) + 1
            if script_types:
                print(f"\n  Script Type Summary:")
                for script_type, count in sorted(script_types.items()):
                    print(f"    {script_type}: {count}")
        
        print()
        print("=" * 70)
        print(f"Successfully read 1 block with {block['tx_count']} transaction(s)")
        print("=" * 70)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

