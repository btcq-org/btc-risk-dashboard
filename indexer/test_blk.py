#!/usr/bin/env python3
"""
Test script to read a single Bitcoin block file (.blk) and extract transactions.
Gets the .bitcoin folder location and reads one blk*.dat file.
"""
import os
import sys
import struct
import glob
from io import BytesIO
from typing import List, Dict, Any

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
        # Script
        script = f.read(script_len)
        # Sequence (4 bytes)
        sequence = struct.unpack('<I', f.read(4))[0]
        
        inputs.append({
            'prev_tx_hash': prev_tx_hash,
            'prev_output_index': prev_output_index,
            'script_len': script_len,
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
        
        outputs.append({
            'value': value,
            'script_len': script_len,
            'script_hex': script.hex()
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
    """
    # Expected magic bytes (mainnet, little-endian when read)
    expected_magic = struct.pack('<I', MAINNET_MAGIC_LE)
    
    # XOR the obfuscated bytes with expected to get the key
    key = bytes(a ^ b for a, b in zip(magic_bytes_raw, expected_magic))
    
    # The key is typically 8 bytes, so we repeat it
    # For the first 4 bytes, we have the key. For Bitcoin Core, the key is usually 8 bytes
    # We'll use the first 4 bytes and repeat them, or try to detect the full 8-byte key
    return key * 2  # Repeat to make 8 bytes


def apply_xor(data: bytes, key: bytes) -> bytes:
    """Apply XOR obfuscation/deobfuscation."""
    if len(key) == 0:
        return data
    return bytes(data[i] ^ key[i % len(key)] for i in range(len(data)))


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
    
    # If not valid and no XOR key provided, try to detect it
    if not valid_magic and xor_key is None:
        # Try to detect XOR key from magic bytes
        detected_key = detect_xor_key(magic_bytes_raw)
        # Try deobfuscating with detected key
        deobfuscated_magic = apply_xor(magic_bytes_raw, detected_key[:4])
        deobfuscated_value = struct.unpack('<I', deobfuscated_magic)[0]
        
        if deobfuscated_value in [MAINNET_MAGIC_LE, TESTNET_MAGIC_LE, REGTEST_MAGIC_LE]:
            print(f"Detected XOR obfuscation (Bitcoin Core v28+). Key: {detected_key.hex()}")
            xor_key = detected_key
            # Re-read and deobfuscate
            f.seek(-4, 1)  # Rewind 4 bytes
            magic_bytes_raw = apply_xor(f.read(4), xor_key[:4])
            magic_bytes = struct.unpack('<I', magic_bytes_raw)[0]
        else:
            print(f"Warning: Unexpected magic bytes: {magic_bytes} (0x{magic_bytes:08x})")
            print(f"  Raw bytes: {magic_bytes_raw.hex()}")
            print(f"  This might be obfuscated. Continuing anyway...")
    
    # Use XOR key if we have one
    if xor_key is not None:
        # We already deobfuscated the magic bytes above, continue with rest
        pass
    
    # Block size (4 bytes, little-endian)
    block_size_raw = f.read(4)
    if xor_key is not None:
        block_size_raw = apply_xor(block_size_raw, xor_key[:4])
    block_size = struct.unpack('<I', block_size_raw)[0]
    
    block_start = f.tell()
    
    # Read entire block data
    block_data = f.read(block_size)
    if len(block_data) < block_size:
        return None  # Not enough data
    
    # Deobfuscate entire block if XOR key is set
    if xor_key is not None:
        block_data = apply_xor(block_data, xor_key)
    
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
    for i in range(tx_count):
        tx = read_tx(block_stream)
        transactions.append(tx)
    
    block_end = f.tell()
    actual_size = block_size
    
    return {
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


def find_bitcoin_folder() -> str:
    """Find the Bitcoin data folder location."""
    # Common locations
    possible_paths = [
        os.path.expanduser("~/.bitcoin/blocks"),
        os.path.expanduser("~/.bitcoin/testnet3/blocks"),
        os.path.expanduser("~/.bitcoin/regtest/blocks"),
    ]
    
    # Check environment variable
    bitcoin_dir = os.getenv('BITCOIN_DIR')
    if bitcoin_dir:
        possible_paths.insert(0, os.path.join(bitcoin_dir, "blocks"))
    
    for path in possible_paths:
        if os.path.isdir(path):
            return path
    
    # Default to mainnet
    return os.path.expanduser("~/.bitcoin/blocks")


def read_blk_file(blk_file_path: str, max_blocks: int = 1, xor_key: bytes = None) -> List[Dict[str, Any]]:
    """
    Read blocks from a single blk*.dat file.
    
    Args:
        blk_file_path: Path to the blk*.dat file
        max_blocks: Maximum number of blocks to read (default: 1)
    
    Returns:
        List of block dictionaries
    """
    if not os.path.isfile(blk_file_path):
        raise FileNotFoundError(f"Block file not found: {blk_file_path}")
    
    print(f"Reading block file: {blk_file_path}")
    print(f"File size: {os.path.getsize(blk_file_path):,} bytes")
    print()
    
    blocks = []
    
    with open(blk_file_path, 'rb') as f:
        while len(blocks) < max_blocks:
            current_pos = f.tell()
            
            # Check if we're at the end of the file
            if current_pos >= os.path.getsize(blk_file_path):
                break
            
            try:
                block = read_block(f, xor_key)
                if block is None:
                    # No more valid blocks
                    break
                blocks.append(block)
            except struct.error:
                # End of file or invalid data
                break
            except Exception as e:
                print(f"Error reading block at position {current_pos}: {e}")
                break
    
    return blocks


def main():
    """Main entry point."""
    # Find Bitcoin folder
    blocks_folder = find_bitcoin_folder()
    
    if not os.path.isdir(blocks_folder):
        print(f"Error: Bitcoin blocks folder not found: {blocks_folder}")
        print("Please specify the path to your .bitcoin/blocks folder")
        print("Usage: python test_blk.py [blocks_folder_path]")
        sys.exit(1)
    
    print(f"Bitcoin blocks folder: {blocks_folder}")
    
    # Find blk*.dat files
    blk_files = sorted(glob.glob(os.path.join(blocks_folder, "blk*.dat")))
    
    if not blk_files:
        print(f"Error: No blk*.dat files found in {blocks_folder}")
        sys.exit(1)
    
    print(f"Found {len(blk_files)} block file(s)")
    print(f"Reading first block file: {os.path.basename(blk_files[0])}")
    print("=" * 70)
    print()
    
    # Read one block from the first file
    try:
        blocks = read_blk_file(blk_files[0], max_blocks=1)
        
        if not blocks:
            print("No blocks found in the file")
            return
        
        block = blocks[0]
        
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
                print(f"      Script Length: {out['script_len']} bytes")
                print(f"      Script (hex): {out['script_hex'][:64]}..." if len(out['script_hex']) > 64 else f"      Script (hex): {out['script_hex']}")
            if len(tx['outputs']) > 3:
                print(f"    ... and {len(tx['outputs']) - 3} more outputs")
        
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

