#!/usr/bin/env python3
"""
Test script to read a single Bitcoin block file (.blk) and extract transactions.
Gets the .bitcoin folder location and reads one blk*.dat file.
"""
import os
import sys
import struct
import glob
from typing import List, Dict, Any

# Bitcoin block file magic bytes
MAINNET_MAGIC = 0xf9beb4d9
TESTNET_MAGIC = 0x0b110907
REGTEST_MAGIC = 0xfabfb5da

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


def read_tx(f) -> Dict[str, Any]:
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


def read_block(f) -> Dict[str, Any]:
    """Read a block from the block file."""
    # Magic bytes (4 bytes)
    magic_bytes = struct.unpack('<I', f.read(4))[0]
    
    if magic_bytes not in [MAINNET_MAGIC, TESTNET_MAGIC, REGTEST_MAGIC]:
        return None
    
    # Block size (4 bytes, little-endian)
    block_size = struct.unpack('<I', f.read(4))[0]
    
    block_start = f.tell()
    
    # Block header (80 bytes)
    version = struct.unpack('<I', f.read(4))[0]
    prev_block_hash = f.read(32)[::-1].hex()
    merkle_root = f.read(32)[::-1].hex()
    timestamp = struct.unpack('<I', f.read(4))[0]
    bits = struct.unpack('<I', f.read(4))[0]
    nonce = struct.unpack('<I', f.read(4))[0]
    
    # Transaction count (varint)
    tx_count = read_varint(f)
    
    # Read all transactions
    transactions = []
    for i in range(tx_count):
        tx = read_tx(f)
        transactions.append(tx)
    
    block_end = f.tell()
    actual_size = block_end - block_start
    
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


def read_blk_file(blk_file_path: str, max_blocks: int = 1) -> List[Dict[str, Any]]:
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
                block = read_block(f)
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
        print(f"Magic: {block['magic']}")
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

