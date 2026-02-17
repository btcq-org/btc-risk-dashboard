#!/usr/bin/env python3
"""
Block reader abstraction for fetching Bitcoin blocks from different sources.
Supports both RPC and BLK file reading methods.
"""
import os
import glob
import hashlib
import struct
from typing import Dict, List, Protocol, Any, Optional
from io import BytesIO

# Import BLK file reading functions from test_blk.py
try:
    from .test_blk import (
        read_block, read_blk_file, read_xor_key_from_file, find_bitcoin_folder,
        read_varint, apply_xor
    )
except ImportError:
    # Fallback for direct execution
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from indexer.test_blk import (
        read_block, read_blk_file, read_xor_key_from_file, find_bitcoin_folder,
        read_varint, apply_xor
    )


class BlockReader(Protocol):
    """
    Protocol defining the interface for block readers.
    All block readers must implement fetch_blocks method.
    """
    def fetch_blocks(self, heights: List[int]) -> Dict[int, Dict[str, Any]]:
        """
        Fetch blocks for the given heights.
        
        Args:
            heights: List of block heights to fetch
        
        Returns:
            Dictionary mapping height -> block dict (in RPC format)
            Block dict should have: 'tx', 'time', and other RPC getblock fields
        
        Raises:
            Exception: If blocks cannot be fetched
        """
        ...


class RPCBlockReader:
    """
    Block reader that fetches blocks via Bitcoin Core RPC.
    Uses existing RPC infrastructure from main.py
    """
    
    def __init__(self, rpc_batch_call, rpc_call=None):
        """
        Initialize RPC block reader.
        
        Args:
            rpc_batch_call: Function for batch RPC calls (from main.py)
            rpc_call: Optional function for single RPC calls (for getblockcount, etc.)
        """
        self.rpc_batch_call = rpc_batch_call
        self.rpc_call = rpc_call
    
    def fetch_blocks(self, heights: List[int]) -> Dict[int, Dict[str, Any]]:
        """
        Fetch blocks via RPC batch calls.
        
        Args:
            heights: List of block heights to fetch
        
        Returns:
            Dictionary mapping height -> block dict (RPC format)
        """
        if not heights:
            return {}
        
        # First, get block hashes for all heights
        hash_batch_requests = [("getblockhash", [height], height) for height in heights]
        hash_results = self.rpc_batch_call(
            hash_batch_requests,
            description=f"getblockhash batch for {len(heights)} blocks"
        )
        
        # Then, get blocks using the hashes
        block_batch_requests = [
            ("getblock", [hash_results[height], 3], height)
            for height in heights
            if height in hash_results
        ]
        block_results = self.rpc_batch_call(
            block_batch_requests,
            description=f"getblock batch for {len(heights)} blocks"
        )
        
        # Return results with block hashes included
        result = {}
        for height in heights:
            if height in block_results:
                block = block_results[height]
                blockhash = hash_results.get(height, "")
                # Store blockhash in the block dict for consistency
                if block:
                    block['_blockhash'] = blockhash
                result[height] = block
        
        return result


def calculate_txid(tx_raw: bytes) -> str:
    """
    Calculate transaction ID (txid) from raw transaction bytes.
    txid is double SHA256 of the transaction (for non-SegWit) or 
    single SHA256 for SegWit transactions (wtxid).
    For our purposes, we use double SHA256 which matches RPC format.
    
    Args:
        tx_raw: Raw transaction bytes
    
    Returns:
        Transaction ID as hex string (reversed, as Bitcoin displays it)
    """
    # Double SHA256
    first_hash = hashlib.sha256(tx_raw).digest()
    second_hash = hashlib.sha256(first_hash).digest()
    # Return reversed (Bitcoin displays txid in reverse byte order)
    return second_hash[::-1].hex()


def calculate_block_hash(header_bytes: bytes) -> str:
    """
    Calculate block hash from block header bytes.
    Block hash is double SHA256 of the 80-byte header.
    
    Args:
        header_bytes: 80-byte block header
    
    Returns:
        Block hash as hex string (reversed, as Bitcoin displays it)
    """
    if len(header_bytes) != 80:
        raise ValueError(f"Block header must be 80 bytes, got {len(header_bytes)}")
    
    # Double SHA256
    first_hash = hashlib.sha256(header_bytes).digest()
    second_hash = hashlib.sha256(first_hash).digest()
    # Return reversed (Bitcoin displays block hash in reverse byte order)
    return second_hash[::-1].hex()


def serialize_tx_for_txid(tx: Dict[str, Any]) -> bytes:
    """
    Serialize a transaction from BLK format to calculate txid.
    This reconstructs the transaction in the format used for txid calculation.
    Note: For SegWit transactions, we need to exclude witness data for txid.
    
    Args:
        tx: Transaction dict from BLK format
    
    Returns:
        Serialized transaction bytes (without witness for txid calculation)
    """
    tx_bytes = BytesIO()
    
    # Version (4 bytes, little-endian)
    tx_bytes.write(struct.pack('<I', tx.get('version', 1)))
    
    # For txid, SegWit transactions exclude witness data
    # So we don't write the witness marker here
    
    # Input count (varint)
    input_count = tx.get('input_count', len(tx.get('inputs', [])))
    if input_count < 0xfd:
        tx_bytes.write(bytes([input_count]))
    elif input_count <= 0xffff:
        tx_bytes.write(bytes([0xfd]))
        tx_bytes.write(struct.pack('<H', input_count))
    elif input_count <= 0xffffffff:
        tx_bytes.write(bytes([0xfe]))
        tx_bytes.write(struct.pack('<I', input_count))
    else:
        tx_bytes.write(bytes([0xff]))
        tx_bytes.write(struct.pack('<Q', input_count))
    
    # Inputs
    for inp in tx.get('inputs', []):
        # Previous output hash (32 bytes, in internal byte order for serialization)
        prev_tx_hash_bytes = bytes.fromhex(inp.get('prev_tx_hash', '0' * 64))[::-1]
        tx_bytes.write(prev_tx_hash_bytes)
        # Previous output index (4 bytes, little-endian)
        tx_bytes.write(struct.pack('<I', inp.get('prev_output_index', 0)))
        # Script length (varint) and script (scriptSig) - required for correct txid
        script_hex = inp.get('script_hex', '')
        script_bytes = bytes.fromhex(script_hex) if script_hex else b''
        script_len = len(script_bytes)
        if script_len < 0xfd:
            tx_bytes.write(bytes([script_len]))
        elif script_len <= 0xffff:
            tx_bytes.write(bytes([0xfd]))
            tx_bytes.write(struct.pack('<H', script_len))
        elif script_len <= 0xffffffff:
            tx_bytes.write(bytes([0xfe]))
            tx_bytes.write(struct.pack('<I', script_len))
        else:
            tx_bytes.write(bytes([0xff]))
            tx_bytes.write(struct.pack('<Q', script_len))
        tx_bytes.write(script_bytes)
        # Sequence (4 bytes)
        tx_bytes.write(struct.pack('<I', inp.get('sequence', 0)))
    
    # Output count (varint)
    output_count = tx.get('output_count', len(tx.get('outputs', [])))
    if output_count < 0xfd:
        tx_bytes.write(bytes([output_count]))
    elif output_count <= 0xffff:
        tx_bytes.write(bytes([0xfd]))
        tx_bytes.write(struct.pack('<H', output_count))
    elif output_count <= 0xffffffff:
        tx_bytes.write(bytes([0xfe]))
        tx_bytes.write(struct.pack('<I', output_count))
    else:
        tx_bytes.write(bytes([0xff]))
        tx_bytes.write(struct.pack('<Q', output_count))
    
    # Outputs
    for out in tx.get('outputs', []):
        # Value (8 bytes, little-endian)
        tx_bytes.write(struct.pack('<Q', out.get('value', 0)))
        # Script length (varint)
        script_hex = out.get('script_hex', '')
        script_bytes = bytes.fromhex(script_hex) if script_hex else b''
        script_len = len(script_bytes)
        if script_len < 0xfd:
            tx_bytes.write(bytes([script_len]))
        elif script_len <= 0xffff:
            tx_bytes.write(bytes([0xfd]))
            tx_bytes.write(struct.pack('<H', script_len))
        elif script_len <= 0xffffffff:
            tx_bytes.write(bytes([0xfe]))
            tx_bytes.write(struct.pack('<I', script_len))
        else:
            tx_bytes.write(bytes([0xff]))
            tx_bytes.write(struct.pack('<Q', script_len))
        tx_bytes.write(script_bytes)
    
    # Locktime (4 bytes, little-endian)
    tx_bytes.write(struct.pack('<I', tx.get('locktime', 0)))
    
    return tx_bytes.getvalue()


def convert_blk_to_rpc_format(blk_block: Dict[str, Any], height: int) -> Dict[str, Any]:
    """
    Convert BLK file block format to RPC format compatible with _extract_block_data.
    
    Args:
        blk_block: Block dict from read_block() in test_blk.py format
        height: Block height
    
    Returns:
        Block dict in RPC format (compatible with getblock RPC response)
    """
    header = blk_block.get('header', {})
    timestamp = header.get('timestamp', 0)
    
    # Calculate block hash from header
    version = header.get('version', 0)
    prev_block_hash = bytes.fromhex(header.get('prev_block_hash', '0' * 64))[::-1]
    merkle_root = bytes.fromhex(header.get('merkle_root', '0' * 64))[::-1]
    timestamp_int = header.get('timestamp', 0)
    bits = header.get('bits', 0)
    nonce = header.get('nonce', 0)
    
    header_bytes = (
        struct.pack('<I', version) +
        prev_block_hash +
        merkle_root +
        struct.pack('<I', timestamp_int) +
        struct.pack('<I', bits) +
        struct.pack('<I', nonce)
    )
    blockhash = calculate_block_hash(header_bytes)
    
    # Convert transactions
    rpc_txs = []
    for tx in blk_block.get('transactions', []):
        # Calculate txid from serialized transaction
        tx_raw = serialize_tx_for_txid(tx)
        txid = calculate_txid(tx_raw)
        
        # Convert inputs to RPC format
        rpc_vins = []
        for inp in tx.get('inputs', []):
            # Check if this is a coinbase transaction (first input with special format)
            # In BLK format, coinbase has prev_tx_hash of all zeros
            prev_tx_hash = inp.get('prev_tx_hash', '')
            is_coinbase = (prev_tx_hash == '0' * 64 or prev_tx_hash == '')
            
            if is_coinbase:
                vin = {
                    'coinbase': inp.get('script_hex', ''),
                    'sequence': inp.get('sequence', 0)
                }
            else:
                vin = {
                    'txid': prev_tx_hash,
                    'vout': inp.get('prev_output_index', 0),
                    'scriptSig': {
                        'hex': inp.get('script_hex', '')
                    },
                    'sequence': inp.get('sequence', 0)
                }
            if 'witness' in inp:
                vin['txinwitness'] = inp['witness']
            rpc_vins.append(vin)
        
        # Convert outputs to RPC format
        rpc_vouts = []
        for out in tx.get('outputs', []):
            value_btc = out.get('value', 0) / 100_000_000  # Convert satoshis to BTC
            vout = {
                'value': value_btc,
                'n': len(rpc_vouts),
                'scriptPubKey': {
                    'hex': out.get('script_hex', ''),
                    'type': out.get('script_type', 'unknown')
                }
            }
            # Add address if available
            if out.get('address'):
                vout['scriptPubKey']['address'] = out.get('address')
                vout['scriptPubKey']['addresses'] = [out.get('address')]
            else:
                vout['scriptPubKey']['addresses'] = []
            rpc_vouts.append(vout)
        
        # Create RPC transaction format
        rpc_tx = {
            'txid': txid,
            'version': tx.get('version', 1),
            'vin': rpc_vins,
            'vout': rpc_vouts,
            'locktime': tx.get('locktime', 0)
        }
        
        rpc_txs.append(rpc_tx)
    
    # Create RPC block format
    rpc_block = {
        'hash': blockhash,
        'height': height,
        'version': version,
        'versionHex': f"{version:08x}",
        'merkleroot': header.get('merkle_root', ''),
        'time': timestamp,
        'mediantime': timestamp,
        'nonce': nonce,
        'bits': hex(bits),
        'difficulty': 0,  # Would need to calculate
        'chainwork': '',  # Would need to calculate
        'nTx': len(rpc_txs),
        'previousblockhash': header.get('prev_block_hash', ''),
        'nextblockhash': '',  # Not available
        'tx': rpc_txs
    }
    
    return rpc_block


class BLKFileReader:
    """
    Block reader that fetches blocks directly from Bitcoin Core .blk files.
    Uses a hybrid approach: gets block hashes via RPC (minimal), then finds
    blocks in BLK files by matching block hash.
    """
    
    def __init__(self, blocks_dir: Optional[str] = None, rpc_call=None, rpc_batch_call=None):
        """
        Initialize BLK file reader.
        
        Args:
            blocks_dir: Path to Bitcoin blocks directory. If None, auto-detects.
            rpc_call: Optional RPC function for getting block hashes (minimal RPC usage)
            rpc_batch_call: Optional RPC batch function for getting multiple block hashes
        """
        if blocks_dir:
            self.blocks_dir = blocks_dir
        else:
            self.blocks_dir = find_bitcoin_folder()
        
        if not os.path.isdir(self.blocks_dir):
            raise ValueError(f"Blocks directory not found: {self.blocks_dir}")
        
        # Read XOR key if available
        self.xor_key = read_xor_key_from_file(self.blocks_dir)
        if self.xor_key:
            print(f"BLK Reader: Loaded XOR key from xor.dat")
        
        # RPC functions for getting block hashes (minimal RPC usage)
        self.rpc_call = rpc_call
        self.rpc_batch_call = rpc_batch_call
        
        # Cache of blk files (will be populated on first use)
        self._blk_files = None
        # Cache: block_hash -> (file_path, block_index_in_file)
        self._block_cache = {}
    
    def _get_blk_files(self) -> List[str]:
        """Get sorted list of blk*.dat files."""
        if self._blk_files is None:
            self._blk_files = sorted(glob.glob(os.path.join(self.blocks_dir, "blk*.dat")))
        return self._blk_files
    
    def _find_block_by_hash(self, target_hash: str) -> Optional[Dict[str, Any]]:
        """
        Find a block in BLK files by matching block hash.
        
        Args:
            target_hash: Block hash to find (hex string, normal order)
        
        Returns:
            Block dict in BLK format, or None if not found
        """
        blk_files = self._get_blk_files()
        
        for blk_file in blk_files:
            try:
                # Read blocks from this file
                blocks, _ = read_blk_file(
                    blk_file,
                    max_blocks=10000,  # Read in batches
                    xor_key=self.xor_key,
                    start_block_index=0
                )
                
                for blk_block in blocks:
                    # Calculate block hash from header
                    header = blk_block.get('header', {})
                    version = header.get('version', 0)
                    prev_block_hash = bytes.fromhex(header.get('prev_block_hash', '0' * 64))[::-1]
                    merkle_root = bytes.fromhex(header.get('merkle_root', '0' * 64))[::-1]
                    timestamp_int = header.get('timestamp', 0)
                    bits = header.get('bits', 0)
                    nonce = header.get('nonce', 0)
                    
                    header_bytes = (
                        struct.pack('<I', version) +
                        prev_block_hash +
                        merkle_root +
                        struct.pack('<I', timestamp_int) +
                        struct.pack('<I', bits) +
                        struct.pack('<I', nonce)
                    )
                    block_hash = calculate_block_hash(header_bytes)
                    
                    # Compare hashes (both should be in normal display order)
                    if block_hash.lower() == target_hash.lower():
                        return blk_block
                        
            except Exception as e:
                print(f"Error scanning {blk_file} for block {target_hash}: {e}")
                continue
        
        return None
    
    def fetch_blocks(self, heights: List[int]) -> Dict[int, Dict[str, Any]]:
        """
        Fetch blocks from BLK files.
        Uses RPC minimally to get block hashes, then finds blocks in BLK files.
        
        Args:
            heights: List of block heights to fetch
        
        Returns:
            Dictionary mapping height -> block dict (RPC format)
        """
        if not heights:
            return {}
        
        result = {}
        
        # Step 1: Get block hashes via RPC (minimal RPC usage)
        if not self.rpc_batch_call:
            raise ValueError("BLKFileReader requires rpc_batch_call for getting block hashes")
        
        try:
            hash_batch_requests = [("getblockhash", [height], height) for height in heights]
            hash_results = self.rpc_batch_call(
                hash_batch_requests,
                description=f"getblockhash for BLK reader ({len(heights)} blocks)"
            )
        except Exception as e:
            raise RuntimeError(f"Failed to get block hashes via RPC: {e}")
        
        # Step 2: Find blocks in BLK files by matching hash
        blk_files = self._get_blk_files()
        if not blk_files:
            raise ValueError(f"No blk*.dat files found in {self.blocks_dir}")
        
        for height in heights:
            if height not in hash_results:
                print(f"Warning: Could not get hash for block {height}")
                continue
            
            target_hash = hash_results[height]
            
            # Find block in BLK files
            blk_block = self._find_block_by_hash(target_hash)
            
            if blk_block:
                # Convert BLK format to RPC format
                rpc_block = convert_blk_to_rpc_format(blk_block, height)
                rpc_block['hash'] = target_hash  # Ensure hash matches
                result[height] = rpc_block
            else:
                print(f"Warning: Block {height} (hash: {target_hash}) not found in BLK files")
        
        return result
