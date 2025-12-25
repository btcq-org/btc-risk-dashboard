#!/usr/bin/env python3
"""
Read the first blocks from Bitcoin blockchain files using bitcoin-blockchain-parser.
This script reads blocks directly from the blockchain data files (blk*.dat) in the blocks folder.
"""
import os
import sys
import glob
import time
from blockchain_parser.blockchain import Blockchain

def read_test_blocks(blocks_folder_path):
    """
    Read and process the first 100 blocks from the blockchain files.
    
    Args:
        blocks_folder_path: Path to the directory containing blk*.dat files
                           (typically ~/.bitcoin/blocks/ for mainnet or ~/.bitcoin/testnet3/blocks/ for testnet)
    """
    if not os.path.isdir(blocks_folder_path):
        print(f"Error: Blocks folder not found: {blocks_folder_path}")
        sys.exit(1)
    
    print(f"Reading blocks from: {blocks_folder_path}")
    
    # Check how many blk*.dat files exist
    block_files = sorted(glob.glob(os.path.join(blocks_folder_path, "blk*.dat")))
    if not block_files:
        print(f"Error: No blk*.dat files found in {blocks_folder_path}")
        sys.exit(1)
    
    print(f"Found {len(block_files)} blockchain data file(s)")
    print("Initializing blockchain parser (this may take a moment for large blockchains)...")
    
    try:
        init_start = time.time()
        # Initialize the blockchain parser
        blockchain = Blockchain(blocks_folder_path)
        init_time = time.time() - init_start
        print(f"✓ Blockchain parser initialized in {init_time:.2f} seconds")
        
        print("Starting to read blocks (parsing files as we go)...")
        print("Note: The first block may take a moment as files are being scanned...")
        block_count = 0
        max_blocks = 100
        first_block_time = None
        last_progress_time = time.time()
        
        # Use a generator with progress feedback
        block_iterator = blockchain.get_unordered_blocks()
        
        # Show progress while waiting for first block
        print("Scanning blockchain files for blocks...", end="", flush=True)
        
        try:
            # Try to get the first block with timeout feedback
            import threading
            progress_active = True
            
            def show_progress():
                dots = 0
                while progress_active:
                    time.sleep(2)
                    if progress_active:
                        dots = (dots + 1) % 4
                        print(f"\rScanning blockchain files for blocks{'...'[:dots]:<3}", end="", flush=True)
            
            progress_thread = threading.Thread(target=show_progress, daemon=True)
            progress_thread.start()
            
            for block in block_iterator:
                progress_active = False
                print()  # New line after progress indicator
                
                if first_block_time is None:
                    first_block_time = time.time()
                    elapsed = first_block_time - init_time
                    print(f"✓ First block found after {elapsed:.2f} seconds")
                
                block_count += 1
                
                # Print block information
                print(f"\n--- Block {block_count} ---")
                print(f"Block Hash: {block.hash}")
                print(f"Version: {block.header.version}")
                print(f"Previous Block Hash: {block.header.previous_block_hash}")
                print(f"Merkle Root: {block.header.merkle_root}")
                print(f"Timestamp: {block.header.timestamp}")
                print(f"Difficulty: {block.header.difficulty}")
                print(f"Nonce: {block.header.nonce}")
                print(f"Number of Transactions: {len(block.transactions)}")
                
                # Print transaction count and some transaction details
                for idx, tx in enumerate(block.transactions):
                    if idx < 3:  # Show first 3 transactions
                        print(f"  TX {idx}: {tx.hash} ({len(tx.inputs)} inputs, {len(tx.outputs)} outputs)")
                
                if len(block.transactions) > 3:
                    print(f"  ... and {len(block.transactions) - 3} more transactions")
                
                # Stop after reading 100 blocks
                if block_count >= max_blocks:
                    print(f"\n✓ Successfully read {block_count} blocks")
                    break
        except StopIteration:
            progress_active = False
            print("\nNo more blocks found in the blockchain files.")
        except KeyboardInterrupt:
            progress_active = False
            print(f"\n\nInterrupted by user. Read {block_count} blocks before stopping.")
    except Exception as e:
        print(f"\nError reading blocks: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def main():
    """Main entry point."""
    # Default paths - adjust these based on your Bitcoin Core installation
    # For mainnet: typically ~/.bitcoin/blocks/
    # For testnet: typically ~/.bitcoin/testnet3/blocks/
    
    if len(sys.argv) > 1:
        blocks_folder = sys.argv[1]
    else:
        # Default to a common location - user should provide path
        blocks_folder = os.path.expanduser("~/.bitcoin/blocks")
        print(f"No path provided, using default: {blocks_folder}")
        print("Usage: python read_blocks.py <path_to_blocks_folder>")
        print("Example: python read_blocks.py ~/.bitcoin/blocks")
        print("         python read_blocks.py ~/.bitcoin/testnet3/blocks")
        print()
    
    read_test_blocks(blocks_folder)

if __name__ == "__main__":
    main()

