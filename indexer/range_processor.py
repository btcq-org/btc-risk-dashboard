#!/usr/bin/env python3
"""
Range processor for bootstrapping Bitcoin blocks.
Supports processing blocks in chunks using different block readers (RPC or BLK files).
"""
import time
from collections import defaultdict
from typing import Dict, List, Any, Callable, Optional

from . import db
from .block_data import _extract_block_data
from .utils import detect_script_type, address_from_vout
from .block_reader import BlockReader
from .test_blk import ShutdownRequested

# Import database functions from main.py
# These are standalone functions that don't create circular dependencies
try:
    from .main import (
        bulk_insert_utxos_copy,
        bulk_insert_block_log_copy,
        bulk_delete_utxos,
        update_address_status_with_vout_balance,
        update_address_status_with_vin_balance,
        prepare_stats_batch,
        upsert_stats,
        update_address_stats_incremental
    )
except ImportError:
    # Fallback if main.py is not available
    bulk_insert_utxos_copy = None
    bulk_insert_block_log_copy = None
    bulk_delete_utxos = None
    update_address_status_with_vout_balance = None
    update_address_status_with_vin_balance = None
    prepare_stats_batch = None
    upsert_stats = None
    update_address_stats_incremental = None


def process_range(
    start_height: int,
    end_height: int,
    block_reader: BlockReader,
    chunk_size: int = 10,
    shutdown_check: Optional[Callable[[], bool]] = None
):
    """
    Process a range of blocks using the specified block reader.
    
    Args:
        start_height: Starting block height
        end_height: Ending block height (inclusive)
        block_reader: BlockReader instance (RPCBlockReader or BLKFileReader)
        chunk_size: Number of blocks to process per chunk
        shutdown_check: Optional function to check if shutdown was requested
    """
    # Verify database functions are available
    if (bulk_insert_utxos_copy is None or bulk_insert_block_log_copy is None or
        bulk_delete_utxos is None or update_address_status_with_vout_balance is None or
        update_address_status_with_vin_balance is None or prepare_stats_batch is None or
        upsert_stats is None or update_address_stats_incremental is None):
        raise ImportError("Database functions not available. Ensure main.py is importable.")
    if start_height > end_height:
        return

    print(f"Processing blocks {start_height} → {end_height} in chunks of {chunk_size}")
    print(f"Using block reader: {type(block_reader).__name__}")

    def process_block_data(block, height, blockhash):
        """Process a single block's data and extract VOUTs (UTXOs) and VINs (spends)."""
        block_data = _extract_block_data(block, height, blockhash, shutdown_check)
        if block_data is None:
            print(f"Shutdown requested while preparing chunk data for block {height}")
            return {
                'vout_rows': [],
                'spend_rows': [],
                'block_info': None
            }
        return block_data

    for chunk_start in range(start_height, end_height + 1, chunk_size):
        if shutdown_check and shutdown_check():
            print("Shutdown requested during catch-up")
            break

        chunk_end = min(chunk_start + chunk_size - 1, end_height)
        print(f"Processing chunk {chunk_start} → {chunk_end}")

        out_lists = {'vout_rows': [], 'spend_rows': [], 'scanned': []}
        chunk_heights = list(range(chunk_start, chunk_end + 1))
        chunk_type_counts: Dict[str, int] = defaultdict(int)
        chunk_type_balances: Dict[str, int] = defaultdict(int)
        chunk_total_utxo = 0
        chunk_total_balance_sat = 0

        if not chunk_heights:
            continue

        fetch_start = time.time()

        try:
            # Use block reader to fetch blocks
            block_results = block_reader.fetch_blocks(chunk_heights)
        except ShutdownRequested:
            print("Shutdown requested while reading BLK files")
            break
        except Exception as e:
            print(f"Fatal error while fetching chunk {chunk_start}-{chunk_end}: {e}")
            raise

        for height in chunk_heights:
            if shutdown_check and shutdown_check():
                print(f"Shutdown requested while processing chunk {chunk_start}-{chunk_end}, stopping block accumulation")
                break

            block = block_results.get(height)

            if block is None:
                print(f"Warning: Block {height} not found by block reader")
                continue

            # Get block hash from block (RPC format has 'hash' field, or '_blockhash' from RPC reader)
            blockhash = block.get('hash') or block.get('_blockhash', '')

            try:
                block_data = process_block_data(block, height, blockhash)
            except Exception as e:
                raise RuntimeError(f"Failed to process block {height}: {e}") from e

            out_lists['vout_rows'].extend(block_data['vout_rows'])
            out_lists['spend_rows'].extend(block_data.get('spend_rows', []))
            if block_data['block_info']:
                out_lists['scanned'].append(block_data['block_info'])
            block_stats = block_data.get('stats', {})
            chunk_total_utxo += block_stats.get('total_utxo', 0)
            chunk_total_balance_sat += block_stats.get('total_balance_sat', 0)
            for script_type, count in block_stats.get('type_counts', {}).items():
                chunk_type_counts[script_type] += count
            for script_type, balance in block_stats.get('type_balances', {}).items():
                chunk_type_balances[script_type] += balance

        if shutdown_check and shutdown_check():
            break

        fetch_time = time.time() - fetch_start
        print(f"[Chunk {chunk_start}-{chunk_end}] Fetched {chunk_end - chunk_start + 1} blocks in {fetch_time:.3f}s")

        if shutdown_check and shutdown_check():
            print(f"Skipping database write for chunk {chunk_start}-{chunk_end} due to shutdown request")
            break

        try:
            db_start = time.time()
            print(f"[Chunk {chunk_start}-{chunk_end}] Starting database write...")
            with db.get_db_cursor() as cur:
                if shutdown_check and shutdown_check():
                    print(f"Shutdown requested before writing chunk {chunk_start}-{chunk_end} to database")
                    break

                if out_lists['vout_rows']:
                    vout_start = time.time()
                    vout_count = len(out_lists['vout_rows'])
                    print(f"[Chunk {chunk_start}-{chunk_end}] Inserting {vout_count} UTXOs ...")
                    bulk_insert_utxos_copy(cur, out_lists['vout_rows'])
                    update_address_status_with_vout_balance(cur, out_lists['vout_rows'])
                    vout_time = time.time() - vout_start
                    if vout_time > 0:
                        rows_per_sec = vout_count / vout_time
                        print(f"[Chunk {chunk_start}-{chunk_end}] Inserted {vout_count} UTXOs in {vout_time:.3f}s ({rows_per_sec:.0f} rows/sec)")

                # Delete UTXOs that have been spent
                deleted_balance_sat = 0
                deleted_type_balances: Dict[str, int] = defaultdict(int)
                deleted_type_counts: Dict[str, int] = defaultdict(int)
                deleted_utxos = []

                if out_lists['spend_rows']:
                    spend_start = time.time()
                    spend_count = len(out_lists['spend_rows'])
                    print(f"[Chunk {chunk_start}-{chunk_end}] Deleting {spend_count} spent UTXOs...")
                    deleted_utxos = bulk_delete_utxos(cur, out_lists['spend_rows'])

                    # Extract max block height and timestamp from spend_rows for new address creation
                    # spend_rows structure: (spent_by_txid, spent_vin, spent_block, spent_block_timestamp, txid, vout)
                    max_height = max(row[2] for row in out_lists['spend_rows']) if out_lists['spend_rows'] else None
                    max_block_ts = max(row[3] for row in out_lists['spend_rows']) if out_lists['spend_rows'] else None

                    # Update address_status to decrement balances for spent UTXOs
                    update_address_status_with_vin_balance(cur, deleted_utxos, height=max_height, block_ts=max_block_ts)

                    # Track balance and counts for deleted UTXOs
                    for address, value_sat, script_type in deleted_utxos:
                        deleted_balance_sat += value_sat
                        deleted_type_balances[script_type] += value_sat
                        deleted_type_counts[script_type] += 1
                    spend_time = time.time() - spend_start
                    if spend_time > 0:
                        rows_per_sec = spend_count / spend_time
                        print(f"[Chunk {chunk_start}-{chunk_end}] Deleted {spend_count} spent UTXOs in {spend_time:.3f}s ({rows_per_sec:.0f} rows/sec)")

                if out_lists['scanned']:
                    block_log_start = time.time()
                    block_count = len(out_lists['scanned'])
                    block_vals = [(b['block_height'], b['block_hash'], b['block_timestamp']) for b in out_lists['scanned']]
                    print(f"[Chunk {chunk_start}-{chunk_end}] Inserting {block_count} block_log entries using COPY...")
                    bulk_insert_block_log_copy(cur, block_vals)
                    block_log_time = time.time() - block_log_start
                    print(f"[Chunk {chunk_start}-{chunk_end}] Inserted {block_count} block_log entries in {block_log_time:.3f}s")

                # Calculate net changes for stats (additions - deletions)
                net_total_utxo = chunk_total_utxo - sum(deleted_type_counts.values())
                net_total_balance_sat = chunk_total_balance_sat - deleted_balance_sat
                all_type_keys = set(chunk_type_counts.keys()) | set(deleted_type_counts.keys())
                net_type_counts = {k: chunk_type_counts.get(k, 0) - deleted_type_counts.get(k, 0) for k in all_type_keys}
                all_balance_keys = set(chunk_type_balances.keys()) | set(deleted_type_balances.keys())
                net_type_balances = {k: chunk_type_balances.get(k, 0) - deleted_type_balances.get(k, 0) for k in all_balance_keys}
                # Remove zero values
                net_type_counts = {k: v for k, v in net_type_counts.items() if v != 0}
                net_type_balances = {k: v for k, v in net_type_balances.items() if v != 0}

                stats_batch = prepare_stats_batch(net_total_utxo, net_type_counts, net_total_balance_sat, net_type_balances)
                upsert_stats(cur, stats_batch)

                # Update address_stats table for affected script types
                update_address_stats_incremental(cur, out_lists['vout_rows'], deleted_utxos)

            db_time = time.time() - db_start
            print(f"[Chunk {chunk_start}-{chunk_end}] Database write completed in {db_time:.3f}s")
        except Exception as e:
            print(f"Error writing chunk {chunk_start}-{chunk_end} to PostgreSQL: {e}")
        print(f"Saved and flushed chunk {chunk_start}–{chunk_end}")
