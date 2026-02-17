#!/usr/bin/env python3
"""
Address processing script - calculates addresses from UTXOs and checks for reuse.
Runs address.sql to create tables, then:
1. Calculates addresses from UTXOs table
2. Fetches blocks to check if addresses are reused (by looking at VINs)
3. Tracks all P2PK addresses until the latest block
4. Updates address_status with reuse information
5. Updates address_stats after address_status is complete
"""
import os
from typing import Set
from psycopg2.extras import execute_values, RealDictCursor
from pycoin.symbols.btc import network

from . import config
from . import db
from .main import rpc_call, rpc_batch_call, RPCBatchError
from .utils import detect_script_type, address_from_vout

DB_PAGE_ROWS = config.DB_PAGE_ROWS


def schema_init():
    """Initialize address tables by running address.sql"""
    import os
    schema_path = os.path.join(os.path.dirname(__file__), "db", "address.sql")
    
    db.init_pool()
    with db.get_db_cursor() as cur:
        # Check if address_status table exists (indicates schema is already initialized)
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'address_status'
            )
        """)
        table_exists = cur.fetchone()[0]
        
        if table_exists:
            print("Address schema already initialized (address_status table exists), skipping initialization.")
            return
        
        # Schema not initialized, run the SQL
        print("Initializing address database schema...")
        with open(schema_path, "r") as sf:
            schema_sql = sf.read()
        cur.execute(schema_sql)
        print("Address schema initialization completed.")


def calculate_addresses_from_utxos(return_total: bool = False):
    """
    Calculate addresses from UTXOs table and populate address_status.
    For each unique address, we get:
    - script_pub_type (from the most recent UTXO)
    - created_block (first appearance)
    - created_block_timestamp (first appearance timestamp)
    - balance_sat (current balance)
    
    Uses INSERT ... SELECT to process data entirely in the database without loading into RAM.
    
    Args:
        return_total: If True, return total_count from address_status table.
                     If False, return inserted_count (newly inserted rows).
    
    Returns:
        int: Either inserted_count or total_count based on return_total parameter
    """
    print("Calculating addresses from UTXOs table...")
    
    with db.get_db_cursor() as cur:
        # Insert/update address_status directly from UTXOs using a single query
        cur.execute("""
            INSERT INTO address_status (
                address,
                script_pub_type,
                reused,
                created_block,
                created_block_timestamp,
                balance_sat
            )
            SELECT 
                address,
                MAX(script_pub_type) AS script_pub_type,
                FALSE AS reused,
                MIN(created_block) AS created_block,
                MIN(created_block_timestamp) AS created_block_timestamp,
                SUM(CASE WHEN NOT spent THEN value_sat ELSE 0 END) AS balance_sat
            FROM utxos
            WHERE address IS NOT NULL AND address <> ''
            GROUP BY address
            ON CONFLICT (address) DO NOTHING
        """)
        
        # Get count of inserted rows using rowcount (more efficient than fetching all rows)
        inserted_count = cur.rowcount
        
        if return_total:
            # Get total count
            cur.execute("SELECT COUNT(*) FROM address_status")
            total_count = cur.fetchone()[0]
            print(f"Address status populated successfully. Inserted: {inserted_count}, Total addresses: {total_count}")
            return total_count
        else:
            print(f"Address status populated successfully. Inserted: {inserted_count} addresses")
            return inserted_count


def check_address_reuse_from_blocks(start_block: int, num_blocks: int = 10):
    """
    Fetch multiple blocks and check if addresses are reused by looking at VINs.
    Uses verbose=4 to get addresses directly from VINs, then updates address_status.
    Uses RPC batching with retry logic (up to 5 retries) similar to main.py.
    
    Args:
        start_block: The starting block height to check for address reuse
        num_blocks: Number of blocks to check (default: 100)
    
    Returns:
        int: Number of addresses marked as reused
    """
    print(f"Fetching blocks {start_block} to {start_block + num_blocks - 1} to check for address reuse...")
    
    # Collect all addresses from VINs (reused addresses)
    reused_addresses = set()
    
    # Process blocks in batches using RPC batching
    block_heights = list(range(start_block, start_block + num_blocks))
    
    if not block_heights:
        print("No blocks to process")
        return 0
    
    hash_results = {}
    block_results = {}
    
    try:
        # Batch getblockhash calls for all blocks
        hash_batch_requests = [("getblockhash", [height], height) for height in block_heights]
        hash_results = rpc_batch_call(hash_batch_requests, description=f"getblockhash {start_block}-{start_block + num_blocks - 1}")
        
        # Batch getblock calls for all block hashes (using verbose=4 to get addresses in VINs)
        block_batch_requests = [("getblock", [hash_results[height], 4], height) for height in block_heights if height in hash_results]
        block_results = rpc_batch_call(block_batch_requests, description=f"getblock {start_block}-{start_block + num_blocks - 1}")
        
    except RPCBatchError as e:
        print(f"Fatal RPC failure while fetching blocks {start_block}-{start_block + num_blocks - 1}: {e}")
        # Continue with whatever blocks we successfully fetched
        if not block_results:
            return 0
    
    # Process all successfully fetched blocks
    for block_height in block_heights:
        if block_height not in block_results:
            print(f"Block {block_height} not found or failed to fetch. Skipping...")
            continue
        
        block = block_results[block_height]
        if block is None:
            print(f"Block {block_height} returned None. Skipping...")
            continue
        
        # Process all transactions in the block
        for tx in block.get("tx", []):
            # Process vins (transaction inputs) to get addresses directly
            for vin in tx.get("vin", []):
                # Skip coinbase transactions
                if "coinbase" in vin:
                    continue
                
                # With verbose=4, VIN contains prevout with address information
                prevout = vin.get("prevout", {})
                if prevout:
                    # Get address from scriptPubKey.addresses or scriptPubKey.address
                    script_pub_key = prevout.get("scriptPubKey", {})
                    if script_pub_key:
                        addresses = script_pub_key.get("addresses", [])
                        address = script_pub_key.get("address")
                        script_hex = script_pub_key.get("hex")
                        if addresses and len(addresses) > 0:
                            reused_addresses.add(addresses[0])
                        elif address:
                            reused_addresses.add(address)
                        elif script_hex:
                            try:
                                paddress = network.address.for_script(bytes.fromhex(script_hex))
                                if paddress and str(paddress):
                                    reused_addresses.add(str(paddress))
                            except Exception:
                                # Skip if we can't parse the script
                                pass
    
    if not reused_addresses:
        print("No reused addresses found in the checked blocks")
        return 0
    
    print(f"Found {len(reused_addresses)} unique reused addresses across {num_blocks} block(s)")
    
    # Update address_status directly for addresses that exist
    with db.get_db_cursor() as cur:
        # Build a query to update address_status for reused addresses
        placeholders = ','.join(['%s'] * len(reused_addresses))
        update_sql = f"""
            UPDATE address_status
            SET reused = TRUE
            WHERE address IN ({placeholders})
            AND reused = FALSE
        """
        cur.execute(update_sql, list(reused_addresses))
        updated_count = cur.rowcount
    
    print(f"Updated {updated_count} addresses as reused in address_status")
    return updated_count


def _insert_p2pk_addresses_to_db(p2pk_addresses: dict):
    """
    Helper function to insert/update P2PK addresses in the p2pk_status table.
    All tracking (balance deltas, reuse status) is done in the p2pk_addresses dict.
    
    Args:
        p2pk_addresses: Dictionary mapping address -> (block_height, block_timestamp, script_pub_type, balance_delta_sat, is_reused)
                        balance_delta: positive from VOUTs, negative from VINs
                        is_reused: True if address appears in any VIN
    
    Returns:
        int: Number of addresses inserted/updated
    """
    if not p2pk_addresses:
        return 0
    
    with db.get_db_cursor() as cur:
        # Prepare data for bulk insert
        address_data = []
        for address, address_info in p2pk_addresses.items():
            if len(address_info) == 5:
                block_height, block_timestamp, script_type, balance_delta, is_reused = address_info
            elif len(address_info) == 4:
                # Backward compatibility: assume not reused
                block_height, block_timestamp, script_type, balance_delta = address_info
                is_reused = False
            else:
                # Backward compatibility: assume no balance delta and not reused
                block_height, block_timestamp, script_type = address_info
                balance_delta = 0
                is_reused = False
            
            # Include all required fields: address, script_pub_type, reused, created_block, created_block_timestamp, balance_sat
            address_data.append((address, script_type, is_reused, block_height, block_timestamp, balance_delta))
        
        # Single insert/update with all data
        execute_values(
            cur,
            """
            INSERT INTO p2pk_status (
                address,
                script_pub_type,
                reused,
                created_block,
                created_block_timestamp,
                balance_sat
            )
            VALUES %s
            ON CONFLICT (address) DO UPDATE SET
                script_pub_type = EXCLUDED.script_pub_type,
                reused = p2pk_status.reused OR EXCLUDED.reused,
                created_block = LEAST(p2pk_status.created_block, EXCLUDED.created_block),
                created_block_timestamp = LEAST(p2pk_status.created_block_timestamp, EXCLUDED.created_block_timestamp),
                balance_sat = p2pk_status.balance_sat + EXCLUDED.balance_sat
            """,
            address_data,
            template=None,
            page_size=DB_PAGE_ROWS
        )
        inserted_count = cur.rowcount
    
    return inserted_count


def track_p2pk_addresses_from_blocks(start_block: int, end_block: int, chunk_size: int = 10, insert_chunk_threshold: int = 1000):
    """
    Track all P2PK addresses by scanning blocks from start_block to end_block.
    Extracts P2PK addresses from VOUTs and ensures they're recorded in p2pk_status table.
    Accumulates addresses in memory and only inserts to SQL after processing a specified number of chunks.
    
    Args:
        start_block: The starting block height to scan
        end_block: The ending block height to scan (inclusive)
        chunk_size: Number of blocks to process in each batch (default: 10)
        insert_chunk_threshold: Number of chunks to process before inserting to SQL (default: 1000)
    
    Returns:
        int: Number of P2PK addresses found and tracked
    """
    print(f"Tracking P2PK addresses from blocks {start_block} to {end_block}...")
    print(f"Will insert to database after every {insert_chunk_threshold} chunks ({insert_chunk_threshold * chunk_size} blocks)")
    
    # Collect all P2PK addresses with their block information, balance delta, and reuse status
    # address -> (block_height, block_timestamp, script_pub_type, balance_delta_sat, is_reused)
    # balance_delta: positive from VOUTs, negative from VINs
    # is_reused: True if address appears in any VIN
    p2pk_addresses = {}
    
    current_block = start_block
    chunk_count = 0
    total_inserted = 0
    
    while current_block <= end_block:
        # Calculate how many blocks to process in this chunk
        num_blocks = min(chunk_size, end_block - current_block + 1)
        block_heights = list(range(current_block, current_block + num_blocks))
        
        if not block_heights:
            break
        
        hash_results = {}
        block_results = {}
        
        try:
            # Batch getblockhash calls for all blocks
            hash_batch_requests = [("getblockhash", [height], height) for height in block_heights]
            hash_results = rpc_batch_call(hash_batch_requests, description=f"getblockhash {current_block}-{current_block + num_blocks - 1}")
            
            # Batch getblock calls for all block hashes (using verbose=4 to get addresses in VOUTs)
            block_batch_requests = [("getblock", [hash_results[height], 4], height) for height in block_heights if height in hash_results]
            block_results = rpc_batch_call(block_batch_requests, description=f"getblock {current_block}-{current_block + num_blocks - 1}")
            
        except RPCBatchError as e:
            print(f"RPC failure while fetching blocks {current_block}-{current_block + num_blocks - 1}: {e}")
            # Continue with whatever blocks we successfully fetched
            if not block_results:
                current_block += num_blocks
                chunk_count += 1
                continue
        
        # Process all successfully fetched blocks
        for block_height in block_heights:
            if block_height not in block_results:
                print(f"Block {block_height} not found or failed to fetch. Skipping...")
                continue
            
            block = block_results[block_height]
            if block is None:
                print(f"Block {block_height} returned None. Skipping...")
                continue
            
            block_timestamp = block.get("time", 0)
            
            # Process all transactions in the block
            for tx in block.get("tx", []):
                # First, process vins (transaction inputs) to check for reused P2PK addresses and track spent amounts
                for vin in tx.get("vin", []):
                    # Skip coinbase transactions
                    if "coinbase" in vin:
                        continue
                    
                    # With verbose=4, VIN contains prevout with address information
                    prevout = vin.get("prevout", {})
                    if prevout:
                        script_pub_key = prevout.get("scriptPubKey", {})
                        if script_pub_key:
                            # Check if this is a P2PK script type
                            script_type = detect_script_type(script_pub_key)
                            if script_type == "P2PK":
                                # Extract address from the prevout
                                addresses = script_pub_key.get("addresses", [])
                                address = script_pub_key.get("address")
                                script_hex = script_pub_key.get("hex")
                                
                                p2pk_address = None
                                if addresses and len(addresses) > 0:
                                    p2pk_address = addresses[0]
                                elif address:
                                    p2pk_address = address
                                elif script_hex:
                                    try:
                                        paddress = network.address.for_script(bytes.fromhex(script_hex))
                                        if paddress:
                                            p2pk_address = str(paddress)
                                    except Exception:
                                        pass
                                
                                if p2pk_address:
                                    # Get the value from prevout (this is being spent, so negative)
                                    prevout_value = prevout.get("value", 0)
                                    # Convert from BTC to satoshis if needed
                                    if isinstance(prevout_value, float):
                                        prevout_value = int(prevout_value * 100000000)
                                    
                                    # Track in p2pk_addresses: subtract value (negative) and mark as reused
                                    if p2pk_address not in p2pk_addresses:
                                        # Address not seen before, initialize with negative value and reused=True
                                        # We don't have block info from VIN, so use current block
                                        p2pk_addresses[p2pk_address] = (block_height, block_timestamp, script_type, -prevout_value, True)
                                    else:
                                        # Address exists, subtract value and mark as reused
                                        existing_block, existing_timestamp, existing_type, existing_balance, existing_reused = p2pk_addresses[p2pk_address]
                                        new_balance = existing_balance - prevout_value
                                        p2pk_addresses[p2pk_address] = (existing_block, existing_timestamp, existing_type, new_balance, True)
                
                # Process vouts (transaction outputs) to find P2PK addresses and track values
                for vout in tx.get("vout", []):
                    script_pub_key = vout.get("scriptPubKey", {})
                    if not script_pub_key:
                        continue
                    
                    # Check if this is a P2PK script type
                    script_type = detect_script_type(script_pub_key)
                    if script_type != "P2PK":
                        continue
                    
                    # Get the value from this output
                    value_sat = vout.get("value", 0)
                    # Convert from BTC to satoshis if needed (Bitcoin Core RPC returns BTC)
                    if isinstance(value_sat, float):
                        value_sat = int(value_sat * 100000000)
                    
                    # Extract address from the scriptPubKey
                    address = address_from_vout(vout)
                    if not address:
                        # For P2PK, we might need to derive address from script hex
                        script_hex = script_pub_key.get("hex")
                        if script_hex:
                            try:
                                script_bytes = bytes.fromhex(script_hex)
                                paddress = network.address.for_script(script_bytes)
                                if paddress:
                                    address = str(paddress)
                            except Exception:
                                pass
                    
                    if address:
                        # Track the earliest block where this address appears and add value (positive from VOUT)
                        if address not in p2pk_addresses:
                            # New address, initialize with positive value and reused=False
                            p2pk_addresses[address] = (block_height, block_timestamp, script_type, value_sat, False)
                        else:
                            # Address exists, add value and preserve reused status
                            existing_block, existing_timestamp, existing_type, existing_balance, existing_reused = p2pk_addresses[address]
                            new_balance = existing_balance + value_sat
                            if block_height < existing_block:
                                # Earlier block found, update block info
                                p2pk_addresses[address] = (block_height, block_timestamp, script_type, new_balance, existing_reused)
                            else:
                                # Same or later block, just update balance
                                p2pk_addresses[address] = (existing_block, existing_timestamp, existing_type, new_balance, existing_reused)
        
        current_block += num_blocks
        chunk_count += 1
        
        # Insert to database after processing insert_chunk_threshold chunks
        if chunk_count >= insert_chunk_threshold:
            # Calculate stats for logging
            total_balance_delta = sum(info[3] if len(info) >= 4 else 0 for info in p2pk_addresses.values())
            reused_count = sum(1 for info in p2pk_addresses.values() if len(info) >= 5 and info[4])
            print(f"Processed {chunk_count} chunks ({chunk_count * chunk_size} blocks), inserting {len(p2pk_addresses)} P2PK addresses to p2pk_status table...")
            print(f"  Balance delta: {total_balance_delta:+,} satoshis, Reused addresses: {reused_count}")
            inserted = _insert_p2pk_addresses_to_db(p2pk_addresses)
            total_inserted += inserted
            print(f"Inserted/updated {inserted} P2PK addresses in p2pk_status. Total so far: {total_inserted}")
            # Clear the dictionary to free memory
            p2pk_addresses = {}
            chunk_count = 0
        elif len(p2pk_addresses) > 0 and len(p2pk_addresses) % 1000 == 0:
            print(f"Processed {chunk_count} chunks, found {len(p2pk_addresses)} P2PK addresses in memory so far...")
    
    # Insert any remaining addresses at the end
    if p2pk_addresses:
        total_balance_delta = sum(info[3] if len(info) >= 4 else 0 for info in p2pk_addresses.values())
        reused_count = sum(1 for info in p2pk_addresses.values() if len(info) >= 5 and info[4])
        print(f"Inserting remaining {len(p2pk_addresses)} P2PK addresses to p2pk_status table...")
        print(f"  Balance delta: {total_balance_delta:+,} satoshis, Reused addresses: {reused_count}")
        inserted = _insert_p2pk_addresses_to_db(p2pk_addresses)
        total_inserted += inserted
        print(f"Inserted/updated {inserted} P2PK addresses in p2pk_status. Total: {total_inserted}")
    
    if total_inserted == 0:
        print("No P2PK addresses found in the scanned blocks")
        return 0
    
    print(f"Total P2PK addresses tracked and inserted into p2pk_status: {total_inserted}")
    return total_inserted


def calculate_address_stats():
    """
    After address_status is complete, update address_stats with aggregated statistics.
    Groups by script_pub_type and calculates:
    - reused_sat: total balance of reused addresses
    - total_sat: total balance of all addresses
    - reused_count: count of reused addresses
    - count: total count of addresses
    """
    print("Updating address_stats from address_status...")
    
    with db.get_db_cursor() as cur:
        # Aggregate stats by script_pub_type
        cur.execute("""
            INSERT INTO address_stats (
                script_pub_type,
                reused_sat,
                total_sat,
                reused_count,
                count
            )
            SELECT 
                script_pub_type,
                SUM(CASE WHEN reused THEN balance_sat ELSE 0 END) AS reused_sat,
                SUM(balance_sat) AS total_sat,
                COUNT(*) FILTER (WHERE reused) AS reused_count,
                COUNT(*) AS count
            FROM address_status
            WHERE script_pub_type IS NOT NULL
            GROUP BY script_pub_type
            ON CONFLICT (script_pub_type) DO UPDATE SET
                reused_sat = EXCLUDED.reused_sat,
                total_sat = EXCLUDED.total_sat,
                reused_count = EXCLUDED.reused_count,
                count = EXCLUDED.count
        """)
        
        # Get summary
        cur.execute("""
            SELECT 
                script_pub_type,
                reused_count,
                count,
                reused_sat,
                total_sat
            FROM address_stats
            ORDER BY script_pub_type
        """)
        
        stats_rows = cur.fetchall()
        print(f"\nAddress stats updated:")
        print(f"{'Script Type':<15} {'Reused':<10} {'Total':<10} {'Reused Sat':<15} {'Total Sat':<15}")
        print("-" * 70)
        for row in stats_rows:
            script_type, reused_count, total_count, reused_sat, total_sat = row
            print(f"{script_type:<15} {reused_count:<10} {total_count:<10} {reused_sat:<15} {total_sat:<15}")


def get_latest_block_height():
    """Get the latest block height from the database"""
    try:
        with db.get_db_cursor() as cur:
            cur.execute("SELECT MAX(block_height) FROM block_log")
            row = cur.fetchone()
            return row[0] if row and row[0] is not None else None
    except Exception as e:
        print(f"Error getting latest block height: {e}")
        return None


def get_biggest_and_oldest_address():
    """
    Calculate and return the biggest address (by balance_sat) and oldest address (by created block).
    Also inserts these values into address_global_stats table as biggest_balance and oldest_address.
    
    Returns:
        dict: Dictionary with two keys:
            - 'biggest': dict with address, balance_sat, and other address details
            - 'oldest': dict with address, first_seen_block (created block), and other address details
            Returns None for each if no addresses found.
    """
    try:
        with db.get_db_cursor(cursor_factory=RealDictCursor) as cur:
            # Create the address_global_stats table if it doesn't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS address_global_stats (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    biggest_balance BIGINT,
                    oldest_address TEXT,
                    CHECK (id = 1)
                )
            """)
            
            # Get the biggest address (highest balance_sat)
            cur.execute("""
                SELECT 
                    address,
                    created_block,
                    created_block_timestamp,
                    balance_sat,
                    script_pub_type
                FROM address_status
                WHERE balance_sat > 0
                ORDER BY balance_sat DESC
                LIMIT 1
            """)
            biggest = cur.fetchone()
            
            # Get the oldest address (lowest created_block, which is the created block)
            cur.execute("""
                SELECT 
                    address,
                    created_block,
                    created_block_timestamp,
                    balance_sat,
                    script_pub_type
                FROM address_status
                ORDER BY created_block ASC
                LIMIT 1
            """)
            oldest = cur.fetchone()
            
            # Insert/update biggest_balance and oldest_address into address_global_stats
            if biggest or oldest:
                biggest_balance = biggest['balance_sat'] if biggest else None
                oldest_address = oldest['created_block'] if oldest else None
                
                cur.execute("""
                    INSERT INTO address_global_stats (id, biggest_balance, oldest_address)
                    VALUES (1, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        biggest_balance = EXCLUDED.biggest_balance,
                        oldest_address = EXCLUDED.oldest_address
                """, (biggest_balance, oldest_address))
            
            return {
                "biggest": dict(biggest) if biggest else None,
                "oldest": dict(oldest) if oldest else None
            }
    except Exception as e:
        print(f"Error getting biggest and oldest address: {e}")
        return {"biggest": None, "oldest": None}

def main():
    """Main function to process addresses"""
    print("=" * 70)
    print("Address Processing Script")
    print("=" * 70)
    
    # Initialize database pool
    db.init_pool()
    
    # Initialize schema (run address.sql)
    schema_init()
    
    # Step 1: Calculate addresses from UTXOs table
    calculate_addresses_from_utxos()
    
    # Step 2: Get the latest block height and check for address reuse
    latest_block = get_latest_block_height()
    if latest_block is not None:
        print(f"\nChecking address reuse in blocks from 1 to {latest_block}...")
        # Check all blocks from the start (block 1) until latest block in chunks
        chunk_size = 10
        start_block = 924000
        total_updated = 0
        
        while start_block <= latest_block:
            # Calculate how many blocks to process in this chunk
            num_blocks = min(chunk_size, latest_block - start_block + 1)
            print(f"\nProcessing blocks {start_block} to {start_block + num_blocks - 1}...")
            updated = check_address_reuse_from_blocks(start_block, num_blocks)
            total_updated += updated
            start_block += chunk_size
        
        print(f"\nTotal addresses marked as reused: {total_updated}")
    else:
        print("No blocks found in block_log. Skipping reuse check.")
    
    # Step 3: Track all P2PK addresses until the latest block
    latest_block = get_latest_block_height()
    if latest_block is not None:
        print(f"\nTracking all P2PK addresses from block 1 to {latest_block}...")
        # Track P2PK addresses from the beginning until latest block
        chunk_size = 10
        start_block = 1
        total_p2pk = track_p2pk_addresses_from_blocks(start_block, latest_block, chunk_size)
        print(f"\nTotal P2PK addresses tracked: {total_p2pk}")
    else:
        print("No blocks found in block_log. Skipping P2PK tracking.")
   
    # Step 4: Update address_stats after address_status is complete
    calculate_address_stats()

    get_biggest_and_oldest_address()
    
    print("\n" + "=" * 70)
    print("Address processing complete!")
    print("=" * 70)
    
    # Cleanup
    db.shutdown_pool()


if __name__ == "__main__":
    main()

