#!/usr/bin/env python3
"""
Address processing script - calculates addresses from UTXOs and checks for reuse.
Runs address.sql to create tables, then:
1. Calculates addresses from UTXOs table
2. Fetches 1 block to check if addresses are reused (by looking at VINs)
3. Updates address_status with reuse information
4. Updates address_stats after address_status is complete
"""
import os
from typing import Set
from psycopg2.extras import execute_values, RealDictCursor
from pycoin.symbols.btc import network

from . import db
from .main import rpc_call, rpc_batch_call, RPCBatchError

# ========================
# CONFIGURATION
# ========================
DB_PAGE_ROWS = int(os.getenv('DB_PAGE_ROWS', '1000'))


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
    # calculate_addresses_from_utxos()
    
    # Step 2: Get the latest block height and check for address reuse
    # latest_block = get_latest_block_height()
    # if latest_block is not None:
    #     print(f"\nChecking address reuse in blocks from 1 to {latest_block}...")
    #     # Check all blocks from the start (block 1) until latest block in chunks
    #     chunk_size = 10
    #     start_block = 924000
    #     total_updated = 0
        
    #     while start_block <= latest_block:
    #         # Calculate how many blocks to process in this chunk
    #         num_blocks = min(chunk_size, latest_block - start_block + 1)
    #         print(f"\nProcessing blocks {start_block} to {start_block + num_blocks - 1}...")
    #         updated = check_address_reuse_from_blocks(start_block, num_blocks)
    #         total_updated += updated
    #         start_block += chunk_size
        
    #     print(f"\nTotal addresses marked as reused: {total_updated}")
    # else:
    #     print("No blocks found in block_log. Skipping reuse check.")
   
    # Step 3: Update address_stats after address_status is complete
    # calculate_address_stats()

    get_biggest_and_oldest_address()
    
    print("\n" + "=" * 70)
    print("Address processing complete!")
    print("=" * 70)
    
    # Cleanup
    db.shutdown_pool()


if __name__ == "__main__":
    main()

