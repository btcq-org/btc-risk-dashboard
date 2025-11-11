import psycopg2
from psycopg2.extras import execute_values
import time
import random
import secrets
import string
import io  # For COPY variant

def generate_fake_utxo(i):
    """Generate a single fake UTXO row as a tuple."""
    txid = secrets.token_hex(32)  # 64 hex chars
    vout = random.randint(0, 10)
    address = f"1{random.choice(string.ascii_uppercase)}" + ''.join(random.choices(string.ascii_uppercase + string.digits, k=33))
    value_sat = random.randint(1, 100_000_000)  # 1-100M satoshis
    script_pub_key_hex = secrets.token_hex(random.randint(20, 30))  # ~40-60 hex chars
    script_pub_type = random.choice(['p2pkh', 'p2sh', 'p2wpkh', 'p2wsh'])
    created_block = random.randint(800_000, 850_000)
    created_block_timestamp = random.randint(1_600_000_000, 1_700_000_000)  # Unix ts ~2020-2023
    spent = random.choice([True, False])
    
    spent_by_txid = None
    spent_vin = None
    spent_block = None
    spent_block_timestamp = None
    
    if spent:
        spent_by_txid = secrets.token_hex(32)
        spent_vin = random.randint(0, 5)
        spent_block = random.randint(created_block + 1, created_block + 1000)
        spent_block_timestamp = random.randint(created_block_timestamp + 3600, created_block_timestamp + 86400 * 7)
    
    return (txid, vout, address, value_sat, script_pub_key_hex, script_pub_type,
            created_block, created_block_timestamp, spent, spent_by_txid, spent_vin,
            spent_block, spent_block_timestamp)

# Connect and create table
conn = psycopg2.connect(database="postgres", user="indexer", password="password", host="localhost", port="5432")
cur = conn.cursor()

# Create table if not exists
create_table_sql = """
CREATE TABLE IF NOT EXISTS utxos (
    txid TEXT NOT NULL,
    vout INTEGER NOT NULL,
    address TEXT,
    value_sat BIGINT NOT NULL,
    script_pub_key_hex TEXT,
    script_pub_type TEXT,
    created_block BIGINT NOT NULL,
    created_block_timestamp BIGINT NOT NULL,
    spent BOOLEAN NOT NULL DEFAULT FALSE,
    spent_by_txid TEXT,
    spent_vin INTEGER,
    spent_block BIGINT,
    spent_block_timestamp BIGINT,
    PRIMARY KEY (txid, vout)
);
"""
cur.execute(create_table_sql)

# Generate data: 200K rows
print("Generating 200K fake rows...")
data = [generate_fake_utxo(i) for i in range(200_000)]

# Method 1: execute_values (recommended for simplicity/speed balance)
print("Inserting with execute_values...")
start_time = time.time()
execute_values(
    cur,
    """
    INSERT INTO utxos (
        txid, vout, address, value_sat, script_pub_key_hex, script_pub_type,
        created_block, created_block_timestamp, spent, spent_by_txid,
        spent_vin, spent_block, spent_block_timestamp
    ) VALUES %s
    """,
    data,
    page_size=1000  # Optimal for memory/speed
)
conn.commit()
end_time = time.time()
print(f"execute_values time: {end_time - start_time:.2f} seconds")