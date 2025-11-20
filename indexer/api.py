from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from . import db
from psycopg2.extras import RealDictCursor

app = FastAPI()

# Add CORS middleware to allow cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

@app.get("/stats/overview")
def stats_overview():
    try:
        with db.get_db_cursor(cursor_factory=RealDictCursor) as cur:
            # Get all stats from stats table
            cur.execute("SELECT key, value FROM stats")
            stats_rows = cur.fetchall()
            
            # Initialize stats dictionary
            stats_dict = {}
            for row in stats_rows:
                stats_dict[row["key"]] = int(row["value"])
            
            # Get total_utxo count
            utxo_count = stats_dict.get("total_utxo", 0)
            
            # Get all script type counts
            script_type_counts = {}
            script_types = ["P2TR", "P2WPKH", "P2PKH", "P2SH", "P2MS", "P2WSH", "P2PK", "P2PR", "unknown", "Bech32"]
            for script_type in script_types:
                key = f"{script_type}_count"
                if key in stats_dict:
                    script_type_counts[script_type] = stats_dict[key]

            # Get block stats directly from block_log table
            cur.execute("""
                WITH latest_block_info AS (
                    SELECT block_height, block_hash, scanned_at
                    FROM block_log
                    ORDER BY block_height DESC, scanned_at DESC
                    LIMIT 1
                )
                SELECT
                    (SELECT COUNT(*)::bigint FROM block_log) AS scanned_blocks,
                    (SELECT block_height FROM latest_block_info) AS latest_block_height,
                    (SELECT block_hash FROM latest_block_info) AS latest_block_hash,
                    (SELECT scanned_at FROM latest_block_info) AS latest_block_scanned_at
            """)
            block_stats_row = cur.fetchone()
            if block_stats_row and block_stats_row["latest_block_height"] is not None:
                scanned_blocks = int(block_stats_row["scanned_blocks"])
                latest_block = {
                    "height": int(block_stats_row["latest_block_height"]),
                    "hash": block_stats_row["latest_block_hash"],
                    "scanned_at": str(block_stats_row["latest_block_scanned_at"]),
                }
                latest_block_height = int(block_stats_row["latest_block_height"])
            else:
                scanned_blocks = 0
                latest_block = None
                latest_block_height = None

            # Get address count from the latest block
            if latest_block_height is not None:
                cur.execute("""
                    SELECT COUNT(DISTINCT address)::bigint AS address_count
                    FROM utxos
                    WHERE address IS NOT NULL 
                      AND address <> ''
                      AND created_block = %s
                """, (latest_block_height,))
                address_row = cur.fetchone()
                address_count = int(address_row["address_count"]) if address_row and address_row["address_count"] else 0
            else:
                address_count = 0

            return {
                "utxo_count": utxo_count,
                "address_count": address_count,
                "script_type_counts": script_type_counts,
                "scanned_blocks": scanned_blocks,
                "latest_block": latest_block,
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats/supply")
def stats_supply():
    """Return total supply and balance_sat for each script type from the stats table."""
    try:
        with db.get_db_cursor(cursor_factory=RealDictCursor) as cur:
            # Get all stats from stats table
            cur.execute("SELECT key, value FROM stats")
            stats_rows = cur.fetchall()
            
            # Initialize stats dictionary
            stats_dict = {}
            for row in stats_rows:
                stats_dict[row["key"]] = int(row["value"])
            
            # Get total supply (total_balance_sat)
            total_supply_sat = stats_dict.get("total_balance_sat", 0)
            
            # Get all script type balances (keys ending with _balance_sat, excluding total_balance_sat)
            script_type_balances = {}
            for key, value in stats_dict.items():
                if key.endswith("_balance_sat") and key != "total_balance_sat":
                    # Extract script type by removing "_balance_sat" suffix
                    script_type = key[:-12]  # Remove "_balance_sat" (13 characters)
                    script_type_balances[script_type] = value
            
            return {
                "total_supply_sat": total_supply_sat,
                "script_type_balances": script_type_balances,
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/search")
def search(q: str = Query(..., min_length=1)):
    results = []
    try:
        with db.get_db_cursor(cursor_factory=RealDictCursor) as cur:
            # Search for address - uses utxos_address_idx index
            # Optimized to use index on address column
            cur.execute("""
                SELECT
                    address,
                    MIN(created_block) AS first_seen_block,
                    MIN(created_block_timestamp) AS first_seen_block_timestamp,
                    GREATEST(
                        COALESCE(MAX(created_block), 0),
                        COALESCE(MAX(spent_block), 0)
                    ) AS last_seen_block,
                    GREATEST(
                        COALESCE(MAX(created_block_timestamp), 0),
                        COALESCE(MAX(spent_block_timestamp), 0)
                    ) AS last_seen_block_timestamp,
                    SUM(CASE WHEN NOT spent THEN value_sat ELSE 0 END) AS balance_sat,
                    COUNT(*) FILTER (WHERE NOT spent) AS utxo_count,
                    COUNT(*) AS appearances,
                    (SELECT script_pub_type 
                     FROM utxos u2 
                     WHERE u2.address = utxos.address 
                     ORDER BY CASE WHEN NOT u2.spent THEN 0 ELSE 1 END, 
                              u2.created_block DESC, 
                              u2.created_block_timestamp DESC 
                     LIMIT 1) AS script_pub_type
                FROM utxos
                WHERE address = %s
                GROUP BY address
            """, (q,))
            for row in cur.fetchall():
                row['source'] = 'address_status'
                results.append(row)

            # Search utxos for txid (as created transaction) - uses PRIMARY KEY index on (txid, vout)
            # This query uses the primary key index efficiently
            cur.execute("""
                SELECT * 
                FROM utxos 
                WHERE txid = %s
                ORDER BY vout ASC
            """, (q,))
            for row in cur.fetchall():
                row['source'] = 'utxos'
                results.append(row)

        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/latest/utxos")
def latest_utxos(
    limit: int = Query(20, ge=1, le=100),
    spent: bool = Query(None, description="Filter by spent status (true for spent, false for unspent, null for all)")
):
    """Return the most recent UTXOs created.
    Optionally filter by spent status.
    Ordered by created_block (most recently created first).
    """
    try:
        with db.get_db_cursor(cursor_factory=RealDictCursor) as cur:
            if spent is not None:
                cur.execute("""
                    SELECT *
                    FROM utxos
                    WHERE spent = %s
                    ORDER BY created_block DESC, created_block_timestamp DESC
                    LIMIT %s
                """, (spent, limit))
            else:
                cur.execute("""
                    SELECT *
                    FROM utxos
                    ORDER BY created_block DESC, created_block_timestamp DESC
                    LIMIT %s
                """, (limit,))
            rows = cur.fetchall()
        return {"results": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/latest/blocks")
def latest_blocks(limit: int = Query(20, ge=1, le=100)):
    """Return the most recent blocks scanned.
    """
    try:
        with db.get_db_cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM block_log
                ORDER BY block_height DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
        return {"results": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/latest/addresses")
def latest_addresses(limit: int = Query(150, ge=1, le=1000)):
    """Return the most recently seen addresses from latest UTXOs.
    Uses created_block index for efficient querying.
    """
    try:
        with db.get_db_cursor(cursor_factory=RealDictCursor) as cur:
            # Get a larger sample of latest UTXOs to ensure we have enough unique addresses
            sample_size = limit * 10
            cur.execute("""
                WITH latest_utxos AS (
                    SELECT address, created_block, created_block_timestamp
                    FROM utxos
                    WHERE address IS NOT NULL AND address <> ''
                    ORDER BY created_block DESC
                    LIMIT %s
                ),
                unique_addresses AS (
                    SELECT DISTINCT ON (address)
                        address,
                        created_block,
                        created_block_timestamp
                    FROM latest_utxos
                    ORDER BY address, created_block DESC, created_block_timestamp DESC
                    LIMIT %s
                )
                SELECT
                    ua.address,
                    ua.created_block,
                    ua.created_block_timestamp,
                    MIN(u.created_block) AS first_seen_block,
                    MIN(u.created_block_timestamp) AS first_seen_block_timestamp,
                    GREATEST(
                        COALESCE(MAX(u.created_block), 0),
                        COALESCE(MAX(u.spent_block), 0)
                    ) AS last_seen_block,
                    GREATEST(
                        COALESCE(MAX(u.created_block_timestamp), 0),
                        COALESCE(MAX(u.spent_block_timestamp), 0)
                    ) AS last_seen_block_timestamp,
                    SUM(CASE WHEN NOT u.spent THEN u.value_sat ELSE 0 END) AS balance_sat,
                    COUNT(*) FILTER (WHERE NOT u.spent) AS utxo_count,
                    COUNT(*) AS appearances,
                    (SELECT script_pub_type 
                     FROM utxos u2 
                     WHERE u2.address = ua.address 
                     ORDER BY CASE WHEN NOT u2.spent THEN 0 ELSE 1 END, 
                              u2.created_block DESC, 
                              u2.created_block_timestamp DESC 
                     LIMIT 1) AS script_pub_type
                FROM unique_addresses ua
                JOIN utxos u ON u.address = ua.address
                GROUP BY ua.address, ua.created_block, ua.created_block_timestamp
                ORDER BY ua.created_block DESC, ua.created_block_timestamp DESC
            """, (sample_size, limit))
            rows = cur.fetchall()
        return {"results": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/address")
def address_utxo_stats(q: str = Query(..., min_length=1)):
    """
    For the given address, return UTXO stats: total_utxos, spent_utxos, unspent_utxos, spent_value_sat, unspent_value_sat, total_value_sat.
    Only returns data for addresses having at least 2 UTXOs with both spent AND unspent present.
    """
    try:
        with db.get_db_cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                  address,
                  COUNT(*) AS total_utxos,
                  COUNT(*) FILTER (WHERE spent) AS spent_utxos,
                  COUNT(*) FILTER (WHERE NOT spent) AS unspent_utxos,
                  SUM(value_sat) FILTER (WHERE spent) AS spent_value_sat,
                  SUM(value_sat) FILTER (WHERE NOT spent) AS unspent_value_sat,
                  SUM(value_sat) AS total_value_sat
                FROM utxos
                WHERE address = %s
                GROUP BY address
                HAVING COUNT(*) >= 2
                   AND COUNT(*) FILTER (WHERE spent) >= 1
                   AND COUNT(*) FILTER (WHERE NOT spent) >= 1
                LIMIT 1;
            """, (q,))
            row = cur.fetchone()
            if row:
                return row
            else:
                raise HTTPException(status_code=404, detail=f"No stats found for address: {q}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
