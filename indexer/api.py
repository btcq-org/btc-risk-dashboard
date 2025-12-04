from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from . import db
from psycopg2.extras import RealDictCursor
from typing import Optional

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


@app.get("/address/status")
def address_status_list(
    script_type: Optional[str] = Query(None, description="Filter by script_pub_type"),
    reused: Optional[bool] = Query(None, description="Filter by reused flag"),
    address: Optional[str] = Query(None, description="Exact address to fetch"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Return address_status rows with optional filtering."""
    try:
        with db.get_db_cursor(cursor_factory=RealDictCursor) as cur:
            where_clauses = []
            params = []

            if address:
                where_clauses.append("address = %s")
                params.append(address)

            if script_type:
                where_clauses.append("script_pub_type = %s")
                params.append(script_type)
            if reused is not None:
                where_clauses.append("reused = %s")
                params.append(reused)

            where_sql = ""
            if where_clauses:
                where_sql = " WHERE " + " AND ".join(where_clauses)

            count_sql = f"SELECT COUNT(*)::bigint AS total FROM address_status{where_sql}"
            cur.execute(count_sql, params)
            total = int(cur.fetchone()["total"])

            query_sql = f"""
                SELECT
                    address,
                    script_pub_type,
                    reused,
                    created_block,
                    created_block_timestamp,
                    balance_sat
                FROM address_status
                {where_sql}
                ORDER BY balance_sat DESC
                LIMIT %s OFFSET %s
            """
            query_params = params + [limit, offset]
            cur.execute(query_sql, query_params)
            rows = cur.fetchall()

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "results": rows,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/address/stats")
def address_stats():
    """Return aggregated address stats from address_stats table."""
    try:
        with db.get_db_cursor(cursor_factory=RealDictCursor) as cur:
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
            rows = cur.fetchall()

        return {"results": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/search")
def search(q: str = Query(..., min_length=1)):
    results = []
    try:
        with db.get_db_cursor(cursor_factory=RealDictCursor) as cur:
            # Get global stats for risk score calculation from address_global_stats
            cur.execute("""
                SELECT biggest_balance
                FROM address_global_stats
                WHERE id = 1
            """)
            global_stats = cur.fetchone()
            max_balance_sat = global_stats['biggest_balance'] if global_stats and global_stats['biggest_balance'] else 0
            
            # Set newest block as constant for age score calculation
            newest_block = 940000

            # Search for address - uses address_status table
            # Optimized to use primary key index on address column
            # Includes UTXO count from utxos table
            cur.execute("""
                SELECT
                    as_table.address,
                    as_table.script_pub_type,
                    as_table.reused,
                    as_table.created_block,
                    as_table.created_block_timestamp,
                    as_table.balance_sat,
                    COUNT(u.txid) AS utxo_count
                FROM address_status as_table
                LEFT JOIN utxos u ON u.address = as_table.address
                WHERE as_table.address = %s
                GROUP BY as_table.address, as_table.script_pub_type, as_table.reused, 
                         as_table.created_block, as_table.created_block_timestamp, as_table.balance_sat
            """, (q,))
            for row in cur.fetchall():
                row['source'] = 'address_status'
                
                # Calculate address risk score
                address_balance = row['balance_sat'] or 0
                address_block = row['created_block'] or 0
                
                # Balance score: biggest address = 10
                if max_balance_sat > 0:
                    balance_score = min(10.0, (address_balance / max_balance_sat) * 10.0)
                else:
                    balance_score = 0.0
                
                # Age score: based on address block relative to newest block
                # Formula: (address_block / newest_block) * 10
                age_score = (1 - (address_block / newest_block)) * 10.0
                age_score = max(0.0, min(10.0, age_score))  # Clamp between 0 and 10
                
                # Combined risk score: average of balance and age (0-10)
                risk_score = (balance_score + age_score) / 2.0
                risk_score = max(0.0, min(10.0, risk_score))  # Clamp between 0 and 10
                
                row['balance_risk_score'] = round(balance_score, 2)
                row['age_risk_score'] = round(age_score, 2)
                row['risk_score'] = round(risk_score, 2)
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
    """Return the most recently seen addresses from address_status table.
    Uses created_block index for efficient querying.
    """
    try:
        with db.get_db_cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    address,
                    script_pub_type,
                    reused,
                    created_block,
                    created_block_timestamp::text AS created_block_timestamp,
                    balance_sat
                FROM address_status
                ORDER BY created_block DESC, created_block_timestamp DESC
                LIMIT %s
            """, (limit,))
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
