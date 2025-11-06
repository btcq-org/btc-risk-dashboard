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
            # Get pre-aggregated address stats from materialized view
            cur.execute("SELECT * FROM address_stats LIMIT 1")
            address_stats_row = cur.fetchone()
            if address_stats_row:
                address_count = int(address_stats_row["address_count"])
                high_risk_address_count = int(address_stats_row["high_risk_address_count"])
                medium_risk_address_count = int(address_stats_row["medium_risk_address_count"])
            else:
                address_count = 0
                high_risk_address_count = 0
                medium_risk_address_count = 0

            # Get pre-aggregated UTXO stats from materialized view
            cur.execute("SELECT * FROM utxo_stats LIMIT 1")
            utxo_stats_row = cur.fetchone()
            if utxo_stats_row:
                utxo_count = int(utxo_stats_row["total_utxos"])
                spent_utxos = int(utxo_stats_row["spent_utxos"])
                unspent_utxos = int(utxo_stats_row["unspent_utxos"])
                spent_value_sat = int(utxo_stats_row["spent_value_sat"])
                unspent_value_sat = int(utxo_stats_row["unspent_value_sat"])
                total_value_sat = int(utxo_stats_row["total_value_sat"])
            else:
                utxo_count = 0
                spent_utxos = 0
                unspent_utxos = 0
                spent_value_sat = 0
                unspent_value_sat = 0
                total_value_sat = 0

            # Get pre-aggregated block stats from materialized view
            cur.execute("SELECT * FROM block_stats LIMIT 1")
            block_stats_row = cur.fetchone()
            if block_stats_row and block_stats_row["latest_block_height"] is not None:
                scanned_blocks = int(block_stats_row["scanned_blocks"])
                latest_block = {
                    "height": int(block_stats_row["latest_block_height"]),
                    "hash": block_stats_row["latest_block_hash"],
                    "scanned_at": str(block_stats_row["latest_block_scanned_at"]),
                }
            else:
                scanned_blocks = 0
                latest_block = None

            return {
                "address_count": address_count,
                "utxo_count": utxo_count,
                "utxo_stats": {
                    "total_utxos": spent_utxos + unspent_utxos,
                    "spent_utxos": spent_utxos,
                    "unspent_utxos": unspent_utxos,
                    "spent_value_sat": spent_value_sat,
                    "unspent_value_sat": unspent_value_sat,
                    "total_value_sat": total_value_sat,
                },
                "scanned_blocks": scanned_blocks,
                "high_risk_address_count": high_risk_address_count,
                "medium_risk_address_count": medium_risk_address_count,
                "latest_block": latest_block,
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/search")
def search(q: str = Query(..., min_length=1)):
    results = []
    try:
        with db.get_db_cursor(cursor_factory=RealDictCursor) as cur:
            # Search address_status for address
            cur.execute("SELECT * FROM address_status WHERE address = %s", (q,))
            for row in cur.fetchall():
                row['source'] = 'address_status'
                results.append(row)

            # Search utxos for txid
            cur.execute("SELECT * FROM utxos WHERE txid = %s", (q,))
            for row in cur.fetchall():
                row['source'] = 'utxos'
                results.append(row)

        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/latest/utxos")
def latest_utxos(limit: int = Query(20, ge=1, le=100)):
    """Return the most recent UTXOs created.
    """
    try:
        with db.get_db_cursor(cursor_factory=RealDictCursor) as cur:
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
def latest_addresses(limit: int = Query(20, ge=1, le=100)):
    """Return the most recently added addresses from address_status.
    Ordered by first_seen_block (most recently first seen addresses first).
    """
    try:
        with db.get_db_cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM address_status
                ORDER BY first_seen_block DESC, first_seen_block_timestamp DESC
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
