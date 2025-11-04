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
            # Get counts from all tables
            cur.execute("SELECT COUNT(*)::bigint AS address_count FROM address_status")
            addr_row = cur.fetchone()
            address_count = int(addr_row["address_count"]) if addr_row else 0

            cur.execute("SELECT COUNT(*)::bigint AS utxo_count FROM utxos")
            utxo_row = cur.fetchone()
            utxo_count = int(utxo_row["utxo_count"]) if utxo_row else 0

            cur.execute("SELECT COUNT(*)::bigint AS scanned_blocks FROM block_log")
            sb_row = cur.fetchone()
            scanned_blocks = int(sb_row["scanned_blocks"]) if sb_row else 0

            # Risk classification based on script types
            high_risk_types = ("P2PK", "P2MS", "P2PR")
            medium_risk_types = ("P2PKH", "P2SH", "P2WPKH", "P2WSH")

            cur.execute(
                """
                SELECT COUNT(*)::bigint AS cnt
                FROM address_status
                WHERE script_pub_type = ANY(%s)
                """,
                (list(high_risk_types),),
            )
            hr_row = cur.fetchone()
            high_risk_address_count = int(hr_row["cnt"]) if hr_row else 0

            cur.execute(
                """
                SELECT COUNT(*)::bigint AS cnt
                FROM address_status
                WHERE script_pub_type = ANY(%s)
                """,
                (list(medium_risk_types),),
            )
            mr_row = cur.fetchone()
            medium_risk_address_count = int(mr_row["cnt"]) if mr_row else 0

            # latest scanned block (height, hash, scanned_at)
            cur.execute("""
                SELECT block_height, block_hash, scanned_at
                FROM block_log
                ORDER BY block_height DESC LIMIT 1
            """)
            latest_row = cur.fetchone()
            if latest_row:
                latest_block = {
                    "height": int(latest_row["block_height"]),
                    "hash": latest_row["block_hash"],
                    "scanned_at": str(latest_row["scanned_at"]),
                }
            else:
                latest_block = None

            return {
                "address_count": address_count,
                "utxo_count": utxo_count,
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
