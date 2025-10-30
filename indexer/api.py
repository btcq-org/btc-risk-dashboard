from fastapi import FastAPI, HTTPException, Query
from . import db
from psycopg2.extras import RealDictCursor

app = FastAPI()


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
