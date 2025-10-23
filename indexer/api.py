from fastapi import FastAPI, HTTPException
from . import db
from psycopg2.extras import RealDictCursor

app = FastAPI()


@app.get("/stats/overview")
def stats_overview():
    try:
        with db.get_db_cursor(cursor_factory=RealDictCursor) as cur:
            # Get counts from all tables
            cur.execute("SELECT COUNT(*)::bigint AS tracked_out FROM quantum_exposed")
            out_row = cur.fetchone()
            tracked_out = int(out_row["tracked_out"]) if out_row else 0

            cur.execute("SELECT COUNT(*)::bigint AS tracked_in FROM quantum_revealed")
            in_row = cur.fetchone()
            tracked_in = int(in_row["tracked_in"]) if in_row else 0

            cur.execute("SELECT COUNT(*)::bigint AS scanned_blocks FROM quantum_scanned")
            sb_row = cur.fetchone()
            scanned_blocks = int(sb_row["scanned_blocks"]) if sb_row else 0

            # latest scanned block (height + hash)
            cur.execute("SELECT block_height, block_hash FROM quantum_scanned ORDER BY block_height DESC LIMIT 1")
            latest_row = cur.fetchone()
            if latest_row:
                latest_block = {"height": int(latest_row["block_height"]), "hash": latest_row["block_hash"]}
            else:
                latest_block = None

            total = tracked_out + tracked_in

            return {
                "tracked_out": tracked_out,
                "tracked_in": tracked_in,
                "scanned_blocks": scanned_blocks,
                "latest_block": latest_block,
                "total": total
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
