"""
Central configuration for the btcq-indexer project.
All settings can be overridden via environment variables.
See project root .env.example for a full list of variables.
"""
import os

# ---------------------------------------------------------------------------
# Bitcoin / RPC
# ---------------------------------------------------------------------------
NETWORK = os.getenv("NETWORK", "mainnet")  # 'mainnet' | 'testnet' | 'regtest'

RPC_USER = os.getenv("RPC_USER", "admin")
RPC_PASSWORD = os.getenv("RPC_PASSWORD", "pass")
RPC_HOST = os.getenv("RPC_HOST", "127.0.0.1")
RPC_PORT = int(os.getenv("RPC_PORT", "18332"))
RPC_URL = f"http://{RPC_HOST}:{RPC_PORT}/"

# Block source: 'rpc' (Bitcoin Core RPC) or 'blk' (read from blk*.dat files)
BLOCK_SOURCE = os.getenv("BLOCK_SOURCE", "blk").lower()
# Path to blocks directory (only when BLOCK_SOURCE=blk). e.g. /path/to/data-bitcoin/blocks
BLOCKS_DIR = os.path.expanduser(os.getenv("BLOCKS_DIR", "~/data-bitcoin/blocks"))
# Optional: Bitcoin data dir for auto-detecting blocks (e.g. ~/.bitcoin or /path/to/data-bitcoin)
BITCOIN_DIR = os.path.expanduser(os.getenv("BITCOIN_DIR", "~/data-bitcoin"))
# Optional: path to block index (LevelDB). Default: BLOCKS_DIR/index. If Bitcoin Core is running
# it holds the lock; use a copy: stop Core, cp -r blocks/index /path/to/index-copy, start Core,
# then set BLOCK_INDEX_DIR=/path/to/index-copy (or leave unset to scan blk files).
BLOCK_INDEX_DIR = os.path.expanduser(os.getenv("BLOCK_INDEX_DIR", "./data/index").strip()) or None

# ---------------------------------------------------------------------------
# Database (PostgreSQL / TimescaleDB)
# ---------------------------------------------------------------------------
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "indexer")
DB_PASS = os.getenv("DB_PASS", "password")

# ---------------------------------------------------------------------------
# Indexer tuning
# ---------------------------------------------------------------------------
# Blocks per catch-up chunk when using process_range (RPC mode)
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10"))
# Blocks per chunk when using BLK file reader (read N blocks from blk*.dat, then flush to DB)
BLK_CHUNK_SIZE = int(os.getenv("BLK_CHUNK_SIZE", "10"))
# Max RPC retries for failed requests
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
# Rows per DB insert page (execute_values / COPY)
DB_PAGE_ROWS = int(os.getenv("DB_PAGE_ROWS", "1000"))
# Seconds between polling for new blocks in continuous sync
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1.0"))
# Seconds to wait after RPC connection error before retry
RECONNECT_DELAY = float(os.getenv("RECONNECT_DELAY", "5.0"))

# ---------------------------------------------------------------------------
# Indexer flags
# ---------------------------------------------------------------------------
# Index UTXOs (True) or full TXOs (False)
IS_UTXO = bool(int(os.getenv("IS_UTXO", "0")))
# Recalculate stats from DB and exit (no block processing)
IS_RECALC = bool(int(os.getenv("IS_RECALC", "0")))