"""Shared database connection handling for quantum-at-risk indexer."""

from contextlib import contextmanager
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor, execute_values

from . import config

DB_HOST = config.DB_HOST
DB_PORT = config.DB_PORT
DB_NAME = config.DB_NAME
DB_USER = config.DB_USER
DB_PASS = config.DB_PASS

# Connection pool (min=1, max=20 connections)
_pool = None

def init_pool():
    """Initialize the connection pool. Call this once at startup."""
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(
            minconn=1,
            maxconn=20,
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )

def shutdown_pool():
    """Close all pool connections. Call before exit."""
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None

@contextmanager
def get_db_cursor(cursor_factory=None):
    """Context manager for getting a DB cursor from the pool.
    Ensures the pool is initialized before use.
    Usage:
        with get_db_cursor() as cur:
            cur.execute("SELECT ...")
    The connection will be returned to pool automatically.
    """
    global _pool
    if _pool is None:
        init_pool()
    conn = None
    try:
        conn = _pool.getconn()
        with conn:  # transaction management
            with conn.cursor(cursor_factory=cursor_factory) as cur:
                yield cur
    finally:
        if conn is not None:
            _pool.putconn(conn)

# Initialize pool on module import
init_pool()