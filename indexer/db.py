"""Shared database connection handling for quantum-at-risk indexer."""

from contextlib import contextmanager
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor, execute_values

from . import config

DB_HOST = config.DB_HOST
DB_PORT = config.DB_PORT
DB_NAME = config.DB_NAME

# Credentials used when the pool is created. The indexer uses DB_USER/DB_PASS;
# the API switches to the read-only role with use_credentials() before connecting.
_user = config.DB_USER
_password = config.DB_PASS
_password_env = "DB_PASS"  # named in the error when the password is missing

# Connection pool (min=1, max=20 connections)
_pool = None

def use_credentials(user, password, password_env="DB_PASS"):
    """Select the role the pool connects as. Must be called before the pool is created."""
    global _user, _password, _password_env
    if _pool is not None:
        raise RuntimeError("use_credentials() must be called before the connection pool is created")
    if not password:
        raise RuntimeError(f"{password_env} is not set. Set it in the environment or .env (there is no default password).")
    _user, _password, _password_env = user, password, password_env

def init_pool():
    """Initialize the connection pool. Call this once at startup."""
    global _pool
    if _pool is None:
        if not _password:
            raise RuntimeError(
                f"{_password_env} is not set. Set it in the environment or .env "
                f"(there is no default password)."
            )
        _pool = ThreadedConnectionPool(
            minconn=1,
            maxconn=20,
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=_user,
            password=_password
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

# The pool is created on first use (get_db_cursor) or by an explicit init_pool(), not at
# import, so the API can pick its read-only role with use_credentials() first.