-- version 0.1
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE OR REPLACE FUNCTION current_nano() RETURNS BIGINT
LANGUAGE SQL STABLE AS $$
    SELECT CAST(1000000000 * EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) AS BIGINT)
$$;

CREATE OR REPLACE PROCEDURE setup_hypertable(t regclass)
LANGUAGE SQL
AS $$
    SELECT create_hypertable(t, 'block_timestamp',
        chunk_time_interval => (40 * 24 * 60 * 60 * 1000000000 :: BIGINT));
    SELECT set_integer_now_func(t, 'current_nano');
$$;

-- TABLES --

CREATE TABLE IF NOT EXISTS block_log (
    block_height BIGINT NOT NULL,
    block_hash TEXT,
    scanned_at TIMESTAMPTZ DEFAULT now(),
    block_timestamp BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS block_log_height_idx ON block_log (block_height DESC);
CALL setup_hypertable('block_log');

-- =======================
-- UTXO STORAGE
-- =======================

-- A canonical per-output table. One row per created output; spend info is filled when an input spends it.
CREATE TABLE IF NOT EXISTS utxos (
    txid TEXT NOT NULL,
    vout INTEGER NOT NULL,
    address TEXT,
    value_sat BIGINT NOT NULL,
    script_pub_key_hex TEXT,
    script_pub_type TEXT,
    created_block BIGINT NOT NULL,
    created_block_timestamp BIGINT,
    spent BOOLEAN NOT NULL DEFAULT FALSE,
    spent_by_txid TEXT,
    spent_vin INTEGER,
    spent_block BIGINT,
    spent_block_timestamp BIGINT,
    PRIMARY KEY (txid, vout)
);

-- Index on address for lookups
-- CREATE INDEX IF NOT EXISTS utxos_address_idx ON utxos (address);

-- =======================
-- Address Status (table updated by indexer; has reused, created_block, etc.)
-- =======================

-- Drop materialized views if they exist (migration from MV to table)
DROP MATERIALIZED VIEW IF EXISTS address_stats CASCADE;
DROP MATERIALIZED VIEW IF EXISTS address_status CASCADE;
DROP VIEW IF EXISTS address_status CASCADE;

CREATE TABLE IF NOT EXISTS address_status (
    address TEXT NOT NULL,
    script_pub_type TEXT NOT NULL,
    reused BOOLEAN NOT NULL,
    created_block BIGINT NOT NULL,
    created_block_timestamp BIGINT NOT NULL,
    balance_sat BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (address)
);

CREATE INDEX IF NOT EXISTS address_status_script_pub_type_idx ON address_status (script_pub_type);
CREATE INDEX IF NOT EXISTS address_status_address_idx ON address_status (address);

-- No-op: address_status is a table, not a materialized view
CREATE OR REPLACE FUNCTION refresh_address_status() RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    NULL;
END;
$$;

-- =======================
-- UTXO Stats Aggregation (computed from UTXOs)
-- =======================

-- Drop existing view if it exists (for migration)
DROP MATERIALIZED VIEW IF EXISTS utxo_stats CASCADE;

-- Materialized view with aggregated UTXO statistics
-- Pre-computes spent/unspent aggregations to avoid expensive queries on every API call
CREATE MATERIALIZED VIEW utxo_stats AS
SELECT
    COUNT(*)::bigint AS total_utxos,
    COUNT(*) FILTER (WHERE spent)::bigint AS spent_utxos,
    COUNT(*) FILTER (WHERE NOT spent)::bigint AS unspent_utxos,
    COALESCE(SUM(value_sat) FILTER (WHERE spent), 0)::bigint AS spent_value_sat,
    COALESCE(SUM(value_sat) FILTER (WHERE NOT spent), 0)::bigint AS unspent_value_sat,
    COALESCE(SUM(value_sat), 0)::bigint AS total_value_sat
FROM utxos;

-- Function to refresh the UTXO stats materialized view
CREATE OR REPLACE FUNCTION refresh_utxo_stats() RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW utxo_stats;
END;
$$;

-- =======================
-- Address Stats (table updated by indexer from address_status)
-- =======================

CREATE TABLE IF NOT EXISTS address_stats (
    script_pub_type TEXT NOT NULL,
    reused_sat BIGINT NOT NULL DEFAULT 0,
    total_sat BIGINT NOT NULL DEFAULT 0,
    reused_count BIGINT NOT NULL DEFAULT 0,
    count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (script_pub_type)
);

-- No-op: address_stats is a table, updated by indexer
CREATE OR REPLACE FUNCTION refresh_address_stats() RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    NULL;
END;
$$;

-- =======================
-- Block Stats Aggregation (computed from block_log)
-- =======================

-- Drop existing view if it exists (for migration)
DROP MATERIALIZED VIEW IF EXISTS block_stats CASCADE;

-- Materialized view with aggregated block statistics
-- Pre-computes block count and latest block info to avoid expensive queries on every API call
CREATE MATERIALIZED VIEW block_stats AS
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
    (SELECT scanned_at FROM latest_block_info) AS latest_block_scanned_at;

-- Function to refresh the block stats materialized view
CREATE OR REPLACE FUNCTION refresh_block_stats() RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW block_stats;
END;
$$;

-- Function to refresh all materialized views (address_status/address_stats are tables, not MVs)
CREATE OR REPLACE FUNCTION refresh_all_materialized_views() RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW utxo_stats;
    REFRESH MATERIALIZED VIEW block_stats;
END;
$$;

-- =======================
-- STATS
-- =======================

CREATE TABLE IF NOT EXISTS stats (
    key TEXT PRIMARY KEY,
    value BIGINT NOT NULL
);