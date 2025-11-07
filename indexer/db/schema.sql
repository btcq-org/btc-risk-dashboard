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

-- address table to keep track of address metadata
-- scriptPubKey is the latest known scriptPubKey type for the address UTXO
-- scriptSig shows if the address has re-used its scriptSig (P2PKH, P2SH, etc)

CREATE TABLE IF NOT EXISTS addresses (
    address TEXT NOT NULL,
    first_seen_block BIGINT,
    last_seen_block BIGINT,
    tx_count BIGINT DEFAULT 0,
    script_pub_type TEXT,
    script_sig_type TEXT,
    balance_sat BIGINT DEFAULT 0,
    block_timestamp BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS addresses_address_idx ON addresses (address);
CREATE INDEX IF NOT EXISTS addresses_block_timestamp_idx ON addresses (block_timestamp DESC);

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
    created_block_timestamp BIGINT NOT NULL,
    spent BOOLEAN NOT NULL DEFAULT FALSE,
    spent_by_txid TEXT,
    spent_vin INTEGER,
    spent_block BIGINT,
    spent_block_timestamp BIGINT,
    PRIMARY KEY (txid, vout)
);

-- Index on address for lookups (TEXT index for equality checks)
CREATE INDEX IF NOT EXISTS utxos_address_idx ON utxos (address);
-- Index on spent flag for filtering unspent UTXOs
CREATE INDEX IF NOT EXISTS utxos_spent_idx ON utxos (spent);
-- Index for ordering by creation block (descending for newest first)
CREATE INDEX IF NOT EXISTS utxos_created_block_idx ON utxos (created_block DESC);
-- Partial index for spent UTXOs by block (smaller, faster)
CREATE INDEX IF NOT EXISTS utxos_spent_block_idx ON utxos (spent_block DESC) WHERE spent;
-- Note: PRIMARY KEY (txid, vout) already provides index for UPDATE/ON CONFLICT queries

-- =======================
-- Address Status (computed from UTXOs)
-- =======================

-- Drop existing view if it exists (for migration)
DROP VIEW IF EXISTS address_status CASCADE;

-- Materialized view with latest status per address derived from utxos table
-- Pre-computes aggregations to avoid expensive subqueries on every query
-- Uses window functions to efficiently get latest script_pub_type without correlated subqueries
CREATE MATERIALIZED VIEW address_status AS
WITH script_type_ranked AS (
    SELECT
        address,
        script_pub_type,
        ROW_NUMBER() OVER (
            PARTITION BY address 
            ORDER BY 
                CASE WHEN NOT spent THEN 0 ELSE 1 END,  -- Unspent first
                created_block DESC, 
                created_block_timestamp DESC
        ) AS rn
    FROM utxos
    WHERE address IS NOT NULL AND address <> ''
),
latest_script_type AS (
    SELECT address, script_pub_type
    FROM script_type_ranked
    WHERE rn = 1
),
per_addr AS (
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
        COUNT(*) AS appearances
    FROM utxos
    WHERE address IS NOT NULL AND address <> ''
    GROUP BY address
)
SELECT
    pa.address,
    pa.first_seen_block,
    pa.first_seen_block_timestamp,
    pa.last_seen_block,
    pa.last_seen_block_timestamp,
    pa.balance_sat,
    pa.utxo_count,
    pa.appearances AS tx_count,
    lst.script_pub_type
FROM per_addr pa
LEFT JOIN latest_script_type lst ON pa.address = lst.address;

-- Indexes for fast lookups
CREATE INDEX IF NOT EXISTS address_status_script_pub_type_idx ON address_status (script_pub_type);
CREATE INDEX IF NOT EXISTS address_status_address_idx ON address_status (address);

-- Function to refresh the materialized view
CREATE OR REPLACE FUNCTION refresh_address_status() RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW address_status;
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
-- Address Stats Aggregation (computed from address_status)
-- =======================

-- Drop existing view if it exists (for migration)
DROP MATERIALIZED VIEW IF EXISTS address_stats CASCADE;

-- Materialized view with aggregated address statistics
-- Pre-computes address counts and risk classifications to avoid expensive queries on every API call
CREATE MATERIALIZED VIEW address_stats AS
SELECT
    COUNT(*)::bigint AS address_count,
    COUNT(*) FILTER (WHERE script_pub_type = ANY(ARRAY['P2PK', 'P2MS', 'P2PR']))::bigint AS high_risk_address_count,
    COUNT(*) FILTER (WHERE script_pub_type = ANY(ARRAY['P2PKH', 'P2SH', 'P2WPKH', 'P2WSH']))::bigint AS medium_risk_address_count
FROM address_status;

-- Function to refresh the address stats materialized view
CREATE OR REPLACE FUNCTION refresh_address_stats() RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW address_stats;
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

-- Function to refresh all materialized views
CREATE OR REPLACE FUNCTION refresh_all_materialized_views() RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW address_status;
    REFRESH MATERIALIZED VIEW utxo_stats;
    REFRESH MATERIALIZED VIEW address_stats;
    REFRESH MATERIALIZED VIEW block_stats;
END;
$$;
