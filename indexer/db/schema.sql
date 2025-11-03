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
