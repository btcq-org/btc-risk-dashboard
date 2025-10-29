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

CREATE INDEX IF NOT EXISTS utxos_address_idx ON utxos (address);
CREATE INDEX IF NOT EXISTS utxos_spent_idx ON utxos (spent);
CREATE INDEX IF NOT EXISTS utxos_created_block_idx ON utxos (created_block DESC);
CREATE INDEX IF NOT EXISTS utxos_spent_block_idx ON utxos (spent_block DESC) WHERE spent;

-- =======================
-- Address Status (computed from UTXOs)
-- =======================

-- View with latest status per address derived from utxos table
CREATE OR REPLACE VIEW address_status AS
WITH per_addr AS (
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
    -- Latest known script pub type from the newest unspent creation; fallback to newest creation
    COALESCE(
        (
            SELECT u.script_pub_type
            FROM utxos u
            WHERE u.address = pa.address AND (NOT u.spent)
            ORDER BY u.created_block DESC, u.created_block_timestamp DESC
            LIMIT 1
        ),
        (
            SELECT u2.script_pub_type
            FROM utxos u2
            WHERE u2.address = pa.address
            ORDER BY u2.created_block DESC, u2.created_block_timestamp DESC
            LIMIT 1
        )
    ) AS script_pub_type
FROM per_addr pa;
