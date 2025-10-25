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
    block_timestamp BIGINT NOT NULL,
    CONSTRAINT addresses_constraint PRIMARY KEY (address, block_timestamp)
);

CREATE INDEX IF NOT EXISTS addresses_address_idx ON addresses (address DESC);
CALL setup_hypertable('addresses');

CREATE TABLE IF NOT EXISTS block_log (
    block_height BIGINT NOT NULL,
    block_hash TEXT,
    scanned_at TIMESTAMPTZ DEFAULT now(),
    block_timestamp BIGINT NOT NULL,
    CONSTRAINT block_constraint PRIMARY KEY (block_height, block_timestamp)
);

CREATE INDEX IF NOT EXISTS block_log_height_idx ON block_log (block_height DESC);
CALL setup_hypertable('block_log');
