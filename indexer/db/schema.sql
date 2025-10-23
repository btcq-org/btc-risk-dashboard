-- Schema for quantum-at-risk indexer

CREATE TABLE IF NOT EXISTS quantum_exposed (
    address TEXT PRIMARY KEY,
    block_height BIGINT,
    txid TEXT
);

CREATE INDEX IF NOT EXISTS quantum_exposed_txid_idx ON quantum_exposed(txid);

CREATE TABLE IF NOT EXISTS quantum_revealed (
    address TEXT PRIMARY KEY,
    block_height BIGINT,
    txid TEXT
);

CREATE INDEX IF NOT EXISTS quantum_revealed_txid_idx ON quantum_revealed(txid);

CREATE TABLE IF NOT EXISTS quantum_scanned (
    block_height BIGINT PRIMARY KEY,
    block_hash TEXT,
    scanned_at TIMESTAMPTZ DEFAULT now(),
    exposed_count BIGINT DEFAULT 0,
    revealed_count BIGINT DEFAULT 0
);
