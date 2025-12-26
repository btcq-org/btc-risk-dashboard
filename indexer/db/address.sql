CREATE TABLE IF NOT EXISTS address_status (
    address TEXT NOT NULL,
    script_pub_type TEXT NOT NULL,
    reused BOOLEAN NOT NULL,
    created_block BIGINT NOT NULL,
    created_block_timestamp BIGINT NOT NULL,
    balance_sat BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (address)
);

-- Stats table for address status for latest block
-- TODO: This can be aggregated by timescaleDB for each block
CREATE TABLE IF NOT EXISTS address_stats (
    script_pub_type TEXT NOT NULL,
    reused_sat BIGINT NOT NULL DEFAULT 0,
    total_sat BIGINT NOT NULL DEFAULT 0,
    reused_count BIGINT NOT NULL DEFAULT 0,
    count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (script_pub_type)
);

CREATE TABLE IF NOT EXISTS address_global_stats (
    id INTEGER PRIMARY KEY DEFAULT 1,
    biggest_balance BIGINT,
    oldest_address TEXT,
    CHECK (id = 1)
);

-- P2PK status table - same structure as address_status but specifically for P2PK addresses
CREATE TABLE IF NOT EXISTS p2pk_status (
    address TEXT NOT NULL,
    script_pub_type TEXT NOT NULL,
    reused BOOLEAN NOT NULL,
    created_block BIGINT NOT NULL,
    created_block_timestamp BIGINT NOT NULL,
    balance_sat BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (address)
);