-- address_status and address_stats are created by schema.sql (main indexer).
-- This file only defines tables used by the address module (address.py).

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