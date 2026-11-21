#!/usr/bin/env python3
"""
Regression tests for issue #11: a block that is missing or fails to write
becomes a permanent, silent gap in the index.

Each test checks that the indexer stops (or retries) instead of leaving a gap.

No PostgreSQL or bitcoind is needed. The DB layer is replaced with an in-memory
fake that keeps the two properties the bug depends on:
  * utxos has a primary key on (txid, vout), so a duplicate insert raises;
  * get_db_cursor() is one transaction: an exception rolls back everything
    written inside it (like `with conn:` in indexer/db.py).

Run:
  pytest indexer/test_block_gaps.py -v
"""
import copy
from contextlib import contextmanager
from unittest import mock

import psycopg2
import pytest

# indexer/db.py opens a connection pool at import time; stop it connecting.
with mock.patch("psycopg2.pool.ThreadedConnectionPool"):
    from indexer import db
    from indexer import main as indexer_main
    from indexer import range_processor
    from indexer.block_data import BIP30_DUPLICATE_COINBASES

BIP30 = {height: txid for height, txid in BIP30_DUPLICATE_COINBASES}  # 91842, 91880


# ---------------------------------------------------------------------------
# In-memory database
# ---------------------------------------------------------------------------
class FakeDB:
    def __init__(self):
        self.utxos = {}          # (txid, vout) -> (address, value_sat, script_type)
        self.block_log = set()   # heights

    def max_height(self):
        return max(self.block_log) if self.block_log else None

    def missing_heights(self, start, end):
        return sorted(set(range(start, end + 1)) - self.block_log)


class FakeCursor:
    def __init__(self, fdb):
        self.fdb = fdb
        self._row = None
        self.connection = mock.MagicMock()

    def execute(self, sql, params=None):
        if "MIN(block_height), MAX(block_height), COUNT(*) FROM block_log" in sql:
            bl = self.fdb.block_log
            self._row = (min(bl), max(bl), len(bl)) if bl else (None, None, 0)
        elif "MAX(block_height) FROM block_log" in sql:
            self._row = (self.fdb.max_height(),)
        # CREATE INDEX / ALTER TABLE / SET LOCAL ...: no-op

    def fetchone(self):
        return self._row


def install_fake_db(monkeypatch, fdb):
    @contextmanager
    def fake_get_db_cursor(cursor_factory=None):
        snapshot = (copy.deepcopy(fdb.utxos), set(fdb.block_log))
        try:
            yield FakeCursor(fdb)
        except BaseException:
            fdb.utxos, fdb.block_log = snapshot  # ROLLBACK
            raise

    def insert_utxos(cur, rows):
        for txid, vout, address, value_sat, _hex, stype, _blk, _ts in rows:
            if (txid, vout) in fdb.utxos:
                raise psycopg2.IntegrityError(
                    'duplicate key value violates unique constraint "utxos_pkey"'
                )
            fdb.utxos[(txid, vout)] = (address, value_sat, stype)

    def delete_utxos(cur, spend_rows):
        out = []
        for *_, txid, vout in spend_rows:
            if (txid, vout) in fdb.utxos:
                out.append(fdb.utxos.pop((txid, vout)))
        return out

    def insert_block_log(cur, rows):
        for height, _hash, _ts in rows:
            fdb.block_log.add(height)

    noop = lambda *a, **k: None
    monkeypatch.setattr(db, "get_db_cursor", fake_get_db_cursor)
    for mod in (range_processor, indexer_main):
        monkeypatch.setattr(mod, "bulk_insert_utxos_copy", insert_utxos)
        monkeypatch.setattr(mod, "bulk_delete_utxos", delete_utxos)
        monkeypatch.setattr(mod, "bulk_insert_block_log_copy", insert_block_log)
        monkeypatch.setattr(mod, "update_address_status_with_vout_balance", noop)
        monkeypatch.setattr(mod, "update_address_status_with_vin_balance", noop)
        monkeypatch.setattr(mod, "upsert_stats", noop)
        monkeypatch.setattr(mod, "update_address_stats_incremental", noop)


# ---------------------------------------------------------------------------
# Synthetic chain
# ---------------------------------------------------------------------------
def _vout(value=50.0):
    return {"value": value, "scriptPubKey": {"type": "pubkeyhash", "address": "1Addr", "hex": "76a9"}}


def make_block(height, coinbase_txid=None, spends=()):
    """A block with one coinbase output, plus one tx spending each outpoint in `spends`."""
    txs = [{"txid": coinbase_txid or f"cb{height}", "vin": [{"coinbase": "00"}], "vout": [_vout()]}]
    for i, (txid, vout) in enumerate(spends):
        txs.append({"txid": f"spend{height}_{i}", "vin": [{"txid": txid, "vout": vout}], "vout": [_vout(1.0)]})
    return {"hash": f"hash{height}", "time": 1_231_006_505 + height, "tx": txs}


class FakeReader:
    def __init__(self, blocks):
        self.blocks = blocks

    def fetch_blocks(self, heights):
        # Same as RPCBlockReader / BLKFileReader: heights it can't get are just left out.
        return {h: self.blocks[h] for h in heights if h in self.blocks}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
def test_missing_block_stops_range_processing(monkeypatch):
    """range_processor.py: a block the reader can't return must stop the run, not be skipped."""
    fdb = FakeDB()
    install_fake_db(monkeypatch, fdb)
    blocks = {h: make_block(h) for h in range(0, 20) if h != 7}  # block 7 unavailable

    with pytest.raises(RuntimeError, match="Block 7 not found"):
        range_processor.process_range(0, 19, FakeReader(blocks), chunk_size=10)

    assert fdb.block_log == set(), "nothing from the chunk with the missing block may be written"


def test_db_error_stops_range_processing(monkeypatch):
    """range_processor.py: a failed chunk write must be raised, not swallowed."""
    fdb = FakeDB()
    install_fake_db(monkeypatch, fdb)
    blocks = {h: make_block(h) for h in range(0, 40)}
    blocks[25] = make_block(25, coinbase_txid="cb3")  # non-BIP30 duplicate txid -> utxos_pkey violation

    with pytest.raises(psycopg2.IntegrityError, match="utxos_pkey"):
        range_processor.process_range(0, 39, FakeReader(blocks), chunk_size=10)

    assert fdb.block_log == set(range(0, 20)), (
        f"only the chunks before the failure may be written; block_log={sorted(fdb.block_log)}"
    )
    assert indexer_main.get_last_processed_height() + 1 == 20, "restart must resume at the failed chunk"


def test_startup_refuses_block_log_with_gaps(monkeypatch):
    """main.py: resuming from MAX(block_height) would hide a gap forever, so startup must refuse."""
    fdb = FakeDB()
    install_fake_db(monkeypatch, fdb)
    fdb.block_log = set(range(0, 50)) - set(range(20, 30))
    monkeypatch.setattr(indexer_main, "schema_init", lambda: None)
    monkeypatch.setattr(db, "init_pool", lambda: None)
    monkeypatch.setattr(db, "shutdown_pool", lambda: None)
    monkeypatch.setattr(indexer_main.signal, "signal", lambda *a: None)

    with pytest.raises(RuntimeError, match="10 missing height"):
        indexer_main.check_block_log_contiguous()
    with pytest.raises(SystemExit) as exc:
        indexer_main.main()
    assert exc.value.code == 1

    fdb.block_log = set(range(0, 50))
    indexer_main.check_block_log_contiguous()  # contiguous: no error
    fdb.block_log = {923867}                     # UTXO-import mode leaves a single row
    indexer_main.check_block_log_contiguous()


def test_bip30_duplicate_coinbases_are_indexed_without_gaps(monkeypatch):
    """
    BIP30: the coinbase of 91842 duplicates 91812, and 91880 duplicates 91722.
    A from-genesis sync must index every block, keep one UTXO per duplicated
    txid (as Bitcoin Core does), and apply spends in those chunks.
    """
    fdb = FakeDB()
    install_fake_db(monkeypatch, fdb)
    start, end = 91720, 91899
    blocks = {h: make_block(h) for h in range(start, end + 1)}
    blocks[91722] = make_block(91722, coinbase_txid=BIP30[91880])
    blocks[91880] = make_block(91880, coinbase_txid=BIP30[91880])
    blocks[91812] = make_block(91812, coinbase_txid=BIP30[91842])
    blocks[91842] = make_block(91842, coinbase_txid=BIP30[91842])
    blocks[91845] = make_block(91845, spends=[("cb91800", 0)])

    range_processor.process_range(start, end, FakeReader(blocks), chunk_size=10)

    assert fdb.missing_heights(start, end) == []
    assert (BIP30[91842], 0) in fdb.utxos and (BIP30[91880], 0) in fdb.utxos
    assert ("cb91800", 0) not in fdb.utxos, "spend in block 91845 must be applied"


def test_live_sync_retries_failed_block_instead_of_skipping(monkeypatch):
    """main.py: after MAX_RETRIES failures live sync must not move on to the next height."""
    fdb = FakeDB()
    install_fake_db(monkeypatch, fdb)
    fdb.block_log = set(range(0, 101))  # caught up to 100
    tip = 103
    attempts = []

    def fake_process_single_block(height):
        attempts.append(height)
        if height == 101:
            return False  # e.g. DB error, always fails
        fdb.block_log.add(height)
        return True

    polls = {"n": 0}

    def fake_sleep(seconds):
        if seconds == indexer_main.POLL_INTERVAL:
            polls["n"] += 1
            if polls["n"] >= 2:  # let the poll loop run twice, then stop
                indexer_main.shutdown_requested = True

    monkeypatch.setattr(indexer_main, "shutdown_requested", False)
    monkeypatch.setattr(indexer_main, "STOP_AT_HEIGHT", None)
    monkeypatch.setattr(indexer_main, "IS_RECALC", False)
    monkeypatch.setattr(indexer_main, "IS_UTXO", False)
    monkeypatch.setattr(indexer_main, "POLL_INTERVAL", 12345)
    monkeypatch.setattr(indexer_main, "RECONNECT_DELAY", 54321)
    monkeypatch.setattr(indexer_main, "schema_init", lambda: None)
    monkeypatch.setattr(indexer_main, "test_rpc_connection", lambda: True)
    monkeypatch.setattr(indexer_main, "rpc_call", lambda method, params=None: tip)
    monkeypatch.setattr(indexer_main, "process_single_block", fake_process_single_block)
    monkeypatch.setattr(indexer_main.time, "sleep", fake_sleep)
    monkeypatch.setattr(db, "init_pool", lambda: None)
    monkeypatch.setattr(db, "shutdown_pool", lambda: None)
    monkeypatch.setattr(indexer_main.signal, "signal", lambda *a: None)

    indexer_main.main()

    assert 102 not in attempts, f"live sync indexed past failed block 101 (attempts={attempts})"
    assert attempts.count(101) == 2 * indexer_main.MAX_RETRIES, "block 101 must be retried on the next poll"
    assert fdb.max_height() == 100
