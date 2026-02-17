#!/usr/bin/env python3
"""
Test that the first 100 blocks are consumed correctly via BLK files,
in the same way as indexer/main.py (using _extract_block_data), and match RPC.

Requires:
  - Bitcoin Core RPC (for getblockhash; used by BLK reader and for comparison).
  - BLOCKS_DIR pointing to the blocks directory (e.g. /path/to/data-bitcoin/blocks)
    containing blk*.dat with at least the first 100 blocks.

Run:
  BLOCKS_DIR=/path/to/blocks pytest indexer/test_blk_first_100.py -v
  # Fewer blocks for a quick run (default 100):
  BLK_TEST_N_BLOCKS=5 BLOCKS_DIR=/path/to/blocks pytest indexer/test_blk_first_100.py -v
  python -m indexer.test_blk_first_100
"""
import os
import sys

try:
    import pytest
except ImportError:
    pytest = None

from . import config

RPC_USER = config.RPC_USER
RPC_PASSWORD = config.RPC_PASSWORD
RPC_HOST = config.RPC_HOST
RPC_PORT = config.RPC_PORT
RPC_URL = config.RPC_URL
BLOCKS_DIR = config.BLOCKS_DIR
FIRST_N_BLOCKS = int(os.getenv("BLK_TEST_N_BLOCKS", "100"))


def _rpc_call(method, params=None):
    import json
    import requests
    payload = json.dumps({
        "jsonrpc": "1.0",
        "id": "test-blk",
        "method": method,
        "params": params or [],
    })
    resp = requests.post(
        RPC_URL,
        auth=(RPC_USER, RPC_PASSWORD),
        headers={"content-type": "text/plain"},
        data=payload,
        timeout=30,
    )
    resp.raise_for_status()
    r = resp.json()
    if r.get("error"):
        raise RuntimeError(r["error"])
    return r["result"]


def _rpc_batch_call(requests_list):
    """Minimal batch: list of (method, params, id). Returns dict id -> result."""
    import json
    import requests
    payload = [
        {
            "jsonrpc": "1.0",
            "id": idx,
            "method": method,
            "params": params or [],
        }
        for (method, params, idx) in requests_list
    ]
    resp = requests.post(
        RPC_URL,
        auth=(RPC_USER, RPC_PASSWORD),
        headers={"content-type": "text/plain"},
        data=json.dumps(payload),
        timeout=120,
    )
    resp.raise_for_status()
    out = resp.json()
    results = {}
    for entry in (out if isinstance(out, list) else [out]):
        err = entry.get("error")
        if err:
            raise RuntimeError(err)
        # Preserve key type (int height) for batch id
        results[entry["id"]] = entry.get("result")
    return results


def _can_run_integration():
    """True if we have RPC and BLOCKS_DIR and RPC has at least 100 blocks."""
    if not BLOCKS_DIR or not os.path.isdir(BLOCKS_DIR):
        return False
    try:
        count = _rpc_call("getblockcount")
        return count >= FIRST_N_BLOCKS - 1  # 0-based, so 99 is enough
    except Exception:
        return False


if pytest:

    @pytest.fixture(scope="module")
    def rpc_blocks():
        """Fetch first FIRST_N_BLOCKS blocks via RPC (getblockhash + getblock 2)."""
        heights = list(range(FIRST_N_BLOCKS))
        hash_reqs = [("getblockhash", [h], h) for h in heights]
        hashes = _rpc_batch_call(hash_reqs)
        block_reqs = [
            ("getblock", [hashes[h], 2], h)
            for h in heights
            if h in hashes and hashes[h]
        ]
        blocks = _rpc_batch_call(block_reqs)
        return {h: blocks[h] for h in heights if h in blocks}

    @pytest.fixture(scope="module")
    def blk_blocks(rpc_blocks):
        """Fetch same blocks via BLKFileReader (same as main.py BLOCK_SOURCE=blk)."""
        from .block_reader import BLKFileReader
        heights = list(range(FIRST_N_BLOCKS))
        reader = BLKFileReader(
            blocks_dir=BLOCKS_DIR,
            rpc_call=_rpc_call,
            rpc_batch_call=_rpc_batch_call,
        )
        return reader.fetch_blocks(heights)


def _normalize_vout_rows(vout_rows):
    """Sort for stable comparison. Each row: (txid, vout, address, value_sat, script_hex, script_type, height, block_ts)."""
    return sorted(vout_rows, key=lambda r: (r[0], r[1]))


def _vout_rows_comparable(vout_rows):
    """Compare vout rows ignoring script_type (RPC vs BLK can differ: e.g. P2PK vs nonstandard)."""
    return sorted([(r[0], r[1], r[2], r[3], r[4], r[6], r[7]) for r in vout_rows], key=lambda r: (r[0], r[1]))


def _normalize_spend_rows(spend_rows):
    """Sort for stable comparison."""
    return sorted(spend_rows, key=lambda r: (r[4], r[5]))  # txid, vout


if pytest:

    @pytest.mark.skipif(
        not _can_run_integration(),
        reason="Need BLOCKS_DIR and Bitcoin RPC with at least 100 blocks",
    )
    class TestBlkFirst100:
        """Compare BLK vs RPC for first 100 blocks using same consumption as main.py."""

        def test_blk_returns_same_number_of_blocks(self, rpc_blocks, blk_blocks):
            assert len(blk_blocks) == len(rpc_blocks), (
                f"BLK returned {len(blk_blocks)} blocks, RPC returned {len(rpc_blocks)}"
            )
            assert len(blk_blocks) == FIRST_N_BLOCKS

        def test_blk_block_hashes_match_rpc(self, rpc_blocks, blk_blocks):
            for height in range(FIRST_N_BLOCKS):
                if height not in rpc_blocks or height not in blk_blocks:
                    continue
                rpc_hash = rpc_blocks[height].get("hash") or rpc_blocks[height].get("_blockhash", "")
                blk_hash = blk_blocks[height].get("hash", "")
                assert rpc_hash == blk_hash, f"Height {height}: RPC hash {rpc_hash} != BLK hash {blk_hash}"

        def test_blk_txids_match_rpc_per_block(self, rpc_blocks, blk_blocks):
            for height in range(FIRST_N_BLOCKS):
                if height not in rpc_blocks or height not in blk_blocks:
                    continue
                rpc_txids = [tx.get("txid", "") for tx in rpc_blocks[height].get("tx", [])]
                blk_txids = [tx.get("txid", "") for tx in blk_blocks[height].get("tx", [])]
                assert rpc_txids == blk_txids, (
                    f"Height {height}: txid list mismatch. RPC count={len(rpc_txids)}, BLK count={len(blk_txids)}"
                )

        def test_blk_vout_counts_match_rpc_per_tx(self, rpc_blocks, blk_blocks):
            for height in range(FIRST_N_BLOCKS):
                if height not in rpc_blocks or height not in blk_blocks:
                    continue
                rpc_txs = rpc_blocks[height].get("tx", [])
                blk_txs = blk_blocks[height].get("tx", [])
                assert len(rpc_txs) == len(blk_txs)
                for i, (rpc_tx, blk_tx) in enumerate(zip(rpc_txs, blk_txs)):
                    rpc_n = len(rpc_tx.get("vout", []))
                    blk_n = len(blk_tx.get("vout", []))
                    assert rpc_n == blk_n, (
                        f"Height {height} tx index {i} (txid={rpc_tx.get('txid', '')[:16]}...): "
                        f"RPC vouts={rpc_n}, BLK vouts={blk_n}"
                    )

        def test_extract_block_data_blk_matches_rpc(self, rpc_blocks, blk_blocks):
            """Run _extract_block_data (same as main.py) on both and compare vout_rows and spend_rows."""
            from .block_data import _extract_block_data

            for height in range(FIRST_N_BLOCKS):
                if height not in rpc_blocks or height not in blk_blocks:
                    continue
                rpc_block = rpc_blocks[height]
                blk_block = blk_blocks[height]
                blockhash = rpc_block.get("hash") or rpc_block.get("_blockhash", "")

                rpc_data = _extract_block_data(rpc_block, height, blockhash)
                blk_data = _extract_block_data(blk_block, height, blockhash)

                assert rpc_data is not None and blk_data is not None

                rpc_vouts = _vout_rows_comparable(rpc_data["vout_rows"])
                blk_vouts = _vout_rows_comparable(blk_data["vout_rows"])
                assert rpc_vouts == blk_vouts, (
                    f"Height {height}: vout_rows differ (txid, vout, address, value, script_hex, height, ts). "
                    f"RPC count={len(rpc_vouts)}, BLK count={len(blk_vouts)}"
                )

                rpc_spends = _normalize_spend_rows(rpc_data["spend_rows"])
                blk_spends = _normalize_spend_rows(blk_data["spend_rows"])
                assert rpc_spends == blk_spends, (
                    f"Height {height}: spend_rows differ. "
                    f"RPC count={len(rpc_spends)}, BLK count={len(blk_spends)}"
                )


def main():
    """Run the same checks as a script (no pytest)."""
    if not _can_run_integration():
        print("Skipping: set BLOCKS_DIR and ensure Bitcoin RPC has at least 100 blocks.")
        print("  Example: BLOCKS_DIR=/root/data-bitcoin/blocks python -m indexer.test_blk_first_100")
        sys.exit(0)

    from .block_reader import BLKFileReader
    from .block_data import _extract_block_data

    heights = list(range(FIRST_N_BLOCKS))
    reader = BLKFileReader(
        blocks_dir=BLOCKS_DIR,
        rpc_call=_rpc_call,
        rpc_batch_call=_rpc_batch_call,
    )
    blk_blocks = reader.fetch_blocks(heights)

    hash_reqs = [("getblockhash", [h], h) for h in heights]
    hashes = _rpc_batch_call(hash_reqs)
    block_reqs = [("getblock", [hashes[h], 2], h) for h in heights if h in hashes and hashes[h]]
    rpc_blocks = _rpc_batch_call(block_reqs)

    errors = []
    for height in heights:
        if height not in blk_blocks or height not in rpc_blocks:
            errors.append(f"Height {height}: missing in BLK or RPC")
            continue
        rpc_block = rpc_blocks[height]
        blk_block = blk_blocks[height]
        blockhash = rpc_block.get("hash") or rpc_block.get("_blockhash", "")

        if rpc_block.get("hash") != blk_block.get("hash"):
            errors.append(f"Height {height}: hash mismatch")
        rpc_data = _extract_block_data(rpc_block, height, blockhash)
        blk_data = _extract_block_data(blk_block, height, blockhash)
        if _normalize_vout_rows(rpc_data["vout_rows"]) != _normalize_vout_rows(blk_data["vout_rows"]):
            errors.append(f"Height {height}: vout_rows differ")
        if _normalize_spend_rows(rpc_data["spend_rows"]) != _normalize_spend_rows(blk_data["spend_rows"]):
            errors.append(f"Height {height}: spend_rows differ")

    if errors:
        for e in errors:
            print("FAIL:", e)
        sys.exit(1)
    print(f"OK: First {FIRST_N_BLOCKS} blocks consumed via BLK match RPC (hashes, txids, vout_rows, spend_rows).")


if __name__ == "__main__":
    main()
