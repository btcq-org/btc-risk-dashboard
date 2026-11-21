"""
Block data extraction (no DB dependency).
Used by range_processor, main, and tests so tests can run without PostgreSQL.
"""
from collections import defaultdict
from typing import Dict, Callable, Optional

from .utils import detect_script_type, address_from_vout

# BIP30: the only two coinbase transactions that reuse the txid of an earlier,
# still-unspent coinbase (mainnet). Bitcoin Core lets the second copy overwrite
# the first, so the UTXO set holds a single output for each txid. Indexing the
# second copy would violate utxos_pkey and double-count the supply, so its
# outputs are skipped. Keyed by (height, txid) so no other block can match.
BIP30_DUPLICATE_COINBASES = {
    (91842, "d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599"),  # dup of 91812
    (91880, "e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468"),  # dup of 91722
}


def _extract_block_data(block, height, blockhash, shutdown_check: Optional[Callable[[], bool]] = None):
    """
    Extract VOUTs (UTXOs) and VINs (spends) from a block.
    Returns: dict with vout_rows, spend_rows, block_info, and stats, or None if shutdown requested.
    """
    if shutdown_check and shutdown_check():
        return None

    block_time = block.get("time", 0)
    block_ts = int(block_time * 1_000_000_000)

    vout_rows = []
    spend_rows = []
    type_counts: Dict[str, int] = defaultdict(int)
    type_balances: Dict[str, int] = defaultdict(int)
    total_utxo = 0
    total_balance_sat = 0

    for tx in block.get("tx", []):
        txid = tx.get("txid", "")
        if shutdown_check and shutdown_check():
            return None

        if (height, txid) in BIP30_DUPLICATE_COINBASES:
            print(f"Block {height}: skipping outputs of BIP30 duplicate coinbase {txid}")
            continue

        for vin_idx, vin in enumerate(tx.get("vin", [])):
            if "coinbase" in vin:
                continue
            spent_txid = vin.get("txid")
            spent_vout = vin.get("vout")
            if spent_txid and spent_vout is not None:
                spend_rows.append((
                    txid, vin_idx, height, block_ts, spent_txid, spent_vout
                ))

        for idx, v in enumerate(tx.get("vout", [])):
            spk = v.get("scriptPubKey", {})
            address = address_from_vout(v)
            value_btc = float(v.get("value", 0))
            value_sat = int(value_btc * 100_000_000)
            script_type = detect_script_type(spk)
            vout_rows.append((
                txid, idx, address or None, value_sat,
                spk.get("hex", ""), script_type, height, block_ts
            ))
            type_counts[script_type] += 1
            type_balances[script_type] += value_sat
            total_utxo += 1
            total_balance_sat += value_sat

    return {
        "vout_rows": vout_rows,
        "spend_rows": spend_rows,
        "block_info": {
            "block_height": height,
            "block_hash": blockhash,
            "block_timestamp": block_ts,
        },
        "stats": {
            "total_utxo": total_utxo,
            "total_balance_sat": total_balance_sat,
            "type_counts": dict(type_counts),
            "type_balances": dict(type_balances),
        },
    }
