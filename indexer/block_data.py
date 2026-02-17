"""
Block data extraction (no DB dependency).
Used by range_processor, main, and tests so tests can run without PostgreSQL.
"""
from collections import defaultdict
from typing import Dict, Callable, Optional

from .utils import detect_script_type, address_from_vout


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
