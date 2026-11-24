"""
Unit tests for the BLK parsing path (no bitcoind, no PostgreSQL).

The mainnet genesis block is parsed from its raw bytes through the same steps
the indexer uses with BLOCK_SOURCE=blk:
  read_block() -> convert_blk_to_rpc_format() -> _extract_block_data()
and the result is checked against the block's known hash, txid and output.

Run:
  pytest indexer/test_blk_parse.py -v
"""

import io
import struct

import pytest

from indexer.block_data import _extract_block_data
from indexer.block_reader import convert_blk_to_rpc_format
from indexer.test_blk import apply_xor, read_block
from indexer.utils import get_script_type

GENESIS_HASH = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
GENESIS_TXID = "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"
GENESIS_TIME = 1231006505
GENESIS_PUBKEY_SCRIPT = (
    "4104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f"
    "4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac"
)
GENESIS_BLOCK = bytes.fromhex(
    # header: version, prev hash, merkle root, time, bits, nonce
    "01000000"
    + "00" * 32
    + "3ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a"
    + "29ab5f49"
    + "ffff001d"
    + "1dac2b7c"
    # 1 tx: the coinbase
    + "01"
    + "01000000"
    + "01"
    + "00" * 32
    + "ffffffff"
    + "4d"
    + "04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f"
    + "72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73"
    + "ffffffff"
    + "01"
    + "00f2052a01000000"
    + "43"
    + GENESIS_PUBKEY_SCRIPT
    + "00000000"
)
MAINNET_MAGIC = bytes.fromhex("f9beb4d9")


def _blk_record(block: bytes) -> bytes:
    """One blk*.dat record: magic, little-endian size, block bytes."""
    return MAINNET_MAGIC + struct.pack("<I", len(block)) + block


def _parse(raw: bytes, xor_key: bytes):
    blk = read_block(io.BytesIO(raw), xor_key=xor_key)
    rpc_block = convert_blk_to_rpc_format(blk, 0)
    return blk, rpc_block, _extract_block_data(rpc_block, 0, rpc_block["hash"])


def _check_genesis(blk, rpc_block, data):
    assert blk["block_size"] == len(GENESIS_BLOCK)
    assert blk["header"]["timestamp"] == GENESIS_TIME
    assert rpc_block["hash"] == GENESIS_HASH
    assert [tx["txid"] for tx in rpc_block["tx"]] == [GENESIS_TXID]

    assert data["spend_rows"] == []  # coinbase input is not a spend
    assert data["vout_rows"] == [
        (
            GENESIS_TXID,
            0,
            "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
            50 * 100_000_000,
            GENESIS_PUBKEY_SCRIPT,
            "P2PK",
            0,
            GENESIS_TIME * 1_000_000_000,
        )
    ]
    assert data["block_info"] == {
        "block_height": 0,
        "block_hash": GENESIS_HASH,
        "block_timestamp": GENESIS_TIME * 1_000_000_000,
    }
    assert data["stats"]["total_utxo"] == 1
    assert data["stats"]["total_balance_sat"] == 50 * 100_000_000
    assert dict(data["stats"]["type_counts"]) == {"P2PK": 1}


def test_genesis_block_plain():
    _check_genesis(*_parse(_blk_record(GENESIS_BLOCK), xor_key=bytes(8)))


def test_genesis_block_xor_obfuscated():
    """Bitcoin Core v28+ XORs blk*.dat with the 8-byte key from xor.dat."""
    key = bytes.fromhex("0123456789abcdef")
    raw = apply_xor(_blk_record(GENESIS_BLOCK), key, offset=0)
    assert raw[:4] != MAINNET_MAGIC
    _check_genesis(*_parse(raw, xor_key=key))


H20 = "11" * 20
H32 = "22" * 32
PK33 = "02" + "33" * 32


@pytest.mark.parametrize(
    ("script_hex", "expected"),
    [
        ("76a914" + H20 + "88ac", "P2PKH"),
        ("a914" + H20 + "87", "P2SH"),
        ("0014" + H20, "P2WPKH"),
        ("0020" + H32, "P2WSH"),
        ("5120" + H32, "P2TR"),
        ("21" + PK33 + "ac", "P2PK"),
        (GENESIS_PUBKEY_SCRIPT, "P2PK"),
        ("51" + "21" + PK33 + "21" + PK33 + "52ae", "P2MS"),
        ("6a0568656c6c6f", "unknown"),  # OP_RETURN
    ],
)
def test_get_script_type(script_hex, expected):
    assert get_script_type(script_hex, "") == expected
