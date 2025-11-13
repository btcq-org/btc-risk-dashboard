from typing import Optional, Dict, Any

from pycoin.symbols.btc import network


def detect_script_type(spk: Dict[str, Any]) -> Optional[str]:
    """Detect scriptPubKey type for common and rare types."""
    if not spk:
        return None

    script_type_map = {
        "pubkey": "P2PK",
        "pubkeyhash": "P2PKH",
        "scripthash": "P2SH",
        "multisig": "P2MS",
        "witness_v0_keyhash": "P2WPKH",
        "witness_v0_scripthash": "P2WSH",
        "v1_p2tr": "P2TR",
        "witness_v1_taproot": "P2TR",
        "nulldata": "OP_RETURN",
        "op_return": "OP_RETURN",
        "nonstandard": "nonstandard",
    }

    return script_type_map.get(spk.get("type"), "nonstandard")


def address_from_vout(v: Dict[str, Any]) -> Optional[str]:
    """Extract address from vout's scriptPubKey.addresses array (Bitcoin Core RPC format)."""
    if not v:
        return None
    spk = v.get("scriptPubKey", {})
    if not spk:
        return None
    addresses = spk.get("addresses", [])
    if addresses and len(addresses) > 0:
        return addresses[0]
    address = spk.get("address")
    if address:
        return address

    script_hex = v.get("scriptPubKey", {}).get("hex")
    if script_hex:
        script_bytes = bytes.fromhex(script_hex)
        address = network.address.for_script(script_bytes)
        if address:
            return str(address)

    return None


def get_script_type(script_hex: str, address: str) -> str:
    """
    Classifies scriptPubKey hex into one of: P2PK, P2PKH, P2SH, P2MS, P2WPKH, P2WSH, P2TR.
    """
    script = bytes.fromhex(script_hex)

    def disassemble():
        opcodes = {
            0x76: 'OP_DUP', 0xa9: 'OP_HASH160', 0x88: 'OP_EQUALVERIFY', 0xac: 'OP_CHECKSIG',
            0x87: 'OP_EQUAL', 0x51: 'OP_1', 0x52: 'OP_2', 0x53: 'OP_3', 0xae: 'OP_CHECKMULTISIG',
            0x00: 'OP_0', 0x01: 'OP_1'
        }
        disasm = []
        i = 0
        while i < len(script):
            op = script[i]
            i += 1
            if 1 <= op <= 75:
                data_len = op
                if i + data_len > len(script):
                    return ['ERROR']
                data_hex = script[i:i + data_len].hex()
                disasm.append(f'PUSH_{data_len}:{data_hex}')
                i += data_len
            else:
                name = opcodes.get(op, f'OP_{op:02x}')
                disasm.append(name)
        return disasm

    disasm = disassemble()

    if (len(disasm) == 5 and
        disasm[0] == 'OP_DUP' and
        disasm[1] == 'OP_HASH160' and
        disasm[2].startswith('PUSH_20:') and
        disasm[3] == 'OP_EQUALVERIFY' and
        disasm[4] == 'OP_CHECKSIG'):
        return 'P2PKH'

    if (len(disasm) == 3 and
        disasm[0] == 'OP_HASH160' and
        disasm[1].startswith('PUSH_20:') and
        disasm[2] == 'OP_EQUAL'):
        return 'P2SH'

    if (len(disasm) == 2 and
        disasm[0] == 'OP_0' and
        disasm[1].startswith('PUSH_20:')):
        return 'P2WPKH'

    if (len(disasm) == 2 and
        disasm[0] == 'OP_0' and
        disasm[1].startswith('PUSH_32:')):
        return 'P2WSH'

    if (len(disasm) == 2 and
        disasm[0] == 'OP_1' and
        disasm[1].startswith('PUSH_32:')):
        return 'P2TR'

    if (len(disasm) == 2 and disasm[1] == 'OP_CHECKSIG' and
        (disasm[0].startswith('PUSH_33:') or disasm[0].startswith('PUSH_65:'))):
        return 'P2PK'

    if (len(disasm) >= 4 and disasm[-1] == 'OP_CHECKMULTISIG' and
        disasm[-2] in ['OP_1', 'OP_2', 'OP_3'] and disasm[0] in ['OP_1', 'OP_2', 'OP_3']):
        pubkey_pushes = [d for d in disasm[1:-2] if d.startswith('PUSH_33:') or d.startswith('PUSH_65:')]
        if len(pubkey_pushes) >= 2:
            return 'P2MS'

    if address.startswith('bc1p'):
        return 'Bech32'

    return 'unknown'
