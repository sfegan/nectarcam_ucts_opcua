"""
ucts_emulator.py
────────────────
Interactive emulator for the UCTS-TiCkS board.

Runs three concurrent services:
  1. SNMP agent   -- serves all UCTS OIDs over SNMPv2c (default port 1161)
                     Supports GET and GETNEXT (so snmpwalk works).
                     Uses a hand-rolled BER encoder/decoder -- no pysnmp
                     high-level API needed, works with any pysnmp version.
  2. UDP listener -- accepts TiCkS command packets (default port 55010),
                     decodes them per the ICD, updates state, and echoes
                     each packet back as acknowledgement.
  3. Terminal CLI -- interactive prompt to inspect and modify all variables.

Usage
-----
  python ucts_emulator.py [options]

  --snmp-port N     SNMP UDP port (default 1161; use 161 as root)
  --cmd-port N      TiCkS command UDP port (default 55010)
  --bind HOST       Interface to bind (default 0.0.0.0)
  --log-level LEVEL DEBUG/INFO/WARNING/ERROR (default INFO)

Interactive commands (at the ucts> prompt)
------------------------------------------
  show                       Print all current variable values
  set Temperature <val>      Set temperature in °C (e.g. 25.6)
  set EventCount <val>       Set event counter
  set BusyCount <val>        Set busy counter
  set Throttle <val>         Set throttle (decimal or 0x hex)
  set DstIpAddr <x.x.x.x>   Set destination IP
  set DstPort <val>          Set destination port
  set WrpcSwVersion <str>    Set WR software version string
  set WrpcHwType <str>       Set WR hardware type string
  set WrpcBuildBy <str>      Set WR build-by string
  set WrpcBuildDate <str>    Set WR build date string
  set SpllMode <n>           Set SPLL mode (0=na,1=grandmaster,2=master,3=slave)
  set SpllSeqState <n>       Set SPLL sequencer state (8=ready)
  set FirmwareVersion <n>    Set firmware version integer
  set PortLinkStatus <n>     Set port link status (0=na, 1=down, 2=up)
  state <0|1|2>              Set TiCkS state (0=Online, 1=Running, 2=Unknown)
  status <hex_or_dec>        Set raw status word directly
  reset                      Apply Reset command (state=Online, counters=0)
  getready                   Apply GetReady command (state=Running)
  help                       Show this help
  quit / exit                Shut down

Dependencies
------------
  No extra dependencies beyond the standard library and pyasn1 (installed
  as a transitive dependency of pysnmp/asyncua).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import socket
import struct
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("ucts_emulator")

# ─────────────────────────────────────────────────────────────────────────────
# Minimal hand-rolled BER codec
# Covers only what is needed to serve SNMPv2c GET requests.
# ─────────────────────────────────────────────────────────────────────────────

# Tag bytes for the types we use
_T_INTEGER    = 0x02
_T_OCTETSTR   = 0x04
_T_NULL       = 0x05
_T_OID        = 0x06
_T_SEQUENCE   = 0x30
_T_IPADDRESS  = 0x40   # APPLICATION 0
_T_COUNTER32  = 0x41   # APPLICATION 1
_T_GAUGE32    = 0x42   # APPLICATION 2
_T_TIMETICKS  = 0x43   # APPLICATION 3
_T_COUNTER64  = 0x46   # APPLICATION 6
_T_NOSUCHINS  = 0x81   # [1] PRIMITIVE  (NoSuchInstance)
_T_ENDOFMIB   = 0x82   # [2] PRIMITIVE  (EndOfMibView)
_T_GET_REQ    = 0xA0   # [0] CONSTRUCTED
_T_GETNEXT    = 0xA1   # [1] CONSTRUCTED
_T_RESPONSE   = 0xA2   # [2] CONSTRUCTED


def _ber_length(n: int) -> bytes:
    if n < 0x80:
        return bytes([n])
    if n < 0x100:
        return bytes([0x81, n])
    if n < 0x10000:
        return bytes([0x82, n >> 8, n & 0xFF])
    raise ValueError(f"Length {n} too large for this encoder")


def _ber_tlv(tag: int, value: bytes) -> bytes:
    return bytes([tag]) + _ber_length(len(value)) + value


def _ber_int(value: int) -> bytes:
    """Encode a signed integer in the minimum number of bytes."""
    if value == 0:
        return _ber_tlv(_T_INTEGER, b"\x00")
    n = value
    result = []
    while n not in (0, -1):
        result.append(n & 0xFF)
        n >>= 8
    # Extend sign
    if value > 0 and (result[-1] & 0x80):
        result.append(0x00)
    elif value < 0 and not (result[-1] & 0x80):
        result.append(0xFF)
    result.reverse()
    return _ber_tlv(_T_INTEGER, bytes(result))


def _ber_uint_app(tag: int, value: int, width: int) -> bytes:
    """
    Encode an unsigned application-tagged integer (Counter32, Gauge32, etc.).

    BER integer encoding is always signed: if the high bit of the first content
    byte is set, the value would be interpreted as negative.  We therefore
    prepend a 0x00 byte when needed, exactly as for signed INTEGER encoding.
    This matters for values like 55000 (0xD6D8) where the leading byte 0xD6
    has bit 7 set -- without the extra zero, pysnmp's Gauge32 constraint
    (0..4294967295) rejects the decoded value as -10536.
    """
    if value == 0:
        raw = b"\x00"
    else:
        raw = value.to_bytes(width, "big").lstrip(b"\x00")
        if raw[0] & 0x80:          # high bit set -> add unsigned-marker zero
            raw = b"\x00" + raw
    return _ber_tlv(tag, raw)


def _ber_octetstr(value: bytes) -> bytes:
    return _ber_tlv(_T_OCTETSTR, value)


def _ber_ipaddr(ip_str: str) -> bytes:
    try:
        parts = [int(p) for p in ip_str.split(".")]
        return _ber_tlv(_T_IPADDRESS, bytes(parts))
    except Exception:
        return _ber_tlv(_T_IPADDRESS, b"\x00\x00\x00\x00")


def _ber_oid(oid: Tuple[int, ...]) -> bytes:
    body = bytes([oid[0] * 40 + oid[1]])
    for v in oid[2:]:
        if v == 0:
            body += b"\x00"
        else:
            parts: List[int] = []
            while v:
                parts.append(v & 0x7F)
                v >>= 7
            parts.reverse()
            body += bytes([(p | 0x80) for p in parts[:-1]] + [parts[-1]])
    return _ber_tlv(_T_OID, body)


def _ber_counter64(value: int) -> bytes:
    # Counter64 must be encoded unsigned, minimum bytes, no leading zeros except
    # a single 0x00 when the high bit of the first content byte would be 1.
    raw = value.to_bytes(8, "big").lstrip(b"\x00") or b"\x00"
    if raw[0] & 0x80:
        raw = b"\x00" + raw
    return _ber_tlv(_T_COUNTER64, raw)


def _ber_nosuchinstance() -> bytes:
    return _ber_tlv(_T_NOSUCHINS, b"")


def _ber_endofmibview() -> bytes:
    return _ber_tlv(_T_ENDOFMIB, b"")


def _ber_sequence(body: bytes) -> bytes:
    return _ber_tlv(_T_SEQUENCE, body)


def _ber_pdu(tag: int, body: bytes) -> bytes:
    return _ber_tlv(tag, body)


# ── decoder ───────────────────────────────────────────────────────────────────

def _decode_tlv(data: bytes, pos: int) -> Tuple[int, bytes, int]:
    """Return (tag_byte, value_bytes, new_pos)."""
    tag = data[pos]; pos += 1
    lb  = data[pos]; pos += 1
    if lb & 0x80:
        n = lb & 0x7F
        length = int.from_bytes(data[pos:pos + n], "big"); pos += n
    else:
        length = lb
    value = data[pos:pos + length]
    return tag, value, pos + length


def _decode_int(data: bytes) -> int:
    if not data:
        return 0
    return int.from_bytes(data, "big", signed=True)


def _decode_oid(data: bytes) -> Tuple[int, ...]:
    oid = [data[0] // 40, data[0] % 40]
    i = 1
    while i < len(data):
        val = 0
        while True:
            b = data[i]; i += 1
            val = (val << 7) | (b & 0x7F)
            if not (b & 0x80):
                break
        oid.append(val)
    return tuple(oid)


def _parse_snmp_request(
    packet: bytes,
) -> Optional[Tuple[int, str, int, int, List[Tuple[int, ...]]]]:
    """
    Parse an SNMPv2c GET or GETNEXT request packet.
    Returns (version, community, request_id, pdu_type, [oid_tuple, ...]) or None on error.
    """
    try:
        tag, body, _ = _decode_tlv(packet, 0)
        if tag != _T_SEQUENCE:
            return None
        pos = 0
        tag, ver_bytes, pos = _decode_tlv(body, pos)
        version = _decode_int(ver_bytes)
        tag, comm_bytes, pos = _decode_tlv(body, pos)
        community = comm_bytes.decode("latin-1")
        tag, pdu_body, pos = _decode_tlv(body, pos)
        if tag not in (_T_GET_REQ, _T_GETNEXT):
            return None   # only handle GET and GETNEXT; ignore SET, etc.
        pdu_type = tag
        pos2 = 0
        tag, req_id_bytes, pos2 = _decode_tlv(pdu_body, pos2)
        request_id = _decode_int(req_id_bytes)
        _decode_tlv(pdu_body, pos2)   # skip error-status
        tag, _, pos2 = _decode_tlv(pdu_body, pos2)
        tag, _, pos2 = _decode_tlv(pdu_body, pos2)
        tag, vbl_body, pos2 = _decode_tlv(pdu_body, pos2)
        oids = []
        pos3 = 0
        while pos3 < len(vbl_body):
            tag, vb_body, pos3 = _decode_tlv(vbl_body, pos3)
            tag, oid_bytes, _ = _decode_tlv(vb_body, 0)
            if tag == _T_OID:
                oids.append(_decode_oid(oid_bytes))
        return version, community, request_id, pdu_type, oids
    except Exception as exc:
        log.debug("SNMP parse error: %s", exc)
        return None


def _build_response(
    community: str,
    request_id: int,
    var_binds: List[Tuple[Tuple[int, ...], bytes]],
) -> bytes:
    """
    Build an SNMPv2c GET-Response packet.
    var_binds is a list of (oid_tuple, already_encoded_value_tlv) pairs.
    """
    vb_list = b""
    for oid, value_tlv in var_binds:
        vb_list += _ber_sequence(_ber_oid(oid) + value_tlv)
    pdu_body = (
        _ber_int(request_id)    # requestID
        + _ber_int(0)           # errorStatus = noError
        + _ber_int(0)           # errorIndex  = 0
        + _ber_sequence(vb_list)
    )
    msg_body = (
        _ber_int(1)                              # version = 1 (SNMPv2c)
        + _ber_octetstr(community.encode("latin-1"))
        + _ber_pdu(_T_RESPONSE, pdu_body)
    )
    return _ber_sequence(msg_body)


# ─────────────────────────────────────────────────────────────────────────────
# Shared emulated state
# ─────────────────────────────────────────────────────────────────────────────

_START_TIME = time.time()


class UCTSState:
    """Single shared mutable state object representing the emulated UCTS board."""

    def __init__(self) -> None:
        self._ticks_state: int = 0       # 0=Online, 1=Running, 2=Unknown
        self._fw_version:  int = 1
        self._spi_enabled: bool = True
        self.dst_ip:       str   = "10.10.3.250"
        self.dst_mac_h:    bytes = b"\x44\xa8\x42\x44"   # 4 MSB
        self.dst_mac_l:    bytes = b"\x32\xc9"           # 2 LSB
        self.dst_port:     int   = 55000
        self.event_count:  int   = 0
        self.busy_count:   int   = 0
        self.throttle:     int   = 0xFFFF
        self.temperature:  float = 25.6   # degrees C, sent as DisplayString
        self.port_link_status: int = 2    # 2=up
        self.wrpc_sw_version: str = "wrpc-v4.2-dirty"
        self.wrpc_hw_type:    str = "NA"
        self.wrpc_build_by:   str = "UCTS Emulator"
        self.wrpc_build_date: str = "Jan  1 2025 00:00:00"
        self.temperature_name: str = "pcb"
        self.spll_mode:       int = 3   # 3=slave
        self.spll_irq_cnt:    int = 0
        self.spll_seq_state:  int = 8   # 8=ready
        self.sfp_pn:          bytes = b"BO15C3149620D   "  # 16 bytes
        self.sfp_in_db:       int = 1   # 1=notInDataBase
        self.port_internal_tx: int = 0
        self.port_internal_rx: int = 0
        self.aux_diag_ro_reg_nb: int = 8
        self.time_tai: int = int(time.time()) + 37

        # ── Standard MIB-II system group (1.3.6.1.2.1.1.*) ───────────────────
        self.sys_descr:    str = "UCTS-TiCkS Board Emulator v1.0"
        self.sys_contact:  str = "UCTS Emulator"
        self.sys_name:     str = socket.gethostname()
        self.sys_location: str = "Emulated"

        # ── SNMP statistics counters (1.3.6.1.2.1.11.*) ──────────────────────
        self.snmp_in_pkts:  int = 0
        self.snmp_out_pkts: int = 0

    # ── derived ───────────────────────────────────────────────────────────────

    @property
    def status_word(self) -> int:
        w = 0
        if self._ticks_state == 1:   # Running: cnt_en_ack=1
            w |= (1 << 7)
        elif self._ticks_state == 0: # Online: non-zero but bit7=0
            w |= (1 << 0)
        w |= (self._fw_version & 0xFF) << 16
        if self._spi_enabled:
            w |= (1 << 4)
        return w

    @property
    def status_bytes(self) -> bytes:
        return self.status_word.to_bytes(4, "big")

    @property
    def uptime_str(self) -> str:
        s = int(time.time() - _START_TIME)
        h, s = divmod(s, 3600)
        m, s = divmod(s, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    @property
    def tai_string(self) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(self.time_tai))

    # ── OID value map ─────────────────────────────────────────────────────────

    @property
    def sys_uptime_centiseconds(self) -> int:
        """sysUpTime: time since the agent was started, in hundredths of a second."""
        return int((time.time() - _START_TIME) * 100)

    @property
    def if_oper_status(self) -> int:
        """
        RFC 2863 ifOperStatus: 1=up, 2=down, 3=testing.
        Maps from port_link_status (0=n/a→2, 1=down→2, 2=up→1).
        """
        return 1 if self.port_link_status == 2 else 2

    def oid_table(self) -> Dict[Tuple[int, ...], bytes]:
        """
        Return the complete OID → encoded-TLV mapping for this state snapshot.
        Called once per request so all values are consistent within a response.
        """
        return {
            # ── WR-WRPC-MIB: Version group (wrpcCore.1.*) ─────────────────────
            (1,3,6,1,4,1,96,101,1,1,1,0): _ber_octetstr(self.wrpc_hw_type.encode()),
            (1,3,6,1,4,1,96,101,1,1,2,0): _ber_octetstr(self.wrpc_sw_version.encode()),
            (1,3,6,1,4,1,96,101,1,1,3,0): _ber_octetstr(self.wrpc_build_by.encode()),
            (1,3,6,1,4,1,96,101,1,1,4,0): _ber_octetstr(self.wrpc_build_date.encode()),

            # ── WR-WRPC-MIB: Time group (wrpcCore.2.*) ────────────────────────
            (1,3,6,1,4,1,96,101,1,2,1,0): _ber_counter64(self.time_tai),
            (1,3,6,1,4,1,96,101,1,2,2,0): _ber_octetstr(self.tai_string.encode()),
            (1,3,6,1,4,1,96,101,1,2,3,0): _ber_uint_app(_T_TIMETICKS, self.sys_uptime_centiseconds, 4),

            # ── WR-WRPC-MIB: Temperature table (wrpcCore.3.1.1.*) ─────────────
            # wrpcTemperatureIndex.1 is not-accessible; Name and Value are row .2 and .3
            (1,3,6,1,4,1,96,101,1,3,1,2,1): _ber_octetstr(self.temperature_name.encode()),
            (1,3,6,1,4,1,96,101,1,3,1,3,1): _ber_octetstr(
                f"{self.temperature:.4f}".rstrip("0").rstrip(".").encode()
            ),

            # ── WR-WRPC-MIB: SPLL group (wrpcCore.4.*) ────────────────────────
            (1,3,6,1,4,1,96,101,1,4,1,0): _ber_int(self.spll_mode),
            (1,3,6,1,4,1,96,101,1,4,2,0): _ber_uint_app(_T_COUNTER32, self.spll_irq_cnt, 4),
            (1,3,6,1,4,1,96,101,1,4,3,0): _ber_int(self.spll_seq_state),

            # ── WR-WRPC-MIB: Port group (wrpcCore.7.*) ────────────────────────
            (1,3,6,1,4,1,96,101,1,7,1,0): _ber_int(self.port_link_status),
            (1,3,6,1,4,1,96,101,1,7,2,0): _ber_octetstr(self.sfp_pn),
            (1,3,6,1,4,1,96,101,1,7,3,0): _ber_int(self.sfp_in_db),
            (1,3,6,1,4,1,96,101,1,7,4,0): _ber_uint_app(_T_COUNTER32, self.port_internal_tx, 4),
            (1,3,6,1,4,1,96,101,1,7,5,0): _ber_uint_app(_T_COUNTER32, self.port_internal_rx, 4),

            # ── AUX-DIAG MIB: wrpcAuxDiag101RoTable (wrpcCore.2.1.1.1.*) ──────
            (1,3,6,1,4,1,96,101,2,1,1,1,1,2,1):  _ber_uint_app(_T_GAUGE32, self.aux_diag_ro_reg_nb, 4),
            (1,3,6,1,4,1,96,101,2,1,1,1,1,3,1):  _ber_ipaddr(self.dst_ip),
            (1,3,6,1,4,1,96,101,2,1,1,1,1,4,1):  _ber_octetstr(self.dst_mac_h),
            (1,3,6,1,4,1,96,101,2,1,1,1,1,5,1):  _ber_octetstr(self.dst_mac_l + b"\x00\x00"),
            (1,3,6,1,4,1,96,101,2,1,1,1,1,6,1):  _ber_uint_app(_T_GAUGE32, self.dst_port, 4),
            (1,3,6,1,4,1,96,101,2,1,1,1,1,7,1):  _ber_uint_app(_T_GAUGE32, self.event_count, 4),
            (1,3,6,1,4,1,96,101,2,1,1,1,1,8,1):  _ber_uint_app(_T_GAUGE32, self.busy_count, 4),
            (1,3,6,1,4,1,96,101,2,1,1,1,1,9,1):  _ber_octetstr(self.status_bytes),
            (1,3,6,1,4,1,96,101,2,1,1,1,1,10,1): _ber_uint_app(_T_GAUGE32, self.throttle, 4),

            # ── MIB-II System group (RFC 3418 / 1.3.6.1.2.1.1.*) ─────────────
            (1,3,6,1,2,1,1,1,0): _ber_octetstr(self.sys_descr.encode()),
            (1,3,6,1,2,1,1,2,0): _ber_oid((1,3,6,1,4,1,96,101)),
            (1,3,6,1,2,1,1,3,0): _ber_uint_app(_T_TIMETICKS, self.sys_uptime_centiseconds, 4),
            (1,3,6,1,2,1,1,4,0): _ber_octetstr(self.sys_contact.encode()),
            (1,3,6,1,2,1,1,5,0): _ber_octetstr(self.sys_name.encode()),
            (1,3,6,1,2,1,1,6,0): _ber_octetstr(self.sys_location.encode()),
            (1,3,6,1,2,1,1,7,0): _ber_int(72),

            # ── MIB-II Interfaces group (RFC 2863 / 1.3.6.1.2.1.2.*) ─────────
            (1,3,6,1,2,1,2,1,0):     _ber_int(1),
            (1,3,6,1,2,1,2,2,1,1,1): _ber_int(1),
            (1,3,6,1,2,1,2,2,1,2,1): _ber_octetstr(b"WR Port 0"),
            (1,3,6,1,2,1,2,2,1,3,1): _ber_int(6),
            (1,3,6,1,2,1,2,2,1,7,1): _ber_int(1),
            (1,3,6,1,2,1,2,2,1,8,1): _ber_int(self.if_oper_status),

            # ── MIB-II SNMP group (RFC 3418 / 1.3.6.1.2.1.11.*) ─────────────
            (1,3,6,1,2,1,11,1,0):  _ber_uint_app(_T_COUNTER32, self.snmp_in_pkts,  4),
            (1,3,6,1,2,1,11,2,0):  _ber_uint_app(_T_COUNTER32, self.snmp_out_pkts, 4),
            (1,3,6,1,2,1,11,30,0): _ber_int(2),
        }

    def oid_value_tlv(self, oid: Tuple[int, ...]) -> Optional[bytes]:
        """Return the pre-encoded TLV value bytes for a given OID, or None."""
        return self.oid_table().get(oid)

    def next_oid_value_tlv(
        self, oid: Tuple[int, ...]
    ) -> Optional[Tuple[Tuple[int, ...], bytes]]:
        """
        Return (next_oid, tlv) for the lexicographically next OID after *oid*,
        or None if *oid* is at or past the end of the MIB.
        Used to serve GETNEXT requests (and therefore snmpwalk).
        """
        table = self.oid_table()
        sorted_oids = sorted(table)
        for candidate in sorted_oids:
            if candidate > oid:
                return candidate, table[candidate]
        return None

    # ── state mutators ────────────────────────────────────────────────────────

    def apply_reset(self) -> None:
        self._ticks_state = 0
        self.event_count  = 0
        self.busy_count   = 0
        log.info("[state] Reset: state=Online, counters zeroed")

    def apply_get_ready(self) -> None:
        self._ticks_state = 1
        log.info("[state] GetReady: state=Running")

    def apply_set_mac(self, msb: bytes, lsb: bytes) -> None:
        self.dst_mac_h = msb
        self.dst_mac_l = lsb
        mac = ":".join(f"{b:x}" for b in msb + lsb)
        log.info("[state] SetMAC: %s", mac)

    def apply_set_dst_ip(self, ip: str) -> None:
        self.dst_ip = ip
        log.info("[state] SetDstIp: %s", ip)

    def apply_set_dst_port(self, port: int) -> None:
        self.dst_port = port
        log.info("[state] SetDstPort: %d", port)

    def apply_set_throttle(self, value: int) -> None:
        self.throttle = value
        log.info("[state] SetThrottle: 0x%04X", value)

    def apply_spi(self, enable: bool) -> None:
        self._spi_enabled = enable
        log.info("[state] SPI: %s", "enabled" if enable else "disabled")

    def show(self) -> str:
        mac = ":".join(f"{b:x}" for b in self.dst_mac_h + self.dst_mac_l)
        state_names = {0: "Online/Standby", 1: "Running", 2: "Unknown"}
        spll_modes  = {0: "na", 1: "grandmaster", 2: "master", 3: "slave"}
        spll_states = {1: "startup", 2: "sync_nsec", 3: "sync_sec", 4: "sync_phase",
                       5: "track_phase", 6: "wait_offs", 8: "ready"}
        sfp_db      = {1: "notInDataBase", 2: "inDataBase"}
        return "\n".join([
            f"  State           : {self._ticks_state} ({state_names.get(self._ticks_state,'?')})",
            f"  Status word     : 0x{self.status_word:08X}  ({self.status_word})",
            f"  FirmwareVersion : {self._fw_version}",
            f"  SPI enabled     : {self._spi_enabled}",
            f"  DstIpAddr       : {self.dst_ip}",
            f"  DstMacAddr      : {mac}",
            f"  DstPort         : {self.dst_port}",
            f"  EventCount      : {self.event_count}",
            f"  BusyCount       : {self.busy_count}",
            f"  Throttle        : 0x{self.throttle:04X}  ({self.throttle})",
            f"  Temperature     : {self.temperature:.4f} °C",
            f"  PortLinkStatus  : {self.port_link_status}  (2=up)",
            f"  TimeTAI         : {self.time_tai}",
            f"  TimeTAIString   : {self.tai_string}",
            f"  UpTime          : {self.uptime_str}",
            f"  WrpcSwVersion   : {self.wrpc_sw_version}",
            f"  WrpcHwType      : {self.wrpc_hw_type}",
            f"  WrpcBuildBy     : {self.wrpc_build_by}",
            f"  WrpcBuildDate   : {self.wrpc_build_date}",
            f"  SpllMode        : {self.spll_mode}  ({spll_modes.get(self.spll_mode,'?')})",
            f"  SpllIrqCnt      : {self.spll_irq_cnt}",
            f"  SpllSeqState    : {self.spll_seq_state}  ({spll_states.get(self.spll_seq_state,'?')})",
            f"  SfpPn           : {self.sfp_pn!r}",
            f"  SfpInDB         : {self.sfp_in_db}  ({sfp_db.get(self.sfp_in_db,'?')})",
            f"  PortInternalTx  : {self.port_internal_tx}",
            f"  PortInternalRx  : {self.port_internal_rx}",
            f"  --- Standard MIB-II ---",
            f"  sysDescr        : {self.sys_descr}",
            f"  sysContact      : {self.sys_contact}",
            f"  sysName         : {self.sys_name}",
            f"  sysLocation     : {self.sys_location}",
            f"  sysUpTime       : {self.sys_uptime_centiseconds} cs  ({self.uptime_str})",
            f"  ifOperStatus    : {self.if_oper_status}  (1=up, 2=down)",
            f"  snmpInPkts      : {self.snmp_in_pkts}",
            f"  snmpOutPkts     : {self.snmp_out_pkts}",
        ])


state = UCTSState()

# ─────────────────────────────────────────────────────────────────────────────
# SNMP agent  (hand-rolled BER, no pysnmp high-level API)
# ─────────────────────────────────────────────────────────────────────────────

class SNMPAgentProtocol(asyncio.DatagramProtocol):
    """SNMPv2c GET responder using the hand-rolled BER codec above."""

    def __init__(self) -> None:
        self._transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self._transport = transport
        log.info("SNMP agent ready")

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        parsed = _parse_snmp_request(data)
        if parsed is None:
            log.debug("SNMP: ignored non-GET/GETNEXT packet from %s", addr)
            return
        version, community, request_id, pdu_type, oids = parsed
        log.debug("SNMP %s from %s  req_id=%d  oids=%d",
                  "GET" if pdu_type == _T_GET_REQ else "GETNEXT",
                  addr, request_id, len(oids))

        state.snmp_in_pkts += 1

        var_binds: List[Tuple[Tuple[int, ...], bytes]] = []
        for oid in oids:
            if pdu_type == _T_GET_REQ:
                tlv = state.oid_value_tlv(oid)
                if tlv is None:
                    log.debug("  GET  %s -> NoSuchInstance", oid)
                    tlv = _ber_nosuchinstance()
                    var_binds.append((oid, tlv))
                else:
                    log.debug("  GET  %s -> %s", oid, tlv.hex())
                    var_binds.append((oid, tlv))
            else:  # GETNEXT
                result = state.next_oid_value_tlv(oid)
                if result is None:
                    log.debug("  GETNEXT %s -> EndOfMibView", oid)
                    var_binds.append((oid, _ber_endofmibview()))
                else:
                    next_oid, tlv = result
                    log.debug("  GETNEXT %s -> %s  %s", oid, next_oid, tlv.hex())
                    var_binds.append((next_oid, tlv))

        response = _build_response(community, request_id, var_binds)
        state.snmp_out_pkts += 1
        self._transport.sendto(response, addr)

    def error_received(self, exc: Exception) -> None:
        log.error("SNMP socket error: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# UDP command listener  (TiCkS command protocol)
# ─────────────────────────────────────────────────────────────────────────────

class TiCkSCommandProtocol(asyncio.DatagramProtocol):
    """
    Accepts 8-byte TiCkS command packets, decodes them per the ICD,
    updates emulated state, and echoes each packet back as acknowledgement.
    """

    def __init__(self) -> None:
        self._transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self._transport = transport
        log.info("TiCkS command listener ready")

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        if len(data) != 8:
            log.warning("CMD: unexpected length %d from %s", len(data), addr)
            return
        word = int.from_bytes(data, "big")
        log.info("CMD recv: %s from %s", data.hex().upper(), addr)

        if word == 0xFFFFFFFFFFFFFFF0:
            state.apply_get_ready()
        elif word == 0xFFFFFFFFFFFFFF00:
            state.apply_reset()
        else:
            func = word & 0xF
            if func == 0x1:
                mac_int = (word >> 4) & 0xFFFFFFFFFFFF
                mac_bytes = mac_int.to_bytes(6, "big")
                state.apply_set_mac(mac_bytes[:4], mac_bytes[4:])
            elif func == 0x2:
                sub_ns8 = (word >> 4)  & 0x0FFFFFFF
                tai_sec = (word >> 32) & 0x01FFFFFF
                log.info("CMD ScheduleTrigger: TAI=%d sub_ns8=%d", tai_sec, sub_ns8)
            elif func == 0x3:
                state.apply_set_throttle((word >> 4) & 0xFFFF)
            elif func == 0x4:
                ip_int = (word >> 4) & 0xFFFFFFFF
                ip_str = ".".join(str((ip_int >> (8*i)) & 0xFF) for i in (3,2,1,0))
                state.apply_set_dst_ip(ip_str)
            elif func == 0x5:
                state.apply_spi(bool((word >> 4) & 0xFF))
            elif func == 0x6:
                state.apply_set_dst_port((word >> 4) & 0xFFFF)
            else:
                log.warning("CMD: unknown function 0x%X", func)

        self._transport.sendto(data, addr)   # echo-back acknowledge


# ─────────────────────────────────────────────────────────────────────────────
# Background: TAI time updater
# ─────────────────────────────────────────────────────────────────────────────

async def _update_time() -> None:
    while True:
        state.time_tai = int(time.time()) + 37
        await asyncio.sleep(1.0)


# ─────────────────────────────────────────────────────────────────────────────
# Interactive terminal CLI
# ─────────────────────────────────────────────────────────────────────────────

_HELP = """
Commands:
  show                     Print all current variable values
  set Temperature <val>    degrees C (e.g. 25.6)
  set EventCount <val>     Event counter
  set BusyCount <val>      Busy counter
  set Throttle <val>       Throttle (decimal or 0x hex)
  set DstIpAddr <x.x.x.x> Destination IP
  set DstPort <val>        Destination port
  set WrpcSwVersion <str>  WR software version string
  set WrpcHwType <str>     WR hardware type string
  set WrpcBuildBy <str>    WR build-by string
  set WrpcBuildDate <str>  WR build date string
  set SpllMode <n>         SPLL mode (0=na,1=grandmaster,2=master,3=slave)
  set SpllSeqState <n>     SPLL sequencer state (8=ready)
  set FirmwareVersion <n>  Firmware version integer (bits 23:16 of status)
  set PortLinkStatus <n>   Port link status (0=na, 1=down, 2=up)
  set SysDescr <str>       sysDescr (MIB-II 1.3.6.1.2.1.1.1.0)
  set SysContact <str>     sysContact (MIB-II 1.3.6.1.2.1.1.4.0)
  set SysName <str>        sysName (MIB-II 1.3.6.1.2.1.1.5.0)
  set SysLocation <str>    sysLocation (MIB-II 1.3.6.1.2.1.1.6.0)
  state <0|1|2>            TiCkS state (0=Online, 1=Running, 2=Unknown)
  status <hex_or_dec>      Set raw status word (overrides state/fw/spi)
  reset                    Apply Reset (state=Online, counters=0)
  getready                 Apply GetReady (state=Running)
  help                     Show this help
  quit / exit              Shut down
"""


def _apply_terminal_command(line: str) -> bool:
    parts = line.strip().split(None, 2)
    if not parts:
        return True
    cmd = parts[0].lower()

    if cmd in ("quit", "exit"):
        return False
    elif cmd == "help":
        print(_HELP)
    elif cmd == "show":
        print(state.show())
    elif cmd == "reset":
        state.apply_reset()
        print("  -> state=Online, counters zeroed")
    elif cmd == "getready":
        state.apply_get_ready()
        print("  -> state=Running")
    elif cmd == "state":
        if len(parts) < 2:
            print("Usage: state <0|1|2>")
        else:
            try:
                v = int(parts[1])
                if v not in (0, 1, 2): raise ValueError
                state._ticks_state = v
                print(f"  -> state={v}")
            except ValueError:
                print("state must be 0, 1, or 2")
    elif cmd == "status":
        if len(parts) < 2:
            print("Usage: status <hex_or_decimal>")
        else:
            try:
                raw = int(parts[1], 0)
                state._ticks_state = 2 if raw == 0 else int((raw >> 7) & 0x1)
                state._fw_version  = (raw >> 16) & 0xFF
                state._spi_enabled = bool((raw >> 4) & 0x1)
                print(f"  -> status=0x{raw:08X}  state={state._ticks_state}  fw={state._fw_version}")
            except ValueError:
                print("Invalid value -- use decimal or 0x hex")
    elif cmd == "set":
        if len(parts) < 3:
            print("Usage: set <variable> <value>")
        else:
            _apply_set(parts[1], parts[2])
    else:
        print(f"Unknown command: {cmd!r}  (type 'help')")
    return True


def _apply_set(var: str, val: str) -> None:
    try:
        if var == "Temperature":
            state.temperature = float(val)
            print(f"  -> Temperature={state.temperature:.4f} °C")
        elif var == "EventCount":
            state.event_count = int(val)
            print(f"  -> EventCount={state.event_count}")
        elif var == "BusyCount":
            state.busy_count = int(val)
            print(f"  -> BusyCount={state.busy_count}")
        elif var == "Throttle":
            state.throttle = int(val, 0)
            print(f"  -> Throttle=0x{state.throttle:04X}")
        elif var == "DstIpAddr":
            state.dst_ip = val
            print(f"  -> DstIpAddr={state.dst_ip}")
        elif var == "DstPort":
            state.dst_port = int(val)
            print(f"  -> DstPort={state.dst_port}")
        elif var == "WrpcSwVersion":
            state.wrpc_sw_version = val
            print(f"  -> WrpcSwVersion={state.wrpc_sw_version}")
        elif var == "WrpcHwType":
            state.wrpc_hw_type = val
            print(f"  -> WrpcHwType={state.wrpc_hw_type}")
        elif var == "WrpcBuildBy":
            state.wrpc_build_by = val
            print(f"  -> WrpcBuildBy={state.wrpc_build_by}")
        elif var == "WrpcBuildDate":
            state.wrpc_build_date = val
            print(f"  -> WrpcBuildDate={state.wrpc_build_date}")
        elif var == "SpllMode":
            state.spll_mode = int(val)
            print(f"  -> SpllMode={state.spll_mode}")
        elif var == "SpllSeqState":
            state.spll_seq_state = int(val)
            print(f"  -> SpllSeqState={state.spll_seq_state}")
        elif var == "FirmwareVersion":
            state._fw_version = int(val)
            print(f"  -> FirmwareVersion={state._fw_version}  (status=0x{state.status_word:08X})")
        elif var == "PortLinkStatus":
            state.port_link_status = int(val)
            print(f"  -> PortLinkStatus={state.port_link_status}")
        elif var == "SysDescr":
            state.sys_descr = val
            print(f"  -> SysDescr={state.sys_descr}")
        elif var == "SysContact":
            state.sys_contact = val
            print(f"  -> SysContact={state.sys_contact}")
        elif var == "SysName":
            state.sys_name = val
            print(f"  -> SysName={state.sys_name}")
        elif var == "SysLocation":
            state.sys_location = val
            print(f"  -> SysLocation={state.sys_location}")
        else:
            print(f"Unknown variable: {var!r}")
    except ValueError as exc:
        print(f"Invalid value {val!r}: {exc}")


async def _terminal_loop(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    print("\nUCTS Emulator ready. Type 'help' for commands, 'quit' to exit.\n")
    print(state.show())
    print()
    while not stop_event.is_set():
        try:
            line = await loop.run_in_executor(None, lambda: input("ucts> "))
        except EOFError:
            break
        if not _apply_terminal_command(line):
            stop_event.set()
            break


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="UCTS-TiCkS board emulator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--snmp-port", type=int, default=1161,
                   help="SNMP UDP port (161 requires root)")
    p.add_argument("--cmd-port",  type=int, default=55010,
                   help="TiCkS command UDP port")
    p.add_argument("--bind",      default="0.0.0.0",
                   help="Interface address to bind")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()



def _bind_addresses(bind: str) -> list:
    """
    Return the list of addresses to bind.

    When bind is the wildcard "0.0.0.0" we bind both "0.0.0.0" (IPv4) and
    "::" (IPv6) so that clients reaching us via either 127.0.0.1 or ::1
    (macOS resolves "localhost" to ::1 by default) are both served.
    When bind is any other specific address, bind only that address.
    """
    if bind == "0.0.0.0":
        return ["0.0.0.0", "::"]
    return [bind]


async def _async_main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    # Bind SNMP on both IPv4 and IPv6 so that clients using either
    # "localhost" -> 127.0.0.1 or "localhost" -> ::1 (macOS default) both work.
    snmp_transports = []
    for bind_addr in _bind_addresses(args.bind):
        try:
            t, _ = await loop.create_datagram_endpoint(
                SNMPAgentProtocol,
                local_addr=(bind_addr, args.snmp_port),
            )
            snmp_transports.append(t)
            log.info("SNMP agent bound to %s:%d", bind_addr, args.snmp_port)
        except OSError as exc:
            log.debug("SNMP bind %s:%d skipped: %s", bind_addr, args.snmp_port, exc)

    cmd_transports = []
    for bind_addr in _bind_addresses(args.bind):
        try:
            t, _ = await loop.create_datagram_endpoint(
                TiCkSCommandProtocol,
                local_addr=(bind_addr, args.cmd_port),
            )
            cmd_transports.append(t)
            log.info("TiCkS command listener bound to %s:%d", bind_addr, args.cmd_port)
        except OSError as exc:
            log.debug("Cmd bind %s:%d skipped: %s", bind_addr, args.cmd_port, exc)

    time_task = asyncio.create_task(_update_time())
    term_task  = asyncio.create_task(_terminal_loop(stop_event))

    await stop_event.wait()

    time_task.cancel()
    term_task.cancel()
    for t in snmp_transports: t.close()
    for t in cmd_transports:  t.close()
    await asyncio.gather(time_task, term_task, return_exceptions=True)
    log.info("Emulator stopped")


def main() -> None:
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        print("\nInterrupted")


if __name__ == "__main__":
    main()
