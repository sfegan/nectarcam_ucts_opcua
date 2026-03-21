"""
ucts_asyncua_server.py
──────────────────────
OPC UA server for the UCTS (Universal Clock and Time Stamping) controller.

Three-class design, each with a single responsibility:

  UCTSPoller(SNMPPoller)
      Pure SNMP monitoring.  Overrides the three SNMPPoller hooks:
        build_variable_specs()   - local (underscore-prefixed) OIDs are already
                                   excluded by the base class; derived variables
                                   (Status, DstMacAddr, State, FirmwareVersion,
                                   Temperature) are declared as constants with
                                   value=None so the base creates them with
                                   BadWaitingForInitialData.
        write_variables()        - computes derived store entries from local OIDs
                                   when their sources are Good, or delegates
                                   staleness to self._apply_staleness() when they
                                   are not.  Applies in-place transformations to
                                   PortLinkStatus and TimeTAIString.  Calls
                                   super().write_variables() to perform the actual
                                   OPC UA writes.
        on_address_space_ready() - nothing extra; pure monitoring only.

  UCTSCommander
      All UDP command logic for TiCkS.  Knows nothing about OPC UA nodes or
      SNMP.  Exposes one method per ICD command and a register_methods() entry
      point that installs them as OPC UA Method nodes on any given parent node.

  UCTSOPCUAServer(OPCUAServer)
      Overrides _build_address_space() to call super() (which builds
      Objects/UCTS/Monitoring via the registered UCTSPoller), then resolves the
      already-created Objects/UCTS root node and calls
      UCTSCommander.register_methods() to attach the command methods there.

Node layout (root_path="UCTS", opcua_path="Monitoring"):

  Objects/
  UCTS/                    <- root node, created by OPCUAServer._ensure_path
      Configure()          <- UCTSCommander methods registered here
      Start()
      Reset()
      ScheduleTrigger()
      XMLConfiguration()
      SetDstIpAddress()
      SetDstPort()
      Monitoring/          <- UCTSPoller device node (self._device_node)
          BusyCount
          DstIpAddr
          DstMacAddr       <- derived: merged from _DstMacAddr_32MSB/_DstMacAddr_16LSB
          DstPort
          EventCount
          FirmwareVersion  <- derived: from Status bits 23:16
          PortLinkStatus   <- transformed: INTEGER enum → "na"/"down"/"up"
          State            <- derived: from Status bit 7
          Status           <- derived: raw uint32 as Int64
          Temperature      <- polled: DisplayString decoded to Float °C directly
          Throttle
          TimeTAI
          TimeTAIString    <- transformed: ISO 8601 with T separator
          UpTime           <- polled: timedelta → formatted String (e.g. "5:23:49.160000")
          UpTimeMilliseconds <- derived: UpTime timedelta → Double milliseconds
          WrpcSwVersion
          SoftwareVersion  <- constant
          snmp_host                   <- built-in: SNMP device IP
          snmp_port                   <- built-in: SNMP port
          snmp_polling_timestamp      <- built-in
          snmp_polling_age            <- built-in
          snmp_polling_interval       <- built-in
          snmp_polling_success_count  <- built-in: cumulative successful polls
          snmp_server_online          <- built-in: True when SNMP agent reachable
          cls_state                   <- built-in: 0=offline 1=online

  Internal (local) OIDs — polled and held in self._store, no OPC UA node:
      _DstMacAddr_32MSB    <- 4 MSB of destination MAC (ByteString)
      _DstMacAddr_16LSB    <- 2 LSB of destination MAC (ByteString, device pads to 4 bytes)
      _RawStatus           <- uint32 status word as bytes (ByteString)

Usage
-----
  python ucts_asyncua_server.py [options]

  --ucts-ip IP          IP of the UCTS-TiCkS board  (default: 10.10.3.99)
  --ucts-snmp-port N    SNMP port                    (default: 161)
  --ucts-cmd-port N     UDP command port             (default: 55010)
  --snmp-community S    SNMP community string        (default: public)
  --poll-interval F     Poll interval in seconds     (default: 1.0)
  --snmp-timeout F      SNMP request timeout in seconds (default: 2.0)
  --snmp-retries N      SNMP retries after first attempt (default: 1)
  --monitoring-path S   OPC UA path for monitoring node (default: Monitoring)
  --variable-lifetime F Variable lifetime in seconds    (default: 120.0)
  --opcua-endpoint URL  OPC UA endpoint URL          (default: opc.tcp://0.0.0.0:4840/ucts/)
  --opcua-namespace URI OPC UA namespace URI
  --opcua-user U:P      Enable username/password auth
  --log-level LEVEL     DEBUG/INFO/WARNING/ERROR     (default: INFO)
  --log-file PATH       Optional rotating log file

Dependencies
------------
  pip install pysnmp-lextudio asyncua
  snmp_asyncua_bridge.py must be importable (same directory or on PYTHONPATH)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import socket
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# ── bridge imports ────────────────────────────────────────────────────────────
try:
    from snmp_asyncua_bridge import (
        NodeSpec,
        OPCUAServer,
        SNMPPoller,
        StoreEntry,
        _cast_to_ua,
        setup_logging,
    )
except ImportError:
    sys.exit(
        "snmp_asyncua_bridge.py not found. "
        "Place it in the same directory or add it to PYTHONPATH."
    )

# ── asyncua ───────────────────────────────────────────────────────────────────
try:
    from asyncua import Server, ua
    from asyncua.common.methods import uamethod
except ImportError:
    sys.exit("asyncua is required:  pip install asyncua")

log = logging.getLogger("ucts_server")

# ─────────────────────────────────────────────────────────────────────────────
# Hardcoded UCTS device configuration
# All OIDs already in dotted-decimal numeric form.
#
# Local (underscore-prefixed) OIDs are polled and held in self._store but
# never published as OPC UA nodes:
#   _DstMacAddr_32MSB / _DstMacAddr_16LSB  → merged into DstMacAddr
#   _RawStatus                              → decoded into Status/State/FirmwareVersion
#
# Derived variables are declared as constants with value=None so the base
# class creates their OPC UA nodes with BadWaitingForInitialData; they are
# computed and their store entries updated in write_variables().
# ─────────────────────────────────────────────────────────────────────────────

_UCTS_CONFIG: dict = {
    "host":          "10.10.3.99",   # overridden at runtime via --ucts-ip
    "port":          161,
    "community":     "public",
    "description":   "UCTS SNMP Device",
    "opcua_path":    "Monitoring",
    "poll_interval": 1,
    "oids": [
        {
            "oid":         "1.3.6.1.4.1.96.101.2.1.1.1.1.8.1",
            "opcua_name":  "BusyCount",
            "opcua_type":  "Int32",
            "description": "Number of busy triggers rejected during the run",
        },
        {
            "oid":         "1.3.6.1.4.1.96.101.2.1.1.1.1.3.1",
            "opcua_name":  "DstIpAddr",
            "opcua_type":  "String",
            "description": "Destination IP address of UCTS timestamps",
        },
        {
            "oid":         "1.3.6.1.4.1.96.101.2.1.1.1.1.4.1",
            "opcua_name":  "_DstMacAddr_32MSB",
            "opcua_type":  "ByteString",
            "description": "(internal) 32 MSB of destination MAC address",
        },
        {
            "oid":         "1.3.6.1.4.1.96.101.2.1.1.1.1.5.1",
            "opcua_name":  "_DstMacAddr_16LSB",
            "opcua_type":  "ByteString",
            "description": "(internal) 16 LSB of destination MAC address",
        },
        {
            "oid":         "1.3.6.1.4.1.96.101.2.1.1.1.1.6.1",
            "opcua_name":  "DstPort",
            "opcua_type":  "Int32",
            "description": "Destination port of UCTS timestamps",
        },
        {
            "oid":         "1.3.6.1.4.1.96.101.2.1.1.1.1.7.1",
            "opcua_name":  "EventCount",
            "opcua_type":  "Int32",
            "description": "Number of triggers accepted during the run",
        },
        {
            # wrpcAuxDiagStatus — ASN_OCTET_STR encoding a uint32 bitmask.
            # Declared ByteString so _cast_to_ua leaves raw bytes intact.
            "oid":         "1.3.6.1.4.1.96.101.2.1.1.1.1.9.1",
            "opcua_name":  "_RawStatus",
            "opcua_type":  "ByteString",
            "description": "(internal) raw uint32 status word as bytes",
        },
        {
            "oid":         "1.3.6.1.4.1.96.101.2.1.1.1.1.10.1",
            "opcua_name":  "Throttle",
            "opcua_type":  "Int64",
            "description": "Throttle parameter of UCTS TiCkS",
        },
        {
            "oid":         "1.3.6.1.4.1.96.101.1.7.1.0",
            "opcua_name":  "PortLinkStatus",
            "opcua_type":  "String",
            "description": "Port link status",
        },
        {
            # wrpcTemperatureValue — MIB SYNTAX is DisplayString, device sends
            # a decimal float string e.g. "41.9375" (degrees C directly).
            # The bridge's _cast_to_ua() decodes the bytes to str before
            # calling float(), so no derived variable or scaling is needed.
            "oid":         "1.3.6.1.4.1.96.101.1.3.1.3.1",
            "opcua_name":  "Temperature",
            "opcua_type":  "Float",
            "description": "Temperature of the TiCkS PCB (degrees C)",
        },
        {
            "oid":         "1.3.6.1.4.1.96.101.1.2.1.0",
            "opcua_name":  "TimeTAI",
            "opcua_type":  "Int64",
            "description": "TAI time",
        },
        {
            "oid":         "1.3.6.1.4.1.96.101.1.2.2.0",
            "opcua_name":  "TimeTAIString",
            "opcua_type":  "String",
            "description": "TAI time string (ISO 8601)",
        },
        {
            "oid":         "1.3.6.1.4.1.96.101.1.2.3.0",
            "opcua_name":  "UpTime",
            "opcua_type":  "String",
            "description": "Uptime of the UCTS (formatted string, e.g. '5:23:49.160000')",
        },
        {
            "oid":         "1.3.6.1.4.1.96.101.1.1.2.0",
            "opcua_name":  "WrpcSwVersion",
            "opcua_type":  "String",
            "description": "Version of the wrpc software",
        },
    ],
    "constants": [
        {
            "opcua_name":  "SoftwareVersion",
            "opcua_type":  "String",
            "description": "Version of the UCTS controller",
            "value":       "2.0.0",
        },
        # Derived variables — computed in write_variables() from local OIDs.
        # value=None → base class creates OPC UA node with BadWaitingForInitialData.
        # Their lifetime settings govern how long they remain UncertainLastUsableValue
        # when their source OIDs are unavailable (0 = never expire).
        {
            "opcua_name":  "Status",
            "opcua_type":  "Int64",
            "description": "Status of TiCkS board (raw uint32 status word)",
            "value":       None,
        },
        {
            "opcua_name":  "DstMacAddr",
            "opcua_type":  "String",
            "description": "Destination MAC address (aa:bb:cc:dd:ee:ff)",
            "value":       None,
        },
        {
            "opcua_name":  "State",
            "opcua_type":  "Int32",
            "description": "TiCkS state: 1=Running, 0=Online, 2=Unknown",
            "value":       None,
        },
        {
            "opcua_name":  "FirmwareVersion",
            "opcua_type":  "String",
            "description": "TiCkS firmware version (from Status bits 23:16)",
            "value":       None,
        },
        {
            "opcua_name":  "UpTimeMilliseconds",
            "opcua_type":  "Double",
            "description": "Uptime of the UCTS in milliseconds (derived from UpTime)",
            "value":       None,
        },
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
# SNMP value transformations  (ported from snmp_ucts.cpp)
# ─────────────────────────────────────────────────────────────────────────────

def _octetstr_to_uint32(raw_bytes: bytes) -> int:
    """
    Reconstruct a uint32 from an ASN_OCTET_STR value via big-endian byte-shift,
    matching C++ get_ucts_status():
        counter = (counter << 8) | (value[i] & 0xFF)
    """
    result = 0
    for b in raw_bytes:
        result = (result << 8) | (b & 0xFF)
    return result


def _merge_mac(msb: bytes, lsb: bytes) -> str:
    """
    Merge the two PhysAddress OIDs into a colon-separated MAC address string.

    Both OIDs arrive as raw bytes (pysnmp decodes PhysAddress as OctetString).
    The MSB field carries the 4 most-significant bytes of the MAC address.
    The LSB field is documented as "16 lsb" (2 bytes) but the device pads it
    to 4 bytes (e.g. b'\\x8f\\x28\\x00\\x00'); only the first 2 bytes are used.

    Format follows C++ get_ucts_dst_mac_addr(): lowercase hex, no zero-padding,
    colon-separated — e.g. "68:5:ca:3a:8f:28".  Note that zero-padding is not
    applied (matching the C++ %x format), so single-nibble bytes appear without
    a leading zero.
    """
    b_msb = (msb + b"\x00" * 4)[:4]
    b_lsb = (lsb + b"\x00" * 2)[:2]
    return ":".join(f"{b:02X}" for b in b_msb + b_lsb)


def _state_from_status(status: int) -> int:
    """bit 7: 1=Running, 0=Online/Standby; 0 input → 2=Unknown."""
    return 2 if status == 0 else int((status >> 7) & 0x1)


def _fw_version_from_status(status: int) -> str:
    """Firmware version integer from bits 23:16 of the status word."""
    return "" if status == 0 else str((status >> 16) & 0xFF)


def _good_dv(value: Any, opcua_type: str) -> ua.DataValue:
    """Shorthand: build a Good ua.DataValue from a Python value."""
    variant = _cast_to_ua(value, opcua_type)
    return ua.DataValue(variant) if isinstance(variant, ua.Variant) else variant


def _entry_is_good(entry: Optional[StoreEntry]) -> bool:
    """
    Return True if *entry* exists and carries Good OPC UA status (StatusCode == 0).

    Used to gate derived-value computation: a derived variable is only
    recalculated when all the source entries it depends on were refreshed with
    Good status in the current polling iteration.
    """
    if entry is None:
        return False
    sc = entry.data_value.StatusCode
    return sc is not None and sc.value == 0


# ─────────────────────────────────────────────────────────────────────────────
# UCTSPoller -- pure SNMP monitoring
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class UCTSPoller(SNMPPoller):
    """
    SNMPPoller subclass for the UCTS-TiCkS board.

    Responsibilities
    ----------------
    - Poll all UCTS SNMP OIDs on the configured interval.
    - Compute derived store entries from local OIDs each cycle:
        · _DstMacAddr_32MSB + _DstMacAddr_16LSB  →  DstMacAddr  (String)
        · _RawStatus  →  Status (Int64), State (Int32), FirmwareVersion (String)
        · UpTime (String timedelta)  →  UpTimeMilliseconds (Double, ms)
    - Apply in-place transformations to published OIDs:
        · PortLinkStatus: INTEGER enum  →  "na" / "down" / "up"
        · TimeTAIString: reformat to ISO 8601 T separator
    - When a source OID is not Good this cycle, delegate to
      self._apply_staleness() on the derived entry so staleness and lifetime
      expiry are handled consistently for both polled and derived variables.

    No knowledge of UDP commands or OPC UA Methods.
    """

    # ── write_variables ───────────────────────────────────────────────────────

    async def write_variables(self) -> None:
        """
        Compute derived store entries then delegate to super() for OPC UA writes.

        For each derived variable: if all source OID entries are Good this cycle,
        compute the new value and update the entry's timestamp to now.  If any
        source is not Good, call self._apply_staleness(name, entry, now) on the
        derived entry — the base class single-entry method — so that its status
        degrades (Uncertain → BadNoCommunication) according to its own lifetime
        setting, consistently with how polled OIDs are handled.

        In-place transformations (PortLinkStatus enum mapping, TimeTAIString
        reformatting) update an already-Good entry's data_value without touching
        its timestamp, since the underlying OID was refreshed this cycle by the
        base-class poll loop.

        super().write_variables() then iterates self._store and writes every
        non-local entry to its OPC UA node.
        """
        now = time.monotonic()

        # ── MAC address: merge two OctetString halves ─────────────────────────
        msb_entry = self._store.get("_DstMacAddr_32MSB")
        lsb_entry = self._store.get("_DstMacAddr_16LSB")
        mac_entry  = self._store.get("DstMacAddr")

        if mac_entry is not None:
            if _entry_is_good(msb_entry) and _entry_is_good(lsb_entry):
                msb_raw = msb_entry.data_value.Value.Value
                lsb_raw = lsb_entry.data_value.Value.Value
                if isinstance(msb_raw, (bytes, bytearray)) and \
                   isinstance(lsb_raw, (bytes, bytearray)):
                    try:
                        mac_entry.data_value = _good_dv(
                            _merge_mac(bytes(msb_raw), bytes(lsb_raw)), "String"
                        )
                        mac_entry.timestamp = now
                    except Exception as exc:
                        log.warning("MAC merge error: %s", exc)
                        self._apply_staleness("DstMacAddr", mac_entry, now)
                else:
                    log.warning("_DstMacAddr halves not bytes: msb=%r lsb=%r",
                                msb_raw, lsb_raw)
                    self._apply_staleness("DstMacAddr", mac_entry, now)
            else:
                self._apply_staleness("DstMacAddr", mac_entry, now)

        # ── Status word: ByteString → uint32 → Status / State / FirmwareVersion
        raw_entry    = self._store.get("_RawStatus")
        status_entry = self._store.get("Status")
        state_entry  = self._store.get("State")
        fw_entry     = self._store.get("FirmwareVersion")

        if _entry_is_good(raw_entry):
            raw = raw_entry.data_value.Value.Value
            if isinstance(raw, (bytes, bytearray)):
                try:
                    status_int = _octetstr_to_uint32(bytes(raw))
                    for entry, name, val in (
                        (status_entry, "Status",          status_int),
                        (state_entry,  "State",           _state_from_status(status_int)),
                        (fw_entry,     "FirmwareVersion", _fw_version_from_status(status_int)),
                    ):
                        if entry is not None:
                            entry.data_value = _good_dv(val, entry.opcua_type)
                            entry.timestamp  = now
                except Exception as exc:
                    log.warning("Status decode error: %s", exc)
                    for entry, name in (
                        (status_entry, "Status"),
                        (state_entry,  "State"),
                        (fw_entry,     "FirmwareVersion"),
                    ):
                        if entry is not None:
                            self._apply_staleness(name, entry, now)
            else:
                log.warning("_RawStatus value not bytes: %r", raw)
                for entry, name in (
                    (status_entry, "Status"),
                    (state_entry,  "State"),
                    (fw_entry,     "FirmwareVersion"),
                ):
                    if entry is not None:
                        self._apply_staleness(name, entry, now)
        else:
            for entry, name in (
                (status_entry, "Status"),
                (state_entry,  "State"),
                (fw_entry,     "FirmwareVersion"),
            ):
                if entry is not None:
                    self._apply_staleness(name, entry, now)

        # ── UpTimeMilliseconds: derived from UpTime (timedelta → Double ms) ────
        # UpTime is a String (C++ compatible, e.g. "5:23:49.160000").
        # UpTimeMilliseconds is a Double for clients that need arithmetic.
        # The store value is a timedelta until _cast_to_ua converts it to String;
        # we read it before that conversion happens.
        uptime_entry   = self._store.get("UpTime")
        uptime_ms_entry = self._store.get("UpTimeMilliseconds")
        if uptime_ms_entry is not None:
            if _entry_is_good(uptime_entry):
                try:
                    td = uptime_entry.data_value.Value.Value
                    if isinstance(td, str):
                        # Already converted to string — parse it back
                        # Format is H:MM:SS.ffffff or H:MM:SS
                        import datetime as _dt
                        parts = td.split(":")
                        h, m, s = int(parts[0]), int(parts[1]), float(parts[2])
                        td = _dt.timedelta(hours=h, minutes=m, seconds=s)
                    ms = td.total_seconds() * 1000.0
                    uptime_ms_entry.data_value = _good_dv(ms, "Double")
                    uptime_ms_entry.timestamp  = now
                except Exception as exc:
                    log.warning("UpTimeMilliseconds conversion error: %s", exc)
                    self._apply_staleness("UpTimeMilliseconds", uptime_ms_entry, now)
            else:
                self._apply_staleness("UpTimeMilliseconds", uptime_ms_entry, now)

        # ── PortLinkStatus: INTEGER enum → "na" / "down" / "up" ──────────────
        # In-place transformation on an already-Good entry; timestamp is left
        # unchanged since the underlying OID was refreshed this cycle.
        pls_entry = self._store.get("PortLinkStatus")
        if _entry_is_good(pls_entry):
            try:
                mapped = {0: "na", 2: "up"}.get(
                    int(pls_entry.data_value.Value.Value), "down"
                )
                pls_entry.data_value = _good_dv(mapped, "String")
            except (ValueError, TypeError) as exc:
                log.warning("PortLinkStatus conversion error: %s", exc)

        # ── TimeTAIString: "2024-12-10-13:22:50" → "2024-12-10T13:22:50" ─────
        # In-place transformation; only reformat if T separator is absent
        # (the hardware emulator already produces ISO 8601 directly).
        tai_entry = self._store.get("TimeTAIString")
        if _entry_is_good(tai_entry):
            try:
                s = str(tai_entry.data_value.Value.Value)
                if "T" not in s:
                    pos = s.rfind("-")
                    if pos != -1:
                        s = s[:pos] + "T" + s[pos + 1:]
                tai_entry.data_value = _good_dv(s, "String")
            except Exception as exc:
                log.warning("TimeTAIString reformat error: %s", exc)

        await super().write_variables()


# ─────────────────────────────────────────────────────────────────────────────
# UCTSCommander -- all UDP command logic, no OPC UA / SNMP knowledge
# ─────────────────────────────────────────────────────────────────────────────

_UDP_TIMEOUT   = 2.0   # seconds to wait for TiCkS echo-back acknowledge
_TAI_UTC_DELTA = 37    # TAI minus UTC offset in seconds (correct as of 2017)


@dataclass
class UCTSCommander:
    """
    Encapsulates all UDP command interactions with the TiCkS board and exposes
    them as OPC UA Methods via register_methods().

    Attributes
    ----------
    ucts_ip            Current IP of the TiCkS board (updated by Configure at runtime).
    ucts_cmd_port      UDP command port of TiCkS (default 55010).
    POST_CMD_RELOAD_DELAY
                       Seconds to wait after an ACK is received before issuing a
                       force_reload() so the device's SNMP agent has time to
                       reflect the new state.  Tune upward if reads after commands
                       still show stale values.
    """
    ucts_ip:      str
    ucts_cmd_port: int = 55010

    POST_CMD_RELOAD_DELAY: float = field(default=0.05, init=False, repr=False)

    # ── low-level UDP transport ───────────────────────────────────────────────

    def _send_blocking(self, cmd_hex: str) -> bool:
        """
        Blocking UDP send + echo-back acknowledge.  Always called via _send()
        which runs it in an executor to avoid blocking the event loop.
        """
        try:
            cmd_bytes = bytes.fromhex(cmd_hex)
        except ValueError as exc:
            log.error("Bad TiCkS command hex %r: %s", cmd_hex, exc)
            return False
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(_UDP_TIMEOUT)
            sock.sendto(cmd_bytes, (self.ucts_ip, self.ucts_cmd_port))
            log.debug("TiCkS UDP -> %s:%d  cmd=%s",
                      self.ucts_ip, self.ucts_cmd_port, cmd_hex.upper())
            data, _ = sock.recvfrom(8)
            ack = data.hex().upper()
            if ack == cmd_hex.upper():
                log.info("TiCkS ACK OK  cmd=%s", cmd_hex.upper())
                return True
            log.warning("TiCkS ACK mismatch: sent=%s got=%s", cmd_hex.upper(), ack)
            return False
        except socket.timeout:
            log.warning("TiCkS ACK timeout  cmd=%s", cmd_hex.upper())
            return False
        except OSError as exc:
            log.error("TiCkS UDP error: %s", exc)
            return False
        finally:
            sock.close()

    async def _send(self, cmd_hex: str) -> bool:
        """Async wrapper: runs _send_blocking() in the thread-pool executor."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._send_blocking, cmd_hex)

    # ── ICD commands ─────────────────────────────────────────────────────────

    async def reset(self) -> int:
        """0xFFFFFFFFFFFFFF00 -- stop TDC and reset counters."""
        return 0 if await self._send("FFFFFFFFFFFFFF00") else 1

    async def get_ready(self) -> int:
        """0xFFFFFFFFFFFFFFF0 -- start TDC, counters, external trigger."""
        return 0 if await self._send("FFFFFFFFFFFFFFF0") else 1

    async def set_mac(self, mac: str) -> int:
        """Function code 0x1 -- configure destination MAC address."""
        clean = mac.translate(str.maketrans("", "", ":- "))
        if len(clean) != 12:
            log.error("Invalid MAC %r", mac)
            return 1
        return 0 if await self._send("FFF" + clean + "1") else 1

    async def set_dst_ip(self, dst_ip: str) -> int:
        """Function code 0x4 -- set destination IP address."""
        try:
            ip_hex = "".join(f"{int(p):02X}" for p in dst_ip.strip().split("."))
        except (ValueError, AttributeError) as exc:
            log.error("Invalid IP %r: %s", dst_ip, exc)
            return 1
        return 0 if await self._send("F" * 7 + ip_hex + "4") else 1

    async def set_dst_port(self, dst_port: int) -> int:
        """Function code 0x6 -- set destination UDP port."""
        word = (0xFFFFFFFFFFF << 20) | ((dst_port & 0xFFFF) << 4) | 0x6
        return 0 if await self._send(f"{word:016X}") else 1

    async def schedule_trigger(self, utc_iso: str) -> int:
        """
        Function code 0x2 -- schedule a software trigger at a UTC timestamp.

        Bit layout of the 64-bit word:
          bits  3: 0  function code 0x2
          bits 31: 4  28-bit sub-second time in units of 8 ns
          bits 56:32  25-bit TAI seconds
          bits 63:57  0x7F (upper padding)
        """
        try:
            dt = datetime.fromisoformat(utc_iso.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            tai_sec = int(dt.timestamp()) + _TAI_UTC_DELTA
            sub_ns8 = int((dt.timestamp() % 1.0) * 1e9 / 8)
        except (ValueError, OverflowError) as exc:
            log.error("Invalid UTC timestamp %r: %s", utc_iso, exc)
            return 1
        word  = 0x2
        word |= (sub_ns8 & 0x0FFFFFFF) << 4
        word |= (tai_sec & 0x01FFFFFF) << 32
        word |= 0x7F << 57
        return 0 if await self._send(f"{word:016X}") else 1

    async def xml_configuration(self, xml_body: str) -> int:
        """
        Parse XML body for known tags and apply the corresponding commands.
        Supported tags: <MACAddress>, <DstIpAddress>, <DstPort>, <SPI>.
        Returns 0 on full success, 1 if any command failed.
        """
        import re
        rc = 0
        m = re.search(r"<MACAddress>\s*([^<]+)\s*</MACAddress>",     xml_body, re.I)
        if m: rc |= await self.set_mac(m.group(1).strip())
        m = re.search(r"<DstIpAddress>\s*([^<]+)\s*</DstIpAddress>", xml_body, re.I)
        if m: rc |= await self.set_dst_ip(m.group(1).strip())
        m = re.search(r"<DstPort>\s*(\d+)\s*</DstPort>",             xml_body, re.I)
        if m: rc |= await self.set_dst_port(int(m.group(1)))
        m = re.search(r"<SPI>\s*([^<]+)\s*</SPI>",                   xml_body, re.I)
        if m:
            enable = m.group(1).strip().lower() in ("1", "true", "yes")
            rc |= (0 if await self._send("FFFFFFFFFFFFFF15" if enable
                                   else "FFFFFFFFFFFFFF05") else 1)
        return rc

    # ── OPC UA method registration ────────────────────────────────────────────

    async def register_methods(
        self,
        parent_node: Any,
        ns: int,
        poller: Optional[UCTSPoller] = None,
    ) -> None:
        """
        Install all seven ICD-defined OPC UA Methods on parent_node.

        Parameters
        ----------
        parent_node  asyncua Object node that will own the methods (UCTS root).
        ns           OPC UA namespace index.
        poller       Optional UCTSPoller reference; when a state-changing
                     command succeeds, an immediate SNMP reload is triggered
                     so monitoring variables update without waiting for the
                     next scheduled poll interval.
        """
        commander = self

        @uamethod
        async def Configure(parent,
                            PC_IP_ADDRESS:   str,
                            UCTS_IP_ADDRESS: str,
                            PC_MAC_ADDRESS:  str) -> int:
            log.info("Configure: pc=%s ucts=%s mac=%s",
                     PC_IP_ADDRESS, UCTS_IP_ADDRESS, PC_MAC_ADDRESS)
            # Set destination MAC (func 0x1) and destination IP (func 0x4) on the board.
            # UCTS_IP_ADDRESS (the board's own IP) is accepted for ICD compatibility but
            # ignored -- the board IP is fixed at startup via --ucts-ip.
            rc  = await commander.set_mac(PC_MAC_ADDRESS.strip())
            rc |= await commander.set_dst_ip(PC_IP_ADDRESS.strip())
            if rc == 0 and poller is not None:
                await asyncio.sleep(commander.POST_CMD_RELOAD_DELAY)
                await poller.force_reload()
            return int(rc)

        @uamethod
        async def Start(parent) -> int:
            log.info("Start -> %s:%d", commander.ucts_ip, commander.ucts_cmd_port)
            rc = await commander.get_ready()
            if rc == 0 and poller is not None:
                await asyncio.sleep(commander.POST_CMD_RELOAD_DELAY)
                await poller.force_reload()
            return int(rc)

        @uamethod
        async def Reset(parent) -> int:
            log.info("Reset -> %s:%d", commander.ucts_ip, commander.ucts_cmd_port)
            rc = await commander.reset()
            if rc == 0 and poller is not None:
                await asyncio.sleep(commander.POST_CMD_RELOAD_DELAY)
                await poller.force_reload()
            return int(rc)

        @uamethod
        async def ScheduleTrigger(parent, timestamp_UTC_ISO: str) -> int:
            log.info("ScheduleTrigger: %s", timestamp_UTC_ISO)
            return int(await commander.schedule_trigger(timestamp_UTC_ISO.strip()))

        @uamethod
        async def XMLConfiguration(parent, XML_Message: str) -> int:
            log.info("XMLConfiguration (len=%d)", len(XML_Message))
            idx = XML_Message.find("<")
            xml_body = XML_Message[idx:] if idx >= 0 else XML_Message
            rc = await commander.xml_configuration(xml_body)
            if rc == 0:
                log.info("XMLConfiguration: issuing Reset")
                rc |= await commander.reset()
            if poller is not None:
                await asyncio.sleep(commander.POST_CMD_RELOAD_DELAY)
                await poller.force_reload()
            return int(rc)

        @uamethod
        async def SetDstIpAddress(parent, ip_address: str) -> int:
            log.info("SetDstIpAddress: %s", ip_address)
            rc = await commander.set_dst_ip(ip_address.strip())
            if rc == 0 and poller is not None:
                await asyncio.sleep(commander.POST_CMD_RELOAD_DELAY)
                await poller.force_reload()
            return int(rc)

        @uamethod
        async def SetDstPort(parent, port: int) -> int:
            log.info("SetDstPort: %d", port)
            rc = await commander.set_dst_port(int(port))
            if rc == 0 and poller is not None:
                await asyncio.sleep(commander.POST_CMD_RELOAD_DELAY)
                await poller.force_reload()
            return int(rc)

        def _arg(name: str, type_node_id: ua.NodeId) -> ua.Argument:
            a = ua.Argument()
            a.Name = name
            a.DataType = type_node_id
            a.ValueRank = -1
            a.ArrayDimensions = []
            a.Description = ua.LocalizedText("")
            return a

        S = ua.NodeId(ua.ObjectIds.String)
        I = ua.NodeId(ua.ObjectIds.Int32)
        R = [_arg("Result", I)]

        method_defs = [
            (Configure,        "Configure",
             [_arg("PC_IP_ADDRESS",   S),
              _arg("UCTS_IP_ADDRESS", S),
              _arg("PC_MAC_ADDRESS",  S)], R),
            (Start,            "Start",            [], R),
            (Reset,            "Reset",            [], R),
            (ScheduleTrigger,  "ScheduleTrigger",
             [_arg("timestamp_UTC_ISO", S)], R),
            (XMLConfiguration, "XMLConfiguration",
             [_arg("XML_Message",      S)], R),
            (SetDstIpAddress,  "SetDstIpAddress",
             [_arg("ip_address",       S)], R),
            (SetDstPort,       "SetDstPort",
             [_arg("port",             I)], R),
        ]
        for fn, name, in_args, out_args in method_defs:
            await parent_node.add_method(ns, name, fn, in_args, out_args)
            log.debug("Registered method: %s", name)

        log.info("UCTSCommander: %d methods registered", len(method_defs))


# ─────────────────────────────────────────────────────────────────────────────
# UCTSOPCUAServer -- OPCUAServer subclass
# ─────────────────────────────────────────────────────────────────────────────

class UCTSOPCUAServer(OPCUAServer):
    """
    OPCUAServer subclass that, after building the standard monitoring address
    space, resolves the UCTS root node and attaches the UCTSCommander methods.
    """

    def __init__(
        self,
        *args: Any,
        commander: UCTSCommander,
        poller: UCTSPoller,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._commander = commander
        self._poller    = poller

    async def _build_address_space(self, server: Server, ns_idx: int) -> None:
        """
        Build the standard monitoring subtree via super(), then resolve the
        already-created UCTS root node and register the command methods on it.
        """
        await super()._build_address_space(server, ns_idx)
        ucts_node = await self._ensure_path(server, ns_idx, self.root_parts)
        await self._commander.register_methods(ucts_node, ns_idx, self._poller)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="UCTS OPC UA server (derived from snmp_asyncua_bridge)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--ucts-ip",        default="10.10.3.99",
                   help="IP address of the UCTS-TiCkS board")
    p.add_argument("--ucts-snmp-port", default=161,   type=int,
                   help="SNMP UDP port of the UCTS board")
    p.add_argument("--ucts-cmd-port",  default=55010, type=int,
                   help="UDP command port of the TiCkS board")
    p.add_argument("--snmp-community", default="public",
                   help="SNMP community string")
    p.add_argument("--poll-interval",  default=1.0,   type=float,
                   help="SNMP poll interval in seconds")
    p.add_argument("--opcua-endpoint",
                   default="opc.tcp://0.0.0.0:4840/ucts/",
                   help="OPC UA server endpoint URL")
    p.add_argument("--opcua-namespace",
                   default="http://cta-observatory.org/nectarcam/ucts/",
                   help="OPC UA namespace URI")
    p.add_argument("--opcua-user", default=None, metavar="USER:PASS",
                   help="OPC UA username:password (disables anonymous access)")
    p.add_argument("--snmp-timeout",  default=2.0, type=float, metavar="SECONDS",
                   help="SNMP request timeout in seconds (per attempt)")
    p.add_argument("--snmp-retries",  default=1,   type=int,   metavar="N",
                   help="Number of SNMP retries after the first attempt")
    p.add_argument("--monitoring-path", default="Monitoring",
                   help="OPC UA path of the monitoring variables node "
                        "(relative to the UCTS root node)")
    p.add_argument("--variable-lifetime", default=120.0, type=float, metavar="SECONDS",
                   help="Default lifetime in seconds for all polled variables "
                        "before they expire to BadNoCommunication (0 = never expire)")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    p.add_argument("--log-file", default=None,
                   help="Optional rotating log file path")
    p.add_argument("--dump-device-config", action="store_true",
                   help=(
                       "Print the fully-resolved device configuration as JSON to stdout "                       "(incorporating all CLI overrides) and exit immediately. "                       "Useful for generating a --device-config file."
                   ))
    return p.parse_args()


async def _async_main() -> None:
    args = _parse_args()
    setup_logging(args.log_level, args.log_file)

    user = password = None
    if args.opcua_user:
        parts = args.opcua_user.split(":", 1)
        if len(parts) != 2:
            sys.exit("--opcua-user must be in USER:PASS format")
        user, password = parts

    cfg = dict(_UCTS_CONFIG)
    cfg["host"]             = args.ucts_ip
    cfg["port"]             = args.ucts_snmp_port
    cfg["community"]        = args.snmp_community
    cfg["poll_interval"]    = args.poll_interval
    cfg["snmp_timeout"]     = args.snmp_timeout
    cfg["snmp_retries"]     = args.snmp_retries
    cfg["opcua_path"]       = args.monitoring_path
    cfg["default_lifetime"] = args.variable_lifetime

    if args.dump_device_config:
        import json
        print(json.dumps(cfg, indent=2))
        return

    poller    = UCTSPoller.from_dict(cfg)
    commander = UCTSCommander(
        ucts_ip=args.ucts_ip,
        ucts_cmd_port=args.ucts_cmd_port,
    )

    opcua_server = UCTSOPCUAServer(
        endpoint=args.opcua_endpoint,
        namespace=args.opcua_namespace,
        root_path="UCTS",
        user=user,
        password=password,
        commander=commander,
        poller=poller,
    )
    opcua_server.register(poller)
    await opcua_server.run()


def main() -> None:
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        log.info("Interrupted by user")


if __name__ == "__main__":
    main()
