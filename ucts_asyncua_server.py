"""
ucts_asyncua_server.py
──────────────────────
OPC UA server for the UCTS (Universal Clock and Time Stamping) controller.

Three-class design, each with a single responsibility:

  UCTSPoller(SNMPPoller)
      Pure SNMP monitoring.  Overrides the three SNMPPoller hooks:
        build_variable_specs()  - removes internal MAC half-OIDs; adds
                                  DstMacAddr, State, FirmwareVersion.
        write_variables()       - transforms raw SNMP values before publication
                                  (MAC merge, Status byte-shift, Temperature
                                  div-10, PortLinkStatus enum, TimeTAIString
                                  ISO-8601 reformat).
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
          DstMacAddr       <- merged from MSB+LSB OctetStrings
          DstPort
          EventCount
          FirmwareVersion  <- derived from Status bits 23:16
          PortLinkStatus   <- enum mapped to "na"/"down"/"up"
          State            <- derived from Status bit 7
          Status           <- raw uint32 as decimal string
          Temperature      <- tenths-of-C divided by 10 -> float
          Throttle
          TimeTAI
          TimeTAIString    <- ISO 8601 reformat
          UpTime
          WrpcSwVersion
          SoftwareVersion  <- constant
          host             <- built-in: SNMP device IP
          port             <- built-in: SNMP port
          cls_state        <- built-in: 0=offline 1=online

Usage
-----
  python ucts_asyncua_server.py [options]

  --ucts-ip IP          IP of the UCTS-TiCkS board  (default: 10.10.3.99)
  --ucts-snmp-port N    SNMP port                    (default: 161)
  --ucts-cmd-port N     UDP command port             (default: 55010)
  --snmp-community S    SNMP community string        (default: public)
  --poll-interval F     Poll interval in seconds     (default: 1.0)
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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# ── bridge imports ────────────────────────────────────────────────────────────
try:
    from snmp_asyncua_bridge import (
        NodeSpec,
        OPCUAServer,
        SNMPPoller,
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
# Mirrors device_ucts_resolved_oids.json exactly; all OIDs already numeric.
# DstMacAddr_32MSB / DstMacAddr_16LSB are polled but suppressed from direct
# OPC UA publication -- write_variables() merges them into DstMacAddr.
# ─────────────────────────────────────────────────────────────────────────────

_UCTS_CONFIG: dict = {
    "ip":            "10.10.3.99",   # overridden at runtime via --ucts-ip
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
            # Internal: 4 MSB of destination MAC -- merged into DstMacAddr
            "oid":         "1.3.6.1.4.1.96.101.2.1.1.1.1.4.1",
            "opcua_name":  "DstMacAddr_32MSB",
            "opcua_type":  "ByteString",
            "description": "(internal) 32 MSB of destination MAC address",
        },
        {
            # Internal: 2 LSB of destination MAC -- merged into DstMacAddr
            "oid":         "1.3.6.1.4.1.96.101.2.1.1.1.1.5.1",
            "opcua_name":  "DstMacAddr_16LSB",
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
            # wrpcAuxDiagStatus -- ASN_OCTET_STR encoding a uint32 bitmask.
            # Declared ByteString so _cast_to_ua leaves the raw bytes intact;
            # write_variables intercepts this, byte-shifts to uint32, and
            # publishes the derived decimal string to the Status OPC UA node.
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
            "description": "Uptime of the UCTS",
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
    ],
}

_INTERNAL_NAMES = frozenset({"DstMacAddr_32MSB", "DstMacAddr_16LSB", "_RawStatus"})

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
    Merge the two MAC OctetString OIDs into a single address string.
    C++ get_ucts_dst_mac_addr() uses '%x' (lowercase, no zero-padding):
        e.g. "44:a8:42:44:32:c9"
    """
    b_msb = (msb + b"\x00" * 4)[:4]
    b_lsb = (lsb + b"\x00" * 2)[:2]
    return ":".join(f"{b:x}" for b in b_msb + b_lsb)


def _state_from_status(status: int) -> int:
    """bit 7: 1=Running, 0=Online/Standby; 0 input -> 2=Unknown."""
    return 2 if status == 0 else int((status >> 7) & 0x1)


def _fw_version_from_status(status: int) -> str:
    """Firmware version integer from bits 23:16 of the status word."""
    return "" if status == 0 else str((status >> 16) & 0xFF)


def _dv(value: Any, opcua_type: str) -> ua.DataValue:
    """Shorthand: build a Good ua.DataValue from a Python value."""
    variant = _cast_to_ua(value, opcua_type)
    return ua.DataValue(variant) if isinstance(variant, ua.Variant) else variant



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
    - Suppress internal MAC half-OIDs from OPC UA; merge them into DstMacAddr.
    - Derive State and FirmwareVersion from the Status bitmask.
    - Apply the remaining per-OID transformations (Temperature, PortLinkStatus,
      TimeTAIString) before writing to OPC UA.

    No knowledge of UDP commands or OPC UA Methods.
    """

    # ── hook 1: build_variable_specs ─────────────────────────────────────────

    def build_variable_specs(self) -> Dict[str, NodeSpec]:
        """
        Remove the two internal MAC half-OIDs from the published spec, override
        the Status node type from ByteString to String (the SNMP OID is declared
        ByteString so raw bytes arrive in write_variables, but what we publish to
        OPC UA is the derived decimal string), and add the three derived variables:
        DstMacAddr, State, FirmwareVersion.
        """
        specs = super().build_variable_specs()

        for name in _INTERNAL_NAMES:
            specs.pop(name, None)

        _bad = ua.StatusCode(ua.StatusCodes.BadWaitingForInitialData)

        # Status is derived from the internal _RawStatus bytes -- add it here
        # just like DstMacAddr/State/FirmwareVersion.
        specs["Status"] = NodeSpec(
            opcua_type="String",
            initial_value="",
            description="Status of TiCkS board (raw uint32 as decimal string)",
            initial_status=_bad,
        )

        # initial_value must be a valid typed zero so asyncua registers the
        # VariantType on the node.  The bad status is then written on top;
        # clients see BadWaitingForInitialData but the type is correctly set.
        specs["DstMacAddr"] = NodeSpec(
            opcua_type="String",
            initial_value="00:00:00:00:00:00",
            description="Destination MAC address (aa:bb:cc:dd:ee:ff)",
            initial_status=_bad,
        )
        specs["State"] = NodeSpec(
            opcua_type="Int32",
            initial_value=2,            # 2 = Unknown
            description="TiCkS state: 1=Running, 0=Online, 2=Unknown",
            initial_status=_bad,
        )
        specs["FirmwareVersion"] = NodeSpec(
            opcua_type="String",
            initial_value="",
            description="TiCkS firmware version (from Status bits 23:16)",
            initial_status=_bad,
        )
        return specs

    # ── hook 0: SNMP transport -- tighter timeout to avoid poll overruns ────────

    @staticmethod
    def _resolve_ip(hostname: str) -> str:
        """
        Resolve a hostname to a numeric IPv4 address string.

        UdpTransportTarget internally forces AF_INET, but on macOS the OS
        may still route "localhost" ambiguously depending on /etc/hosts and
        the active network configuration.  Pre-resolving to a dotted-decimal
        address sidesteps any platform-specific hostname resolution quirks
        and guarantees pysnmp uses the correct IPv4 path.
        """
        import socket as _socket
        try:
            results = _socket.getaddrinfo(
                hostname, None,
                family=_socket.AF_INET,
                type=_socket.SOCK_DGRAM,
            )
            if results:
                return results[0][4][0]
        except _socket.gaierror:
            pass
        return hostname   # already numeric or unresolvable -- pass through

    async def _get_all_oids(self) -> Optional[Dict[str, Any]]:
        """
        Override to use timeout=1s / retries=0 (vs base class timeout=2s /
        retries=1) and to pre-resolve the hostname to a numeric IPv4 address
        before passing it to UdpTransportTarget.  This avoids a macOS-specific
        issue where "localhost" can be routed unexpectedly by the OS resolver
        despite pysnmp explicitly requesting AF_INET.
        """
        from pysnmp.hlapi.v3arch.asyncio import (
            CommunityData, ContextData, ObjectIdentity, ObjectType, get_cmd,
            UdpTransportTarget,
        )
        from pysnmp.proto.rfc1905 import EndOfMibView, NoSuchInstance, NoSuchObject

        object_types = [
            ObjectType(ObjectIdentity(oid_cfg.oid)) for oid_cfg in self.oids
        ]
        if self._transport_target is None:
            ip = self._resolve_ip(self.ip)
            if ip != self.ip:
                log.debug("Resolved %s -> %s for SNMP transport", self.ip, ip)
            self._transport_target = await UdpTransportTarget.create(
                (ip, self.port), timeout=1, retries=0,
            )
        error_indication, error_status, error_index, var_binds = await get_cmd(
            self._snmp_engine,
            CommunityData(self.community, mpModel=1),
            self._transport_target,
            ContextData(),
            *object_types,
        )
        if error_indication:
            log.warning("SNMP GET %s: %s", self.ip, error_indication)
            return None
        if error_status:
            bad_idx = int(error_index) - 1 if error_index else None
            bad_oid = str(var_binds[bad_idx][0]) if bad_idx is not None else "unknown"
            log.warning("SNMP GET %s: agent error at OID %s -- skipping",
                        self.ip, bad_oid)
            if bad_idx is not None:
                var_binds = list(var_binds)
                var_binds.pop(bad_idx)
        results: Dict[str, Any] = {}
        for oid_obj, value in var_binds:
            if isinstance(value, (NoSuchObject, NoSuchInstance, EndOfMibView)):
                continue
            results[str(oid_obj)] = value
        return results


    # ── hook 2: write_variables ───────────────────────────────────────────────

    async def write_variables(self, values: Dict[str, ua.DataValue]) -> None:
        """
        Transform raw SNMP DataValues before writing to OPC UA, then delegate
        to super() for the actual node writes.
        """
        # ── MAC: merge two OctetString halves ─────────────────────────────────
        msb_dv = values.pop("DstMacAddr_32MSB", None)
        lsb_dv = values.pop("DstMacAddr_16LSB", None)

        if msb_dv is not None and lsb_dv is not None:
            msb_raw = msb_dv.Value.Value if msb_dv.Value else None
            lsb_raw = lsb_dv.Value.Value if lsb_dv.Value else None
            if isinstance(msb_raw, (bytes, bytearray)) and \
               isinstance(lsb_raw, (bytes, bytearray)):
                try:
                    values["DstMacAddr"] = _dv(
                        _merge_mac(bytes(msb_raw), bytes(lsb_raw)), "String"
                    )
                except Exception as exc:
                    log.warning("MAC merge error: %s", exc)
            # else: source OIDs had no value -- leave DstMacAddr unchanged
        # elif: only one half arrived -- leave DstMacAddr unchanged

        # ── Status: ByteString -> uint32 byte-shift -> decimal string + derived vars
        status_dv = values.pop("_RawStatus", None)
        if status_dv is not None and status_dv.Value is not None:
            raw = status_dv.Value.Value
            if isinstance(raw, (bytes, bytearray)):
                try:
                    status_int = _octetstr_to_uint32(bytes(raw))
                    values["Status"]          = _dv(str(status_int),                     "String")
                    values["State"]           = _dv(_state_from_status(status_int),      "Int32")
                    values["FirmwareVersion"] = _dv(_fw_version_from_status(status_int), "String")
                except Exception as exc:
                    log.warning("Status decode error: %s", exc)
            else:
                log.warning("_RawStatus value not decodable: %r -- leaving Status/State/FirmwareVersion unchanged", raw)
        # ── Temperature: tenths-of-C integer -> float ─────────────────────────
        temp_dv = values.get("Temperature")
        if temp_dv is not None and temp_dv.Value is not None:
            try:
                values["Temperature"] = _dv(int(temp_dv.Value.Value) / 10.0, "Float")
            except (ValueError, TypeError):
                pass

        # ── PortLinkStatus: INTEGER enum -> "na" / "down" / "up" ─────────────
        pls_dv = values.get("PortLinkStatus")
        if pls_dv is not None and pls_dv.Value is not None:
            try:
                values["PortLinkStatus"] = _dv(
                    {0: "na", 2: "up"}.get(int(pls_dv.Value.Value), "down"),
                    "String",
                )
            except (ValueError, TypeError):
                pass

        # ── TimeTAIString: "2024-12-10-13:22:50" -> "2024-12-10T13:22:50" ────
        # Only reformat if the string doesn't already contain a T separator
        # (the emulator already produces ISO 8601 format directly).
        tai_dv = values.get("TimeTAIString")
        if tai_dv is not None and tai_dv.Value is not None:
            try:
                s = str(tai_dv.Value.Value)
                if "T" not in s:
                    pos = s.rfind("-")
                    if pos != -1:
                        s = s[:pos] + "T" + s[pos + 1:]
                values["TimeTAIString"] = _dv(s, "String")
            except Exception:
                pass

        await super().write_variables(values)


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
    ucts_ip      Current IP of the TiCkS board (updated by Configure at runtime).
    ucts_cmd_port UDP command port of TiCkS (default 55010).
    """
    ucts_ip:      str
    ucts_cmd_port: int = 55010

    # ── low-level UDP transport ───────────────────────────────────────────────

    def _send_blocking(self, cmd_hex: str) -> bool:
        """
        Blocking UDP send + echo-back acknowledge.  Always called via
        _send() which runs it in an executor to avoid blocking the event loop.
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
        """
        Async wrapper around _send_blocking().
        Runs the blocking socket I/O in the default thread-pool executor so
        the asyncio event loop is never stalled during the UDP timeout window.
        """
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
        return 0 if await self._send("F" * 11 + f"{dst_port:02X}" + "6") else 1

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
        parent_node
            The asyncua Object node that will own the methods (the UCTS root).
        ns
            OPC UA namespace index.
        poller
            Optional UCTSPoller reference.  When Start() is called and succeeds,
            an immediate poll is triggered so monitoring variables update without
            waiting for the next poll interval.
        """
        commander = self   # closure reference; self.ucts_ip may change via Configure

        @uamethod
        async def Configure(parent,
                            PC_IP_ADDRESS:   str,
                            UCTS_IP_ADDRESS: str,
                            PC_MAC_ADDRESS:  str) -> int:
            log.info("Configure: pc=%s ucts=%s mac=%s",
                     PC_IP_ADDRESS, UCTS_IP_ADDRESS, PC_MAC_ADDRESS)
            commander.ucts_ip = UCTS_IP_ADDRESS.strip()
            # Invalidate the poller's cached transport so it reconnects
            if poller is not None:
                poller.ip = commander.ucts_ip
                poller._transport_target = None
            return int(
                await commander.set_mac(PC_MAC_ADDRESS.strip())
            )

        @uamethod
        async def Start(parent) -> int:
            log.info("Start -> %s:%d", commander.ucts_ip, commander.ucts_cmd_port)
            rc = await commander.get_ready()
            if rc == 0 and poller is not None:
                await asyncio.sleep(1.0)   # let TiCkS transition state
                await poller._poll_once()  # refresh monitoring immediately
            return int(rc)

        @uamethod
        async def Reset(parent) -> int:
            log.info("Reset -> %s:%d", commander.ucts_ip, commander.ucts_cmd_port)
            return int(await commander.reset())

        @uamethod
        async def ScheduleTrigger(parent,
                                  timestamp_UTC_ISO: str) -> int:
            log.info("ScheduleTrigger: %s", timestamp_UTC_ISO)
            return int(
                await commander.schedule_trigger(timestamp_UTC_ISO.strip())
            )

        @uamethod
        async def XMLConfiguration(parent,
                                   XML_Message: str) -> int:
            log.info("XMLConfiguration (len=%d)", len(XML_Message))
            idx = XML_Message.find("<")
            xml_body = XML_Message[idx:] if idx >= 0 else XML_Message
            rc = await commander.xml_configuration(xml_body)
            if rc == 0:
                log.info("XMLConfiguration: issuing Reset")
                rc |= await commander.reset()
            return int(rc)

        @uamethod
        async def SetDstIpAddress(parent,
                                  ip_address: str) -> int:
            log.info("SetDstIpAddress: %s", ip_address)
            return int(await commander.set_dst_ip(ip_address.strip()))

        @uamethod
        async def SetDstPort(parent, port: int) -> int:
            log.info("SetDstPort: %d", port)
            return int(await commander.set_dst_port(int(port)))

        def _arg(name: str, type_node_id: ua.NodeId) -> ua.Argument:
            """Build a scalar OPC UA Argument with a LocalizedText description."""
            a = ua.Argument()
            a.Name = name
            a.DataType = type_node_id
            a.ValueRank = -1          # -1 = Scalar (not an array)
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

    Parameters
    ----------
    commander
        A UCTSCommander instance whose methods will be registered on the root.
    poller
        The UCTSPoller instance (passed to register_methods so Start() can
        trigger an immediate poll).
    All other parameters are forwarded to OPCUAServer.
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

        super() calls _ensure_path(server, ns_idx, ["UCTS", "Monitoring"]),
        which creates Objects/UCTS and Objects/UCTS/Monitoring in sequence and
        returns the Monitoring node.  Objects/UCTS therefore already exists by
        the time we need it for the methods.
        """
        await super()._build_address_space(server, ns_idx)

        # _ensure_path walked UCTS -> Monitoring; re-walk just UCTS to get the root.
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
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    p.add_argument("--log-file", default=None,
                   help="Optional rotating log file path")
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
    cfg["ip"]            = args.ucts_ip
    cfg["port"]          = args.ucts_snmp_port
    cfg["community"]     = args.snmp_community
    cfg["poll_interval"] = args.poll_interval

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
