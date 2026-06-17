"""
ucts_client.py
──────────────
Interactive OPC UA test client for the UCTS server.

Copyright 2026, Stephen Fegan <sfegan@llr.in2p3.fr>
Laboratoire Leprince-Ringuet, CNRS/IN2P3, Ecole Polytechnique, Institut Polytechnique de Paris

Connects to the UCTS OPC UA server and provides:
  • Read any monitoring variable
  • Call any ICD method (Configure, Start, Reset, etc.)
  • Subscribe to monitoring variables and print change notifications

Usage
-----
  python ucts_client.py [options]

  --endpoint URL    OPC UA server endpoint  (default: opc.tcp://localhost:4840/ucts/)
  --namespace URI   OPC UA namespace URI    (default: http://cta-observatory.org/nectarcam/ucts/)
  --user USER       OPC UA username         (optional)
  --password PASS   OPC UA password         (optional)
  --log-level LEVEL DEBUG/INFO/WARNING/ERROR (default WARNING -- keeps terminal clean)

Interactive commands
--------------------
  read [<var>]          Read one or all monitoring variables
  sub [<var> ...]       Subscribe to variables (default: all); print on change
  unsub                 Cancel active subscription
  configure <pc_ip> <ucts_ip> <mac>   Call Configure method
  start                 Call Start method
  reset                 Call Reset method
  trigger <utc_iso>     Call ScheduleTrigger with ISO timestamp
  xml <xml_string>      Call XMLConfiguration
  setip <ip>            Call SetDstIpAddress
  setport <port>        Call SetDstPort
  setmac <mac>          Call SetDstMacAddress
  setspi <0|1>          Call SetUseSpiReception
  settai <seconds>      Call SetTaiOffset (e.g. settai 37)
  browse                Print the UCTS node tree
  help                  Show this help
  quit / exit           Disconnect and exit

Dependencies
------------
  pip install asyncua
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import Any, Dict, List, Optional

try:
    from asyncua import Client, ua
    try:
        from asyncua.common.subscription import SubHandler
    except ImportError:
        from asyncua import Client, ua
        SubHandler = object  # fallback base class; datachange_notification is duck-typed
except ImportError:
    sys.exit("asyncua is required:  pip install asyncua")

log = logging.getLogger("ucts_client")

# ─────────────────────────────────────────────────────────────────────────────
# Known node paths
# ─────────────────────────────────────────────────────────────────────────────

_MONITORING_VARS = [
    # SNMP-polled and derived variables
    "BusyCount", "DstIpAddr", "DstMacAddr", "DstPort",
    "EventCount", "FirmwareVersion", "PortLinkStatus", "PortTxAndRx",
    "PtpClockOffset", "PtpErrorCounts", "PtpRoundTripTime", "PtpServoState",
    "PtpTxAndRx", "SpllSeqState", "State", "Status", "Temperature",
    "Throttle", "TimeTAI", "UpTime", "WrpcSwVersion",
    "SoftwareVersion", "tai_offset",
    # Built-in server variables
    "device_host", "device_port", "device_command_port", "device_polling_interval",
    "device_connected", "device_connection_uptime", "device_connection_downtime", 
    "device_state",
]

_METHODS = [
    "Configure", "Start", "Reset", "ScheduleTrigger",
    "XMLConfiguration", "SetDstIpAddress", "SetDstPort",
    "SetDstMacAddress", "SetUseSpiReception", "SetTaiOffset",
]

_HELP = """
Commands:
  read                     Read all monitoring variables
  read <VarName>           Read a single variable
  sub                      Subscribe to all monitoring variables
  sub <Var1> [Var2 ...]    Subscribe to specific variables
  unsub                    Cancel active subscription
  configure <pc_ip> <ucts_ip> <mac>
                           Call Configure (e.g. configure 10.0.0.1 10.10.3.99 aa:bb:cc:dd:ee:ff)
  start                    Call Start (GetReady)
  reset                    Call Reset
  trigger <UTC_ISO>        Call ScheduleTrigger (e.g. trigger 2026-03-19T12:00:00Z)
  xml <xml_string>         Call XMLConfiguration
  setip <ip>               Call SetDstIpAddress
  setport <port>           Call SetDstPort
  setmac <mac>             Call SetDstMacAddress (e.g. setmac 68:05:ca:3a:8f:28)
  setspi <0|1>             Call SetUseSpiReception (1=enable, 0=disable)
  settai <seconds>         Call SetTaiOffset (e.g. settai 37)
  browse                   Browse and print the UCTS node tree
  help                     Show this help
  quit / exit              Disconnect and exit
"""


# ─────────────────────────────────────────────────────────────────────────────
# Subscription handler
# ─────────────────────────────────────────────────────────────────────────────

class _ChangeHandler(SubHandler):
    """Print monitoring variable changes to stdout."""

    def __init__(self, node_name_map: Dict[str, str]) -> None:
        # Maps node id string -> variable name
        self._map = node_name_map

    def datachange_notification(self, node, val, data) -> None:
        name = self._map.get(str(node.nodeid), str(node.nodeid))
        dv = data.monitored_item.Value
        status = dv.StatusCode if dv.StatusCode else "Good"
        ts = dv.SourceTimestamp or dv.ServerTimestamp or ""
        print(f"\r  [sub] {name:30s} = {val!r:30s}  status={status}  {ts}")
        print("ucts> ", end="", flush=True)

    def event_notification(self, event) -> None:
        print(f"\r  [event] {event}")
        print("ucts> ", end="", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# Client wrapper
# ─────────────────────────────────────────────────────────────────────────────

class UCTSClient:

    def __init__(self, endpoint: str, namespace: str,
                 user: Optional[str], password: Optional[str]) -> None:
        self.endpoint  = endpoint
        self.namespace = namespace
        self.user      = user
        self.password  = password
        self._client:       Optional[Client] = None
        self._ns_idx:       int = 2
        self._ucts_node:    Any = None
        self._mon_node:     Any = None
        self._subscription: Any = None
        self._sub_handles:  List[Any] = []
        self._node_name_map: Dict[str, str] = {}

    async def connect(self) -> None:
        self._client = Client(self.endpoint)
        if self.user:
            self._client.set_user(self.user)
        if self.password:
            self._client.set_password(self.password)
        await self._client.connect()
        self._ns_idx = await self._client.get_namespace_index(self.namespace)
        log.info("Connected to %s  ns=%d", self.endpoint, self._ns_idx)

        # Locate UCTS and Monitoring nodes
        objects = self._client.nodes.objects
        self._ucts_node = await objects.get_child(
            [f"{self._ns_idx}:UCTS"]
        )
        self._mon_node = await self._ucts_node.get_child(
            [f"{self._ns_idx}:Monitoring"]
        )
        print(f"Connected to {self.endpoint}  (ns={self._ns_idx})")

    async def disconnect(self) -> None:
        if self._subscription:
            await self._subscription.delete()
            self._subscription = None
        if self._client:
            await self._client.disconnect()
            self._client = None
        print("Disconnected.")

    # ── read ──────────────────────────────────────────────────────────────────

    async def read_var(self, name: str) -> Any:
        node = await self._mon_node.get_child([f"{self._ns_idx}:{name}"])
        dv = await node.read_data_value()
        return dv

    async def cmd_read(self, names: List[str]) -> None:
        targets = names if names else _MONITORING_VARS
        for name in targets:
            try:
                dv = await self.read_var(name)
                val = dv.Value.Value if dv.Value else None
                sc = dv.StatusCode
                print(f"  {name:30s} = {val!r:30s}  [{sc}]")
            except Exception as exc:
                print(f"  {name:30s}  ERROR: {exc}")

    # ── subscribe ─────────────────────────────────────────────────────────────

    async def cmd_subscribe(self, names: List[str]) -> None:
        if self._subscription:
            print("  Already subscribed -- run 'unsub' first")
            return
        targets = names if names else _MONITORING_VARS
        self._node_name_map = {}
        nodes = []
        for name in targets:
            try:
                node = await self._mon_node.get_child([f"{self._ns_idx}:{name}"])
                nodes.append(node)
                self._node_name_map[str(node.nodeid)] = name
            except Exception as exc:
                print(f"  Warning: could not find node {name!r}: {exc}")

        handler = _ChangeHandler(self._node_name_map)
        self._subscription = await self._client.create_subscription(500, handler)
        self._sub_handles  = await self._subscription.subscribe_data_change(nodes)
        print(f"  Subscribed to {len(nodes)} variable(s). Changes will print automatically.")

    async def cmd_unsubscribe(self) -> None:
        if not self._subscription:
            print("  No active subscription.")
            return
        await self._subscription.delete()
        self._subscription = None
        self._sub_handles  = []
        print("  Subscription cancelled.")

    # ── methods ───────────────────────────────────────────────────────────────

    async def _call_method(self, method_name: str, *args: Any) -> Any:
        method_node = await self._ucts_node.get_child(
            [f"{self._ns_idx}:{method_name}"]
        )
        result = await self._ucts_node.call_method(method_node, *args)
        return result

    async def cmd_configure(self, pc_ip: str, ucts_ip: str, mac: str) -> None:
        rc = await self._call_method("Configure", pc_ip, ucts_ip, mac)
        print(f"  Configure -> rc={rc}")

    async def cmd_start(self) -> None:
        rc = await self._call_method("Start")
        print(f"  Start -> rc={rc}")

    async def cmd_reset(self) -> None:
        rc = await self._call_method("Reset")
        print(f"  Reset -> rc={rc}")

    async def cmd_schedule_trigger(self, ts: str) -> None:
        rc = await self._call_method("ScheduleTrigger", ts)
        print(f"  ScheduleTrigger({ts!r}) -> rc={rc}")

    async def cmd_xml_configuration(self, xml: str) -> None:
        rc = await self._call_method("XMLConfiguration", xml)
        print(f"  XMLConfiguration -> rc={rc}")

    async def cmd_set_dst_ip(self, ip: str) -> None:
        rc = await self._call_method("SetDstIpAddress", ip)
        print(f"  SetDstIpAddress({ip!r}) -> rc={rc}")

    async def cmd_set_dst_port(self, port: int) -> None:
        rc = await self._call_method("SetDstPort", ua.Variant(port, ua.VariantType.Int32))
        print(f"  SetDstPort({port}) -> rc={rc}")

    async def cmd_set_dst_mac(self, mac: str) -> None:
        rc = await self._call_method("SetDstMacAddress", mac)
        print(f"  SetDstMacAddress({mac!r}) -> rc={rc}")

    async def cmd_set_use_spi_reception(self, enable: bool) -> None:
        rc = await self._call_method("SetUseSpiReception",
                                     ua.Variant(enable, ua.VariantType.Boolean))
        print(f"  SetUseSpiReception({enable}) -> rc={rc}")

    async def cmd_set_tai_offset(self, offset: int) -> None:
        rc = await self._call_method("SetTaiOffset",
                                     ua.Variant(offset, ua.VariantType.Int32))
        print(f"  SetTaiOffset({offset}) -> rc={rc}")

    # ── browse ────────────────────────────────────────────────────────────────

    async def cmd_browse(self) -> None:
        print("  Objects/UCTS")
        await self._browse_node(self._ucts_node, "    ")

    async def _browse_node(self, node: Any, indent: str) -> None:
        try:
            children = await node.get_children()
            for child in children:
                bn = await child.read_browse_name()
                nc = await child.read_node_class()
                nc_name = str(nc).split(".")[-1]
                try:
                    dv = await child.read_data_value()
                    val = f" = {dv.Value.Value!r}" if dv.Value else " [no value]"
                    sc  = f" [{dv.StatusCode}]" if dv.StatusCode else ""
                except Exception:
                    val = sc = ""
                print(f"{indent}{bn.Name}  ({nc_name}){val}{sc}")
                if nc_name == "Object":
                    await self._browse_node(child, indent + "  ")
        except Exception as exc:
            print(f"{indent}[browse error: {exc}]")


# ─────────────────────────────────────────────────────────────────────────────
# Terminal loop
# ─────────────────────────────────────────────────────────────────────────────

async def _dispatch(client: UCTSClient, line: str) -> bool:
    """Parse and execute one command. Returns False to exit."""
    parts = line.strip().split(None, 3)
    if not parts:
        return True
    cmd = parts[0].lower()

    if cmd in ("quit", "exit"):
        return False

    elif cmd == "help":
        print(_HELP)

    elif cmd == "browse":
        await client.cmd_browse()

    elif cmd == "read":
        await client.cmd_read(parts[1:])

    elif cmd == "sub":
        await client.cmd_subscribe(parts[1:])

    elif cmd == "unsub":
        await client.cmd_unsubscribe()

    elif cmd == "start":
        await client.cmd_start()

    elif cmd == "reset":
        await client.cmd_reset()

    elif cmd == "configure":
        rest = line.strip().split(None, 4)
        if len(rest) < 4:
            print("Usage: configure <pc_ip> <ucts_ip> <mac>")
        else:
            await client.cmd_configure(rest[1], rest[2], rest[3])

    elif cmd == "trigger":
        if len(parts) < 2:
            print("Usage: trigger <UTC_ISO>")
        else:
            await client.cmd_schedule_trigger(parts[1])

    elif cmd == "xml":
        rest = line.strip().split(None, 1)
        if len(rest) < 2:
            print("Usage: xml <xml_string>")
        else:
            await client.cmd_xml_configuration(rest[1])

    elif cmd == "setip":
        if len(parts) < 2:
            print("Usage: setip <ip>")
        else:
            await client.cmd_set_dst_ip(parts[1])

    elif cmd == "setport":
        if len(parts) < 2:
            print("Usage: setport <port>")
        else:
            try:
                await client.cmd_set_dst_port(int(parts[1]))
            except ValueError:
                print("Port must be an integer")

    elif cmd == "setmac":
        if len(parts) < 2:
            print("Usage: setmac <mac>  (e.g. setmac 68:05:ca:3a:8f:28)")
        else:
            await client.cmd_set_dst_mac(parts[1])

    elif cmd == "setspi":
        if len(parts) < 2:
            print("Usage: setspi <0|1>")
        else:
            await client.cmd_set_use_spi_reception(parts[1].strip() in ("1", "true", "yes"))

    elif cmd == "settai":
        if len(parts) < 2:
            print("Usage: settai <seconds>  (e.g. settai 37)")
        else:
            try:
                await client.cmd_set_tai_offset(int(parts[1]))
            except ValueError:
                print("TAI offset must be an integer")

    else:
        print(f"Unknown command: {cmd!r}  (type 'help' for commands)")

    return True


async def _terminal_loop(client: UCTSClient) -> None:
    loop = asyncio.get_running_loop()
    print("\nUCTS Client ready. Type 'help' for commands, 'quit' to exit.\n")
    while True:
        try:
            line = await loop.run_in_executor(None, lambda: input("ucts> "))
        except EOFError:
            break
        try:
            if not await _dispatch(client, line):
                break
        except Exception as exc:
            print(f"  Error: {exc}")
            log.debug("Command error", exc_info=True)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Interactive OPC UA test client for the UCTS server",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--endpoint",
                   default="opc.tcp://localhost:4840/ucts/",
                   help="OPC UA server endpoint URL")
    p.add_argument("--namespace",
                   default="http://cta-observatory.org/nectarcam/ucts/",
                   help="OPC UA namespace URI")
    p.add_argument("--user",     default=None, help="OPC UA username")
    p.add_argument("--password", default=None, help="OPC UA password")
    p.add_argument("--log-level", default="WARNING",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


async def _async_main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )

    client = UCTSClient(
        endpoint=args.endpoint,
        namespace=args.namespace,
        user=args.user,
        password=args.password,
    )
    try:
        await client.connect()
    except Exception as exc:
        sys.exit(f"Could not connect to {args.endpoint}: {exc}")

    try:
        await _terminal_loop(client)
    finally:
        await client.disconnect()


def main() -> None:
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        print("\nInterrupted")


if __name__ == "__main__":
    main()
