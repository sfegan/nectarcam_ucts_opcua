# NectarCAM UCTS OPC UA Server

OPC UA server bridge for the **UCTS (Universal Clock and Time Stamping)** controller and **TiCkS** timing board. Monitors the device via SNMP and exposes ten ICD-defined command methods that translate OPC UA calls into UDP commands sent to the TiCkS board. See the [TiCkS documentation](https://mdpunch.pages.in2p3.fr/ticks/index.html) for full hardware and firmware details.

## Dependencies

```bash
pip install pysnmp-lextudio asyncua
```

The server also requires the `snmp_asyncua_bridge` module from [nectarcam_snmp_opcua](https://github.com/sfegan/nectarcam_snmp_opcua):

```bash
git clone https://github.com/sfegan/nectarcam_snmp_opcua.git
export PYTHONPATH="/path/to/nectarcam_snmp_opcua:$PYTHONPATH"
```

## Running the Server

```bash
python ucts_asyncua_server.py
```

Defaults: UCTS board at `10.10.3.99`, OPC UA endpoint at `opc.tcp://0.0.0.0:4840/ucts/`, SNMP poll interval 1 s.

Full options:

| Option | Default | Description |
|--------|---------|-------------|
| `--ucts-ip IP` | `10.10.3.99` | IP of the UCTS-TiCkS board |
| `--ucts-snmp-port N` | `161` | SNMP port |
| `--ucts-cmd-port N` | `55010` | UDP command port |
| `--snmp-community S` | `public` | SNMP community string |
| `--poll-interval F` | `1.0` | Poll interval (seconds) |
| `--snmp-timeout F` | `2.0` | SNMP request timeout (seconds) |
| `--snmp-retries N` | `1` | SNMP retries after first attempt |
| `--monitoring-path S` | `Monitoring` | OPC UA path for monitoring node |
| `--variable-lifetime F` | `120.0` | Variable lifetime (seconds) |
| `--post-cmd-delay F` | `0.2` | Settling delay (seconds) before SNMP reload after a command ACK |
| `--tai-offset N` | `37` | TAI minus UTC offset in seconds (last leap second 2016-12-31) |
| `--opcua-endpoint URL` | `opc.tcp://0.0.0.0:4840/ucts/` | OPC UA endpoint |
| `--opcua-namespace URI` | — | OPC UA namespace URI |
| `--opcua-user U:P` | — | Enable username/password authentication |
| `--log-level LEVEL` | `INFO` | DEBUG / INFO / WARNING / ERROR |
| `--log-file PATH` | — | Optional rotating log file |

## OPC UA Address Space

All monitoring variables are exposed under `Objects/UCTS/Monitoring/`. Command methods are registered directly on `Objects/UCTS/`.

### Monitoring Variables

| Variable | Type | Description |
|----------|------|-------------|
| `BusyCount` | Int32 | Cumulative count of busy/rejected triggers since last reset |
| `DstIpAddr` | String | Destination IP address for UDP timestamp delivery (default derived from TiCkS IP: last 10 bits replaced with `3.250`) |
| `DstMacAddr` | String | Destination MAC address for UDP timestamp delivery (merged from two SNMP OIDs, colon-separated: `01:23:45:67:89:AB`) |
| `DstPort` | Int32 | Destination UDP port for timestamp delivery (default: `55000`) |
| `EventCount` | Int32 | Cumulative count of read-out trigger events since last reset |
| `FirmwareVersion` | String | Firmware version number, extracted from `Status` bits 23:16 |
| `PortLinkStatus` | String | White Rabbit ethernet link status: `na`, `down`, or `up` |
| `State` | Int32 | TiCkS operating state derived from `Status` bit 7 (`rst_cnt_ack`): `0` = Online/Standby (TDC and counters stopped), `1` = Running (TDC and counters active) |
| `Status` | Int64 | Raw uint32 SNMP status word. Bit layout (LSB first): bit 0 = throttle enabled, bit 1 = WR time valid, bit 2 = SPI enabled, bit 7 = counters/TDC enabled (`rst_cnt_ack`), bits 16–23 = firmware version, bits 24–31 = data format version |
| `Temperature` | Float | PCB temperature (°C) from the WR node temperature sensor |
| `Throttle` | Int64 | Trigger throttle register value. The throttle suppresses events when the time between first and last event in a bunch is less than the configured minimum (default `0x30D3` = 12488 counts ≈ 200 µs at 62.5 MHz). `0` means throttling is disabled |
| `TimeTAI` | Int64 | Current board time in TAI seconds (from White Rabbit) |
| `TimeTAIString` | String | Current TAI time as ISO 8601 string |
| `UpTime` | String | Board uptime as formatted string (e.g. `5:23:49.160000`) |
| `UpTimeMilliseconds` | Double | Board uptime in milliseconds |
| `WrpcSwVersion` | String | White Rabbit PTP core software version |
| `SoftwareVersion` | String | Version of this server implementation (constant: `2.0.0`) |
| `tai_offset` | Int32 | TAI minus UTC offset in seconds as configured in this server (set via `--tai-offset` or `SetTaiOffset`). Reflects the server's working assumption and is not an authoritative statement of the current IERS value |
| `snmp_host` | String | IP address of the SNMP device |
| `snmp_port` | UInt16 | SNMP UDP port |
| `snmp_polling_timestamp` | DateTime | Timestamp of the most recent poll |
| `snmp_polling_age` | Double | Seconds since the last successful poll; keeps incrementing while the device is offline |
| `snmp_polling_interval` | Double | Current polling interval (seconds) |
| `snmp_polling_success_count` | UInt32 | Cumulative successful SNMP polls |
| `snmp_server_online` | Boolean | `True` when the SNMP agent is reachable (set exclusively by the bridge; never overridden by application logic) |
| `cls_state` | Byte | Application-level device state: `0` = offline, `1` = online (may be overridden by subclasses) |

Variables become `UncertainLastUsableValue` if the SNMP agent is unreachable, and transition to `BadNoCommunication` after `--variable-lifetime` seconds.

## Command Methods

All methods are under `Objects/UCTS/` and return `Int32`: `0` = success, non-zero = failure. UDP commands include a 2-second echo-back timeout.

Methods that change hardware state (`Start`, `Reset`, `Configure`, `XMLConfiguration`, `SetDstIpAddress`, `SetDstPort`, `SetDstMacAddress`, `SetUseSpiReception`) automatically trigger a full SNMP poll after the ACK is received (with a short configurable settling delay, default 200 ms, controlled by `--post-cmd-delay`) so that monitoring variables reflect the new state without waiting for the next scheduled poll interval. `ScheduleTrigger` and `SetTaiOffset` do not trigger a reload.

### `Start() → Int32`
Enable the TDC, reset event/busy counters, enable external trigger reception. On success, triggers an immediate SNMP reload.

### `Reset() → Int32`
Stop the TDC, reset all event and busy counters. On success, triggers an immediate SNMP reload.

### `Configure(PC_IP_ADDRESS: String, UCTS_IP_ADDRESS: String, PC_MAC_ADDRESS: String) → Int32`
Set the destination IP and MAC address for timestamp delivery and update the UCTS board's own IP. On success, triggers an immediate SNMP reload.

### `ScheduleTrigger(timestamp_UTC_ISO: String) → Int32`
Schedule a software trigger at a UTC timestamp (ISO 8601, e.g. `2025-06-15T14:30:45.123456`). Converted internally to TAI using the server's current `tai_offset` value, and encoded into a 64-bit command word at 8 ns resolution. Does not trigger an SNMP reload.

### `SetDstIpAddress(ip_address: String) → Int32`
Set the destination IP address for timestamp delivery. On success, triggers an immediate SNMP reload.

### `SetDstPort(port: Int32) → Int32`
Set the destination UDP port for timestamp delivery. On success, triggers an immediate SNMP reload.

### `SetDstMacAddress(mac_address: String) → Int32`
Set the destination MAC address for timestamp delivery (e.g. `68:05:ca:3a:8f:28`). Colons, hyphens, and spaces are stripped before encoding. On success, triggers an immediate SNMP reload.

### `SetUseSpiReception(enable: Boolean) → Int32`
Enable (`True`) or disable (`False`) SPI trigger reception on the TiCkS board. On success, triggers an immediate SNMP reload.

### `SetTaiOffset(offset: Int32) → Int32`
Update the TAI minus UTC offset used by `ScheduleTrigger` without restarting the server. The new value is reflected immediately in the `tai_offset` monitoring variable. The current offset can also be set at startup via `--tai-offset`. Does not trigger an SNMP reload.

### `XMLConfiguration(XML_Message: String) → Int32`
Apply configuration via an XML message. The message must contain a `<UCTS>` root element with named child elements carrying their values in a `value` attribute, matching the C++ MOS server schema:

```xml
<UCTS>
  <OPCUA_server_IP_Address value="192.168.1.10"/>  <!-- accepted, ignored -->
  <UCTS_IP_ADDRESS         value="10.10.3.99"/>    <!-- accepted, ignored -->
  <CDTS_MAC_Address        value="68:05:ca:3a:8f:28"/>  <!-- required -->
  <DST_IP_ADDRESS          value="10.10.3.250"/>         <!-- optional -->
  <DstPort                 value="55000"/>               <!-- optional, Python extension -->
  <SPI                     value="1"/>                   <!-- optional, Python extension -->
</UCTS>
```

`CDTS_MAC_Address` is required; all other tags are optional. `OPCUA_server_IP_Address` and `UCTS_IP_ADDRESS` are accepted for compatibility with existing XML messages but have no effect (the board IP and server address are fixed at startup). Returns `0` only if all commands succeed. On completion, triggers an immediate SNMP reload.

## Local Testing

The emulator (`ucts_emulator.py`) simulates the UCTS/TiCkS hardware and an interactive test client (`ucts_client.py`) is provided. Run them in separate terminals alongside the server.

**Terminal 1 — emulator** (SNMP agent on port 1161, UDP command listener on port 55010):
```bash
python ucts_emulator.py
```

**Terminal 2 — server** (pointed at the emulator):
```bash
python ucts_asyncua_server.py --ucts-ip localhost --ucts-snmp-port 1161
```

**Terminal 3 — test client:**
```bash
python ucts_client.py --endpoint opc.tcp://localhost:4840/ucts/
```

### Emulator options

| Option | Default | Description |
|--------|---------|-------------|
| `--snmp-port N` | `1161` | SNMP UDP port (use `161` as root) |
| `--cmd-port N` | `55010` | TiCkS command UDP port |
| `--bind HOST` | `0.0.0.0` | Interface to bind (binds both IPv4 and IPv6 on wildcard) |
| `--log-level LEVEL` | `INFO` | DEBUG / INFO / WARNING / ERROR |

### Emulator interactive commands

| Command | Description |
|---------|-------------|
| `show` | Print all current variable values |
| `set Temperature <val>` | Set temperature in °C (e.g. `25.6`) |
| `set EventCount <val>` | Set event counter |
| `set BusyCount <val>` | Set busy counter |
| `set Throttle <val>` | Set throttle (decimal or `0x` hex) |
| `set DstIpAddr <x.x.x.x>` | Set destination IP |
| `set DstPort <val>` | Set destination port |
| `set WrpcSwVersion <str>` | Set WR software version string |
| `set FirmwareVersion <n>` | Set firmware version integer |
| `set PortLinkStatus <n>` | Set port link status (0=na, 1=down, 2=up) |
| `state <0\|1>` | Set TiCkS state (0=Online/Standby, 1=Running) |
| `status <hex_or_dec>` | Set raw status word directly |
| `reset` | Apply Reset command (state=Online, counters=0) |
| `getready` | Apply GetReady command (state=Running) |
| `help` | Show full command help |
| `quit` / `exit` | Shut down |

### Client commands

| Command | Description |
|---------|-------------|
| `read [VarName]` | Read one or all monitoring variables |
| `sub [Var ...]` | Subscribe to variables; changes print automatically |
| `unsub` | Cancel active subscription |
| `configure <pc_ip> <ucts_ip> <mac>` | Call `Configure` |
| `start` | Call `Start` (GetReady) |
| `reset` | Call `Reset` |
| `trigger <UTC_ISO>` | Call `ScheduleTrigger` |
| `xml <xml_string>` | Call `XMLConfiguration` |
| `setip <ip>` | Call `SetDstIpAddress` |
| `setport <port>` | Call `SetDstPort` |
| `setmac <mac>` | Call `SetDstMacAddress` (e.g. `setmac 68:05:ca:3a:8f:28`) |
| `setspi <0\|1>` | Call `SetUseSpiReception` |
| `settai <seconds>` | Call `SetTaiOffset` (e.g. `settai 37`) |
| `browse` | Print the UCTS node tree |
| `help` | Show full command help |
