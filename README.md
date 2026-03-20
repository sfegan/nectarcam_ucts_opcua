# NectarCAM UCTS OPC UA Server

OPC UA server bridge for the **UCTS (Universal Clock and Time Stamping)** controller and **TiCkS** timing board. Monitors the device via SNMP and exposes seven ICD-defined command methods that translate OPC UA calls into UDP commands sent to the TiCkS board. See the [TiCkS documentation](https://mdpunch.pages.in2p3.fr/ticks/index.html) for full hardware and firmware details.

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
| `DstMacAddr` | String | Destination MAC address for UDP timestamp delivery (merged from two SNMP OIDs, colon-separated) |
| `DstPort` | Int32 | Destination UDP port for timestamp delivery (default: `55000`) |
| `EventCount` | Int32 | Cumulative count of read-out trigger events since last reset |
| `FirmwareVersion` | String | Firmware version number, extracted from `Status` bits 23:16 |
| `PortLinkStatus` | String | White Rabbit ethernet link status: `na`, `down`, or `up` |
| `State` | Int32 | TiCkS operating state derived from `Status` bit 7 (`rst_cnt_ack`): `0` = Reset/Standby (TDC and counters stopped), `1` = Running (TDC and counters active), `2` = Unknown |
| `Status` | String | Raw uint32 SNMP status word as decimal string. Bit layout (LSB first): bit 0 = throttle enabled, bit 1 = WR time valid, bit 2 = SPI enabled, bit 7 = counters/TDC enabled (`rst_cnt_ack`), bits 16–23 = firmware version, bits 24–31 = data format version |
| `Temperature` | Float | PCB temperature (°C) from the WR node temperature sensor |
| `Throttle` | Int32 | Trigger throttle register value. The throttle suppresses events when the time between first and last event in a bunch is less than the configured minimum (default `0x30D3` = 12488 counts ≈ 200 µs at 62.5 MHz). `0` means throttling is disabled |
| `TimeTAI` | Int64 | Current board time in TAI seconds (from White Rabbit) |
| `TimeTAIString` | String | Current TAI time as ISO 8601 string |
| `UpTime` | String | Board uptime as formatted string (e.g. `5:23:49.160000`) |
| `UpTimeMilliseconds` | Double | Board uptime in milliseconds |
| `WrpcSwVersion` | String | White Rabbit PTP core software version |
| `SoftwareVersion` | String | Version of this server implementation (constant: `2.0.0`) |
| `snmp_host` | String | IP address of the SNMP device |
| `snmp_port` | Int32 | SNMP UDP port |
| `snmp_polling_timestamp` | Int64 | Unix timestamp of the most recent poll |
| `snmp_polling_age` | Double | Age of the most recent poll (seconds) |
| `snmp_polling_interval` | Double | Current polling interval (seconds) |
| `snmp_polling_success_count` | Int64 | Cumulative successful SNMP polls |
| `snmp_server_online` | Boolean | `True` when the SNMP agent is reachable |
| `cls_state` | Int32 | Bridge connection state: `0` = offline, `1` = online |

Variables become `UncertainLastUsableValue` if the SNMP agent is unreachable, and transition to `BadNoCommunication` after `--variable-lifetime` seconds.

## Command Methods

All methods are under `Objects/UCTS/` and return `Int32`: `0` = success, non-zero = failure. UDP commands include a 2-second echo-back timeout.

### `Start() → Int32`
Enable the TDC, reset event/busy counters, enable external trigger reception.

### `Reset() → Int32`
Stop the TDC, reset all event and busy counters.

### `Configure(PC_IP_ADDRESS: String, UCTS_IP_ADDRESS: String, PC_MAC_ADDRESS: String) → Int32`
Set the destination IP and MAC address for timestamp delivery and update the UCTS board's own IP.

### `ScheduleTrigger(timestamp_UTC_ISO: String) → Int32`
Schedule a software trigger at a UTC timestamp (ISO 8601, e.g. `2025-06-15T14:30:45.123456`). Converted to TAI (hardcoded 37 s offset, valid since 2017) and encoded into a 64-bit command word at 8 ns resolution.

### `SetDstIpAddress(ip_address: String) → Int32`
Set the destination IP address for timestamp delivery.

### `SetDstPort(port: Int32) → Int32`
Set the destination UDP port for timestamp delivery.

### `XMLConfiguration(XML_Message: String) → Int32`
Apply configuration via an XML message fragment. Supported tags: `<MACAddress>`, `<DstIpAddress>`, `<DstPort>`, `<SPI>`. Returns `0` only if all commands succeed.

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
