# NectarCAM UCTS OPC UA Server

An OPC UA server bridge for the **UCTS (Universal Clock and Time Stamping)** controller and **TiCkS** timing board, providing real-time monitoring via SNMP and remote command control via UDP.

## Overview

This server bridges SNMP monitoring and UDP command protocols to OPC UA, enabling standardized industrial automation clients to:

- **Monitor** UCTS/TiCkS device metrics (temperature, status, timing information, event counts)
- **Control** TiCkS operations via OPC UA methods (start/reset triggers, configure IP/MAC/port, schedule timestamped events)
- **Integrate** with enterprise environments via the industry-standard OPC UA protocol

The server continuously polls SNMP OIDs from the UCTS device on a configurable interval and computes derived variables (e.g., extracting firmware version and state from packed status bytes). It also exposes seven ICD-defined command methods that translate OPC UA method calls into UDP commands sent to the TiCkS board.

## Architecture

The implementation uses a three-class design:

- **`UCTSPoller`** — Pure SNMP monitoring; polls OIDs at regular intervals and computes derived variables (e.g., merged MAC address, status bit extraction, uptime scaling)
- **`UCTSCommander`** — UDP command interface; handles all TiCkS board communication (reset, start, configure IP/MAC/port, schedule triggers, XML configuration)
- **`UCTSOPCUAServer`** — OPC UA server; builds the address space with monitoring variables and attaches command methods

## Installation

### Prerequisites

- Python 3.7+
- SNMP access to the UCTS board (default: `10.10.3.99:161`)
- UDP access to the TiCkS command port (default: `10.10.3.99:55010`)

### Dependencies

Install required packages:

```bash
pip install pysnmp-lextudio asyncua
```

### Setup

1. Clone or download this repository
2. Ensure `snmp_asyncua_bridge.py` is in the same directory or on your `PYTHONPATH`
3. Copy or symlink the configuration files (optional; hardcoded defaults are provided):
   - `device_ucts.json` or `device_ucts_resolved_oids.json`

## Quick Start

### Default Configuration

Run the server with default settings:

```bash
python ucts_asyncua_server.py
```

This will:
- Connect to UCTS board at `10.10.3.99` via SNMP (port 161)
- Listen for OPC UA clients at `opc.tcp://0.0.0.0:4840/ucts/`
- Poll SNMP variables every 1 second
- Log at INFO level to stdout

### Custom Configuration

```bash
python ucts_asyncua_server.py \
  --ucts-ip 192.168.1.100 \
  --ucts-snmp-port 161 \
  --opcua-endpoint opc.tcp://0.0.0.0:4840/ucts/ \
  --poll-interval 2.0 \
  --log-level DEBUG \
  --log-file ucts_server.log
```

### Enable OPC UA Authentication

```bash
python ucts_asyncua_server.py --opcua-user admin:secretpassword
```

## Local Testing with Emulator

The package includes a complete UCTS-TiCkS board emulator (`ucts_emulator.py`) and interactive OPC UA test client (`ucts_client.py`). This allows you to test and develop locally without physical hardware.

### Prerequisites

The server requires the `snmp_asyncua_bridge` module, which is a separate package available on GitHub:

1. **Clone the bridge repository:**
   ```bash
   git clone https://github.com/sfegan/nectarcam_snmp_opcua.git
   cd nectarcam_snmp_opcua
   ```

2. **Set PYTHONPATH to include the bridge:**
   ```bash
   export PYTHONPATH="$(pwd):$PYTHONPATH"
   ```
   
   Or add it to your shell profile for persistence (e.g., `~/.bashrc` or `~/.zshrc`):
   ```bash
   export PYTHONPATH="/path/to/nectarcam_snmp_opcua:$PYTHONPATH"
   ```

3. **Return to the UCTS server directory:**
   ```bash
   cd ../nectarcam_ucts_opcua
   ```

### Test Environment Setup

For local testing, you'll run three components in separate terminals:

1. **UCTS Emulator** (simulates the hardware)
2. **OPC UA Server** (the bridge)
3. **Test Client** (interactive testing tool)

### 1. Start the UCTS Emulator

In terminal 1:

```bash
python ucts_emulator.py
```

Default output:
```
Connected: 0.0.0.0:1161 (SNMP agent)
TiCkS command listener: 0.0.0.0:55010
Current state:
  ... [variable values] ...

ucts> 
```

The emulator provides:
- **SNMP agent** on UDP port 1161 (non-privileged alternative to 161)
- **UDP command listener** on port 55010
- **Interactive CLI** to inspect and modify variables in real-time

**Emulator CLI commands** (type `help` for full list):
```
show                              Print all variables
set Temperature 35.2              Set temperature
set EventCount 100                Set counter
set PortLinkStatus 2              Set port status (0=na, 1=down, 2=up)
state 1                           Set TiCkS state (0=Online, 1=Running)
reset                             Reset the board
getready                          Start the board
quit                              Shut down
```

### 2. Start the OPC UA Server

In terminal 2:

```bash
python ucts_asyncua_server.py \
  --ucts-ip localhost \
  --ucts-snmp-port 1161 \
  --poll-interval 0.5 \
  --log-level INFO
```

**Key differences for emulator testing:**
- Use `--ucts-ip localhost` (not `10.10.3.99`) to connect to the emulator
- Use `--ucts-snmp-port 1161` (emulator's non-privileged port, not 161)
- Use `--poll-interval 0.5` for faster updates during testing (default is 1.0)

Expected output:
```
2026-03-20 14:23:45  INFO     ucts_server  Starting OPC UA server on opc.tcp://0.0.0.0:4840/ucts/
2026-03-20 14:23:45  INFO     ucts_server  UCTSPoller registered
2026-03-20 14:23:45  INFO     ucts_server  SNMP polling started: host=localhost:1161
2026-03-20 14:23:46  INFO     ucts_server  UCTSCommander: 7 methods registered
```

### 3. Run the Test Client

In terminal 3:

```bash
python ucts_client.py \
  --endpoint opc.tcp://localhost:4840/ucts/ \
  --namespace http://cta-observatory.org/nectarcam/ucts/
```

Expected output:
```
Connected to opc.tcp://localhost:4840/ucts/  (ns=2)

UCTS Client ready. Type 'help' for commands, 'quit' to exit.

ucts> 
```

### Test Scenarios

#### Scenario 1: Read All Monitoring Variables

```
ucts> read
  BusyCount            = 0                                [Good]
  DstIpAddr            = '10.10.3.250'                    [Good]
  DstMacAddr           = '44:a8:42:44:32:c9'             [Good]
  DstPort              = 55000                            [Good]
  EventCount           = 0                                [Good]
  FirmwareVersion      = '1'                              [Good]
  PortLinkStatus       = 'up'                             [Good]
  State                = 0                                [Good]
  Status               = '0'                              [Good]
  Temperature          = 25.600000381469727               [Good]
  Throttle             = 65535                            [Good]
  TimeTAI              = 1711000000                        [Good]
  TimeTAIString        = '2025-03-20T14:26:40'            [Good]
  UpTime               = '0:00:15.123456'                 [Good]
  WrpcSwVersion        = 'wrpc-v4.2-dirty'                [Good]
```

#### Scenario 2: Subscribe to Variables and Observe Changes

In the test client:
```
ucts> sub Temperature PortLinkStatus EventCount
  Subscribed to 3 variable(s). Changes will print automatically.
```

In the emulator, run:
```
ucts> set Temperature 45.0
ucts> set EventCount 42
ucts> set PortLinkStatus 1
```

In the client, you'll see real-time updates:
```
  [sub] Temperature            = 45.0                         status=Good
  [sub] EventCount             = 42                           status=Good
  [sub] PortLinkStatus         = 'down'                       status=Good
```

#### Scenario 3: Call Control Methods

Start the board:
```
ucts> start
  Start -> rc=0
```

Check state in the emulator:
```
ucts> show
  State: 1 (Running)
  ...
```

Schedule a trigger (5 seconds from now):
```
ucts> trigger 2025-03-20T14:35:00Z
  ScheduleTrigger('2025-03-20T14:35:00Z') -> rc=0
```

Reset the board:
```
ucts> reset
  Reset -> rc=0
```

#### Scenario 4: Configure Network Parameters

```
ucts> configure 192.168.1.100 localhost aa:bb:cc:dd:ee:ff
  Configure -> rc=0

ucts> setip 192.168.1.200
  SetDstIpAddress('192.168.1.200') -> rc=0

ucts> setport 5000
  SetDstPort(5000) -> rc=0
```

Check the emulator to confirm the changes persisted:
```
ucts> show
  DstIpAddr: 192.168.1.200
  DstPort: 5000
  DstMacAddr: aa:bb:cc:dd:ee:ff
  ...
```

#### Scenario 5: Browse the Address Space

```
ucts> browse
  Objects/UCTS
      Configure  (Method)
      Start  (Method)
      Reset  (Method)
      ScheduleTrigger  (Method)
      XMLConfiguration  (Method)
      SetDstIpAddress  (Method)
      SetDstPort  (Method)
      Monitoring  (Object)
        BusyCount  (Variable)  = 0  [Good]
        DstIpAddr  (Variable)  = '192.168.1.200'  [Good]
        ...
```

### Log Level Recommendations

- **INFO (default)** — Normal operation; shows major events (connection, polling, method calls)
- **DEBUG** — Detailed diagnostics; shows every polling cycle, every SNMP request/response
- **WARNING/ERROR** — Only errors; useful for production

For local testing, use DEBUG to see detailed behavior:

```bash
python ucts_asyncua_server.py \
  --ucts-ip localhost \
  --ucts-snmp-port 1161 \
  --log-level DEBUG \
  --log-file test.log
```

Then monitor the log in another terminal:
```bash
tail -f test.log
```

### Emulator Ports & Firewall

By default, the emulator binds to `0.0.0.0` on:
- **UDP 1161** — SNMP agent (configurable with `--snmp-port`)
- **UDP 55010** — TiCkS command listener (configurable with `--cmd-port`)

On macOS, you may see firewall permission prompts for Python; allow them. No root privileges are required because the emulator uses high-numbered ports.

### Troubleshooting Local Tests

**"Connection refused" (server to emulator)**

The server can't connect to the emulator's SNMP agent.

- Verify the emulator is running: Check terminal 1 for `Connected: 0.0.0.0:1161`
- Verify port matches: Use `--ucts-snmp-port 1161` (not 161)
- Verify IP is correct: Use `--ucts-ip localhost` (not `10.10.3.99`)

**"Module not found: snmp_asyncua_bridge"**

The PYTHONPATH is not set correctly.

- Verify the bridge is cloned: `ls ../nectarcam_snmp_opcua/snmp_asyncua_bridge.py`
- Set PYTHONPATH before running: `export PYTHONPATH="../nectarcam_snmp_opcua:$PYTHONPATH"`
- Verify it's set: `echo $PYTHONPATH` should show the bridge path first

**Client can't connect to server**

- Verify server is running: Check terminal 2 for startup messages
- Use `--endpoint opc.tcp://localhost:4840/ucts/` in client (not `0.0.0.0`)
- Check firewall: OPC UA uses TCP 4840

**Variables show "BadWaitingForInitialData"**

This is normal during startup. The server hasn't received its first SNMP poll yet.

- Wait 1–2 seconds for the first poll cycle to complete
- Watch the server logs for initial poll results

**Emulator receives but doesn't respond to commands**

Check that the command port matches.

- Emulator: `--cmd-port 55010` (default)
- Server: `--ucts-cmd-port 55010` (default)
- Should match; if you change one, change both

## Command Line Options

All options have sensible defaults and can be omitted for typical setups.

### UCTS/TiCkS Board Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--ucts-ip` | IP address | `10.10.3.99` | IP address of the UCTS-TiCkS board |
| `--ucts-snmp-port` | port | `161` | SNMP UDP port of the UCTS board |
| `--ucts-cmd-port` | port | `55010` | UDP command port of the TiCkS board |

### SNMP Polling Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--snmp-community` | string | `public` | SNMP community string (read-only; no SET support) |
| `--poll-interval` | seconds | `1.0` | How often to poll SNMP OIDs |
| `--snmp-timeout` | seconds | `2.0` | Timeout per SNMP request (per attempt) |
| `--snmp-retries` | number | `1` | Number of automatic retries after initial attempt fails |

### OPC UA Server Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--opcua-endpoint` | URL | `opc.tcp://0.0.0.0:4840/ucts/` | OPC UA endpoint URL where clients connect |
| `--opcua-namespace` | URI | `http://cta-observatory.org/nectarcam/ucts/` | OPC UA namespace URI (unique identifier for this server's nodes) |
| `--opcua-user` | `USER:PASS` | (none) | Enable authentication; format is `username:password` |

### Monitoring Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--monitoring-path` | string | `Monitoring` | OPC UA path for monitoring variables node (relative to UCTS root) |
| `--variable-lifetime` | seconds | `120.0` | How long monitored variables remain usable after SNMP communication fails; `0` = never expire |

### Logging Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--log-level` | level | `INFO` | Logging level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `--log-file` | path | (none) | Optional rotating log file path; if omitted, logs go to stdout |

### Debugging & Configuration Export

| Option | Type | Description |
|--------|------|-------------|
| `--dump-device-config` | flag | Print fully-resolved device configuration as JSON to stdout and exit. Useful for generating configuration files or debugging. |

## OPC UA Address Space

The server creates an address space under `Objects/UCTS/` with the following structure:

```
Objects/
└── UCTS/                          ← Root node
    ├── Configure()                ← Command Methods
    ├── Start()
    ├── Reset()
    ├── ScheduleTrigger()
    ├── XMLConfiguration()
    ├── SetDstIpAddress()
    ├── SetDstPort()
    └── Monitoring/                ← Monitoring Variables
        ├── BusyCount
        ├── DstIpAddr
        ├── DstMacAddr
        ├── DstPort
        ├── EventCount
        ├── FirmwareVersion
        ├── PortLinkStatus
        ├── State
        ├── Status
        ├── Temperature
        ├── Throttle
        ├── TimeTAI
        ├── TimeTAIString
        ├── UpTime
        ├── UpTimeMilliseconds
        ├── WrpcSwVersion
        ├── SoftwareVersion
        ├── snmp_host              ← Built-in monitoring nodes
        ├── snmp_port
        ├── snmp_polling_timestamp
        ├── snmp_polling_age
        ├── snmp_polling_interval
        ├── snmp_polling_success_count
        ├── snmp_server_online
        └── cls_state
```

## Monitoring Variables

All monitoring variables are located under `Objects/UCTS/Monitoring/`. Variables are periodically updated via SNMP polling.

### Polled (Raw) Variables

These variables are fetched directly from SNMP OIDs:

| Variable | Type | Unit | Description |
|----------|------|------|-------------|
| `BusyCount` | Int32 | count | Number of busy triggers rejected during the run |
| `DstIpAddr` | String | IP | Destination IP address receiving UCTS timestamps |
| `DstPort` | Int32 | port | Destination UDP port receiving UCTS timestamps |
| `EventCount` | Int32 | count | Number of triggers successfully accepted during the run |
| `PortLinkStatus` | String | enum | Port link status: `"up"`, `"down"`, or `"na"` |
| `Temperature` | Float | °C | Temperature of the TiCkS PCB (degrees Celsius) |
| `Throttle` | Int64 | ticks | Throttle parameter of UCTS TiCkS |
| `TimeTAI` | Int64 | seconds | TAI timestamp (International Atomic Time) |
| `TimeTAIString` | String | ISO 8601 | TAI timestamp as ISO 8601 string (e.g., `2024-12-10T13:22:50`) |
| `UpTime` | String | H:MM:SS.ffffff | Uptime of UCTS since last reset (formatted string) |
| `WrpcSwVersion` | String | version | Version of the wrpc (white rabbit precise clock) software |

### Derived Variables

These variables are computed from polled SNMP OIDs during each polling cycle:

| Variable | Type | Source OIDs | Description |
|----------|------|-------------|-------------|
| `Status` | String | `_RawStatus` | Raw status word as decimal string; contains packed bit fields |
| `State` | Int32 | `_RawStatus` (bit 7) | TiCkS operating state: `1` = Running, `0` = Online/Standby, `2` = Unknown |
| `FirmwareVersion` | String | `_RawStatus` (bits 23:16) | Extracted firmware version integer from status bits |
| `DstMacAddr` | String | `_DstMacAddr_32MSB`, `_DstMacAddr_16LSB` | Merged destination MAC address in colon-separated format (e.g., `68:5:ca:3a:8f:28`) |
| `UpTimeMilliseconds` | Double | `UpTime` | Uptime converted to milliseconds for arithmetic operations |

### Constants

| Variable | Type | Value | Description |
|----------|------|-------|-------------|
| `SoftwareVersion` | String | `2.0.0` | Software version of this UCTS controller implementation |

### Built-in (Framework) Variables

These system variables monitor SNMP bridge health and are automatically provided by the underlying SNMP/OPC UA bridge:

| Variable | Type | Description |
|----------|------|-------------|
| `snmp_host` | String | IP address of the SNMP device |
| `snmp_port` | Int32 | SNMP UDP port |
| `snmp_polling_timestamp` | Int64 | Unix timestamp of the most recent polling cycle |
| `snmp_polling_age` | Double | Age of the most recent polling cycle (seconds) |
| `snmp_polling_interval` | Double | Current polling interval (seconds) |
| `snmp_polling_success_count` | Int64 | Cumulative successful SNMP polling attempts |
| `snmp_server_online` | Boolean | `True` when SNMP agent is reachable |
| `cls_state` | Int32 | Bridge connection state: `0` = offline, `1` = online |

## Command Methods

Seven command methods are available under `Objects/UCTS/`. All methods return an Int32 result code: `0` = success, non-zero = failure.

### Configure (PC_IP_ADDRESS: String, UCTS_IP_ADDRESS: String, PC_MAC_ADDRESS: String) → Result: Int32

Updates destination configuration for UCTS timestamp delivery. Sets the IP address and MAC address of the PC receiving timestamped events, plus updates the internal UCTS IP (in case the device is accessed via a different network route).

**Parameters:**
- `PC_IP_ADDRESS` — IPv4 address of the PC receiving timestamps (e.g., `192.168.1.50`)
- `UCTS_IP_ADDRESS` — IPv4 address of the UCTS board itself (e.g., `10.10.3.99`)
- `PC_MAC_ADDRESS` — MAC address of the PC (e.g., `00:11:22:33:44:55`)

**Returns:** `0` on success, `1` on failure

**Example (OPC UA Client):**
```
Call Configure with:
  PC_IP_ADDRESS = "192.168.1.100"
  UCTS_IP_ADDRESS = "10.10.3.99"
  PC_MAC_ADDRESS = "aa:bb:cc:dd:ee:ff"
```

### Start () → Result: Int32

Start the TiCkS board: enable the Time-to-Digital Converter (TDC), reset event/busy counters, and enable external trigger reception.

**Returns:** `0` on success, `1` on failure

**ICD Code:** `0xFFFFFFFFFFFFFFF0`

### Reset () → Result: Int32

Reset the TiCkS board: stop the TDC, reset all event and busy counters.

**Returns:** `0` on success, `1` on failure

**ICD Code:** `0xFFFFFFFFFFFFFF00`

### ScheduleTrigger (timestamp_UTC_ISO: String) → Result: Int32

Schedule a software trigger at a specific UTC timestamp. The timestamp is converted to TAI, then encoded into a 64-bit command word with sub-nanosecond precision (8 ns increments).

**Parameters:**
- `timestamp_UTC_ISO` — UTC timestamp in ISO 8601 format (e.g., `2025-06-15T14:30:45.123456`)

**Returns:** `0` on success, `1` on failure (e.g., timestamp in the past, invalid format)

**Bit Layout:**
```
Bits   3: 0  — Function code (0x2)
Bits  31: 4  — 28-bit sub-second time in units of 8 ns
Bits  56:32  — 25-bit TAI seconds
Bits  63:57  — 0x7F (padding)
```

**Note:** The TAI-UTC offset is currently hardcoded to 37 seconds (valid as of 2017; consult leap second tables for current values).

### XMLConfiguration (XML_Message: String) → Result: Int32

Apply configuration via an XML message fragment. Supported tags:

- `<MACAddress>` — Set destination MAC address (calls `SetDstIpAddress`)
- `<DstIpAddress>` — Set destination IP (calls `set_dst_ip`)
- `<DstPort>` — Set destination UDP port (calls `set_dst_port`)
- `<SPI>` — Set Station Port Interface (calls `set_mac`)

**Parameters:**
- `XML_Message` — XML fragment with one or more supported tags (case-insensitive)

**Returns:** `0` if all commands succeeded, `1` if any command failed

**Example:**
```xml
<MACAddress>aa:bb:cc:dd:ee:ff</MACAddress>
<DstIpAddress>192.168.1.100</DstIpAddress>
<DstPort>5000</DstPort>
```

### SetDstIpAddress (ip_address: String) → Result: Int32

Set the destination IP address for timestamp delivery.

**Parameters:**
- `ip_address` — IPv4 address (e.g., `192.168.1.100`)

**Returns:** `0` on success, `1` on failure

**ICD Code:** Function code 0x4

### SetDstPort (port: Int32) → Result: Int32

Set the destination UDP port for timestamp delivery.

**Parameters:**
- `port` — UDP port number (0–65535)

**Returns:** `0` on success, `1` on failure

**ICD Code:** Function code 0x6

## Configuration Examples

### Example 1: Check Server Configuration

Export the fully resolved configuration as JSON (useful for debugging or creating a config file):

```bash
python ucts_asyncua_server.py --dump-device-config | tee ucts_config.json
```

Output example:
```json
{
  "host": "10.10.3.99",
  "port": 161,
  "community": "public",
  "poll_interval": 1.0,
  "snmp_timeout": 2.0,
  "snmp_retries": 1,
  "opcua_path": "Monitoring",
  "default_lifetime": 120.0,
  "oids": [ ... ]
}
```

### Example 2: Production Deployment

```bash
python ucts_asyncua_server.py \
  --ucts-ip 10.10.3.99 \
  --opcua-endpoint opc.tcp://0.0.0.0:4840/ucts/ \
  --opcua-user admin:SecurePassword123 \
  --poll-interval 0.5 \
  --variable-lifetime 300.0 \
  --log-level INFO \
  --log-file /var/log/ucts_server.log
```

### Example 3: Fast Polling for Testing

```bash
python ucts_asyncua_server.py \
  --poll-interval 0.1 \
  --snmp-timeout 1.0 \
  --variable-lifetime 0 \
  --log-level DEBUG
```

## Status Codes & Health Monitoring

### SNMP Polling Status

The bridge automatically tracks SNMP communication health:

- **`snmp_server_online`** — `True` when the SNMP agent responds normally
- **`snmp_polling_success_count`** — Cumulative successful polls
- **`snmp_polling_age`** — Age of the most recent poll (seconds)

Variables become `UncertainLastUsableValue` if the SNMP agent is unreachable, and transition to `BadNoCommunication` if unreachable for longer than `--variable-lifetime`.

### UDP Command Status

All UDP commands include a 2-second receive timeout waiting for an echo-back acknowledgment from the TiCkS board. Method calls return `0` on success and `1` on timeout/socket error.

## Troubleshooting

### SNMP Connection Fails

1. **Check UCTS IP and port:**
   ```bash
   python ucts_asyncua_server.py --dump-device-config | grep host
   ```

2. **Test SNMP connectivity (requires `snmp-tools`):**
   ```bash
   snmpget -v 2c -c public 10.10.3.99 1.3.6.1.4.1.96.101.1.1.2.0
   ```

3. **Verify firewall allows UDP 161 (SNMP):**
   ```bash
   telnet 10.10.3.99 161  # May not work; try nmap instead
   nmap -sU -p 161 10.10.3.99
   ```

### OPC UA Client Cannot Connect

1. **Check endpoint is accessible:**
   ```bash
   curl -v opc.tcp://0.0.0.0:4840/ucts/
   ```

2. **Verify firewall allows TCP 4840 (OPC UA):**
   ```bash
   nmap -p 4840 localhost
   ```

3. **Enable DEBUG logging:**
   ```bash
   python ucts_asyncua_server.py --log-level DEBUG
   ```

### Variables Show BadWaitingForInitialData

This is normal during startup (before the first SNMP poll completes). Wait 1-2 seconds for the first poll cycle to complete.

### Method Calls Timeout

1. Verify the TiCkS board is reachable on the UDP command port (default: 55010)
2. Increase `--snmp-timeout` if the device is slow

## License

This project is provided under the MIT License. See [LICENSE](LICENSE) for details.

## Support

For issues or questions, contact the NectarCAM team or consult the UCTS/TiCkS documentation.
