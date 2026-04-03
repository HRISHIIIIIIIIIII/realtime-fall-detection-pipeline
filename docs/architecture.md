# MQTT → Kafka → Flink → Delta Lake: Architecture Guide

## 1. What This Project Does

This is a real-time data streaming pipeline for fall detection sensor data.
Multiple radar sensors deployed in rooms detect people, track their positions,
and identify fall events. This pipeline collects that data, processes it,
and stores it in Delta Lake tables for analysis.

Previously this data was stored in Databricks (managed Delta Lake). This PoC
replaces Databricks with a self-hosted, open-source stack — same Delta Lake
format, no subscription required.

## 2. Pipeline Overview

```
Multiple PCs (radar sensors)
    │
    ├── FDS data ──→ Remote MQTT Broker (3.6.75.203)
    │                        │
    └── OBJ data ──→ Remote MQTT Broker (3.3.5.3)
                             │
    ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
    kcsn0010 (this machine)  │  (all services in Docker)
                             ▼
                    MQTT-Kafka Bridge (Python)
                      │              │
                      ▼              ▼
                  Kafka topic    Kafka topic
                  "fds-data"    "obj-data"
                      │              │
                      ▼              ▼
                     PyFlink Job
                      │              │
                      ▼              ▼
              Kafka output topics (4 topics)
              ┌─ bronze-fds  ┌─ bronze-obj
              └─ silver-fds  └─ silver-obj
                      │              │
                      ▼              ▼
                  Delta Lake Writer (Python)
                      │              │
                      ▼              ▼
                  Delta Lake (local filesystem)
                  ├── bronze_fds/    (raw FDS events)
                  ├── bronze_obj/    (raw OBJ tracking)
                  ├── silver_fds/    (cleaned & parsed)
                  └── silver_obj/    (flattened, 1 row per person)
```

### Why Flink writes to Kafka instead of directly to Delta Lake

PyFlink depends on `apache-beam`, which requires `pyarrow < 10`.
Delta Lake's Python library (`deltalake`) requires `pyarrow >= 16`.
These are fundamentally incompatible — no version of pyarrow satisfies both.

The solution is a common production pattern: **separate stream processing
from storage writing**. Flink handles the transformations and writes to
intermediate Kafka topics. A separate lightweight Python service (Delta Writer)
reads those topics and writes to Delta Lake. Each service has its own
container with compatible dependencies.

See [troubleshooting.md](troubleshooting.md) Section 4 for the full story.

---

## 3. Component Deep Dive

### 3.1 MQTT (Message Queuing Telemetry Transport)

**What is it?**
MQTT is a lightweight messaging protocol designed for IoT devices. It follows
a publish/subscribe pattern — devices PUBLISH messages to TOPICS, and
interested clients SUBSCRIBE to those topics.

**Key concepts:**
- **Broker**: The central server that routes messages (we use Mosquitto)
- **Topic**: A string like "sensors/room1/fds" — think of it as a channel
- **Publisher**: Sends messages to a topic
- **Subscriber**: Receives messages from a topic
- **QoS (Quality of Service)**:
  - QoS 0: Fire and forget (fastest, may lose messages)
  - QoS 1: At least once delivery (may duplicate)
  - QoS 2: Exactly once delivery (slowest)

**Why MQTT for IoT?**
- Extremely lightweight (runs on tiny devices)
- Low bandwidth (small packet headers)
- Works over unreliable networks
- Built-in keep-alive and last-will messages

**In our pipeline:**
- Radar sensors on multiple PCs publish FDS and OBJ data to remote MQTT brokers
- FDS data goes to broker at 3.6.75.203 (topic: "alerts")
- OBJ data goes to broker at 3.3.5.3 (topic: "obj")
- The existing Mosquitto broker on kcsn0010 (port 1883) is used by both
  the IoT bridge scripts and the sensor simulator for testing

### 3.2 Apache Kafka

**What is it?**
Kafka is a distributed event streaming platform. Think of it as a
"commit log" — an append-only, ordered, durable sequence of messages.

**Why not just use MQTT for everything?**

| MQTT | Kafka |
|------|-------|
| Messages disappear after delivery | Messages are stored on disk (days/weeks) |
| No replay — once consumed, gone | Any consumer can replay from any point |
| Single broker, limited throughput | Distributed, handles millions of msgs/sec |
| Great for IoT devices | Great for data processing pipelines |

**Key concepts:**
- **Topic**: A named stream of messages (e.g., "fds-data")
- **Partition**: A topic is split into partitions for parallelism.
  Messages with the same KEY always go to the same partition,
  preserving order per key.
- **Producer**: Sends messages to Kafka (our MQTT-Kafka bridge)
- **Consumer**: Reads messages from Kafka (our PyFlink job)
- **Consumer Group**: Multiple consumers sharing the work of reading
  a topic. Each partition is read by only one consumer in the group.
- **Offset**: A sequential ID for each message in a partition.
  Consumers track their offset to know where they left off.
- **Broker**: A Kafka server. In production you'd have 3+.
  For our PoC, we use 1.

**KRaft mode (no Zookeeper):**
Older Kafka required Zookeeper (a separate coordination service) to manage
brokers. Since Kafka 3.3+, KRaft mode handles this internally — one fewer
service to run. We use KRaft mode with `apache/kafka:3.9.0`.

**In our pipeline — 6 Kafka topics:**

| Topic | Producer | Consumer | Purpose |
|-------|----------|----------|---------|
| `fds-data` | MQTT-Kafka Bridge | PyFlink | Raw FDS messages from MQTT |
| `obj-data` | MQTT-Kafka Bridge | PyFlink | Raw OBJ messages from MQTT |
| `bronze-fds` | PyFlink | Delta Writer | Raw FDS (passthrough) |
| `bronze-obj` | PyFlink | Delta Writer | Raw OBJ (passthrough) |
| `silver-fds` | PyFlink | Delta Writer | Cleaned/parsed FDS |
| `silver-obj` | PyFlink | Delta Writer | Flattened OBJ (1 row per person) |

Message key = device_id (board_sn / board) — ensures all data from
one device stays ordered in the same partition.

### 3.3 Apache Flink (PyFlink)

**What is it?**
Flink is a stream processing engine. It reads a continuous stream of data,
processes it in real-time, and outputs results — all with fault tolerance.

**Why Flink instead of just a Python script?**
A simple Python script could read from Kafka and process data.
But Flink gives you:
- **Windowing**: Group events by time windows (e.g., "falls per 5 minutes")
- **Event-time processing**: Handle late/out-of-order data correctly
- **Exactly-once semantics**: Guarantees each event is processed once
- **Fault tolerance**: Checkpoints state, recovers from crashes
- **Scalability**: Distribute processing across multiple machines
- **Backpressure handling**: Slows down if downstream can't keep up

**Key concepts:**
- **JobManager**: The "brain" — coordinates the job, manages checkpoints
- **TaskManager**: The "workers" — actually process the data
- **Job**: Your processing logic (our fall_detection_job.py)
- **Source**: Where data comes from (Kafka consumer)
- **Sink**: Where results go (Kafka output topics)
- **Watermark**: Flink's way of tracking "how far along in time" the
  stream has progressed. Used to know when a time window is complete.
- **Window**: A time-bounded bucket for grouping events
  - Tumbling: Fixed-size, non-overlapping (e.g., every 30 seconds)
  - Sliding: Fixed-size, overlapping (e.g., 30 sec window every 10 sec)

**Flink DataStream API operators used in this project:**
- `.map(fn)`: Apply a function to every element — 1 input → 1 output
- `.flat_map(fn)`: Apply a function — 1 input → 0 or more outputs
  (used for OBJ flattening: 1 message → N person rows)
- `.filter(fn)`: Keep only elements where fn returns True
- `.union()`: Merge two streams into one
- `.sink_to()`: Write the stream to an output (Kafka)

**PyFlink:**
Flink is written in Java, but PyFlink lets you write jobs in Python.
Under the hood, it communicates with the JVM via Apache Beam's
portability layer. Slightly slower than Java Flink but much more
accessible for Python developers.

**In our pipeline:**
- PyFlink reads from Kafka topics "fds-data" and "obj-data"
- Processes: parses, cleans, flattens nested JSON
- Writes results to 4 output Kafka topics (bronze + silver for each data type)
- Does NOT write to Delta Lake directly (see dependency conflict note above)

### 3.4 Delta Lake Writer (Custom Service)

**What is it?**
A lightweight Python service that reads from Flink's output Kafka topics
and writes to Delta Lake tables on the local filesystem.

**Why a separate service?**
Because of the pyarrow dependency conflict between PyFlink and deltalake
(see Section 2 above). By running in its own container, it can use
whatever pyarrow version deltalake needs.

**Micro-batching pattern:**
Writing one Parquet file per Kafka message would create thousands of tiny
files (the "small files problem"). Instead, the writer accumulates rows
in memory and flushes them as a batch:
- Every `FLUSH_INTERVAL` seconds (default: 10), OR
- When the buffer reaches `FLUSH_SIZE` rows (default: 100)
- Whichever comes first

Each flush creates one Parquet file + one _delta_log entry.

### 3.5 Delta Lake

**What is it?**
Delta Lake is an open-source storage layer that brings ACID transactions
to data lakes. It is NOT a database — it's a file format (Parquet files +
a JSON transaction log).

**The problem it solves:**
Without Delta Lake, a data lake is just a folder of Parquet/CSV files.
This causes problems:
- Partial writes: Job crashes midway → corrupted data
- No schema enforcement: Someone writes wrong column types → silent errors
- No rollback: Bad data goes in → no easy way to undo
- Read/write conflicts: Reader sees half-written files

**How Delta Lake works:**

```
my_table/
├── _delta_log/                      ← Transaction log
│   ├── 00000000000000000000.json    ← "Version 0: added file-001.parquet"
│   ├── 00000000000000000001.json    ← "Version 1: added file-002.parquet"
│   └── 00000000000000000002.json    ← "Version 2: added file-003.parquet"
├── file-001.parquet                 ← Actual data
├── file-002.parquet
└── file-003.parquet
```

Each JSON file in `_delta_log/` records what changed:
- Which Parquet files were added
- Which were removed
- Schema information
- Statistics (min/max values per column)

**Key features:**
- **ACID transactions**: Writes fully succeed or fully fail
- **Schema enforcement**: Rejects data with wrong types
- **Time travel**: Query data as it was at any past version
  `deltalake.DeltaTable("path", version=5)` reads version 5
- **Schema evolution**: Add new columns without rewriting old data

**We were using Databricks — what changes?**
Nothing about the format changes. Delta Lake is open source.
Databricks just provided managed compute (Spark clusters) and storage.
Now we provide our own compute (PyFlink + Delta Writer) and storage (local disk).
The Delta table files are 100% compatible.

**In our pipeline:**
- Python `deltalake` package (built on delta-rs, a Rust implementation)
- Writes to local filesystem: `./data/delta/`
- Four tables: `bronze_fds`, `bronze_obj`, `silver_fds`, `silver_obj`

### 3.6 Medallion Architecture (Bronze → Silver → Gold)

A data organization pattern where data flows through layers of
increasing quality:

**Bronze (Raw):**
- Exact copy of source data, no transformation
- Includes duplicates, errors, inconsistencies
- Purpose: preserve the original data forever
- If upstream processing has bugs, you can always reprocess from Bronze

**Silver (Cleaned & Conformed):**
- Duplicates removed
- Timestamps parsed and standardized
- Nested JSON flattened into columns
- Column names standardized (board_sn → device_id)
- Data types enforced
- Ready for analysis and ad-hoc queries

**Gold (Business-level Aggregations):**
- Pre-computed metrics (falls per hour per room)
- Optimized for dashboards and reports
- Not implemented in this PoC

---

## 4. Data Schemas

### 4.1 FDS (Fall Detection System) — Raw Message

```json
{
  "fn": 1411,
  "live": 30,
  "time": "2026/03/31-03:36:30",
  "type": "FDS",
  "report": "3,active,notify,Falling",
  "result": "Falling",
  "activity": 100,
  "board_sn": "kc2508p025",
  "location": "room1",
  "platform": "Intel NUC, Ubuntu 20.04",
  "position": "0.80,1.40",
  "model_name": "ModelV01",
  "model_index": "0",
  "software_version": "V20.04.22_2"
}
```

**Field descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| fn | int | Frame number (sequential counter from the sensor) |
| live | int | Heartbeat/liveness counter |
| time | string | Timestamp from the device (format: YYYY/MM/DD-HH:MM:SS) |
| type | string | Always "FDS" for fall detection messages |
| report | string | CSV status: zone, state, alert_level, result |
| result | string | Fall detection verdict: "Falling" or "None" |
| activity | int | Activity level (0-100) |
| board_sn | string | Device serial number (e.g., "kc2508p025") |
| location | string | Room where sensor is installed |
| platform | string | Edge hardware/OS info |
| position | string | Person's x,y position in meters ("0.80,1.40") |
| model_name | string | AI model used for fall detection |
| model_index | string | Model variant index |
| software_version | string | Firmware/software version |

### 4.2 OBJ (Object Tracking) — Raw Message

```json
{
  "fn": 75483,
  "time": "2026/04/02-04:09:10",
  "board": "kc2508p020",
  "location": "room1",
  "obj": {
    "FDS_185": {
      "max_coords": [0.0805, 1.511, 1.2352],
      "min_coords": [-0.0153, 1.1955, 0.8992],
      "center": [0.035, 1.071],
      "vol": 0.0102,
      "pts": 23,
      "velocity": -0.019,
      "p_value": 9.999,
      "ai_state": "notify_tmp",
      "energy_acc": 0.0
    }
  }
}
```

**Note:** The `obj` field can contain multiple tracked people (e.g., FDS_185 *and* FDS_186) in a single message. The OBJ flattening process creates one row per person.

**Field descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| fn | int | Frame number |
| time | string | Timestamp from device |
| board | string | Device serial number (note: FDS uses `board_sn`, OBJ uses `board`) |
| location | string | Room where sensor is installed |
| obj | object | Dictionary of tracked objects (people) |

**Each tracked object (e.g., FDS_185) contains:**

| Field | Type | Description |
|-------|------|-------------|
| max_coords | [x,y,z] | 3D bounding box maximum corner (meters) |
| min_coords | [x,y,z] | 3D bounding box minimum corner (meters) |
| center | [x,y] | Center position of the person (meters) |
| vol | float | Bounding box volume (cubic meters) |
| pts | int | Number of radar point cloud points |
| velocity | float | Movement velocity (m/s, negative = moving away) |
| p_value | float | Confidence/probability score |
| ai_state | string | AI classification state |
| energy_acc | float | Accumulated energy metric |

### 4.3 Silver FDS Schema (after transformation by PyFlink)

| Column | Type | Transformation |
|--------|------|---------------|
| device_id | string | Renamed from `board_sn` |
| event_time | string (ISO 8601) | Parsed from "2026/03/31-03:36:30" → "2026-03-31T03:36:30" |
| frame_number | int | Renamed from `fn` |
| result | string | "Falling" or "None" |
| is_falling | boolean | `true` if result == "Falling" |
| activity | int | As-is |
| position_x | float | Parsed from "0.80,1.40" → 0.80 |
| position_y | float | Parsed from "0.80,1.40" → 1.40 |
| location | string | As-is |
| report_zone | string | Parsed from report CSV → "3" |
| report_state | string | Parsed from report CSV → "active" |
| report_alert | string | Parsed from report CSV → "notify" |
| model_name | string | As-is |
| software_version | string | As-is |
| ingestion_time | string (ISO 8601) | When the pipeline received it |

### 4.4 Silver OBJ Schema (after flattening by PyFlink)

One row per tracked person per frame:

| Column | Type | Transformation |
|--------|------|---------------|
| device_id | string | Renamed from `board` |
| event_time | string (ISO 8601) | Parsed from time field |
| frame_number | int | Renamed from `fn` |
| location | string | As-is |
| object_id | string | The key name (e.g., "FDS_185") |
| center_x | float | Extracted from center[0] |
| center_y | float | Extracted from center[1] |
| min_x | float | Extracted from min_coords[0] |
| min_y | float | Extracted from min_coords[1] |
| min_z | float | Extracted from min_coords[2] |
| max_x | float | Extracted from max_coords[0] |
| max_y | float | Extracted from max_coords[1] |
| max_z | float | Extracted from max_coords[2] |
| height | float | Computed: max_z - min_z |
| volume | float | Renamed from `vol` |
| point_count | int | Renamed from `pts` |
| velocity | float | As-is |
| confidence | float | Renamed from `p_value` |
| ai_state | string | As-is |
| energy_acc | float | As-is |
| ingestion_time | string (ISO 8601) | When the pipeline received it |

---

## 5. Docker Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| kafka | apache/kafka:3.9.0 | 9092 | Distributed event streaming (KRaft mode, no Zookeeper) |
| sensor-simulator | python:3.10-slim (custom) | — | Generates fake FDS and OBJ data for testing |
| mqtt-kafka-bridge | python:3.10-slim (custom) | — | Subscribes to MQTT topics, produces to Kafka |
| flink-jobmanager | flink:1.18 (custom) | 8081 | Flink coordinator + Web UI |
| flink-taskmanager | flink:1.18 (custom) | — | Flink worker (processes data) |
| delta-writer | python:3.10-slim (custom) | — | Reads from Kafka, writes to Delta Lake |

**Note:** Mosquitto is NOT containerized — the existing system Mosquitto on
kcsn0010 (port 1883) is used directly. See [troubleshooting.md](troubleshooting.md)
Section 1 for details on why.

### 5.1 Networking

```
┌─────────────────────────────────────────────────────┐
│  Docker "pipeline" network                          │
│                                                     │
│  kafka ←──→ mqtt-kafka-bridge ←──→ flink-*          │
│               │                      │              │
│               │                    delta-writer      │
│               │                                     │
└───────────────│─────────────────────────────────────┘
                │
                │ extra_hosts: host.docker.internal
                ▼
        Host Mosquitto (192.168.29.44:1883)

┌─────────────────────────────────────────────────────┐
│  Host network (network_mode: host)                  │
│                                                     │
│  sensor-simulator ──→ Host Mosquitto (localhost:1883)│
└─────────────────────────────────────────────────────┘
```

- Services on the `pipeline` network can reach each other by container name
  (e.g., `kafka:9092`, `flink-jobmanager:8081`)
- `mqtt-kafka-bridge` uses `extra_hosts` to map `host.docker.internal` to
  the host machine's IP, so it can reach Mosquitto
- `sensor-simulator` uses `network_mode: host` to share the host's network
  stack directly

---

## 6. How to Run

```bash
# Start all services
docker compose up -d

# Watch logs
docker compose logs -f

# Check Flink UI
# Open http://localhost:8081 in browser

# Submit the Flink job
docker compose exec flink-jobmanager /opt/flink/bin/flink run \
    --python /opt/flink/jobs/fall_detection_job.py

# Verify Delta Lake tables
python scripts/query_delta.py

# Stop everything
docker compose down
```

---

## 7. Project Directory Structure

```
mqtt-kafka-flink-poc/
├── docker-compose.yml              # Orchestrates all services
├── .gitignore
│
├── src/
│   ├── sensor_simulator/           # Fake data generator
│   │   ├── simulator.py
│   │   ├── requirements.txt        # paho-mqtt
│   │   └── Dockerfile
│   │
│   ├── mqtt_kafka_bridge/          # MQTT → Kafka forwarder
│   │   ├── bridge.py
│   │   ├── requirements.txt        # paho-mqtt, kafka-python-ng
│   │   └── Dockerfile
│   │
│   ├── flink_jobs/                 # PyFlink stream processing
│   │   ├── fall_detection_job.py
│   │   ├── requirements.txt        # apache-flink, apache-flink-libraries
│   │   └── Dockerfile
│   │
│   └── delta_writer/               # Kafka → Delta Lake writer
│       ├── writer.py
│       ├── requirements.txt        # kafka-python-ng, deltalake, pyarrow, pandas
│       └── Dockerfile
│
├── data/
│   └── delta/                      # Delta Lake tables (Docker volume mount)
│       ├── bronze_fds/
│       ├── bronze_obj/
│       ├── silver_fds/
│       └── silver_obj/
│
├── scripts/
│   └── query_delta.py              # Standalone script to read/verify Delta tables
│
└── docs/
    ├── architecture.md             # This file
    └── troubleshooting.md          # Common issues and solutions
```

---

## 8. Python Dependencies by Service

| Service | Package | Version | Purpose |
|---------|---------|---------|---------|
| sensor-simulator | paho-mqtt | 2.1.0 | MQTT client library |
| mqtt-kafka-bridge | paho-mqtt | 2.1.0 | MQTT subscriber |
| mqtt-kafka-bridge | kafka-python-ng | 2.2.2 | Kafka producer |
| flink-jobs | apache-flink | 1.18.1 | Stream processing framework |
| flink-jobs | apache-flink-libraries | 1.18.1 | Flink connector support |
| delta-writer | kafka-python-ng | 2.2.2 | Kafka consumer |
| delta-writer | deltalake | 0.22.3 | Delta Lake writer (delta-rs) |
| delta-writer | pyarrow | >=16 | Columnar data format (required by deltalake) |
| delta-writer | pandas | 2.2.3 | DataFrame operations for batching |

**Why `kafka-python-ng` instead of `kafka-python`?**
The original `kafka-python` library is no longer maintained (last release 2020).
`kafka-python-ng` is an actively maintained community fork.
