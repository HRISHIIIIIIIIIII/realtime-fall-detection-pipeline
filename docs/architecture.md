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
    ├── FDS data (topic: "alerts") ──→ Remote MQTT Broker 
    │                        │
    └── OBJ data (topic: "obj")   ──→ Remote MQTT Broker 
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
              Scala Flink Delta Writer (DeltaSink)
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
from storage writing**. The PyFlink job handles transformations and writes
to intermediate Kafka topics. A separate Flink job written in Scala
(the `flink-delta-writer` service) reads those topics and writes to
Delta Lake using Flink's native `DeltaSink` connector.

By moving the writer to the JVM (Scala), the pyarrow conflict disappears
entirely. The Scala job also gets Flink's checkpointing and exactly-once
delivery for free — no custom micro-batching loop needed.

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
- **Topic**: A string like "alerts" or "sensors/room1/motion" — think of it as a channel
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
- FDS data goes to broker (topic: "alerts")
- OBJ data goes to broker (topic: "obj")
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
| `bronze-fds` | PyFlink | Scala Delta Writer | Raw FDS (passthrough) |
| `bronze-obj` | PyFlink | Scala Delta Writer | Raw OBJ (passthrough) |
| `silver-fds` | PyFlink | Scala Delta Writer, Redis Writer | Cleaned/parsed FDS |
| `silver-obj` | PyFlink | Scala Delta Writer, Redis Writer | Flattened OBJ (1 row per person) |

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

### 3.4 Scala Flink Delta Writer

**What is it?**
A Flink job written in Scala that reads from the 4 processed Kafka topics
and writes to Delta Lake tables using Flink's official `delta-flink` connector
(`DeltaSink`). It runs as a second job on the same Flink cluster alongside
the PyFlink processing job.

**Why Scala instead of Python?**
Because of the pyarrow dependency conflict between PyFlink and `deltalake`
(see Section 2 above). By writing the Delta writer in Scala, it runs on the
JVM — no Python environment, no pyarrow at all. The conflict disappears.

**How DeltaSink works:**
`DeltaSink` is a Flink sink that integrates with Flink's checkpointing:
- Data from Kafka is buffered in memory by Flink
- Every 30 seconds a checkpoint fires
- On checkpoint completion, DeltaSink writes a Parquet file and adds an entry
  to `_delta_log/`

This gives **exactly-once** guarantees: if the job crashes and restarts,
Flink replays from the last checkpoint offset. No data is lost or duplicated.

**Source:** `src/flink_delta_writer_scala/`
- `pom.xml` — Maven build, produces a fat JAR
- `DeltaWriterJob.scala` — main job: 4 Kafka sources → 4 DeltaSinks
- `schemas/` — RowType definitions (column names + types per table)
- `parsers/` — JSON string → Flink Row for each topic
- `Dockerfile` — two-stage: Maven build → submit JAR to Flink cluster

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
| sensor-simulator | python:3.10-slim (custom) | — | Generates fake FDS data for testing (profile: testing) |
| mqtt-kafka-bridge | python:3.10-slim (custom) | — | Subscribes to MQTT topics, produces to Kafka |
| flink-jobmanager | flink:1.18 (custom) | 8081 | Flink coordinator + Web UI |
| flink-taskmanager | flink:1.18 (custom) | — | Flink worker (processes data) |
| flink-delta-writer | eclipse-temurin:17 (custom) | — | Builds Scala fat JAR + submits DeltaSink job to Flink cluster |
| redis | redis:7-alpine | 6379 | In-memory store (Feast online store + hot cache) |
| redis-writer | python:3.10-slim (custom) | — | Kafka → Redis + Feast feature push |
| fastapi-app | python:3.10-slim (custom) | 8000 | REST API + Feast serving + MQTT alerts |

**Note:** Mosquitto is NOT containerized — the existing system Mosquitto on
kcsn0010 (port 1883) is used directly. See [troubleshooting.md](troubleshooting.md)
Section 1 for details on why.

### 5.1 Networking

```
┌──────────────────────────────────────────────────────────────┐
│  Docker "pipeline" network                                   │
│                                                              │
│  kafka ←──→ mqtt-kafka-bridge ←──→ flink-jobmanager         │
│    │                                  │                      │
│    │                          flink-taskmanager              │
│    │                                  │                      │
│    │                         flink-delta-writer              │
│    │                                                         │
│    ├──→ redis-writer ──→ redis                               │
│    │                                                         │
│    └──→ (redis-writer, fastapi-app read from redis)          │
│                                                              │
└──────────────────────────────────────────────────────────────┘
              │
              │ MQTT_BROKER: <remote-mqtt-broker>:1883
              ▼
      Remote MQTT Broker (AWS)

┌─────────────────────────────────────────────────────┐
│  Host network (network_mode: host)                  │
│                                                     │
│  sensor-simulator ──→ Remote MQTT Broker            │
└─────────────────────────────────────────────────────┘
```

- Services on the `pipeline` network can reach each other by container name
  (e.g., `kafka:9092`, `flink-jobmanager:8081`)
- Both `mqtt-kafka-bridge` and `fastapi-app` connect to the remote MQTT broker (configured via `MQTT_BROKER` env var)
- `sensor-simulator` uses `network_mode: host` and also points at the remote broker
- `flink-delta-writer` submits its JAR to `flink-jobmanager:8081` and then exits (the actual job runs on `flink-taskmanager`)

---

## 6. How to Run

```bash
# Start all services (flink-delta-writer builds Scala fat JAR on first run — takes ~5 min)
docker compose up -d

# Watch logs
docker compose logs -f

# Check Flink UI
# Open http://localhost:8081 in browser

# Submit the PyFlink processing job
docker compose exec flink-jobmanager /opt/flink/bin/flink run \
    --python /opt/flink/jobs/fall_detection_job.py

# Initialize Feast feature store (must be done once after first start)
docker compose exec redis-writer bash -c "cd /app/feast_repo && feast apply"

# Restart redis-writer to pick up the Feast registry
docker compose restart redis-writer

# Verify Delta Lake tables (data appears every ~30 seconds via Scala checkpoints)
python scripts/query_delta.py

# Verify fall events only
python scripts/query_delta.py falls

# Stop everything
docker compose down
```

**Running with simulator (no real sensors needed):**
```bash
docker compose --profile testing up -d
```
The simulator publishes FDS-only data (fall events) by default (`PUBLISH_MODE: "alerts"`).
OBJ data comes from real sensors. Set `PUBLISH_MODE: "both"` in docker-compose.yml to simulate both.

**Expected state after full startup:**
- Flink Web UI at `http://localhost:8081` shows **two running jobs**: `Fall Detection Pipeline` (PyFlink) and `Scala Delta Lake Writer`
- `flink-delta-writer` container shows as **exited (0)** — this is normal (it submitted the JAR and exited)
- Data appears in Delta Lake tables every ~30 seconds (checkpoint interval)

---

## 7. Project Directory Structure

```
mqtt-kafka-flink-poc/
├── docker-compose.yml              # Orchestrates all services
├── .gitignore
│
├── src/
│   ├── sensor_simulator/           # Fake data generator (Docker profile: testing)
│   │   ├── simulator.py            # Publishes FDS (and optionally OBJ) to MQTT
│   │   ├── requirements.txt        # paho-mqtt
│   │   └── Dockerfile
│   │
│   ├── mqtt_kafka_bridge/          # MQTT → Kafka forwarder
│   │   ├── bridge.py               # Subscribes to "alerts" + "obj", produces to Kafka
│   │   ├── requirements.txt        # paho-mqtt, kafka-python-ng
│   │   └── Dockerfile
│   │
│   ├── flink_jobs/                 # PyFlink stream processing
│   │   ├── fall_detection_job.py   # Reads fds-data + obj-data → writes bronze/silver topics
│   │   ├── requirements.txt        # apache-flink, apache-flink-libraries
│   │   └── Dockerfile
│   │
│   ├── flink_delta_writer_scala/   # Scala Flink job — writes Kafka → Delta Lake (DeltaSink)
│   │   ├── pom.xml                 # Maven build (produces fat JAR)
│   │   ├── Dockerfile              # Multi-stage: Maven build → flink run submit
│   │   └── src/main/scala/com/kubocare/delta/
│   │       ├── DeltaWriterJob.scala         # Main job: 4 Kafka sources → 4 DeltaSinks
│   │       ├── schemas/
│   │       │   ├── FdsSchemas.scala         # RowType definitions for FDS tables
│   │       │   └── ObjSchemas.scala         # RowType definitions for OBJ tables
│   │       └── parsers/
│   │           ├── BronzeFdsParser.scala    # JSON → Flink Row (bronze_fds)
│   │           ├── SilverFdsParser.scala    # JSON → Flink Row (silver_fds)
│   │           ├── BronzeObjParser.scala    # JSON → Flink Row (bronze_obj)
│   │           └── SilverObjParser.scala    # JSON → Flink Row (silver_obj)
│   │
│   ├── redis_writer/               # Kafka → Redis + Feast feature push
│   │   ├── writer.py               # Consumes silver-fds + silver-obj, writes Redis + Feast
│   │   ├── requirements.txt        # kafka-python-ng, redis, feast[redis], pandas
│   │   └── Dockerfile
│   │
│   ├── fastapi_app/                # REST API + Feast serving + MQTT alerts
│   │   ├── app.py                  # FastAPI endpoints (devices, falls, features, verify-fall)
│   │   ├── requirements.txt        # fastapi, uvicorn, redis, paho-mqtt, feast[redis], pandas
│   │   └── Dockerfile
│   │
│   └── feast_repo/                 # Feast feature store definitions
│       ├── features.py             # Feature views: fds_realtime, obj_realtime, fall_history
│       ├── feature_store.yaml      # Feast config (provider: local, online_store: redis)
│       └── data/                   # Offline store parquet files (used by feast apply)
│
├── data/
│   └── delta/                      # Delta Lake tables (bind-mounted into flink-delta-writer)
│       ├── bronze_fds/             # Raw FDS events (passthrough)
│       ├── bronze_obj/             # Raw OBJ events (passthrough)
│       ├── silver_fds/             # Cleaned FDS (parsed timestamps, split position/report)
│       └── silver_obj/             # Flattened OBJ (1 row per tracked person)
│
├── scripts/
│   └── query_delta.py              # Standalone script to read/verify Delta tables
│
└── docs/
    ├── architecture.md             # This file
    └── troubleshooting.md          # Common issues and solutions
```

---

## 8. Flink Web UI

The Flink Web UI is available at **`http://localhost:8081`** while the pipeline is running.

### 8.1 Job Graph

The Job Graph tab shows a visual map of your pipeline as a DAG (Directed Acyclic Graph):

```
Source: FDS Source -> FlatMap          Source: OBJ Source -> FlatMap
      Parallelism: 1                         Parallelism: 1
      Busy: ~4%                              Busy: ~5%
           │    │    │                            │    │    │
     FORWARD FORWARD FORWARD               FORWARD FORWARD FORWARD
           │    │                                │         │
           ▼    ▼                                ▼         ▼
  FlatMap->Sink  FlatMap->Sink      FlatMap->Sink   FlatMap->Sink
  (bronze-fds)   (silver-fds)      (bronze-obj)    (silver-obj)
```

**What each element means:**

- **Source boxes (top):** Reading from Kafka input topics (`fds-data`, `obj-data`). "Busy 4-5%" means Flink spends 4-5% of its time processing — the rest is waiting for new messages. Normal for low-volume data.
- **Sink boxes (bottom):** Writing to Kafka output topics. Each box shows `Writer -> Committer` — a two-phase pattern for exactly-once delivery: Writer sends the message, Committer confirms Kafka acknowledged it.
- **FORWARD arrows:** Data flows directly between operators on the same thread — no network shuffle. Most efficient routing strategy.
- **Both sources connect to all 4 sinks:** Because of `.union()` in the Flink job — FDS and OBJ streams are merged before routing.

### 8.2 Health Indicators

| Indicator | What it means | Healthy value |
|-----------|--------------|---------------|
| **Backpressured (max)** | Downstream can't keep up — messages piling up | 0% |
| **Busy (max)** | CPU time spent actively processing | Low % = pipeline has headroom |
| **Parallelism** | Number of threads running this operator | 1 (fine for PoC) |

**Backpressure** is the most important metric to watch. If a sink shows high backpressure (turns red), it means Kafka or the Delta Writer can't keep up with incoming data.

### 8.3 Other UI Tabs

| Tab | What you can do |
|-----|----------------|
| **Overview** | See available task slots, running/failed jobs |
| **Metrics** | Live charts: records in/out per second, Kafka consumer lag |
| **Checkpoints** | See checkpoint history, duration, last success time |
| **Task Managers** | JVM heap usage, GC time, network throughput per worker |
| **Exceptions** | Full stack trace if the job fails |

**Actions available from the UI:**
- **Cancel a job** — stops processing cleanly, commits final Kafka offsets
- **Trigger a savepoint** — manually snapshot state before stopping (safe for upgrades)

---

## 9. Latency

**Two latency paths exist in this pipeline:**

### 9.1 Hot path (MQTT → Redis via redis-writer)

End-to-end latency for the hot path (Redis queries, FastAPI `/api/devices`, `/api/falls`) is sub-second:

| Stage | Latency |
|-------|---------|
| MQTT publish → Kafka receive | < 50ms |
| Kafka → PyFlink processing | < 50ms |
| Flink → Kafka silver topics | < 50ms |
| Kafka → redis-writer → Redis | < 100ms |
| **Total** | **< 300ms** |

### 9.2 Cold path (MQTT → Delta Lake via Scala Flink DeltaSink)

End-to-end latency for Delta Lake writes is dominated by the checkpoint interval (30 seconds). DeltaSink buffers data in memory and only flushes a Parquet file + `_delta_log` entry when a Flink checkpoint completes.

| Stage | Latency |
|-------|---------|
| MQTT publish → Kafka silver topics | < 200ms |
| Kafka → Scala Delta Writer buffer | < 50ms |
| Flink checkpoint interval | 0–30,000ms |
| Delta Lake write (Parquet + _delta_log) | 200–500ms |
| **Total** | **~0–30 seconds** |

The 30-second checkpoint interval is intentional — DeltaSink requires checkpointing for exactly-once guarantees, and shorter intervals increase overhead without benefit for analytics workloads.

**Why the hot path exists:**
For truly real-time queries (sub-second freshness for fall alerts, device status), Delta Lake is too slow. Redis provides the hot cache with 10-minute TTL. The Feast feature store on top of Redis serves pre-computed ML features to the fall verification model in `POST /api/verify-fall`.

---

## 10. Python Dependencies by Service

| Service | Package | Version | Purpose |
|---------|---------|---------|---------|
| sensor-simulator | paho-mqtt | 2.1.0 | MQTT client library |
| mqtt-kafka-bridge | paho-mqtt | 2.1.0 | MQTT subscriber |
| mqtt-kafka-bridge | kafka-python-ng | 2.2.2 | Kafka producer |
| flink-jobs | apache-flink | 1.18.1 | Stream processing framework |
| flink-jobs | apache-flink-libraries | 1.18.1 | Flink connector support |
| flink-delta-writer | delta-flink | 3.2.1 | DeltaSink connector for Flink |
| flink-delta-writer | delta-standalone_2.12 | 3.2.1 | Delta transaction log reader/writer |
| flink-delta-writer | flink-connector-kafka | 3.1.0-1.18 | Kafka source for Flink |
| flink-delta-writer | hadoop-client | 3.3.6 | Hadoop FileSystem API (needed by DeltaSink, provided by Flink image) |
| redis-writer | kafka-python-ng | 2.2.2 | Kafka consumer |
| redis-writer | redis | 4.6.0 | Redis client |
| redis-writer | feast[redis] | 0.40.0 | Feature store (push to online store) |
| redis-writer | pandas | 2.2.3 | DataFrame for Feast push |
| fastapi-app | fastapi | 0.115.0 | REST API framework |
| fastapi-app | uvicorn | 0.30.0 | ASGI server for FastAPI |
| fastapi-app | redis | 4.6.0 | Redis client |
| fastapi-app | paho-mqtt | 2.1.0 | MQTT publisher for alerts |
| fastapi-app | feast[redis] | 0.40.0 | Feature store (online feature serving) |
| fastapi-app | pandas | 2.2.3 | DataFrame for Feast queries |

**Why `kafka-python-ng` instead of `kafka-python`?**
The original `kafka-python` library is no longer maintained (last release 2020).
`kafka-python-ng` is an actively maintained community fork.

**Why `redis==4.6.0` instead of `5.x`?**
Feast 0.40.0 requires `redis < 5`. The 4.x API is functionally identical for our use case.

---

## 11. Pipeline Components — Why Each Piece Exists

This section explains each component from first principles: what it is, why it was chosen, and how it fits into the pipeline.

### 11.1 Eclipse Mosquitto (MQTT Broker)

**What it is:** A message broker that speaks the MQTT protocol. Think of it like a post office — publishers drop off messages, subscribers pick them up.

**Why MQTT and not HTTP?**
IoT devices (radar sensors, wearables) are constrained — low power, unstable connections, limited compute. HTTP is too heavy. MQTT is designed for exactly this:
- Tiny packet overhead (~2 bytes header vs ~hundreds for HTTP)
- Persistent connections (device stays connected, no repeated handshakes)
- QoS levels: 0 = fire and forget, 1 = at least once, 2 = exactly once
- If a device disconnects mid-publish, the broker handles it gracefully

**Why not use Kafka directly from the sensor?**
Kafka's protocol is complex and heavy — not suitable for embedded devices. MQTT is the IoT standard. The bridge translates between worlds.

**In this pipeline:** Radar PCs publish FDS data to topic `alerts` and OBJ data to topic `obj` on the remote Mosquitto broker. The local Mosquitto broker on kcsn0010 (port 1883) is used by the sensor simulator.

---

### 11.2 MQTT-Kafka Bridge (`bridge.py`)

**What it is:** A Python script that sits between MQTT and Kafka. It subscribes to MQTT topics and re-publishes those messages as Kafka records.

**Why is this needed?**
MQTT and Kafka are two different worlds:
- MQTT = IoT messaging (lightweight, fire-and-forget, no replay)
- Kafka = data infrastructure (persistent, replayable, scalable)

The bridge is the translation layer. Without it, sensor data would be lost the moment it's consumed — Kafka lets you replay, process, and distribute it to multiple consumers independently.

**Key design decision — `device_id` as Kafka key:**
```
board_sn → Kafka key
```
Kafka uses the key to decide which partition a message goes to. Same key = same partition = **ordering guaranteed per device**. If device-001 sends frame 1, 2, 3 — they always arrive in order.

---

### 11.3 Apache Kafka

**What it is:** A distributed, persistent event log. Every message written to Kafka is stored on disk and can be replayed.

**Why Kafka and not just a queue (RabbitMQ, Redis)?**

| Feature | Queue (RabbitMQ) | Kafka |
|---------|-----------------|-------|
| Message deleted after consumed? | Yes | No — retained for configurable time |
| Multiple consumers? | Hard | Native — each consumer group reads independently |
| Replay old messages? | No | Yes |
| Throughput | Medium | Millions/sec |
| Ordering | Per-queue | Per-partition |

**Key concepts in this pipeline:**
- **Topics:** `fds-data`, `obj-data` (input) → `bronze-fds`, `silver-fds`, `bronze-obj`, `silver-obj` (output)
- **Partitions:** Each topic is split into partitions. Flink's TaskManagers read partitions in parallel
- **Consumer groups:** PyFlink, the Scala Delta Writer, and Redis Writer each have their own group ID, so they read independently without interfering
- **Offsets:** Kafka tracks how far each consumer has read. If any consumer crashes, it resumes from its last committed offset — no data loss

**KRaft mode (no Zookeeper):**
Old Kafka needed Zookeeper (a separate cluster coordination service) just to function. KRaft (Kafka Raft) bakes coordination directly into Kafka. One fewer service to manage.

---

### 11.4 Apache Flink (PyFlink)

**What it is:** A stateful stream processing engine. It reads data from Kafka in real-time, transforms it, and writes results back to Kafka.

**Why Flink and not just Python scripts?**
A plain Python script reading from Kafka works, but:
- No parallelism — one thread, one partition
- No fault tolerance — crashes lose in-flight data
- No windowing — can't do "count falls in last 5 minutes" natively
- No backpressure — if Kafka produces faster than you consume, you fall behind

Flink handles all of this. It's designed to process millions of events per second reliably.

**Key concepts:**

*JobManager vs TaskManager:*
- **JobManager** = the boss. Coordinates the job, assigns work, monitors health
- **TaskManager** = the worker. Actually processes data. You can have many TaskManagers for scale

*DataStream API:*
```
KafkaSource → map/flatMap (transform) → KafkaSink
```

*`flat_map` for OBJ data:*
One OBJ message contains multiple people (array). `flat_map` explodes 1 message → N rows (one per person). A regular `map` can only do 1 → 1.

*Why Flink writes back to Kafka (not directly to Delta Lake):*
PyFlink's dependency (`apache-beam`) requires `pyarrow < 10`. Delta Lake requires `pyarrow >= 16`. Unresolvable conflict. The solution: PyFlink writes to Kafka output topics → a separate Scala Flink job (`flink-delta-writer`) reads those and writes to Delta Lake using `DeltaSink`. This avoids the Python/pyarrow environment entirely. It's also a common production pattern — stream processing and storage writing are naturally separate concerns.

---

### 11.5 Delta Lake (Scala `flink-delta-writer` + DeltaSink)

**What it is:** An open table format that sits on top of Parquet files. Adds ACID transactions, schema enforcement, and time travel to plain files.

**Why Delta Lake and not just Parquet files?**

Plain Parquet has no safety guarantees:
- Write a file → it's there. Crash mid-write → corrupted partial file
- No history — you can't go back to yesterday's data
- No schema enforcement — wrong column types go in silently

Delta Lake adds a `_delta_log/` directory alongside your Parquet files. Every write is recorded as a JSON transaction log entry. This gives you:
- **Atomicity:** Write either fully succeeds or fully fails
- **Time travel:** `DeltaTable(path, version=5)` reads the table as it was at version 5
- **Schema enforcement:** Can't accidentally write a column with wrong type
- **ACID transactions:** Multiple writers won't corrupt each other

**Medallion Architecture in this pipeline:**
```
Bronze  →  raw JSON exactly as it came from MQTT (never modified)
Silver  →  cleaned, parsed, flattened (board_sn → device_id, OBJ array → 1 row per person)
Gold    →  (not yet built) aggregations: falls per hour, avg velocity per zone
```

**Why keep Bronze?**
If the Silver transformation has a bug, you can re-process from Bronze without going back to re-collect sensor data.

**How DeltaSink writes (checkpointing, not micro-batching):**
DeltaSink is not a micro-batch loop — it integrates with Flink's native checkpointing:
- Data from Kafka is buffered in Flink's operator state (in-memory)
- Every 30 seconds a checkpoint fires (configurable via `enableCheckpointing(30_000L)`)
- On checkpoint completion, DeltaSink atomically writes a Parquet file + a `_delta_log` entry
- Flink saves Kafka offsets in the checkpoint — crash recovery replays from the last checkpoint offset

This gives **exactly-once** semantics with zero custom polling code.

---

### 11.6 How It All Connects

```
Radar PCs
    │ MQTT publish (topics: "alerts", "obj")
    ▼
Remote MQTT Broker
    │ paho-mqtt subscribe
    ▼
MQTT-Kafka Bridge (bridge.py)
    │ KafkaProducer — key=device_id
    ▼
Kafka (fds-data, obj-data topics)
    │ KafkaSource — consumer group: flink-fds-consumer / flink-obj-consumer
    ▼
PyFlink (fall_detection_job.py)
    │ ProcessFDS: parse + enrich
    │ ProcessOBJ: parse + flatten per-person
    │ KafkaSink — 4 output topics
    ▼
Kafka (bronze-fds, silver-fds, bronze-obj, silver-obj)
    │
    ├──→ Scala Flink DeltaWriterJob (consumer groups: scala-delta-*)
    │         │ DeltaSink — checkpoint every 30s → Parquet + _delta_log
    │         ▼
    │    Delta Lake (local filesystem)
    │         ├── bronze_fds/   ← raw FDS JSON
    │         ├── silver_fds/   ← parsed, is_falling bool
    │         ├── bronze_obj/   ← raw OBJ JSON
    │         └── silver_obj/   ← one row per tracked person
    │
    └──→ Redis Writer (consumer group: redis-writer)
              │ write to Redis (TTL 600s) + push to Feast online store
              ▼
         Redis + Feast → FastAPI → REST queries + fall verification
```

Each component has a single, well-defined responsibility. That's why the system is debuggable — if something breaks, you check one component at a time.

---

## 12. Hot Path: Redis + Feast + FastAPI

### 12.1 Why a Hot Path?

The cold path (Scala Flink DeltaSink → Delta Lake) is optimized for durable storage and batch analytics.
Data appears every ~30 seconds (checkpoint interval) and requires scanning Parquet files to answer queries.
This is unsuitable for real-time use cases like:
- "What is device kc2508p025 doing RIGHT NOW?"
- "How many people are in room1?"
- "Serve ML features to the fall verification model in <10ms"

The hot path uses Redis (in-memory, sub-millisecond reads) with Feast (feature store)
and FastAPI (REST API + MQTT alerting) to handle real-time queries.

### 12.2 Architecture — Cold Path vs Hot Path

```
Flink → Kafka (silver topics)
              │
              ├──→ Delta Writer → Delta Lake          COLD PATH
              │    (~30s checkpoint interval)          Permanent storage, training data
              │                                        Consumer groups: scala-delta-*
              │
              └──→ Redis Writer → Redis                HOT PATH
                   (streaming, <50ms latency)           10-min TTL, real-time queries
                   Consumer group: redis-writer         Feast online store
                                    │
                                    └──→ Feast          Feature serving layer
                                          │
                                          └──→ FastAPI  REST API + verification model
                                                │
                                                └──→ MQTT (alerts/verified)
```

Both paths consume from the same Kafka topics independently — they have
separate consumer groups, so one doesn't affect the other.

### 12.3 Redis Data Model

All keys auto-expire after 10 minutes (TTL 600s). If a device stops transmitting, its data disappears automatically.

| Key Pattern | Redis Type | What it stores |
|-------------|-----------|---------------|
| `device:{device_id}:fds` | String (JSON) | Latest FDS reading for this device |
| `device:{device_id}:obj` | String (JSON) | Latest OBJ frame with all tracked people |
| `fall:{device_id}:{timestamp}` | String (JSON) | Individual fall event |
| `devices:active` | Set | All device IDs seen in last 10 min |

**Why Redis Sets for active devices?**
`SMEMBERS devices:active` returns all active device IDs in one call — no need to scan keys with patterns.

### 12.4 Feast Feature Store

**What is Feast?**
A feature store that manages ML features. It ensures the same features used during
model training (from batch data in Delta Lake) are available at serving time
(from real-time data in Redis). Without it, training and serving features drift apart.

**Key concepts:**
- **Entity:** The "thing" features are computed for — `device_id` in our case
- **Feature View:** A group of related features with a schema and TTL
- **Push Source:** Streaming ingestion — redis-writer pushes features directly to the online store
- **Online Store:** Redis — sub-millisecond feature retrieval at serving time
- **Offline Store:** File-based (can be upgraded to Delta Lake for training)

**Feature views defined:**

*fds_realtime_features:*
| Feature | Type | Description |
|---------|------|-------------|
| activity | Int64 | Activity level (0-100) |
| position_x | Float64 | X position in room (meters) |
| position_y | Float64 | Y position in room (meters) |
| is_falling | Bool | Current fall status from on-device model |
| result | String | "Falling" or "None" |

*obj_realtime_features:*
| Feature | Type | Description |
|---------|------|-------------|
| people_count | Int64 | Number of people tracked in room |
| avg_velocity | Float64 | Average velocity of tracked people |
| avg_height | Float64 | Average height of tracked people |

*fall_history_features:*
| Feature | Type | Description |
|---------|------|-------------|
| fall_count_10min | Int64 | Number of falls in last 10 minutes |
| last_fall_time | String | Timestamp of most recent fall |
| time_since_last_fall_sec | Float64 | Seconds since last fall |

### 12.5 FastAPI REST API

Access at: `http://localhost:8000`
Swagger docs at: `http://localhost:8000/docs`

| Method | Path | Source | What it does |
|--------|------|--------|-------------|
| GET | `/api/devices` | Redis | List all active devices |
| GET | `/api/devices/{device_id}` | Redis | Latest FDS + OBJ data for one device |
| GET | `/api/falls` | Redis | All fall events in last 10 min |
| GET | `/api/falls/{device_id}` | Redis | Falls for a specific device |
| GET | `/api/features/{device_id}` | Feast | All ML features for the verification model |
| POST | `/api/verify-fall` | Feast + Model | Get features → run model → publish MQTT alert |
| POST | `/api/mqtt/publish` | MQTT | Publish any message to any MQTT topic |
| GET | `/api/health` | All | Redis + MQTT + Feast connectivity check |

### 12.6 Fall Verification Flow

The centralized ML model determines whether an on-device `result: "Falling"` is a
true positive or false positive, using richer context from multiple feature sources.

```
1. Sensor detects fall → publishes FDS with result: "Falling"
2. Flink processes → writes to silver-fds Kafka topic
3. Redis Writer:
   a. Stores fall event in Redis (fall:{device_id}:{timestamp})
   b. Pushes FDS features to Feast (activity, position, is_falling)
   c. Updates fall history features (fall_count_10min, time_since_last_fall)
4. POST /api/verify-fall called with {device_id, event_time}
5. FastAPI queries Feast for all features:
   - activity, position_x, position_y (from FDS)
   - people_count, avg_velocity, avg_height (from OBJ)
   - fall_count_10min, time_since_last_fall_sec (from history)
6. Features fed to verification model → true positive or false positive
7. If true positive → publish to MQTT topic "alerts/verified"
```

The verification model is currently a placeholder (verifies if fall_count >= 1).
Replace with the real model in `src/fastapi_app/app.py` at the `verify_fall` function.
