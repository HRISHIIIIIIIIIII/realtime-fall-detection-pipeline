# Real-Time Fall Detection Pipeline

A real-time data streaming pipeline that processes fall detection sensor data from radar-based IoT devices.

```
MQTT Sensors → Kafka → Apache Flink → Delta Lake
```

## Architecture

```
Multiple PCs (radar sensors)
    │
    ├── FDS data ──→ Remote MQTT Broker
    │                     │
    └── OBJ data ──→ Remote MQTT Broker
                          │
                          ▼
                 MQTT-Kafka Bridge
                   │          │
                   ▼          ▼
               Kafka       Kafka
             (fds-data)  (obj-data)
                   │          │
                   ▼          ▼
                  PyFlink (stream processing)
                   │          │
                   ▼          ▼
               Kafka output topics
             (bronze + silver layers)
                   │          │
                   ▼          ▼
                Delta Lake Writer
                   │          │
                   ▼          ▼
              Delta Lake (local filesystem)
              ├── bronze_fds/
              ├── bronze_obj/
              ├── silver_fds/
              └── silver_obj/
```

## Data Sources

| Source | Description | Frequency |
|--------|-------------|-----------|
| **FDS** (Fall Detection System) | Fall detection events with result `Falling` or `None`, activity level, position | Per-event |
| **OBJ** (Object Tracking) | 3D bounding boxes, velocity, position of tracked people | Per-frame (~10-30 FPS) |

## Medallion Architecture

| Layer | Purpose | Example |
|-------|---------|---------|
| **Bronze** | Raw data exactly as received from MQTT | Original JSON preserved |
| **Silver** | Cleaned, parsed, flattened | `board_sn` → `device_id`, nested OBJ → 1 row per person |
| **Gold** | Business aggregations | Not yet implemented |

## Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| kafka | apache/kafka:3.9.0 | 9092 | Distributed event streaming (KRaft mode) |
| sensor-simulator | python:3.10-slim | — | Generates fake FDS/OBJ data for testing |
| mqtt-kafka-bridge | python:3.10-slim | — | Subscribes to MQTT, produces to Kafka |
| flink-jobmanager | flink:1.18 | 8081 | Flink coordinator + Web UI |
| flink-taskmanager | flink:1.18 | — | Flink worker |
| delta-writer | python:3.10-slim | — | Reads from Kafka, writes to Delta Lake |

## Prerequisites

- Docker and Docker Compose
- Python 3.10+
- Mosquitto MQTT broker running on the host (port 1883)

## Quick Start

```bash
# Start all services
docker compose up -d

# Submit the Flink job
docker compose exec flink-jobmanager /opt/flink/bin/flink run \
    --python /opt/flink/jobs/fall_detection_job.py

# Verify data is flowing
python3 scripts/query_delta.py

# View fall events
python3 scripts/query_delta.py falls

# View Delta Lake version history
python3 scripts/query_delta.py history

# Check Flink Web UI
# Open http://localhost:8081

# Stop everything
docker compose down
```

## Project Structure

```
├── docker-compose.yml
├── src/
│   ├── sensor_simulator/      # Fake data generator
│   ├── mqtt_kafka_bridge/     # MQTT → Kafka
│   ├── flink_jobs/            # PyFlink processing
│   └── delta_writer/          # Kafka → Delta Lake
├── data/delta/                # Delta Lake tables
├── scripts/                   # Query and utility scripts
└── docs/
    ├── architecture.md        # Detailed architecture guide
    └── troubleshooting.md     # Common issues and fixes
```

## Documentation

- [Architecture Guide](docs/architecture.md) — Deep dive into each component, data schemas, concepts
- [Troubleshooting Guide](docs/troubleshooting.md) — Issues encountered during setup and their solutions

## Tech Stack

- **MQTT**: Eclipse Mosquitto — lightweight IoT messaging
- **Kafka**: Apache Kafka 3.9 (KRaft mode) — distributed event streaming
- **Flink**: Apache Flink 1.18 (PyFlink) — stream processing
- **Delta Lake**: delta-rs 0.22 — open-source lakehouse storage
- **Docker Compose** — container orchestration
