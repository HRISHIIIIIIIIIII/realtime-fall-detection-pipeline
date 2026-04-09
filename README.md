# Real-Time Fall Detection Pipeline

MQTT sensors → Kafka → PyFlink → Kafka → Scala Flink (DeltaSink) → Delta Lake + Redis + Feast + FastAPI

## Run with real sensors

```bash
# 1. Clone and enter
git clone git@github.com:HRISHIIIIIIIIIII/realtime-fall-detection-pipeline.git
cd realtime-fall-detection-pipeline

# 2. Start all services (simulator excluded by default)
# Note: flink-delta-writer builds the Scala fat JAR on first run (~5 min)
docker compose up -d

# 3. Create Kafka input topics
# Must be done before submitting the PyFlink job — Flink's AdminClient checks
# for topic existence at startup and fails if they don't exist yet.
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic fds-data --partitions 1 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic obj-data --partitions 1 --replication-factor 1

# 4. Submit the PyFlink processing job
docker compose exec flink-jobmanager /opt/flink/bin/flink run \
    --python /opt/flink/jobs/fall_detection_job.py

# 5. Initialize Feast feature store
docker compose exec redis-writer bash -c "cd /app/feast_repo && feast apply"

# 6. Restart redis-writer to pick up Feast registry
docker compose restart redis-writer
```

## Run with simulator (no real sensors needed)

```bash
# 1. Clone and enter
git clone git@github.com:HRISHIIIIIIIIIII/realtime-fall-detection-pipeline.git
cd realtime-fall-detection-pipeline

# 2. Start all services + simulator
# Note: flink-delta-writer builds the Scala fat JAR on first run (~5 min)
docker compose --profile testing up -d

# 3. Create Kafka input topics
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic fds-data --partitions 1 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic obj-data --partitions 1 --replication-factor 1

# 4. Submit the PyFlink processing job
docker compose exec flink-jobmanager /opt/flink/bin/flink run \
    --python /opt/flink/jobs/fall_detection_job.py

# 5. Initialize Feast feature store
docker compose exec redis-writer bash -c "cd /app/feast_repo && feast apply"

# 6. Restart redis-writer to pick up Feast registry
docker compose restart redis-writer
```

The simulator publishes FDS-only data (fall events) to the MQTT broker. OBJ data comes from real sensors. To publish both FDS and OBJ from simulator, set `PUBLISH_MODE: "both"` in docker-compose.yml.

## Verify

```bash
# Check all containers are running
# flink-delta-writer will show as "exited" after successful JAR submission — that's normal
docker compose ps

# Check Delta Lake tables
python3 scripts/query_delta.py

# Check fall events
python3 scripts/query_delta.py falls

# Check active devices via API
curl http://localhost:8000/api/devices

# Check device details
curl http://localhost:8000/api/devices/kc2508p025

# Check ML features for a device
curl http://localhost:8000/api/features/kc2508p025

# Check recent falls
curl http://localhost:8000/api/falls

# Health check
curl http://localhost:8000/api/health
```

## UIs

| UI | URL |
|----|-----|
| Flink Web UI | http://localhost:8081 |
| FastAPI Swagger Docs | http://localhost:8000/docs |

The Flink Web UI should show **two running jobs**: `Fall Detection Pipeline` (PyFlink) and `Scala Delta Lake Writer`. Data appears in Delta tables every ~30 seconds (checkpoint interval).

## Stop

```bash
docker compose down
```

## Documentation

- [Architecture Guide](docs/architecture.md) — components, data schemas, Feast features, FastAPI endpoints
- [Troubleshooting Guide](docs/troubleshooting.md) — issues and fixes encountered during setup
