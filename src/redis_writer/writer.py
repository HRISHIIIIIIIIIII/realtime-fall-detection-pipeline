"""
Redis Writer + Feast Feature Pusher

Two responsibilities:
  A) Write raw sensor data to Redis for direct FastAPI queries (key-value)
  B) Push computed features to Feast online store for the verification model

Consumes from the same Kafka silver topics as delta-writer, but uses its
own consumer group (redis-writer) so they read independently.
"""

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import redis
import json
import os
import time
from datetime import datetime
from collections import defaultdict

import pandas as pd
from feast import FeatureStore

# --- Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_TTL = int(os.getenv("REDIS_TTL", "600"))  # 10 minutes
FEAST_REPO_PATH = os.getenv("FEAST_REPO_PATH", "/app/feast_repo")

# Kafka topics to consume
TOPICS = ["silver-fds", "silver-obj"]

# --- Fall history tracking (in-memory per device) ---
# { device_id: [datetime1, datetime2, ...] }
fall_history = defaultdict(list)


def connect_redis():
    """Connect to Redis with retry."""
    while True:
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            print(f"Connected to Redis: {REDIS_HOST}:{REDIS_PORT}")
            return r
        except redis.ConnectionError:
            print("Redis not available. Retrying in 5s...")
            time.sleep(5)


def connect_kafka():
    """Connect to Kafka with retry."""
    while True:
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BROKER,
                group_id="redis-writer",
                value_deserializer=lambda m: m.decode("utf-8"),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                consumer_timeout_ms=1000,
            )
            print(f"Connected to Kafka: {KAFKA_BROKER}")
            print(f"Subscribed to: {', '.join(TOPICS)}")
            return consumer
        except NoBrokersAvailable:
            print("Kafka not available. Retrying in 5s...")
            time.sleep(5)


def connect_feast():
    """Initialize Feast feature store."""
    try:
        store = FeatureStore(repo_path=FEAST_REPO_PATH)
        print(f"Feast initialized from: {FEAST_REPO_PATH}")
        return store
    except Exception as e:
        print(f"[WARN] Feast init failed: {e}. Running without Feast.")
        return None


def cleanup_fall_history(device_id):
    """Remove falls older than 10 minutes from history."""
    cutoff = datetime.utcnow().timestamp() - 600
    fall_history[device_id] = [
        t for t in fall_history[device_id] if t.timestamp() > cutoff
    ]


def handle_fds(row, r, feast_store):
    """Process a silver-fds message."""
    device_id = row.get("device_id", "unknown")

    # --- Part A: Direct Redis write ---
    r.set(f"device:{device_id}:fds", json.dumps(row), ex=REDIS_TTL)
    r.sadd("devices:active", device_id)
    r.expire("devices:active", REDIS_TTL)

    # Store fall event
    if row.get("is_falling"):
        event_time = row.get("event_time", "unknown")
        r.set(
            f"fall:{device_id}:{event_time}",
            json.dumps(row),
            ex=REDIS_TTL,
        )
        print(f"[FALL] {device_id} at {event_time}")

    # --- Part B: Push to Feast ---
    if feast_store is None:
        return

    try:
        # FDS features
        fds_df = pd.DataFrame([{
            "device_id": device_id,
            "activity": int(row.get("activity", 0)),
            "position_x": float(row.get("position_x", 0) or 0),
            "position_y": float(row.get("position_y", 0) or 0),
            "is_falling": bool(row.get("is_falling", False)),
            "result": str(row.get("result", "None")),
            "event_time": datetime.utcnow(),
        }])
        feast_store.push("fds_push_source", fds_df)

        # Fall history features
        if row.get("is_falling"):
            fall_history[device_id].append(datetime.utcnow())

        cleanup_fall_history(device_id)
        falls = fall_history[device_id]
        last_fall = falls[-1] if falls else None

        fall_df = pd.DataFrame([{
            "device_id": device_id,
            "fall_count_10min": len(falls),
            "last_fall_time": last_fall.isoformat() if last_fall else "",
            "time_since_last_fall_sec": round(
                (datetime.utcnow() - last_fall).total_seconds(), 1
            ) if last_fall else -1.0,
            "event_time": datetime.utcnow(),
        }])
        feast_store.push("fall_push_source", fall_df)

    except Exception as e:
        print(f"[WARN] Feast push failed (FDS): {e}")


def handle_obj(row, r, feast_store):
    """Process a silver-obj message."""
    device_id = row.get("device_id", "unknown")

    # --- Part A: Direct Redis write ---
    # Append this person's data to the device's OBJ key
    # We store the latest frame's data as a JSON array of people
    obj_key = f"device:{device_id}:obj"
    existing = r.get(obj_key)

    # Check if this is a new frame or same frame
    current_frame = row.get("frame_number")
    if existing:
        existing_data = json.loads(existing)
        existing_frame = existing_data.get("frame_number")
        if existing_frame == current_frame:
            # Same frame — add this person to the people list
            existing_data["people"].append({
                "object_id": row.get("object_id"),
                "center_x": row.get("center_x"),
                "center_y": row.get("center_y"),
                "height": row.get("height"),
                "velocity": row.get("velocity"),
                "confidence": row.get("confidence"),
                "ai_state": row.get("ai_state"),
            })
        else:
            # New frame — start fresh
            existing_data = {
                "device_id": device_id,
                "event_time": row.get("event_time"),
                "location": row.get("location"),
                "frame_number": current_frame,
                "people": [{
                    "object_id": row.get("object_id"),
                    "center_x": row.get("center_x"),
                    "center_y": row.get("center_y"),
                    "height": row.get("height"),
                    "velocity": row.get("velocity"),
                    "confidence": row.get("confidence"),
                    "ai_state": row.get("ai_state"),
                }],
            }
    else:
        existing_data = {
            "device_id": device_id,
            "event_time": row.get("event_time"),
            "location": row.get("location"),
            "frame_number": current_frame,
            "people": [{
                "object_id": row.get("object_id"),
                "center_x": row.get("center_x"),
                "center_y": row.get("center_y"),
                "height": row.get("height"),
                "velocity": row.get("velocity"),
                "confidence": row.get("confidence"),
                "ai_state": row.get("ai_state"),
            }],
        }

    r.set(obj_key, json.dumps(existing_data), ex=REDIS_TTL)
    r.sadd("devices:active", device_id)
    r.expire("devices:active", REDIS_TTL)

    # --- Part B: Push to Feast ---
    if feast_store is None:
        return

    try:
        people = existing_data.get("people", [])
        velocities = [p["velocity"] for p in people if p.get("velocity") is not None]
        heights = [p["height"] for p in people if p.get("height") is not None]

        obj_df = pd.DataFrame([{
            "device_id": device_id,
            "people_count": len(people),
            "avg_velocity": round(sum(velocities) / len(velocities), 3) if velocities else 0.0,
            "avg_height": round(sum(heights) / len(heights), 3) if heights else 0.0,
            "event_time": datetime.utcnow(),
        }])
        feast_store.push("obj_push_source", obj_df)

    except Exception as e:
        print(f"[WARN] Feast push failed (OBJ): {e}")


def main():
    r = connect_redis()
    consumer = connect_kafka()
    feast_store = connect_feast()

    print(f"Redis Writer started")
    print(f"  Redis TTL: {REDIS_TTL}s")
    print(f"  Feast: {'enabled' if feast_store else 'disabled'}")

    try:
        while True:
            messages = consumer.poll(timeout_ms=1000)

            for topic_partition, records in messages.items():
                topic = topic_partition.topic

                for record in records:
                    try:
                        row = json.loads(record.value)

                        if topic == "silver-fds":
                            handle_fds(row, r, feast_store)
                        elif topic == "silver-obj":
                            handle_obj(row, r, feast_store)

                    except json.JSONDecodeError:
                        print(f"[WARN] Invalid JSON from {topic}")

    except KeyboardInterrupt:
        consumer.close()
        print("Redis Writer stopped.")


if __name__ == "__main__":
    main()
