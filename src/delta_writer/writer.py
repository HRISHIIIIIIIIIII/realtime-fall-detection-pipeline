"""
Delta Lake Writer — Reads processed data from Kafka and writes to Delta Lake.

This service is separated from Flink because of a dependency conflict:
  - PyFlink requires pyarrow < 10 (via apache-beam)
  - deltalake requires pyarrow >= 16
  They cannot coexist in the same Python environment.

This is actually a common production pattern: the stream processor
(Flink) handles transformations, and a separate service handles
the storage layer.

How it works:
  1. Subscribes to 4 Kafka topics (bronze-fds, silver-fds, bronze-obj, silver-obj)
  2. Accumulates messages in memory buffers (micro-batching)
  3. Every FLUSH_INTERVAL seconds or FLUSH_SIZE rows, writes a batch
     to the corresponding Delta Lake table
"""

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import time
import os
import threading
from datetime import datetime

import pandas as pd
from deltalake import write_deltalake

# --- Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
DELTA_BASE_PATH = os.getenv("DELTA_BASE_PATH", "/data/delta")
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", "2"))  # seconds
FLUSH_SIZE = int(os.getenv("FLUSH_SIZE", "25"))  # rows

# --- Topic to Delta table mapping ---
TOPIC_TABLE_MAP = {
    "bronze-fds": "bronze_fds",
    "silver-fds": "silver_fds",
    "bronze-obj": "bronze_obj",
    "silver-obj": "silver_obj",
}

# --- Buffers for micro-batching ---
buffers = {table: [] for table in TOPIC_TABLE_MAP.values()}
buffer_lock = threading.Lock()


def get_table_path(table_name):
    return os.path.join(DELTA_BASE_PATH, table_name)


def flush_buffer(table_name):
    """
    Write buffered rows to a Delta Lake table.

    Steps:
    1. Take the rows from the buffer (thread-safe with lock)
    2. Convert to pandas DataFrame
    3. Write to Delta Lake using write_deltalake()
       - mode="append": adds new data without touching existing data
       - This creates a new Parquet file + updates _delta_log/
    """
    with buffer_lock:
        rows = buffers[table_name]
        if not rows:
            return
        batch = rows.copy()
        buffers[table_name] = []

    try:
        df = pd.DataFrame(batch)
        table_path = get_table_path(table_name)
        os.makedirs(table_path, exist_ok=True)

        write_deltalake(
            table_path,
            df,
            mode="append",
        )
        print(f"[DELTA] Wrote {len(batch)} rows to {table_name}")
    except Exception as e:
        print(f"[ERROR] Failed to write to {table_name}: {e}")
        # Put rows back to avoid data loss
        with buffer_lock:
            buffers[table_name] = batch + buffers[table_name]


def periodic_flush():
    """Background thread: flush all buffers every FLUSH_INTERVAL seconds."""
    while True:
        time.sleep(FLUSH_INTERVAL)
        for table_name in buffers:
            flush_buffer(table_name)


def connect_kafka():
    """Create a Kafka consumer with retry logic."""
    topics = list(TOPIC_TABLE_MAP.keys())
    while True:
        try:
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=KAFKA_BROKER,
                group_id="delta-writer",
                # Read messages as UTF-8 strings
                value_deserializer=lambda m: m.decode("utf-8"),
                # Start from the latest message (don't replay old data)
                auto_offset_reset="latest",
                # Commit offsets automatically every 5 seconds
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                # Poll timeout
                consumer_timeout_ms=1000,
            )
            print(f"Connected to Kafka: {KAFKA_BROKER}")
            print(f"Subscribed to: {', '.join(topics)}")
            return consumer
        except NoBrokersAvailable:
            print("Kafka not available. Retrying in 5s...")
            time.sleep(5)


def main():
    consumer = connect_kafka()

    # Start periodic flush thread
    flush_thread = threading.Thread(target=periodic_flush, daemon=True)
    flush_thread.start()

    print(f"Delta Writer started")
    print(f"  Delta path: {DELTA_BASE_PATH}")
    print(f"  Flush interval: {FLUSH_INTERVAL}s, Flush size: {FLUSH_SIZE}")

    try:
        while True:
            # Poll for messages (non-blocking, returns after consumer_timeout_ms)
            messages = consumer.poll(timeout_ms=1000)

            for topic_partition, records in messages.items():
                topic = topic_partition.topic
                table_name = TOPIC_TABLE_MAP.get(topic)
                if not table_name:
                    continue

                for record in records:
                    try:
                        row = json.loads(record.value)
                        with buffer_lock:
                            buffers[table_name].append(row)
                            current_size = len(buffers[table_name])

                        # Flush if buffer is full
                        if current_size >= FLUSH_SIZE:
                            flush_buffer(table_name)
                    except json.JSONDecodeError:
                        print(f"[WARN] Invalid JSON from {topic}, skipping")

    except KeyboardInterrupt:
        print("\nFlushing remaining buffers...")
        for table_name in buffers:
            flush_buffer(table_name)
        consumer.close()
        print("Delta Writer stopped.")


if __name__ == "__main__":
    main()
