"""
MQTT-Kafka Bridge — Forwards MQTT messages to Kafka topics.

This script is the "glue" between the IoT world (MQTT) and the
big-data world (Kafka).

How it works:
  1. Subscribes to MQTT topics (fds, obj) on the local broker
  2. When a message arrives, determines which Kafka topic to send it to
  3. Produces the raw JSON bytes to Kafka with the device ID as the key

Why use a device ID as the Kafka key?
  Kafka uses the key to decide which PARTITION a message goes to.
  All messages with the same key go to the same partition.
  This guarantees ORDER per device — messages from kc2508p025 are
  always in order, even if messages from different devices interleave.

This follows the same pattern as your existing mqtt_local_bridge.py
but instead of forwarding to a remote MQTT broker, it forwards to Kafka.
"""

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import os

# --- Configuration ---
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# MQTT topics to subscribe to
MQTT_TOPICS = os.getenv("MQTT_TOPICS", "fds,obj").split(",")

# Mapping: MQTT topic name → Kafka topic name
# fds → fds-data, obj → obj-data
TOPIC_MAP = {
    "fds": "fds-data",
    "obj": "obj-data",
}

# --- Initialize Kafka Producer with retry ---
producer = None


def connect_kafka():
    """
    Create a Kafka producer with retry logic.

    KafkaProducer is the client that sends messages to Kafka.
    Key settings:
      - bootstrap_servers: Kafka broker address(es) to connect to
      - value_serializer: How to convert the message value to bytes
        (we pass raw bytes, so no serialization needed — identity function)
      - key_serializer: How to convert the key to bytes (UTF-8 string)
      - acks='all': Wait for ALL replicas to confirm the write.
        Slowest but safest. For a single-node PoC, this just means
        "wait for the one broker to confirm."
    """
    global producer
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: v,  # pass raw bytes through
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
            )
            print(f"Connected to Kafka broker: {KAFKA_BROKER}")
            return
        except NoBrokersAvailable as e:
            print(f"Kafka not available: {e}. Retrying in 5s...")
            time.sleep(5)


def extract_device_id(mqtt_topic, payload):
    """
    Extract the device ID from the message payload.

    FDS messages use "board_sn" as the device ID field.
    OBJ messages use "board" as the device ID field.
    (This inconsistency exists in the real sensor data.)
    """
    try:
        data = json.loads(payload)
        if mqtt_topic == "fds":
            return data.get("board_sn", "unknown")
        elif mqtt_topic == "obj":
            return data.get("board", "unknown")
    except json.JSONDecodeError:
        pass
    return "unknown"


def on_connect(client, userdata, flags, reason_code, properties):
    """
    Called when MQTT client connects to the broker.

    We subscribe to topics HERE (inside on_connect) rather than in main()
    because if the connection drops and reconnects, on_connect is called
    again and the subscriptions are automatically restored.
    This is a best practice you'll see in all robust MQTT code.
    """
    if reason_code == 0:
        print(f"Connected to MQTT broker {MQTT_BROKER}:{MQTT_PORT}")
        for topic in MQTT_TOPICS:
            client.subscribe(topic.strip())
            print(f"Subscribed to MQTT topic: {topic.strip()}")
    else:
        print(f"MQTT connection failed, reason code: {reason_code}")


def on_message(client, userdata, msg):
    """
    Called for every MQTT message received.

    This is the core of the bridge:
    1. Get the raw payload bytes
    2. Figure out which Kafka topic to send to
    3. Extract the device ID for the Kafka key
    4. Send to Kafka
    """
    mqtt_topic = msg.topic
    payload = msg.payload  # raw bytes

    # Map MQTT topic to Kafka topic
    kafka_topic = TOPIC_MAP.get(mqtt_topic)
    if kafka_topic is None:
        print(f"No Kafka mapping for MQTT topic: {mqtt_topic}, skipping")
        return

    # Extract device ID for Kafka key (ensures ordering per device)
    device_id = extract_device_id(mqtt_topic, payload)

    # Send to Kafka
    try:
        producer.send(kafka_topic, key=device_id, value=payload)
        # producer.send() is async — it queues the message and returns
        # immediately. The message is sent in the background.
        # For higher throughput, we don't call .get() (which would block).
    except Exception as e:
        print(f"Kafka send error: {e}")


def main():
    # Connect to Kafka first (blocks until connected)
    connect_kafka()

    # Create MQTT client
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    # Connect to MQTT with retry
    while True:
        try:
            mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
            break
        except Exception as e:
            print(f"MQTT connection failed: {e}. Retrying in 5s...")
            time.sleep(5)

    print("Bridge running — forwarding MQTT messages to Kafka...")

    # loop_forever() blocks and handles reconnections automatically.
    # Unlike loop_start() (which runs in a background thread),
    # loop_forever() runs in the main thread — perfect for a script
    # whose only job is to process MQTT messages.
    try:
        mqtt_client.loop_forever()
    except KeyboardInterrupt:
        print("\nBridge stopped.")
        producer.close()
        mqtt_client.disconnect()


if __name__ == "__main__":
    main()
