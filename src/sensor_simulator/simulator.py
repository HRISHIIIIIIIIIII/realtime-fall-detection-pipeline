"""
Sensor Simulator — Generates fake FDS and OBJ data for testing.

This script mimics what the real radar sensors produce:
  - FDS messages: Fall detection events (published to topic "fds")
  - OBJ messages: Object/person tracking (published to topic "obj")

It publishes to the local Mosquitto broker so we can test the full
pipeline without needing real radar hardware.
"""

import paho.mqtt.client as mqtt
import json
import time
import random
import os
import math

# --- Configuration ---
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
DEVICE_ID = os.getenv("DEVICE_ID", "kc_sim_001")
LOCATION = os.getenv("LOCATION", "room1")
PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL", "0.5"))
# How often a fall occurs (0.02 = ~2% of FDS messages)
FALL_PROBABILITY = float(os.getenv("FALL_PROBABILITY", "0.02"))

# --- MQTT Topics ---
FDS_TOPIC = "fds"
OBJ_TOPIC = "obj"

# --- Frame counter (simulates the sensor's sequential frame number) ---
fds_frame = 0
obj_frame = 0


def generate_fds_message(is_falling):
    """
    Generate a fake FDS (Fall Detection System) message.

    Matches the real sensor format:
    {
        "fn": 1411,
        "live": 30,
        "time": "2026/03/31-03:36:30",
        "type": "FDS",
        "report": "3,active,notify,Falling",
        "result": "Falling",
        ...
    }
    """
    global fds_frame
    fds_frame += 1

    result = "Falling" if is_falling else "None"
    activity = random.randint(80, 100) if is_falling else random.randint(0, 60)
    alert_level = "notify" if is_falling else "idle"
    state = "active" if activity > 30 else "inactive"

    # Simulate person position in room (x: 0-3m, y: 0-4m)
    pos_x = round(random.uniform(0.5, 2.5), 2)
    pos_y = round(random.uniform(0.5, 3.5), 2)

    return {
        "fn": fds_frame,
        "live": fds_frame % 60,
        "time": time.strftime("%Y/%m/%d-%H:%M:%S"),
        "type": "FDS",
        "report": f"3,{state},{alert_level},{result}",
        "result": result,
        "activity": activity,
        "board_sn": DEVICE_ID,
        "location": LOCATION,
        "platform": "Simulator, Python",
        "position": f"{pos_x},{pos_y}",
        "model_name": "ModelV01",
        "model_index": "0",
        "software_version": "SIM_V1.0"
    }


def generate_obj_message(num_people):
    """
    Generate a fake OBJ (Object Tracking) message.

    The real sensor tracks multiple people simultaneously.
    Each person gets an entry like "FDS_185" with 3D bounding box,
    center position, velocity, etc.

    Matches the real format:
    {
        "fn": 75483,
        "time": "2026/04/02-04:09:10",
        "board": "kc2508p020",
        "location": "room1",
        "obj": {
            "FDS_185": { ... },
            "FDS_186": { ... }
        }
    }
    """
    global obj_frame
    obj_frame += 1

    objects = {}
    for i in range(num_people):
        obj_id = f"FDS_{100 + i}"

        # Simulate person position (center of room)
        cx = round(random.uniform(0.0, 3.0), 3)
        cy = round(random.uniform(0.5, 3.5), 3)

        # Bounding box around the person
        # Width ~0.1m, depth ~0.3m, height ~0.3-1.5m
        half_w = round(random.uniform(0.03, 0.08), 4)
        half_d = round(random.uniform(0.1, 0.2), 4)
        height = round(random.uniform(0.3, 1.5), 4)
        base_z = round(random.uniform(0.8, 1.0), 4)

        objects[obj_id] = {
            "max_coords": [
                round(cx + half_w, 4),
                round(cy + half_d, 4),
                round(base_z + height, 4)
            ],
            "min_coords": [
                round(cx - half_w, 4),
                round(cy - half_d, 4),
                round(base_z, 4)
            ],
            "center": [round(cx, 3), round(cy, 3)],
            "vol": round(2 * half_w * 2 * half_d * height, 4),
            "pts": random.randint(10, 50),
            "velocity": round(random.uniform(-0.5, 0.5), 3),
            "p_value": round(random.uniform(5.0, 10.0), 3),
            "ai_state": random.choice(["active", "idle", "notify_tmp"]),
            "energy_acc": round(random.uniform(0.0, 1.0), 3)
        }

    return {
        "fn": obj_frame,
        "time": time.strftime("%Y/%m/%d-%H:%M:%S"),
        "board": DEVICE_ID,
        "location": LOCATION,
        "obj": objects
    }


def on_connect(client, userdata, flags, reason_code, properties):
    """Called when the MQTT client connects to the broker."""
    if reason_code == 0:
        print(f"Connected to MQTT broker {MQTT_BROKER}:{MQTT_PORT}")
    else:
        print(f"Failed to connect, reason code: {reason_code}")


def main():
    # --- Create MQTT client ---
    # mqtt.CallbackAPIVersion.VERSION2 is required for paho-mqtt 2.x
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect

    # --- Connect with retry ---
    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            break
        except Exception as e:
            print(f"Connection failed: {e}. Retrying in 5s...")
            time.sleep(5)

    # Start the MQTT network loop in a background thread
    # This handles keepalive pings and reconnections automatically
    client.loop_start()

    print(f"Simulator started — Device: {DEVICE_ID}, Location: {LOCATION}")
    print(f"Publishing every {PUBLISH_INTERVAL}s, fall probability: {FALL_PROBABILITY}")
    print(f"FDS topic: {FDS_TOPIC}, OBJ topic: {OBJ_TOPIC}")

    try:
        while True:
            # Decide if this frame is a fall event
            is_falling = random.random() < FALL_PROBABILITY

            # Number of people in the room (1-3)
            num_people = random.randint(1, 3)

            # Generate and publish FDS message
            fds_msg = generate_fds_message(is_falling)
            client.publish(FDS_TOPIC, json.dumps(fds_msg))

            # Generate and publish OBJ message
            obj_msg = generate_obj_message(num_people)
            client.publish(OBJ_TOPIC, json.dumps(obj_msg))

            if is_falling:
                print(f"[FALL] Frame {fds_msg['fn']} — Falling detected!")
            elif fds_frame % 50 == 0:
                # Print a status update every 50 frames so you know it's working
                print(f"[OK] Frame {fds_msg['fn']} — {num_people} people tracked")

            time.sleep(PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        print("\nSimulator stopped.")
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
