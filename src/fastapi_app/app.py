"""
FastAPI Application — REST API + Feast Feature Serving + MQTT Publisher

Endpoints:
  GET  /api/devices              — List all active devices
  GET  /api/devices/{device_id}  — Latest FDS + OBJ data from Redis
  GET  /api/falls                — All fall events in last 10 min
  GET  /api/falls/{device_id}    — Falls for a specific device
  GET  /api/features/{device_id} — Feast features for verification model
  POST /api/verify-fall          — Get features → run model → publish alert
  POST /api/mqtt/publish         — Publish any message to MQTT
  GET  /api/health               — Redis + MQTT connectivity check
"""

import os
import json
from contextlib import asynccontextmanager

import redis
import paho.mqtt.client as mqtt
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# --- Configuration ---
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
MQTT_BROKER = os.getenv("MQTT_BROKER", "34.236.215.53")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
FEAST_REPO_PATH = os.getenv("FEAST_REPO_PATH", "/app/feast_repo")

# --- Global clients ---
r = None
mqtt_client = None
feast_store = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize connections on startup, clean up on shutdown."""
    global r, mqtt_client, feast_store

    # Redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    print(f"Connected to Redis: {REDIS_HOST}:{REDIS_PORT}")

    # MQTT
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()
    print(f"Connected to MQTT: {MQTT_BROKER}:{MQTT_PORT}")

    # Feast
    try:
        from feast import FeatureStore
        feast_store = FeatureStore(repo_path=FEAST_REPO_PATH)
        print(f"Feast initialized from: {FEAST_REPO_PATH}")
    except Exception as e:
        print(f"[WARN] Feast init failed: {e}. Feature endpoints disabled.")
        feast_store = None

    yield

    # Cleanup
    mqtt_client.loop_stop()
    mqtt_client.disconnect()


app = FastAPI(
    title="Fall Detection API",
    description="Real-time sensor data, Feast features, and MQTT alerting",
    version="1.0.0",
    lifespan=lifespan,
)


# --- Request/Response Models ---

class MQTTPublishRequest(BaseModel):
    topic: str
    message: str


class VerifyFallRequest(BaseModel):
    device_id: str
    event_time: str


# --- Endpoints ---

@app.get("/api/devices")
def list_devices():
    """List all active devices (seen in last 10 minutes)."""
    devices = r.smembers("devices:active")
    return {"devices": sorted(devices), "count": len(devices)}


@app.get("/api/devices/{device_id}")
def get_device(device_id: str):
    """Get latest FDS + OBJ data for a device from Redis."""
    fds_raw = r.get(f"device:{device_id}:fds")
    obj_raw = r.get(f"device:{device_id}:obj")

    if not fds_raw and not obj_raw:
        raise HTTPException(status_code=404, detail=f"Device {device_id} not found")

    result = {"device_id": device_id}
    if fds_raw:
        result["fds"] = json.loads(fds_raw)
    if obj_raw:
        result["obj"] = json.loads(obj_raw)

    return result


@app.get("/api/falls")
def list_falls():
    """Get all fall events in the last 10 minutes."""
    keys = r.keys("fall:*")
    if not keys:
        return {"falls": [], "count": 0}

    values = r.mget(keys)
    falls = []
    for v in values:
        if v:
            falls.append(json.loads(v))

    # Sort by event_time descending
    falls.sort(key=lambda f: f.get("event_time", ""), reverse=True)
    return {"falls": falls, "count": len(falls)}


@app.get("/api/falls/{device_id}")
def get_device_falls(device_id: str):
    """Get falls for a specific device."""
    keys = r.keys(f"fall:{device_id}:*")
    if not keys:
        return {"device_id": device_id, "falls": [], "count": 0}

    values = r.mget(keys)
    falls = [json.loads(v) for v in values if v]
    falls.sort(key=lambda f: f.get("event_time", ""), reverse=True)
    return {"device_id": device_id, "falls": falls, "count": len(falls)}


@app.get("/api/features/{device_id}")
def get_features(device_id: str):
    """
    Get all Feast features for a device.
    These are the features the verification model uses.
    """
    if feast_store is None:
        raise HTTPException(status_code=503, detail="Feast not available")

    try:
        features = feast_store.get_online_features(
            features=[
                "fds_realtime_features:activity",
                "fds_realtime_features:position_x",
                "fds_realtime_features:position_y",
                "fds_realtime_features:is_falling",
                "fds_realtime_features:result",
                "obj_realtime_features:people_count",
                "obj_realtime_features:avg_velocity",
                "obj_realtime_features:avg_height",
                "fall_history_features:fall_count_10min",
                "fall_history_features:last_fall_time",
                "fall_history_features:time_since_last_fall_sec",
            ],
            entity_rows=[{"device_id": device_id}],
        ).to_dict()

        # Convert from {feature: [value]} to {feature: value}
        result = {k: v[0] if v else None for k, v in features.items()}
        return {"device_id": device_id, "features": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Feast error: {str(e)}")


@app.post("/api/verify-fall")
def verify_fall(req: VerifyFallRequest):
    """
    Fall verification pipeline:
    1. Get features from Feast for the device
    2. Run verification model (placeholder)
    3. If verified → publish alert to MQTT
    """
    # Step 1 — Get features
    features = {}
    if feast_store:
        try:
            feat_response = feast_store.get_online_features(
                features=[
                    "fds_realtime_features:activity",
                    "fds_realtime_features:position_x",
                    "fds_realtime_features:position_y",
                    "obj_realtime_features:people_count",
                    "obj_realtime_features:avg_velocity",
                    "obj_realtime_features:avg_height",
                    "fall_history_features:fall_count_10min",
                    "fall_history_features:time_since_last_fall_sec",
                ],
                entity_rows=[{"device_id": req.device_id}],
            ).to_dict()
            features = {k: v[0] if v else None for k, v in feat_response.items()}
        except Exception as e:
            print(f"[WARN] Feast query failed: {e}")

    # Step 2 — Verification model (PLACEHOLDER)
    # Replace this with your actual model inference
    # For now: verified = True if fall_count_10min >= 1
    fall_count = features.get("fall_count_10min", 0) or 0
    verified = fall_count >= 1

    # Step 3 — Publish to MQTT if verified
    if verified:
        # Get location from Redis
        fds_raw = r.get(f"device:{req.device_id}:fds")
        location = "unknown"
        if fds_raw:
            fds_data = json.loads(fds_raw)
            location = fds_data.get("location", "unknown")

        alert = json.dumps({
            "device_id": req.device_id,
            "event_time": req.event_time,
            "location": location,
            "verified": True,
            "fall_count_10min": fall_count,
            "features": features,
        })
        mqtt_client.publish("alerts/verified", alert)
        print(f"[ALERT] Verified fall: {req.device_id} at {req.event_time}")

    return {
        "device_id": req.device_id,
        "event_time": req.event_time,
        "verified": verified,
        "features": features,
    }


@app.post("/api/mqtt/publish")
def publish_mqtt(req: MQTTPublishRequest):
    """Publish a message to any MQTT topic."""
    result = mqtt_client.publish(req.topic, req.message)
    return {
        "topic": req.topic,
        "message": req.message,
        "status": "published" if result.rc == 0 else "failed",
    }


@app.get("/api/health")
def health_check():
    """Check Redis and MQTT connectivity."""
    status = {"redis": False, "mqtt": False, "feast": False}

    try:
        r.ping()
        status["redis"] = True
    except Exception:
        pass

    status["mqtt"] = mqtt_client.is_connected() if mqtt_client else False
    status["feast"] = feast_store is not None

    healthy = status["redis"] and status["mqtt"]
    return {"healthy": healthy, "services": status}
