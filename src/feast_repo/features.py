"""
Feast Feature Definitions for Fall Detection Pipeline.

Defines entities and feature views that the Redis Writer pushes to
and the FastAPI app reads from.

Three feature groups:
  1. FDS real-time features — activity, position, fall status per device
  2. OBJ real-time features — people count, velocity, height per device
  3. Fall history features  — fall count in last 10 min, time since last fall
"""

from datetime import timedelta

from feast import Entity, FeatureView, Field, PushSource, FileSource
from feast.types import Float64, Int64, String, Bool


# --- Entity ---
device = Entity(
    name="device_id",
    join_keys=["device_id"],
    description="Radar sensor device identifier (e.g., kc2508p025)",
)

# --- Batch sources (required by PushSource, not actually used for online serving) ---
fds_batch_source = FileSource(
    name="fds_batch_source",
    path="/app/feast_repo/data/fds_features.parquet",
    timestamp_field="event_time",
)

obj_batch_source = FileSource(
    name="obj_batch_source",
    path="/app/feast_repo/data/obj_features.parquet",
    timestamp_field="event_time",
)

fall_batch_source = FileSource(
    name="fall_batch_source",
    path="/app/feast_repo/data/fall_features.parquet",
    timestamp_field="event_time",
)

# --- Push Sources (streaming ingestion from Redis Writer) ---
fds_push_source = PushSource(
    name="fds_push_source",
    batch_source=fds_batch_source,
)

obj_push_source = PushSource(
    name="obj_push_source",
    batch_source=obj_batch_source,
)

fall_push_source = PushSource(
    name="fall_push_source",
    batch_source=fall_batch_source,
)

# --- Feature Views ---
fds_realtime_features = FeatureView(
    name="fds_realtime_features",
    entities=[device],
    ttl=timedelta(seconds=600),
    schema=[
        Field(name="activity", dtype=Int64),
        Field(name="position_x", dtype=Float64),
        Field(name="position_y", dtype=Float64),
        Field(name="is_falling", dtype=Bool),
        Field(name="result", dtype=String),
    ],
    source=fds_push_source,
    online=True,
)

obj_realtime_features = FeatureView(
    name="obj_realtime_features",
    entities=[device],
    ttl=timedelta(seconds=600),
    schema=[
        Field(name="people_count", dtype=Int64),
        Field(name="avg_velocity", dtype=Float64),
        Field(name="avg_height", dtype=Float64),
    ],
    source=obj_push_source,
    online=True,
)

fall_history_features = FeatureView(
    name="fall_history_features",
    entities=[device],
    ttl=timedelta(seconds=600),
    schema=[
        Field(name="fall_count_10min", dtype=Int64),
        Field(name="last_fall_time", dtype=String),
        Field(name="time_since_last_fall_sec", dtype=Float64),
    ],
    source=fall_push_source,
    online=True,
)
