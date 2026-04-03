"""
Fall Detection PyFlink Job

Reads sensor data from Kafka, processes it, and writes the results
back to Kafka output topics.

Pipeline:
  Kafka (fds-data) ──→ bronze-fds (raw passthrough)
                   ──→ silver-fds (parsed, cleaned)

  Kafka (obj-data) ──→ bronze-obj (raw passthrough)
                   ──→ silver-obj (flattened, 1 row per person)

Why write to Kafka instead of directly to Delta Lake?
  PyFlink depends on apache-beam which requires pyarrow < 10,
  but deltalake requires pyarrow >= 16. These are incompatible.
  So we separate concerns: Flink handles processing, and a
  separate lightweight service writes to Delta Lake.
  This is actually a common production pattern.
"""

import json
import os
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.functions import FlatMapFunction

# --- Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")


class ProcessFDS(FlatMapFunction):
    """
    Processes FDS messages and outputs to both bronze and silver topics.

    FlatMapFunction: A Flink function that takes one input and can
    produce zero, one, or many outputs. We use it here because each
    FDS message produces TWO outputs (bronze + silver).

    Why FlatMapFunction instead of MapFunction?
      - MapFunction: 1 input → exactly 1 output
      - FlatMapFunction: 1 input → 0 or more outputs
      We need to emit to multiple output topics, so FlatMap is the right choice.
    """

    def flat_map(self, raw_json):
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError:
            return

        ingestion_time = datetime.utcnow().isoformat()

        # --- Bronze output: raw data + metadata ---
        bronze = json.dumps({
            "raw_json": raw_json,
            "mqtt_topic": "fds",
            "ingestion_time": ingestion_time,
        })

        # --- Silver output: cleaned and structured ---
        # Parse position "0.80,1.40" → x, y
        position_x = None
        position_y = None
        pos_str = data.get("position", "")
        if "," in pos_str:
            parts = pos_str.split(",")
            try:
                position_x = float(parts[0])
                position_y = float(parts[1])
            except (ValueError, IndexError):
                pass

        # Parse report "3,active,notify,Falling" → components
        report_zone = None
        report_state = None
        report_alert = None
        report_str = data.get("report", "")
        report_parts = report_str.split(",")
        if len(report_parts) >= 4:
            report_zone = report_parts[0]
            report_state = report_parts[1]
            report_alert = report_parts[2]

        # Parse timestamp "2026/03/31-03:36:30" → ISO format
        event_time = None
        try:
            dt = datetime.strptime(data.get("time", ""), "%Y/%m/%d-%H:%M:%S")
            event_time = dt.isoformat()
        except (ValueError, TypeError):
            pass

        silver = json.dumps({
            "device_id": data.get("board_sn", "unknown"),
            "event_time": event_time,
            "frame_number": data.get("fn"),
            "result": data.get("result"),
            "is_falling": data.get("result") == "Falling",
            "activity": data.get("activity"),
            "position_x": position_x,
            "position_y": position_y,
            "location": data.get("location"),
            "report_zone": report_zone,
            "report_state": report_state,
            "report_alert": report_alert,
            "model_name": data.get("model_name"),
            "software_version": data.get("software_version"),
            "ingestion_time": ingestion_time,
        })

        # Yield both outputs with a tag so we can route them
        yield json.dumps({"topic": "bronze-fds", "value": bronze})
        yield json.dumps({"topic": "silver-fds", "value": silver})


class ProcessOBJ(FlatMapFunction):
    """
    Processes OBJ messages. Flattens nested objects into separate rows.

    Input (1 message with 2 people):
      {"obj": {"FDS_185": {...}, "FDS_186": {...}}}

    Output (3 messages: 1 bronze + 2 silver):
      bronze-obj: raw JSON
      silver-obj: {"object_id": "FDS_185", "center_x": 0.035, ...}
      silver-obj: {"object_id": "FDS_186", "center_x": 0.821, ...}
    """

    def flat_map(self, raw_json):
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError:
            return

        ingestion_time = datetime.utcnow().isoformat()

        # --- Bronze output ---
        bronze = json.dumps({
            "raw_json": raw_json,
            "mqtt_topic": "obj",
            "ingestion_time": ingestion_time,
        })
        yield json.dumps({"topic": "bronze-obj", "value": bronze})

        # --- Silver output: one row per tracked person ---
        event_time = None
        try:
            dt = datetime.strptime(data.get("time", ""), "%Y/%m/%d-%H:%M:%S")
            event_time = dt.isoformat()
        except (ValueError, TypeError):
            pass

        device_id = data.get("board", "unknown")
        location = data.get("location")
        frame_number = data.get("fn")

        objects = data.get("obj", {})
        for object_id, obj_data in objects.items():
            center = obj_data.get("center", [None, None])
            min_coords = obj_data.get("min_coords", [None, None, None])
            max_coords = obj_data.get("max_coords", [None, None, None])

            # Compute height from bounding box
            height = None
            if min_coords[2] is not None and max_coords[2] is not None:
                height = round(max_coords[2] - min_coords[2], 4)

            silver = json.dumps({
                "device_id": device_id,
                "event_time": event_time,
                "frame_number": frame_number,
                "location": location,
                "object_id": object_id,
                "center_x": center[0] if len(center) > 0 else None,
                "center_y": center[1] if len(center) > 1 else None,
                "min_x": min_coords[0] if len(min_coords) > 0 else None,
                "min_y": min_coords[1] if len(min_coords) > 1 else None,
                "min_z": min_coords[2] if len(min_coords) > 2 else None,
                "max_x": max_coords[0] if len(max_coords) > 0 else None,
                "max_y": max_coords[1] if len(max_coords) > 1 else None,
                "max_z": max_coords[2] if len(max_coords) > 2 else None,
                "height": height,
                "volume": obj_data.get("vol"),
                "point_count": obj_data.get("pts"),
                "velocity": obj_data.get("velocity"),
                "confidence": obj_data.get("p_value"),
                "ai_state": obj_data.get("ai_state"),
                "energy_acc": obj_data.get("energy_acc"),
                "ingestion_time": ingestion_time,
            })
            yield json.dumps({"topic": "silver-obj", "value": silver})


class RouteToTopic(FlatMapFunction):
    """
    Routes messages to the correct Kafka topic based on the "topic" field.

    Each upstream message is wrapped as:
      {"topic": "bronze-fds", "value": "...actual data..."}

    This function extracts the value and sets the target topic.
    Since Flink's KafkaSink sends to a fixed topic, we use a workaround:
    we'll create separate sinks for each output topic.
    """

    def __init__(self, target_topic):
        self.target_topic = target_topic

    def flat_map(self, tagged_json):
        try:
            msg = json.loads(tagged_json)
            if msg.get("topic") == self.target_topic:
                yield msg.get("value", "")
        except json.JSONDecodeError:
            pass


def create_kafka_sink(topic):
    """
    Create a Kafka sink for a specific output topic.

    KafkaSink is the opposite of KafkaSource — it writes messages to Kafka.
    KafkaRecordSerializationSchema defines how to serialize each message
    (we use SimpleStringSchema since our data is already JSON strings).
    """
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


def main():
    """
    Main entry point for the Flink job.

    Flow:
    1. Read from fds-data and obj-data Kafka topics
    2. Process with FlatMap functions (parse, clean, flatten)
    3. Route outputs to 4 Kafka topics (bronze-fds, silver-fds, bronze-obj, silver-obj)
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # --- Kafka Sources ---
    fds_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics("fds-data")
        .set_group_id("flink-fds-consumer")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    obj_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics("obj-data")
        .set_group_id("flink-obj-consumer")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # --- Read from Kafka ---
    fds_stream = env.from_source(
        fds_source, WatermarkStrategy.no_watermarks(), "FDS Source"
    )
    obj_stream = env.from_source(
        obj_source, WatermarkStrategy.no_watermarks(), "OBJ Source"
    )

    # --- Process FDS data ---
    fds_processed = fds_stream.flat_map(ProcessFDS(), output_type=Types.STRING())

    # --- Process OBJ data ---
    obj_processed = obj_stream.flat_map(ProcessOBJ(), output_type=Types.STRING())

    # --- Merge both processed streams ---
    all_processed = fds_processed.union(obj_processed)

    # --- Route to output Kafka topics ---
    # Each output topic gets its own sink
    output_topics = ["bronze-fds", "silver-fds", "bronze-obj", "silver-obj"]
    for topic in output_topics:
        (
            all_processed
            .flat_map(RouteToTopic(topic), output_type=Types.STRING())
            .sink_to(create_kafka_sink(topic))
        )

    print(f"Starting Fall Detection Pipeline...")
    print(f"  Kafka: {KAFKA_BROKER}")
    print(f"  Input topics: fds-data, obj-data")
    print(f"  Output topics: {', '.join(output_topics)}")

    env.execute("Fall Detection Pipeline")


if __name__ == "__main__":
    main()
