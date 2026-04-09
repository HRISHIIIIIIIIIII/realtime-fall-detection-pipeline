package com.kubocare.delta.schemas

import org.apache.flink.table.types.logical._

object ObjSchemas {

  // bronze_obj: raw JSON passthrough — same 3 columns as bronze_fds
  val bronzeObjRowType: RowType = RowType.of(
    Array[LogicalType](
      new VarCharType(VarCharType.MAX_LENGTH), // raw_json
      new VarCharType(VarCharType.MAX_LENGTH), // mqtt_topic
      new VarCharType(VarCharType.MAX_LENGTH)  // ingestion_time
    ),
    Array("raw_json", "mqtt_topic", "ingestion_time")
  )

  // silver_obj: flattened OBJ tracking data — 21 columns
  // IMPORTANT: object_id is STRING not INT — the actual data contains
  // string identifiers like "FDS_100", not plain integers
  val silverObjRowType: RowType = RowType.of(
    Array[LogicalType](
      new VarCharType(VarCharType.MAX_LENGTH), // device_id
      new VarCharType(VarCharType.MAX_LENGTH), // event_time
      new BigIntType(),                         // frame_number
      new VarCharType(VarCharType.MAX_LENGTH), // location
      new VarCharType(VarCharType.MAX_LENGTH), // object_id (e.g. "FDS_100")
      new DoubleType(),                         // center_x
      new DoubleType(),                         // center_y
      new DoubleType(),                         // min_x
      new DoubleType(),                         // min_y
      new DoubleType(),                         // min_z
      new DoubleType(),                         // max_x
      new DoubleType(),                         // max_y
      new DoubleType(),                         // max_z
      new DoubleType(),                         // height
      new DoubleType(),                         // volume
      new BigIntType(),                         // point_count
      new DoubleType(),                         // velocity
      new DoubleType(),                         // confidence
      new VarCharType(VarCharType.MAX_LENGTH), // ai_state
      new DoubleType(),                         // energy_acc
      new VarCharType(VarCharType.MAX_LENGTH)  // ingestion_time
    ),
    Array(
      "device_id", "event_time", "frame_number", "location", "object_id",
      "center_x", "center_y", "min_x", "min_y", "min_z",
      "max_x", "max_y", "max_z", "height", "volume",
      "point_count", "velocity", "confidence", "ai_state", "energy_acc",
      "ingestion_time"
    )
  )
}
