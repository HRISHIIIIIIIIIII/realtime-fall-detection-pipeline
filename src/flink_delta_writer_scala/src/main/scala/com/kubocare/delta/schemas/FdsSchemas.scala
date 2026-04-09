package com.kubocare.delta.schemas

import org.apache.flink.table.types.logical._

object FdsSchemas {

  // bronze_fds: raw JSON passthrough — 3 string columns
  // Matches existing Delta table created by the Python writer
  val bronzeFdsRowType: RowType = RowType.of(
    Array[LogicalType](
      new VarCharType(VarCharType.MAX_LENGTH), // raw_json
      new VarCharType(VarCharType.MAX_LENGTH), // mqtt_topic
      new VarCharType(VarCharType.MAX_LENGTH)  // ingestion_time
    ),
    Array("raw_json", "mqtt_topic", "ingestion_time")
  )

  // silver_fds: parsed/cleaned FDS data — 15 columns
  // frame_number and activity are BIGINT (long) because pandas int64
  // maps to Delta "long" when written by the Python writer
  val silverFdsRowType: RowType = RowType.of(
    Array[LogicalType](
      new VarCharType(VarCharType.MAX_LENGTH), // device_id
      new VarCharType(VarCharType.MAX_LENGTH), // event_time
      new BigIntType(),                         // frame_number
      new VarCharType(VarCharType.MAX_LENGTH), // result
      new BooleanType(),                        // is_falling
      new BigIntType(),                         // activity
      new DoubleType(),                         // position_x
      new DoubleType(),                         // position_y
      new VarCharType(VarCharType.MAX_LENGTH), // location
      new VarCharType(VarCharType.MAX_LENGTH), // report_zone
      new VarCharType(VarCharType.MAX_LENGTH), // report_state
      new VarCharType(VarCharType.MAX_LENGTH), // report_alert
      new VarCharType(VarCharType.MAX_LENGTH), // model_name
      new VarCharType(VarCharType.MAX_LENGTH), // software_version
      new VarCharType(VarCharType.MAX_LENGTH)  // ingestion_time
    ),
    Array(
      "device_id", "event_time", "frame_number", "result", "is_falling",
      "activity", "position_x", "position_y", "location", "report_zone",
      "report_state", "report_alert", "model_name", "software_version",
      "ingestion_time"
    )
  )
}
