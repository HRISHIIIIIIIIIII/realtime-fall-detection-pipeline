package com.kubocare.delta.parsers

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.types.Row

class SilverObjParser extends MapFunction[String, Row] {

  @transient lazy val mapper = new ObjectMapper()

  override def map(jsonStr: String): Row = {
    try {
      val n = mapper.readTree(jsonStr)
      Row.of(
        str(n, "device_id"),
        str(n, "event_time"),
        lng(n, "frame_number"),
        str(n, "location"),
        str(n, "object_id"),      // stored as String e.g. "FDS_100"
        dbl(n, "center_x"),
        dbl(n, "center_y"),
        dbl(n, "min_x"),
        dbl(n, "min_y"),
        dbl(n, "min_z"),
        dbl(n, "max_x"),
        dbl(n, "max_y"),
        dbl(n, "max_z"),
        dbl(n, "height"),
        dbl(n, "volume"),
        lng(n, "point_count"),
        dbl(n, "velocity"),
        dbl(n, "confidence"),
        str(n, "ai_state"),
        dbl(n, "energy_acc"),
        str(n, "ingestion_time")
      )
    } catch {
      case _: Exception =>
        Row.withPositions(org.apache.flink.types.RowKind.INSERT, 21)
    }
  }

  private def str(n: JsonNode, f: String): String = {
    val v = n.get(f); if (v == null || v.isNull) null else v.asText()
  }
  private def lng(n: JsonNode, f: String): java.lang.Long = {
    val v = n.get(f); if (v == null || v.isNull) null
    else java.lang.Long.valueOf(v.asLong())
  }
  private def dbl(n: JsonNode, f: String): java.lang.Double = {
    val v = n.get(f); if (v == null || v.isNull) null
    else java.lang.Double.valueOf(v.asDouble())
  }
}
