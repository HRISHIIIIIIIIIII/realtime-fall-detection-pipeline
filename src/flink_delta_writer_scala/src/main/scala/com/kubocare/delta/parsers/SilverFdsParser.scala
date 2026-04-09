package com.kubocare.delta.parsers

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.types.Row

class SilverFdsParser extends MapFunction[String, Row] {

  @transient lazy val mapper = new ObjectMapper()

  override def map(jsonStr: String): Row = {
    try {
      val n = mapper.readTree(jsonStr)
      // Row.of() accepts AnyRef (Java objects), not Scala primitives.
      // Use java.lang.Long / Double / Boolean (boxed types) for numeric fields.
      // Pass null for missing/null JSON fields — the schema allows nulls.
      Row.of(
        str(n, "device_id"),
        str(n, "event_time"),
        lng(n, "frame_number"),   // java.lang.Long
        str(n, "result"),
        bool(n, "is_falling"),    // java.lang.Boolean
        lng(n, "activity"),       // java.lang.Long
        dbl(n, "position_x"),     // java.lang.Double
        dbl(n, "position_y"),
        str(n, "location"),
        str(n, "report_zone"),
        str(n, "report_state"),
        str(n, "report_alert"),
        str(n, "model_name"),
        str(n, "software_version"),
        str(n, "ingestion_time")
      )
    } catch {
      case _: Exception =>
        // Return a null-filled row rather than crashing the whole job
        Row.withPositions(org.apache.flink.types.RowKind.INSERT, 15)
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
  private def bool(n: JsonNode, f: String): java.lang.Boolean = {
    val v = n.get(f); if (v == null || v.isNull) null
    else java.lang.Boolean.valueOf(v.asBoolean())
  }
}
