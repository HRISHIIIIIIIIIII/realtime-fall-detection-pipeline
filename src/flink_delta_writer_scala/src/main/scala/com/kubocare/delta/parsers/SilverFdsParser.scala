package com.kubocare.delta.parsers

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.table.data.{GenericRowData, RowData, StringData}

class SilverFdsParser extends MapFunction[String, RowData] {

  @transient lazy val mapper = new ObjectMapper()

  override def map(jsonStr: String): RowData = {
    try {
      val n = mapper.readTree(jsonStr)
      val row = new GenericRowData(15)
      row.setField(0,  str(n, "device_id"))
      row.setField(1,  str(n, "event_time"))
      row.setField(2,  lng(n, "frame_number"))
      row.setField(3,  str(n, "result"))
      row.setField(4,  bool(n, "is_falling"))
      row.setField(5,  lng(n, "activity"))
      row.setField(6,  dbl(n, "position_x"))
      row.setField(7,  dbl(n, "position_y"))
      row.setField(8,  str(n, "location"))
      row.setField(9,  str(n, "report_zone"))
      row.setField(10, str(n, "report_state"))
      row.setField(11, str(n, "report_alert"))
      row.setField(12, str(n, "model_name"))
      row.setField(13, str(n, "software_version"))
      row.setField(14, str(n, "ingestion_time"))
      row
    } catch {
      case _: Exception =>
        new GenericRowData(15)
    }
  }

  private def str(n: JsonNode, f: String): StringData = {
    val v = n.get(f); if (v == null || v.isNull) null else StringData.fromString(v.asText())
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
