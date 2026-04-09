package com.kubocare.delta.parsers

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.table.data.{GenericRowData, RowData, StringData}

class SilverObjParser extends MapFunction[String, RowData] {

  @transient lazy val mapper = new ObjectMapper()

  override def map(jsonStr: String): RowData = {
    try {
      val n = mapper.readTree(jsonStr)
      val row = new GenericRowData(21)
      row.setField(0,  str(n, "device_id"))
      row.setField(1,  str(n, "event_time"))
      row.setField(2,  lng(n, "frame_number"))
      row.setField(3,  str(n, "location"))
      row.setField(4,  str(n, "object_id"))
      row.setField(5,  dbl(n, "center_x"))
      row.setField(6,  dbl(n, "center_y"))
      row.setField(7,  dbl(n, "min_x"))
      row.setField(8,  dbl(n, "min_y"))
      row.setField(9,  dbl(n, "min_z"))
      row.setField(10, dbl(n, "max_x"))
      row.setField(11, dbl(n, "max_y"))
      row.setField(12, dbl(n, "max_z"))
      row.setField(13, dbl(n, "height"))
      row.setField(14, dbl(n, "volume"))
      row.setField(15, lng(n, "point_count"))
      row.setField(16, dbl(n, "velocity"))
      row.setField(17, dbl(n, "confidence"))
      row.setField(18, str(n, "ai_state"))
      row.setField(19, dbl(n, "energy_acc"))
      row.setField(20, str(n, "ingestion_time"))
      row
    } catch {
      case _: Exception =>
        new GenericRowData(21)
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
}
