package com.kubocare.delta.parsers

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.table.data.{GenericRowData, RowData, StringData}

class BronzeFdsParser extends MapFunction[String, RowData] {

  @transient lazy val mapper = new ObjectMapper()

  override def map(jsonStr: String): RowData = {
    try {
      val node = mapper.readTree(jsonStr)
      val row = new GenericRowData(3)
      row.setField(0, str(node, "raw_json"))
      row.setField(1, str(node, "mqtt_topic"))
      row.setField(2, str(node, "ingestion_time"))
      row
    } catch {
      case _: Exception =>
        val row = new GenericRowData(3)
        row.setField(0, StringData.fromString(jsonStr))
        row.setField(1, StringData.fromString("bronze-fds"))
        row.setField(2, StringData.fromString(java.time.Instant.now().toString))
        row
    }
  }

  private def str(node: JsonNode, field: String): StringData = {
    val v = node.get(field)
    if (v == null || v.isNull) null else StringData.fromString(v.asText())
  }
}
