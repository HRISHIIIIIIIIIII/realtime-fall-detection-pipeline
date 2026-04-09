package com.kubocare.delta.parsers

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.types.Row

class BronzeObjParser extends MapFunction[String, Row] {

  @transient lazy val mapper = new ObjectMapper()

  override def map(jsonStr: String): Row = {
    try {
      val node = mapper.readTree(jsonStr)
      Row.of(
        str(node, "raw_json"),
        str(node, "mqtt_topic"),
        str(node, "ingestion_time")
      )
    } catch {
      case _: Exception =>
        Row.of(jsonStr, "bronze-obj", java.time.Instant.now().toString)
    }
  }

  private def str(node: JsonNode, field: String): String = {
    val v = node.get(field)
    if (v == null || v.isNull) null else v.asText()
  }
}
