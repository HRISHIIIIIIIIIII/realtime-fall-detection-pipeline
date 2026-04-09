package com.kubocare.delta.parsers

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.types.Row

// MapFunction[String, Row]: takes one Kafka JSON string, returns one Row
class BronzeFdsParser extends MapFunction[String, Row] {

  // @transient: don't serialize ObjectMapper when Flink ships this operator to
  // the TaskManager. lazy val: recreate it on first use on the worker.
  @transient lazy val mapper = new ObjectMapper()

  override def map(jsonStr: String): Row = {
    try {
      val node = mapper.readTree(jsonStr)
      // Column order must match bronzeFdsRowType in FdsSchemas.scala
      Row.of(
        str(node, "raw_json"),
        str(node, "mqtt_topic"),
        str(node, "ingestion_time")
      )
    } catch {
      // Don't crash the job on a bad message — return the raw string as raw_json
      case _: Exception =>
        Row.of(jsonStr, "bronze-fds", java.time.Instant.now().toString)
    }
  }

  private def str(node: JsonNode, field: String): String = {
    val v = node.get(field)
    if (v == null || v.isNull) null else v.asText()
  }
}
