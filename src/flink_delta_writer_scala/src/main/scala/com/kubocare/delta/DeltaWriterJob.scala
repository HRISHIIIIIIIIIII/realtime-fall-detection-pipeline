package com.kubocare.delta

import com.kubocare.delta.parsers._
import com.kubocare.delta.schemas.FdsSchemas._
import com.kubocare.delta.schemas.ObjSchemas._

import io.delta.flink.sink.DeltaSink
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.Row
import org.apache.hadoop.conf.Configuration

object DeltaWriterJob {

  val KAFKA_BROKER: String   = sys.env.getOrElse("KAFKA_BROKER",   "kafka:9092")
  val DELTA_BASE:   String   = sys.env.getOrElse("DELTA_BASE_PATH", "/opt/flink/delta")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    // ── Checkpointing ──────────────────────────────────────────────────────────
    // DeltaSink REQUIRES checkpointing. It buffers data in memory and only
    // writes a Parquet file + Delta log entry when a checkpoint completes.
    // Without this, nothing will ever appear in the Delta tables.
    //
    // Interval: 30 seconds. This is how often data appears in Delta Lake.
    // EXACTLY_ONCE: Flink saves Kafka offsets in the checkpoint. If the job
    // restarts after a crash, it replays from the saved offset — no data lost,
    // no duplicates.
    // ──────────────────────────────────────────────────────────────────────────
    env.enableCheckpointing(30_000L, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage(
      "file:///opt/flink/checkpoints/delta-writer"
    )
    env.setParallelism(1)

    // ── Hadoop configuration ───────────────────────────────────────────────────
    // Delta Lake uses Hadoop's FileSystem API even for local disk.
    // Empty Configuration defaults to LocalFileSystem for paths without a scheme.
    // ──────────────────────────────────────────────────────────────────────────
    val hadoopConf = new Configuration()

    // ── Kafka sources ──────────────────────────────────────────────────────────
    // One source per topic. Group IDs are distinct from the PyFlink job's IDs
    // so both jobs track their Kafka offsets independently.
    //
    // OffsetsInitializer.latest(): start from newest message on first run.
    // After a checkpoint, Flink uses the saved offset instead of "latest".
    // ──────────────────────────────────────────────────────────────────────────
    def kafkaSource(topic: String, groupId: String): KafkaSource[String] =
      KafkaSource.builder[String]()
        .setBootstrapServers(KAFKA_BROKER)
        .setTopics(topic)
        .setGroupId(groupId)
        .setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build()

    val srcBronzeFds = kafkaSource("bronze-fds", "scala-delta-bronze-fds")
    val srcSilverFds = kafkaSource("silver-fds", "scala-delta-silver-fds")
    val srcBronzeObj = kafkaSource("bronze-obj", "scala-delta-bronze-obj")
    val srcSilverObj = kafkaSource("silver-obj", "scala-delta-silver-obj")

    // ── Read streams from Kafka ────────────────────────────────────────────────
    val stmBronzeFds = env.fromSource(srcBronzeFds, WatermarkStrategy.noWatermarks(), "bronze-fds")
    val stmSilverFds = env.fromSource(srcSilverFds, WatermarkStrategy.noWatermarks(), "silver-fds")
    val stmBronzeObj = env.fromSource(srcBronzeObj, WatermarkStrategy.noWatermarks(), "bronze-obj")
    val stmSilverObj = env.fromSource(srcSilverObj, WatermarkStrategy.noWatermarks(), "silver-obj")

    // ── Parse JSON → Flink Row ─────────────────────────────────────────────────
    val rowBronzeFds = stmBronzeFds.map(new BronzeFdsParser())
    val rowSilverFds = stmSilverFds.map(new SilverFdsParser())
    val rowBronzeObj = stmBronzeObj.map(new BronzeObjParser())
    val rowSilverObj = stmSilverObj.map(new SilverObjParser())

    // ── Delta sinks ────────────────────────────────────────────────────────────
    // DeltaSink.forRowData(path, hadoopConf, rowType):
    //   path      — filesystem path to the Delta table directory
    //   hadoopConf — LocalFileSystem for us (no HDFS/S3)
    //   rowType   — column names + types; must match existing Delta table schema
    //
    // On first write: creates the table and _delta_log/
    // On subsequent writes: appends new Parquet files + adds a log entry
    // ──────────────────────────────────────────────────────────────────────────
    def deltaSink(table: String, rowType: RowType): DeltaSink[Row] =
      DeltaSink
        .forRowData(new Path(s"$DELTA_BASE/$table"), hadoopConf, rowType)
        .build()

    rowBronzeFds.sinkTo(deltaSink("bronze_fds", bronzeFdsRowType))
    rowSilverFds.sinkTo(deltaSink("silver_fds", silverFdsRowType))
    rowBronzeObj.sinkTo(deltaSink("bronze_obj", bronzeObjRowType))
    rowSilverObj.sinkTo(deltaSink("silver_obj", silverObjRowType))

    println(s"[DeltaWriterJob] Starting Scala Delta Lake Writer")
    println(s"[DeltaWriterJob]   Kafka broker : $KAFKA_BROKER")
    println(s"[DeltaWriterJob]   Delta path   : $DELTA_BASE")
    println(s"[DeltaWriterJob]   Checkpoints  : every 30 seconds")

    env.execute("Scala Delta Lake Writer")
  }
}
