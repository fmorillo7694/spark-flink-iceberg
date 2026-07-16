package com.benchmark

import java.util.concurrent.{Executors, Semaphore}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

/**
 * Spark Structured Streaming → Iceberg ingest benchmark.
 *
 * Kafka (JSON) → parse → Iceberg table via the DataSourceV2 streaming writer.
 * Micro-batch with a tight processingTime trigger; the trigger interval and the
 * Iceberg commit boundary coincide, so — as with Flink checkpoints — the commit
 * interval is the end-to-end latency floor for the table sink.
 *
 * Write distribution mode is a *table property* on the Iceberg side; Spark
 * additionally honours `write.distribution-mode` when planning the write, and
 * `fanout-enabled` controls whether partitions are written without a pre-sort.
 *
 * Args: --distribution-mode none|hash|range  --upsert true|false  --trigger 30
 */
object IcebergIngestJob {

  private val schema = StructType(Seq(
    StructField("event_id", StringType),
    StructField("user_id", LongType),
    StructField("event_type", StringType),
    StructField("product_id", LongType),
    StructField("country", StringType),
    StructField("amount", DoubleType),
    StructField("currency", StringType),
    StructField("quantity", IntegerType),
    StructField("event_ts", LongType),
    StructField("ingest_ts", LongType),
    StructField("payload", StringType)
  ))

  def main(args: Array[String]): Unit = {
    val a = args.sliding(2, 2).collect { case Array(k, v) => k.stripPrefix("--") -> v }.toMap
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "kafka:9092")
    val topic = sys.env.getOrElse("SOURCE_TOPIC", "events")
    val db = sys.env.getOrElse("ICEBERG_DB", "streaming")
    val table = sys.env.getOrElse("ICEBERG_TABLE", "events") + "_spark"
    val mode = a.getOrElse("distribution-mode", sys.env.getOrElse("WRITE_DISTRIBUTION_MODE", "hash"))
    val upsert = a.getOrElse("upsert", "false").toBoolean
    val triggerSec = a.getOrElse("trigger", sys.env.getOrElse("CHECKPOINT_INTERVAL_MS", "30000").toInt / 1000 max 1).toString
    // partitioning: "bucket" = bucket(N,user_id) | "time" = hours(event_time)
    val partitioning = a.getOrElse("partitioning", sys.env.getOrElse("PARTITIONING", "bucket"))
    // bucket count = concurrent write streams (see Flink job note). Default 48 to
    // match Flink so the comparison holds the write-parallelism knob equal.
    val buckets = a.getOrElse("buckets", sys.env.getOrElse("BUCKETS", "48"))
    val formatVersion = a.getOrElse("format-version", sys.env.getOrElse("TABLE_FORMAT_VERSION", "2"))
    // Upsert engine mode: "merge-on-read" (deletion vectors / delete files, cheap
    // writes + read-time merge) vs "copy-on-write" (rewrite touched data files on
    // each MERGE, heavier writes + clean reads). Set on the delete/update/merge
    // table properties so the MERGE in foreachBatch honours it. Lets us compare BOTH
    // Spark patterns against Flink's DV upsert. Ignored for append.
    val mergeMode = a.getOrElse("merge-mode", sys.env.getOrElse("WRITE_MERGE_MODE", "merge-on-read"))
    // "time" partitions on epoch-minute (identity) so a 5-min run spreads across
    // ~5 partitions (Iceberg has no minute() transform); "bucket" = cardinality.
    val partitionClause = if (partitioning == "time") "event_minute" else s"bucket($buckets, user_id)"

    // Catalog config source depends on environment:
    //  * EKS (Spark operator): ALL spark.sql.catalog.demo.* come from the manifest
    //    sparkConf (baked into spark-defaults.conf). We must NOT re-set them here —
    //    the operator pre-creates the SparkSession, so builder .config() calls are
    //    DROPPED by getOrCreate(), and Iceberg's GlueCatalog then initializes with
    //    empty props → wrong S3 region → 301. Detected via CONFIG_FROM_MANIFEST=1.
    //  * Local: we set them programmatically (REST catalog on MinIO).
    val catalogType = sys.env.getOrElse("CATALOG_TYPE", "rest")
    val warehouse = s"s3://${sys.env.getOrElse("WAREHOUSE_BUCKET", "warehouse")}/"
    val configFromManifest = sys.env.getOrElse("CONFIG_FROM_MANIFEST", "0") == "1"
    val checkpointBase = sys.env.getOrElse("CHECKPOINT_BASE", "file:///tmp/spark-ckpt")

    val builder = SparkSession.builder()
      .appName(s"spark-structured-streaming-iceberg[$mode${if (upsert) ",upsert" else ""}]")
    if (!configFromManifest) {
      // Local path: set catalog config in code (manifest/spark-defaults not used).
      builder
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.demo.type", catalogType)
        .config("spark.sql.catalog.demo.warehouse", warehouse)
        .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
      if (catalogType == "rest") {
        builder
          .config("spark.sql.catalog.demo.uri", sys.env.getOrElse("CATALOG_URI", "http://iceberg-rest:8181"))
          .config("spark.sql.catalog.demo.s3.endpoint", sys.env.getOrElse("S3_ENDPOINT", "http://minio:9000"))
          .config("spark.sql.catalog.demo.s3.path-style-access", "true")
      }
    }
    val spark = builder.getOrCreate()
    import spark.implicits._

    // ── Storage-Partitioned Join (SPJ) for the upsert MERGE ──────────────────
    // The target table is bucket(N, user_id) and the MERGE key is user_id, so the
    // target is ALREADY laid out by the join key. With SPJ on, Spark can co-partition
    // the source micro-batch to the target's bucketing and skip the full shuffle-to-
    // target on every batch. These are session-level SQL configs (safe to set in
    // code even under CONFIG_FROM_MANIFEST). Toggle with SPJ=1 to measure the delta.
    //   NOTE: our internal "autospj" auto-invokes this at the engine layer (source
    //   auto-partitioned to the target spec). Here we enable the OSS-available knobs
    //   + explicitly pre-cluster the source, which is the reproducible equivalent.
    if (sys.env.getOrElse("SPJ", "0") == "1") {
      spark.conf.set("spark.sql.sources.v2.bucketing.enabled", "true")
      spark.conf.set("spark.sql.iceberg.planning.preserve-data-grouping", "true")
      spark.conf.set("spark.sql.sources.v2.bucketing.pushPartValues.enabled", "true")
      spark.conf.set("spark.sql.requireAllClusterKeysForCoPartition", "false")
      spark.conf.set("spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled", "true")
    }
    val spjEnabled = sys.env.getOrElse("SPJ", "0") == "1"

    val fqTable = s"demo.$db.$table"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS demo.$db")
    // Clean per-run state: drop+recreate the table so each run measures only its
    // own output (RECREATE_TABLE=1). PURGE removes the data files too, so the
    // engine self-cleans without any gated AWS CLI call.
    if (sys.env.getOrElse("RECREATE_TABLE", "0") == "1") {
      // Plain DROP (no PURGE): PURGE triggers a DataSourceV2Relation code path that
      // NoSuchMethodErrors on this iceberg-4.0 / Spark 4.1.2 combo. Metadata-only
      // drop is enough — per-run measurement reads the current snapshot, so any
      // orphaned data files from a prior run aren't counted.
      spark.sql(s"DROP TABLE IF EXISTS $fqTable")
    }
    // For upsert, set delete/update/merge modes to the chosen engine (MoR vs CoW).
    // MoR needs format-version >= 2 (v3 for deletion vectors); the run passes FMT=3.
    val mergeProps = if (upsert)
      s""",
         |  'write.delete.mode'='$mergeMode',
         |  'write.update.mode'='$mergeMode',
         |  'write.merge.mode'='$mergeMode'""".stripMargin
    else ""
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $fqTable (
         |  event_id string, user_id bigint, event_type string, product_id bigint,
         |  country string, amount double, currency string, quantity int,
         |  event_ts bigint, ingest_ts bigint, payload string,
         |  event_time timestamp, event_minute bigint)
         |USING iceberg
         |PARTITIONED BY ($partitionClause)
         |TBLPROPERTIES (
         |  'format-version'='$formatVersion',
         |  'write.distribution-mode'='$mode'$mergeProps
         |)""".stripMargin)

    // maxOffsetsPerTrigger is UNSET by default so Spark drains as fast as it can
    // each micro-batch — matching Flink's uncapped continuous read for a fair
    // overload comparison. Set MAX_OFFSETS_PER_TRIGGER only to deliberately throttle.
    val maxOffsets = sys.env.getOrElse("MAX_OFFSETS_PER_TRIGGER", "")
    val rawReader = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", a.getOrElse("starting-offsets", sys.env.getOrElse("STARTING_OFFSETS", "latest")))
      // aggressive Kafka retention can purge offsets a stale checkpoint expects;
      // don't fail the query on that.
      .option("failOnDataLoss", "false")
    val raw = (if (maxOffsets.nonEmpty) rawReader.option("maxOffsetsPerTrigger", maxOffsets) else rawReader)
      .load()

    val parsed: DataFrame = raw
      .select(from_json(col("value").cast("string"), schema).as("e"))
      .select("e.*")
      // derive time columns: a real timestamp + an epoch-minute bucket for
      // minute-granular identity partitioning.
      .withColumn("event_time", (col("event_ts") / 1000).cast("timestamp"))
      .withColumn("event_minute", (col("event_ts") / 60000).cast("long"))

    val query = if (upsert) {
      // Upsert path: MERGE INTO on each micro-batch keyed by user_id — the same
      // equality key Flink's .upsert() uses, so the two are comparable. Iceberg
      // MERGE errors if >1 source row matches one target row, so we dedup the
      // batch to the latest event per user_id (highest ingest_ts) first.
      // In-submit compaction modes — modeled on AWS's iceberg-streaming-examples
      // (SparkCustomIcebergIngest: compaction=none|inline|scheduled). Spark has NO
      // in-query maintenance operator like Flink, so the options are:
      //   none      — no in-submit compaction (baseline; use a separate scheduled job,
      //               which AWS calls the RECOMMENDED production baseline — our compact_spark.sh)
      //   inline    — run compaction inside foreachBatch every N batches. Simple, but
      //               SYNCHRONOUS → lengthens those batches / stalls ingest.
      //   scheduled — background ScheduledExecutorService fires compaction on a timer,
      //               concurrent with ingest on the SAME submit's executors (still
      //               competes with the writer, but doesn't block the batch).
      // Every maintenance call is guarded (a failed compaction must NEVER kill ingest).
      val compactMode = sys.env.getOrElse("SPARK_COMPACTION", "none")   // none|inline|scheduled
      val maintEvery = sys.env.getOrElse("SPARK_INLINE_EVERY", "10").toInt
      val maintExpire = sys.env.getOrElse("SPARK_MAINT_EXPIRE", "1") == "1"
      val maintGate = new Semaphore(1)  // at most one compaction in flight (drop overlaps)
      def runMaintenance(s: SparkSession, why: String): Unit = {
        if (!maintGate.tryAcquire()) return  // previous compaction still running — skip
        try {
          s.sql(s"CALL demo.system.rewrite_data_files(table => '$db.$table', options => map('min-input-files','2'))")
          s.sql(s"CALL demo.system.rewrite_position_delete_files(table => '$db.$table')")
          if (maintExpire) s.sql(s"CALL demo.system.expire_snapshots(table => '$db.$table', retain_last => 5)")
        } catch {
          case e: Throwable => System.err.println(s"[maint:$why] failed (ingest continues): ${e.getMessage}")
        } finally maintGate.release()
      }
      // scheduled mode: background timer thread on the driver, concurrent with ingest.
      val scheduler: java.util.concurrent.ScheduledExecutorService =
        if (compactMode == "scheduled") {
          val sc = Executors.newSingleThreadScheduledExecutor((r: Runnable) => {
            val th = new Thread(r, "sfi-compaction"); th.setDaemon(true); th
          })
          val everySec = sys.env.getOrElse("SPARK_SCHEDULED_EVERY_SEC", "120").toLong
          sc.scheduleAtFixedRate(() => runMaintenance(spark, "scheduled"), everySec, everySec,
            java.util.concurrent.TimeUnit.SECONDS)
          sc
        } else null
      parsed.writeStream
        .foreachBatch { (batch: DataFrame, batchId: Long) =>
          val deduped = batch
            .withColumn("_rn", row_number().over(
              Window.partitionBy("user_id").orderBy(col("ingest_ts").desc)))
            .filter(col("_rn") === 1).drop("_rn")
          // SPJ: cluster the source by the join key so the planner can co-partition
          // it with the target's bucket(N, user_id) layout and skip the shuffle-to-
          // target inside MERGE. We repartition by user_id into `buckets` partitions
          // (matching the target bucket count); with preserve-data-grouping +
          // v2.bucketing on, Spark's KeyGroupedPartitioning aligns this with the
          // target's bucket transform. (NOTE: `bucket()` is an Iceberg DDL transform,
          // not a DataFrame function — hash-repartitioning by the key is the correct
          // OSS way to feed SPJ here.) Without SPJ this branch is skipped.
          val src =
            if (spjEnabled && partitioning != "time")
              deduped.repartition(buckets.toInt, col("user_id"))
            else deduped
          src.createOrReplaceTempView("updates")
          batch.sparkSession.sql(
            s"""MERGE INTO $fqTable t
               |USING (SELECT * FROM updates) s
               |ON t.user_id = s.user_id
               |WHEN MATCHED THEN UPDATE SET *
               |WHEN NOT MATCHED THEN INSERT *""".stripMargin)

          // inline mode: compact synchronously inside the batch every N batches
          // (blocks the next batch — the AWS "simple but lengthens batches" pattern).
          if (compactMode == "inline" && batchId > 0 && batchId % maintEvery == 0) {
            runMaintenance(batch.sparkSession, s"inline@batch$batchId")
          }
          () // foreachBatch requires a Unit-returning closure
        }
        .option("checkpointLocation", s"${checkpointBase}/$table")
        .trigger(Trigger.ProcessingTime(s"$triggerSec seconds"))
        .start()
    } else {
      // Append path: native DataSourceV2 streaming writer.
      parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", s"${checkpointBase}/$table")
        .option("fanout-enabled", "true")
        .trigger(Trigger.ProcessingTime(s"$triggerSec seconds"))
        .toTable(fqTable)
    }

    query.awaitTermination()
  }
}
