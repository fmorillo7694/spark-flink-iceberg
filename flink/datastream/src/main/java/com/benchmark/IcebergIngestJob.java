package com.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.Map;

/**
 * Flink DataStream → Iceberg ingest benchmark job.
 *
 * <p>Kafka (JSON) → parse → RowData → {@link IcebergSink} (SinkV2). The write
 * distribution mode, format version, and upsert behaviour are all controlled by
 * job args so a single jar drives every feature-comparison run.
 *
 * <p>Args (all optional, sensible defaults from env):
 * <pre>
 *   --bootstrap  kafka:9092
 *   --topic      events
 *   --catalog-uri http://iceberg-rest:8181
 *   --warehouse  s3://warehouse/
 *   --database   streaming
 *   --table      events
 *   --distribution-mode none|hash|range   (default hash)
 *   --upsert     true|false               (default false)
 *   --parallelism N
 * </pre>
 */
public final class IcebergIngestJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Iceberg schema mirrored from common/event_schema.md, plus event_time: a
    // real timestamp column so we can partition by hours(event_time) (the
    // timestamp-partitioning axis) as an alternative to bucket(user_id).
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "event_id", Types.StringType.get()),
            Types.NestedField.required(2, "user_id", Types.LongType.get()),
            Types.NestedField.optional(3, "event_type", Types.StringType.get()),
            Types.NestedField.optional(4, "product_id", Types.LongType.get()),
            Types.NestedField.optional(5, "country", Types.StringType.get()),
            Types.NestedField.optional(6, "amount", Types.DoubleType.get()),
            Types.NestedField.optional(7, "currency", Types.StringType.get()),
            Types.NestedField.optional(8, "quantity", Types.IntegerType.get()),
            Types.NestedField.optional(9, "event_ts", Types.LongType.get()),
            Types.NestedField.optional(10, "ingest_ts", Types.LongType.get()),
            Types.NestedField.optional(11, "payload", Types.StringType.get()),
            Types.NestedField.optional(12, "event_time", Types.TimestampType.withoutZone()),
            // epoch-minute bucket for minute-granular time partitioning (Iceberg
            // has no native minute() transform, so we identity-partition on this).
            Types.NestedField.optional(13, "event_minute", Types.LongType.get()));

    public static void main(String[] args) throws Exception {
        ParamUtil p = ParamUtil.fromArgs(args);
        String bootstrap = p.get("bootstrap", System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:9092"));
        String topic = p.get("topic", System.getenv().getOrDefault("SOURCE_TOPIC", "events"));
        String catalogUri = p.get("catalog-uri", System.getenv().getOrDefault("CATALOG_URI", "http://iceberg-rest:8181"));
        String warehouse = p.get("warehouse", "s3://" + System.getenv().getOrDefault("WAREHOUSE_BUCKET", "warehouse") + "/");
        String database = p.get("database", System.getenv().getOrDefault("ICEBERG_DB", "streaming"));
        String table = p.get("table", System.getenv().getOrDefault("ICEBERG_TABLE", "events"));
        DistributionMode mode = DistributionMode.fromName(
                p.get("distribution-mode", System.getenv().getOrDefault("WRITE_DISTRIBUTION_MODE", "hash")));
        boolean upsert = Boolean.parseBoolean(p.get("upsert", "false"));
        int parallelism = Integer.parseInt(p.get("parallelism", "4"));
        // partitioning axis: "bucket" = bucket(N,user_id) | "time" = hours(event_time)
        String partitioning = p.get("partitioning", System.getenv().getOrDefault("PARTITIONING", "bucket"));
        // bucket count = number of concurrent write streams the sink can run. The
        // upsert write-path (per-bucket data + equality-delete files) is I/O-bound, so
        // this — not core count — is the real throughput lever. Default 48 lets all 12
        // slots write in parallel (was 16, which capped concurrency below parallelism).
        int buckets = Integer.parseInt(p.get("buckets", System.getenv().getOrDefault("BUCKETS", "48")));
        String formatVersion = p.get("format-version", System.getenv().getOrDefault("TABLE_FORMAT_VERSION", "2"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        // Commit interval is the latency floor for an Iceberg sink — make it explicit.
        long ckpt = Long.parseLong(System.getenv().getOrDefault("CHECKPOINT_INTERVAL_MS", "30000"));
        env.enableCheckpointing(ckpt);

        // earliest = chew any pre-filled backlog (overload test); latest = live only.
        String startOffsets = p.get("starting-offsets", System.getenv().getOrDefault("STARTING_OFFSETS", "latest"));
        OffsetsInitializer initial = "earliest".equalsIgnoreCase(startOffsets)
                ? OffsetsInitializer.earliest() : OffsetsInitializer.latest();
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId("flink-iceberg-bench")
                .setStartingOffsets(initial)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");
        DataStream<RowData> rows = raw.map(IcebergIngestJob::parse).name("json->rowdata");

        CatalogLoader catalogLoader = restCatalog(catalogUri, warehouse);
        TableIdentifier id = TableIdentifier.of(Namespace.of(database), table);
        ensureTable(catalogLoader, id, mode, partitioning, formatVersion, upsert, buckets);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, id);

        IcebergSink.Builder builder = IcebergSink.forRowData(rows)
                .tableLoader(tableLoader)
                .distributionMode(mode)
                .writeParallelism(parallelism);
        if (upsert) {
            builder.upsert(true).equalityFieldColumns(java.util.List.of("user_id"));
        }
        builder.append();

        // INLINE table maintenance — embedded in THIS ingest job's env (single
        // env.execute), matching the AWS sample DataStreamIcebergJob. Compaction runs
        // in the same job graph, so it SHARES the ingest job's slots (the fair contrast
        // to Spark's separate spark-submit). forTable(env, tableLoader) with NO lock
        // factory → Iceberg's Flink COORDINATOR lock (state-based, PR#15151) — no
        // JDBC/ZooKeeper needed. Gated by FLINK_INLINE_MAINT=1 so we can A/B against
        // the no-maintenance baseline.
        if ("1".equals(System.getenv().getOrDefault("FLINK_INLINE_MAINT", "0"))) {
            setupInlineMaintenance(env, tableLoader);
        }

        env.execute("flink-datastream-iceberg-ingest[" + mode + (upsert ? ",upsert" : "")
                + (System.getenv().getOrDefault("FLINK_INLINE_MAINT", "0").equals("1") ? ",inline-maint" : "") + "]");
    }

    /**
     * Attach Iceberg TableMaintenance operators to the ingest env (shared resources).
     * Triggers are commit/file-count based (per AWS sample) so a streaming table that
     * accrues delete-file debt gets continuously compacted in-job.
     */
    private static void setupInlineMaintenance(StreamExecutionEnvironment env, TableLoader tableLoader) throws Exception {
        int rewriteFileCount = Integer.parseInt(System.getenv().getOrDefault("MAINT_REWRITE_FILE_COUNT", "20"));
        int expireCommits = Integer.parseInt(System.getenv().getOrDefault("MAINT_EXPIRE_COMMITS", "10"));
        int retainLast = Integer.parseInt(System.getenv().getOrDefault("MAINT_RETAIN_LAST", "5"));
        // forTable(env, tableLoader) — 2-arg overload → coordinator lock (no external lock).
        org.apache.iceberg.flink.maintenance.api.TableMaintenance
                .forTable(env, tableLoader)
                .uidSuffix("sfi-inline-maint")
                .rateLimit(java.time.Duration.ofSeconds(10))
                .add(org.apache.iceberg.flink.maintenance.api.RewriteDataFiles.builder()
                        .scheduleOnDataFileCount(rewriteFileCount)
                        .scheduleOnPosDeleteRecordCount(500_000)
                        .deleteFileThreshold(1)
                        .targetFileSizeBytes(128L * 1024 * 1024)
                        .partialProgressEnabled(true)
                        .partialProgressMaxCommits(5))
                .add(org.apache.iceberg.flink.maintenance.api.ExpireSnapshots.builder()
                        .scheduleOnCommitCount(expireCommits)
                        .maxSnapshotAge(java.time.Duration.ofMinutes(30))
                        .retainLast(retainLast)
                        .cleanExpiredMetadata(true))
                .append();
    }

    private static RowData parse(String json) throws Exception {
        JsonNode n = MAPPER.readTree(json);
        GenericRowData r = new GenericRowData(13);
        long eventTs = n.path("event_ts").asLong();
        r.setField(0, StringData.fromString(n.path("event_id").asText("")));
        r.setField(1, n.path("user_id").asLong());
        r.setField(2, StringData.fromString(n.path("event_type").asText("")));
        r.setField(3, n.path("product_id").asLong());
        r.setField(4, StringData.fromString(n.path("country").asText("")));
        r.setField(5, n.path("amount").asDouble());
        r.setField(6, StringData.fromString(n.path("currency").asText("")));
        r.setField(7, n.path("quantity").asInt());
        r.setField(8, eventTs);
        r.setField(9, n.path("ingest_ts").asLong());
        r.setField(10, StringData.fromString(n.path("payload").asText("")));
        r.setField(11, org.apache.flink.table.data.TimestampData.fromEpochMillis(eventTs));
        r.setField(12, eventTs / 60_000L); // epoch minute
        return r;
    }

    /**
     * Local runs use the Iceberg REST catalog on MinIO; EKS runs set
     * CATALOG_TYPE=glue (or pass --catalog-uri glue) to use AWS Glue + real S3.
     * The rest of the job is identical — this is the only local↔cloud binding.
     */
    static CatalogLoader restCatalog(String uri, String warehouse) {  // package-private: reused by FlinkMaintenanceJob
        String catalogType = System.getenv().getOrDefault("CATALOG_TYPE", "rest");
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

        if ("glue".equalsIgnoreCase(catalogType) || "glue".equalsIgnoreCase(uri)) {
            String region = System.getenv().getOrDefault("AWS_REGION", "us-east-1");
            Map<String, String> props = new HashMap<>();
            props.put("warehouse", warehouse);
            props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            props.put("client.region", region);
            props.put("s3.region", region);
            // Explicit REGIONAL S3 endpoint: without it the SDK uses virtual-hosted
            // global "bucket.s3.amazonaws.com", whose wildcard cert doesn't cover a
            // multi-dot bucket name → SSLPeerUnverifiedException. The regional host
            // "s3.<region>.amazonaws.com" is covered by "*.s3.<region>.amazonaws.com".
            props.put("s3.endpoint", "https://s3." + region + ".amazonaws.com");
            return CatalogLoader.custom(
                    "demo", props, conf, "org.apache.iceberg.aws.glue.GlueCatalog");
        }

        Map<String, String> props = new HashMap<>();
        props.put("uri", uri);
        props.put("warehouse", warehouse);
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint", System.getenv().getOrDefault("S3_ENDPOINT", "http://minio:9000"));
        props.put("s3.path-style-access", "true");
        return CatalogLoader.rest("demo", conf, props);
    }

    private static void ensureTable(CatalogLoader loader, TableIdentifier id, DistributionMode mode,
                                    String partitioning, String formatVersion, boolean upsert, int buckets) {
        // Works for any Catalog impl (REST locally, Glue on EKS).
        org.apache.iceberg.catalog.Catalog catalog = loader.loadCatalog();
        try {
            // The REST/Glue catalog requires the namespace to exist before createTable.
            if (catalog instanceof org.apache.iceberg.catalog.SupportsNamespaces) {
                org.apache.iceberg.catalog.SupportsNamespaces ns =
                        (org.apache.iceberg.catalog.SupportsNamespaces) catalog;
                if (!ns.namespaceExists(id.namespace())) {
                    ns.createNamespace(id.namespace());
                }
            }
            // Clean per-run state: drop+purge the table so each run measures only
            // its own output (RECREATE_TABLE=1). purge=true removes the data files
            // too — the engine self-cleans, no gated AWS CLI call needed.
            if ("1".equals(System.getenv().getOrDefault("RECREATE_TABLE", "0")) && catalog.tableExists(id)) {
                catalog.dropTable(id, true);
            }
            if (!catalog.tableExists(id)) {
                // Partitioning axis: by cardinality (bucket on the upsert key) or by time.
                // "time" = identity on epoch-minute (≈5 partitions in a 5-min run,
                // so time-partitioning fan-out is actually exercised); "bucket" =
                // 16 hash buckets on the upsert key (cardinality partitioning).
                PartitionSpec spec = "time".equalsIgnoreCase(partitioning)
                        ? PartitionSpec.builderFor(SCHEMA).identity("event_minute").build()
                        : PartitionSpec.builderFor(SCHEMA).bucket("user_id", buckets).build();  // N concurrent write streams
                Map<String, String> props = new HashMap<>();
                props.put("format-version", formatVersion);
                props.put("write.distribution-mode", mode.modeName());
                if (upsert) {
                    props.put("write.upsert.enabled", "true");
                }
                catalog.createTable(id, SCHEMA, spec, props);
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to ensure table " + id, e);
        } finally {
            if (catalog instanceof AutoCloseable) {
                try { ((AutoCloseable) catalog).close(); } catch (Exception ignore) { /* best effort */ }
            }
        }
    }
}
