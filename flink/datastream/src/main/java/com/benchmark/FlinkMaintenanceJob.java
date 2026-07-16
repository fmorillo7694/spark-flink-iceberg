package com.benchmark;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.ExpireSnapshots;
import org.apache.iceberg.flink.maintenance.api.RewriteDataFiles;
import org.apache.iceberg.flink.maintenance.api.TableMaintenance;
import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory;

/**
 * Flink INLINE / SCHEDULED table maintenance — the contrast to Spark's bolt-on batch
 * compaction. Runs as its own long-lived Flink job (or embedded next to ingest) using
 * Iceberg 1.11's {@code TableMaintenance} API. Unlike Spark's manual per-invocation
 * CALL procedures, this schedules compaction AUTOMATICALLY on table-health thresholds
 * (commit count, position-delete record count, data-file count) — so a streaming table
 * that continuously accrues delete-file debt gets continuously compacted, instead of
 * needing an external scheduler firing a separate job.
 *
 * <p>Notably this API also includes ExpireSnapshots + orphan-file cleanup — the
 * METADATA maintenance that is currently BROKEN via Spark SQL procedures on the
 * iceberg-1.11 / Spark-4.1.2 combo (NoSuchMethodError). So Flink can bound both data
 * AND metadata debt in-job; Spark (on this release) can only compact data files.
 *
 * <p>Args / env:
 *   --database streaming  --table events_spark
 *   REWRITE_EVERY_COMMITS (default 3), EXPIRE_RETAIN_LAST (default 1),
 *   MAINT_INTERVAL_S (fallback interval trigger, default 60)
 */
public final class FlinkMaintenanceJob {

    public static void main(String[] args) throws Exception {
        ParamUtil p = ParamUtil.fromArgs(args);
        String catalogUri = p.get("catalog-uri", System.getenv().getOrDefault("CATALOG_URI", "http://iceberg-rest:8181"));
        String warehouse = p.get("warehouse", "s3://" + System.getenv().getOrDefault("WAREHOUSE_BUCKET", "warehouse") + "/");
        String database = p.get("database", System.getenv().getOrDefault("ICEBERG_DB", "streaming"));
        String table = p.get("table", System.getenv().getOrDefault("ICEBERG_TABLE", "events") + "_spark");
        int rewriteEveryCommits = Integer.parseInt(System.getenv().getOrDefault("REWRITE_EVERY_COMMITS", "3"));
        int expireRetainLast = Integer.parseInt(System.getenv().getOrDefault("EXPIRE_RETAIN_LAST", "1"));
        long intervalS = Long.parseLong(System.getenv().getOrDefault("MAINT_INTERVAL_S", "60"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        CatalogLoader catalogLoader = IcebergIngestJob.restCatalog(catalogUri, warehouse);
        TableIdentifier id = TableIdentifier.of(Namespace.of(database), table);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, id);

        // TableMaintenance needs a lock so it never runs concurrently with itself
        // across recoveries. In production this is JDBC/ZK; for a single-job local demo
        // an in-JVM lock is sufficient and keeps the stack self-contained.
        TriggerLockFactory lockFactory = new InJvmLockFactory();

        TableMaintenance.Builder b = TableMaintenance.forTable(env, tableLoader, lockFactory)
                .uidSuffix("sfi-maint")
                // rate-limit so scheduled tasks don't stampede
                .rateLimit(Duration.ofSeconds(10))
                .lockCheckDelay(Duration.ofSeconds(5));

        // RewriteDataFiles: bin-pack small files AND merge position-delete / deletion
        // vectors into the rewritten data (deleteFileThreshold triggers on DV debt).
        // Scheduled on commit count OR position-delete record count — whichever trips
        // first — so heavy-upsert bursts get compacted promptly.
        b.add(RewriteDataFiles.builder()
                .scheduleOnCommitCount(rewriteEveryCommits)
                .scheduleOnPosDeleteRecordCount(500_000)
                .deleteFileThreshold(1)          // rewrite a group as soon as it has any deletes
                .targetFileSizeBytes(128L * 1024 * 1024)
                .partialProgressEnabled(true)
                .partialProgressMaxCommits(2));

        // ExpireSnapshots: bound metadata growth (the thing Spark SQL can't do on 4.1.2).
        b.add(ExpireSnapshots.builder()
                .scheduleOnCommitCount(rewriteEveryCommits * 2)
                .maxSnapshotAge(Duration.ofMinutes(5))
                .retainLast(expireRetainLast)
                .cleanExpiredMetadata(true));

        b.append();  // wires the maintenance operators into the job graph

        env.execute("flink-iceberg-maintenance[" + database + "." + table
                + " rewrite/" + rewriteEveryCommits + "c interval/" + intervalS + "s]");
    }

    /**
     * Minimal in-JVM {@link TriggerLockFactory}. Fine for a single local maintenance
     * job (one JVM). Production deployments use JdbcLockFactory / ZkLockFactory so the
     * lock is shared across TaskManagers and survives recovery.
     */
    static final class InJvmLockFactory implements TriggerLockFactory {
        private static final AtomicBoolean TASK = new AtomicBoolean(false);
        private static final AtomicBoolean RECOVERY = new AtomicBoolean(false);

        @Override public void open() { }

        @Override public Lock createLock() { return new MemLock(TASK); }

        @Override public Lock createRecoveryLock() { return new MemLock(RECOVERY); }

        @Override public void close() { }

        private static final class MemLock implements Lock {
            private final AtomicBoolean held;
            MemLock(AtomicBoolean held) { this.held = held; }
            @Override public boolean tryLock() { return held.compareAndSet(false, true); }
            @Override public boolean isHeld() { return held.get(); }
            @Override public void unlock() { held.set(false); }
        }
    }
}
