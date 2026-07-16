#!/usr/bin/env bash
# Run ONE Part 2 cell by hand (reliable — no matrix orchestration). Each cell:
#   ingest (Spark MoR / Flink DV) + read_probe (DuckDB) + metadata_probe, for the
#   window; COMPACT cells fire that engine's compaction in an ISOLATED container
#   (Spark) or a maintenance job (Flink) mid-run, then keep probing to see recovery.
#
# Usage: bench/part2_cell.sh <engine> <mode>
#   engine=spark|flink   mode=nocompact|compact
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; source .env 2>/dev/null; set +a
PY=.venv/bin/python

ENGINE="$1"; MODE="$2"
INGEST_S="${INGEST_S:-360}"; COMPACT_AT="${COMPACT_AT:-210}"; PROBE_INT="${PROBE_INT:-20}"
TABLE=events; [ "$ENGINE" = spark ] && TABLE=events_spark
CELL="${ENGINE}_${MODE}"; CO="results/local/part2/$CELL"
mkdir -p results/local/part2
# Flink tables (equality deletes) time out in DuckDB — short scan timeout so the probe
# records TIMEOUT quickly instead of stalling; Spark (position DVs) reads in ms.
STO=45; [ "$ENGINE" = flink ] && STO=25
log(){ echo "[$(date '+%H:%M:%S')] $*"; }

log "CELL $CELL — ingest ${INGEST_S}s, compact@${COMPACT_AT}s ($MODE)"

# 1. ingest
if [ "$ENGINE" = spark ]; then
  SPJ=0 RUN_SECONDS="$INGEST_S" WARMUP=25 bash bench/run_local_bench.sh spark upsert merge-on-read > "$CO.ingest.log" 2>&1 &
else
  RUN_SECONDS="$INGEST_S" WARMUP=25 bash bench/run_local_bench.sh flink upsert > "$CO.ingest.log" 2>&1 &
fi
IPID=$!
for _ in $(seq 1 40); do curl -s "http://localhost:8181/v1/namespaces/${ICEBERG_DB:-streaming}/tables/${TABLE}" 2>/dev/null | grep -q current-snapshot-id && break; sleep 4; done
log "  table live; probes on"

# 2. probes
$PY bench/read_probe.py     --table "$TABLE" --key user_id --scan-timeout "$STO" --seconds "$((INGEST_S+30))" --interval "$PROBE_INT" --out "$CO" > "$CO.readprobe.log" 2>&1 &
RP=$!
$PY bench/metadata_probe.py --table "$TABLE" --seconds "$((INGEST_S+30))" --interval "$PROBE_INT" --out "$CO" > "$CO.metaprobe.log" 2>&1 &
MP=$!

# 3. compaction (isolated) mid-run
if [ "$MODE" = compact ]; then
  sleep "$COMPACT_AT"
  log "  [t≈${COMPACT_AT}s] $ENGINE compaction (isolated) while ingest live"
  T0=$(date +%s)
  if [ "$ENGINE" = spark ]; then
    # concurrent data compaction via docker exec, CORES=2 (proven to coexist with the
    # live 4-core ingest in the shared container — 32s, rc=0). before/after file counts
    # + duration ARE the compaction-cost record (compact_spark.sh writes compact.csv).
    CORES=2 MEM_GB=3 DO_DATA=1 DO_DELETES=1 DO_MANIFESTS=0 DO_EXPIRE=0 \
      bash bench/compact_spark.sh "$TABLE" > "$CO.compact.log" 2>&1 || true
    cp results/local/compact_${TABLE}.compact.csv "$CO.compact.csv" 2>/dev/null || true
  else
    docker cp flink/datastream/target/flink-iceberg-bench.jar flink-jobmanager:/opt/flink/app.jar >/dev/null 2>&1
    docker exec -d -e CATALOG_URI=http://iceberg-rest:8181 -e WAREHOUSE_BUCKET=warehouse -e S3_ENDPOINT=http://minio:9000 \
      -e AWS_ACCESS_KEY_ID=admin -e AWS_SECRET_ACCESS_KEY=password -e REWRITE_EVERY_COMMITS=1 -e EXPIRE_RETAIN_LAST=10 \
      flink-jobmanager flink run -d -c com.benchmark.FlinkMaintenanceJob /opt/flink/app.jar --database "${ICEBERG_DB:-streaming}" --table "$TABLE" > "$CO.compact.log" 2>&1 || true
    sleep 90
  fi
  echo "compact_duration_s,$(( $(date +%s)-T0 ))" > "$CO.compact_meta.csv"
  log "  compaction returned; conflicts: $(grep -icE 'CommitFailed|conflict|ValidationException' "$CO.ingest.log" 2>/dev/null)"
fi

# 4. finish
wait "$IPID" 2>/dev/null || true
kill "$RP" "$MP" 2>/dev/null || true
for j in $(curl -s http://localhost:8081/jobs 2>/dev/null | $PY -c "import sys,json;[print(x['id']) for x in json.load(sys.stdin).get('jobs',[]) if x['status']=='RUNNING']" 2>/dev/null); do curl -s -XPATCH "http://localhost:8081/jobs/$j?mode=cancel" >/dev/null 2>&1; done
docker exec spark bash -c "pkill -9 -f spark-submit" 2>/dev/null || true
docker rm -f "sfi-compactor-${TABLE}" >/dev/null 2>&1 || true
log "CELL $CELL DONE → $CO.*"
