#!/usr/bin/env bash
# Part 2 local matrix — compaction & table health for streaming Iceberg upsert.
#
# 4 cells = {Spark MoR, Flink DV} × {no-compact, compact-mid-run}. Each cell:
#   - runs the streaming upsert (builds delete/DV debt)
#   - read_probe (DuckDB): query latency vs accumulating position-deletes
#   - metadata_probe: metadata.json bytes, snapshots, manifests
#   - health from snapshots.csv: data/delete files, position-deletes over commits
#   - COMPACT cells: at COMPACT_AT, run that engine's native compaction WHILE ingest
#     is live, and sample the compactor container's CPU/mem (resource cost) + duration
#   - captures ingest lag before/during/after compaction (contention)
#
# Engines' native compaction:
#   spark → bench/compact_spark.sh   (bolt-on batch spark-sql CALL procedures)
#   flink → FlinkMaintenanceJob      (in-job TableMaintenance: rewrite + expire)
#
# Usage: bench/part2_matrix.sh                 # all 4 cells, 8-min each
#        CELLS="spark_nocompact spark_compact"  bench/part2_matrix.sh   # subset
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; source .env 2>/dev/null; set +a
PY=.venv/bin/python

INGEST_S="${INGEST_S:-360}"     # 6 min ingest
COMPACT_AT="${COMPACT_AT:-210}" # compact ~3.5 min in (while live), leaves 2.5m to observe recovery
PROBE_INT="${PROBE_INT:-20}"
OUT=results/local/part2
mkdir -p "$OUT"
CELLS="${CELLS:-spark_nocompact spark_compact flink_nocompact flink_compact}"

log(){ echo "[$(date '+%H:%M:%S')] $*"; }

# sample a container's CPU(cores)+mem(MiB) each 3s into a csv while $1(pid) alive
sample_stats(){ # $1=container $2=outfile
  ( echo "t_s,cpu_cores,mem_mb" > "$2"; s=$(date +%s)
    while :; do
      docker stats --no-stream --format '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}' 2>/dev/null \
        | awk -F'|' -v n="$1" -v t="$(( $(date +%s)-s ))" '$1==n{gsub(/%/,"",$2);split($3,a,"/");m=a[1];v=m;gsub(/[^0-9.]/,"",v);if(m~/GiB/)v=v*1024;else if(m~/KiB/)v=v/1024;print t","$2/100","v}' >> "$2"
      sleep 3
    done ) & echo $!
}

run_cell(){ # $1 = cell name (engine_mode)
  local cell="$1"; local engine="${cell%%_*}"; local mode="${cell##*_}"
  local co="$OUT/$cell"
  log "=== CELL $cell (engine=$engine, $mode) ==="
  local TABLE=events; [ "$engine" = spark ] && TABLE=events_spark

  # 1. start streaming upsert (Spark MoR / Flink DV) in background
  if [ "$engine" = spark ]; then
    SPJ=0 RUN_SECONDS="$INGEST_S" WARMUP=25 bash bench/run_local_bench.sh spark upsert merge-on-read > "$co.ingest.log" 2>&1 &
  else
    RUN_SECONDS="$INGEST_S" WARMUP=25 bash bench/run_local_bench.sh flink upsert > "$co.ingest.log" 2>&1 &
  fi
  local IPID=$!
  # wait for table to exist
  for _ in $(seq 1 40); do
    curl -s "http://localhost:8181/v1/namespaces/${ICEBERG_DB:-streaming}/tables/${TABLE}" 2>/dev/null | grep -q current-snapshot-id && break; sleep 4; done

  # 2. probes for the whole window (+30s tail)
  $PY bench/read_probe.py     --table "$TABLE" --seconds "$((INGEST_S+30))" --interval "$PROBE_INT" --out "$co" > "$co.read.log" 2>&1 &
  local RP=$!
  $PY bench/metadata_probe.py --table "$TABLE" --seconds "$((INGEST_S+30))" --interval "$PROBE_INT" --out "$co" > "$co.metad.log" 2>&1 &
  local MP=$!

  # 3. compaction cells: fire native compaction mid-run, sample compactor resources
  if [ "$mode" = compact ]; then
    sleep "$COMPACT_AT"
    log "  [t≈${COMPACT_AT}s] $engine compaction while ingest live"
    if [ "$engine" = spark ]; then
      # CONCURRENT Spark compaction in an ISOLATED container (own cores/mem) so it
      # doesn't contend with the live ingest container — mirrors production (compaction
      # is separate compute) and avoids the single-shared-container deadlock. Data-only
      # here (rewrite_data_files + position/DV deletes); metadata maintenance runs
      # post-ingest in a quiescent window (Spark procs stall under live writes).
      local SPID; SPID=$(sample_stats "sfi-compactor-${TABLE}" "$co.compactor_stats.csv")
      T0=$(date +%s)
      COMPACT_ISOLATED=1 COMPACT_TIMEOUT=180 CORES="${COMPACT_CORES:-3}" MEM_GB="${COMPACT_MEM_GB:-4}" \
        DO_DATA=1 DO_DELETES=1 DO_MANIFESTS=0 DO_EXPIRE=0 \
        bash bench/compact_spark.sh "$TABLE" > "$co.compact.wrap.log" 2>&1 || true
      echo "compact_duration_s,$(( $(date +%s)-T0 ))" > "$co.compact_meta.csv"
      kill "$SPID" 2>/dev/null || true
    else
      # Flink maintenance as a separate job; sample the TM container
      local MPID; MPID=$(sample_stats "spark-flink-iceberg-flink-taskmanager-1" "$co.compactor_stats.csv")
      T0=$(date +%s)
      docker cp flink/datastream/target/flink-iceberg-bench.jar flink-jobmanager:/opt/flink/app.jar >/dev/null 2>&1
      docker exec -d -e CATALOG_URI=http://iceberg-rest:8181 -e WAREHOUSE_BUCKET=warehouse -e S3_ENDPOINT=http://minio:9000 \
        -e AWS_ACCESS_KEY_ID=admin -e AWS_SECRET_ACCESS_KEY=password -e REWRITE_EVERY_COMMITS=1 -e EXPIRE_RETAIN_LAST=1 \
        flink-jobmanager flink run -d -c com.benchmark.FlinkMaintenanceJob /opt/flink/app.jar --database "${ICEBERG_DB:-streaming}" --table "$TABLE" > "$co.compact.log" 2>&1 || true
      sleep 90   # let a maintenance cycle run
      echo "compact_duration_s,$(( $(date +%s)-T0 ))" > "$co.compact_meta.csv"
      kill "$MPID" 2>/dev/null || true
    fi
    grep -icE 'CommitFailed|conflict|ValidationException' "$co.ingest.log" 2>/dev/null | xargs echo "  writer conflicts:"
  fi

  # 4. let ingest + probes finish
  wait "$IPID" 2>/dev/null || true
  wait "$RP" "$MP" 2>/dev/null || true
  # cancel any leftover flink maintenance job
  for j in $(curl -s http://localhost:8081/jobs 2>/dev/null | $PY -c "import sys,json;[print(x['id']) for x in json.load(sys.stdin).get('jobs',[]) if x['status']=='RUNNING']" 2>/dev/null); do
    curl -s -XPATCH "http://localhost:8081/jobs/$j?mode=cancel" >/dev/null 2>&1; done
  docker exec spark bash -c "pkill -9 -f spark-submit" 2>/dev/null || true

  # 5. POST-INGEST metadata maintenance (quiescent window) — Spark: rewrite_manifests
  # + expire_snapshots (they stall under live ingest, so run them now, table idle).
  # This captures metadata-maintenance timing/cost separately from data compaction.
  if [ "$mode" = compact ] && [ "$engine" = spark ]; then
    log "  post-ingest metadata maintenance (rewrite_manifests + expire_snapshots)"
    T0=$(date +%s)
    DO_DATA=0 DO_DELETES=0 DO_MANIFESTS=1 DO_EXPIRE=1 timeout 120 bash bench/compact_spark.sh "$TABLE" > "$co.metamaint.log" 2>&1 || true
    echo "metadata_maint_duration_s,$(( $(date +%s)-T0 ))" >> "$co.compact_meta.csv"
  fi
  log "  cell $cell done → $co.*"
  sleep 10
}

log "PART 2 MATRIX START — cells: $CELLS  (ingest ${INGEST_S}s, compact@${COMPACT_AT}s)"
for c in $CELLS; do run_cell "$c"; done
log "PART 2 MATRIX COMPLETE → $OUT/"
