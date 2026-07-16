#!/usr/bin/env bash
# Local (Docker) mirror of eks/04_run_benchmark.sh — SAME commit-honest methodology
# so local and EKS numbers are directly comparable (same ratios, same metrics):
#   lag  = kafka produced (end offsets) − Iceberg current-snapshot total-records
#   files/bytes/avg = Iceberg current-snapshot summary
#   cpu/mem = docker stats over the engine container(s)
#   + Spark per-batch durationMs / Flink checkpoint stats at teardown
#
# PROPORTIONAL ENVELOPE (scaled 2:1 from EKS's 14c/28GB @ 100k so it fits the
# 14-core/31GB host alongside Kafka+MinIO+REST): 7 cores / 14 GB per engine,
# 50k rows/s input (same ~7.1k rows/core/s as EKS), IDENTICAL 24 buckets + 60s
# commits + 12 partitions (config, not resources — kept equal to EKS).
#
# Usage: bench/run_local_bench.sh <engine> [write] [merge_mode]
#   engine=flink|spark  write=append|upsert  merge_mode=merge-on-read|copy-on-write
# Env: RUN_SECONDS=300 WARMUP=60 CORES=7 MEM_GB=14 RATE=50000 BUCKETS=24 PARTITIONS=12
set -uo pipefail
cd "$(dirname "$0")/.."
# Preserve caller-provided overrides BEFORE sourcing .env (which defines its own
# RUN_SECONDS=600 etc. and would otherwise clobber the values passed on the CLI).
_RUN_SECONDS="${RUN_SECONDS:-}"; _WARMUP="${WARMUP:-}"; _RATE="${RATE:-}"
_CORES="${CORES:-}"; _MEM_GB="${MEM_GB:-}"; _BUCKETS="${BUCKETS:-}"; _PARTITIONS="${PARTITIONS:-}"
set -a; source .env; set +a
# restore caller overrides
[ -n "$_RUN_SECONDS" ] && RUN_SECONDS="$_RUN_SECONDS"; [ -n "$_WARMUP" ] && WARMUP="$_WARMUP"
[ -n "$_RATE" ] && RATE="$_RATE"; [ -n "$_CORES" ] && CORES="$_CORES"; [ -n "$_MEM_GB" ] && MEM_GB="$_MEM_GB"
[ -n "$_BUCKETS" ] && BUCKETS="$_BUCKETS"; [ -n "$_PARTITIONS" ] && PARTITIONS="$_PARTITIONS"
PY=.venv/bin/python

ENGINE="${1:-flink}"
WRITE="${2:-append}"
MERGE_MODE="${3:-merge-on-read}"
CORES="${CORES:-7}"
MEM_GB="${MEM_GB:-14}"
RATE="${RATE:-50000}"
BUCKETS="${BUCKETS:-24}"
PARTITIONS="${PARTITIONS:-12}"
PRODUCERS="${PRODUCERS:-5}"          # 5 × 10k = 50k
RUN_SECONDS="${RUN_SECONDS:-300}"
WARMUP="${WARMUP:-60}"
COMMIT_S="${COMMIT_S:-60}"           # 60s commits, same as EKS
UPSERT=false; FMT=2; DUP=0.0
[ "$WRITE" = "upsert" ] && { UPSERT=true; FMT=3; DUP=0.5; }
TABLE="events"; [ "$ENGINE" = "spark" ] && TABLE="events_spark"
LABEL="local_${ENGINE}_${WRITE}_bucket"
if [ "$WRITE" = "upsert" ] && [ "$ENGINE" = "spark" ]; then
  case "$MERGE_MODE" in copy-on-write) LABEL="${LABEL}_cow";; *) LABEL="${LABEL}_mor";; esac
  [ "${SPJ:-0}" = "1" ] && LABEL="${LABEL}_spj"   # distinct result files for SPJ before/after
fi
OUT="results/local/${LABEL}"
mkdir -p results/local
echo "==> $LABEL  cores=$CORES mem=${MEM_GB}G rate=${RATE} buckets=$BUCKETS commit=${COMMIT_S}s win=${RUN_SECONDS}s"

# ---- helpers ----
koffsets() { docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$SOURCE_TOPIC" --time -1 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'; }

# landed(): current-snapshot total-records + snapshot count via REST catalog (same
# summary schema as Glue on EKS). Prints "total_records snapshot_count".
landed() {
  curl -s "http://localhost:8181/v1/namespaces/${ICEBERG_DB}/tables/${TABLE}" 2>/dev/null | $PY -c '
import sys,json
try: d=json.load(sys.stdin); m=d.get("metadata",{})
except Exception: print("0 0"); sys.exit(0)
cur=m.get("current-snapshot-id"); snaps=m.get("snapshots",[]) or []
tot=0
for s in snaps:
    if s.get("snapshot-id")==cur: tot=int(s.get("summary",{}).get("total-records",0) or 0)
print(f"{tot} {len(snaps)}")
' 2>/dev/null || echo "0 0"
}

drop_table() {
  $PY - "$TABLE" <<'PYEOF' 2>/dev/null || true
import sys
from pyiceberg.catalog import load_catalog
c=load_catalog('demo',**{'type':'rest','uri':'http://localhost:8181','warehouse':'s3://warehouse/','s3.endpoint':'http://localhost:9000','s3.access-key-id':'admin','s3.secret-access-key':'password','s3.path-style-access':'true'})
try: c.drop_table(('streaming', sys.argv[1]))
except Exception: pass
PYEOF
  docker run --rm --network sfi-bench --entrypoint sh minio/mc:RELEASE.2024-11-05T11-29-45Z -c "
    mc alias set m http://minio:9000 admin password >/dev/null 2>&1;
    mc rm -r --force m/warehouse/streaming/$TABLE >/dev/null 2>&1;
    mc rm -r --force m/warehouse/streaming.db/$TABLE >/dev/null 2>&1;" 2>/dev/null || true
}

cancel_flink() {
  for j in $(curl -s http://localhost:8081/jobs 2>/dev/null | $PY -c "import sys,json;[print(x['id']) for x in json.load(sys.stdin).get('jobs',[]) if x['status'] in ('RUNNING','RESTARTING','CREATED')]" 2>/dev/null); do
    curl -s -XPATCH "http://localhost:8081/jobs/$j?mode=cancel" >/dev/null 2>&1 || true
  done
  sleep 5
}

# ---- 1. clean slate: fresh topic + drop table ----
echo "==> fresh topic ($PARTITIONS partitions) + drop $TABLE"
cancel_flink
docker exec spark bash -c "pkill -9 -f spark-submit" 2>/dev/null || true
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$SOURCE_TOPIC" 2>/dev/null || true
sleep 3
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic "$SOURCE_TOPIC" --partitions "$PARTITIONS" --replication-factor 1 \
  --config retention.ms=180000 --config retention.bytes=1073741824 \
  --config segment.ms=30000 2>/dev/null || true
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group flink-iceberg-bench 2>/dev/null || true
drop_table

# ---- 2. start producers FIRST (so Spark batch 0 has data), then engine at 'latest' ----
echo "==> producers ($PRODUCERS × $((RATE/PRODUCERS)) = $RATE rows/s)"
PRODUCER_PIDS=()
per=$(( RATE / PRODUCERS ))
dup=""; [ "$WRITE" = "upsert" ] && dup="--dup-rate 0.5"
for i in $(seq 1 "$PRODUCERS"); do
  $PY common/producer.py --bootstrap "$KAFKA_BOOTSTRAP_HOST" --topic "$SOURCE_TOPIC" \
    --rate "$per" --seconds "$((WARMUP + RUN_SECONDS + 120))" --payload-bytes "$PAYLOAD_BYTES" $dup \
    > "results/local/producer.$i.log" 2>&1 &
  PRODUCER_PIDS+=($!)
done
sleep 8

# ---- 3. deploy engine ----
if [ "$ENGINE" = "flink" ]; then
  docker cp flink/datastream/target/flink-iceberg-bench.jar flink-jobmanager:/opt/flink/app.jar >/dev/null 2>&1
  docker exec -d -e STARTING_OFFSETS=latest -e BUCKETS="$BUCKETS" -e CHECKPOINT_INTERVAL_MS="$((COMMIT_S*1000))" \
    -e FLINK_INLINE_MAINT="${FLINK_INLINE_MAINT:-0}" \
    -e MAINT_REWRITE_FILE_COUNT="${MAINT_REWRITE_FILE_COUNT:-20}" -e MAINT_EXPIRE_COMMITS="${MAINT_EXPIRE_COMMITS:-10}" -e MAINT_RETAIN_LAST="${MAINT_RETAIN_LAST:-5}" \
    flink-jobmanager flink run -d -c com.benchmark.IcebergIngestJob /opt/flink/app.jar \
    --distribution-mode hash --upsert "$UPSERT" --partitioning bucket \
    --format-version "$FMT" --parallelism "$CORES" >/dev/null 2>&1
  for _ in $(seq 1 20); do
    [ "$(curl -s http://localhost:8081/jobs 2>/dev/null | $PY -c "import sys,json;print(sum(1 for x in json.load(sys.stdin).get('jobs',[]) if x['status']=='RUNNING'))" 2>/dev/null || echo 0)" -ge 1 ] && break; sleep 3
  done
  echo "    flink RUNNING — UI http://localhost:8081"
else
  docker exec spark bash -c "rm -rf /tmp/spark-ckpt/$TABLE" 2>/dev/null || true
  docker cp spark/scala/target/scala-2.13/spark-iceberg-bench_2.13-1.0.0.jar spark:/opt/spark/work-dir/app.jar >/dev/null 2>&1
  docker exec -d -e KAFKA_BOOTSTRAP=kafka:9092 -e CATALOG_URI=http://iceberg-rest:8181 \
    -e AWS_ACCESS_KEY_ID=admin -e AWS_SECRET_ACCESS_KEY=password -e AWS_REGION=us-east-1 \
    -e WAREHOUSE_BUCKET=warehouse -e S3_ENDPOINT=http://minio:9000 \
    -e PARTITIONING=bucket -e TABLE_FORMAT_VERSION="$FMT" -e BUCKETS="$BUCKETS" \
    -e STARTING_OFFSETS=latest -e WRITE_MERGE_MODE="$MERGE_MODE" -e SPJ="${SPJ:-0}" \
    -e SPARK_COMPACTION="${SPARK_COMPACTION:-none}" -e SPARK_INLINE_EVERY="${SPARK_INLINE_EVERY:-10}" \
    -e SPARK_SCHEDULED_EVERY_SEC="${SPARK_SCHEDULED_EVERY_SEC:-120}" -e SPARK_MAINT_EXPIRE="${SPARK_MAINT_EXPIRE:-1}" \
    spark bash -c "nohup /opt/spark/bin/spark-submit --master 'local[$CORES]' --driver-memory ${MEM_GB}g \
      --class com.benchmark.IcebergIngestJob --conf spark.sql.shuffle.partitions=$BUCKETS \
      /opt/spark/work-dir/app.jar --distribution-mode hash --upsert '$UPSERT' \
      --partitioning bucket --format-version '$FMT' --merge-mode '$MERGE_MODE' --trigger '$COMMIT_S' \
      > /tmp/spark_${LABEL}.log 2>&1 &"
  sleep 25
  ( while :; do docker cp "spark:/tmp/spark_${LABEL}.log" "results/local/${LABEL}.spark.log" 2>/dev/null; sleep 5; done ) & TAILER=$!
  echo "    spark submitted — UI http://localhost:4040"
fi

# ---- 4. sample every 15s: produced, landed, lag, snapshots, cpu, mem ----
echo "t_s,produced,landed,lag,snapshots,cpu_cores,mem_mb" > "${OUT}.lag.csv"
S=$(date +%s); END=$(( S + WARMUP + RUN_SECONDS ))
# Container NAME to sum stats for. Flink TM name CONTAINS 'taskmanager'; the Spark
# container is EXACTLY 'spark' (but the Flink TM name also contains 'spark-flink…',
# so we match $1 exactly for Spark). Matched on the awk $1 field, not grep, to avoid
# ERE pitfalls (a literal '|' means alternation) and substring collisions.
CNAME='taskmanager'; MODE='contains'
[ "$ENGINE" = "spark" ] && { CNAME='spark'; MODE='exact'; }
while [ "$(date +%s)" -lt "$END" ]; do
  t=$(( $(date +%s) - S )); prod=$(koffsets || true); prod=${prod:-0}
  la=$(landed || true); ln=${la% *}; snaps=${la#* }; ln=${ln:-0}; snaps=${snaps:-0}
  lag=$(( prod - ln ))
  # docker stats: cores = CPU% / 100; mem normalized to MiB (handles GiB/MiB/KiB).
  # Match container name on awk field $1 ($MODE = exact|contains).
  cm=$(docker stats --no-stream --format '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}' 2>/dev/null \
    | awk -F'|' -v name="$CNAME" -v mode="$MODE" '
        (mode=="exact" && $1==name) || (mode=="contains" && index($1,name)) {
          gsub(/%/,"",$2); c+=$2;
          split($3,a,"/"); m=a[1]; val=m; gsub(/[^0-9.]/,"",val);
          if (m ~ /GiB/) val=val*1024; else if (m ~ /KiB/) val=val/1024;   # else already MiB
          mm+=val
        } END{printf "%.2f,%d", c/100, mm}' || true)
  echo "$t,$prod,$ln,$lag,$snaps,${cm:-,}" >> "${OUT}.lag.csv"
  sleep 15
done

# ---- 5. file sizing from Iceberg snapshot summary (engine-agnostic) ----
curl -s "http://localhost:8181/v1/namespaces/${ICEBERG_DB}/tables/${TABLE}" 2>/dev/null | OUT="$OUT" $PY -c '
import sys,json,os
d=json.load(sys.stdin); m=d.get("metadata",{})
cur=m.get("current-snapshot-id"); snaps=m.get("snapshots",[]) or []
def g(su,k):
    v=su.get(k); return int(v) if v not in (None,"") else 0
curs=next((s for s in snaps if s.get("snapshot-id")==cur), None)
if curs:
    su=curs["summary"]; files=g(su,"total-data-files"); recs=g(su,"total-records"); by=g(su,"total-files-size")
    print("files=%d bytes=%d records=%d avg_mb=%.1f snapshots=%d"%(files,by,recs,(by/files/1e6) if files else 0,len(snaps)))
out=os.environ["OUT"]
with open(out+".snapshots.csv","w") as f:
    # per-commit health: data + delete/DV files, so we can chart read-amplification
    # (delete files piling up), target growth, and prove v3 deletion vectors (added_dvs).
    f.write("idx,ts_ms,interval_s,op,added_records,added_files,added_size,commit_avg_mb,"
            "total_records,total_files,added_delete_files,added_dvs,total_delete_files,total_position_deletes\n")
    prev=None
    for i,s in enumerate(snaps):
        su=s.get("summary",{}); ts=int(s.get("timestamp-ms",0)); iv=((ts-prev)/1000.0) if prev else 0.0; prev=ts
        af=g(su,"added-data-files"); asz=g(su,"added-files-size")
        f.write("%d,%d,%.1f,%s,%d,%d,%d,%.1f,%d,%d,%d,%d,%d,%d\n"%(i,ts,iv,su.get("operation",""),
            g(su,"added-records"),af,asz,(asz/af/1e6) if af else 0,g(su,"total-records"),g(su,"total-data-files"),
            g(su,"added-delete-files"),g(su,"added-dvs"),g(su,"total-delete-files"),g(su,"total-position-deletes")))
' > "${OUT}.files.txt" || true
cat "${OUT}.files.txt" 2>/dev/null || true

# ---- 5b. engine-native batch/checkpoint timing ----
if [ "$ENGINE" = "flink" ]; then
  JID=$(curl -s http://localhost:8081/jobs 2>/dev/null | $PY -c "import sys,json;js=[x['id'] for x in json.load(sys.stdin).get('jobs',[]) if x['status']=='RUNNING'];print(js[0] if js else '')" 2>/dev/null)
  [ -n "$JID" ] && curl -s "http://localhost:8081/jobs/$JID/checkpoints" 2>/dev/null | $PY -c '
import sys,json
try: d=json.load(sys.stdin)
except Exception: sys.exit(0)
print("idx,ckpt_id,duration_ms,state_size_bytes,status")
for c in d.get("history",[]) or []:
    print("%s,%s,%s,%s,%s"%(c.get("id"),c.get("id"),c.get("end_to_end_duration"),c.get("state_size"),c.get("status")))
' > "${OUT}.ckpt.csv" 2>/dev/null || true
else
  $PY -c '
import sys,json,re
txt=open("results/local/'"${LABEL}"'.spark.log").read()
print("batchId,triggerExecution_ms,addBatch_ms,walCommit_ms,getBatch_ms,queryPlanning_ms,numInputRows,inputRowsPerSecond,processedRowsPerSecond")
for mobj in re.finditer(r"\{\s*\"id\"\s*:.*?\n\}", txt, re.S):
    blk=mobj.group(0)
    if "durationMs" not in blk: continue
    try: p=json.loads(blk)
    except Exception: continue
    d=p.get("durationMs",{}) or {}
    print("%s,%s,%s,%s,%s,%s,%s,%s,%s"%(p.get("batchId"),d.get("triggerExecution"),d.get("addBatch"),d.get("walCommit"),d.get("getBatch"),d.get("queryPlanning"),p.get("numInputRows"),p.get("inputRowsPerSecond"),p.get("processedRowsPerSecond")))
' > "${OUT}.batches.csv" 2>/dev/null || true
fi

# ---- 6. teardown ----
[ "$ENGINE" = "flink" ] && cancel_flink
[ "$ENGINE" = "spark" ] && { kill "${TAILER:-0}" 2>/dev/null || true; docker exec spark bash -c "pkill -9 -f spark-submit" 2>/dev/null || true; }
kill "${PRODUCER_PIDS[@]}" 2>/dev/null || true
echo "==> $LABEL done → ${OUT}.*"
