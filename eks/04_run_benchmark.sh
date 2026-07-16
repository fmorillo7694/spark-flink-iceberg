#!/usr/bin/env bash
# Run one engine's EKS benchmark: deploy the engine, fan out the producer fleet,
# then measure Kafka consumer-group lag + committed rows + pod CPU/mem, and clean
# up the engine. 1-minute commits; workload sized to pressure the sources.
#
# Usage: eks/04_run_benchmark.sh <engine> [parallelism] [prodpods] [rate_per_pod]
#   engine: flink | spark
# Env: WRITE=append|upsert  PART=bucket|time  RUN_SECONDS=300  WARMUP=90
set -euo pipefail
cd "$(dirname "$0")"
source ./00_profile.env
_sfi_guard || exit 1
source ./.env.eks           # BUCKET, GLUE_DB, ACCOUNT
source ./.registry.env      # REGISTRY, TAG

ENGINE="${1:-flink}"
PARALLELISM="${2:-8}"
PRODPODS="${3:-${PRODPODS:-10}}"
RATE="${4:-${RATE:-10000}}"          # 10 pods × 10k = 100k rps aggregate (keep-up test; matrix passes this)
WRITE="${WRITE:-append}"
PART="${PART:-bucket}"
RUN_SECONDS="${RUN_SECONDS:-300}"
WARMUP="${WARMUP:-90}"
# producers now start BEFORE the engine, so their runtime must also cover the
# prefill (12s) + engine-deploy wait (up to ~180s) before the measurement window.
PRODUCER_SECONDS=$((WARMUP + RUN_SECONDS + 240))
UPSERT=false; FMT=2; DUP=0.0
[ "$WRITE" = "upsert" ] && { UPSERT=true; FMT=3; DUP=0.5; }
# Spark upsert engine mode: merge-on-read (default, deletion vectors) or
# copy-on-write. Flink always does DV-style upsert, so MERGE_MODE only varies the
# Spark runs. Folded into the label so mor/cow write distinct result files.
MERGE_MODE="${MERGE_MODE:-merge-on-read}"
BUCKETS="${BUCKETS:-48}"             # concurrent write streams (throughput lever)
LABEL="${ENGINE}_${WRITE}_${PART}"
if [ "$WRITE" = "upsert" ] && [ "$ENGINE" = "spark" ]; then
  case "$MERGE_MODE" in copy-on-write) LABEL="${LABEL}_cow";; *) LABEL="${LABEL}_mor";; esac
fi
mkdir -p ../results/eks
echo "==> $LABEL  parallelism=$PARALLELISM prodpods=$PRODPODS rate=${RATE}/pod win=${RUN_SECONDS}s warmup=${WARMUP}s"

render() { sed -e "s#__REGISTRY__#$REGISTRY#g" -e "s#__TAG__#$TAG#g" \
    -e "s#__BUCKET__#$BUCKET#g" -e "s#__PARALLELISM__#$PARALLELISM#g" \
    -e "s#__MODE__#hash#g" -e "s#__WRITE__#$WRITE#g" -e "s#__PART__#$PART#g" \
    -e "s#__FMT__#$FMT#g" -e "s#__UPSERT__#$UPSERT#g" -e "s#__DUP__#$DUP#g" \
    -e "s#__MERGE_MODE__#$MERGE_MODE#g" -e "s#__BUCKETS__#$BUCKETS#g" \
    -e "s#__PRODPODS__#$PRODPODS#g" -e "s#__RATE__#$RATE#g" \
    -e "s#__PRODUCER_SECONDS__#$PRODUCER_SECONDS#g" "$1"; }

OUT="../results/eks/${LABEL}"
SOURCE_TOPIC="${SOURCE_TOPIC:-events}"
TOPIC_NS=kafka; BROKER=sfi-bench-broker-0
TABLE="events"; [ "$ENGINE" = "spark" ] && TABLE="events_spark"
ktopic() { kubectl -n $TOPIC_NS exec $BROKER -- bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@" 2>/dev/null; }
koffsets() { kubectl -n $TOPIC_NS exec $BROKER -- bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$SOURCE_TOPIC" --time -1 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'; }

# ---- 0. clean per-run table state is handled BY THE ENGINE (RECREATE_TABLE=1 in
#         the job manifests): the job does DROP TABLE ... PURGE + recreate at start,
#         a catalog operation — no gated AWS CLI delete needed. ----

# ---- 1. FRESH TOPIC (identical clean start for every run). PARTITIONS matches the
#         engine parallelism so each subtask/core owns one partition (fair, no skew). ----
PARTITIONS="${PARTITIONS:-12}"
echo "==> resetting topic $SOURCE_TOPIC to $PARTITIONS partitions (clean start)"
kubectl -n kafka delete kafkatopic "$SOURCE_TOPIC" --ignore-not-found >/dev/null 2>&1
ktopic --delete --topic "$SOURCE_TOPIC" >/dev/null 2>&1 || true
sleep 8
# render the topic with the desired partition count (manifest has __PARTITIONS__)
sed "s/__PARTITIONS__/$PARTITIONS/g" manifests/strimzi/kafka-cluster.yaml | kubectl apply -f - >/dev/null 2>&1
for _ in $(seq 1 20); do ktopic --describe --topic "$SOURCE_TOPIC" | grep -q "PartitionCount: $PARTITIONS" && break; sleep 5; done
echo "    topic ready: $(ktopic --describe --topic "$SOURCE_TOPIC" | grep -oE 'PartitionCount: [0-9]+' | head -1)"

# ---- 1a. PURGE per-run S3 state so each run measures ONLY its own output. The
#   engine drops+recreates the table metadata (RECREATE_TABLE=1), but a plain
#   (non-PURGE) drop leaves orphaned data files behind — those would inflate the
#   file-count/byte metric with prior runs' output. We own this benchmark warehouse
#   and the table is recreated every run, so wiping its data+metadata prefix is the
#   correct, safe cleanup. Also clear the checkpoint so 'latest' isn't overridden
#   by a stale restored offset. ----
aws s3 rm "s3://$BUCKET/streaming.db/$TABLE/" --recursive >/dev/null 2>&1 || true
# Also drop the stale Glue table entry so it can't point at a now-deleted
# metadata.json (Iceberg's plain DROP loads current metadata first → 404 if we
# only wiped S3). Dropping it here lets the engine's CREATE TABLE start clean.
aws glue delete-table --database-name "${GLUE_DB:-streaming}" --name "$TABLE" >/dev/null 2>&1 || true
if [ "$ENGINE" = "spark" ]; then
  aws s3 rm "s3://$BUCKET/checkpoints/spark/$TABLE/" --recursive >/dev/null 2>&1 || true
else
  aws s3 rm "s3://$BUCKET/checkpoints/flink/" --recursive >/dev/null 2>&1 || true
fi

# ---- 1b. START PRODUCERS FIRST, then engine consumes from 'latest'. Two reasons:
#   (1) fairness — both engines process the LIVE stream from ~0 backlog;
#   (2) Spark 4.1.2 has a bug in KafkaMicroBatchStream.metrics(): the first
#       micro-batch against a still-empty topic NPEs in progress reporting
#       (latestPartitionOffsets is null). Guaranteeing data before batch 0 avoids it.
render jobs/producer-fleet.yaml | kubectl apply -f -
echo "==> producers started; engine will consume from latest (live stream)"
sleep 12   # let producers ramp so the engine's first batch always has data

# ---- 2. deploy the engine and WAIT until it's RUNNING (consuming from latest) ----
if [ "$ENGINE" = "flink" ]; then
  render jobs/flink.yaml | kubectl apply -f -
  for _ in $(seq 1 40); do
    [ "$(kubectl -n bench get flinkdeployment sfi-flink -o jsonpath='{.status.jobStatus.state}' 2>/dev/null)" = "RUNNING" ] && break; sleep 6
  done
  echo "    flink: $(kubectl -n bench get flinkdeployment sfi-flink -o jsonpath='{.status.jobStatus.state}' 2>/dev/null)"
  kubectl -n bench port-forward svc/sfi-flink-rest 18081:8081 >/tmp/pf_flink.log 2>&1 & PF=$!
  echo "    Flink UI → http://localhost:18081"

else
  render jobs/spark.yaml | kubectl apply -f -
  for _ in $(seq 1 40); do
    [ "$(kubectl -n bench get sparkapplication sfi-spark -o jsonpath='{.status.applicationState.state}' 2>/dev/null)" = "RUNNING" ] && break; sleep 6
  done
  echo "    spark: $(kubectl -n bench get sparkapplication sfi-spark -o jsonpath='{.status.applicationState.state}' 2>/dev/null)"
  kubectl -n bench port-forward svc/sfi-spark-ui-svc 4040:4040 >/tmp/pf_sparkui.log 2>&1 & PF=$!
  echo "    Spark UI → http://localhost:4040"

fi
echo "==> engine deploying; measuring lag for ${RUN_SECONDS}s (+${WARMUP}s warmup)"

# ---- 4. sample the FAIR overload metric every 15s over the window.
#   produced   = Kafka topic end offsets (total rows generated, absolute from 0)
#   landed     = Iceberg current-snapshot total-records (rows DURABLY committed to the
#                table, absolute from 0) — read straight from the engine's OUTPUT, so
#                it's fully engine-agnostic and commit-honest. No offset scraping, no
#                Flink-vs-Spark definitional mismatch, no orphan/cleanup sensitivity
#                (we read only what the current snapshot references).
#   lag        = produced - landed  (grows if the engine can't land rows fast enough)
# Both start from the SAME fresh topic + freshly-created table at 0 → apples-to-apples.
DUR=$(( WARMUP + RUN_SECONDS ))
echo "t_s,produced,landed,lag,snapshots,cpu_cores,mem_mb" > "${OUT}.lag.csv"
S=$(date +%s); END=$(( S + DUR ))
# landed(): current-snapshot total-records from the table's live Iceberg metadata.
# Resolve the metadata.json via the Glue table's metadata_location pointer, then read
# the CURRENT snapshot's summary.total-records. Also emits the snapshot COUNT (2nd line).
landed() {
  local ml
  ml=$(aws glue get-table --database-name "${GLUE_DB:-streaming}" --name "$TABLE" \
        --query 'Table.Parameters.metadata_location' --output text 2>/dev/null)
  [ -z "$ml" ] || [ "$ml" = "None" ] && { echo "0 0"; return; }
  aws s3 cp "$ml" - 2>/dev/null | python3 -c '
import sys,json
try:
    m=json.load(sys.stdin)
except Exception:
    print("0 0"); sys.exit(0)
cur=m.get("current-snapshot-id")
snaps=m.get("snapshots",[]) or []
tot=0
for s in snaps:
    if s.get("snapshot-id")==cur:
        tot=int(s.get("summary",{}).get("total-records",0) or 0)
print(f"{tot} {len(snaps)}")
' 2>/dev/null || echo "0 0"
}
[ "$ENGINE" = "spark" ] && DRIVER=$(kubectl -n bench get pods -l spark-role=driver -o name 2>/dev/null | head -1)
while [ "$(date +%s)" -lt "$END" ]; do
  # NOTE: each assignment is guarded with `|| true` — under `set -euo pipefail` a
  # non-matching grep (exit 1) inside $(...) would otherwise kill the whole loop.
  t=$(( $(date +%s) - S )); prod=$(koffsets || true); prod=${prod:-0}
  land=$(landed || true); ln=${land% *}; snaps=${land#* }; ln=${ln:-0}; snaps=${snaps:-0}
  lag=$(( prod - ln ))
  # worker-pod CPU/mem: Flink TM (name contains 'taskmanager') or Spark executor
  # (name contains '-exec-'). Median-active is computed later in analysis; here we
  # just record the instantaneous sum across worker pods.
  cm=$(kubectl -n bench top pods --no-headers 2>/dev/null | grep -E 'taskmanager|-exec-' | awk '{gsub(/m/,"",$2); gsub(/Mi/,"",$3); c+=$2; m+=$3} END{printf "%.2f,%d", c/1000, m}' || true)
  echo "$t,$prod,$ln,$lag,$snaps,${cm:-,}" >> "${OUT}.lag.csv"
  sleep 15
done

# ---- 5. capture file sizing FROM ICEBERG SNAPSHOT METRICS (engine-agnostic, and
#   immune to orphans/cleanup): the current snapshot's summary carries the exact
#   live data-file count + total bytes the table references. We also emit the
#   per-commit added-files/added-size series so the blog can show file-size
#   distribution over time. Falls back to an S3 listing only if metadata is
#   unreadable. ----
ML=$(aws glue get-table --database-name "${GLUE_DB:-streaming}" --name "$TABLE" \
      --query 'Table.Parameters.metadata_location' --output text 2>/dev/null)
if [ -n "$ML" ] && [ "$ML" != "None" ]; then
  aws s3 cp "$ML" - 2>/dev/null | OUT="$OUT" python3 -c '
import sys,json,os
m=json.load(sys.stdin)
cur=m.get("current-snapshot-id"); snaps=m.get("snapshots",[]) or []
def g(su,k):
    v=su.get(k); return int(v) if v not in (None,"") else 0
curs=next((s for s in snaps if s.get("snapshot-id")==cur), None)
if curs:
    su=curs["summary"]
    files=g(su,"total-data-files"); recs=g(su,"total-records"); bytes_=g(su,"total-files-size")
    avg=(bytes_/files/1e6) if files else 0
    print("files=%d bytes=%d records=%d avg_mb=%.1f snapshots=%d" % (files, bytes_, recs, avg, len(snaps)))
# Per-commit CSV for the blog charts: commit interval (ts delta), rows/files/bytes
# per commit, and avg file size of THAT commit. First snapshot ts is the baseline.
out=os.environ["OUT"]
with open(out+".snapshots.csv","w") as f:
    # per-commit health: + delete/DV files for read-amplification & v3-DV proof
    f.write("idx,ts_ms,interval_s,op,added_records,added_files,added_size,commit_avg_mb,"
            "total_records,total_files,added_delete_files,added_dvs,total_delete_files,total_position_deletes\n")
    prev=None
    for i,s in enumerate(snaps):
        su=s.get("summary",{}); ts=int(s.get("timestamp-ms",0))
        iv=((ts-prev)/1000.0) if prev else 0.0; prev=ts
        af=g(su,"added-data-files"); asz=g(su,"added-files-size")
        cavg=(asz/af/1e6) if af else 0
        f.write("%d,%d,%.1f,%s,%d,%d,%d,%.1f,%d,%d,%d,%d,%d,%d\n"%(
            i,ts,iv,su.get("operation",""),g(su,"added-records"),af,asz,cavg,
            g(su,"total-records"),g(su,"total-data-files"),
            g(su,"added-delete-files"),g(su,"added-dvs"),g(su,"total-delete-files"),g(su,"total-position-deletes")))
' > "${OUT}.files.txt" || true
else
  aws s3 ls "s3://$BUCKET/streaming.db/$TABLE/data/" --recursive 2>/dev/null \
    | awk '{s+=$3;n++} END{printf "files=%d bytes=%d avg_mb=%.1f (s3-fallback)\n",n,s,(n? s/n/1e6:0)}' > "${OUT}.files.txt" || true
fi
cat "${OUT}.files.txt" 2>/dev/null || true

# ---- 5b. ENGINE-NATIVE per-commit timing (for the blog "where does the time go"
#   charts). Flink: checkpoint duration + state size from REST /checkpoints/history.
#   Spark: per-batch durationMs breakdown (addBatch / walCommit / getBatch / etc.)
#   from the driver-log StreamingQueryProgress JSON. Both accumulate over the run. ----
if [ "$ENGINE" = "flink" ]; then
  JID=$(curl -s "http://localhost:18081/jobs" 2>/dev/null | grep -oE '"id":"[a-f0-9]{32}"' | head -1 | cut -d'"' -f4)
  if [ -n "$JID" ]; then
    curl -s "http://localhost:18081/jobs/$JID/checkpoints" 2>/dev/null | python3 -c '
import sys,json
try: d=json.load(sys.stdin)
except Exception: sys.exit(0)
hist=d.get("history",[]) or []
print("idx,ckpt_id,duration_ms,state_size_bytes,status")
for c in hist:
    print("%s,%s,%s,%s,%s"%(c.get("id"),c.get("id"),
        c.get("end_to_end_duration"),c.get("state_size"),c.get("status")))
' > "${OUT}.ckpt.csv" 2>/dev/null || true
    echo "    flink checkpoint stats → ${OUT}.ckpt.csv ($(wc -l < "${OUT}.ckpt.csv" 2>/dev/null || echo 0) rows)"
  fi
else
  # Spark StreamingQueryProgress blocks in the driver log carry durationMs {addBatch,
  # walCommit, getBatch, queryPlanning, triggerExecution, latestOffset} + numInputRows
  # + inputRowsPerSecond per micro-batch. Extract every complete progress JSON.
  kubectl -n bench logs "$DRIVER" 2>/dev/null | python3 -c '
import sys,json,re
txt=sys.stdin.read()
# progress objects are pretty-printed multi-line JSON starting at a line with "id" :
rows=[]
# find each { ... } block that contains "durationMs"
for mobj in re.finditer(r"\{\s*\"id\"\s*:.*?\n\}", txt, re.S):
    blk=mobj.group(0)
    if "durationMs" not in blk: continue
    try: p=json.loads(blk)
    except Exception: continue
    d=p.get("durationMs",{}) or {}
    rows.append(p)
print("batchId,triggerExecution_ms,addBatch_ms,walCommit_ms,getBatch_ms,queryPlanning_ms,numInputRows,inputRowsPerSecond,processedRowsPerSecond")
for p in rows:
    d=p.get("durationMs",{}) or {}
    print("%s,%s,%s,%s,%s,%s,%s,%s,%s"%(
        p.get("batchId"),d.get("triggerExecution"),d.get("addBatch"),
        d.get("walCommit"),d.get("getBatch"),d.get("queryPlanning"),
        p.get("numInputRows"),p.get("inputRowsPerSecond"),p.get("processedRowsPerSecond")))
' > "${OUT}.batches.csv" 2>/dev/null || true
  echo "    spark batch timing → ${OUT}.batches.csv ($(wc -l < "${OUT}.batches.csv" 2>/dev/null || echo 0) rows)"
fi

kill $PF 2>/dev/null || true
kubectl -n bench delete job sfi-producer --ignore-not-found >/dev/null 2>&1
[ "$ENGINE" = "flink" ] && kubectl -n bench delete flinkdeployment sfi-flink --ignore-not-found >/dev/null 2>&1
[ "$ENGINE" = "spark" ] && kubectl -n bench delete sparkapplication sfi-spark --ignore-not-found >/dev/null 2>&1
echo "==> $LABEL done → results/eks/${LABEL}.*"
