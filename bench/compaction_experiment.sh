#!/usr/bin/env bash
# Part 2 centerpiece — the compaction lifecycle for a streaming Iceberg upsert table.
#
# Phases (Spark MoR upsert into a v3 table, local):
#   1. INGEST + build debt  — run the streaming upsert; producers feed it.
#   2. PROBE (debt)         — read-latency + metadata probes watch amplification climb.
#   3. COMPACT (concurrent) — run bench/compact_spark.sh WHILE ingest is still live,
#                             to expose write+compaction commit CONCURRENCY conflicts.
#   4. PROBE (recovered)    — read-latency after compaction: does query time drop?
#
# Produces: read.csv (latency vs debt over the whole run), meta.csv (metadata growth),
# compact.csv (before/after file counts), and the streaming lag/health CSVs.
#
# Usage: bench/compaction_experiment.sh [ingest_seconds]   (default 420 = 7 min)
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; source .env 2>/dev/null; set +a
PY=.venv/bin/python
TABLE=events_spark
INGEST_S="${1:-420}"
COMPACT_AT="${COMPACT_AT:-300}"   # start compaction this many seconds into ingest (while live)
OUTDIR=results/local/compaction_experiment
mkdir -p "$OUTDIR"

echo "==> COMPACTION LIFECYCLE experiment (MoR, ${INGEST_S}s ingest, compact @ ${COMPACT_AT}s)"

# ---- 1+2. start streaming ingest (SPJ off = the honest MoR baseline) + probes ----
# run the streaming upsert in the background via the existing local runner
SPJ=0 RUN_SECONDS="$INGEST_S" WARMUP=30 bash bench/run_local_bench.sh spark upsert merge-on-read \
  > "$OUTDIR/ingest.log" 2>&1 &
INGEST_PID=$!
echo "    ingest started (pid $INGEST_PID); waiting for table to exist…"
for _ in $(seq 1 30); do
  curl -s "http://localhost:8181/v1/namespaces/${ICEBERG_DB:-streaming}/tables/${TABLE}" 2>/dev/null | grep -q current-snapshot-id && break
  sleep 4
done

# read-latency + metadata probes for the whole ingest window (+ a bit past compaction)
$PY bench/read_probe.py     --table "$TABLE" --seconds "$((INGEST_S+40))" --interval 20 --out "$OUTDIR/lifecycle" > "$OUTDIR/read.log" 2>&1 &
RP=$!
$PY bench/metadata_probe.py --table "$TABLE" --seconds "$((INGEST_S+40))" --interval 20 --out "$OUTDIR/lifecycle" > "$OUTDIR/meta.log" 2>&1 &
MP=$!

# ---- 3. concurrent compaction while ingest is still running ----
sleep "$COMPACT_AT"
echo "==> [t≈${COMPACT_AT}s] launching compaction WHILE ingest is live (concurrency test)"
DO_DATA=1 DO_DELETES=1 bash bench/compact_spark.sh "$TABLE" > "$OUTDIR/concurrent_compact.log" 2>&1 || true
echo "    concurrent compaction done — checking for commit conflicts…"
# Iceberg logs commit retries/conflicts on the writer side; surface them
grep -icE 'CommitFailedException|conflict|retry|ValidationException' "$OUTDIR/ingest.log" 2>/dev/null | xargs echo "    writer conflict/retry mentions in ingest log:"

# ---- 4. let ingest + probes finish (probe past compaction captures recovery) ----
wait $INGEST_PID 2>/dev/null || true
wait $RP $MP 2>/dev/null || true

echo "==> experiment complete → $OUTDIR/"
echo "    lifecycle.read.csv  (query latency vs delete debt, spans compaction)"
echo "    lifecycle.meta.csv  (metadata.json bytes, snapshots, manifests)"
echo "    concurrent_compact.log (before/after + any conflicts)"
# quick read-latency before/after compaction summary
$PY - "$OUTDIR/lifecycle.read.csv" "$COMPACT_AT" <<'PYEOF' 2>/dev/null || true
import csv,sys
rows=list(csv.DictReader(open(sys.argv[1]))); cut=int(sys.argv[2])
pre=[r for r in rows if int(r["t_s"])<cut]; post=[r for r in rows if int(r["t_s"])>=cut]
def avg(rs,k):
    v=[float(r[k]) for r in rs if r[k]]; return sum(v)/len(v) if v else 0
if pre and post:
    print("    read latency  pre-compaction avg %.0f ms  |  post %.0f ms" % (avg(pre,"scan_ms"),avg(post,"scan_ms")))
    print("    position-deletes  pre %s  |  post %s" % (pre[-1]["total_position_deletes"], post[-1]["total_position_deletes"]))
PYEOF
