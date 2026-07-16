#!/usr/bin/env bash
# Spark batch maintenance runner — the "you must bolt on a separate job" pattern.
# Runs Iceberg maintenance CALL procedures against a table and TIMES each one, so we
# can measure what paying down the debt costs and how much it shrinks the table.
#
# Ops (each optional via env, all on by default):
#   rewrite_data_files          — compact small data files (bin-pack)
#   rewrite_position_delete_files — compact/merge position-delete + DV files (v3)
#   rewrite_manifests           — compact the manifest layer (planning cost)
#   expire_snapshots            — drop old snapshots (metadata.json + storage)
#
# Runs as a SEPARATE spark-sql job (local[N]) against the same REST catalog + MinIO —
# deliberately NOT inside the streaming job, to mirror Spark's real batch-compaction
# story (contrast with Flink inline). Use bench/compact_concurrency.sh to run it
# WHILE ingest is live.
#
# Usage: bench/compact_spark.sh [table]   (default events_spark)
# Env: CORES=4 MEM_GB=6  DO_DATA=1 DO_DELETES=1 DO_MANIFESTS=1 DO_EXPIRE=1
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; source .env 2>/dev/null; set +a

TABLE="${1:-events_spark}"
FQ="demo.${ICEBERG_DB:-streaming}.${TABLE}"
CORES="${CORES:-4}"; MEM_GB="${MEM_GB:-6}"
OUT="results/local/compact_${TABLE}"
mkdir -p results/local

# snapshot the table's "before" health from the REST catalog
before() {
  curl -s "http://localhost:8181/v1/namespaces/${ICEBERG_DB:-streaming}/tables/${TABLE}" 2>/dev/null | python3 -c '
import sys,json
try: m=json.load(sys.stdin)["metadata"]
except Exception: print("0 0 0 0"); sys.exit(0)
cur=m.get("current-snapshot-id"); snaps=m.get("snapshots",[]) or []
s=next((x for x in snaps if x.get("snapshot-id")==cur),{}); su=s.get("summary",{})
g=lambda k:int(su.get(k) or 0)
print(g("total-data-files"), g("total-delete-files"), g("total-position-deletes"), len(snaps))'
}

echo "==> Spark batch maintenance on $FQ"
read -r df0 delf0 pd0 sn0 <<<"$(before)"
echo "    BEFORE: data-files=$df0 delete-files=$delf0 position-deletes=$pd0 snapshots=$sn0"

# build the maintenance SQL (only the enabled ops), each wrapped so we can time it
# NOTE: the `table =>` arg is the catalog-relative identifier WITHOUT the catalog
# prefix (the CALL already targets demo.system) — i.e. 'streaming.events_spark'
# is WRONG (parsed as catalog `streaming`); Iceberg wants the db.table only, but on
# this REST catalog the procedure resolves against the demo catalog, so pass the
# full 'streaming.events_spark' as the db-qualified name via the demo catalog by
# using the 3-part name is rejected — use db.table and it resolves in demo.
# All four maintenance procedures WORK provided the iceberg-spark-runtime matches the
# Spark minor (we run Spark 4.1.x → runtime-4.1_2.13; see spark/fetch-jars.sh, which
# derives it from SPARK_VERSION). With the mismatched 4.0 runtime they threw
# NoSuchMethodError DataSourceV2Relation.create — that was a jar-pinning bug, not a
# Spark limitation. compute_table_stats (Puffin stats) also works on the matched runtime.
TBL="${ICEBERG_DB:-streaming}.${TABLE}"
SQL=""
[ "${DO_DATA:-1}" = "1" ]      && SQL+="CALL demo.system.rewrite_data_files(table => '${TBL}', options => map('min-input-files','2'));"$'\n'
[ "${DO_DELETES:-1}" = "1" ]   && SQL+="CALL demo.system.rewrite_position_delete_files(table => '${TBL}');"$'\n'
[ "${DO_MANIFESTS:-1}" = "1" ] && SQL+="CALL demo.system.rewrite_manifests(table => '${TBL}');"$'\n'
# expire_snapshots NEVER removes the current snapshot or anything it references — it
# only drops OLD (non-current) snapshots from history + files no longer reachable from
# any retained snapshot. retain_last keeps at least N most-recent (a rollback/time-
# travel window). Default 10 (realistic); set EXPIRE_RETAIN_LAST=1 for max cleanup
# (drops ALL history — no time travel). Prefer older_than in production.
[ "${DO_EXPIRE:-1}" = "1" ]    && SQL+="CALL demo.system.expire_snapshots(table => '${TBL}', retain_last => ${EXPIRE_RETAIN_LAST:-10});"$'\n'

# Compaction Spark config (same catalog as ingest).
SPARK_ARGS=(/opt/spark/bin/spark-sql --master "local[$CORES]" --driver-memory "${MEM_GB}g"
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.demo.type=rest
  --conf spark.sql.catalog.demo.uri=http://iceberg-rest:8181
  --conf spark.sql.catalog.demo.warehouse=s3://${WAREHOUSE_BUCKET:-warehouse}/
  --conf spark.sql.catalog.demo.io-impl=org.apache.iceberg.aws.s3.S3FileIO
  --conf spark.sql.catalog.demo.s3.endpoint=http://minio:9000
  --conf spark.sql.catalog.demo.s3.path-style-access=true
  -e "$SQL")
ENVS=(-e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-admin}" -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-password}" -e AWS_REGION="${AWS_REGION:-us-east-1}")

T0=$(date +%s)
if [ "${COMPACT_ISOLATED:-0}" = "1" ]; then
  # ISOLATED: run in a SEPARATE transient container (own cores/mem) on the same
  # network, so compaction compute doesn't contend with the live ingest container.
  # This mirrors production (compaction = separate job/cluster) and avoids the
  # single-shared-container deadlock. The container name lets the caller sample its
  # stats; --rm cleans up; we can `docker kill` it to enforce a timeout.
  CNAME="sfi-compactor-${TABLE}"
  docker rm -f "$CNAME" >/dev/null 2>&1 || true
  IMG=$(docker inspect spark --format '{{.Config.Image}}' 2>/dev/null || echo spark-flink-iceberg-spark)
  docker run -d --name "$CNAME" --network sfi-bench "${ENVS[@]}" --entrypoint /bin/bash "$IMG" \
    -c "$(printf '%q ' "${SPARK_ARGS[@]}")" > /dev/null 2>&1
  # wait for it with a hard timeout that actually kills the container
  for _ in $(seq 1 "${COMPACT_TIMEOUT:-180}"); do
    docker ps --filter "name=$CNAME" --format '{{.Names}}' | grep -q "$CNAME" || break
    sleep 1
  done
  docker logs "$CNAME" > "${OUT}.compact.log" 2>&1 || true
  RC=$(docker inspect "$CNAME" --format '{{.State.ExitCode}}' 2>/dev/null || echo 1)
  docker rm -f "$CNAME" >/dev/null 2>&1 || true
else
  # shared: exec into the persistent spark container (fine when NOT concurrent w/ ingest)
  docker exec -i "${ENVS[@]}" spark "${SPARK_ARGS[@]}" > "${OUT}.compact.log" 2>&1 &
  CPID=$!
  wait $CPID; RC=$?
fi
DUR=$(( $(date +%s) - T0 ))

read -r df1 delf1 pd1 sn1 <<<"$(before)"
echo "    AFTER : data-files=$df1 delete-files=$delf1 position-deletes=$pd1 snapshots=$sn1"
echo "    took ${DUR}s (rc=$RC)"
{
  echo "table,phase,data_files,delete_files,position_deletes,snapshots,duration_s"
  echo "$TABLE,before,$df0,$delf0,$pd0,$sn0,"
  echo "$TABLE,after,$df1,$delf1,$pd1,$sn1,$DUR"
} > "${OUT}.compact.csv"
echo "==> maintenance result → ${OUT}.compact.csv (+ .compact.log)"
[ "$RC" != 0 ] && { echo "    NOTE rc=$RC — check ${OUT}.compact.log"; tail -5 "${OUT}.compact.log"; }
