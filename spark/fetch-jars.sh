#!/usr/bin/env bash
# Download the Spark runtime jars (Iceberg, Kafka connector, AWS bundle) into
# spark/jars/ so the Docker image can COPY them. Run once before `docker compose
# build spark`. Versions come from ../.env.
set -euo pipefail
cd "$(dirname "$0")"
set -a; source ../.env; set +a

SB="${SCALA_BINARY:-2.13}"
SV="${SPARK_VERSION:-4.1.2}"
IV="${ICEBERG_VERSION:-1.11.0}"
# iceberg-spark-runtime module MUST match Spark's MAJOR.MINOR — derive it from
# SPARK_VERSION so it can't drift. A mismatched runtime (e.g. 4.0 build on Spark
# 4.1.2) throws NoSuchMethodError DataSourceV2Relation.create on expire_snapshots /
# rewrite_manifests / compute_table_stats / DROP...PURGE — a version-pairing bug we
# hit and fixed. iceberg publishes -4.1_, -4.0_, -3.5_ runtimes; pick by SV.
IM="$(echo "$SV" | cut -d. -f1,2)"   # 4.1.2 -> 4.1

mkdir -p jars && cd jars
base=https://repo1.maven.org/maven2
for url in \
  "${base}/org/apache/iceberg/iceberg-spark-runtime-${IM}_${SB}/${IV}/iceberg-spark-runtime-${IM}_${SB}-${IV}.jar" \
  "${base}/org/apache/iceberg/iceberg-aws-bundle/${IV}/iceberg-aws-bundle-${IV}.jar" \
  "${base}/org/apache/spark/spark-sql-kafka-0-10_${SB}/${SV}/spark-sql-kafka-0-10_${SB}-${SV}.jar" \
  "${base}/org/apache/spark/spark-token-provider-kafka-0-10_${SB}/${SV}/spark-token-provider-kafka-0-10_${SB}-${SV}.jar" \
  "${base}/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar" \
  "${base}/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar" \
; do
  f="$(basename "$url")"
  [ -f "$f" ] && { echo "  have $f"; continue; }
  curl -fSL "$url" -o "$f" && echo "  got  $f"
done
echo "spark jars ready in $(pwd)"
