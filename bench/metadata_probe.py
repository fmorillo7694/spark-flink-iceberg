#!/usr/bin/env python3
"""Metadata-explosion probe — the OTHER way streaming into Iceberg falls over.

Data-file bloat is the obvious debt. But a commit every 60s = thousands of
snapshots/day, and the METADATA layer degrades first: metadata.json is rewritten in
full on every commit, the snapshot log grows unbounded, and manifests pile up so
query planning slows even when data files are fine.

This probe samples, every INTERVAL seconds, from the live catalog + object store:
  - metadata.json size in bytes (rewritten every commit — taxes every write)
  - snapshot count (grows without expire_snapshots)
  - manifests created vs kept on the latest commit
  - total data + delete files (planning cost proxy)

Reads the current metadata.json via the REST catalog's metadata-location, then stats
that object over S3/MinIO. Engine-agnostic; no query engine needed.

Usage: python bench/metadata_probe.py --table events_spark --seconds 600 \
         --interval 30 --out results/local/meta_probe_mor
"""
import argparse, json, os, time, urllib.request

import boto3
from botocore.config import Config


def fetch_metadata(catalog_http, db, table):
    url = f"{catalog_http}/v1/namespaces/{db}/tables/{table}"
    with urllib.request.urlopen(url, timeout=10) as r:
        d = json.load(r)
    return d.get("metadata-location") or d["metadata"]["metadata-location"], d["metadata"]


def s3_client(endpoint):
    return boto3.client(
        "s3", endpoint_url=endpoint,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "admin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}))


def s3_size(s3, uri):
    # s3://bucket/key
    b, k = uri[5:].split("/", 1)
    return s3.head_object(Bucket=b, Key=k)["ContentLength"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default=os.getenv("ICEBERG_TABLE", "events_spark"))
    ap.add_argument("--db", default=os.getenv("ICEBERG_DB", "streaming"))
    ap.add_argument("--seconds", type=int, default=600)
    ap.add_argument("--interval", type=int, default=30)
    ap.add_argument("--out", default="results/local/meta_probe")
    ap.add_argument("--catalog", default=os.getenv("CATALOG_URI_HOST", "http://localhost:8181"))
    ap.add_argument("--s3", default=os.getenv("S3_ENDPOINT_HOST", "http://localhost:9000"))
    a = ap.parse_args()

    s3 = s3_client(a.s3)
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    path = a.out + ".meta.csv"
    with open(path, "w") as f:
        f.write("t_s,metadata_json_bytes,snapshot_count,manifests_created,"
                "manifests_kept,total_data_files,total_delete_files\n")
        start = time.time()
        while time.time() - start < a.seconds:
            t = int(time.time() - start)
            try:
                ml, m = fetch_metadata(a.catalog, a.db, a.table)
                mbytes = s3_size(s3, ml)
                snaps = m.get("snapshots", []) or []
                cur = m.get("current-snapshot-id")
                cs = next((x for x in snaps if x.get("snapshot-id") == cur), {})
                su = cs.get("summary", {})
                g = lambda k: int(su.get(k) or 0)
                f.write("%d,%d,%d,%d,%d,%d,%d\n" % (
                    t, mbytes, len(snaps), g("manifests-created"), g("manifests-kept"),
                    g("total-data-files"), g("total-delete-files")))
                f.flush()
                print(f"[{t:4d}s] metadata.json={mbytes/1024:.1f} KB  snapshots={len(snaps)}  "
                      f"manifests +{g('manifests-created')}/keep {g('manifests-kept')}  "
                      f"data-files={g('total-data-files')}")
            except Exception as e:
                print(f"[{t:4d}s] meta probe error: {str(e)[:120]}")
            time.sleep(a.interval)
    print(f"metadata-growth series → {path}")


if __name__ == "__main__":
    main()
