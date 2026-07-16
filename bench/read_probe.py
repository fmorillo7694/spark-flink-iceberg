#!/usr/bin/env python3
"""Read-latency probe for Iceberg v3 tables — the query side of the benchmark.

Uses DuckDB (verified to read Iceberg v3 AND honor deletion vectors: scan count =
total-records − position-deletes, exact). Athena can't read v3 DVs; DuckDB can.

Every INTERVAL seconds it times a scan of the table's CURRENT snapshot and records
query latency alongside the delete-file / position-delete debt at that moment, so we
can chart read amplification climbing as the streaming upsert accumulates deletes —
and the recovery after compaction.

DuckDB's REST-catalog ATTACH only supports glue/s3_tables, so we resolve the current
metadata.json from the REST catalog ourselves and iceberg_scan() it directly.

Usage:
  python bench/read_probe.py --table events_spark --seconds 600 --interval 30 \
      --out results/local/read_probe_mor
Env (local defaults): CATALOG_URI_HOST, S3_ENDPOINT_HOST, ICEBERG_DB, AWS keys.
"""
import argparse, json, os, time, threading, urllib.request

import duckdb


def current_metadata_location(catalog_http: str, db: str, table: str) -> str:
    url = f"{catalog_http}/v1/namespaces/{db}/tables/{table}"
    with urllib.request.urlopen(url, timeout=10) as r:
        d = json.load(r)
    # REST returns metadata inline; the physical metadata.json path is metadata-location
    return d.get("metadata-location") or d["metadata"]["metadata-location"]


def snapshot_debt(catalog_http: str, db: str, table: str):
    url = f"{catalog_http}/v1/namespaces/{db}/tables/{table}"
    with urllib.request.urlopen(url, timeout=10) as r:
        m = json.load(r)["metadata"]
    cur = m.get("current-snapshot-id")
    snaps = m.get("snapshots", []) or []
    s = next((x for x in snaps if x.get("snapshot-id") == cur), None)
    su = (s or {}).get("summary", {})
    g = lambda k: int(su.get(k) or 0)
    return dict(total_records=g("total-records"),
                total_position_deletes=g("total-position-deletes"),
                total_delete_files=g("total-delete-files"),
                total_data_files=g("total-data-files"),
                snapshots=len(snaps))


def make_con(s3_endpoint_host: str):
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
    host = s3_endpoint_host.replace("http://", "").replace("https://", "")
    use_ssl = "true" if s3_endpoint_host.startswith("https") else "false"
    con.execute(f"""CREATE SECRET s (TYPE s3,
        KEY_ID '{os.getenv("AWS_ACCESS_KEY_ID","admin")}',
        SECRET '{os.getenv("AWS_SECRET_ACCESS_KEY","password")}',
        ENDPOINT '{host}', URL_STYLE 'path', USE_SSL {use_ssl});""")
    return con


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default=os.getenv("ICEBERG_TABLE", "events_spark"))
    ap.add_argument("--db", default=os.getenv("ICEBERG_DB", "streaming"))
    ap.add_argument("--seconds", type=int, default=600)
    ap.add_argument("--interval", type=int, default=30)
    ap.add_argument("--out", default="results/local/read_probe")
    ap.add_argument("--catalog", default=os.getenv("CATALOG_URI_HOST", "http://localhost:8181"))
    ap.add_argument("--s3", default=os.getenv("S3_ENDPOINT_HOST", "http://localhost:9000"))
    ap.add_argument("--key", default=os.getenv("UPSERT_KEY", "user_id"),
                    help="column to count — must be the equality-delete key for Flink tables")
    ap.add_argument("--scan-timeout", type=float, default=float(os.getenv("SCAN_TIMEOUT", "45")),
                    dest="scan_timeout", help="per-scan timeout (s); records rows=-1 TIMEOUT if exceeded")
    a = ap.parse_args()

    con = make_con(a.s3)
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    path = a.out + ".read.csv"
    with open(path, "w") as f:
        f.write("t_s,scan_ms,rows,total_records,total_position_deletes,"
                "total_delete_files,total_data_files,snapshots\n")
        start = time.time()
        while time.time() - start < a.seconds:
            t = int(time.time() - start)
            try:
                ml = current_metadata_location(a.catalog, a.db, a.table)
                d = snapshot_debt(a.catalog, a.db, a.table)
                # count(<key>) not count(*): Flink writes EQUALITY deletes, which DuckDB
                # refuses to apply for a bare count(*) ("relevant columns must be
                # selected"). Selecting the key column lets the delete-merge run. (Spark
                # MoR uses position deletes / DVs and reads fine either way.)
                q = f"SELECT count({a.key}) FROM iceberg_scan('{ml}')"
                res = {}
                def _run():
                    try:
                        res["rows"] = con.execute(q).fetchone()[0]
                    except Exception as ex:
                        res["err"] = str(ex)[:100]
                t0 = time.time()
                th = threading.Thread(target=_run, daemon=True)
                th.start(); th.join(a.scan_timeout)
                scan_ms = (time.time() - t0) * 1000
                if th.is_alive():
                    rows, scan_ms, note = -1, scan_ms, "TIMEOUT"   # slow/unreadable (Flink eq-deletes)
                elif "err" in res:
                    rows, note = -1, res["err"]
                else:
                    rows, note = res["rows"], "ok"
                f.write("%d,%.1f,%d,%d,%d,%d,%d,%d\n" % (
                    t, scan_ms, rows, d["total_records"], d["total_position_deletes"],
                    d["total_delete_files"], d["total_data_files"], d["snapshots"]))
                f.flush()
                print(f"[{t:4d}s] scan {scan_ms:7.0f} ms  rows={rows:,}  {note}  "
                      f"pos-deletes={d['total_position_deletes']:,}  "
                      f"delete-files={d['total_delete_files']}  snaps={d['snapshots']}")
                if th.is_alive():
                    # a hung scan holds the connection; use a fresh one next tick
                    try: con = make_con(a.s3)
                    except Exception: pass
            except Exception as e:
                print(f"[{t:4d}s] probe error: {str(e)[:120]}")
            time.sleep(a.interval)
    print(f"read-latency series → {path}")


if __name__ == "__main__":
    main()
