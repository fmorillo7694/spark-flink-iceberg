# Part 2 scope — Compaction & table health: keeping a streaming Iceberg table fast

**Working title:** *Streaming Data Lake, Part 2 — the table gets slow: compaction,
deletion-vector debt, and two writers on one table.*

## The hook (carries over from Part 1)

Part 1 ended on a finding: Spark MERGE upsert can't keep up because every commit
**re-reads a growing target** and writes more delete files. We proved this with the
health metrics — `total_position_deletes` climbs every commit (897 → 241k in a few
minutes locally), and even Storage-Partitioned Join (which correctly halves the
shuffle) can't help, because the shuffle was never the bottleneck. The target
itself is the bottleneck. **That's a compaction problem.** Part 2 is about the tool
that actually moves the needle here.

This also reframes Flink: its write-only upsert *looks* free in Part 1, but it
pushes the cost to **readers** (who merge deletion vectors at scan time) and to
**future compaction**. Nobody escapes delete-file debt — they just pay at different
times. Part 2 makes that debt visible and measures paying it down.

## The questions to answer

1. **What does the debt cost?** Chart read-amplification over a run: position-deletes
   / DV count growing per commit, and (if we add a read probe) query latency creeping
   up as deletes accumulate. This is the "why you can't ignore compaction" evidence.
2. **Spark batch compaction** — `rewrite_data_files` (+ `rewrite_position_delete_files`
   / DV rewrite). Run as a separate job. Measure: how long, how much it shrinks the
   table, and the read-latency recovery after.
3. **Flink inline / scheduled compaction** — Flink's post-commit compaction
   (`RewriteDataFiles` action / table-maintenance). Measure the same, but crucially:
   **what does compacting *inside* the streaming job do to ingest throughput?** Does
   the compactor steal slots/CPU from the writer and cause the ingest to fall behind?
4. **Concurrency — the production landmine.** Two committers on one table at once:
   the streaming writer *and* a compaction job. Iceberg's optimistic concurrency +
   snapshot isolation means one loses and retries. Measure: commit-conflict/retry
   rate, whether ingest stalls, and whether either engine handles it more gracefully.
5. **What's missing in each engine?** Flink lacks mature batch-grade rewrite tuning;
   Spark lacks in-job/streaming compaction (you bolt on a separate job + scheduler).
   Name the gaps honestly.

## Matrix (local-first, then EKS)

| Dimension | Values |
|---|---|
| Base workload | Spark MoR upsert + Flink DV upsert (the two that accumulate deletes) |
| Compaction | none (baseline debt) · Spark batch rewrite · Flink inline/scheduled |
| Concurrency | compaction alone · compaction **during** live ingest |
| Metric | read-amp curve, file/delete counts (have), + rewrite duration, retry/conflict rate, ingest-throughput-during-compaction, post-compaction read latency |

## The metadata-explosion angle (the OTHER disaster, often the first to bite)

Data-file bloat is the obvious debt. But streaming into Iceberg =  a commit every
60s = **thousands of snapshots/day**, and the *metadata* layer degrades before the
data layer does. Things that quietly make streaming-into-Iceberg a disaster, all
measurable from the metadata tree:

- **Snapshot count** grows unbounded without `expire_snapshots` — every commit adds
  one; the metadata.json carries the whole log.
- **metadata.json size** itself balloons (snapshot log + schema/partition history +
  properties) — it's rewritten *in full* on every commit, so a fat metadata.json
  taxes every single write. Chart its byte size over the run.
- **Manifest list + manifest count**: `manifests-created` vs `manifests-kept` per
  commit (we can read these from the summary). Without rewrite, planning a query
  means opening hundreds/thousands of manifests.
- **Small-files at the manifest level**, not just data: many tiny manifests →
  slow query planning even when data files are fine.
- **Delete-file / DV accumulation** (already captured) — read amplification.
- **Orphan files & expired-but-unreferenced data** piling in storage.
- **`total-data-files` count** vs live records — the planning-cost proxy.

Metrics to add for this: per-commit `metadata_json_bytes` (stat the file),
`manifests_created`/`manifests_kept`, running snapshot count, manifest-list size.
Maintenance ops to test against them: `expire_snapshots`, `rewrite_manifests`,
`remove_orphan_files` — and whether Flink vs Spark expose/automate each.

Narrative: "You set up streaming into Iceberg, it works for an hour, then queries
get slow and commits get slower — and it's not your data, it's the metadata."

## What we already have vs need to build

- ✅ Health metrics: `added_dvs`, `total_delete_files`, `total_position_deletes`,
  file-size dist, target growth — per commit, engine-agnostic (done in Part 1 tooling).
- ✅ **Read-latency probe** — `bench/read_probe.py` (DuckDB 1.4.5, VERIFIED reads v3 +
  honors deletion vectors: scan = total-records − position-deletes, exact). Times a
  scan each interval + records delete debt. Validated on the live MoR table.
- ✅ **Metadata-size probe** — `bench/metadata_probe.py` (boto3 head_object): metadata.json
  bytes, snapshot count, manifests-created/kept, file counts per interval. Validated.
- ⬜ Spark `rewrite_data_files` + `rewrite_manifests` + `expire_snapshots` runner + timing.
- ⬜ Flink compaction action wired into (or alongside) the streaming job.
- ⬜ Concurrency harness: run compaction while producers + streaming writer are live;
  capture commit conflicts from snapshot history.

## Honest framing to preserve

- Compaction helps **both** engines; it's not a Spark-vs-Flink win, it's a
  "streaming-into-a-table tax everyone pays" story.
- The Spark-vs-Flink gap from Part 1 (MERGE read-modify-write vs write-only) is
  orthogonal to compaction — compaction shrinks the target for both, but Spark still
  re-reads it each MERGE. Don't let Part 2 imply compaction closes the Part 1 gap.
- SPJ footnote: engages, halves shuffle, doesn't help throughput here — a clean
  "optimize the right bottleneck" lesson. (autospj = engine-side auto-invocation.)
