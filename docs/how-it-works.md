# How the Comparator Works

## 📚 Documentation Navigation
| [🏠 Home](../README.md) | [📋 Use Cases](use-cases.md) | [🔍 Comparison Modes](comparison-modes.md) | [⚙️ Configuration](configuration.md) | [🏗️ Architecture](architecture.md) | [📋 Reference](reference.md) |
|---|---|---|---|---|---|

---

This document describes **how** a comparison run behaves: what is scanned, how records are matched, and how the optional extras (date filters, set mapping, batch lookups) change that picture.

It is not a flag catalog. For *when* to pick a mode, see [Comparison Modes](comparison-modes.md). For YAML and flags, see [Configuration](configuration.md) and [Quick Reference](reference.md).

## The big picture

The comparator treats each `--hostsN` (or `clusters:` YAML entry) as a **side**. Sides are usually different clusters, but they can be the same cluster listed twice (for example to compare two sets).

A typical `scan` does this:

1. Connect to every side.
2. For each requested **namespace**, then each requested **set** (or the whole namespace if no set list), compare that data on all sides.
3. Namespaces and named sets are handled **one after another**, not in parallel.
4. Differences are written to the console and/or a CSV file. Optional actions (`scan_touch`, and so on) run against those differences.

`--rps` is a **cluster-wide** records-per-second budget for the scan, split across worker threads. It is not per node.

## Partition scans (the default path)

Aerospike stores data in **4,096 partitions**. In `MISSING_RECORDS`, `RECORDS_DIFFERENT`, `RECORD_DIFFERENCES`, and `FIND_OVERLAP`, the comparator walks those partitions (or a `--startPartition` / `--endPartition` / `--partitionList` subset).

- Each worker thread takes **one partition at a time**.
- On every side it reads that partition in **digest order** (the natural order of Aerospike keys).
- It then **merges** those streams, like merging sorted lists: the same digest on every side is “the same record”; a digest that appears on only some sides is missing (or an overlap, depending on mode).

That merge is why two sides with the same records finish in step even when node layouts differ: matching is by key digest, not by which node owns the partition.

### What each scan mode actually compares

| Mode | What is read from the partition query | How a match is decided | What gets reported |
|------|----------------------------------------|------------------------|--------------------|
| `MISSING_RECORDS` | Keys / digests only (no bin data) | Same digest present on every side? | Records that exist on some sides but not others |
| `FIND_OVERLAP` | Keys / digests only | Same digest present on **every** side | Records that exist on all sides (the inverse of missing) |
| `RECORDS_DIFFERENT` | Full records | Digests aligned, then record **contents** compared | Missing records, plus records whose contents differ — not *which* fields |
| `RECORD_DIFFERENCES` | Full records | Same as above, with a field-by-field diff | Missing records, plus paths/values that differ (or bin names only with `--binsOnly`) |

`RECORDS_DIFFERENT` and `RECORD_DIFFERENCES` cost more because every scanned record’s bins travel to the comparator. `MISSING_RECORDS` and `FIND_OVERLAP` stay cheap: they only need identity.

If `--showMetadata` is on (or `--masterCluster` is set), **difference rows** trigger an extra metadata read (last update time, size, generation, TTL). Matched records are not re-read for metadata.

## Quick namespace (no record walk)

`QUICK_NAMESPACE` does **not** scan records.

It asks each cluster for **partition statistics** (record counts and tombstones) and compares those counts. You learn *which partitions* disagree, not *which keys*.

Consequences:

- Fast, low load, no set list, no date filter.
- Cannot be used with `--sourceCluster` / set mapping.
- A follow-up scan with `--partitionList` is the usual way to inspect the partitions it flags.

## Date filters and batch verification

With `--beginDate` and/or `--endDate`, the partition query only returns records whose last update falls in that window.

That creates an ambiguity: a digest seen on cluster A but not B might be **truly missing** on B, or it might exist on B with an older (or newer) last-update time outside the window.

By default the comparator **verifies** those cases:

1. It remembers keys that looked missing during the filtered scan.
2. When a small batch has accumulated (`--lookupBatchSize`, default 100), it re-reads those keys on the sides that missed them — **without** the date filter.
3. If the record is there, it is treated as present (and compared, in content modes). If it is still absent, it is reported missing.

Those follow-up reads are **batch** operations against the node that owns the partition:

- Existence-only modes use **batch exists**.
- Content modes use **batch get**.

`--skipDateRangeVerify` skips this second pass: you only see what the filtered scans returned, which is faster but will call “missing” any record that simply fell outside the window on one side.

## Set mapping (source-driven scans)

Normal scans query the **same logical set name** on every side (after namespace mapping). **Set mapping** is different: the data lives under **another set name** on some sides (rename, copy, or two sets on one cluster).

With `setMapping` in the config and `--sourceCluster`:

1. Only the **source** side is partition-scanned (the set you passed in `--setNames`).
2. For each source record, the comparator builds a key for the **mapped** set on the other side(s) and looks those keys up in **batches** (same `--lookupBatchSize` idea).
3. Digests **include the set name**, so the lookup must rebuild the key. That requires the **user key to be stored** on the source record (`sendKey = true` at write time). Records without a stored key are skipped.

This is **one-directional**. Records that exist only on the destination set are never seen, because that set is not scanned. Use this when the source set is the system of record you want to check *into* another set.

`QUICK_NAMESPACE` cannot use set mapping. Other scan modes can: missing vs content still applies, but identity on the destination comes from batch lookup instead of a parallel partition walk.

Comparing two sets on the **same** cluster uses this path: configure the cluster twice, then map the second side to the other set by `clusterIndex`. See [Same-cluster set comparison](use-cases.md#10-same-cluster-set-comparison).

Records written without a stored user key are skipped. Progress lines and the scan summary report `skipped (no user key)` so a run over keyless data is not mistaken for “no differences”. The per-record warning still requires `--verbose`.

## Namespace mapping

If the same logical data uses different **namespace** names on different clusters, `namespaceMapping` only changes which namespace is queried or looked up on each side. Matching is still by digest (or by rebuilt key when set mapping is also on). Both sides are still scanned unless set mapping is active.

## Multiple namespaces and sets

Without a set list, each namespace is one scan of all sets in that namespace. With `--setNames a,b`, each `(namespace, set)` pair is its own scan.

Progress lines for a **single** namespace (no set list) stay in the classic form. With several namespaces or several named sets, progress includes which unit is running (`Namespace test2 [2/5]`), time for this scan vs the whole run, and cumulative record counts.

## Follow-up runs (files, not partitions)

`touch`, `read`, `custom`, and `rerun` do not walk partitions. They read keys from a previous CSV (`--inputFile`) and act on those keys.

`rerun` compares those keys again (exists or full read, according to compare mode). That is useful after a repair, without repeating a full namespace scan.

## Remote sides

If one side is reached through a **remote comparator** (`remote:host:port`), the controller still drives the same modes. Record-level compares can send **hashes** instead of full bins across that link, and only fetch full records when hashes disagree. The logical result is the same; only the network path changes. See [Architecture](architecture.md).

## Putting it together

| You want to… | What the tool does |
|--------------|--------------------|
| Check volume quickly | Compare partition counts (`QUICK_NAMESPACE`) |
| Check every key exists | Parallel partition walks, digest merge (`MISSING_RECORDS`) |
| Check contents | Same walk, but bins are read and compared |
| Check a time window | Filtered walk, then **batch** verify apparent misses |
| Check a renamed / other set | Scan source partitions, **batch** lookup mapped keys |
| Check two sets on one cluster | Same as set mapping; both “sides” are that cluster |
| Re-check yesterday’s CSV | Key list from file (`rerun` / `touch` / `read`) |

---

**Next:** [Comparison Modes](comparison-modes.md) for choosing a mode, or [Use Cases](use-cases.md) for worked examples.
