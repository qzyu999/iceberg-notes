# Iceberg Operations: Java vs. PyIceberg vs. Pluggable Interface

Last updated: 2026-07-05
Branch: `pluggable-backend-discovery` (commit `4c2f06e2`)

---

## Quick Reference

| Status | Meaning |
|:---:|---|
| ✅ | Fully implemented and working |
| ⚠️ | Partial / has limitations |
| ❌ | Not implemented |
| 🔌 | Enabled/improved by the pluggable interface |

---

## 1. Read Operations

| # | Operation | Java Iceberg | PyIceberg (`main`) | PyIceberg + Pluggable | Issues/PRs | Notes |
|:---:|---|:---:|:---:|:---:|---|---|
| 1.1 | **Scan to materialized table** | ✅ | ✅ | ✅ 🔌 | — | Pluggable adds: OOM warning, try/catch, parallel execution |
| 1.2 | **Scan with limit** | ✅ | ✅ | ✅ 🔌 | — | Pluggable fixes: O(batch) instead of O(full_scan). Generator breaks early. |
| 1.3 | **Scan streaming (batch reader)** | ✅ | ✅ | ✅ 🔌 | — | Pluggable adds: parallel task execution via ExecutorFactory |
| 1.4 | **Positional delete resolution** | ✅ | ✅ (OOM-prone) | ✅ 🔌 | — | **main** loads ALL delete arrays upfront. Pluggable: per-file streaming via `apply_positional_deletes` |
| 1.5 | **Equality delete resolution** | ✅ | ❌ `ValueError` | ✅ 🔌 | [#3270](https://github.com/apache/iceberg-python/issues/3270), [#1210](https://github.com/apache/iceberg-python/issues/1210) | **Pluggable enables this entirely.** Anti-join via `anti_join_from_files` with bounded memory. Planner now adds eq deletes to DeleteFileIndex instead of raising. |

> **What are positional/equality deletes?** These are the READ side of Merge-on-Read (MoR).
> When a table uses MoR mode (common in Flink/Spark streaming), deletes don't rewrite data
> files. Instead, a separate "delete file" is written that says which rows are logically deleted.
> At scan time, the reader must RESOLVE these deletes (exclude deleted rows from results).
> - **Positional deletes:** Delete file contains `(file_path, row_position)` pairs. "Row 42 in file X is deleted."
> - **Equality deletes:** Delete file contains key values. "Any row where `user_id = 123` is deleted."
> The pluggable interface handles BOTH — see §1.4 and §1.5 deep dives.
| 1.6 | **Schema evolution on read** | ✅ | ✅ (inside ArrowScan) | ✅ 🔌 | — | Pluggable: `_to_requested_schema` applied in orchestration layer per-task |
| 1.7 | **Row count (metadata fast path)** | ✅ | ✅ | ✅ 🔌 | — | Pluggable: streaming count for tasks with deletes/filters (no materialization) |
| 1.8 | **Deletion Vector resolution (V3)** | ✅ | ❌ | ❌ | [#1818](https://github.com/apache/iceberg-python/issues/1818) | Not implemented in PyIceberg. Pluggable architecture ready (new branch in `orchestrate_scan`). |

### 1.5 Deep Dive: Equality Delete Resolution

**Pain point:** Tables written by Flink/Spark with MoR (merge-on-read) have equality delete files. PyIceberg on `main` raises `ValueError("PyIceberg does not yet support equality deletes")` — making these tables UNREADABLE.

**What the pluggable interface does:**
1. `ManifestGroupPlanner.plan_files()` — changed `raise ValueError` → `delete_index.add_delete_file(entry)`. Equality delete files are now assigned to data files in scan tasks.
2. `orchestrate_scan()` — detects `DataFileContent.EQUALITY_DELETES` on a task's delete files, calls `backends.compute.anti_join_from_files(data_paths, delete_paths, equality_columns)`.
3. DataFusion backend — performs a LEFT ANTI JOIN with IS NOT DISTINCT FROM (spec-correct NULL semantics) using Grace Hash Join with spill-to-disk.

**Memory:** O(512 MB) regardless of data/delete file sizes.

### 1.4 Deep Dive: Positional Delete Resolution

**Pain point on `main`:** `_read_all_delete_files()` loads ALL position arrays for the ENTIRE scan into memory before processing any data. For scans touching many files with positional deletes, this OOMs.

**Pluggable fix:** `apply_positional_deletes(data_path, position_delete_paths)` resolves deletes per-file. Only one file's positions are in memory at a time.

---

## 2. Write Operations

| # | Operation | Java Iceberg | PyIceberg (`main`) | PyIceberg + Pluggable | Issues/PRs | Notes |
|:---:|---|:---:|:---:|:---:|---|---|
| 2.1 | **Append (pa.Table, unpartitioned)** | ✅ | ✅ | ✅ 🔌 | — | Pluggable adds: sort-on-write via `_apply_sort_order` |
| 2.2 | **Append (pa.Table, partitioned)** | ✅ | ✅ | ✅ 🔌 | — | Same. Partition routing in `_dataframe_to_data_files`. |
| 2.3 | **Append (RecordBatchReader, unpartitioned)** | ✅ | ✅ | ✅ | — | Streaming write via `bin_pack_record_batches`. Works on main. |
| 2.4 | **Append (RecordBatchReader, partitioned)** | ✅ | ❌ `NotImplementedError` | ❌ | [#2152](https://github.com/apache/iceberg-python/issues/2152) (closed — partial) | Requires streaming partition router (fanout writer). NOT solved by pluggable interface — orthogonal write infrastructure. |
| 2.5 | **Sort-order enforcement on write** | ✅ (Spark SortExec) | ❌ | ✅ 🔌 | [#271](https://github.com/apache/iceberg-python/issues/271) | **Pluggable enables this.** `_apply_sort_order` uses DataFusion external merge sort (bounded memory). |
| 2.6 | **Overwrite (full or filtered)** | ✅ | ✅ | ✅ 🔌 | — | Pluggable adds: sort-on-write for the append phase. Delete phase uses pluggable read. |
| 2.7 | **Dynamic partition overwrite** | ✅ (atomic) | ⚠️ (2 commits) | ⚠️ 🔌 | — | Still 2 commits (delete + append). Pluggable improves the delete phase (streaming CoW). Atomic ReplacePartitions needs upstream commit protocol. |
| 2.8 | **MoR delete file write** | ✅ | ❌ | ❌ | [#1078](https://github.com/apache/iceberg-python/issues/1078) | Write a positional/equality delete file (instead of rewriting data). Not implemented. See §3.3 deep dive. Pluggable execution is trivial — blocker is `row_delta` commit protocol. |

### 2.4 Deep Dive: Streaming Partitioned Writes

**Pain point:** `table.append(record_batch_reader)` on a partitioned table raises `NotImplementedError`. Users must materialize the full reader as `pa.Table` first — defeating the purpose of streaming.

**Why the pluggable interface doesn't help:** The problem is partition ROUTING, not computation. You need to hash each batch by partition key and route to per-partition writers. This is a write infrastructure concern inside `_dataframe_to_data_files`, not a backend concern.

**What would fix it:** A fanout writer that maintains N open `ParquetWriter` instances (one per active partition), routes batches by partition value, and closes each when done. Memory: O(batch_size × num_active_partitions).

**Workaround:** If the table's sort order includes partition columns, `_apply_sort_order` sorts the data by partition first (bounded memory), then the sequential writer naturally produces one partition at a time. But this requires materializing to temp Parquet for the sort.

---

## 3. Delete Operations

| # | Operation | Java Iceberg | PyIceberg (`main`) | PyIceberg + Pluggable | Issues/PRs | Notes |
|:---:|---|:---:|:---:|:---:|---|---|
| 3.1 | **Delete (whole-file drop)** | ✅ | ✅ | ✅ | — | Metadata-only. No data read. Unchanged. |
| 3.2 | **Delete (CoW rewrite)** | ✅ | ✅ (OOM) | ✅ 🔌 | — | **main**: O(2×file) — loads full file + filtered copy. **Pluggable**: O(batch_size) for unpartitioned (streaming RBR to writer), O(kept_rows) for partitioned. |
| 3.3 | **Merge-on-Read delete (write delete file)** | ✅ | ❌ (warns, falls back to CoW) | ❌ | [#1078](https://github.com/apache/iceberg-python/issues/1078) | Needs `row_delta` commit protocol. Pluggable architecture supports it (just needs the commit path). |
| 3.4 | **Deletion Vectors (V3)** | ✅ | ❌ | ❌ | [#1818](https://github.com/apache/iceberg-python/issues/1818) | V3 feature. Pluggable ready (new `apply_deletion_vectors` method). |

### 3.2 Deep Dive: CoW Delete

**Pain point on `main`:** `ArrowScan.to_table([file])` loads entire file into memory, then `df.filter(complement)` creates a second copy. For 1 GB files: ~2 GB peak.

**Pluggable fix (unpartitioned tables — O(batch_size)):**
1. `backends.read.read_parquet(file, AlwaysTrue())` — streaming read, O(batch_size)
2. Per-batch: `batch.filter(preserve_row_filter)` — O(batch_size)
3. Peek first batch for schema → create `RecordBatchReader` from generator
4. Pass `RecordBatchReader` directly to `_dataframe_to_data_files` — streams via `bin_pack_record_batches`
5. Only ONE batch in memory at a time. Never holds full file.

**Pluggable fix (partitioned tables — O(kept_rows)):**
1. Same streaming read + filter
2. Must materialize kept batches as `pa.Table` because `_dataframe_to_data_files` needs `pa.Table` for partition routing (`_determine_partitions`)
3. Memory: O(kept_rows per file) — still better than O(2×file) on `main`

**Key subtlety:** Delete path reads with `AlwaysTrue()` directly (NOT via `orchestrate_scan`) because the task's residual would filter to ONLY delete-matching rows, then the complement would produce nothing.

**Speculative write pattern:** For unpartitioned tables, the writer starts before we know the final `kept_row_count`. If `kept_row_count == original_row_count` (no rows actually deleted), the written files are orphans cleaned by maintenance. This is rare (only when partition filter matches but value filter doesn't).

### 3.3 Deep Dive: Merge-on-Read (MoR) Deletes

**What MoR is:** Instead of rewriting data files to remove deleted rows (CoW), MoR writes a SEPARATE delete file that records which rows are deleted. At read time, the scan resolves deletes by anti-joining data against delete files.

**Two sides of MoR:**

| Side | What it does | PyIceberg status | Pluggable helps? |
|------|---|:---:|:---:|
| **READ** (resolve deletes at scan time) | Anti-join data against equality/positional delete files | ✅ Solved | ✅ `anti_join_from_files`, `apply_positional_deletes` |
| **WRITE** (produce delete files instead of rewriting) | Write a delete file, commit via `row_delta` | ❌ Not implemented | Architecture ready, needs commit protocol |

**The READ side is FULLY solved by the pluggable interface:**
- Positional deletes: `apply_positional_deletes(data_path, del_paths)` — per-file, streaming
- Equality deletes: `anti_join_from_files(data_paths, del_paths, cols)` — bounded memory, spill

**The WRITE side is NOT implemented:**

Current PyIceberg code:
```python
if delete_mode == "merge-on-read":
    warnings.warn("Merge on read is not yet supported, falling back to copy-on-write")
```

What's needed to implement MoR writes:
1. **Positional delete file writer:** Scan the data file to find row positions matching the delete filter, write those positions as a Parquet file with `(file_path, pos)` columns
2. **`row_delta` snapshot producer:** A new commit operation that adds delete files to the snapshot WITHOUT touching data files (unlike `overwrite` which replaces files)
3. **Commit protocol:** The `row_delta` must record the relationship between data files and their delete files via sequence numbers

**What the pluggable interface provides for future MoR writes:**
- `backends.read.read_parquet(file, delete_filter)` — stream file to find matching positions (O(batch))
- `backends.write.write_parquet(positions_batch, delete_file_path)` — write the position delete file
- The execution is trivial. The blocker is the commit protocol (`row_delta` snapshot producer in `table/update/snapshot.py`).

**Relation to other operations:**
- `table.delete(filter)` → currently always CoW. With MoR: would write delete file instead (faster write, slower subsequent reads)
- `table.scan().to_arrow()` → already resolves both positional and equality deletes from MoR tables written by Spark/Flink
- Compaction (#1092) → merges delete files with data files to produce clean data files (reverses MoR accumulation)

---

## 4. Upsert

| # | Operation | Java Iceberg | PyIceberg (`main`) | PyIceberg + Pluggable | Issues/PRs | Notes |
|:---:|---|:---:|:---:|:---:|---|---|
| 4.1 | **Upsert (MERGE INTO)** | ✅ (Spark/Flink) | ⚠️ (OOM on large) | ⚠️ 🔌 | [#2159](https://github.com/apache/iceberg-python/issues/2159), [#2138](https://github.com/apache/iceberg-python/issues/2138), [#3129](https://github.com/apache/iceberg-python/issues/3129), [#3508](https://github.com/apache/iceberg-python/issues/3508) | Memory: O(source_size) — already optimal given `upsert(df: pa.Table)` API contract. Source is in memory by definition. |

### 4.1 Deep Dive: Upsert Memory

**Pain point (reported in issues):** Users with 1M+ row sources experience OOM and extreme slowness.

**Analysis:** The algorithm is O(source_size) in memory — which is the theoretical minimum since the user passes a `pa.Table`. The OOM reports are from users with genuinely large source DataFrames (1 GB+). The "exponential" growth reported in #2138 is actually O(source × num_target_batches) in CPU time (per-batch `get_rows_to_update` re-scans the full source), not in memory.

**What the pluggable interface improves:** The target scan now uses parallel `orchestrate_scan` with bounded residual filter binding (was broken before our fix — `TypeError` on `In` predicates). The algorithm itself is unchanged.

**What would truly fix it:** Accept `pa.RecordBatchReader` as upsert source + use `join_from_files` for bounded-memory matching. Requires API change.

---

## 5. Maintenance Operations

| # | Operation | Java Iceberg | PyIceberg (`main`) | PyIceberg + Pluggable | Issues/PRs | Notes |
|:---:|---|:---:|:---:|:---:|---|---|
| 5.1 | **Expire snapshots (metadata only)** | ✅ | ✅ | ✅ | — | Metadata-only. No data I/O. |
| 5.2 | **Expire snapshots (with file cleanup)** | ✅ | ❌ | ❌ | [#2604](https://github.com/apache/iceberg-python/issues/2604) | Needs cross-reference of all snapshot file sets. Pluggable helps: `anti_join_from_files` for set difference. |
| 5.3 | **Delete orphan files** | ✅ | ❌ | ❌ 🔌 (ready) | [#1200](https://github.com/apache/iceberg-python/issues/1200) | List storage objects, cross-reference with metadata. Pluggable: `anti_join_from_files(storage_paths, metadata_paths)` for bounded memory. |
| 5.4 | **Rewrite data files (compaction)** | ✅ | ❌ | ❌ | [#1092](https://github.com/apache/iceberg-python/issues/1092) | Needs: read multiple files → sort → write new files → atomic commit. Pluggable: `sort_from_files` handles the sort+write. Missing: `RewriteFiles` commit protocol. |
| 5.5 | **Rewrite manifests** | ✅ | ❌ | ❌ | [#270](https://github.com/apache/iceberg-python/issues/270) | Metadata-only compaction. No data I/O. Doesn't need pluggable interface. |
| 5.6 | **Rewrite position delete files** | ✅ | ❌ | ❌ | — | Compact small positional delete files into larger ones. Needs `RewriteFiles`. |
| 5.7 | **Convert equality → position deletes** | ✅ | ❌ | ❌ | — | Scan data to find row positions matching equality deletes. Pluggable helps: bounded-memory scan + write. |
| 5.8 | **Compute table statistics (NDV sketches)** | ✅ | ❌ | ❌ | — | Full table scan producing Puffin statistics. Pluggable: bounded-memory scan. |

### 5.3 Deep Dive: Delete Orphan Files

**Pain point:** Without orphan cleanup, failed writes leave unreferenced Parquet files in storage indefinitely, wasting money.

**How the pluggable interface enables it:**
```python
# Pseudocode for bounded-memory orphan detection:
storage_paths = backends.read.list_objects(table_location, io_props)  # streaming
metadata_paths = stream_all_referenced_paths(table_metadata)          # from manifests

# Anti-join: paths in storage NOT in metadata = orphans
orphans = backends.compute.anti_join_from_files(
    left_paths=[storage_paths_parquet],
    right_paths=[metadata_paths_parquet],
    on=["path"],
    io_properties=io_props,
)
# Delete orphans (bounded memory — streaming)
for batch in orphans:
    for path in batch.column("path"):
        io.delete(path)
```

Memory: O(512 MB) with spill. Without pluggable: would need to hold all paths in Python dicts.

---

## 6. Transaction & Commit Operations

| # | Operation | Java Iceberg | PyIceberg (`main`) | PyIceberg + Pluggable | Issues/PRs | Notes |
|:---:|---|:---:|:---:|:---:|---|---|
| 6.1 | **Fast append** | ✅ | ✅ | ✅ | — | Unchanged. |
| 6.2 | **Overwrite (atomic)** | ✅ | ✅ | ✅ 🔌 | — | Pluggable improves execution, not commit. |
| 6.3 | **Row delta (MoR commit)** | ✅ | ❌ | ❌ | [#1078](https://github.com/apache/iceberg-python/issues/1078) | Needs new snapshot producer that accepts data + delete files. |
| 6.4 | **Rewrite files (compaction commit)** | ✅ | ❌ | ❌ | [#3130](https://github.com/apache/iceberg-python/issues/3130) | Atomic replace: old files → new files with validation. |
| 6.5 | **Replace partitions (atomic DPO)** | ✅ | ❌ | ❌ | — | Atomic single-snapshot dynamic partition overwrite. Current PyIceberg does 2 commits. |
| 6.6 | **Commit retry with validation** | ✅ | ❌ | ❌ | [#3319](https://github.com/apache/iceberg-python/issues/3319) | OCC retry with conflict detection. |

---

## 7. Summary: What the Pluggable Interface Enables

| Category | Operations enabled/improved | Key mechanism |
|----------|---|---|
| **Read** | Equality deletes (was ValueError), positional deletes (OOM-safe), limit (O(batch)), count (streaming) | `anti_join_from_files`, `apply_positional_deletes`, generator early-break |
| **Write** | Sort-on-write (was missing) | `_apply_sort_order` → `sort_from_files` |
| **Delete** | CoW rewrite (O(kept) not O(2×file)) | `backends.read.read_parquet(AlwaysTrue)` + streaming filter |
| **Maintenance** | Orphan detection, compaction sort (architecturally ready) | `anti_join_from_files`, `sort_from_files` |
| **Planning** | Bounded-memory for extreme-scale (>100K delete entries) | `BoundedMemoryPlanner` auto-switch |

---

## 8. What the Pluggable Interface Does NOT Help With

| Operation | Why not | What's needed instead |
|-----------|---------|---|
| Streaming partitioned writes (#2152) | Write routing problem, not compute | Fanout writer in `_dataframe_to_data_files` |
| Merge-on-Read delete write (#1078) | Commit protocol gap | `row_delta` snapshot producer |
| Rewrite files / compaction commit (#1092) | Commit protocol gap | `RewriteFiles` snapshot producer + conflict validation |
| Atomic dynamic partition overwrite | Commit protocol gap | `ReplacePartitions` snapshot producer |
| Commit retry (#3319) | Conflict resolution logic | OCC retry loop with validation |
| V3 write support (#1551) | Metadata format gap | `TableMetadataV3.model_dump_json()` |
| Deletion Vectors (#1818) | New format support | Puffin reader/writer + new delete type |

---

## 9. Issue Cross-Reference

| Issue | Title | Relation to Pluggable Interface |
|:---:|---|---|
| [#3554](https://github.com/apache/iceberg-python/issues/3554) | Integrate DataFusion as execution engine | **This is our EPIC.** The pluggable interface IS this. |
| [#3270](https://github.com/apache/iceberg-python/issues/3270) | Equality Delete support | **SOLVED** by pluggable (anti_join_from_files). |
| [#1210](https://github.com/apache/iceberg-python/issues/1210) | Support reading equality delete files | **SOLVED** by pluggable. |
| [#271](https://github.com/apache/iceberg-python/issues/271) | Support writing to a table with sort-order | **SOLVED** by pluggable (_apply_sort_order). |
| [#2159](https://github.com/apache/iceberg-python/issues/2159) | Upserting large table extremely slow | Partially helped (parallel scan, correct binding). Full fix needs algorithm change. |
| [#2138](https://github.com/apache/iceberg-python/issues/2138) | Upsert memory grows exponentially | Same as above. O(source) is optimal given API. |
| [#3129](https://github.com/apache/iceberg-python/issues/3129) | Upsert 1M rows slow due to create_match_filter | CPU issue (In predicate with 1M values), not memory. |
| [#1078](https://github.com/apache/iceberg-python/issues/1078) | Support Merge-on-Read mode | Not solved. Needs row_delta commit. |
| [#1092](https://github.com/apache/iceberg-python/issues/1092) | Support data files compaction | Not solved. Needs RewriteFiles commit. Pluggable provides the sort/merge. |
| [#1200](https://github.com/apache/iceberg-python/issues/1200) | Delete orphan files | Architecturally ready via pluggable (anti_join_from_files). |
| [#2604](https://github.com/apache/iceberg-python/issues/2604) | Remove deleted data files with expire_snapshots | Same pattern as orphan detection. |
| [#1818](https://github.com/apache/iceberg-python/issues/1818) | V3 Tracking issue | Pluggable ready for DVs (new method). V3 write is metadata gap. |
| [#1551](https://github.com/apache/iceberg-python/issues/1551) | Support writing V3 tables | Not related to pluggable. Metadata serialization gap. |
| [#3319](https://github.com/apache/iceberg-python/issues/3319) | Commit retry with validation | Not related to pluggable. Transaction protocol gap. |
| [#3508](https://github.com/apache/iceberg-python/issues/3508) | Segfault on large upserts | Likely PyArrow memory corruption. Pluggable doesn't address. |
| [#3162](https://github.com/apache/iceberg-python/issues/3162) | load_table memory on large metadata | Metadata parsing issue, not execution. |
| [#2152](https://github.com/apache/iceberg-python/issues/2152) | Streaming write to partitioned tables | Closed (partial). Not solved by pluggable — needs fanout writer. |


---

## 10. The Non-Pluggable Write Endpoint: `_dataframe_to_data_files`

### 10.1 What It Is

`_dataframe_to_data_files` (in `pyiceberg/io/pyarrow.py`) is the "last mile" writer that produces Iceberg-compliant Parquet files. It's the ONE function that the pluggable interface intentionally does NOT replace.

```
Input:  pa.Table or pa.RecordBatchReader + TableMetadata + FileIO
Output: Iterable[DataFile] (manifest entries ready for commit)
```

### 10.2 What It Does (Step by Step)

```
1. Schema mapping:  Arrow schema → Iceberg Schema (with field IDs via name_mapping)
2. Routing:
   - RecordBatchReader + unpartitioned → bin_pack_record_batches (streaming, O(batch))
   - pa.Table + unpartitioned → bin_pack_arrow_table (split by target file size)
   - pa.Table + partitioned → _determine_partitions() → per-partition bin_pack
3. For each batch group → write_file():
   a. _to_requested_schema() — reconcile batch to table schema (adds field IDs)
   b. LocationProvider.new_data_location() — generate correct file path
   c. pq.ParquetWriter → write to FileIO output stream
   d. Extract column statistics from writer.writer.metadata
   e. Construct DataFile(file_path, stats, partition, spec_id, ...)
4. Parallel: ExecutorFactory.map(write_parquet, tasks)
```

### 10.3 Why It Stays in `pyarrow.py`

It depends on PyArrow primitives that no other Python library provides:

| Requirement | PyArrow API used | Alternative exists? |
|---|---|:---:|
| Embed field IDs in Parquet column metadata | `pa.Schema` metadata dict → ParquetWriter | No |
| Write to arbitrary output stream (FileIO) | `pq.ParquetWriter(fos, ...)` where `fos` is `FileIO.create()` | No |
| Extract column stats after write | `writer.writer.metadata.row_group(i).column(j).statistics` | No |
| Compute file size | `len(fo)` on the OutputFile | PyIceberg-specific |

Moving it to the execution module would just create a circular dependency back to `pyarrow.py`.

### 10.4 How It Wires to the Pluggable Interface

```
User operation     →  Pluggable layer (read/compute)  →  _dataframe_to_data_files  →  commit
─────────────────     ──────────────────────────────     ─────────────────────────     ──────
table.append(df)       _apply_sort_order(df)              writes sorted data            fast_append
table.overwrite(df)    delete(filter) via backends.read   writes new data               overwrite
table.delete(filter)   backends.read + filter             writes kept rows              overwrite
```

The pluggable interface handles everything UPSTREAM of `_dataframe_to_data_files`:
- **Read:** Which data to read, with what engine, streaming
- **Compute:** Sort, filter, join — with bounded memory
- **Reconcile:** Schema evolution mapping

`_dataframe_to_data_files` handles DOWNSTREAM (writing the final output to storage with Iceberg metadata). It receives already-processed data and writes it.

### 10.4.1 OOM Resilience at the Handoff Point

The `df` passed to `_dataframe_to_data_files` is NOT always a fully materialized table. It depends on the operation:

| Operation | What's passed | Memory at handoff | OOM-safe? |
|---|---|:---:|:---:|
| `table.append(df)` | The user's original `pa.Table` (or sorted version) | O(source) — user already holds this | ✅ (user's choice) |
| `table.append(reader)` (unpartitioned) | `pa.RecordBatchReader` | O(batch) — streaming | ✅ |
| `table.append(reader)` (partitioned) | N/A — raises `NotImplementedError` | — | ❌ (not supported) |
| `table.delete(filter)` CoW (unpartitioned) | `pa.RecordBatchReader` of kept batches | O(batch) — streaming | ✅ |
| `table.delete(filter)` CoW (partitioned) | `pa.Table` of kept rows per file | O(kept_rows per file) | ⚠️ |
| `table.overwrite(df)` | Same as append (user's df) | O(source) — user's choice | ✅ |

**The ⚠️ case (CoW delete on partitioned tables):** After filtering a file, the kept rows must be materialized as `pa.Table` because `_dataframe_to_data_files` needs it for partition routing. For a 1 GB file where 1% is deleted: O(990 MB) of kept rows in memory.

**The ✅ case (CoW delete on unpartitioned tables):** Kept batches are streamed directly via `RecordBatchReader` to `_dataframe_to_data_files` which consumes them with `bin_pack_record_batches`. Only ONE batch in memory at a time — O(batch_size).

```
UNPARTITIONED (O(batch_size)):
backends.read.read_parquet(file, AlwaysTrue)  →  O(batch) streaming
    ↓
batch.filter(complement)  →  O(batch) per batch
    ↓
peek first batch → RecordBatchReader(first + rest)  →  O(batch)
    ↓
_dataframe_to_data_files(reader)  →  bin_pack_record_batches streams, O(batch)

PARTITIONED (O(kept_rows)):
backends.read.read_parquet(file, AlwaysTrue)  →  O(batch) streaming
    ↓
batch.filter(complement)  →  O(batch) per batch
    ↓
kept_batches = list(filtered)  →  O(kept_rows) ← MATERIALIZATION POINT
    ↓
pa.Table.from_batches(kept_batches)  →  O(kept_rows)
    ↓
_dataframe_to_data_files(kept_table)  →  writes to disk, releases
```

**Why this materialization exists:** `_dataframe_to_data_files` for partitioned tables requires a `pa.Table` to call `_determine_partitions()`. For unpartitioned tables, we COULD pass a `RecordBatchReader` (which streams) — but the kept batches are already collected for the row count comparison.

**Practical impact:** The kept rows are always ≤ the original file size. For a 1 GB Parquet file that expands to ~3 GB in Arrow, the worst case (deleting 1 row) holds ~3 GB. Typical case (deleting 50%): ~1.5 GB. This is per-file, not per-table — only one file is processed at a time.

**UX concern (partitioned tables only):** A user who never materializes their table can still OOM on `table.delete("rare_column = 'x'")` on a partitioned table, because internally one file's kept rows are materialized. For unpartitioned tables, this is now fully streaming O(batch_size).

```python
# Unpartitioned table — O(batch_size), safe:
table.delete("id = 42")  # streams kept rows directly to writer

# Partitioned table — O(kept_rows per file), potential OOM:
table.delete("id = 42")  # materializes kept rows for partition routing
# For 1 GB file with 1 row deleted: holds ~3 GB (Arrow expansion) 
```

**Could the partitioned case be O(batch)?** Yes, with a streaming partition router (fanout writer #2152) that routes batches to per-partition writers without full materialization. This is a separate write infrastructure project, not an execution backend concern.

### 10.5 Known Limitations

| Limitation | Impact | Issue | Pluggable helps? |
|---|:---:|---|:---:|
| `NotImplementedError` for RecordBatchReader + partitioned | Can't stream writes to partitioned tables | [#2152](https://github.com/apache/iceberg-python/issues/2152) | No (needs fanout writer) |
| `_determine_partitions` requires full `pa.Table` | Forces materialization for partitioned writes | Same as above | No |
| Target file size = uncompressed Arrow bytes | Files are 3-10× smaller than target on disk | #2998 | No |
| Statistics are Parquet-level (not Arrow-level) | Correct behavior per spec | — | — |

### 10.6 Can We Leave It As-Is?

**Yes.** It's:
- Battle-tested (used by all existing PyIceberg write paths since day 1)
- Correct (produces valid Iceberg files with full column statistics)
- Already parallelized (ExecutorFactory for concurrent file writes)
- Already streaming for unpartitioned RecordBatchReader inputs

The pluggable interface feeds it better data (sorted, filtered, bounded-memory processed). The function itself doesn't need changes unless streaming partitioned writes (#2152) are implemented — which is a separate effort unrelated to the pluggable architecture.


---

## 11. MoR vs CoW: Complete Matrix

### 11.1 What MoR and CoW Mean

- **CoW (Copy-on-Write):** Rewrites the data file without the deleted/updated rows. Write is expensive (full file rewrite). Read is cheap (data files are clean, no resolution needed).
- **MoR (Merge-on-Read):** Writes a separate delete file. Write is cheap (small delete file). Read is expensive (must resolve deletes at scan time by anti-joining data against delete files).

### 11.2 All Operations × Both Modes

| Operation | CoW behavior | MoR behavior | PyIceberg (main) | PyIceberg + Pluggable |
|---|---|---|:---:|:---:|
| **Append** | Write data file | Write data file (same) | ✅ | ✅ |
| **Delete** | Read file → filter → rewrite without deleted rows | Write positional delete file listing deleted row positions | ✅ CoW only | ✅ CoW only (MoR: #1078) |
| **Update (overwrite matched)** | Read file → modify matched rows → rewrite | Write equality delete (old keys) + write data file (new values) | ✅ CoW only | ✅ CoW only (MoR: #1078) |
| **Upsert (matched update)** | CoW overwrite of matched partitions | Write equality delete (old keys) + append new values via `row_delta` | ✅ CoW only | ✅ CoW only |
| **Upsert (not matched insert)** | Append new rows | Append new rows (same) | ✅ | ✅ |
| **Scan (no deletes)** | Read data files directly | Read data files directly (same) | ✅ | ✅ |
| **Scan (with pos deletes)** | N/A (CoW has no delete files) | Read data + resolve positional deletes | ⚠️ OOM-prone | ✅ 🔌 streaming |
| **Scan (with eq deletes)** | N/A | Read data + anti-join against equality delete keys | ❌ ValueError | ✅ 🔌 bounded memory |
| **Compaction** | N/A (files already clean) | Merge delete files INTO data files → produce clean files | ❌ | ❌ (needs RewriteFiles) |

### 11.3 The MoR Lifecycle

```
WRITE TIME (fast):
    table.delete("user_id = 42")
    → Instead of rewriting the data file:
    → Write a DELETE FILE: [(file_path, pos=17)] or [(user_id=42)]
    → Commit via row_delta: "file X now has delete file Y"

READ TIME (extra work):
    table.scan().to_arrow()
    → For each data file with associated delete files:
    → Read data file batches
    → Read delete file
    → Anti-join: exclude rows matching delete keys/positions
    → Return only surviving rows

COMPACTION (cleanup):
    table.compact()  [not implemented]
    → Read data file + all its delete files
    → Produce clean data file (deletes physically applied)
    → Atomic replace: old data + delete files → new clean data file
    → After compaction: no more delete files to resolve at read time
```

### 11.4 MoR in Upsert (How Flink Does It)

Flink's Iceberg upsert in MoR mode:
1. For each incoming row with key `K`:
   - Write an **equality delete file** containing key `K` (logically deletes any existing row with that key)
   - Write a **data file** containing the new row
   - Commit both via `row_delta` (atomic: "delete old K, insert new K")

This is extremely fast for writes (no read of existing data!) but accumulates delete files that slow reads. Eventually compaction merges them.

**PyIceberg's upsert** always uses CoW: it reads the target, finds matches, overwrites. It could use MoR instead (write eq delete + append) which would be faster for write-heavy workloads — but needs the `row_delta` commit protocol (#1078).

### 11.5 What the Pluggable Interface Provides for Each

| Concern | CoW | MoR READ | MoR WRITE |
|---------|:---:|:---:|:---:|
| Read data files | ✅ `backends.read.read_parquet` | ✅ Same | ✅ Same (for finding positions) |
| Filter/anti-join | ✅ `batch.filter(complement)` | ✅ `anti_join_from_files` | N/A |
| Positional delete resolution | N/A (no delete files) | ✅ `apply_positional_deletes` | N/A |
| Write data files | ✅ `_dataframe_to_data_files` | N/A | ✅ Same (append new values) |
| Write delete files | N/A | N/A | ⚠️ Trivial write, but no `row_delta` commit |
| Bounded memory | ✅ DataFusion spill | ✅ DataFusion spill | ✅ (small files) |
| **Commit protocol** | ✅ `overwrite` snapshot | ✅ (read-only, no commit) | ❌ Needs `row_delta` |

### 11.6 Summary

The pluggable interface fully solves the **READ side** of MoR (resolving deletes at scan time). The **WRITE side** of MoR (producing delete files instead of rewriting) is blocked by the `row_delta` commit protocol — not by execution capabilities. The execution part (writing a small Parquet delete file) is trivial; the hard part is the transaction semantics.


---

## 12. Iceberg V3 Spec: Complete Feature Set

### 12.1 What V3 Adds (Full Spec)

V3 extends V2 with new types, metadata structures, and capabilities. Tracked in PyIceberg: [#1818](https://github.com/apache/iceberg-python/issues/1818), [#1551](https://github.com/apache/iceberg-python/issues/1551).

| # | V3 Feature | Description | Java Iceberg | PyIceberg (main) | PyIceberg + Pluggable | Pluggable Impact |
|:---:|---|---|:---:|:---:|:---:|---|
| 12.1 | **Nanosecond timestamps** | `timestamp_ns` and `timestamptz_ns` — nanosecond precision timestamps | ✅ Read+Write | ✅ Read only | ✅ Read only | None — handled by schema/type system, not execution |
| 12.2 | **UnknownType** | NULL-typed column promotable to any primitive. For schema evolution (add column, fill later). | ✅ | ✅ Read | ✅ Read | None — `_to_requested_schema` handles promotion |
| 12.3 | **Geometry type** | WKB-encoded spatial geometry. CRS metadata. | ✅ | ✅ Type defined | ✅ Type defined | None — stored as binary in Parquet, backends read binary fine |
| 12.4 | **Geography type** | WKB-encoded spatial geography (earth coordinates). CRS metadata. | ✅ | ✅ Type defined | ✅ Type defined | Same as Geometry |
| 12.5 | **Row lineage** | `first_row_id` and `next_row_id` on snapshots. Tracks row-level provenance across commits. | ✅ | ✅ Read+tracked | ✅ Read+tracked | None — metadata-only, no execution impact |
| 12.6 | **Default column values** | Columns can have default values (used when reading old files missing the column). | ✅ | ❌ | ❌ | Minor: `_to_requested_schema` fills NULL; would need to fill with default instead. 1-line change in ArrowProjectionVisitor. |
| 12.7 | **Deletion Vectors (DVs)** | Bitmap-encoded delete indicators in Puffin files. More compact than positional delete files. | ✅ | ❌ | ❌ 🔌 (ready) | **Pluggable ready:** new `apply_deletion_vectors` method on ComputeBackend. Same pattern as `apply_positional_deletes`. |
| 12.8 | **Multi-argument transforms** | Partition transforms with multiple args (e.g., `bucket(N, col)` already exists, but V3 generalizes). | ✅ | ⚠️ Partial | ⚠️ Partial | None — transforms are in the partitioning module, not execution |
| 12.9 | **V3 metadata write** | Serialize `TableMetadataV3` to JSON. | ✅ | ❌ `NotImplementedError` | ❌ | None — metadata serialization, not execution |
| 12.10 | **Variant type** (proposed) | Semi-structured data (JSON-like). Under discussion for V3/V4. | ⚠️ Draft | ❌ | ❌ | Would need type mapping + Parquet encoding support |

### 12.2 Deep Dive: Deletion Vectors (V3)

**What DVs are:** Instead of a separate Parquet file listing deleted positions `[(file_path, pos), ...]`, a DV is a **roaring bitmap** stored in a Puffin file. It's more compact for large numbers of deletes (bitmap vs. repeated integers).

**How DVs differ from positional deletes:**

| Aspect | Positional Deletes (V2) | Deletion Vectors (V3) |
|--------|---|---|
| Format | Parquet file with `(file_path, pos)` columns | Puffin file with roaring bitmap blob |
| Size for 1M deletes | ~8 MB (1M × 8 bytes) | ~125 KB (bitmap) |
| Resolution algorithm | Set lookup: `if row_position in delete_set` | Bitmap check: `if bitmap.contains(row_position)` |
| Assigned to files | Via manifest metadata (sequence number gating) | Via manifest metadata (same) |

**What the pluggable interface provides:**

The architecture already has `apply_positional_deletes(data_path, position_delete_paths, ...)`. DVs would need:

```python
class ComputeBackend(Protocol):
    # Existing:
    def apply_positional_deletes(self, data_path, position_delete_paths, ...) -> Iterator[RecordBatch]: ...
    
    # New for V3 DVs:
    def apply_deletion_vectors(self, data_path, dv_paths, ...) -> Iterator[RecordBatch]: ...
```

And in `orchestrate_scan`:
```python
if dv_deletes:
    batches = backends.compute.apply_deletion_vectors(data_path, dv_paths, ...)
elif pos_deletes:
    batches = backends.compute.apply_positional_deletes(...)
elif eq_deletes:
    batches = backends.compute.anti_join_from_files(...)
```

**No architecture changes needed** — just a new method implementation following the same pattern.

### 12.3 Deep Dive: Default Column Values

**What it is:** In V3, a column can specify a default value (e.g., `email DEFAULT 'unknown'`). When reading old files that don't have this column, instead of filling with NULL, fill with the default.

**Current behavior:** `ArrowProjectionVisitor` (used by `_to_requested_schema`) fills missing columns with NULL arrays:
```python
# In ArrowProjectionVisitor:
if field_id not in file_schema.field_ids:
    return pa.nulls(batch.num_rows, type=target_type)  # ← always NULL
```

**V3 fix:** Would be:
```python
if field_id not in file_schema.field_ids:
    default = table_schema.find_field(field_id).default_value
    if default is not None:
        return pa.array([default] * batch.num_rows, type=target_type)
    return pa.nulls(batch.num_rows, type=target_type)
```

**Pluggable impact:** None — this is inside `_to_requested_schema` (reconciliation), which is already in the orchestration layer. The change would be ~3 lines in `ArrowProjectionVisitor`. No backend changes needed.

### 12.4 Deep Dive: Nanosecond Timestamps

**What it is:** V2 only supports microsecond timestamp precision. V3 adds `timestamp_ns` and `timestamptz_ns` with nanosecond precision. Parquet already supports ns natively.

**Current PyIceberg behavior:**
- V1/V2: nanosecond timestamps are downcast to microsecond on read (lossy)
- V3: nanosecond precision is preserved (lossless)
- The `format_version` check gates this behavior

**Pluggable impact:** None. The ns/us handling is in:
1. `pyarrow_to_schema` (type inference from Arrow schema)
2. `_to_requested_schema` (type casting during reconciliation)
3. Not in any backend's read/write/compute logic

### 12.5 Summary: Pluggable Interface vs V3

| V3 Feature | Where it lives | Pluggable impact |
|---|---|:---:|
| Nanosecond timestamps | Type system + reconciliation | None |
| UnknownType | Type promotion in reconciliation | None |
| Geometry/Geography | Type definitions + Parquet binary | None |
| Row lineage | Metadata (snapshot fields) | None |
| Default values | Reconciliation (`ArrowProjectionVisitor`) | None (3-line fix) |
| **Deletion Vectors** | **Execution (new scan resolution type)** | **Ready: new ComputeBackend method** |
| Multi-argument transforms | Partitioning module | None |
| V3 metadata write | Serialization (`TableMetadataV3`) | None |

**Conclusion:** The pluggable interface neither helps nor hinders V3 adoption, with ONE exception: Deletion Vectors require a new execution method (`apply_deletion_vectors`) which the architecture is designed to accommodate (same pattern as existing positional deletes). Everything else is type system, metadata, or reconciliation — all orthogonal to the execution backends.


---

## 13. Complete Decision Tree: Every Operation, Every Nuance

```python
def execute_iceberg_operation(table, operation, **kwargs):
    """
    Pseudocode capturing EVERY operation and ALL decision branches
    in the pluggable interface architecture.
    """
    backends = Backends.resolve(table.io.properties)  # Read from config / env / auto-detect

    # =========================================================================
    # SCAN OPERATIONS
    # =========================================================================
    if operation == "scan.to_arrow":
        tasks = plan_files(table, kwargs["filter"])  # see PLANNING below
        warn_if_large_result(tasks)  # ResourceWarning if estimated > 2 GB

        for task in parallel(tasks):  # ExecutorFactory.map
            # --- READ ---
            if task.has_equality_deletes:
                batches = backends.compute.anti_join_from_files(
                    data_paths=[task.file.path],
                    delete_paths=[d.path for d in task.eq_delete_files],
                    on=equality_field_names(task.eq_delete_files),
                )  # O(memory_limit) with spill
            elif task.has_positional_deletes:
                batches = backends.compute.apply_positional_deletes(
                    data_path=task.file.path,
                    position_delete_paths=[d.path for d in task.pos_delete_files],
                )  # O(batch + positions_for_one_file)
            elif task.has_deletion_vectors:  # V3 FUTURE
                batches = backends.compute.apply_deletion_vectors(
                    data_path=task.file.path,
                    dv_paths=[d.path for d in task.dv_files],
                )  # O(batch + bitmap)
            else:
                batches = backends.read.read_parquet(
                    task.file.path, schema, task.residual, props,
                    dictionary_columns=kwargs.get("dictionary_columns", ()),
                )  # O(batch) streaming

            # --- COMPUTE (residual filter) ---
            if task.residual != AlwaysTrue:
                bound = bind(schema, task.residual)
                batches = backends.compute.filter(batches, bound)  # O(batch) per batch

            # --- RECONCILE (schema evolution) ---
            file_schema = infer_schema_from_batch(batches[0])
            if file_schema.field_ids != projected_schema.field_ids:
                batches = [_to_requested_schema(b, file_schema, projected_schema) for b in batches]
                # handles: column add/remove/reorder/rename/type promote/partition inject

            yield from batches

        # --- MATERIALIZE ---
        if kwargs.get("limit"):
            # O(batch) — collect only enough batches to satisfy limit, then STOP
            collected = collect_until(batches_generator, limit)
            return concat_tables(collected).slice(0, limit)
        else:
            # O(result) — user asked for full Table
            try:
                return concat_tables(all_batches, promote_options="permissive")
            except MemoryError:
                raise with helpful message (batch_reader, limit, filter alternatives)

    elif operation == "scan.to_arrow_batch_reader":
        # Same as above but NO materialization — pure streaming O(batch)
        return RecordBatchReader.from_batches(schema, batches_generator)

    elif operation == "scan.count":
        total = 0
        for task in plan_files(table, kwargs["filter"]):
            if task.residual == AlwaysTrue and no_delete_files(task):
                total += task.file.record_count  # O(1) — metadata only
            else:
                for batch in orchestrate_scan(task):  # streaming
                    total += batch.num_rows  # O(batch) — never materializes
        return total

    # =========================================================================
    # WRITE OPERATIONS
    # =========================================================================
    elif operation == "append":
        df = kwargs["df"]  # pa.Table or RecordBatchReader — user already holds this

        # --- SORT-ON-WRITE ---
        sort_order = get_sort_order(table.metadata)
        if sort_order and backends.supports_bounded_memory:
            # Materialize to temp file → external merge sort → sorted output
            with materialize_to_parquet(df) as tmp:
                sorted = backends.compute.sort_from_files([tmp], sort_order)
                df = Table.from_batches(list(sorted))  # O(memory_limit) with spill
        # else: no sort, pass through unchanged

        # --- WRITE (non-pluggable, PyArrow-specific) ---
        data_files = _dataframe_to_data_files(df, table.metadata, table.io)
        #   internally:
        #   if df is RecordBatchReader and unpartitioned:
        #       bin_pack_record_batches(df) → streaming write O(batch)
        #   elif df is Table and unpartitioned:
        #       bin_pack_arrow_table(df) → write in chunks
        #   elif df is Table and partitioned:
        #       _determine_partitions(df) → per-partition bin_pack → write
        #   elif df is RecordBatchReader and partitioned:
        #       raise NotImplementedError  ← #2152 gap

        # --- COMMIT ---
        commit_fast_append(data_files)

    elif operation == "overwrite":
        # overwrite = delete(filter) + append(df)
        execute_iceberg_operation(table, "delete", filter=kwargs["overwrite_filter"])
        execute_iceberg_operation(table, "append", df=kwargs["df"])

    # =========================================================================
    # DELETE OPERATIONS
    # =========================================================================
    elif operation == "delete":
        filter = kwargs["filter"]

        # Phase 1: metadata-only deletes (whole-file drops)
        with delete_snapshot:
            delete_snapshot.delete_by_predicate(filter)  # drops files fully contained

        # Phase 2: CoW rewrites (partial file matches)
        if delete_snapshot.rewrites_needed:
            for file_task in plan_files(table, filter):

                # --- READ ALL ROWS (no residual!) ---
                batches = backends.read.read_parquet(
                    file_task.file.path, schema, AlwaysTrue(), props
                )  # O(batch) streaming — NOT orchestrate_scan (would apply residual)

                # --- FILTER (keep complement) ---
                preserve_filter = complement_of(filter)  # NOT(filter) OR IS_NULL(filter_cols)
                kept_count = 0
                def filtered():
                    for batch in batches:
                        kept = batch.filter(preserve_filter)  # O(batch)
                        if kept.num_rows > 0:
                            kept_count += kept.num_rows
                            yield kept

                # --- WRITE ---
                if table.spec.is_unpartitioned():
                    # O(batch) — streaming via RecordBatchReader
                    first = next(filtered(), None)
                    if first is None:
                        replaced_files.append((file, []))  # all deleted
                        continue
                    reader = RecordBatchReader.from_batches(first.schema, chain([first], filtered()))
                    new_files = _dataframe_to_data_files(reader, ...)  # streams!
                else:
                    # O(kept_rows) — must materialize for partition routing
                    kept_list = list(filtered())
                    if not kept_list:
                        replaced_files.append((file, []))
                        continue
                    kept_table = Table.from_batches(kept_list)
                    new_files = _dataframe_to_data_files(kept_table, ...)

                # --- DECIDE ---
                if kept_count == 0:
                    replaced_files.append((file, []))         # all rows deleted
                elif kept_count < file.record_count:
                    replaced_files.append((file, new_files))  # some deleted
                # else: kept == original → no-op (orphan files from speculative write)

            # --- COMMIT ---
            commit_overwrite(replaced_files)

    # =========================================================================
    # UPSERT
    # =========================================================================
    elif operation == "upsert":
        source_df = kwargs["df"]       # pa.Table — already in memory (user's choice)
        join_cols = kwargs["join_cols"]

        # Validation
        assert no_duplicates(source_df, join_cols)
        assert schema_compatible(source_df.schema, table.schema)

        # Scan target (only partitions matching source keys)
        match_filter = create_match_filter(source_df, join_cols)  # In("id", [1,2,3,...])
        target_batches = scan(table, filter=match_filter).to_arrow_batch_reader()

        updates = []
        inserts = source_df

        for batch in target_batches:  # streaming, O(batch) per iteration
            target_rows = Table.from_batches([batch])

            if when_matched_update_all:
                # Find source rows matching this target batch where values differ
                rows_to_update = get_rows_to_update(source_df, target_rows, join_cols)
                # ↑ inner join + per-row comparison. O(source + batch). CPU-intensive.
                if rows_to_update:
                    updates.append(rows_to_update)

            if when_not_matched_insert_all:
                # Remove from inserts any source row that matches this target batch
                match_expr = create_match_filter(target_rows, join_cols)
                inserts = inserts.filter(~expression_to_pyarrow(match_expr))

        # Commit updates (overwrite matched partitions)
        if updates:
            all_updates = concat_tables(updates)  # O(matched) ≤ O(source)
            overwrite(all_updates, filter=overwrite_predicate)

        # Commit inserts (append remaining)
        if inserts and len(inserts) > 0:
            append(inserts)

        return UpsertResult(rows_updated=len(updates), rows_inserted=len(inserts))

    # =========================================================================
    # PLANNING (internal, auto-switch)
    # =========================================================================
    elif operation == "plan_files":
        manifests = snapshot.manifests(io)

        # Check if bounded planner needed
        delete_entry_count = sum(m.existing_rows_count for m in manifests if m.content == DELETES)

        if delete_entry_count > 100_000:  # _BOUNDED_PLANNER_THRESHOLD
            try:
                return BoundedMemoryPlanner().plan_files(manifests, ...)
                # Streams entries to temp Parquet → DataFusion SQL JOIN → O(512 MB)
            except ImportError:
                warn("Install DataFusion for bounded-memory planning")

        # Default: in-memory (fast for <100K entries, ~20 MB)
        return ManifestGroupPlanner.plan_files(manifests)
        #   internally:
        #   for entry in all_manifest_entries:   ← O(entries × 200 bytes)
        #       if DATA: data_entries.append(entry)
        #       elif POSITION_DELETES: delete_index.add(entry)
        #       elif EQUALITY_DELETES: delete_index.add(entry)  ← was ValueError on main!
        #   for data_entry in data_entries:
        #       yield FileScanTask(data_entry, delete_index.for_file(data_entry))

    # =========================================================================
    # MAINTENANCE (future — architecture ready)
    # =========================================================================
    elif operation == "delete_orphan_files":  # NOT IMPLEMENTED
        storage_paths = backends.read.list_objects(table_location)  # streaming
        metadata_paths = stream_all_referenced_paths(table.metadata)
        orphans = backends.compute.anti_join_from_files(storage, metadata, on=["path"])
        for batch in orphans:
            for path in batch.column("path"):
                io.delete(path)  # O(batch) memory

    elif operation == "compact":  # NOT IMPLEMENTED
        files_to_compact = select_files(strategy="bin_pack" | "sort" | "z_order")
        sorted = backends.compute.sort_from_files(
            [f.path for f in files_to_compact], sort_keys, props
        )  # O(memory_limit) with spill
        new_files = _dataframe_to_data_files(RecordBatchReader(sorted), ...)
        commit_rewrite_files(old=files_to_compact, new=new_files)  # NEEDS row_delta/#1092

    elif operation == "mor_delete":  # NOT IMPLEMENTED
        # MoR write: produce delete file instead of rewriting data
        for file_task in plan_files(table, filter):
            positions = []
            row_pos = 0
            for batch in backends.read.read_parquet(file_task.file.path, ...):
                mask = batch.filter(filter)  # which rows match delete
                for i in range(batch.num_rows):
                    if mask[i]:
                        positions.append(row_pos + i)
                row_pos += batch.num_rows
            # Write position delete file
            del_file = write_position_delete_file(file_task.file.path, positions)
            # Commit via row_delta (NOT overwrite) — NEEDS #1078
            commit_row_delta(data_file=file_task.file, delete_file=del_file)
```

### 13.1 Key Decision Points Summarized

| Decision | Condition | Path A | Path B |
|----------|-----------|--------|--------|
| Which compute backend? | DataFusion installed? | DataFusion (spill) | PyArrow (in-memory) |
| Which planner? | >100K delete entries? | BoundedMemoryPlanner | ManifestGroupPlanner |
| Sort on write? | Table has sort order + DataFusion? | Sort via `sort_from_files` | Pass through |
| Delete read path? | Always | `backends.read.read_parquet(AlwaysTrue)` | Never `orchestrate_scan` |
| Delete write path? | Unpartitioned? | RecordBatchReader (O(batch)) | pa.Table (O(kept_rows)) |
| Scan residual? | task.residual != AlwaysTrue? | Bind + filter | Skip |
| Schema reconciliation? | file_schema != projected_schema? | `_to_requested_schema` per batch | Skip |
| OOM warning? | Estimated > 2 GB? | ResourceWarning | Silent |
| Limit optimization? | scan.limit set? | Break generator early | Consume all |


---

## 14. PyArrow Fallback (Path B): Inefficiencies and Optimization Room

### 14.1 What Path B Is

When DataFusion is NOT installed, all operations use `PyArrowComputeBackend` — our new code, not the old `ArrowScan`. It's correct and streaming where possible, but **cannot spill to disk**. Large stateful operations (sort, join) must fit in RAM.

### 14.2 Per-Operation Analysis

| Operation | PyArrow Path B Implementation | Memory | Optimal? | Could optimize? |
|---|---|:---:|:---:|:---:|
| **Read** | `ds.dataset().scanner().to_batches()` | O(batch) | ✅ Yes | No — PyArrow's C++ scanner is highly optimized |
| **Filter** | `batch.filter(pc.expression)` per batch | O(batch) | ✅ Yes | No — per-batch, already streaming |
| **Sort** | `Table.from_batches(all)` → `pc.sort_indices` → `table.take` | **O(n)** | ❌ No spill | Theoretically yes (external sort in Python) — not worth it |
| **Anti-join** | Load both sides → `pc.is_in` on struct arrays | **O(left + right)** | ❌ No spill | Theoretically yes (hash-partition) — not worth it |
| **Positional deletes** | Load all positions into Python `set` → per-batch `take(indices)` | **O(positions)** | ⚠️ | Moderate: could merge-stream sorted positions with data |
| **Aggregate** | `table.group_by().aggregate()` | **O(n)** | ❌ No spill | Not worth it |

### 14.3 Where Path B OOMs (and Path A Doesn't)

| Scenario | Path B (PyArrow) | Path A (DataFusion) |
|----------|:---:|:---:|
| Sort 5 GB of data | OOM (needs 5 GB + sorted copy) | O(512 MB) — external merge sort with spill |
| Anti-join 10 GB data × 1 GB deletes | OOM (needs 11 GB) | O(512 MB) — Grace Hash Join with spill |
| 10M positional deletes | ~80 MB for the set (usually fine) | Same approach (positions must be in memory) |
| Filter 100 GB table | Fine (streaming, O(batch)) | Same (streaming) |
| Read 100 GB table | Fine (streaming, O(batch)) | Same (streaming) |

### 14.4 The Design Philosophy: Why We Don't Optimize Path B

Optimizing the PyArrow fallback for large data would mean **reimplementing DataFusion poorly in Python**:

- External merge sort in Python = ~500 lines of complex code, slower than DataFusion's Rust implementation by 10-100×
- Hash-partitioned join in Python = ~300 lines, same performance gap
- Both already exist as production-grade implementations in DataFusion

**The correct UX is:**
```
Small data (< 2 GB): Path B works perfectly. No action needed.
Large data (> 2 GB): pip install 'pyiceberg[datafusion]' → everything automatically uses Path A.
```

The warning system makes this explicit:
```python
# When Path B is used for a compute-intensive operation:
UserWarning: 'equality_delete_resolution' will use PyArrow (in-memory only, may OOM on large data).
For bounded-memory execution: pip install 'pyiceberg[datafusion]'
```

### 14.5 What IS Optimized in Path B

Despite not having spill, the PyArrow backend is NOT the old `ArrowScan`. It's new code with:

| Improvement over old ArrowScan | How |
|---|---|
| Parallel task execution | `ExecutorFactory.map()` across files (was sequential in old count/delete paths) |
| Per-file positional deletes | Resolves one file at a time (old ArrowScan loaded ALL delete files upfront) |
| Streaming filter | Per-batch `batch.filter()` (was inside monolithic ArrowScan method) |
| No double materialization | CoW delete: streaming read → filter → write (old: full Table + filtered copy) |
| Limit-aware early stop | Generator breaks at N rows (old: consumed entire iterator then sliced) |

### 14.6 Could Path B Be Improved? (Practical Options)

| Improvement | Effort | Benefit | Recommendation |
|---|:---:|:---:|---|
| Stream positional deletes (sorted merge) | Medium | Save ~80 MB for files with many deletes | Consider for a follow-up PR |
| PyArrow sort with `pa.RecordBatchReader` + temp files | Very High | Sort without OOM on PyArrow-only | Don't do — install DataFusion instead |
| PyArrow anti-join with partitioned hashing | High | Join without OOM on PyArrow-only | Don't do — install DataFusion instead |
| Better error messages when Path B OOMs | Low | User knows exactly why and what to do | Already done (UserWarning + MemoryError catch) |

**Bottom line:** Path B is the correct-and-sufficient fallback for small-medium data. For large data, the answer is always "install DataFusion" — not "make PyArrow do things it wasn't designed for."
