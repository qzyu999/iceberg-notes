# DataFusion Integration: All Issues & PRs Tracking

## Our Issues/PRs

| Repo | ID | Title | Status | Blocked On |
|------|-----|-------|--------|------------|
| iceberg-python | [#3554](https://github.com/apache/iceberg-python/issues/3554) | EPIC: Integrate DataFusion as execution engine | Open | — |
| iceberg-rust | [#2716](https://github.com/apache/iceberg-rust/issues/2716) | EPIC: Bounded-memory execution operations for pyiceberg-core | Open | — |
| iceberg-rust | [#2717](https://github.com/apache/iceberg-rust/issues/2717) | Bounded-memory session helper | Open | Nothing |
| iceberg-rust | [#2718](https://github.com/apache/iceberg-rust/issues/2718) | `pyiceberg_core.execution` module | Open | #2717 |
| iceberg-rust | — | `IcebergOverwriteCommitExec` | Not created | #2185 or #2244 |
| iceberg-python | — | Engine resolution module | Not created | Nothing |

## Upstream Dependencies (Others' Work)

| Repo | ID | Title | Status | Why We Need It |
|------|-----|-------|--------|----------------|
| iceberg-rust | [#2185](https://github.com/apache/iceberg-rust/pull/2185) | `OverwriteAction` (PR) | Under review | Write-path operations need atomic overwrite commit |
| iceberg-rust | [#2620](https://github.com/apache/iceberg-rust/pull/2620) | `MergingSnapshotProducer` (PR) | Draft | Foundation for RewriteFiles |
| iceberg-rust | [#1607](https://github.com/apache/iceberg-rust/issues/1607) | `RewriteFiles` support (umbrella) | Open | Full compaction with validation |
| iceberg-rust | [#2244](https://github.com/apache/iceberg-rust/issues/2244) | `RewriteFilesAction` | Open | Alternative to #2185 for compaction |
| iceberg-rust | [#2242](https://github.com/apache/iceberg-rust/issues/2242) | Process delete files in snapshots | Open | Required by #1607 |
| iceberg-rust | [#2243](https://github.com/apache/iceberg-rust/issues/2243) | `SnapshotValidator` | Open | Conflict detection for overwrites |
| iceberg-rust | [#2711](https://github.com/apache/iceberg-rust/issues/2711) | InsertOp::Overwrite silently ignored | Open | Confirms the gap we're filling |

## PyIceberg Prerequisites (In iceberg-python)

| ID | Title | Status | Why We Need It |
|----|-------|--------|----------------|
| [#3319](https://github.com/apache/iceberg-python/issues/3319) / [#3320](https://github.com/apache/iceberg-python/pull/3320) | Commit retry | In progress | Safe compaction commits |
| [#3130](https://github.com/apache/iceberg-python/issues/3130) / [#3131](https://github.com/apache/iceberg-python/pull/3131) | REPLACE API | Blocked on #3320 | Compaction commit path |
| [#3285](https://github.com/apache/iceberg-python/pull/3285) | DeleteFileIndex for equality deletes | Open, checks passing, asking for review (by @rambleraptor). Milestoned 0.12.0. | Index plumbing — tells you which eq delete files apply to which data files. Prerequisite for resolution. |
| [#3270](https://github.com/apache/iceberg-python/issues/3270) | Equality Delete support (tracking issue) | Open (by @rambleraptor) | Tracking issue for eq delete reads. @rambleraptor is active contributor. |
| [PR #3269](https://github.com/apache/iceberg-python/pull/3269) | Reading Equality deletes (full impl) | **Closed (stale)** | Was +591 lines doing resolution in-memory via PyArrow. Stalled/closed. Our DataFusion approach supersedes this. |
| [#2918](https://github.com/apache/iceberg-python/pull/2918) | DeleteFileIndex for positional deletes | Merged | Foundation for #3285 |

**Key finding:** @rambleraptor owns equality delete work. PR #3285 (index plumbing) must merge before we can do the resolution (anti-join). Don't step on this — wait for #3285, then contribute the DataFusion-powered resolution on top.
| [#2918](https://github.com/apache/iceberg-python/pull/2918) | DeleteFileIndex for positional deletes | Merged | Foundation for #3285 |

## All Operations (Verified from Source Code Analysis)

### Currently Implemented — OOM Risk (verified in code)

| # | Operation | File:Line | OOM Pattern | DataFusion Operator |
|---|-----------|-----------|-------------|---------------------|
| 1 | **CoW Delete (file rewrite)** | `table/__init__.py:784` | `ArrowScan.to_table(tasks=[original_file])` loads entire Parquet file (~1GB) per file | `FilterExec` + streaming write |
| 2 | **CoW Overwrite** | Same as above (same code path) | Same — triggered by `overwrite_filter` requiring rewrite | `FilterExec` + streaming write |
| 3 | **Upsert — join** | `table/upsert_util.py:100` | `source_index.join(target_index, ...)` — PyArrow in-memory hash join, both sides in memory | `HashJoinExec` (Grace Hash) |
| 4 | **Upsert — row comparison** | `table/upsert_util.py:103-111` | Row-by-row `.slice(idx, 1)` + `.as_py()` comparison — O(n) Python overhead | `HashJoinExec` with column diff |
| 5 | **Upsert — accumulation** | `table/__init__.py:965` | `pa.concat_tables(batches_to_overwrite)` — all matched rows in memory | `HashJoinExec` output streaming |
| 6 | **Upsert — filter building** | `table/upsert_util.py:36` | `unique_keys[0].to_pylist()` — millions of values → huge `In(...)` expression | Different approach needed |
| 7 | **Scan to_table()** | `io/pyarrow.py:1797` | `pa.concat_tables(all batches)` — entire scan result materialized | Streaming `RecordBatchStream` |
| 8 | **Positional delete loading** | `io/pyarrow.py:1705-1721` | `_read_all_delete_files()` holds ALL delete arrays in memory for entire scan | Streaming merge / bounded buffer |
| 9 | **Individual delete file read** | `io/pyarrow.py:1119-1147` | `Scanner.from_fragment(...).to_table()` — each delete file fully loaded | Streaming read |
| 10 | **Positional delete application** | `io/pyarrow.py:1157` | `pa.array(range(start_index, end_index))` — linear memory in row count | Index-based filter |
| 11 | **Dynamic partition overwrite** | `io/pyarrow.py:2940-2950` | `_determine_partitions()` requires full `pa.Table` in memory for partition splitting | `HashAggregateExec` (spillable) |
| 12 | **Metadata inspect (all_manifests/files)** | `table/inspect.py:678,700,718` | `pa.concat_tables(results)` for all files across all snapshots | Streaming / pagination |

### Not Implemented — From Java Iceberg (Verified Gap Analysis)

These exist in Java Iceberg but are completely missing from PyIceberg:

| # | Java Operation | Java Location | Description | Needs Bounded Memory? | Type |
|---|----------------|---------------|-------------|----------------------|------|
| 13 | **Equality delete reads** | `data/EqualityDeleteFilter` | Anti-join to remove deleted rows at scan time | YES (hash join) | Read-path |
| 14 | **RowDelta (MoR write path)** | `api/.../RowDelta.java` | Add data + delete files in single atomic commit | No (metadata commit) but *reading* with deletes does | Transaction |
| 15 | **RewriteDataFiles (compaction)** | `actions/RewriteDataFiles.java` | Rewrite data files: BinPack, Sort, Z-Order strategies | YES (sort, shuffle) | Maintenance |
| 16 | **DeleteOrphanFiles** | `actions/DeleteOrphanFiles.java` | List storage, cross-reference metadata, delete unreferenced files | YES (anti-join millions of paths) | Maintenance |
| 17 | **ExpireSnapshots (with file cleanup)** | `actions/ExpireSnapshots.java` | Expire snapshots AND delete unreferenced data/manifest files | YES (cross-reference all snapshots) | Maintenance |
| 18 | **RewriteManifests** | `actions/RewriteManifests.java` | Rewrite manifests for scan optimization (sort by partition, merge small) | Moderate (metadata I/O) | Maintenance |
| 19 | **RewritePositionDeleteFiles** | `actions/RewritePositionDeleteFiles.java` | Compact position delete files (merge small PD files, remove stale) | YES (join with data file info) | Maintenance |
| 20 | **ConvertEqualityDeleteFiles** | `actions/ConvertEqualityDeleteFiles.java` | Convert eq deletes → pos deletes (scan data to find positions) | YES (scan + join) | Maintenance |
| 21 | **RemoveDanglingDeleteFiles** | `actions/RemoveDanglingDeleteFiles.java` | Remove delete files no longer applying to any live data files | Moderate (cross-reference) | Maintenance |
| 22 | **RewriteFiles (transaction)** | `api/.../RewriteFiles.java` | Atomically replace files (compaction commit path) | No (metadata commit) | Transaction |
| 23 | **ReplacePartitions (atomic)** | `api/.../ReplacePartitions.java` | Dynamic partition overwrite as single atomic snapshot | No (metadata commit) | Transaction |
| 24 | **Deletion Vectors (DV)** | `data/DeletionVector*`, Puffin DV | Write/read/compact deletion vectors for efficient MoR | YES (bitmap + data rewrite) | Read/Write |
| 25 | **Z-Order / Hilbert sorting** | Part of RewriteDataFiles strategies | Global sort on interleaved-bit key for multi-dim clustering | YES (external sort) | Maintenance |
| 26 | **Sort-order enforcement on write** | Spark write path (SortExec) | Full sort before write for tables with sort orders | YES (external sort) | Write-path |
| 27 | **Streaming writes to partitioned tables** | Spark/Flink write paths | Partition-split a stream without full materialization | YES (hash partition) | Write-path |
| 28 | **ComputeTableStats** | `actions/ComputeTableStats.java` | Column-level NDV sketches written to Puffin | YES (full table scan) | Maintenance |
| 29 | **DeleteReachableFiles** | `actions/DeleteReachableFiles.java` | Delete ALL files from metadata tree (table drop cleanup) | YES (traverse entire metadata) | Maintenance |

**Additional code evidence from PyIceberg:**
- `table/__init__.py:2285` → `raise ValueError("PyIceberg does not yet support equality deletes")` (#13)
- `table/__init__.py:2059` → `raise NotImplementedError(...)` for REST path equality deletes (#13)
- `table/__init__.py:752` → `warnings.warn("Merge on read is not yet supported, falling back to copy-on-write")` (#14)
- `table/maintenance.py` → only has `expire_snapshots()` which is metadata-only (no file cleanup) (#15, #17)
- `io/pyarrow.py:2914` → `raise NotImplementedError("Writing a pa.RecordBatchReader to a partitioned table...")` (#27)

**Notable gaps:**
- PyIceberg's `expire_snapshots()` is metadata-only — removes snapshot refs but does NOT delete unreachable files. Java's Action version does both.
- PyIceberg's `dynamic_partition_overwrite` is 2 commits (delete + append), not atomic `ReplacePartitions`.
- No compaction of any kind exists (no BinPack, Sort, or Z-Order strategies).
- No deletion vector support at all.

### What Each Needs

| # | Operation | Needs (iceberg-rust) | Needs (iceberg-python) | Can Start? |
|---|-----------|---------------------|------------------------|------------|
| — | Engine resolution module | Nothing | Nothing | **YES** |
| 1-2 | CoW delete/overwrite | #2717 (session) + #2185 (overwrite) | Nothing | After both |
| 3-6 | Upsert | #2717 (session) | Nothing | After #2717 |
| 7 | Scan to_table() | #2717 (session) | Nothing | After #2717 (for eq delete resolution) |
| 8-10 | Positional delete loading | #2717 (session) | Nothing | After #2717 |
| 11 | Dynamic partition overwrite | #2717 (session) | Nothing | After #2717 |
| 13 | Equality delete reads | #2717 (session) | #3285 (delete index) | After #2717 |
| 14 | RowDelta (MoR write) | #2717 + iceberg-rust RowDelta | Nothing | After RowDelta lands |
| 15 | Compaction (RewriteDataFiles) | #2717 + #2185 (overwrite) | #3131 (REPLACE API) | After all |
| 16 | DeleteOrphanFiles | #2717 (session) | Nothing | After #2717 |
| 17 | ExpireSnapshots (file cleanup) | #2717 (session) | Nothing | After #2717 |
| 18 | RewriteManifests | Nothing (metadata-only) | Nothing | **YES** (no DataFusion needed) |
| 19 | RewritePositionDeleteFiles | #2717 + #2185 | Nothing | After both |
| 20 | ConvertEqualityDeleteFiles | #2717 (session) | Op #13 first | Later |
| 21 | RemoveDanglingDeleteFiles | Nothing (metadata cross-ref) | Nothing | Moderate complexity |
| 22 | RewriteFiles (transaction) | iceberg-rust #1607/#2244 | Nothing | After upstream lands |
| 23 | ReplacePartitions (atomic) | iceberg-rust OverwriteAction | Nothing | After #2185 |
| 24 | Deletion Vectors | #2717 + DV support in iceberg-rust | #1818 (V3) | Later |
| 25 | Z-Order sorting | #2717 + #2185 | Op #15 first | Later |
| 26 | Sort-order enforcement | #2717 (session) | Nothing | After #2717 |
| 27 | Streaming partitioned writes | #2717 (session) | Nothing | After #2717 |
| 28 | ComputeTableStats | #2717 (session) | Nothing | Later |
| 29 | DeleteReachableFiles | Nothing (metadata traversal) | Nothing | Moderate complexity |

### Priority Tiers

**Tier 1 — Highest impact, fewest blockers:**
- Engine resolution module (zero blockers)
- Equality delete reads (#13 — unblocks reading ALL Flink-written tables)
- DeleteOrphanFiles (#16 — read-only anti-join, high user demand)
- Upsert fix (#3-6 — fixes existing O(n²) + OOM)
- ExpireSnapshots with file cleanup (#17 — current implementation is incomplete)

**Tier 2 — Needs OverwriteAction/RewriteFiles to land:**
- Streaming CoW delete/overwrite (#1-2 — fixes existing OOM)
- RewriteDataFiles / Compaction (#15 — #1 most requested missing feature)
- Sort-order enforcement (#26)
- RewritePositionDeleteFiles (#19)

**Tier 3 — Metadata-only (no DataFusion needed, can do anytime):**
- RewriteManifests (#18 — manifest optimization)
- RemoveDanglingDeleteFiles (#21 — delete file cleanup)
- ReplacePartitions atomic (#23 — fix 2-commit pattern)
- RewriteFiles transaction (#22 — commit primitive)

**Tier 4 — Composite / builds on earlier:**
- ConvertEqualityDeleteFiles (#20 — needs eq reads first)
- Z-Order (#25 — needs compaction first)
- Deletion Vectors (#24 — needs V3 + iceberg-rust DV support)
- ComputeTableStats (#28)
- Streaming partitioned writes (#27)
- DeleteReachableFiles (#29)

---

## What I Can Do Right Now (Zero Blockers)

1. Start coding #2717 bounded session helper (iceberg-rust)
2. Start coding #2718 Phase 1: module stubs (iceberg-rust)
3. **First PyIceberg PR: Engine resolver + Upsert via DataFusion hash join**
   - No upstream blockers (doesn't need OverwriteAction)
   - Nobody else working on this
   - Fixes existing O(n²) + OOM
   - Doesn't conflict with @rambleraptor's equality delete work

## What's Blocked

| Want to do | Blocked on | Who |
|------------|-----------|-----|
| Equality delete resolution | PR #3285 (index plumbing) must merge first | @rambleraptor |
| CoW delete/overwrite (Track 2) | iceberg-rust #2185 (OverwriteAction) | @glitchy |
| Compaction | iceberg-rust #2185 + pyiceberg #3131 (REPLACE API) | @glitchy + us |
| `IcebergOverwriteCommitExec` | iceberg-rust #2185 | @glitchy |
