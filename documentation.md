# Issue #1092: Data File Compaction — Technical Documentation

> **Scope**: "Introduce an API to compact data files. The first version of the API will do the following: take a predicate expression as input parameter to find data files matching the filter that will be re-written; group data files by partitions and rewrite them using the same bin-packing constraints of the writer."

---

## Table of Contents
- [Overview](#overview)
- [Requirement 1: Predicate-Based File Selection](#requirement-1-predicate-based-file-selection)
    - [What It Means](#what-it-means)
    - [Java Reference](#java-reference)
    - [PyIceberg: What Already Exists](#pyiceberg-what-already-exists)
    - [What's New for This Requirement](#whats-new-for-this-requirement)
- [Requirement 2: Partition-Grouped Bin-Pack Rewriting](#requirement-2-partition-grouped-bin-pack-rewriting)
    - [Step A: Group by Partition](#step-a-group-by-partition)
    - [Step B: Filter Files by Size](#step-b-filter-files-by-size)
    - [Step C: Bin-Pack into File Groups](#step-c-bin-pack-into-file-groups)
    - [Step D: Filter Groups](#step-d-filter-groups)
    - [Step E: Read and Rewrite](#step-e-read-and-rewrite)
    - [Step F: Atomic Commit](#step-f-atomic-commit)
- [User-Facing API](#user-facing-api)
- [Configuration (Writer Defaults)](#configuration-writer-defaults)
- [File Inventory](#file-inventory)

---

## Overview

Compaction solves the **small files problem**. After many appends, a table accumulates thousands of tiny data files. Each file carries overhead in metadata (manifests, column statistics) and I/O (file open/close). Compaction reads these small files and writes them back as fewer, properly-sized files.

The issue asks for the **simplest correct version**: a Python API that takes a filter, finds matching files, groups them by partition, and rewrites them using the same sizing logic the writer already uses.

```
User: table.maintenance.rewrite_data_files(filter=EqualTo("date", "2024-01-01"))

Under the hood:
  1. Scan table metadata → find files matching filter
  2. Group those files by partition
  3. Within each partition: filter files outside size range, bin-pack, rewrite
  4. Atomic commit: delete old files, add new files
```

---

## Requirement 1: Predicate-Based File Selection

### What It Means

> "take a predicate expression as input parameter to find data files matching the filter"

Given a user-provided boolean expression (e.g., `EqualTo("date", "2024-01-01")`), scan the table's current snapshot to find all **data files** whose contents could match that predicate. This is metadata-only — we don't read the actual Parquet data yet, just the manifest files and their file-level statistics.

The predicate flows through 3 levels of progressive pruning:

```
User predicate: EqualTo("date", "2024-01-01")
        │
        ▼
Level 1: MANIFEST PRUNING — skip entire manifests where partition
         summary shows no matching partitions exist
        │
        ▼
Level 2: PARTITION PRUNING — skip individual data files whose
         partition value doesn't match the projected filter
        │
        ▼
Level 3: METRICS PRUNING — skip data files whose column stats
         (min/max bounds) prove no rows can match
        │
        ▼
Result: List[FileScanTask]
        Each contains: DataFile (.file_path, .file_size_in_bytes,
                                 .partition, .record_count)
                       + associated delete files
```

### Java Reference

In the Java implementation, this is handled inside the planner:

```java
// BinPackRewriteFilePlanner.java, planFileGroups() — line 286
CloseableIterable<FileScanTask> fileScanTasks = table
    .newScan()
    .filter(this.filter)          // ← user's predicate
    .caseSensitive(caseSensitive)
    .ignoreResiduals()
    .planFiles();                 // ← 3-level pruning happens here
```

Source: [BinPackRewriteFilePlanner.java](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java)

### PyIceberg: What Already Exists

**This is fully implemented.** The identical 3-level pruning is in `DataScan`:

```python
# pyiceberg/table/__init__.py

# Level 1: Manifest pruning (line 2002)
manifest_evaluators = KeyDefaultDict(self._build_manifest_evaluator)
manifests = [m for m in snapshot.manifests(self.io)
             if manifest_evaluators[m.partition_spec_id](m)]

# Level 2: Partition pruning (line 2013, via _open_manifest at line 1887)
partition_evaluators = KeyDefaultDict(self._build_partition_evaluator)

# Level 3: Metrics pruning (line 1941)
# _InclusiveMetricsEvaluator checks column-level min/max/null stats
```

**To use it:**
```python
tasks = list(table.scan(row_filter=EqualTo("date", "2024-01-01")).plan_files())
# tasks is List[FileScanTask]
# Each task.file is a DataFile with .file_path, .file_size_in_bytes, .partition, etc.
```

Source: [pyiceberg/table/__init__.py#L2090](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/table/__init__.py)

### What's New for This Requirement

**Nothing.** Call `table.scan(row_filter=filter).plan_files()` and you get the file list. The only new thing is wiring this into `MaintenanceTable.rewrite_data_files()` as the first step.

---

## Requirement 2: Partition-Grouped Bin-Pack Rewriting

> "group data files by partitions and rewrite them using the same bin-packing constraints of the writer"

This has 6 concrete steps. Let's trace each through both codebases.

### Step A: Group by Partition

All files sharing the same partition value must be rewritten together (you can't merge files from different partitions into one file).

**Java:**
```java
// BinPackRewriteFilePlanner.java, line 293
StructLikeMap<List<FileScanTask>> filesByPartition =
    groupByPartition(table, outputSpecId, scannedSpecId, fileScanTasks);
```

**PyIceberg (new, ~5 lines):**
```python
from collections import defaultdict

files_by_partition: dict[Record, list[FileScanTask]] = defaultdict(list)
for task in tasks:
    files_by_partition[task.file.partition].append(task)
```

### Step B: Filter Files by Size

Not every file needs rewriting. A file is a rewrite candidate if it's **too small** or **too large** relative to the target file size. The thresholds come from the writer's own target size.

**Java:**
```java
// SizeBasedFileRewritePlanner.java, line 131–139
// Default thresholds derived from target file size:
long targetFileSize = ...;  // default: write.target-file-size-bytes (512MB)
long minFileSize = (long) (targetFileSize * 0.75);   // 384MB
long maxFileSize = (long) (targetFileSize * 1.80);   // 921MB

// BinPackRewriteFilePlanner.java, filterFiles() — line 188
protected Iterable<FileScanTask> filterFiles(Iterable<FileScanTask> tasks) {
    return filter(tasks, task -> outsideDesiredFileSizeRange(task));
    // outsideDesiredFileSizeRange: file_size < minFileSize || file_size > maxFileSize
}
```

Source: [SizeBasedFileRewritePlanner.java#L131](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java)

**PyIceberg (new, ~10 lines):**
```python
# Read the writer's target file size from existing table properties
target = table.properties.get(
    "write.target-file-size-bytes",
    512 * 1024 * 1024  # 512MB default — same constant already in pyiceberg
)
min_file_size = int(target * 0.75)
max_file_size = int(target * 1.80)

def should_rewrite(task: FileScanTask) -> bool:
    size = task.file.file_size_in_bytes
    return size < min_file_size or size > max_file_size
```

The 512MB default already exists as `TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT` in [pyiceberg/table/__init__.py#L151](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/table/__init__.py).

### Step C: Bin-Pack into File Groups

Filtered files are packed into groups where each group's total size ≤ `max_file_group_size` (default 100GB). Each group will become one "rewrite job".

**Java:**
```java
// SizeBasedFileRewritePlanner.java, planFileGroups() — line 180
BinPacking.ListPacker<T> packer =
    new BinPacking.ListPacker<>(maxGroupSize, 1, false);
List<List<T>> groups = packer.pack(filteredTasks, ContentScanTask::length);
```

Source: [SizeBasedFileRewritePlanner.java#L180](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java)

**PyIceberg (already exists — `ListPacker`):**
```python
from pyiceberg.utils.bin_packing import ListPacker

MAX_FILE_GROUP_SIZE = 100 * 1024 * 1024 * 1024  # 100GB

packer = ListPacker(target_weight=MAX_FILE_GROUP_SIZE, lookback=1, largest_bin_first=False)
groups = packer.pack(
    items=filtered_tasks,
    weight_func=lambda t: t.file.file_size_in_bytes,
)
```

Source: [pyiceberg/utils/bin_packing.py](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/utils/bin_packing.py) — `ListPacker` is already used by the existing writer.

### Step D: Filter Groups

A group is only worth rewriting if it has enough files to actually produce a benefit. Default: a group needs ≥ 5 files.

**Java:**
```java
// BinPackRewriteFilePlanner.java, filterFileGroups() — line 208
protected Iterable<List<FileScanTask>> filterFileGroups(...) {
    return filter(groups, group ->
        group.size() >= minInputFiles     // default: 5
        || totalSize(group) > targetSize  // group makes at least one full file
    );
}
```

**PyIceberg (new, ~5 lines):**
```python
MIN_INPUT_FILES = 5

def should_rewrite_group(group: list[FileScanTask]) -> bool:
    return (
        len(group) >= MIN_INPUT_FILES
        or sum(t.file.file_size_in_bytes for t in group) > target_file_size
    )
```

### Step E: Read and Rewrite

For each group: read all files into an Arrow table, then write them back using the **existing writer pipeline** — which already handles bin-packing record batches into target-sized output files.

**Java:**
```java
// SparkBinPackFileRewriteRunner.java, doRewrite() — line 42
// READ with Spark
Dataset<Row> scanDF = spark.read().format("iceberg")...load(groupId);
// WRITE with Spark
scanDF.write().format("iceberg")...save(groupId);
```

**PyIceberg (uses existing infrastructure):**

Reading — `ArrowScan` already reads `FileScanTask` lists into Arrow tables:
```python
# pyiceberg/io/pyarrow.py — ArrowScan.to_table() already exists
from pyiceberg.io.pyarrow import ArrowScan

arrow_scan = ArrowScan(table.metadata, table.io, table.schema(), ALWAYS_TRUE, True, None)
arrow_table = arrow_scan.to_table(group)  # reads all files in the group
```

Writing — `_dataframe_to_data_files()` already handles partitioning + bin-packing + Parquet writing:
```python
# pyiceberg/io/pyarrow.py — already exists!
from pyiceberg.io.pyarrow import _dataframe_to_data_files

new_data_files = list(_dataframe_to_data_files(
    table_metadata=table.metadata,
    df=arrow_table,
    io=table.io,
))
# new_data_files is List[DataFile] with proper statistics, partitioning, etc.
```

The phrase **"using the same bin-packing constraints of the writer"** means exactly this: reuse `_dataframe_to_data_files()`, which internally calls `bin_pack_arrow_table()` with `target_file_size` from `write.target-file-size-bytes`.

Source:
- [pyiceberg/io/pyarrow.py#L2873 (_dataframe_to_data_files)](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/io/pyarrow.py) — orchestrates per-partition bin-pack writing
- [pyiceberg/io/pyarrow.py#L2740 (bin_pack_arrow_table)](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/io/pyarrow.py) — splits Arrow table into target-sized batches
- [pyiceberg/io/pyarrow.py#L2668 (write_file)](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/io/pyarrow.py) — writes Parquet with proper statistics

### Step F: Atomic Commit

Delete the old files and add the new files in a single atomic snapshot update. This ensures readers always see a consistent view.

**Java:**
```java
// RewriteDataFilesCommitManager.java, commitFileGroups() — line 89
RewriteFiles rewrite = table.newRewrite();
rewrittenDataFiles.forEach(rewrite::deleteFile);
addedDataFiles.forEach(rewrite::addFile);
rewrite.commit();
```

**PyIceberg (uses existing `_OverwriteFiles`):**
```python
# pyiceberg/table/update/snapshot.py — _OverwriteFiles already exists

with table.transaction() as tx:
    snapshot = tx.update_snapshot().overwrite()
    for old_file in old_data_files:
        snapshot._deleted_data_files.add(old_file)
    for new_file in new_data_files:
        snapshot._added_data_files.append(new_file)
    snapshot.commit()
```

Source: [pyiceberg/table/update/snapshot.py#L580 (_OverwriteFiles)](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/table/update/snapshot.py)

---

## User-Facing API

The entry point goes on the existing `MaintenanceTable` class:

```python
# pyiceberg/table/maintenance.py — existing class, new method

class MaintenanceTable:
    def rewrite_data_files(
        self,
        filter: BooleanExpression = ALWAYS_TRUE,
    ) -> RewriteDataFilesResult:
        """Compact data files matching the filter.

        Finds data files matching the filter, groups them by partition,
        and rewrites them using the writer's bin-packing constraints.

        Args:
            filter: Predicate to select which data files to consider.

        Returns:
            Result with counts of rewritten and added files.
        """
        ...
```

Usage:
```python
from pyiceberg.expressions import EqualTo

table = catalog.load_table("db.my_table")

# Compact all files
result = table.maintenance.rewrite_data_files()

# Compact only files in a specific partition
result = table.maintenance.rewrite_data_files(filter=EqualTo("date", "2024-01-01"))

print(f"Rewrote {result.rewritten_data_files_count} files into {result.added_data_files_count} files")
```

---

## Configuration (Writer Defaults)

The issue says: **"using the same bin-packing constraints of the writer"**. This means all sizing parameters come from existing table properties — no new config needed:

| Parameter | Source | Default |
|---|---|---|
| Target file size | `write.target-file-size-bytes` | 512 MB |
| Min file size (rewrite threshold) | 75% of target | 384 MB |
| Max file size (rewrite threshold) | 180% of target | 921 MB |
| Min input files per group | Hardcoded | 5 |
| Max file group size | Hardcoded | 100 GB |

---

## File Inventory

### Files to Modify
| File | Change |
|---|---|
| [maintenance.py](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/table/maintenance.py) | Add `rewrite_data_files()` method to `MaintenanceTable` |

### Files to Create
| File | Purpose |
|---|---|
| `pyiceberg/table/compaction.py` (suggested) | Planner logic: group by partition, filter files, bin-pack groups |

### Existing Files Used (No Modifications)
| File | What We Use |
|---|---|
| [pyiceberg/table/__init__.py](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/table/__init__.py) | `DataScan.plan_files()`, `TableProperties`, `FileScanTask` |
| [pyiceberg/utils/bin_packing.py](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/utils/bin_packing.py) | `ListPacker` for bin-packing file groups |
| [pyiceberg/io/pyarrow.py](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/io/pyarrow.py) | `ArrowScan`, `_dataframe_to_data_files()`, `write_file()` |
| [pyiceberg/table/update/snapshot.py](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/table/update/snapshot.py) | `_OverwriteFiles` for atomic commit |
| [pyiceberg/expressions/__init__.py](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/expressions/__init__.py) | `BooleanExpression`, `EqualTo`, etc. |