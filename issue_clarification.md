# Issue #1092 Clarification: What Exactly Is Being Asked

This document rigorously explains what the issue description means in concrete code terms, cross-referencing **both** the Java (Iceberg) and Python (PyIceberg) codebases so the translation path is explicit.

---

## The Issue Statement (Verbatim)

> Introduce an API to compact data files. The first version of the API will do the following:
>
> - **take a predicate expression as input parameter to find data files matching the filter** that will be re-written
> - **group data files by partitions and rewrite them using the same bin-packing constraints of the writer**

There are **two concrete requirements** here. Let's break each one down.

---

## Requirement 1: "Take a predicate expression as input parameter to find data files matching the filter"

### What this means

Given a user-provided filter (e.g., `date == "2024-01-01"` or `category == "electronics"`), scan the table's metadata to find **data files** whose contents could match that filter. These are the files that are candidates for rewriting.

> **Key distinction**: We are NOT reading the data inside the files yet. We're using file-level metadata (partition values, column statistics like min/max, null counts) to determine **which files** could contain matching rows. This is metadata-only filtering.

### How it works in Java

The filter flows through 3 levels of pruning:

```
User provides: Expression filter (e.g., Expressions.equal("date", "2024-01-01"))
                    │
                    ▼
┌─────────────────────────────────────────────┐
│  Level 1: MANIFEST FILE pruning             │
│  File: BinPackRewriteFilePlanner.java       │
│  Method: planFileGroups() → table.newScan() │
│                                             │
│  Uses partition summary in manifest file    │
│  header to skip entire manifest files       │
│  that can't contain matching partitions.    │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│  Level 2: PARTITION VALUE pruning           │
│  For each ManifestEntry, check if the       │
│  data file's partition value matches        │
│  the projected partition filter.            │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│  Level 3: COLUMN STATS (metrics) pruning    │
│  Check file-level column statistics         │
│  (min/max bounds, null counts) to see if    │
│  the file could contain matching rows.      │
└─────────────────────────────────────────────┘
                    │
                    ▼
            List<FileScanTask>
            (each has: DataFile + associated DeleteFiles)
```

**Java Code Path:**

```
BinPackRewriteFilePlanner.planFileGroups()                   // line 286
  └→ table.newScan()                                         // creates TableScan
       .filter(this.filter)                                  // sets the predicate
       .caseSensitive(true)
       .ignoreResiduals()
       .planFiles()                                          // returns Iterable<FileScanTask>
```

Source: [BinPackRewriteFilePlanner.java#L286-L310](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java)

### How it works in PyIceberg (ALREADY EXISTS)

The **exact same 3-level pruning** is already implemented:

```
table.scan(row_filter=EqualTo("date", "2024-01-01")).plan_files()
         │
         ▼
┌──────────────────────────────────────────────────────┐
│  Level 1: Manifest file pruning                      │
│  File: pyiceberg/table/__init__.py                   │
│  Method: DataScan.scan_plan_helper()  (line 1989)    │
│                                                      │
│  manifest_evaluators[spec_id](manifest_file)         │
│  → Uses _build_manifest_evaluator() → calls          │
│    manifest_evaluator() from expressions/visitors.py │
│  → Prunes entire manifests using partition summaries │
└──────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────┐
│  Level 2: Partition value pruning                    │
│  File: pyiceberg/table/__init__.py                   │
│  Method: _open_manifest()  (line 1887)               │
│                                                      │
│  partition_evaluators[spec_id](data_file)            │
│  → Checks each entry's partition against the filter  │
└──────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────┐
│  Level 3: Column statistics pruning                  │
│  File: pyiceberg/table/__init__.py                   │
│  Method: _build_metrics_evaluator()  (line 1941)     │
│                                                      │
│  _InclusiveMetricsEvaluator(schema, filter).eval()   │
│  → Checks file-level min/max/null statistics         │
└──────────────────────────────────────────────────────┘
         │
         ▼
    List[FileScanTask]
    (each has: .file (DataFile) + .delete_files + .residual)
```

Source: [pyiceberg/table/__init__.py#L1989-L2102](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/table/__init__.py)

### What you need to build (for Requirement 1)

**Almost nothing new.** You just call the existing `table.scan(row_filter=filter).plan_files()`. The result gives you a list of `FileScanTask` objects, each containing a `DataFile` with:
- `.file_path` — the Parquet file location
- `.file_size_in_bytes` — file size (needed for size-based filtering in Req 2)
- `.partition` — the partition record (needed for grouping in Req 2)
- `.record_count` — row count

---

## Requirement 2: "Group data files by partitions and rewrite them using the same bin-packing constraints of the writer"

This has **three sub-parts**: (A) group by partition, (B) bin-pack within groups, (C) rewrite using the writer's sizing constraints.

### Sub-part A: Group by Partition

**Java:**
```java
// BinPackRewriteFilePlanner.java, line 293
StructLikeMap<List<FileScanTask>> filesByPartition =
    groupByPartition(table, outputSpecId, scannedSpecId, fileScanTasks);
```

This builds a `Map<Partition, List<FileScanTask>>` — files sharing the same partition value are grouped together.

**PyIceberg equivalent (to build):**
```python
from collections import defaultdict
files_by_partition = defaultdict(list)
for task in plan_files_result:
    files_by_partition[task.file.partition].append(task)
```

### Sub-part B: Bin-pack within groups

**Java:**

Within each partition group, files are bin-packed so that each "file group" has total size ≤ `max-file-group-size-bytes` (default 100GB).

```java
// SizeBasedFileRewritePlanner.java, line 180
protected Iterable<List<T>> planFileGroups(Iterable<T> tasks) {
    Iterable<T> filteredTasks = rewriteAll ? tasks : filterFiles(tasks);
    BinPacking.ListPacker<T> packer = new BinPacking.ListPacker<>(maxGroupSize, 1, false);
    List<List<T>> groups = packer.pack(filteredTasks, ContentScanTask::length);
    return rewriteAll ? groups : filterFileGroups(groups);
}
```

Before bin-packing, files are **filtered** — only files that are **too small**, **too large**, or **have too many deletes** are selected:

```java
// BinPackRewriteFilePlanner.java, line 188
protected Iterable<FileScanTask> filterFiles(Iterable<FileScanTask> tasks) {
    return Iterables.filter(tasks, task ->
        outsideDesiredFileSizeRange(task)    // file < min_size OR file > max_size
        || tooManyDeletes(task)             // Phase 2 concern
        || tooHighDeleteRatio(task));        // Phase 2 concern
}
```

After bin-packing, groups are **filtered** — a group must have enough files or enough data:

```java
// BinPackRewriteFilePlanner.java, line 208
protected Iterable<List<FileScanTask>> filterFileGroups(Iterable<List<FileScanTask>> groups) {
    return Iterables.filter(groups, group ->
        group.size() >= minInputFiles       // default: 5
        || enoughContent(group)             // total size > target
        || tooMuchContent(group)            // total size > max
    );
}
```

Source: [SizeBasedFileRewritePlanner.java#L180](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java) and [BinPackRewriteFilePlanner.java#L188-L230](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java)

**PyIceberg equivalent (to build):**

The bin-packing utility **already exists** in pyiceberg:

```python
# pyiceberg/utils/bin_packing.py — already exists!
from pyiceberg.utils.bin_packing import ListPacker

packer = ListPacker(
    target_weight=max_file_group_size_bytes,  # 100GB default
    lookback=1,
    largest_bin_first=False,
)
groups = packer.pack(
    items=filtered_tasks,
    weight_func=lambda task: task.file.file_size_in_bytes,
)
```

Source: [pyiceberg/utils/bin_packing.py](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/utils/bin_packing.py) — `ListPacker` and `PackingIterator` exist and are already used by the writer.

### Sub-part C: "Using the same bin-packing constraints of the writer"

This is the key phrase. **"The writer"** refers to the existing PyIceberg write path. The issue says the rewrite should use the **same file-sizing logic** that the normal `table.append()` / `table.overwrite()` already uses.

**The writer's current pipeline is:**

```
_dataframe_to_data_files()                    # pyiceberg/io/pyarrow.py, line 2873
    │
    │  target_file_size = table_properties["write.target-file-size-bytes"]  # default 512MB
    │
    ├─ if unpartitioned:
    │    bin_pack_arrow_table(df, target_file_size)     # line 2740
    │    └→ PackingIterator(batches, target_weight=target_file_size, ...)
    │    → produces Iterator[List[RecordBatch]]
    │    → each list becomes one WriteTask → one output file
    │
    └─ if partitioned:
         _determine_partitions(spec, schema, df)       # line 2937
         └→ for each partition:
              bin_pack_arrow_table(partition_table, target_file_size)
              → same as above, per partition

Then:
    write_file(io, table_metadata, tasks)              # line 2668
    └→ for each WriteTask:
         write one Parquet file
         collect DataFile metadata (size, stats, partition, etc.)
    → returns Iterator[DataFile]
```

Source: [pyiceberg/io/pyarrow.py#L2740-L2928](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/io/pyarrow.py)

**What "same constraints" means concretely:**

| Constraint | Where it's defined | Value |
|---|---|---|
| Target file size | `TableProperties.WRITE_TARGET_FILE_SIZE_BYTES` | Default 512MB |
| Bin-packing algorithm | `bin_pack_arrow_table()` → `PackingIterator` | First-fit decreasing |
| Parquet writer settings | `_get_parquet_writer_kwargs()` | Compression, page size, dict size, etc. |
| Statistics collection | `data_file_statistics_from_parquet_metadata()` | Column sizes, bounds, counts |

The rewrite should produce output files that are **identical in structure** to what the writer would produce if you were writing the same data fresh.

---

## Putting It All Together: The Full Rewrite Flow

```
                           User calls:
                table.maintenance.rewrite_data_files(filter=...)
                               │
                               ▼
                ┌─────────────────────────────┐
                │  1. SCAN (already exists)    │
                │  table.scan(filter)          │
                │      .plan_files()           │
                │  → List[FileScanTask]        │
                └──────────────┬──────────────┘
                               │
                               ▼
                ┌─────────────────────────────┐
                │  2. GROUP BY PARTITION       │  ◄── NEW CODE
                │  Dict[partition → files]     │
                └──────────────┬──────────────┘
                               │
                               ▼
                ┌─────────────────────────────┐
                │  3. FILTER FILES             │  ◄── NEW CODE
                │  Keep only files that are    │
                │  too small or too large      │
                │  (outside target range)      │
                └──────────────┬──────────────┘
                               │
                               ▼
                ┌─────────────────────────────┐
                │  4. BIN-PACK GROUPS          │  ◄── Uses existing ListPacker
                │  ListPacker(max_group_size)  │
                │  .pack(files, size_func)     │
                └──────────────┬──────────────┘
                               │
                               ▼
                ┌─────────────────────────────┐
                │  5. FILTER GROUPS            │  ◄── NEW CODE
                │  Drop groups with < 5 files  │
                │  (unless enough content)     │
                └──────────────┬──────────────┘
                               │
                               ▼
        ┌───────────────────────────────────────────┐
        │  6. READ + WRITE (per group)               │  ◄── Uses existing writer
        │                                            │
        │  For each file group:                      │
        │    a. Read all files → Arrow Table          │
        │       (using existing ArrowScan.to_table)   │
        │    b. Write back using existing writer:     │
        │       _dataframe_to_data_files(             │
        │           table_metadata, arrow_table, io)  │
        │       → produces new DataFile objects       │
        └──────────────────┬────────────────────────┘
                           │
                           ▼
        ┌───────────────────────────────────────────┐
        │  7. COMMIT (atomic metadata swap)          │  ◄── Uses existing overwrite
        │                                            │
        │  Transaction:                              │
        │    snapshot_update.overwrite()              │
        │      .delete_file(old_file)  # for each    │
        │      .add_file(new_file)     # for each    │
        │      .commit()                             │
        │                                            │
        │  Uses: _OverwriteFiles in                  │
        │  pyiceberg/table/update/snapshot.py         │
        └───────────────────────────────────────────┘
```

### What's NEW vs what ALREADY EXISTS

| Component | Status | Location |
|---|---|---|
| `table.scan(filter).plan_files()` | ✅ **EXISTS** | `pyiceberg/table/__init__.py` → `DataScan.plan_files()` |
| Expression/predicate types | ✅ **EXISTS** | `pyiceberg/expressions/__init__.py` |
| `ListPacker` / `PackingIterator` | ✅ **EXISTS** | `pyiceberg/utils/bin_packing.py` |
| `bin_pack_arrow_table()` | ✅ **EXISTS** | `pyiceberg/io/pyarrow.py` |
| `_dataframe_to_data_files()` | ✅ **EXISTS** | `pyiceberg/io/pyarrow.py` |
| `write_file()` | ✅ **EXISTS** | `pyiceberg/io/pyarrow.py` |
| `_OverwriteFiles` snapshot producer | ✅ **EXISTS** | `pyiceberg/table/update/snapshot.py` |
| `MaintenanceTable` class | ✅ **EXISTS** | `pyiceberg/table/maintenance.py` |
| Group files by partition | 🆕 **NEW** | ~5 lines of code |
| Filter files by size range | 🆕 **NEW** | ~10 lines of code |
| Filter groups by min-input-files | 🆕 **NEW** | ~10 lines of code |
| Orchestrate the full flow | 🆕 **NEW** | ~50-80 lines of code |
| Result dataclass | 🆕 **NEW** | ~10 lines of code |

### Minimal new code sketch

```python
# In pyiceberg/table/maintenance.py

def rewrite_data_files(
    self,
    filter: BooleanExpression = ALWAYS_TRUE,
    options: dict[str, str] | None = None,
) -> RewriteDataFilesResult:
    table = self.tbl
    opts = _parse_options(table, options or {})
    
    # 1. SCAN — find data files matching filter (EXISTING)
    scan = table.scan(row_filter=filter)
    tasks = list(scan.plan_files())
    
    # 2. GROUP BY PARTITION (NEW — trivial)
    files_by_partition: dict[Record, list[FileScanTask]] = defaultdict(list)
    for task in tasks:
        files_by_partition[task.file.partition].append(task)
    
    # 3-5. PLAN — filter + bin-pack + filter groups (NEW — uses existing ListPacker)
    groups = []
    for partition, partition_tasks in files_by_partition.items():
        filtered = [t for t in partition_tasks if _should_rewrite(t, opts)]
        packed = ListPacker(opts.max_group_size, 1, False).pack(
            filtered, lambda t: t.file.file_size_in_bytes
        )
        groups.extend([g for g in packed if _should_rewrite_group(g, opts)])
    
    if not groups:
        return RewriteDataFilesResult(rewritten=0, added=0)
    
    # 6. READ + WRITE per group (EXISTING writer)
    all_old_files, all_new_files = [], []
    for group in groups:
        # Read
        arrow_table = _read_files(table, group)  # uses existing ArrowScan
        # Write using existing writer pipeline
        new_data_files = list(_dataframe_to_data_files(
            table.metadata, arrow_table, table.io
        ))
        all_old_files.extend([t.file for t in group])
        all_new_files.extend(new_data_files)
    
    # 7. COMMIT — atomic swap (EXISTING _OverwriteFiles)
    with table.transaction() as tx:
        snap = tx.update_snapshot().overwrite()
        for old in all_old_files:
            snap._deleted_data_files.add(old)
        for new in all_new_files:
            snap._added_data_files.append(new)
        snap.commit()
    
    return RewriteDataFilesResult(
        rewritten=len(all_old_files), added=len(all_new_files)
    )
```

---

## Java → Python Cross-Reference Table

| Java Class / Method | Python Equivalent | Notes |
|---|---|---|
| `BinPackRewriteFilePlanner.plan()` → `table.newScan().filter().planFiles()` | `table.scan(row_filter=expr).plan_files()` | Already exists |
| `SizeBasedFileRewritePlanner.planFileGroups()` → `BinPacking.ListPacker.pack()` | `ListPacker.pack()` in `utils/bin_packing.py` | Already exists |
| `BinPackRewriteFilePlanner.filterFiles()` → `outsideDesiredFileSizeRange()` | New `_should_rewrite(task, opts)` | ~10 lines |
| `BinPackRewriteFilePlanner.filterFileGroups()` | New `_should_rewrite_group(group, opts)` | ~10 lines |
| `SparkBinPackFileRewriteRunner.doRewrite()` → Spark read + write | `ArrowScan.to_table()` + `_dataframe_to_data_files()` | Both exist |
| `RewriteDataFilesCommitManager.commitFileGroups()` → `table.newRewrite()` | `_OverwriteFiles` via `tx.update_snapshot().overwrite()` | Already exists |
| `TableProperties.WRITE_TARGET_FILE_SIZE_BYTES` (512MB) | `TableProperties.WRITE_TARGET_FILE_SIZE_BYTES` (512MB) | Already exists, same constant |

---

## Summary

The issue is asking for a surprisingly small amount of **new** code. The heavy lifting — scanning with filters, bin-packing record batches, writing parquet files with proper statistics, and committing atomic snapshot updates — **all already exists** in PyIceberg. The new work is essentially:

1. **~5 lines**: Group `FileScanTask`s by partition
2. **~10 lines**: Filter files outside `[min_size, max_size]` range
3. **~10 lines**: Filter groups below `min_input_files` threshold
4. **~50 lines**: Orchestrate the flow (read → write → commit) per group
5. **~10 lines**: Result dataclass and API on `MaintenanceTable`

The total new code is roughly **80-100 lines** (excluding tests).
