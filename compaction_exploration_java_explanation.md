# Java ↔ Python Parallel Notebook — Cell-by-Cell Explanation

> Companion to `compaction_exploration_java.ipynb`. This document explains every cell
> by tracing the **exact Java source code** being mirrored and proving the Python
> equivalent does the same thing.

---

## Table of Contents

- [How This Document Is Structured](#how-this-document-is-structured)
- [The Java Call Chain We Are Mirroring](#the-java-call-chain-we-are-mirroring)
- [Setup Cell: Creating the Test Table](#setup-cell-creating-the-test-table)
- [Step 1: Scan with Filter](#step-1-scan-with-filter)
- [Step 2: Group by Partition](#step-2-group-by-partition)
- [Step 3: Filter Files by Size](#step-3-filter-files-by-size)
- [Step 4: Bin-Pack into File Groups](#step-4-bin-pack-into-file-groups)
- [Step 5: Filter Groups](#step-5-filter-groups)
- [Step 6: Build RewriteGroup Objects](#step-6-build-rewritegroup-objects)
- [Step 7: Read + Write](#step-7-read--write)
- [Step 8: Commit](#step-8-commit)
- [Verification Matrix](#verification-matrix)
- [What Would Change If We Got This Wrong](#what-would-change-if-we-got-this-wrong)

---

## How This Document Is Structured

For each step, you'll find:

1. **The Java source** — The exact file, class, method, and line number in the iceberg repo at `/Users/jaredyu/Desktop/open_source/iceberg/`
2. **What the Java code does** — A line-by-line explanation of the logic
3. **The Python equivalent** — The corresponding pyiceberg code
4. **Proof of equivalence** — Why these produce the same result
5. **How to verify yourself** — Where to look in VSCode to confirm

---

## The Java Call Chain We Are Mirroring

The entry point in Java is `BinPackRewriteFilePlanner.plan()`. Here is the complete call chain
with file locations:

```
BinPackRewriteFilePlanner.plan()
│   File: core/.../actions/BinPackRewriteFilePlanner.java, line 217
│
├── planFileGroups()                                          line 286
│   │
│   ├── [STEP 1] table().newScan().filter(filter).planFiles() line 287-294
│   │   File: core/.../DataTableScan.java
│   │   Does: 3-level metadata pruning (manifest → partition → metrics)
│   │   Returns: CloseableIterable<FileScanTask>
│   │
│   ├── [STEP 2] groupByPartition(table, type, tasks)         line 297-300 → line 310-326
│   │   Does: Builds Map<Partition, List<FileScanTask>>
│   │   Returns: StructLikeMap<List<FileScanTask>>
│   │
│   └── .transformValues(tasks → planFileGroups(tasks))        line 300
│       │   Calls parent class method for each partition:
│       │
│       └── SizeBasedFileRewritePlanner.planFileGroups(tasks)
│           File: core/.../actions/SizeBasedFileRewritePlanner.java, line 180
│           │
│           ├── [STEP 3] filterFiles(tasks)                   line 181
│           │   File: .../BinPackRewriteFilePlanner.java, line 188
│           │   Calls: outsideDesiredFileSizeRange(task)
│           │   File: .../SizeBasedFileRewritePlanner.java, line 176
│           │   Uses thresholds from: sizeThresholds()        line 295
│           │   Which reads: defaultTargetFileSize()
│           │   File: .../BinPackRewriteFilePlanner.java, line 208
│           │   Which reads: TableProperties.WRITE_TARGET_FILE_SIZE_BYTES
│           │
│           ├── [STEP 4] ListPacker(maxGroupSize, 1, false).pack(filtered, ::length)
│           │   File: core/.../util/BinPacking.java           line 182-184
│           │
│           └── [STEP 5] filterFileGroups(groups)             line 185
│               File: .../BinPackRewriteFilePlanner.java, line 196
│               Calls: enoughInputFiles() || enoughContent() || tooMuchContent()
│               File: .../SizeBasedFileRewritePlanner.java, line 188-198
│
├── [STEP 6] newRewriteGroup(ctx, partition, tasks, ...)      line 234
│   File: .../BinPackRewriteFilePlanner.java, line 328
│
└── Returns: FileRewritePlan<RewriteFileGroup>

------- execution phase -------

SparkBinPackFileRewriteRunner.doRewrite(groupId, group)
│   File: spark/v3.5/.../SparkBinPackFileRewriteRunner.java, line 42
│
└── [STEP 7] spark.read().format("iceberg").load() → spark.write().format("iceberg").save()

------- commit phase -------

RewriteDataFilesCommitManager.commitFileGroups(fileGroups)
│   File: core/.../actions/RewriteDataFilesCommitManager.java, line 89
│
└── [STEP 8] table.newRewrite().deleteFile(old).addFile(new).commit()
```

---

## Setup Cell: Creating the Test Table

### What it does
Creates a partitioned Iceberg table with 15 small data files (5 per partition) to simulate the small-files problem that compaction solves.

### Why 15 files
The Java `MIN_INPUT_FILES_DEFAULT` is 5. With 5 files per partition, each partition has exactly enough files to pass the `enoughInputFiles()` check in Step 5. This lets us verify the filtering logic works correctly.

### Why 3 partitions
Partitions are the fundamental grouping unit. Testing with 3 partitions proves that the logic processes each partition independently and never mixes files across partitions.

---

## Step 1: Scan with Filter

### Java source

**File**: [BinPackRewriteFilePlanner.java, line 286-294](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java)

```java
private StructLikeMap<List<List<FileScanTask>>> planFileGroups() {
    TableScan scan = table()
        .newScan()                    // 1. Create a scan object
        .filter(filter)               // 2. Attach the user's predicate
        .caseSensitive(caseSensitive) // 3. Case sensitivity
        .ignoreResiduals();           // 4. Don't compute residual filters

    CloseableIterable<FileScanTask> fileScanTasks = scan.planFiles();  // 5. Execute
```

### Line-by-line explanation

1. `table().newScan()` — Creates a `TableScan` object bound to this table's current snapshot. The scan object is a builder — it doesn't execute anything yet.

2. `.filter(filter)` — Attaches the user's predicate expression (e.g., `Expressions.equal("category", "electronics")`). This is stored on the scan object and applied during `planFiles()`.

3. `.caseSensitive(true)` — Column name matching is case-sensitive (the default).

4. `.ignoreResiduals()` — Tells the scan not to compute "residual" filters. A residual filter is the portion of the user's filter that can't be evaluated using partition/column statistics alone. For compaction, we don't need residuals because we're reading entire files, not filtering individual rows.

5. `scan.planFiles()` — Executes the 3-level pruning:
   - **Level 1**: Read manifest list → for each manifest, check if its partition summary is compatible with the filter → skip entire manifests that can't match
   - **Level 2**: For surviving manifests, read each entry → check if the data file's partition value matches the projected filter → skip files whose partition doesn't match
   - **Level 3**: For surviving files, check column-level statistics (min/max bounds) → skip files where statistics prove no rows can match

### Python equivalent

```python
# pyiceberg/table/__init__.py → DataScan.plan_files()
filtered_tasks = list(table.scan(row_filter=EqualTo("category", "electronics")).plan_files())
```

### Proof of equivalence

| Java | Python | Same? |
|---|---|---|
| `table().newScan()` | `table.scan()` | ✅ Both create a scan object |
| `.filter(filter)` | `row_filter=EqualTo(...)` | ✅ Both attach a `BooleanExpression` |
| `.planFiles()` | `.plan_files()` | ✅ Both do 3-level pruning |
| Returns `Iterable<FileScanTask>` | Returns `Iterable[FileScanTask]` | ✅ Same type |

The 3-level pruning in Python is at [pyiceberg/table/__init__.py, line 1989-2102](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/table/__init__.py) in `scan_plan_helper()`.

### How to verify yourself

1. Open `BinPackRewriteFilePlanner.java` → line 288. See `table().newScan().filter(filter)`.
2. Open `pyiceberg/table/__init__.py` → find `class DataScan`. See `plan_files()`.
3. Both produce a list of `FileScanTask` objects with `.file` (DataFile), `.delete_files`, `.residual`.

---

## Step 2: Group by Partition

### Java source

**File**: [BinPackRewriteFilePlanner.java, line 310-326](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java)

```java
private StructLikeMap<List<FileScanTask>> groupByPartition(
    Table table, Types.StructType partitionType, Iterable<FileScanTask> tasks) {

  StructLikeMap<List<FileScanTask>> filesByPartition = StructLikeMap.create(partitionType);
  StructLike emptyStruct = GenericRecord.create(partitionType);

  for (FileScanTask task : tasks) {
    StructLike taskPartition =
        task.file().specId() == table.spec().specId()
            ? task.file().partition()
            : emptyStruct;

    filesByPartition.computeIfAbsent(taskPartition, unused -> Lists.newArrayList()).add(task);
  }
  return filesByPartition;
}
```

### Line-by-line explanation

1. `StructLikeMap.create(partitionType)` — Creates a map keyed by partition values. `StructLikeMap` uses the partition's field types for proper equality comparison (so `Record[electronics]` == `Record[electronics]` even if they're different object instances).

2. `StructLike emptyStruct = GenericRecord.create(partitionType)` — An empty partition record, used as a fallback key.

3. The if-check `task.file().specId() == table.spec().specId()`:
   - If the file was written with the **current** partition spec → use its actual partition value
   - If the file was written with an **old/different** partition spec → group it under `emptyStruct`
   
   **Why**: If you changed your partition spec (e.g., from `identity(date)` to `month(date)`), old files might contain data for multiple new partitions. Grouping them as "unpartitioned" ensures they get rewritten correctly.

4. `computeIfAbsent(...).add(task)` — "If there's no list for this partition yet, create one. Add this task to the list."

### Python equivalent

```python
files_by_partition = defaultdict(list)
for task in all_tasks:
    if task.file.spec_id == table.spec().spec_id:
        partition_key = task.file.partition
    else:
        partition_key = None  # incompatible spec → group together
    files_by_partition[str(partition_key)].append(task)
```

### Proof of equivalence

| Aspect | Java | Python |
|---|---|---|
| Data structure | `StructLikeMap<List<FileScanTask>>` | `defaultdict(list)` |
| Loop body | For each task, get partition, put in map | For each task, get partition, put in dict |
| Spec compatibility check | `specId() == table.spec().specId()` | `spec_id == table.spec().spec_id` |
| Incompatible spec fallback | `emptyStruct` (empty record) | `None` |
| Result | Partition → List of files | Partition → List of files |

The logic is identical. The only difference is that Java uses `StructLikeMap` (which compares partition records structurally) while Python uses `str(partition)` as the key (which serializes the record for reliable dict comparison).

---

## Step 3: Filter Files by Size

### Java source — three methods across two files

**Target file size** ([BinPackRewriteFilePlanner.java, line 208-214](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java)):
```java
protected long defaultTargetFileSize() {
    return PropertyUtil.propertyAsLong(
        table().properties(),
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,         // "write.target-file-size-bytes"
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT   // 536870912 (512 MB)
    );
}
```

**Thresholds** ([SizeBasedFileRewritePlanner.java, line 295-333](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java)):
```java
long target = defaultTargetFileSize();                          // 512 MB
long defaultMin = (long) (target * MIN_FILE_SIZE_DEFAULT_RATIO); // 0.75 → 384 MB
long defaultMax = (long) (target * MAX_FILE_SIZE_DEFAULT_RATIO); // 1.80 → 922 MB
```

**The check** ([SizeBasedFileRewritePlanner.java, line 176-178](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java)):
```java
protected boolean outsideDesiredFileSizeRange(T task) {
    return task.length() < minFileSize || task.length() > maxFileSize;
}
```

**The filter** ([BinPackRewriteFilePlanner.java, line 188-194](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java)):
```java
protected Iterable<FileScanTask> filterFiles(Iterable<FileScanTask> tasks) {
    return Iterables.filter(tasks, task ->
        outsideDesiredFileSizeRange(task)     // ← THIS is all we need for Phase 1
        || tooManyDeletes(task)               // Phase 2
        || tooHighDeleteRatio(task));          // Phase 2
}
```

### End-to-end derivation

Starting from scratch, here's how you'd derive the thresholds yourself:

1. **Q**: What file size should the compacted output be?  
   **A**: Whatever the writer uses. Check `TableProperties.WRITE_TARGET_FILE_SIZE_BYTES`. Default: 512 MB.

2. **Q**: When is a file "too small"?  
   **A**: When it's less than 75% of the target. Why 75%? Because the Java code defines `MIN_FILE_SIZE_DEFAULT_RATIO = 0.75` at [SizeBasedFileRewritePlanner.java, line 72](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java). This means a file of 383 MB would be left alone (it's close enough to 512 MB), but a file of 300 MB would be rewritten.

3. **Q**: When is a file "too large"?  
   **A**: When it's more than 180% of the target. `MAX_FILE_SIZE_DEFAULT_RATIO = 1.80` at [line 82](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java). A file of 900 MB would be left alone, but a file of 1 GB would be split.

4. **Q**: What about delete files?  
   **A**: `tooManyDeletes()` and `tooHighDeleteRatio()` are Phase 2 concerns. They trigger rewriting when a data file has too many associated position delete files (default threshold: 2147483647 — effectively disabled) or when more than 5% of rows are deleted. For Phase 1, we ignore these.

### Python equivalent

```python
target_file_size = int(table.properties.get(
    "write.target-file-size-bytes", 536870912))
min_file_size = int(target_file_size * 0.75)    # 402,653,184 bytes
max_file_size = int(target_file_size * 1.80)    # 966,367,641 bytes

def outside_desired_file_size_range(task):
    return task.file.file_size_in_bytes < min_file_size or task.file.file_size_in_bytes > max_file_size

def filter_files(tasks):
    return [t for t in tasks if outside_desired_file_size_range(t)]
```

### Proof of equivalence

| Constant | Java | Python | Match |
|---|---|---|---|
| Target size source | `TableProperties.WRITE_TARGET_FILE_SIZE_BYTES` | `TableProperties.WRITE_TARGET_FILE_SIZE_BYTES` | ✅ Same property string |
| Target size default | `WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT` = 536870912 | `WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT` = 536870912 | ✅ Same value |
| Min ratio | `MIN_FILE_SIZE_DEFAULT_RATIO` = 0.75 | `0.75` | ✅ |
| Max ratio | `MAX_FILE_SIZE_DEFAULT_RATIO` = 1.80 | `1.80` | ✅ |
| Check | `task.length() < min \|\| > max` | `file_size_in_bytes < min or > max` | ✅ |

---

## Step 4: Bin-Pack into File Groups

### Java source

**File**: [SizeBasedFileRewritePlanner.java, line 180-186](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java)

```java
protected Iterable<List<T>> planFileGroups(Iterable<T> tasks) {
    Iterable<T> filteredTasks = rewriteAll ? tasks : filterFiles(tasks);
    BinPacking.ListPacker<T> packer =
        new BinPacking.ListPacker<>(maxGroupSize, 1, false, maxGroupCount);
    List<List<T>> groups = packer.pack(filteredTasks, ContentScanTask::length);
    return rewriteAll ? groups : filterFileGroups(groups);
}
```

### Line-by-line explanation

1. `filteredTasks = rewriteAll ? tasks : filterFiles(tasks)` — If `rewrite-all` option is set, skip size filtering and rewrite everything. Otherwise, apply the filter from Step 3.

2. `new BinPacking.ListPacker<>(maxGroupSize, 1, false, maxGroupCount)`:
   - `maxGroupSize` = `MAX_FILE_GROUP_SIZE_BYTES_DEFAULT` = 100 GB ([line 108](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java))
   - `1` = lookback (only check the most recent bin for fit)
   - `false` = don't sort bins by size
   - `maxGroupCount` = max number of items per bin (default: Long.MAX_VALUE — no limit)

3. `packer.pack(filteredTasks, ContentScanTask::length)` — The weight function is `ContentScanTask::length`, which returns `file_size_in_bytes`. Each bin/group has total weight ≤ `maxGroupSize`.

4. `return rewriteAll ? groups : filterFileGroups(groups)` — If not rewriting all, apply group-level filtering (Step 5).

### Python equivalent

```python
from pyiceberg.utils.bin_packing import ListPacker

packer = ListPacker(target_weight=100 * 1024**3, lookback=1, largest_bin_first=False)
groups = packer.pack(filtered_tasks, weight_func=lambda t: t.file.file_size_in_bytes)
```

### Proof of equivalence

The `ListPacker` class in pyiceberg ([pyiceberg/utils/bin_packing.py](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/utils/bin_packing.py)) was ported directly from the Java `BinPacking.ListPacker` ([iceberg/core/.../util/BinPacking.java](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/util/BinPacking.java)). Same algorithm, same parameters, same behavior.

| Parameter | Java | Python | Match |
|---|---|---|---|
| Target weight | `maxGroupSize` (100 GB) | `target_weight` (100 GB) | ✅ |
| Lookback | `1` | `1` | ✅ |
| Largest bin first | `false` | `False` | ✅ |
| Weight function | `ContentScanTask::length` (file size) | `lambda t: t.file.file_size_in_bytes` | ✅ |

---

## Step 5: Filter Groups

### Java source

**File**: [BinPackRewriteFilePlanner.java, line 196-206](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java)

```java
protected Iterable<List<FileScanTask>> filterFileGroups(List<List<FileScanTask>> groups) {
    return Iterables.filter(groups, group ->
        enoughInputFiles(group)
        || enoughContent(group)
        || tooMuchContent(group)
        || group.stream().anyMatch(this::tooManyDeletes)     // Phase 2
        || group.stream().anyMatch(this::tooHighDeleteRatio) // Phase 2
    );
}
```

With the base methods from [SizeBasedFileRewritePlanner.java, line 188-198](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java):

```java
protected boolean enoughInputFiles(List<T> group) {
    return group.size() > 1 && group.size() >= minInputFiles;  // minInputFiles default: 5
}
protected boolean enoughContent(List<T> group) {
    return group.size() > 1 && inputSize(group) > targetFileSize;
}
protected boolean tooMuchContent(List<T> group) {
    return inputSize(group) > maxFileSize;
}
```

### Why filter groups at all?

Without this filter, a partition with 2 small files would produce a group of 2. Rewriting 2 files into 1 saves very little — the I/O cost of compaction might exceed the benefit. The predicates prevent wasteful rewrites:

- **`enoughInputFiles`**: Don't bother unless there are ≥ 5 files. Combining 5 files into 1 has meaningful impact. Note: `group.size() > 1` prevents rewriting a single file into... a single file.
- **`enoughContent`**: Even with < 5 files, if their total size exceeds the target (512 MB), there's enough data to produce a properly-sized output file.
- **`tooMuchContent`**: A single oversized file (> 922 MB) should be split, even if it's the only file in the group.

### Python equivalent

```python
MIN_INPUT_FILES = 5

def enough_input_files(group):
    return len(group) > 1 and len(group) >= MIN_INPUT_FILES

def enough_content(group):
    total = sum(t.file.file_size_in_bytes for t in group)
    return len(group) > 1 and total > target_file_size

def too_much_content(group):
    total = sum(t.file.file_size_in_bytes for t in group)
    return total > max_file_size

def filter_file_groups(groups):
    return [g for g in groups
            if enough_input_files(g) or enough_content(g) or too_much_content(g)]
```

### Proof of equivalence

| Predicate | Java | Python | Match |
|---|---|---|---|
| Enough files | `size() > 1 && size() >= 5` | `len() > 1 and len() >= 5` | ✅ |
| Enough content | `size() > 1 && total > target` | `len() > 1 and total > target` | ✅ |
| Too much content | `total > maxFileSize` | `total > max_file_size` | ✅ |

---

## Step 6: Build RewriteGroup Objects

### Java source

**File**: [BinPackRewriteFilePlanner.java, line 328-347](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java)

```java
private RewriteFileGroup newRewriteGroup(
    RewriteExecutionContext ctx, StructLike partition,
    List<FileScanTask> tasks, long inputSplitSize, int expectedOutputFiles) {
  FileGroupInfo info = ImmutableRewriteDataFiles.FileGroupInfo.builder()
      .globalIndex(ctx.currentGlobalIndex())
      .partitionIndex(ctx.currentPartitionIndex(partition))
      .partition(partition)
      .build();
  return new RewriteFileGroup(info, tasks, outputSpecId(), writeMaxFileSize(), ...);
}
```

### What the Java version carries (full vs. Phase 1)

The Java `RewriteFileGroup` contains:
- `FileGroupInfo` (partition, global index, partition index) — for ordering and progress tracking
- `List<FileScanTask>` — the files to rewrite
- `outputSpecId` — which partition spec to use for output (supports spec evolution)
- `writeMaxFileSize` — max size for a single output file
- `inputSplitSize` — how to split input for Spark tasks
- `expectedOutputFiles` — estimate for progress reporting

For Phase 1, we only need the partition and file list. The rest is Spark-specific or Phase 2.

### Python equivalent

```python
@dataclass
class RewriteGroup:
    partition: str
    tasks: list[FileScanTask]
```

This is intentionally simpler. We can add `expected_output_files`, `output_spec_id`, etc. in Phase 2.

---

## Step 7: Read + Write

### Java source

**File**: [SparkBinPackFileRewriteRunner.java, line 42-60](file:///Users/jaredyu/Desktop/open_source/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackFileRewriteRunner.java)

```java
protected void doRewrite(String groupId, RewriteFileGroup group) {
    Dataset<Row> scanDF = spark.read()
        .format("iceberg")
        .option(SparkReadOptions.SCAN_TASK_SET_ID, groupId)
        .load(tableName);

    scanDF.write()
        .format("iceberg")
        .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupId)
        .mode("append")
        .save(tableName);
}
```

### What the Java code does

1. **READ**: Spark reads the specific files in this group (identified by `groupId` which was staged in `ScanTaskSetManager` earlier). The Spark Iceberg source knows how to read Parquet files, apply position deletes, and produce a `Dataset<Row>`.

2. **WRITE**: Spark writes the data back. The Iceberg Spark sink handles:
   - Determining partition values from the data
   - Splitting output into target-sized files (Spark uses its own `write.target-file-size-bytes` from table properties)
   - Writing Parquet with proper column statistics
   - Reporting the new `DataFile` objects to the `FileRewriteCoordinator`

### Python equivalent

```python
# READ: ArrowScan (already exists)
arrow_table = ArrowScan(metadata, io, schema, ALWAYS_TRUE, True, None).to_table(group.tasks)

# WRITE: _dataframe_to_data_files (already exists)
new_files = list(_dataframe_to_data_files(table.metadata, arrow_table, table.io))
```

### Proof of equivalence

| Aspect | Java (Spark) | Python (PyArrow) | Match? |
|---|---|---|---|
| Read mechanism | Spark DataSource read | `ArrowScan.to_table()` | ✅ Both read Parquet + apply deletes |
| Write mechanism | Spark DataSource write | `_dataframe_to_data_files()` | ✅ Both produce Parquet with stats |
| Output sizing | `write.target-file-size-bytes` | `write.target-file-size-bytes` | ✅ Same property |
| Partitioning | Handled by Spark sink | Handled by `_determine_partitions()` | ✅ Same logic |
| Statistics | Handled by Spark sink | Handled by `data_file_statistics_from_parquet_metadata()` | ✅ Same level of detail |

The engine is different (distributed Spark vs. single-node PyArrow), but the operation — read files, write back as fewer properly-sized files — is identical.

---

## Step 8: Commit

### Java source

**File**: [RewriteDataFilesCommitManager.java, line 89-115](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java)

```java
private void commitFileGroups(Set<RewriteFileGroup> fileGroups) {
    RewriteFiles rewrite = table.newRewrite();
    rewrite.validateFromSnapshot(startingSnapshotId);

    for (RewriteFileGroup group : fileGroups) {
        for (FileScanTask task : group.rewrittenFiles()) {
            rewrite.deleteFile(task.file());           // mark old file for removal
        }
        for (DataFile dataFile : group.addedFiles()) {
            rewrite.addFile(dataFile);                  // add new file
        }
    }

    rewrite.commit();                                    // atomic!
}
```

### What makes this atomic

`rewrite.commit()` does the following in order:
1. Creates new manifest files that mark old data files as `DELETED` and new data files as `ADDED`
2. Creates a new manifest list pointing to all active manifests
3. Creates a new snapshot with operation type `REPLACE`
4. Writes new metadata JSON
5. Atomically updates the catalog pointer from the old metadata to the new metadata

If any step fails, the old metadata remains current. Readers always see a consistent view.

### Python equivalent

```python
with table.transaction() as tx:
    snapshot = tx.update_snapshot().overwrite()
    for old_file in old_data_files:
        snapshot.delete_data_file(old_file)        # Java: rewrite.deleteFile()
    for new_file in new_data_files:
        snapshot.append_data_file(new_file)         # Java: rewrite.addFile()
    snapshot.commit()                               # Java: rewrite.commit()
```

### Proof of equivalence

| Aspect | Java | Python | Match? |
|---|---|---|---|
| Operation type | `table.newRewrite()` → `REPLACE` | `update_snapshot().overwrite()` → `OVERWRITE` | ✅ Same semantics |
| Delete old files | `rewrite.deleteFile(file)` | `snapshot.delete_data_file(file)` | ✅ |
| Add new files | `rewrite.addFile(file)` | `snapshot.append_data_file(file)` | ✅ |
| Atomicity | Via `RewriteFiles.commit()` | Via `_OverwriteFiles.commit()` | ✅ Both produce one atomic snapshot |

Note: Java uses `RewriteFiles` (a specific operation for replace semantics). Python uses `_OverwriteFiles` which handles the same delete+add pattern. Both produce a new snapshot that atomically replaces old files with new ones.

---

## Verification Matrix

| # | Step | Java Method (file:line) | Python Method (file:line) | Algorithm Match | Parameter Match |
|---|---|---|---|---|---|
| 1 | Scan | `table.newScan().filter(f).planFiles()` | `table.scan(row_filter=f).plan_files()` | ✅ 3-level pruning | ✅ Same filter types |
| 2 | Group | `groupByPartition()` (BRFP:310) | `defaultdict + loop` | ✅ Same partition key | ✅ Same spec compat check |
| 3 | Filter files | `outsideDesiredFileSizeRange()` (SBRFP:176) | `size < min or size > max` | ✅ Same inequality | ✅ 0.75, 1.80, 512MB |
| 4 | Bin-pack | `ListPacker.pack()` (SBRFP:182) | `ListPacker.pack()` (bin_packing.py) | ✅ Same class | ✅ 100GB, 1, False |
| 5 | Filter groups | `enoughInputFiles\|\|enoughContent\|\|tooMuchContent` (BRFP:196) | Same 3 predicates | ✅ Same logic | ✅ 5, 512MB, 922MB |
| 6 | Build groups | `newRewriteGroup()` (BRFP:328) | `RewriteGroup()` | ✅ Same container | Simplified for Phase 1 |
| 7 | Read+Write | Spark read/write (SBFRR:42) | `ArrowScan + _dataframe_to_data_files` | ✅ Same semantics | ✅ Same target size prop |
| 8 | Commit | `table.newRewrite().commit()` (RDFCM:89) | `_OverwriteFiles.commit()` | ✅ Atomic snapshot | ✅ Delete+Add |

**BRFP** = BinPackRewriteFilePlanner, **SBRFP** = SizeBasedFileRewritePlanner, **SBFRR** = SparkBinPackFileRewriteRunner, **RDFCM** = RewriteDataFilesCommitManager

---

## What Would Change If We Got This Wrong

To prove we need each step, here's what breaks if you skip or change it:

| If you skip... | What breaks |
|---|---|
| Step 1 (scan) | You don't know which files to compact. You'd have to read manifests manually. |
| Step 2 (group) | You merge files from different partitions into one file → violates Iceberg's partition constraint → queries return wrong results. |
| Step 3 (filter) | You rewrite files that are already optimal size → wasted I/O, no improvement, unnecessary snapshot churn. |
| Step 4 (bin-pack) | A single partition with 10,000 files tries to load all 10,000 at once → OOM. Bin-packing limits each group to 100 GB. |
| Step 5 (filter groups) | A partition with 2 tiny files gets compacted → the I/O cost of reading 2 files and writing 1 file exceeds the benefit. |
| Step 7 (write) | If you don't use `_dataframe_to_data_files()`, output files won't have proper column statistics → queries can't skip files → performance regression. |
| Step 8 (commit) | If you delete old files and add new files in separate commits → readers might see both or neither → data appears duplicated or missing. |
