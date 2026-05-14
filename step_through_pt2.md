# Step-Through: Tracing the Two Issue Requirements in Java/Spark

> This traces the **exact function call chains** for the two operations described in Issue #1092:
>
> 1. "Take a predicate expression as input parameter to find data files matching the filter"
> 2. "Group data files by partitions and rewrite them using the same bin-packing constraints of the writer"
>
> All file paths are relative to `/Users/jaredyu/Desktop/open_source/iceberg`.

---

## Table of Contents
- [Requirement 1: Predicate-Based File Selection](#requirement-1-predicate-based-file-selection)
    - [Step 1.1: The filter is set](#step-11-the-filter-is-set)
    - [Step 1.2: The scan is created with the filter](#step-12-the-scan-is-created-with-the-filter)
    - [Step 1.3: planFiles() runs 3-level pruning](#step-13-planfiles-runs-3-level-pruning)
    - [Step 1.4: Result — a list of FileScanTask objects](#step-14-result--a-list-of-filesscantask-objects)
- [Requirement 2: Partition-Grouped Bin-Pack Rewriting](#requirement-2-partition-grouped-bin-pack-rewriting)
    - [Step 2.1: groupByPartition()](#step-21-groupbypartition)
    - [Step 2.2: planFileGroups() per partition](#step-22-planfilegroups-per-partition)
    - [Step 2.3: filterFiles() — size threshold filtering](#step-23-filterfiles--size-threshold-filtering)
    - [Step 2.4: outsideDesiredFileSizeRange() — the actual check](#step-24-outsidedesiredfilesizerange--the-actual-check)
    - [Step 2.5: Where do the thresholds come from?](#step-25-where-do-the-thresholds-come-from)
    - [Step 2.6: BinPacking.ListPacker.pack() — the bin-packing](#step-26-binpackinglistpackerpack--the-bin-packing)
    - [Step 2.7: filterFileGroups() — drop groups with too few files](#step-27-filterfilegroups--drop-groups-with-too-few-files)
    - [Step 2.8: Result — RewriteFileGroup objects](#step-28-result--rewritefilegroup-objects)
- [Combined Call Chain Diagram](#combined-call-chain-diagram)
- [VSCode Navigation Cheat Sheet](#vscode-navigation-cheat-sheet)

---

## Requirement 1: Predicate-Based File Selection

> "take a predicate expression as input parameter to find data files matching the filter"

### Step 1.1: The filter is set

**File**: `core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java`
**Line**: 286-288

```java
private StructLikeMap<List<List<FileScanTask>>> planFileGroups() {
    TableScan scan =
        table().newScan().filter(filter).caseSensitive(caseSensitive).ignoreResiduals();
```

The `filter` variable here is the user's predicate expression. It was passed in earlier:

**File**: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/RewriteDataFilesProcedure.java`
**Line**: 126-130

```java
// "where" clause from CALL statement becomes the filter
String where = args.isNullAt(2) ? null : args.getString(2);
if (where != null) {
    action.filter(SparkFilters.convert(where));  // sets BooleanExpression filter
}
```

**VSCode**: Open `BinPackRewriteFilePlanner.java`, go to line 288. `Cmd+Click` on `filter` to see where it's assigned. Then `Cmd+Click` on `filter()` to jump to the `TableScan` interface.

---

### Step 1.2: The scan is created with the filter

**File**: `core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java`
**Line**: 294

```java
CloseableIterable<FileScanTask> fileScanTasks = scan.planFiles();
```

This is the critical call. `scan.planFiles()` applies the filter and returns only files whose metadata indicates they **could** contain matching rows.

**VSCode**: `Cmd+Click` on `planFiles()` → this jumps to the `TableScan` interface. To see the actual implementation, press `Cmd+F12` (Go to Implementation) to find `DataTableScan.planFiles()`.

---

### Step 1.3: planFiles() runs 3-level pruning

**File**: `core/src/main/java/org/apache/iceberg/DataTableScan.java`

Inside `planFiles()`, three levels of pruning happen:

```
Level 1: ManifestGroup → evaluates partition summaries on manifest files
         Skips manifests where no partition can match the filter

Level 2: Partition evaluator → for each ManifestEntry, checks data file's
         partition value against the projected partition filter

Level 3: InclusiveMetricsEvaluator → checks file-level column statistics
         (min/max bounds, null counts) against the filter
```

You don't need to trace into `DataTableScan` for the PyIceberg implementation because PyIceberg already has this exact logic in `DataScan.plan_files()` at `pyiceberg/table/__init__.py:2090`.

---

### Step 1.4: Result — a list of FileScanTask objects

After `scan.planFiles()` returns, we have:

```java
CloseableIterable<FileScanTask> fileScanTasks = scan.planFiles();
// Each FileScanTask contains:
//   .file()        → DataFile (path, size, partition, record count, stats)
//   .deletes()     → List<DeleteFile> (associated position delete files)
//   .length()      → long (file size in bytes)
//   .file().specId() → int (partition spec this file was written with)
```

**This is the output of Requirement 1.** The filter was applied, and we now have a filtered list of data files to consider rewriting.

---

## Requirement 2: Partition-Grouped Bin-Pack Rewriting

> "group data files by partitions and rewrite them using the same bin-packing constraints of the writer"

Starting from where Requirement 1 left off (we have `fileScanTasks`):

### Step 2.1: groupByPartition()

**File**: `core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java`
**Lines**: 297-300

```java
Types.StructType partitionType = table().spec().partitionType();
StructLikeMap<List<FileScanTask>> filesByPartition =
    groupByPartition(table(), partitionType, fileScanTasks);
```

**VSCode**: `Cmd+Click` on `groupByPartition` → jumps to line 310 in the same file:

**Lines**: 310-326

```java
private StructLikeMap<List<FileScanTask>> groupByPartition(
    Table table, Types.StructType partitionType, Iterable<FileScanTask> tasks) {
  StructLikeMap<List<FileScanTask>> filesByPartition = StructLikeMap.create(partitionType);
  StructLike emptyStruct = GenericRecord.create(partitionType);

  for (FileScanTask task : tasks) {
    // If a task uses an incompatible partition spec the data inside could contain values
    // which belong to multiple partitions in the current spec. Treating all such files as
    // un-partitioned and grouping them together helps to minimize new files made.
    StructLike taskPartition =
        task.file().specId() == table.spec().specId() ? task.file().partition() : emptyStruct;

    filesByPartition.computeIfAbsent(taskPartition, unused -> Lists.newArrayList()).add(task);
  }

  return filesByPartition;
}
```

**What it does**: Iterates over all filtered files and puts them into a `Map<Partition → List<File>>`. Note the edge case: files with an old/incompatible partition spec are grouped under an empty struct (treated as unpartitioned).

**Result**: `StructLikeMap<List<FileScanTask>>` — files bucketed by partition.

---

### Step 2.2: planFileGroups() per partition

**File**: `core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java`
**Line**: 300

```java
return filesByPartition.transformValues(tasks -> ImmutableList.copyOf(planFileGroups(tasks)));
```

For each partition's file list, it calls `planFileGroups(tasks)`. This is the parent class method:

**VSCode**: `Cmd+Click` on `planFileGroups` → jumps to `SizeBasedFileRewritePlanner.java`:

**File**: `core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java`
**Lines**: 180-186

```java
protected Iterable<List<T>> planFileGroups(Iterable<T> tasks) {
    Iterable<T> filteredTasks = rewriteAll ? tasks : filterFiles(tasks);     // ← Step 2.3
    BinPacking.ListPacker<T> packer =
        new BinPacking.ListPacker<>(maxGroupSize, 1, false, maxGroupCount); // ← Step 2.6
    List<List<T>> groups = packer.pack(filteredTasks, ContentScanTask::length);
    return rewriteAll ? groups : filterFileGroups(groups);                   // ← Step 2.7
}
```

**This single method is the heart of "using the same bin-packing constraints."** Three things happen:
1. `filterFiles(tasks)` → keep only files that need rewriting (Step 2.3)
2. `packer.pack(...)` → bin-pack filtered files into groups (Step 2.6)
3. `filterFileGroups(groups)` → drop groups that are too small to bother (Step 2.7)

---

### Step 2.3: filterFiles() — size threshold filtering

**VSCode**: From line 181, `Cmd+Click` on `filterFiles` → jumps to `BinPackRewriteFilePlanner.java`:

**File**: `core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java`
**Lines**: 188-194

```java
@Override
protected Iterable<FileScanTask> filterFiles(Iterable<FileScanTask> tasks) {
    return Iterables.filter(
        tasks,
        task ->
            outsideDesiredFileSizeRange(task)      // ← Step 2.4
            || tooManyDeletes(task)                 // Phase 2 concern (ignore for now)
            || tooHighDeleteRatio(task));            // Phase 2 concern (ignore for now)
}
```

**For the issue scope**, only `outsideDesiredFileSizeRange` matters. The delete-related checks are Phase 2.

---

### Step 2.4: outsideDesiredFileSizeRange() — the actual check

**VSCode**: `Cmd+Click` on `outsideDesiredFileSizeRange` → jumps to `SizeBasedFileRewritePlanner.java`:

**File**: `core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java`
**Lines**: 176-178

```java
protected boolean outsideDesiredFileSizeRange(T task) {
    return task.length() < minFileSize || task.length() > maxFileSize;
}
```

**This is the key decision**: a file is rewritten if its size is below `minFileSize` (too small) or above `maxFileSize` (too large).

---

### Step 2.5: Where do the thresholds come from?

**VSCode**: `Cmd+Click` on `minFileSize` or `maxFileSize` field declarations → they're set in `init()`:

**File**: `core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java`
**Lines**: 156-160 (init method)

```java
public void init(Map<String, String> options) {
    Map<String, Long> sizeThresholds = sizeThresholds(options);
    this.targetFileSize = sizeThresholds.get(TARGET_FILE_SIZE_BYTES);
    this.minFileSize = sizeThresholds.get(MIN_FILE_SIZE_BYTES);
    this.maxFileSize = sizeThresholds.get(MAX_FILE_SIZE_BYTES);
    // ...
```

**VSCode**: `Cmd+Click` on `sizeThresholds(options)` → jumps to line 295:

**Lines**: 295-333

```java
private Map<String, Long> sizeThresholds(Map<String, String> options) {
    long target =
        PropertyUtil.propertyAsLong(options, TARGET_FILE_SIZE_BYTES, defaultTargetFileSize());
    //                                                              ↑ Falls through to:

    long defaultMin = (long) (target * MIN_FILE_SIZE_DEFAULT_RATIO);   // 0.75 → 384 MB
    long min = PropertyUtil.propertyAsLong(options, MIN_FILE_SIZE_BYTES, defaultMin);

    long defaultMax = (long) (target * MAX_FILE_SIZE_DEFAULT_RATIO);   // 1.80 → 921 MB
    long max = PropertyUtil.propertyAsLong(options, MAX_FILE_SIZE_BYTES, defaultMax);
    // ...
```

And `defaultTargetFileSize()` resolves to the **writer's** table property:

**VSCode**: `Cmd+Click` on `defaultTargetFileSize()` → jumps to `BinPackRewriteFilePlanner.java`:

**File**: `core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java`
**Lines**: 208-214

```java
@Override
protected long defaultTargetFileSize() {
    return PropertyUtil.propertyAsLong(
        table().properties(),
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,        // "write.target-file-size-bytes"
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT  // 512 * 1024 * 1024 (512 MB)
    );
}
```

**This is what "the same bin-packing constraints of the writer" means.** The target file size defaults to `write.target-file-size-bytes` — the **same property** the writer uses when producing files during `INSERT` or `APPEND`.

---

### Step 2.6: BinPacking.ListPacker.pack() — the bin-packing

Back to `SizeBasedFileRewritePlanner.planFileGroups()` at line 182:

**File**: `core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java`
**Lines**: 182-184

```java
BinPacking.ListPacker<T> packer =
    new BinPacking.ListPacker<>(maxGroupSize, 1, false, maxGroupCount);
    //                          ↑ 100 GB      ↑ lookback=1
    //                                            ↑ largest_bin_first=false
List<List<T>> groups = packer.pack(filteredTasks, ContentScanTask::length);
    //                                            ↑ weight = file size in bytes
```

**What it does**: Takes filtered files and packs them into groups. Each group has total size ≤ `maxGroupSize` (100 GB default). The weight function is `ContentScanTask::length` — i.e., `file_size_in_bytes`.

**VSCode**: `Cmd+Click` on `BinPacking.ListPacker` → jumps to:

**File**: `core/src/main/java/org/apache/iceberg/util/BinPacking.java`

This is the same algorithm that's already implemented in PyIceberg at `pyiceberg/utils/bin_packing.py` as `ListPacker`.

**PyIceberg equivalent** (already exists):
```python
# pyiceberg/utils/bin_packing.py
packer = ListPacker(target_weight=100GB, lookback=1, largest_bin_first=False)
groups = packer.pack(filtered_tasks, lambda t: t.file.file_size_in_bytes)
```

---

### Step 2.7: filterFileGroups() — drop groups with too few files

Back to `SizeBasedFileRewritePlanner.planFileGroups()` at line 185:

```java
return rewriteAll ? groups : filterFileGroups(groups);
```

**VSCode**: `Cmd+Click` on `filterFileGroups` → jumps to `BinPackRewriteFilePlanner.java`:

**File**: `core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java`
**Lines**: 196-206

```java
@Override
protected Iterable<List<FileScanTask>> filterFileGroups(List<List<FileScanTask>> groups) {
    return Iterables.filter(
        groups,
        group ->
            enoughInputFiles(group)               // group.size() >= 5
            || enoughContent(group)               // totalSize > targetFileSize
            || tooMuchContent(group)              // totalSize > maxFileSize
            || group.stream().anyMatch(this::tooManyDeletes)    // Phase 2
            || group.stream().anyMatch(this::tooHighDeleteRatio) // Phase 2
    );
}
```

**VSCode**: `Cmd+Click` on `enoughInputFiles` → jumps to `SizeBasedFileRewritePlanner.java`:

**Lines**: 188-189
```java
protected boolean enoughInputFiles(List<T> group) {
    return group.size() > 1 && group.size() >= minInputFiles;  // default: 5
}
```

**Lines**: 192-193
```java
protected boolean enoughContent(List<T> group) {
    return group.size() > 1 && inputSize(group) > targetFileSize;  // total > 512MB
}
```

**What it does**: A group is only kept if it has ≥ 5 files, OR if its total data exceeds the target file size (meaning it can produce at least one optimally-sized output file).

---

### Step 2.8: Result — RewriteFileGroup objects

Back in `BinPackRewriteFilePlanner.plan()` at line 217:

**File**: `core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java`
**Lines**: 217-264

```java
public FileRewritePlan<...> plan() {
    StructLikeMap<List<List<FileScanTask>>> plan = planFileGroups();  // Steps 2.1-2.7
    // ...
    plan.entrySet().stream()
        .filter(e -> !e.getValue().isEmpty())
        .forEach(entry -> {
            StructLike partition = entry.getKey();
            entry.getValue().forEach(fileScanTasks -> {
                selectedFileGroups.add(
                    newRewriteGroup(ctx, partition, fileScanTasks, ...));  // line 234
            });
        });
    // Returns: FileRewritePlan with ordered RewriteFileGroup objects
}
```

Each `RewriteFileGroup` contains:
- The partition value
- The list of `FileScanTask` objects to rewrite
- Metadata for execution (expected output files, split size)

**This is the output of Requirement 2's planning phase.** After this, the runner reads + rewrites the files, and the commit manager does the atomic swap.

---

## Combined Call Chain Diagram

```
BinPackRewriteFilePlanner.plan()                    ← ENTRY POINT
│
├── planFileGroups()                                 ← line 286
│   │
│   ├── table().newScan()                            ← Req 1: Create scan
│   │     .filter(filter)                            ← Req 1: Apply predicate
│   │     .planFiles()                               ← Req 1: 3-level pruning
│   │     → CloseableIterable<FileScanTask>          ← Req 1: RESULT
│   │
│   ├── groupByPartition(table, type, tasks)         ← Req 2: Group by partition
│   │     → StructLikeMap<List<FileScanTask>>        ← line 310-326
│   │
│   └── .transformValues(tasks → planFileGroups(tasks))  ← line 300
│       │
│       └── SizeBasedFileRewritePlanner              ← Req 2: Per-partition planning
│           .planFileGroups(tasks)                   ← line 180
│           │
│           ├── filterFiles(tasks)                   ← line 181
│           │   └── outsideDesiredFileSizeRange()     ← line 176
│           │       size < minFileSize(384MB)         ← from sizeThresholds() line 299
│           │       OR size > maxFileSize(921MB)      ← from sizeThresholds() line 302
│           │       where target = write.target-file-size-bytes  ← line 210-213
│           │
│           ├── ListPacker(100GB, 1, false)           ← line 182-183
│           │   .pack(filtered, ::length)             ← line 184
│           │   → List<List<FileScanTask>>            ← grouped by bin size
│           │
│           └── filterFileGroups(groups)              ← line 185
│               ├── enoughInputFiles: size >= 5       ← line 188
│               └── enoughContent: total > target     ← line 192
│
└── newRewriteGroup(...) for each group              ← line 234
    → List<RewriteFileGroup>                         ← FINAL RESULT
```

---

## VSCode Navigation Cheat Sheet

### Files to Open

| # | File | Key Lines |
|---|---|---|
| 1 | `core/.../actions/BinPackRewriteFilePlanner.java` | 188, 196, 208, 217, 286, 310 |
| 2 | `core/.../actions/SizeBasedFileRewritePlanner.java` | 62-108, 156, 176, 180, 200, 295 |
| 3 | `core/.../util/BinPacking.java` | `ListPacker.pack()` |

### Navigation Steps

1. **Open** `BinPackRewriteFilePlanner.java`, go to **line 286** (`planFileGroups()`)
2. **Line 288**: See `table().newScan().filter(filter)` — this is **Req 1** (the predicate)
3. **Line 294**: See `.planFiles()` — this executes the 3-level pruning
4. **Line 298-300**: See `groupByPartition(...)` — this is **Req 2, Step A** (group by partition)
5. **Line 300**: `Cmd+Click` on `planFileGroups(tasks)` → jumps to `SizeBasedFileRewritePlanner.java` **line 180**
6. **Line 181**: `Cmd+Click` on `filterFiles` → jumps back to `BinPackRewriteFilePlanner` **line 188**
7. **Line 193**: `Cmd+Click` on `outsideDesiredFileSizeRange` → jumps to `SizeBasedFileRewritePlanner` **line 176**
8. **Go Back** (`Ctrl+-`) to `SizeBasedFileRewritePlanner` line 182: see `BinPacking.ListPacker`
9. **Line 185**: `Cmd+Click` on `filterFileGroups` → jumps to `BinPackRewriteFilePlanner` **line 196**
10. **Line 201**: `Cmd+Click` on `enoughInputFiles` → jumps to `SizeBasedFileRewritePlanner` **line 188** (≥ 5 files)

### Keyboard Shortcuts

| Shortcut | Action |
|---|---|
| `Cmd+Click` | Go to Definition |
| `Cmd+F12` | Go to Implementation (for interfaces) |
| `Ctrl+-` | Go Back (navigate history) |
| `Cmd+G` | Go to Line Number |
| `Cmd+P` | Quick Open File |
