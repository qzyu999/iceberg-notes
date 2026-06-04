# Step-Through Guide: Tracing `rewrite_data_files` in the Java/Spark Codebase

This guide tells you exactly which files to open and which functions to look at in VSCode, in order, to trace the full call chain when a user runs:

```sql
spark.sql("CALL catalog.system.rewrite_data_files(table => 'db.table')")
```

> **Base path**: `/Users/jaredyu/Desktop/open_source/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg`

---

## Quick Setup in VSCode

1. Open the folder `/Users/jaredyu/Desktop/open_source/iceberg` as a workspace.
2. Install the **"Extension Pack for Java"** extension (if not already installed) for Go-to-Definition (`F12` / `Cmd+Click`) support.
3. You can use `Cmd+P` to quickly open any file by name.
4. Use `Cmd+Click` on any method/class name to jump to its definition.
5. Use `Ctrl+-` (Go Back) to return to the previous location.

---

## The Call Chain (10 Steps)

### Step 1: Procedure Registry — `SparkProcedures.java`

📁 `spark/procedures/SparkProcedures.java` — [Open file](file:///Users/jaredyu/Desktop/open_source/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/SparkProcedures.java)

When Spark parses `CALL catalog.system.rewrite_data_files(...)`, it looks up the procedure name in a registry. This is where the string `"rewrite_data_files"` maps to a Java class.

**Go to**: `initProcedureBuilders()` (line 45)

```java
mapBuilder.put("rewrite_data_files", RewriteDataFilesProcedure::builder);  // line 51
```

**What happens**: Spark resolves the procedure name and calls `RewriteDataFilesProcedure.builder()` to create the procedure instance.

**Next**: `Cmd+Click` on `RewriteDataFilesProcedure` →

---

### Step 2: Procedure Entry Point — `RewriteDataFilesProcedure.java`

📁 `spark/procedures/RewriteDataFilesProcedure.java` — [Open file](file:///Users/jaredyu/Desktop/open_source/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/RewriteDataFilesProcedure.java)

This is where the SQL arguments get parsed and the action gets built. **The main method is `call()`** (line 109).

**Go to**: `call(InternalRow args)` (line 109)

```java
public InternalRow[] call(InternalRow args) {
    // 1. Parse all SQL arguments
    Identifier tableIdent = input.ident(TABLE_PARAM);         // table => 'db.table'
    String strategy = input.asString(STRATEGY_PARAM, null);    // strategy => 'sort' | 'binpack'
    String sortOrderString = input.asString(SORT_ORDER_PARAM, null);
    Map<String, String> options = input.asStringMap(OPTIONS_PARAM, ImmutableMap.of());
    String where = input.asString(WHERE_PARAM, null);
    String branch = input.asString(BRANCH_PARAM, null);

    // 2. Load the Iceberg table and run the action
    return modifyIcebergTable(tableIdent, table -> {
        // 3. Create the action
        RewriteDataFilesSparkAction action =
            actions().rewriteDataFiles(table).options(options).toBranch(branch);

        // 4. Apply strategy (binpack/sort/zorder)
        if (strategy != null || sortOrderString != null) {
            action = checkAndApplyStrategy(action, strategy, sortOrderString, table.schema());
        }

        // 5. Apply filter (WHERE clause)
        action = checkAndApplyFilter(action, where, tableIdent);

        // 6. EXECUTE — this is where the real work begins
        RewriteDataFiles.Result result = action.execute();

        return toOutputRows(result);
    });
}
```

**Key things to trace**:
- `Cmd+Click` `actions()` → goes to `BaseProcedure.actions()` (returns `SparkActions` instance)
- `Cmd+Click` `rewriteDataFiles(table)` → goes to `SparkActions.rewriteDataFiles()`
- `Cmd+Click` `action.execute()` → this is the main entry to the rewrite logic

**Next**: `Cmd+Click` on `actions()` →

---

### Step 3: Actions Factory — `BaseProcedure.java` → `SparkActions.java`

📁 `spark/procedures/BaseProcedure.java` — [Open file](file:///Users/jaredyu/Desktop/open_source/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/BaseProcedure.java)

```java
// BaseProcedure.java, line 84
protected SparkActions actions() {
    if (actions == null) {
        this.actions = SparkActions.get(spark);
    }
    return actions;
}
```

**Next**: `Cmd+Click` on `SparkActions.get(spark)` →

📁 `spark/actions/SparkActions.java` — [Open file](file:///Users/jaredyu/Desktop/open_source/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkActions.java)

```java
// SparkActions.java, line 74
public RewriteDataFilesSparkAction rewriteDataFiles(Table table) {
    return new RewriteDataFilesSparkAction(spark, table);  // Creates the action
}
```

**Next**: `Cmd+Click` on `RewriteDataFilesSparkAction` →

---

### Step 4: The Main Action — `RewriteDataFilesSparkAction.java`

📁 `spark/actions/RewriteDataFilesSparkAction.java` — [Open file](file:///Users/jaredyu/Desktop/open_source/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java)

**This is the most important file.** It orchestrates everything.

#### 4a. Constructor (line 104)
```java
RewriteDataFilesSparkAction(SparkSession spark, Table table) {
    super(spark.cloneSession());
    spark().conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), false);  // disable AQE
    this.table = table;
}
```

#### 4b. Strategy selection (lines 121–147)
Back in `RewriteDataFilesProcedure.call()`, the procedure calls `.binPack()`, `.sort()`, or `.zOrder()` on the action. Each one sets the `runner` field:

```java
public RewriteDataFilesSparkAction binPack() {
    this.runner = new SparkBinPackFileRewriteRunner(spark(), table);   // line 124
    return this;
}
public RewriteDataFilesSparkAction sort(SortOrder sortOrder) {
    this.runner = new SparkSortFileRewriteRunner(spark(), table, sortOrder); // line 131
    return this;
}
public RewriteDataFilesSparkAction zOrder(String... columnNames) {
    this.runner = new SparkZOrderFileRewriteRunner(spark(), table, ...);     // line 145
    return this;
}
```

#### 4c. `execute()` — The main entry point (line 168)

**Go to**: `execute()` (line 168). This is the method called from `RewriteDataFilesProcedure.call()`.

```java
public RewriteDataFiles.Result execute() {
    // Early exit if table is empty
    if (table.currentSnapshot() == null) return EMPTY_RESULT;

    long startingSnapshotId = table.snapshot(branch).snapshotId();
    
    init(startingSnapshotId);                          // → Step 5

    FileRewritePlan plan = planner.plan();              // → Step 6

    if (plan.totalGroupCount() == 0) return EMPTY_RESULT;

    // Either all-at-once or partial progress
    Builder resultBuilder =
        partialProgressEnabled
            ? doExecuteWithPartialProgress(plan, commitManager(startingSnapshotId))
            : doExecute(plan, commitManager(startingSnapshotId));  // → Step 7
    ...
}
```

**Next**: `Cmd+Click` on `init(startingSnapshotId)` →

---

### Step 5: Initialization — `init()` in `RewriteDataFilesSparkAction.java`

Still in the same file, line 207:

```java
private void init(long startingSnapshotId) {
    // Choose planner based on runner type
    this.planner =
        runner instanceof SparkShufflingFileRewriteRunner
            ? new SparkShufflingDataRewritePlanner(table, filter, startingSnapshotId, caseSensitive)
            : new BinPackRewriteFilePlanner(table, filter, startingSnapshotId, caseSensitive);

    // Default to BinPack if no strategy was selected by the user
    if (this.runner == null) {
        this.runner = new SparkBinPackFileRewriteRunner(spark(), table);
    }

    validateAndInitOptions();  // parses all options, calls planner.init() and runner.init()
}
```

**Key takeaway**: The planner and runner are finalized here. For bin-pack, both are set to their BinPack variants.

**Next**: Go back to `execute()`, then `Cmd+Click` on `planner.plan()` →

---

### Step 6: Planning — `BinPackRewriteFilePlanner.java`

📁 `core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java` — [Open file](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java)

> **Note**: This file is **outside** the `spark/v3.5` directory — it's in `core/`. This is the engine-agnostic planning logic.

**Go to**: `plan()` (line 216)

```java
public FileRewritePlan plan() {
    StructLikeMap<List<List<FileScanTask>>> plan = planFileGroups();  // → 6a
    // Wraps groups into RewriteFileGroup objects with metadata
    // Sorts by rewrite job order
    return new FileRewritePlan<>(groups, totalGroupCount, groupsInPartition);
}
```

#### 6a. `planFileGroups()` (line 286)
```java
private StructLikeMap<List<List<FileScanTask>>> planFileGroups() {
    // 1. SCAN: Read all data files matching the filter
    TableScan scan = table().newScan().filter(filter).caseSensitive(caseSensitive).ignoreResiduals();
    CloseableIterable<FileScanTask> fileScanTasks = scan.planFiles();

    // 2. GROUP BY PARTITION
    StructLikeMap<List<FileScanTask>> filesByPartition = groupByPartition(table(), ..., fileScanTasks);

    // 3. For each partition: FILTER FILES → BIN-PACK → FILTER GROUPS
    return filesByPartition.transformValues(tasks -> ImmutableList.copyOf(planFileGroups(tasks)));
}
```

#### 6b. Parent class `SizeBasedFileRewritePlanner.planFileGroups(tasks)` (line 180)

📁 `core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java` — [Open file](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java)

```java
protected Iterable<List<T>> planFileGroups(Iterable<T> tasks) {
    Iterable<T> filteredTasks = rewriteAll ? tasks : filterFiles(tasks);  // select files
    BinPacking.ListPacker<T> packer = new BinPacking.ListPacker<>(maxGroupSize, 1, false);
    List<List<T>> groups = packer.pack(filteredTasks, ContentScanTask::length);  // bin-pack
    return rewriteAll ? groups : filterFileGroups(groups);  // filter groups
}
```

**`filterFiles()`** is overridden in `BinPackRewriteFilePlanner` (line 188):
```java
protected Iterable<FileScanTask> filterFiles(Iterable<FileScanTask> tasks) {
    return Iterables.filter(tasks, task ->
        outsideDesiredFileSizeRange(task) || tooManyDeletes(task) || tooHighDeleteRatio(task));
}
```

**Next**: Go back to `execute()` → `Cmd+Click` on `doExecute(plan, commitManager(...))` →

---

### Step 7: Execution — `doExecute()` in `RewriteDataFilesSparkAction.java`

Back in `RewriteDataFilesSparkAction.java`, line 249:

```java
private Builder doExecute(FileRewritePlan plan, RewriteDataFilesCommitManager commitManager) {
    ExecutorService rewriteService = rewriteService();  // thread pool (default 5 threads)

    ConcurrentLinkedQueue<RewriteFileGroup> rewrittenGroups = new ConcurrentLinkedQueue<>();

    // Rewrite each group in parallel
    Tasks.foreach(plan.groups())
        .executeWith(rewriteService)
        .stopOnFailure()
        .noRetry()
        .run(fileGroup -> {
            rewrittenGroups.add(rewriteFiles(plan, fileGroup));  // → Step 8
        });

    // Commit all rewritten groups atomically
    commitManager.commitOrClean(Sets.newHashSet(rewrittenGroups));  // → Step 10

    return ImmutableRewriteDataFiles.Result.builder().rewriteResults(rewriteResults);
}
```

**Next**: `Cmd+Click` on `rewriteFiles(plan, fileGroup)` →

---

### Step 8: File Rewrite Dispatch — `rewriteFiles()` in `RewriteDataFilesSparkAction.java`

Still in `RewriteDataFilesSparkAction.java`, line 221:

```java
RewriteFileGroup rewriteFiles(FileRewritePlan plan, RewriteFileGroup fileGroup) {
    String desc = jobDesc(fileGroup, plan);
    Set<DataFile> addedFiles =
        withJobGroupInfo(
            newJobGroupInfo("REWRITE-DATA-FILES", desc),
            () -> runner.rewrite(fileGroup));           // → Step 9 (delegates to runner)

    fileGroup.setOutputFiles(addedFiles);
    return fileGroup;
}
```

**Next**: `Cmd+Click` on `runner.rewrite(fileGroup)` →

---

### Step 9: Spark Rewrite Runner — `SparkDataFileRewriteRunner.java` → `SparkBinPackFileRewriteRunner.java`

📁 `spark/actions/SparkDataFileRewriteRunner.java` — [Open file](file:///Users/jaredyu/Desktop/open_source/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkDataFileRewriteRunner.java)

```java
// SparkDataFileRewriteRunner.java, line 46
public Set<DataFile> rewrite(RewriteFileGroup group) {
    String groupId = UUID.randomUUID().toString();
    try {
        tableCache.add(groupId, table());                     // cache the table ref
        taskSetManager.stageTasks(table(), groupId, group.fileScanTasks()); // stage files

        doRewrite(groupId, group);                             // → the actual Spark job

        return coordinator.fetchNewFiles(table(), groupId);    // collect output files
    } finally {
        tableCache.remove(groupId);
        taskSetManager.removeTasks(table(), groupId);
        coordinator.clearRewrite(table(), groupId);
    }
}
```

**Next**: `Cmd+Click` on `doRewrite(groupId, group)` →

📁 `spark/actions/SparkBinPackFileRewriteRunner.java` — [Open file](file:///Users/jaredyu/Desktop/open_source/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackFileRewriteRunner.java)

```java
// SparkBinPackFileRewriteRunner.java, line 42
protected void doRewrite(String groupId, RewriteFileGroup group) {
    // READ: Load the staged files, splitting them by the computed split size
    Dataset<Row> scanDF = spark().read()
        .format("iceberg")
        .option(SparkReadOptions.SCAN_TASK_SET_ID, groupId)
        .option(SparkReadOptions.SPLIT_SIZE, group.inputSplitSize())
        .option(SparkReadOptions.FILE_OPEN_COST, "0")
        .load(groupId);

    // WRITE: Write the data back as new, properly-sized Parquet files
    scanDF.write()
        .format("iceberg")
        .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupId)
        .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, group.maxOutputFileSize())
        .option(SparkWriteOptions.DISTRIBUTION_MODE, distributionMode(group).modeName())
        .option(SparkWriteOptions.OUTPUT_SPEC_ID, group.outputSpecId())
        .mode("append")
        .save(groupId);
}
```

**This is the actual Spark job.** It reads the old files and writes new ones. The `FileRewriteCoordinator` (in `rewrite()` above) picks up the newly written files.

**Next**: Go back to `doExecute()` → `Cmd+Click` on `commitManager.commitOrClean(...)` →

---

### Step 10: Commit — `RewriteDataFilesCommitManager.java`

📁 `core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java` — [Open file](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java)

> **Note**: Also in `core/`, not `spark/`.

```java
// line 89
public void commitFileGroups(Set<RewriteFileGroup> fileGroups) {
    DataFileSet rewrittenDataFiles = DataFileSet.create();
    DataFileSet addedDataFiles = DataFileSet.create();

    for (RewriteFileGroup group : fileGroups) {
        rewrittenDataFiles.addAll(group.rewrittenFiles());  // old files to remove
        addedDataFiles.addAll(group.addedFiles());          // new files to add
    }

    // Use the Iceberg RewriteFiles API for atomic file swap
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshotId);

    if (useStartingSequenceNumber) {
        long seqNum = table.snapshot(startingSnapshotId).sequenceNumber();
        rewrite.dataSequenceNumber(seqNum);
    }

    rewrittenDataFiles.forEach(rewrite::deleteFile);   // mark old files for removal
    addedDataFiles.forEach(rewrite::addFile);           // mark new files for addition

    rewrite.commit();   // ← ATOMIC COMMIT: creates new snapshot
}
```

**This is the end of the chain.** After `commit()`, the table has a new snapshot where old data files are replaced by new, optimized ones.

---

## Summary: Complete File Sequence

| Step | File | Function | Location |
|------|------|----------|----------|
| 1 | [SparkProcedures.java](file:///Users/jaredyu/Desktop/open_source/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/SparkProcedures.java) | `initProcedureBuilders()` | `spark/procedures/` |
| 2 | [RewriteDataFilesProcedure.java](file:///Users/jaredyu/Desktop/open_source/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/RewriteDataFilesProcedure.java) | `call(args)` | `spark/procedures/` |
| 3a | [BaseProcedure.java](file:///Users/jaredyu/Desktop/open_source/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/BaseProcedure.java) | `actions()`, `modifyIcebergTable()` | `spark/procedures/` |
| 3b | [SparkActions.java](file:///Users/jaredyu/Desktop/open_source/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkActions.java) | `rewriteDataFiles(table)` | `spark/actions/` |
| 4–5 | [RewriteDataFilesSparkAction.java](file:///Users/jaredyu/Desktop/open_source/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java) | `execute()`, `init()`, `doExecute()`, `rewriteFiles()` | `spark/actions/` |
| 6 | [BinPackRewriteFilePlanner.java](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java) | `plan()`, `planFileGroups()`, `filterFiles()` | `core/actions/` |
| 6b | [SizeBasedFileRewritePlanner.java](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java) | `planFileGroups(tasks)`, `outsideDesiredFileSizeRange()` | `core/actions/` |
| 9a | [SparkDataFileRewriteRunner.java](file:///Users/jaredyu/Desktop/open_source/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkDataFileRewriteRunner.java) | `rewrite(group)` | `spark/actions/` |
| 9b | [SparkBinPackFileRewriteRunner.java](file:///Users/jaredyu/Desktop/open_source/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackFileRewriteRunner.java) | `doRewrite(groupId, group)` | `spark/actions/` |
| 10 | [RewriteDataFilesCommitManager.java](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java) | `commitFileGroups()`, `commitOrClean()` | `core/actions/` |

---

## VSCode Navigation Cheat Sheet

| Action | Shortcut |
|--------|----------|
| Go to Definition | `F12` or `Cmd+Click` |
| Go Back | `Ctrl+-` |
| Go Forward | `Ctrl+Shift+-` |
| Find All References | `Shift+F12` |
| Open File by Name | `Cmd+P` |
| Go to Symbol in File | `Cmd+Shift+O` |
| Find in Files | `Cmd+Shift+F` |
| Peek Definition | `Alt+F12` |
