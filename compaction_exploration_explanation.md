# Compaction Exploration Notebook — Cell-by-Cell Explanation

> This document explains every cell in `compaction_exploration.ipynb` from first principles.
> It assumes you know Python but not Iceberg internals.

---

## Table of Contents

- [Background: Why This Notebook Exists](#background-why-this-notebook-exists)
- [Cell 1: Catalog and Warehouse Setup](#cell-1-catalog-and-warehouse-setup)
- [Cell 2: Create a Partitioned Table](#cell-2-create-a-partitioned-table)
- [Cell 3: Append Many Small Files](#cell-3-append-many-small-files)
- [Cell 4: Inspect All Data Files](#cell-4-inspect-all-data-files)
- [Cell 5: Filtered File Scan](#cell-5-filtered-file-scan)
- [Cell 6: Group by Partition](#cell-6-group-by-partition)
- [Cell 7: ListPacker Bin-Packing](#cell-7-listpacker-bin-packing)
- [Cell 8: Writer's Size Thresholds](#cell-8-writers-size-thresholds)
- [Cell 9: Reading Files with ArrowScan](#cell-9-reading-files-with-arrowscan)
- [Cell 10: MaintenanceTable](#cell-10-maintenancetable)
- [Cell 11: _OverwriteFiles](#cell-11-overwritefiles)
- [How These Cells Map to the Compaction Implementation](#how-these-cells-map-to-the-compaction-implementation)

---

## Background: Why This Notebook Exists

Apache Iceberg is a **table format** — it defines how data files and metadata are organized on disk (or object storage). Think of it like this:

```
Traditional database:  data lives inside the database engine
Iceberg:               data lives as Parquet files on disk
                       metadata lives as JSON + Avro files alongside them
                       any engine (Spark, PyArrow, DuckDB) can read/write
```

The problem we're solving is called the **small files problem**. When you append data to an Iceberg table many times (e.g., writing 50 rows every minute), each append creates a separate Parquet file. After a while, you have thousands of tiny files. This is bad because:

1. **Metadata overhead**: Each file needs an entry in a manifest (metadata file). More files = more metadata to read before any query.
2. **I/O overhead**: Opening and closing thousands of files is slower than reading a few large files.
3. **Query planning**: The query engine must evaluate statistics for each file to decide which ones contain relevant data.

**Compaction** (aka `rewrite_data_files`) solves this by reading many small files and writing them back as fewer, properly-sized files.

---

## Cell 1: Catalog and Warehouse Setup

```python
catalog = SqlCatalog("test_catalog", uri="sqlite:///...", warehouse="file:///tmp/...")
```

### What's happening

To use an Iceberg table, you need two things:

1. **A catalog** — a registry that maps table names (like `test_db.events`) to their metadata locations. It answers the question: "Where is the metadata file for this table?" The catalog itself can live in various backends:
   - SQLite (what we use here — a local `.db` file)
   - PostgreSQL
   - AWS Glue
   - A REST API
   - Hive Metastore

2. **A warehouse** — a directory (or S3 bucket, etc.) where the actual data files and metadata files are stored. `file:///tmp/iceberg_compaction_exploration` means: "use a folder on the local filesystem at `/tmp/`."

### What's created on disk

After this cell runs:
```
/tmp/iceberg_compaction_exploration/       ← warehouse root (empty for now)
/tmp/iceberg_compaction_test.db            ← SQLite file storing the catalog
```

### Underlying code path

```
SqlCatalog.__init__()
  → pyiceberg/catalog/sql.py
  → Creates SQLAlchemy engine pointing to the SQLite file
  → Creates the `iceberg_tables` and `iceberg_namespace_properties` tables
```

### Why it matters for compaction

The catalog is how we'll load the table later. The warehouse is where data files will be written, and where compacted files will also be written.

---

## Cell 2: Create a Partitioned Table

```python
schema = Schema(
    NestedField(1, "id", LongType()),
    NestedField(2, "name", StringType()),
    NestedField(3, "category", StringType()),
    NestedField(4, "value", LongType()),
)
partition_spec = PartitionSpec(
    PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="category")
)
table = catalog.create_table("test_db.events", schema=schema, partition_spec=partition_spec)
```

### What's happening

We create a table with 4 columns and **partition it by `category`**. Partitioning means:

- Data is physically split into subdirectories based on the value of `category`
- A query asking for `category = 'electronics'` can skip all files in other partitions without reading them
- Each partition's files are independent — you can compact one partition without touching others

The **identity transform** means the partition value is the raw column value (as opposed to, say, bucketing or date truncation).

### What's created on disk

```
/tmp/iceberg_compaction_exploration/
  test_db.db/
    events/
      metadata/
        00000-<uuid>.metadata.json    ← table metadata (schema, spec, snapshots=[])
```

The metadata JSON file contains:
- The schema definition
- The partition spec
- An empty snapshot list (no data yet)
- Table properties (empty by default, but `write.target-file-size-bytes` defaults to 512MB)

### What `NestedField(1, "id", LongType())` means

Each field has:
- A **field ID** (1, 2, 3, 4) — unique identifier that survives column renames
- A **name** ("id") — human-readable
- A **type** (LongType = 64-bit integer)
- **required** (default False) — whether nulls are allowed

Field IDs are how Iceberg tracks schema evolution. If you rename `id` to `event_id`, the field ID stays `1`, so old data files still map correctly.

### Why it matters for compaction

Compaction must:
- Respect partition boundaries (never merge files from different partitions)
- Preserve the schema and field IDs in output files
- Write output files with the same partition spec

---

## Cell 3: Append Many Small Files

```python
for i in range(15):
    category = categories[i % len(categories)]  # rotates: electronics, clothing, food
    df = pa.table({...})  # 50 rows
    table.append(df)
```

### What's happening

We call `table.append()` **15 times**, each with a tiny 50-row Arrow table. Each call creates:
1. A new Parquet data file in the warehouse
2. A new manifest file listing that data file
3. A new snapshot (a pointer to the current state of the table)

After 15 appends, we have:
- **15 data files** (5 per partition: electronics, clothing, food)
- **15 manifest files** (one per append)
- **15 snapshots** (one per append)

### What `table.append()` does under the hood

```
table.append(df)
  → pyiceberg/table/__init__.py → Table.append()
    → Creates a Transaction
    → Calls _dataframe_to_data_files(table.metadata, df, table.io)
      → pyiceberg/io/pyarrow.py
      → Determines the partition value from the data (e.g., category="electronics")
      → Calls bin_pack_arrow_table(df, target_file_size=512MB)
        → Since df is tiny (50 rows ≈ 2KB), it fits in one batch
      → Calls write_file() to produce one Parquet file
      → Returns a DataFile object with:
          .file_path = "s3://or/local/path/to/file.parquet"
          .file_size_in_bytes = 2190
          .record_count = 50
          .partition = Record[electronics]
          .column_sizes, .value_counts, .null_value_counts, .lower_bounds, .upper_bounds
    → Creates a new _FastAppendFiles snapshot producer
    → Writes a new manifest file containing the DataFile entry
    → Writes new metadata JSON pointing to the new snapshot
    → Commits (updates catalog to point to new metadata)
```

### What's created on disk (after all 15 appends)

```
/tmp/iceberg_compaction_exploration/test_db.db/events/
  metadata/
    00000-<uuid>.metadata.json       ← initial metadata (no data)
    00001-<uuid>.metadata.json       ← after 1st append
    00002-<uuid>.metadata.json       ← after 2nd append
    ...
    00015-<uuid>.metadata.json       ← after 15th append (CURRENT)
    <uuid>-m0.avro                   ← manifest file for 1st append
    <uuid>-m0.avro                   ← manifest file for 2nd append
    ...                              ← 15 manifest files total
    snap-<id>-1-<uuid>.avro          ← manifest list for snapshot 1
    snap-<id>-1-<uuid>.avro          ← manifest list for snapshot 2
    ...                              ← 15 manifest lists total
  data/
    category=electronics/
      00000-0-<uuid>.parquet         ← 5 small files, ~2.2KB each
      00000-0-<uuid>.parquet
      ...
    category=clothing/
      00000-0-<uuid>.parquet         ← 5 small files
      ...
    category=food/
      00000-0-<uuid>.parquet         ← 5 small files
      ...
```

### Why this is a problem

Each of those 15 data files is only ~2KB. The **optimal** file size is 512MB (the writer's default target). These files are **250,000x smaller** than they should be. Any query must now:
1. Read 15 manifest files to find all 15 data files
2. Open and close 15 separate Parquet files
3. Process 15 sets of column statistics

After compaction, this should be 3 files (one per partition), or even 1 file if the table were unpartitioned.

---

## Cell 4: Inspect All Data Files

```python
all_tasks = list(table.scan(row_filter=AlwaysTrue()).plan_files())
```

### What's happening

This is **Requirement 1** of the issue in action. We're asking: "Give me all data files in the table."

`AlwaysTrue()` is a predicate that matches everything (equivalent to `WHERE true` in SQL). If we used `EqualTo("category", "electronics")` instead, we'd only get the 5 electronics files.

### What `plan_files()` does under the hood

This is the most important function to understand for compaction. Here's the full chain:

```
table.scan(row_filter=AlwaysTrue())
  → Creates a DataScan object
  → Stores the filter expression

.plan_files()
  → pyiceberg/table/__init__.py → DataScan.plan_files()
    → Calls _plan_files_local() → scan_plan_helper()

    scan_plan_helper() does:
      1. Get the current snapshot (the most recent one, snapshot #15)
      2. Get its manifest list (an Avro file listing all active manifests)
      3. Read each manifest file

      For each manifest file:
        LEVEL 1 — MANIFEST PRUNING
        → Read the manifest's partition summary (stored in the manifest list)
        → The summary says: "this manifest contains files with category in ['electronics']"
        → If the filter can't match any value in the summary → skip entire manifest
        → For AlwaysTrue, nothing is skipped

        LEVEL 2 — PARTITION PRUNING (per data file entry)
        → Each entry in the manifest has: file_path, partition_value, file_stats
        → Check: does this file's partition value match the filter?
        → For AlwaysTrue, all files pass

        LEVEL 3 — METRICS PRUNING (per data file entry)
        → Check file-level column statistics (min/max bounds)
        → Example: if filter is "id > 1000" and file's max(id) = 500 → skip
        → For AlwaysTrue, all files pass

      4. Collect surviving entries as FileScanTask objects
```

### What a FileScanTask contains

```python
task = all_tasks[0]
task.file                    # DataFile object
task.file.file_path          # "/tmp/.../00000-0-<uuid>.parquet"
task.file.file_size_in_bytes # 2190
task.file.record_count       # 50
task.file.partition          # Record[food]    ← the partition value
task.file.spec_id            # 0               ← which partition spec was used
task.delete_files            # set()           ← position delete files (empty here)
task.residual                # AlwaysTrue()    ← remaining filter after partition pruning
```

### Output from the notebook

```
Total data files: 15
Total rows: 750

  00000-0-<uuid>.parquet  partition=Record[food]         size=   2,190 bytes  rows=50
  00000-0-<uuid>.parquet  partition=Record[clothing]     size=   2,204 bytes  rows=50
  00000-0-<uuid>.parquet  partition=Record[electronics]  size=   2,221 bytes  rows=50
  ...  (15 files total)
```

### Why it matters for compaction

This is Step 1 of compaction: "find data files matching the filter." The `plan_files()` result gives us everything we need: file paths, sizes (for decided if they need rewriting), partition values (for grouping), and record counts.

---

## Cell 5: Filtered File Scan

```python
electronics_tasks = list(
    table.scan(row_filter=EqualTo("category", "electronics")).plan_files()
)
```

### What's happening

Same as Cell 4, but with a filter: only return files for `category = 'electronics'`. This triggers actual partition pruning:

```
Manifest summary says: "this manifest has category='food'"
Filter says: "category = 'electronics'"
→ 'food' ≠ 'electronics' → SKIP entire manifest (never read its entries)
```

### Output

```
Files matching category='electronics': 5
```

Out of 15 files, only 5 are returned. The other 10 (clothing + food) were pruned at the manifest or partition level — their Parquet data was never touched.

### Why it matters for compaction

If a user calls `rewrite_data_files(filter=EqualTo("category", "electronics"))`, only these 5 files would be considered for compaction. The clothing and food partitions would remain untouched.

---

## Cell 6: Group by Partition

```python
files_by_partition = defaultdict(list)
for task in all_tasks:
    partition_key = str(task.file.partition)
    files_by_partition[partition_key].append(task)
```

### What's happening

This implements **the first sub-step of Requirement 2**: "group data files by partitions." We take the flat list of 15 `FileScanTask` objects and organize them into a dictionary where keys are partition values and values are lists of files.

### Why group by partition

You **cannot** merge files from different partitions. If `file_A` has `category='electronics'` and `file_B` has `category='food'`, you can't combine them because the output file must belong to exactly one partition. Iceberg's partition spec requires that every data file contains rows for only one partition value.

### Java equivalent

In Java, this is `BinPackRewriteFilePlanner.groupByPartition()` at [line 310](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java):

```java
StructLikeMap<List<FileScanTask>> filesByPartition = StructLikeMap.create(partitionType);
for (FileScanTask task : tasks) {
    StructLike taskPartition = task.file().partition();
    filesByPartition.computeIfAbsent(taskPartition, unused -> Lists.newArrayList()).add(task);
}
```

The logic is identical — just different syntax.

### Output

```
Files grouped by partition:
  Record[food]: 5 files, 10,868 bytes total
  Record[clothing]: 5 files, 10,964 bytes total
  Record[electronics]: 5 files, 11,015 bytes total
```

---

## Cell 7: ListPacker Bin-Packing

```python
from pyiceberg.utils.bin_packing import ListPacker

packer = ListPacker(target_weight=500, lookback=1, largest_bin_first=False)
bins = packer.pack([100, 200, 300, 150, 250, 50], weight_func=lambda x: x)
```

### What's happening

This cell demonstrates the **existing** `ListPacker` class — the same bin-packing algorithm used by Java's `BinPacking.ListPacker`. It's not new code; it's already been in pyiceberg for other features (the writer uses it to split data into target-sized output files).

### How bin-packing works

**The problem**: Given a list of items with weights, pack them into bins such that each bin's total weight ≤ `target_weight`.

**The algorithm** (first-fit):
1. Create an empty bin
2. For each item:
   - If the item fits in the current bin (total + item ≤ target) → add it
   - Otherwise → close the current bin, start a new bin with this item
3. Return all bins

**The parameters**:
- `target_weight=500` — maximum total weight per bin
- `lookback=1` — how many previous bins to check if the item fits (1 = only check the most recent bin)
- `largest_bin_first=False` — whether to sort bins by size before returning

### Demo output

```
Packing [100, 200, 300, 150, 250, 50] into bins of max 500:
  Bin 0: [100, 200] (total=300)     ← 100+200=300 ≤ 500 ✓, next item 300 would make 600 > 500 ✗
  Bin 1: [300, 150] (total=450)     ← new bin, 300+150=450 ≤ 500 ✓
  Bin 2: [250, 50]  (total=300)     ← new bin, 250+50=300 ≤ 500 ✓
```

### Applied to our actual files

```
Partition Record[food]: 5 files → 1 groups
  Group 0: 5 files, 10,868 bytes
```

All 5 files fit in one group because their total (10,868 bytes) is far below the 100GB `MAX_FILE_GROUP_SIZE`. In production, with larger files, you'd see multiple groups.

### Java equivalent

`SizeBasedFileRewritePlanner.planFileGroups()` at [line 182](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java):

```java
BinPacking.ListPacker<T> packer = new BinPacking.ListPacker<>(maxGroupSize, 1, false);
List<List<T>> groups = packer.pack(filteredTasks, ContentScanTask::length);
```

### Source code location

[pyiceberg/utils/bin_packing.py](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/utils/bin_packing.py) — `ListPacker` calls `PackingIterator` internally.

### Why it matters for compaction

Bin-packing serves **two** purposes in compaction:

1. **Planning**: Group candidate files into manageable "rewrite groups" (what this cell demonstrates). Each group will be processed as one unit — all files in the group are read together and written back together.

2. **Writing**: When the rewriter writes data back, `bin_pack_arrow_table()` splits the Arrow table into record batch groups that each produce one output file ≈ `target_file_size`. This happens automatically inside `_dataframe_to_data_files()`.

---

## Cell 8: Writer's Size Thresholds

```python
target_file_size = int(table.properties.get(
    TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
    TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT,
))
min_file_size = int(target_file_size * 0.75)
max_file_size = int(target_file_size * 1.80)
```

### What's happening

This reads the **writer's configuration** — the same configuration that determines output file sizes during `table.append()`. The issue says "using the same bin-packing constraints of the writer," which means these exact values.

### The three thresholds

| Threshold | Formula | Value | Purpose |
|---|---|---|---|
| Target | `write.target-file-size-bytes` | 512 MB (536,870,912 bytes) | The ideal output file size |
| Min | 75% of target | 384 MB (402,653,184 bytes) | Files below this are "too small" |
| Max | 180% of target | 922 MB (966,367,641 bytes) | Files above this are "too large" |

### How these thresholds are used

A file is a **candidate for rewriting** if:
```
file_size < min_file_size    → file is too small, should be combined with others
file_size > max_file_size    → file is too large, should be split
```

A file in the range `[min_file_size, max_file_size]` is considered **optimal** — leave it alone.

### Where these ratios come from

[SizeBasedFileRewritePlanner.java, lines 72 and 82](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java):

```java
public static final double MIN_FILE_SIZE_DEFAULT_RATIO = 0.75;
public static final double MAX_FILE_SIZE_DEFAULT_RATIO = 1.80;
```

### Output

```
Writer's target file size:     536,870,912 bytes (512 MB)
Min file size (75%):           402,653,184 bytes (384 MB)
Max file size (180%):          966,367,641 bytes (922 MB)

File sizes vs thresholds:
  00000-0-<uuid>.parquet     2,190 bytes  🔴 needs rewrite
  00000-0-<uuid>.parquet     2,204 bytes  🔴 needs rewrite
  ...all 15 files are 🔴 (they're ~2KB, far below the 384MB minimum)
```

Every file is flagged as needing rewrite because 2KB << 384MB. In a production scenario, you'd see a mix of ✅ and 🔴.

### Where the property lives in pyiceberg

Table properties are stored in the metadata JSON file and accessible via `table.properties` (a dict). The constant `TableProperties.WRITE_TARGET_FILE_SIZE_BYTES` is defined as the string `"write.target-file-size-bytes"` and its default `TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT` is `536870912` (512MB).

Source: [pyiceberg/table/__init__.py](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/table/__init__.py) — the `TableProperties` class near the top of the file.

---

## Cell 9: Reading Files with ArrowScan

```python
arrow_table = table.scan(
    row_filter=EqualTo("category", "electronics")
).to_arrow()
```

### What's happening

This reads the **actual data** from the 5 electronics Parquet files and merges them into a single in-memory Arrow table. This is different from `plan_files()` (Cell 4) which only read metadata.

### The data flow

```
table.scan(filter).to_arrow()
  → plan_files()                  ← step 1: find matching files (metadata only)
  → ArrowScan.to_table(tasks)    ← step 2: read the actual Parquet data
    → for each FileScanTask:
        → Open the Parquet file using PyArrow
        → Read columns according to projected schema
        → If there are position delete files:
            → Read the delete file
            → Remove deleted row positions from the data
        → Append to result
    → Return concatenated pa.Table
```

### What ArrowScan handles for us

`ArrowScan` (in `pyiceberg/io/pyarrow.py`) manages several complexities:
- **Schema projection**: Only reads the columns you need
- **Type conversion**: Maps Iceberg types to Arrow types
- **Position deletes**: Merges delete files with data files (removing rows marked as deleted)
- **Multiple files**: Reads all files in the task list and concatenates them

### Output

```
Read 5 files into single Arrow table:
  Rows: 250        ← 5 files × 50 rows each
  Columns: ['id', 'name', 'category', 'value']
  Memory: 11,070 bytes
```

### Why it matters for compaction

This is what the compaction **rewriter** will do: read all files in a group → get one Arrow table → write it back using `_dataframe_to_data_files()`. The read step is already fully implemented.

---

## Cell 10: MaintenanceTable

```python
maintenance = table.maintenance
print(type(maintenance))       # <class 'pyiceberg.table.maintenance.MaintenanceTable'>
print(dir(maintenance))        # ['expire_snapshots', 'tbl']
```

### What's happening

Every `Table` object has a `.maintenance` property that returns a `MaintenanceTable` instance. This is the **entry point for table maintenance operations**.

### The full class (as it exists today)

```python
# pyiceberg/table/maintenance.py — entire file is 46 lines

class MaintenanceTable:
    tbl: Table

    def __init__(self, tbl: Table) -> None:
        self.tbl = tbl

    def expire_snapshots(self) -> ExpireSnapshots:
        """Return an ExpireSnapshots builder for snapshot expiration operations."""
        from pyiceberg.table import Transaction
        from pyiceberg.table.update.snapshot import ExpireSnapshots
        return ExpireSnapshots(transaction=Transaction(self.tbl, autocommit=True))
```

Currently, there is **exactly one** maintenance operation: `expire_snapshots()`. Our job is to add `rewrite_data_files()` as the second.

### The pattern

`expire_snapshots()` shows the established pattern:
1. Create a `Transaction` with `autocommit=True` (commits when the builder's `.commit()` is called)
2. Return a builder object that the user configures
3. The builder's `.commit()` executes the operation

### Output

```
MaintenanceTable class: <class 'pyiceberg.table.maintenance.MaintenanceTable'>
Available methods: ['expire_snapshots', 'tbl']

Current snapshots:
  Snapshot 6457227091833542474: operation=Operation.APPEND
  Snapshot 7168899509377265900: operation=Operation.APPEND
  ... (15 snapshots total, one per append)
```

15 snapshots from our 15 appends. Each snapshot is an immutable record of the table's state at a point in time.

### Why it matters for compaction

This is where `rewrite_data_files()` will live. After implementation, the available methods will be `['expire_snapshots', 'rewrite_data_files', 'tbl']`.

---

## Cell 11: _OverwriteFiles

```python
from pyiceberg.table.update.snapshot import _OverwriteFiles
print(dir(_OverwriteFiles))
```

### What's happening

`_OverwriteFiles` is the **snapshot producer** that handles "replace these old files with these new files." It's the mechanism for the **commit phase** of compaction.

### How snapshots work in Iceberg

Every write operation creates a new **snapshot**. A snapshot is an immutable record that says:
- "Here is a manifest list (Avro file) that points to all currently active manifests"
- "Each manifest lists data files that are part of this table version"

When you compact, you create a new snapshot that:
- **Removes** the old data files (marks them as `DELETED` in new manifest entries)
- **Adds** the new compacted data files (marks them as `ADDED` in new manifest entries)

### The class hierarchy

```
_SnapshotProducer (abstract)        ← base class for all snapshot operations
  ├── _FastAppendFiles              ← used by table.append() — adds files without touching existing
  ├── _MergeAppendFiles             ← merges new files into existing manifests
  ├── _OverwriteFiles               ← deletes old files + adds new files (WHAT WE NEED)
  └── _DeleteFiles                  ← only deletes files
```

### Key methods on _OverwriteFiles

```python
snapshot = _OverwriteFiles(...)

# Mark files to be removed from the table
snapshot.delete_data_file(old_file)      # adds to _deleted_data_files set

# Mark files to be added to the table
snapshot.append_data_file(new_file)      # adds to _added_data_files list

# Execute the atomic swap
snapshot.commit()
# → Produces new manifest files that:
#   - Mark old files as DELETED
#   - Mark new files as ADDED
# → Produces a new manifest list
# → Produces a new snapshot
# → Updates the metadata JSON atomically
```

### Why "atomic" matters

If compaction crashes halfway through, we need safety:
- If crash happens **before commit**: No metadata changes. The old files are still referenced. The partially-written new files are orphans (can be cleaned up later). The table is consistent.
- If crash happens **during commit**: The catalog update is the atomic point. Either the old metadata or the new metadata is current, never a mix.

### Source code location

[pyiceberg/table/update/snapshot.py](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/table/update/snapshot.py) — `_OverwriteFiles` starts at line 580. The `_commit()` method at line 658 produces the new manifest files and snapshot.

---

## How These Cells Map to the Compaction Implementation

```
Cell 1-2: Setup                           → Not part of compaction itself
Cell 3:   Create the problem              → Small files from many appends

Cell 4:   table.scan().plan_files()       → STEP 1: Find files to compact
Cell 5:   Filtered scan                   → STEP 1 (with user-provided predicate)

Cell 6:   Group by partition              → STEP 2a: Group files by partition
Cell 7:   ListPacker bin-pack             → STEP 2b: Bin-pack within each partition
Cell 8:   Size thresholds                 → STEP 2c: Filter files outside [min, max] range

Cell 9:   ArrowScan read                  → STEP 3: Read file group into Arrow table
          (+ _dataframe_to_data_files)    → STEP 4: Write back with proper sizing (not shown)

Cell 10:  MaintenanceTable                → Entry point: where rewrite_data_files() goes
Cell 11:  _OverwriteFiles                 → STEP 5: Atomic commit (delete old + add new)
```

### What exists vs. what needs to be built

```
EXISTS:  Cells 4, 5, 7, 9, 10, 11       (scan, bin-pack, read, write, commit)
NEW:     Cells 6, 8                      (group by partition, size filtering)
NEW:     Orchestration tying it all together (~50 lines)
NEW:     rewrite_data_files() method on MaintenanceTable (~20 lines)
```
