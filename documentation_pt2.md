# Issue #1092: Implementation Details — Part 2

> This document is the companion to `documentation.md`. Part 1 covers the **what** (requirements mapped to code). This part covers the **how** (implementation details, edge cases, and the complete code flow).

---

## Table of Contents
- [Complete Implementation Flow](#complete-implementation-flow)
- [Detailed Code Walkthrough](#detailed-code-walkthrough)
    - [The Planner](#the-planner)
    - [The Rewriter](#the-rewriter)
    - [The Commit](#the-commit)
    - [The Entry Point](#the-entry-point)
- [Edge Cases to Handle](#edge-cases-to-handle)
- [How the Writer's Bin-Packing Actually Works](#how-the-writers-bin-packing-actually-works)
- [Test Strategy](#test-strategy)
- [Java ↔ Python Line-by-Line Mapping](#java--python-line-by-line-mapping)

---

## Complete Implementation Flow

Here's the end-to-end data flow with every function call annotated:

```
table.maintenance.rewrite_data_files(filter=EqualTo("date", "2024-01-01"))
│
├── 1. table.scan(row_filter=filter).plan_files()
│   │   Source: pyiceberg/table/__init__.py → DataScan.plan_files() (line 2090)
│   │   Internally calls: _plan_files_local() → scan_plan_helper()
│   │   Returns: List[FileScanTask]
│   │
│   │   Each FileScanTask contains:
│   │   ├── .file: DataFile
│   │   │     ├── .file_path: str          (e.g., "s3://bucket/data/part-00001.parquet")
│   │   │     ├── .file_size_in_bytes: int (e.g., 15_728_640 = 15MB)
│   │   │     ├── .partition: Record       (e.g., Record(date="2024-01-01"))
│   │   │     ├── .record_count: int       (e.g., 50_000)
│   │   │     └── .spec_id: int            (e.g., 0)
│   │   ├── .delete_files: set[DataFile]   (position delete files, if any)
│   │   └── .residual: BooleanExpression   (remaining filter after partition pruning)
│   │
│   └─→ tasks: List[FileScanTask]
│
├── 2. Group by partition
│   │   files_by_partition[task.file.partition].append(task)
│   └─→ Dict[Record, List[FileScanTask]]
│
├── 3. For each partition:
│   │
│   ├── 3a. Filter files outside size range
│   │   │   Keep files where: size < min_file_size OR size > max_file_size
│   │   └─→ filtered_tasks: List[FileScanTask]
│   │
│   ├── 3b. Bin-pack filtered files into groups
│   │   │   ListPacker(target_weight=100GB).pack(filtered_tasks, size_func)
│   │   │   Source: pyiceberg/utils/bin_packing.py → ListPacker.pack()
│   │   └─→ groups: List[List[FileScanTask]]
│   │
│   └── 3c. Filter groups needing ≥ 5 files (or enough total size)
│       └─→ valid_groups: List[List[FileScanTask]]
│
├── 4. For each valid group:
│   │
│   ├── 4a. READ: Convert files to Arrow table
│   │   │   ArrowScan(metadata, io, schema, ALWAYS_TRUE, True, None).to_table(group)
│   │   │   Source: pyiceberg/io/pyarrow.py → ArrowScan.to_table()
│   │   │   This handles position deletes (if any) and schema projection
│   │   └─→ arrow_table: pa.Table
│   │
│   ├── 4b. WRITE: Produce new data files using existing writer
│   │   │   _dataframe_to_data_files(table.metadata, arrow_table, table.io)
│   │   │   Source: pyiceberg/io/pyarrow.py, line 2873
│   │   │   Internally:
│   │   │     ├── Gets target_file_size from table properties (512MB default)
│   │   │     ├── If unpartitioned: bin_pack_arrow_table(df, target_size)
│   │   │     ├── If partitioned: _determine_partitions() then bin_pack per partition
│   │   │     └── write_file() → writes Parquet with proper stats
│   │   └─→ new_data_files: List[DataFile]
│   │
│   └── 4c. Track: old_files += group files, new_files += new_data_files
│
└── 5. COMMIT: Atomic snapshot update
    │   Transaction → UpdateSnapshot.overwrite()
    │   Delete old files, add new files
    │   Source: pyiceberg/table/update/snapshot.py → _OverwriteFiles
    └─→ New snapshot with compacted files
```

---

## Detailed Code Walkthrough

### The Planner

The planner decides **which files to rewrite and how to group them**. This is where most of the new logic lives.

```python
# Suggested: pyiceberg/table/compaction.py

from collections import defaultdict
from pyiceberg.table import FileScanTask, TableProperties
from pyiceberg.utils.bin_packing import ListPacker

@dataclass
class RewriteGroup:
    """A group of files to be rewritten together."""
    partition: Record
    tasks: list[FileScanTask]

    @property
    def total_size(self) -> int:
        return sum(t.file.file_size_in_bytes for t in self.tasks)

    @property
    def file_count(self) -> int:
        return len(self.tasks)


def plan_rewrite(
    tasks: list[FileScanTask],
    target_file_size: int,
    min_file_size: int | None = None,
    max_file_size: int | None = None,
    min_input_files: int = 5,
    max_file_group_size: int = 100 * 1024**3,  # 100GB
) -> list[RewriteGroup]:
    """
    Plan which files to rewrite and how to group them.

    This mirrors the logic in Java's BinPackRewriteFilePlanner.plan().
    """
    # Derive min/max from target if not provided (matches Java defaults)
    if min_file_size is None:
        min_file_size = int(target_file_size * 0.75)
    if max_file_size is None:
        max_file_size = int(target_file_size * 1.80)

    # Step 1: Group by partition
    files_by_partition: dict[Record, list[FileScanTask]] = defaultdict(list)
    for task in tasks:
        files_by_partition[task.file.partition].append(task)

    # Step 2: For each partition, filter → bin-pack → filter groups
    all_groups: list[RewriteGroup] = []
    packer = ListPacker(
        target_weight=max_file_group_size,
        lookback=1,
        largest_bin_first=False,
    )

    for partition, partition_tasks in files_by_partition.items():
        # Filter: keep only files outside the desired size range
        filtered = [
            t for t in partition_tasks
            if t.file.file_size_in_bytes < min_file_size
            or t.file.file_size_in_bytes > max_file_size
        ]

        if not filtered:
            continue

        # Bin-pack into groups
        packed_groups = packer.pack(
            items=filtered,
            weight_func=lambda t: t.file.file_size_in_bytes,
        )

        # Filter groups: need enough files or enough data
        for group in packed_groups:
            total_size = sum(t.file.file_size_in_bytes for t in group)
            if len(group) >= min_input_files or total_size > target_file_size:
                all_groups.append(RewriteGroup(partition=partition, tasks=group))

    return all_groups
```

### The Rewriter

The rewriter reads files from a group and writes them back. **This uses entirely existing code.**

```python
def rewrite_group(
    table: Table,
    group: RewriteGroup,
) -> tuple[list[DataFile], list[DataFile]]:
    """
    Read all files in a group and write them back as properly-sized files.

    Returns:
        (old_files, new_files) — what to delete and what to add
    """
    from pyiceberg.io.pyarrow import ArrowScan, _dataframe_to_data_files
    from pyiceberg.expressions import ALWAYS_TRUE

    # READ: Use existing ArrowScan to read the group's files
    # This handles position deletes, schema projection, etc.
    arrow_scan = ArrowScan(
        table_metadata=table.metadata,
        io=table.io,
        projected_schema=table.schema(),
        row_filter=ALWAYS_TRUE,  # We already filtered at the file level
        case_sensitive=True,
        limit=None,
    )
    arrow_table = arrow_scan.to_table(group.tasks)

    # WRITE: Use existing writer pipeline
    # _dataframe_to_data_files handles:
    #   - Partition detection (if partitioned)
    #   - bin_pack_arrow_table() to split into target-sized chunks
    #   - write_file() to produce Parquet with proper statistics
    new_files = list(_dataframe_to_data_files(
        table_metadata=table.metadata,
        df=arrow_table,
        io=table.io,
    ))

    old_files = [task.file for task in group.tasks]
    return old_files, new_files
```

### The Commit

The commit atomically swaps old files for new files. **Also existing code.**

```python
def commit_rewrite(
    table: Table,
    old_files: list[DataFile],
    new_files: list[DataFile],
) -> None:
    """
    Atomically replace old data files with new data files.
    Creates an OVERWRITE snapshot.
    """
    from pyiceberg.table import Transaction

    with table.transaction() as tx:
        snapshot_update = tx.update_snapshot().overwrite()

        for old_file in old_files:
            snapshot_update._deleted_data_files.add(old_file)

        for new_file in new_files:
            snapshot_update._added_data_files.append(new_file)

        snapshot_update.commit()
```

### The Entry Point

Everything wired together on `MaintenanceTable`:

```python
# pyiceberg/table/maintenance.py

@dataclass
class RewriteDataFilesResult:
    rewritten_data_files_count: int
    added_data_files_count: int
    rewritten_bytes_count: int


class MaintenanceTable:
    # ... existing code ...

    def rewrite_data_files(
        self,
        filter: BooleanExpression = ALWAYS_TRUE,
    ) -> RewriteDataFilesResult:
        table = self.tbl

        # Early exit
        if table.current_snapshot() is None:
            return RewriteDataFilesResult(0, 0, 0)

        # Get target file size from writer config
        target_file_size = int(table.properties.get(
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT,
        ))

        # 1. Scan for matching files
        tasks = list(table.scan(row_filter=filter).plan_files())

        # 2-3. Plan groups
        groups = plan_rewrite(tasks, target_file_size)

        if not groups:
            return RewriteDataFilesResult(0, 0, 0)

        # 4. Rewrite each group
        all_old_files: list[DataFile] = []
        all_new_files: list[DataFile] = []

        for group in groups:
            old_files, new_files = rewrite_group(table, group)
            all_old_files.extend(old_files)
            all_new_files.extend(new_files)

        # 5. Atomic commit
        commit_rewrite(table, all_old_files, all_new_files)

        return RewriteDataFilesResult(
            rewritten_data_files_count=len(all_old_files),
            added_data_files_count=len(all_new_files),
            rewritten_bytes_count=sum(f.file_size_in_bytes for f in all_old_files),
        )
```

---

## Edge Cases to Handle

| Edge Case | How Java Handles It | How We Should Handle It |
|---|---|---|
| Empty table (no snapshots) | Returns `EMPTY_RESULT` immediately | Return `RewriteDataFilesResult(0, 0, 0)` early |
| No files match filter | `plan_files()` returns `[]` → no groups | `plan_rewrite()` returns `[]` → return empty result |
| All files already optimal size | `filterFiles()` removes all → no groups | No files pass size filter → return empty result |
| Single file in a group | `filterFileGroups()` drops it (< 5 files) | Unless its total size > target (oversized file), skip it |
| Filter is `ALWAYS_TRUE` | Scans all files | Same — compact entire table |
| Table is unpartitioned | All files in one partition group | `task.file.partition` is `Record()` → all in one group |
| Files have position deletes | `ArrowScan.to_table()` applies them during read | Same — deletes are merged during read, new files are clean |

---

## How the Writer's Bin-Packing Actually Works

The issue says "the same bin-packing constraints of the writer." Here's exactly what that means at the code level:

```python
# pyiceberg/io/pyarrow.py, _dataframe_to_data_files() — line 2873

def _dataframe_to_data_files(table_metadata, df, io):
    target_file_size = table_metadata.properties["write.target-file-size-bytes"]  # 512MB

    if spec.is_unpartitioned():
        # For each batch group that fits in target_file_size:
        for batches in bin_pack_arrow_table(df, target_file_size):
            yield write_file(WriteTask(batches))
    else:
        # Split df by partition values first
        for partition in _determine_partitions(spec, schema, df):
            # Then bin-pack each partition's data
            for batches in bin_pack_arrow_table(partition.arrow_table, target_file_size):
                yield write_file(WriteTask(batches, partition_key=partition.key))
```

And `bin_pack_arrow_table` splits an Arrow table into target-sized chunks:

```python
# pyiceberg/io/pyarrow.py, bin_pack_arrow_table() — line 2740

def bin_pack_arrow_table(tbl, target_file_size):
    avg_row_size = tbl.nbytes / tbl.num_rows
    target_rows_per_file = max(1, int(target_file_size / avg_row_size))
    batches = tbl.to_batches(max_chunksize=target_rows_per_file)  # PyArrow splits rows

    return PackingIterator(
        items=batches,
        target_weight=target_file_size,
        weight_func=lambda x: x.nbytes,  # actual byte size, not row estimate
    )
    # Each yielded list of batches → one output file ≈ target_file_size
```

So when we call `_dataframe_to_data_files()` in the rewriter, we automatically get:
- ✅ Files sized to `write.target-file-size-bytes`
- ✅ Correct partitioning
- ✅ Parquet with proper statistics (min/max, null counts, column sizes)
- ✅ Compression settings from table properties
- ✅ Row group size limits from table properties

---

## Test Strategy

### Unit Tests
1. **Planner tests** — given a list of `FileScanTask` objects with known sizes/partitions, verify grouping and filtering produce correct groups
2. **Empty table** — verify early return
3. **All files optimal** — verify no groups produced
4. **Single oversized file** — verify it gets its own group

### Integration Tests
1. **Unpartitioned table**: append many small files → compact → verify file count decreased, row count unchanged
2. **Partitioned table**: append small files across partitions → compact → verify each partition compacted independently
3. **With filter**: compact only one partition, verify others untouched
4. **With position deletes**: verify deleted rows are excluded from compacted files
5. **Idempotency**: compact → compact again → no changes (all files are already optimal)

---

## Java ↔ Python Line-by-Line Mapping

| Step | Java | Python |
|---|---|---|
| Scan with filter | `table.newScan().filter(f).planFiles()` | `table.scan(row_filter=f).plan_files()` |
| Group by partition | `groupByPartition(table, ..., tasks)` | `defaultdict(list)` + loop |
| Target file size | `targetFileSize` from `TableProperties` | `TableProperties.WRITE_TARGET_FILE_SIZE_BYTES` |
| Min file size | `targetFileSize * MIN_FILE_SIZE_DEFAULT_RATIO` (0.75) | `int(target * 0.75)` |
| Max file size | `targetFileSize * MAX_FILE_SIZE_DEFAULT_RATIO` (1.80) | `int(target * 1.80)` |
| Filter files | `outsideDesiredFileSizeRange(task)` | `size < min or size > max` |
| Bin-pack | `BinPacking.ListPacker(maxGroupSize, 1, false).pack(...)` | `ListPacker(max_size, 1, False).pack(...)` |
| Filter groups | `group.size() >= minInputFiles` | `len(group) >= 5` |
| Read files | `spark.read.format("iceberg")...load(groupId)` | `ArrowScan(...).to_table(tasks)` |
| Write files | `df.write.format("iceberg")...save(groupId)` | `_dataframe_to_data_files(metadata, df, io)` |
| Commit | `table.newRewrite().deleteFile().addFile().commit()` | `tx.update_snapshot().overwrite()` → delete + add + commit |
