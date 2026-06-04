# Nuance: Partition Spec Evolution and Its Impact on Compaction

> This document explores a subtle but critical piece of logic in `BinPackRewriteFilePlanner`:
> the handling of **old partition specs** that may exist after schema/partition evolution.
> It explains what the Java code does, why it matters, and how to handle it in pyiceberg.

---

## Table of Contents

- [The Problem in One Sentence](#the-problem-in-one-sentence)
- [What Is Partition Spec Evolution?](#what-is-partition-spec-evolution)
- [The Java Code That Handles This](#the-java-code-that-handles-this)
- [Why the Java Code Does What It Does](#why-the-java-code-does-what-it-does)
- [The Second Spec Nuance: outputSpecId](#the-second-spec-nuance-outputspecid)
- [Walk Through a Concrete Example](#walk-through-a-concrete-example)
- [How PyIceberg Handles Specs Today](#how-pyiceberg-handles-specs-today)
- [What Our Python Compaction Must Do](#what-our-python-compaction-must-do)
- [Edge Cases to Test](#edge-cases-to-test)

---

## The Problem in One Sentence

When an Iceberg table's partition scheme changes (e.g., from `identity(date)` to `month(date)`), old data files still carry the old partition spec — and compaction must handle the mismatch correctly or it will produce corrupt output.

---

## What Is Partition Spec Evolution?

Iceberg supports **changing how a table is partitioned without rewriting existing data**. This is called *partition spec evolution* and is a core Iceberg feature.

### How it works on disk

Every table metadata JSON has:
```json
{
  "partition-specs": [
    {"spec-id": 0, "fields": [{"source-id": 3, "field-id": 1000, "name": "date", "transform": "identity"}]},
    {"spec-id": 1, "fields": [{"source-id": 3, "field-id": 1001, "name": "date_month", "transform": "month"}]}
  ],
  "default-spec-id": 1
}
```

And every **data file** records which spec it was written under:
```json
{
  "file_path": "data/date=2024-01-15/00000.parquet",
  "spec_id": 0,
  "partition": {"date": "2024-01-15"}
}
```

Notice: `spec_id: 0` means this file was written under the old `identity(date)` spec. Even though the table's current spec is now `month(date)` (spec_id: 1), this old file is still valid and queryable.

### Why tables evolve

Common reasons for changing partition specs:
- Too many partitions (daily → monthly to reduce partition count)
- Better query patterns (switched from `bucket(user_id)` → `identity(region)`)
- Added a partition field (unpartitioned → partitioned by `date`)
- Removed a partition field (partitioned by `country` → unpartitioned)

### The key insight

After partition evolution, a single table snapshot can contain **data files from multiple different partition specs simultaneously**. This is by design — Iceberg doesn't require rewriting old data when the spec changes.

---

## The Java Code That Handles This

The critical code is in `groupByPartition()` at [BinPackRewriteFilePlanner.java, lines 310-326](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java):

```java
private StructLikeMap<List<FileScanTask>> groupByPartition(
    Table table, Types.StructType partitionType, Iterable<FileScanTask> tasks) {

  // 1. Create map keyed by the CURRENT spec's partition type
  StructLikeMap<List<FileScanTask>> filesByPartition = StructLikeMap.create(partitionType);

  // 2. Create an empty struct for "homeless" files
  StructLike emptyStruct = GenericRecord.create(partitionType);

  for (FileScanTask task : tasks) {
    // 3. THE KEY CHECK: does this file's spec match the table's current spec?
    StructLike taskPartition =
        task.file().specId() == table.spec().specId()
            ? task.file().partition()   // YES → use its partition value
            : emptyStruct;              // NO  → treat as unpartitioned

    // 4. Group under the resolved key
    filesByPartition.computeIfAbsent(taskPartition, unused -> Lists.newArrayList()).add(task);
  }
  return filesByPartition;
}
```

### The three pieces to understand

#### Piece 1: `partitionType` (line 297, 312)

```java
Types.StructType partitionType = table().spec().partitionType();
StructLikeMap<List<FileScanTask>> filesByPartition = StructLikeMap.create(partitionType);
```

The map is keyed by **the current spec's partition type**. If the current spec is `month(date)`, the keys are `{date_month: "2024-01"}` not `{date: "2024-01-15"}`.

This matters because `StructLikeMap` uses the struct type for equality comparison. Two `StructLike` objects are only "equal" if they have the same type and values.

#### Piece 2: `emptyStruct` (line 313)

```java
StructLike emptyStruct = GenericRecord.create(partitionType);
```

This creates a record with all null fields — like a sentinel value meaning "no partition." All files with incompatible specs get grouped under this single key.

#### Piece 3: The spec ID check (line 319-320)

```java
task.file().specId() == table.spec().specId()
    ? task.file().partition()   // compatible
    : emptyStruct;              // incompatible
```

This is the critical decision:
- **Compatible** (`specId` matches): The file's partition value can be used directly. A file written under `month(date)` with partition `{date_month: "2024-01"}` groups naturally with other `month(date)` files.
- **Incompatible** (`specId` differs): The file's partition value is meaningless in the current spec's context. A file written under `identity(date)` with partition `{date: "2024-01-15"}` can't be compared to `{date_month: "2024-01"}` — the types are different. So it's lumped into the "unpartitioned" bucket.

---

## Why the Java Code Does What It Does

### Why not just skip old-spec files?

You could skip them, but then compaction would never clean up files written under old specs. Over time, these files accumulate and degrade query performance. They need to be compacted eventually.

### Why not convert old partitions to new partitions?

You could try to map `{date: "2024-01-15"}` → `{date_month: "2024-01"}`, but:

1. **The data file might span multiple new partitions.** If the old spec was `identity(region)` and the new spec is `bucket(user_id, 16)`, there's no way to know which bucket(s) the data belongs to without reading the file.

2. **The transform might be lossy.** You can't un-transform `month("2024-01")` back to `identity("2024-01-15")` — you don't know the exact date.

3. **The file might have been written unpartitioned.** Spec 0 might have had no fields at all.

### Why group all incompatible files together?

The comment in the Java source explains it directly:

> *If a task uses an incompatible partition spec the data inside could contain values which belong to multiple partitions in the current spec. Treating all such files as un-partitioned and grouping them together helps to minimize new files made.*

By grouping all old-spec files together:
- They get read as one batch during rewriting
- The **writer** (`_dataframe_to_data_files`) will re-evaluate the data against the **current** partition spec
- The output files will be correctly partitioned under the current spec
- This effectively "migrates" old-spec files to the new spec as a side effect of compaction

### What happens during the rewrite

```
INPUT (old spec):                          OUTPUT (current spec):
  data/date=2024-01-15/file_a.parquet        data/date_month=2024-01/new_file_1.parquet
  data/date=2024-01-20/file_b.parquet        data/date_month=2024-02/new_file_2.parquet
  data/date=2024-02-03/file_c.parquet        ← re-partitioned under month(date)!
```

The reader loads all rows from `file_a`, `file_b`, `file_c` into one Arrow table. The writer evaluates `month(date)` on each row and produces new files partitioned by month. Files `a` and `b` end up in `2024-01`, file `c` ends up in `2024-02`.

---

## The Second Spec Nuance: outputSpecId

There's a second, related concept: the **output spec ID**. This is the spec used to partition the **output** files.

### Java source

[SizeBasedFileRewritePlanner.java, lines 285-293](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java):

```java
private int outputSpecId(Map<String, String> options) {
    int specId = PropertyUtil.propertyAsInt(
        options,
        RewriteDataFiles.OUTPUT_SPEC_ID,   // "output-spec-id"
        table.spec().specId()              // default: current spec
    );
    Preconditions.checkArgument(
        table.specs().containsKey(specId),
        "Cannot use output spec id %s because the table does not contain a reference to this spec-id.",
        specId);
    return specId;
}
```

### What this means

1. **Default behavior**: Output files are written under the table's **current** partition spec. If you evolved from `identity(date)` to `month(date)`, compacted files use `month(date)`.

2. **Override**: Users can explicitly pass `output-spec-id` to write output files under a specific (possibly old) spec. This is rare but useful for testing or partial migrations.

3. **Validation**: The specified spec must exist in `table.specs()` — you can't invent a new spec.

### How the output spec flows through

```
outputSpecId (from options or default)
  → stored in RewriteFileGroup
    → passed to SparkBinPackFileRewriteRunner.doRewrite()
      → Spark writer uses this spec to partition output files
      → New DataFile objects carry this spec_id
        → Manifest entries for new files use this spec_id
```

### For Phase 1 in PyIceberg

We don't need to support `output-spec-id` as a user option. We always use `table.metadata.default_spec_id`, which is the current spec. `_dataframe_to_data_files()` already does this:

```python
# pyiceberg/io/pyarrow.py, line 2913-2914
partitions = _determine_partitions(
    spec=table_metadata.spec(),     # ← always uses current spec
    schema=table_metadata.schema(),
    arrow_table=df
)
```

---

## Walk Through a Concrete Example

### Setup

1. Create table with spec `identity(date)` → spec_id=0
2. Write 100 files over January 2024 (`date=2024-01-01` through `date=2024-01-31`)
3. Evolve partition spec to `month(date)` → spec_id=1
4. Write 50 more files for February 2024 (`date_month=2024-02`)
5. Run compaction on the entire table

### State before compaction

```
Spec 0 files (old):                  Spec 1 files (current):
  data/date=2024-01-01/f01.parquet     data/date_month=2024-02/f101.parquet
  data/date=2024-01-01/f02.parquet     data/date_month=2024-02/f102.parquet
  data/date=2024-01-02/f03.parquet     ...
  ...                                  data/date_month=2024-02/f150.parquet
  data/date=2024-01-31/f100.parquet
```

### What `groupByPartition()` produces

```
files_by_partition = {
    emptyStruct:                 [f01, f02, ..., f100],   ← ALL 100 old-spec files
    Record[date_month=2024-02]:  [f101, f102, ..., f150], ← 50 current-spec files
}
```

Why? The 100 old files have `spec_id=0` ≠ current `spec_id=1`, so they all get lumped under `emptyStruct`.

### What the rewriter does

**Group 1 (emptyStruct → 100 old-spec files):**
1. Read all 100 files → one big Arrow table with 3100 rows (31 days × 100 rows)
2. Write using current spec `month(date)`:
   - `_determine_partitions()` evaluates `month(date)` on every row
   - All rows have `date` in January → partition value = `2024-01`
   - Output: 1 file → `data/date_month=2024-01/new_f01.parquet` (spec_id=1)

**Group 2 (date_month=2024-02 → 50 current-spec files):**
1. Read all 50 files → one Arrow table
2. Write using current spec `month(date)`:
   - All rows already have `date_month=2024-02`
   - Output: 1 file → `data/date_month=2024-02/new_f02.parquet` (spec_id=1)

### State after compaction

```
data/date_month=2024-01/new_f01.parquet   ← spec_id=1, contains ALL January data
data/date_month=2024-02/new_f02.parquet   ← spec_id=1, contains ALL February data
```

The old `data/date=*/` files are marked as `DELETED` in the new snapshot. The table is now fully migrated to spec 1. **Compaction implicitly performed partition spec migration.**

---

## How PyIceberg Handles Specs Today

### Metadata layer

[pyiceberg/table/metadata.py](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/table/metadata.py):

```python
class TableMetadata:
    partition_specs: list[PartitionSpec]     # ALL historical specs
    default_spec_id: int                     # ID of the current spec

    def spec(self) -> PartitionSpec:
        """Returns the current/default partition spec."""
        return next(s for s in self.partition_specs if s.spec_id == self.default_spec_id)

    def specs(self) -> dict[int, PartitionSpec]:
        """Returns all specs by ID (including historical ones)."""
        return {spec.spec_id: spec for spec in self.partition_specs}
```

### Data file layer

Every `DataFile` object has:
```python
data_file.spec_id       # int — which spec this file was written under
data_file.partition      # Record — the partition values under that spec
```

### Writer layer

[pyiceberg/io/pyarrow.py, line 2913](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/io/pyarrow.py):

```python
def _dataframe_to_data_files(table_metadata, df, io):
    if table_metadata.spec().is_unpartitioned():
        # Write without partitioning
        yield from write_file(io, table_metadata, tasks=...)
    else:
        # Partition using the CURRENT spec
        partitions = _determine_partitions(
            spec=table_metadata.spec(),         # ← always current spec
            schema=table_metadata.schema(),
            arrow_table=df
        )
        yield from write_file(io, table_metadata, tasks=...)
```

The writer **always** uses the current spec. Output files will always have `spec_id = default_spec_id`. This is exactly what Java does by default (unless `output-spec-id` is overridden).

### Scan layer

Every `FileScanTask` already carries `file.spec_id`, so we can inspect it during compaction planning.

---

## What Our Python Compaction Must Do

### During planning (groupByPartition)

```python
from collections import defaultdict

def group_by_partition(table, tasks):
    """
    Mirror of BinPackRewriteFilePlanner.groupByPartition().

    Files with the current spec → grouped by their actual partition value.
    Files with old/incompatible specs → grouped together under None.
    """
    current_spec_id = table.spec().spec_id
    files_by_partition = defaultdict(list)

    for task in tasks:
        if task.file.spec_id == current_spec_id:
            # Compatible: use actual partition value
            partition_key = str(task.file.partition)
        else:
            # Incompatible: treat as unpartitioned
            partition_key = None

        files_by_partition[partition_key].append(task)

    return files_by_partition
```

### During rewriting

No special handling needed. `_dataframe_to_data_files()` already:
1. Uses `table_metadata.spec()` (current spec) for partitioning
2. Calls `_determine_partitions()` which evaluates partition transforms on the actual data
3. Produces output `DataFile` objects with `spec_id = default_spec_id`

So when old-spec files are read into an Arrow table and written back, the writer automatically re-partitions them under the current spec. **The migration happens for free.**

### During commit

No special handling needed. `_OverwriteFiles` accepts a mix of `DataFile` objects with different spec IDs in the delete list (old files) and the add list (new files). The manifest writer in snapshot.py already handles multiple spec IDs:

```python
# pyiceberg/table/update/snapshot.py, line 205-207
partition_groups[deleted_entry.data_file.spec_id].append(deleted_entry)
for spec_id, entries in partition_groups.items():
    with self.new_manifest_writer(self.spec(spec_id)) as writer:
        # Writes manifest entries grouped by spec_id
```

---

## Edge Cases to Test

### 1. Table with no spec evolution (common case)

All files have the same `spec_id`. The `if` check always takes the "compatible" branch.
Nothing special happens.

### 2. Table with evolved spec, only current-spec files

Can happen if the table was evolved and then `expire_snapshots` cleaned up old snapshots
(and old manifest entries). Same as case 1 in practice.

### 3. Table with evolved spec, mix of old and new files

The main case this nuance exists for. Old-spec files get grouped under `None`,
new-spec files get grouped by their partition values. Both groups are processed
independently. Old-spec files get re-partitioned to the current spec on write.

### 4. Table with evolved spec, only old-spec files

All files are "incompatible." Everything goes into one group under `None`.
After compaction, all output files use the current spec.

### 5. Table evolved from partitioned → unpartitioned

Current spec is unpartitioned (`spec.is_unpartitioned() == True`).
All files (whether they have old partition values or not) should be written
as unpartitioned output. `_dataframe_to_data_files` handles this via the
`is_unpartitioned()` branch.

### 6. Table evolved from unpartitioned → partitioned

Current spec has partition fields. Old files (spec_id=0, unpartitioned) go
under `None`. When rewritten, `_determine_partitions()` evaluates the partition
transform on the actual data and produces correctly partitioned output files.

### 7. Multiple spec evolutions (spec_id 0, 1, 2, ..., N)

Files from specs 0 through N-1 all have incompatible spec IDs.
They ALL get grouped under `None` together. Only spec N files are
grouped by their partition values. This is correct because all old-spec
files need re-partitioning to spec N regardless of which old spec they used.

---

## Summary

| Aspect | Java | Python (Phase 1) |
|---|---|---|
| Spec compatibility check | `task.file().specId() == table.spec().specId()` | `task.file.spec_id == table.spec().spec_id` |
| Incompatible handling | Group under `emptyStruct` | Group under `None` |
| Output spec | Default: current spec (overridable via `output-spec-id`) | Always current spec |
| Re-partitioning on write | Handled by Spark's Iceberg connector | Handled by `_determine_partitions()` in `_dataframe_to_data_files()` |
| Metadata references | `table.specs()` — all historical specs | `table.metadata.specs()` — same |
| Commit with mixed specs | `RewriteFiles` handles it natively | `_OverwriteFiles` groups manifests by `spec_id` (lines 205-207 in snapshot.py) |

**Key takeaway**: The partition spec evolution handling is mostly **free** in Python because the writer (`_dataframe_to_data_files`) always uses the current spec and the commit layer (`_OverwriteFiles`) already handles mixed spec IDs. The only new code needed is the `spec_id` check in `group_by_partition()` — about 3 lines.
