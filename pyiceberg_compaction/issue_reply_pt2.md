# Draft Reply to Issue #1092 — Detailed Proposal

> This is a draft for posting to the GitHub issue. Edit tone/wording as needed before posting.

---

Thanks for the guidance, @kevinjqliu. Modeling it after the Spark `rewrite_data_files` procedure makes sense for consistency across the ecosystem.

I've spent time studying both the Java/Spark implementation (specifically `BinPackRewriteFilePlanner`, `SizeBasedFileRewritePlanner`, `SparkBinPackFileRewriteRunner`, and `RewriteDataFilesCommitManager`) and the existing PyIceberg internals, and I think the scope of the first version described in the original issue maps very cleanly onto infrastructure PyIceberg already has.

## Proposed API

Following the existing pattern in `MaintenanceTable` (which already has `expire_snapshots()`), I'd add a `rewrite_data_files()` method:

```python
from pyiceberg.expressions import EqualTo

table = catalog.load_table("db.my_table")

# Basic compaction — rewrites all small/oversized files
result = table.maintenance.rewrite_data_files()

# Compact only files matching a predicate
result = table.maintenance.rewrite_data_files(
    filter=EqualTo("date", "2024-01-01")
)

# Result tells you what happened
print(f"Rewrote {result.rewritten_data_files_count} files → {result.added_data_files_count} files")
```

I considered a fluent builder approach (`.filter().options().commit()`), but looking at the existing codebase, I think a simpler function-call API is more appropriate as a first version — we can always add a builder wrapper later if needed.

## Implementation Approach

What encouraged me is that the two requirements in the original issue map directly onto existing PyIceberg components:

### Requirement 1: "take a predicate expression to find data files matching the filter"

This is already fully implemented. `table.scan(row_filter=expr).plan_files()` does the exact same 3-level pruning as the Java side (`table.newScan().filter(f).planFiles()`):
- Manifest-level pruning via partition summaries
- Partition-value pruning per data file
- Column-statistics pruning (min/max bounds)

No new code needed for this piece.

### Requirement 2: "group by partitions and rewrite using the same bin-packing constraints of the writer"

This breaks down into sub-steps, most of which reuse existing code:

| Sub-step | Java | PyIceberg | Status |
|---|---|---|---|
| Group files by partition | `groupByPartition()` in `BinPackRewriteFilePlanner` | `defaultdict(list)` + loop on `task.file.partition` | ~5 lines new |
| Filter files outside size range | `outsideDesiredFileSizeRange()` in `SizeBasedFileRewritePlanner` | `size < min_size or size > max_size` | ~10 lines new |
| Bin-pack filtered files into groups | `BinPacking.ListPacker.pack()` | `ListPacker` in `pyiceberg/utils/bin_packing.py` | **Already exists** |
| Filter groups with too few files | `enoughInputFiles()` in `SizeBasedFileRewritePlanner` | `len(group) >= 5` | ~5 lines new |
| Read files into Arrow table | Spark `read.format("iceberg")` | `ArrowScan.to_table()` | **Already exists** |
| Write new files with target sizing | Spark `write.format("iceberg")` | `_dataframe_to_data_files()` in `io/pyarrow.py` | **Already exists** |
| Atomic commit (delete old + add new) | `table.newRewrite()` via `RewriteDataFilesCommitManager` | `_OverwriteFiles` in `table/update/snapshot.py` | **Already exists** |

The key phrase — **"the same bin-packing constraints of the writer"** — means using the same `write.target-file-size-bytes` table property (default 512MB) that the normal `table.append()` / `table.overwrite()` path already uses. In Java, this is in `BinPackRewriteFilePlanner.defaultTargetFileSize()` which reads `TableProperties.WRITE_TARGET_FILE_SIZE_BYTES`. PyIceberg already has this property defined and used by `_dataframe_to_data_files()`.

### Size thresholds (from Java defaults)

Following the Java implementation in `SizeBasedFileRewritePlanner.sizeThresholds()`:
- `target_file_size` = `write.target-file-size-bytes` (512 MB)
- `min_file_size` = 75% of target (384 MB) — files below this are candidates
- `max_file_size` = 180% of target (921 MB) — files above this are candidates
- `min_input_files` = 5 — groups need at least this many files
- `max_file_group_size` = 100 GB — cap per bin-pack group

## Files to Change

| File | Change |
|---|---|
| `pyiceberg/table/maintenance.py` | Add `rewrite_data_files()` method |
| New: `pyiceberg/table/compaction.py` (suggested) | Planner logic: group by partition, filter, bin-pack |
| Tests | Unit tests for planner + integration test for end-to-end |

The planner is kept separate from the execution so that it can be reused if we later add different execution engines (Ray, Dask).

## What's NOT in scope for v1

To keep the first PR focused:
- ❌ Sort / Z-order strategies (bin-pack only)
- ❌ Partial progress / multi-commit
- ❌ Concurrent file group rewrites
- ❌ Delete-aware compaction (delete-file-threshold, delete-ratio-threshold)
- ❌ Output spec evolution (output-spec-id)
- ❌ Distributed execution (Ray/Dask)

These can all be layered on naturally in follow-up PRs.

---

Does this approach work? Happy to start a draft PR with the implementation. Let me know if you'd like me to adjust the API shape or scope.
