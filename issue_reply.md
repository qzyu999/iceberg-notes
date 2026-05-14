# Proposed Multi-Phase Approach for `rewrite_data_files` in PyIceberg

**Issue**: [apache/iceberg-python#1092](https://github.com/apache/iceberg-python/issues/1092)

---

Hi everyone 👋, I'd like to propose a multi-phase approach for implementing `rewrite_data_files` in PyIceberg. I've been studying the Java/Spark implementation (v3.5) in detail — including `RewriteDataFilesSparkAction`, `BinPackRewriteFilePlanner`, `SizeBasedFileRewritePlanner`, `SparkBinPackFileRewriteRunner`, and `RewriteDataFilesCommitManager` — and I believe we can mirror the architecture in a way that is both immediately useful and incrementally extensible.

The idea is to break this into **3 phases**, each delivered as its own PR, so that each is reviewable, testable, and independently mergeable.

---

## Phase 1: Core BinPack Compaction (Single-Node / PyArrow)

**Goal**: Deliver a working `rewrite_data_files` with bin-pack compaction using PyArrow on a single node. This matches the scope described in the original issue.

### User-Facing API

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("my_catalog")
table = catalog.load_table("db.my_table")

# Basic compaction (bin-pack, default options)
result = table.maintenance.rewrite_data_files()

# With a filter
from pyiceberg.expressions import EqualTo
result = table.maintenance.rewrite_data_files(
    filter=EqualTo("date", "2024-01-01")
)
```

### What Gets Built

1. **`MaintenanceTable.rewrite_data_files()`** — Entry point on the existing `MaintenanceTable` class in `pyiceberg/table/maintenance.py`.
2. **`BinPackPlanner`** — Python port of the core planning logic from `SizeBasedFileRewritePlanner` + `BinPackRewriteFilePlanner`:
   - Scan the table for data files (using the existing `Table.scan()` / manifest reading).
   - Group files by partition.
   - Filter files outside the desired size range (`min-file-size-bytes` ↔ `max-file-size-bytes`).
   - Bin-pack filtered files into groups (≤ `max-file-group-size-bytes`).
   - Filter groups by `min-input-files` / content thresholds.
3. **`PyArrowRewriteRunner`** — Reads files with PyArrow, concatenates, splits by target file size, and writes back to new Parquet files.
4. **Commit** — Uses the existing PyIceberg transaction/snapshot API to atomically swap old files for new files (mirroring `RewriteDataFilesCommitManager.commitFileGroups()` which calls `table.newRewrite()`).
5. **Result dataclass** — Returns counts of rewritten/added files.

### Supported Options (Phase 1)

| Option | Default | Notes |
|---|---|---|
| `target-file-size-bytes` | Table property or 512MB | Target output file size |
| `min-file-size-bytes` | 75% of target | Lower bound for "too small" |
| `max-file-size-bytes` | 180% of target | Upper bound for "too large" |
| `min-input-files` | 5 | Min files per group to trigger rewrite |
| `max-file-group-size-bytes` | 100GB | Max data per bin-pack group |
| `rewrite-all` | false | Force rewrite regardless of size |

### Design Choices
- **Planner and Runner are separate classes** (following the Java architecture). The planner is engine-agnostic; the runner is engine-specific. This enables Phase 3.
- **Runner is injected via a parameter** (e.g., `runner="pyarrow"`) so it can be swapped later without changing the API.
- Sequential processing of file groups (no parallelism in Phase 1 — keeps the code simple and reviewable).

---

## Phase 2: Expanded Options & Robustness

**Goal**: Add the remaining configuration options, partial progress support, delete-aware compaction, and sort/z-order strategies.

### What Gets Added

1. **Delete-aware compaction**:
   - `delete-file-threshold` — Force rewrite if a file has too many associated delete files.
   - `delete-ratio-threshold` — Force rewrite if deleted-row ratio exceeds threshold (default: 30%).
2. **Partial progress**:
   - `partial-progress.enabled` — Commit groups incrementally as they complete.
   - `partial-progress.max-commits` — Cap on intermediate commits.
   - `partial-progress.max-failed-commits` — Tolerance for failed commits.
3. **Sort strategy**:
   - `strategy="sort"` with `sort_order` parameter.
   - Sorts data within each file group before writing (using PyArrow's `sort_indices`).
4. **Additional options**:
   - `max-concurrent-file-group-rewrites` — Parallel processing of file groups (using `concurrent.futures`).
   - `rewrite-job-order` — Control which groups are processed first (bytes-asc, bytes-desc, files-asc, files-desc).
   - `use-starting-sequence-number` — For conflict avoidance with concurrent deletes.
   - `output-spec-id` — Repartition data to a new partition spec during compaction.
   - `remove-dangling-deletes` — Clean up orphaned delete files after compaction.

### Updated API

```python
result = table.maintenance.rewrite_data_files(
    strategy="sort",
    sort_order=SortOrder(table.schema(), ...),
    filter=EqualTo("date", "2024-01-01"),
    options={
        "target-file-size-bytes": 268_435_456,
        "partial-progress.enabled": True,
        "max-concurrent-file-group-rewrites": 4,
        "delete-ratio-threshold": 0.2,
    }
)
```

---

## Phase 3: Pluggable Execution Engines (Ray, Dask, etc.)

**Goal**: Make the execution engine pluggable so that large-scale compaction can leverage distributed frameworks.

### Architecture

Because Phase 1 separates planning from execution, Phase 3 only needs to:

1. **Define a `RewriteRunner` protocol/ABC**:
   ```python
   class RewriteRunner(ABC):
       @abstractmethod
       def rewrite(self, file_group: RewriteFileGroup) -> List[DataFile]:
           """Read input files and write optimized output files."""
           ...
   ```

2. **Implement engine-specific runners**:
   - `PyArrowRewriteRunner` (already exists from Phase 1)
   - `RayRewriteRunner` — Distributes file group rewrites across a Ray cluster.
   - `DaskRewriteRunner` — Potential future option.
   - `SparkRewriteRunner` — Could even delegate to PySpark for hybrid workflows.

3. **User selects engine at call site**:
   ```python
   result = table.maintenance.rewrite_data_files(
       runner="ray",          # or "pyarrow" (default), "dask", etc.
       options={...}
   )
   ```

### Why This Works
The Java/Spark codebase already follows this exact pattern:
- `FileRewritePlanner` (interface) → `SizeBasedFileRewritePlanner` → `BinPackRewriteFilePlanner`
- `FileRewriteRunner` (interface) → `SparkRewriteRunner` → `SparkDataFileRewriteRunner` → `SparkBinPackFileRewriteRunner`

The planner is in `core/` (engine-agnostic). The runners are in `spark/` (engine-specific). We mirror this: the planner lives in `pyiceberg/table/` and the runners are pluggable.

---

## Summary

| Phase | Scope | Engine | PR Size |
|-------|-------|--------|---------|
| **1** | Core bin-pack compaction, basic options, `MaintenanceTable` API | PyArrow (single-node) | Medium |
| **2** | All options, sort/z-order, partial progress, delete-aware, parallelism | PyArrow (single-node) | Medium |
| **3** | Pluggable engine interface, Ray/Dask runners | Ray, Dask, etc. | Small-Medium |

I'm happy to start with a PR for Phase 1. Would love feedback on the API shape and whether this phased approach works for the project's roadmap. 🙏
