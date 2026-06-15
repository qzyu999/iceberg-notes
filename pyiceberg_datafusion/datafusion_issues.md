# DataFusion Integration: Detailed Issue Breakdown

Each issue below specifies three implementation tracks and the OOM-safety guarantee.

**Three Tracks** (applied to every issue):
- **Track 1 (Immediate)**: Python-side DataFusion via `datafusion-python` — no iceberg-rust changes needed
- **Track 2 (Long-term)**: Rust-side DataFusion via `pyiceberg_core.execution` — full pipeline in Rust
- **PyArrow Fallback**: In-memory implementation that works for small data; OOMs gracefully on large

**OOM-Safety Invariant** (applies to all issues):
> A mid-operation OOM cannot corrupt table state. Iceberg's optimistic concurrency control guarantees that no snapshot is committed until the operation completes successfully. Data files written before OOM become orphans (invisible, cleanable) — never referenced by any snapshot.

---

## Issue 1: `[Infra] Add execution engine resolution module`

### Summary

Create `pyiceberg/execution/engine.py` — a thin module that detects whether `pyiceberg-core` (DataFusion) is available and returns an engine enum. All subsequent issues depend on this.

### Implementation

```python
# pyiceberg/execution/engine.py
import warnings
from enum import Enum, auto

class ExecutionEngine(Enum):
    DATAFUSION = auto()
    PYARROW = auto()

def resolve_engine(operation: str) -> ExecutionEngine:
    try:
        import pyiceberg_core  # noqa: F401
        return ExecutionEngine.DATAFUSION
    except ImportError:
        warnings.warn(
            f"'{operation}' will use in-memory (PyArrow) execution. "
            f"For large tables, install: pip install 'pyiceberg[pyiceberg-core]'",
            UserWarning,
            stacklevel=3,
        )
        return ExecutionEngine.PYARROW
```

### Tracks

| Track | Applicable? | Notes |
|-------|-------------|-------|
| Track 1 (Python DF) | N/A | This IS the dispatch infrastructure |
| Track 2 (Rust DF) | N/A | Same module works for both tracks |
| PyArrow Fallback | N/A | The module enables the fallback pattern |

### OOM Safety

No data operations — pure import check. Cannot OOM. Cannot corrupt anything.

### Acceptance Criteria

- [ ] `resolve_engine()` returns `DATAFUSION` when `pyiceberg-core` is installed
- [ ] `resolve_engine()` returns `PYARROW` with a `UserWarning` when not installed
- [ ] No module-level import of `pyiceberg_core` or `datafusion`
- [ ] Unit tests pass both with and without `pyiceberg-core` installed

---

## Issue 2: `[Infra] Add pyiceberg_core.execution Rust module`

### Summary

Add a new `execution` submodule to the `pyiceberg-core` Rust bindings (`bindings/python/src/execution.rs`) that exposes bounded-memory operations to Python via PyO3.

### Implementation

**Rust side** (`bindings/python/src/execution.rs`):
```rust
use datafusion::prelude::*;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;

/// Default batch size for DataFusion execution.
/// Matches DataFusion's own default (8192 rows per RecordBatch).
/// This balances per-batch overhead against memory granularity.
const DEFAULT_BATCH_SIZE: usize = 8192;

/// Memory utilization factor for spill threshold.
/// At 1.0, the pool spills only when the hard limit is hit.
/// Lower values (e.g., 0.8) leave headroom for framework overhead.
const MEMORY_POOL_FRACTION: f64 = 1.0;

fn create_bounded_session(memory_limit_bytes: usize) -> SessionContext {
    let target_partitions = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1);

    let config = SessionConfig::new()
        .with_batch_size(DEFAULT_BATCH_SIZE)
        .with_target_partitions(target_partitions);
    
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(memory_limit_bytes, MEMORY_POOL_FRACTION)
        .with_disk_manager(DiskManagerConfig::new())
        .build_arc()
        .unwrap();
    
    SessionContext::new_with_config_rt(config, runtime)
}

#[pyfunction]
fn execute_cow_rewrite(...) -> PyResult<CowRewriteResult> { ... }

#[pyfunction]
fn execute_compaction(...) -> PyResult<CompactionResult> { ... }
```

### Tracks

| Track | Applicable? | Notes |
|-------|-------------|-------|
| Track 1 (Python DF) | Partially | Can start with `datafusion-python` directly; this module provides the optimized path |
| Track 2 (Rust DF) | Yes — this IS Track 2 | Full execution in Rust below the GIL |
| PyArrow Fallback | N/A | This module is only called when DataFusion is chosen |

### Python-Side Developer API

The Rust module is an implementation detail. Users never import it directly. Instead, the parameters surface through the **Table method signatures** — a DuckDB-style UX where the user configures *what* they want, not *how* it runs:

```python
# User-facing API — this is all they see:
table.compact(memory_limit="512MB")
table.delete("status = 'expired'", memory_limit="1GB")
table.upsert(df, join_cols=["id"], memory_limit="2GB")

# The memory_limit flows through internally:
#   Table.compact(memory_limit="512MB")
#     → resolve_engine("compact")  → DATAFUSION
#       → pyiceberg_core.execution.execute_compaction(..., memory_limit="512MB")
#         → create_bounded_session(parse_memory("512MB"))
#           → FairSpillPool(536_870_912 bytes)
```

**Default behavior** (zero-config):

```python
# No memory_limit specified → uses a sensible default
table.compact()  # defaults to "512MB" (or reads from table property / pyiceberg config)
```

**Configuration hierarchy** (highest priority wins):

```python
# 1. Method argument (highest priority)
table.compact(memory_limit="2GB")

# 2. Table property
# SET IN TABLE: write.execution.memory-limit = 1GB

# 3. PyIceberg config (~/.pyiceberg.yaml)
# execution:
#   memory-limit: 512MB

# 4. Built-in default: 512MB
```

**The key principle**: the user never sees `SessionContext`, `FairSpillPool`, `batch_size`, or `target_partitions`. Those are internal tuning knobs that the Rust module manages. The user's only knob is `memory_limit` — a single human-readable string like DuckDB's `SET memory_limit = '4GB'`.

Advanced users who need fine-grained control can pass additional kwargs that map to DataFusion config:

```python
# Power user: override internals (rarely needed)
table.compact(
    memory_limit="2GB",
    execution_config={
        "batch_size": 16384,           # Larger batches for wide tables
        "target_partitions": 4,        # Limit parallelism (e.g., shared machine)
    },
)
```

But the 99% path is just:

```python
table.compact()  # Just works. Bounded memory. No config needed.
```

### OOM Safety

The Rust module configures `FairSpillPool` with a hard memory limit. If DataFusion itself encounters an unrecoverable error (e.g., disk full during spill), it returns a `DataFusionError` which is propagated as a Python exception. No snapshot is committed.

### Acceptance Criteria

- [ ] `from pyiceberg_core.execution import execute_cow_rewrite` works
- [ ] Memory limit is enforced (test with small budget + large input → spill occurs)
- [ ] Errors propagate cleanly as Python exceptions (no segfault, no panic)

---

## Issue 3: `[Fix] Streaming CoW delete via DataFusion`

### Summary

Fix the OOM in `Transaction.delete()` when Parquet files need rewriting. Currently loads entire files into memory via `ArrowScan.to_table()`.

### Current Code (OOMs)

```python
# pyiceberg/table/__init__.py — Transaction.delete()
for original_file in files:
    df = ArrowScan(...).to_table(tasks=[original_file])  # FULL FILE IN MEMORY
    filtered_df = df.filter(preserve_row_filter)
    # write filtered_df to new file
```

### Track 1 (Immediate — Python-side DataFusion)

```python
from datafusion import SessionContext

def _cow_delete_datafusion(self, files_to_rewrite, filter_expr, memory_limit):
    ctx = SessionContext()  # or configured with memory limit
    for file_task in files_to_rewrite:
        ctx.register_parquet("data", file_task.file.file_path)
        # Filter is the COMPLEMENT of the delete predicate (keep non-matching rows)
        result = ctx.sql(f"SELECT * FROM data WHERE NOT ({filter_expr})")
        filtered_table = result.to_arrow_table()
        # Write filtered_table to new Parquet file using existing write_file()
        new_files = write_file(self._table.io, self.table_metadata, [WriteTask(filtered_table)])
        # ... accumulate (old_file, new_files) pairs for commit
    # Commit atomically via existing overwrite snapshot
```

### Track 2 (Long-term — Rust-side)

```python
from pyiceberg_core.execution import execute_cow_rewrite

result = execute_cow_rewrite(
    metadata_location=self._table.metadata_location,
    file_io_properties=self._table.io.properties,
    files_to_rewrite=[serialize(f) for f in files],
    filter_expression=serialize(delete_filter),
    keep_matching=False,
    memory_limit="512MB",
)
# result.new_files contains serialized DataFile JSON
```

### PyArrow Fallback (Existing Code — Unchanged)

The current code continues to work exactly as-is. It loads the file into memory, applies the filter, writes the result. For files < available RAM, this is fine. For files > RAM, Python raises `MemoryError`.

### OOM Safety Guarantee

| Scenario | What Happens | Table Corruption? |
|----------|-------------|-------------------|
| OOM during `ArrowScan.to_table()` | `MemoryError` raised | **No** — no files written, no snapshot committed |
| OOM during `filtered_df` write | Partial Parquet file on disk | **No** — file is orphaned (never referenced by manifest) |
| OOM during DataFusion execution | DataFusion spills to disk (no OOM) | **N/A** — DataFusion prevents OOM |
| Disk full during DataFusion spill | `DataFusionError` → Python exception | **No** — no snapshot committed |

**Why safe**: `Transaction.delete()` only commits at the very end via `update_snapshot().overwrite()`. If ANY exception occurs before that point, the snapshot is never created. Written data files become orphans (cleaned up by `delete_orphan_files()`).

### Acceptance Criteria

- [ ] `table.delete("col = 'value'")` works on 1GB+ Parquet files with 256MB memory budget
- [ ] Existing PyArrow path still works when `pyiceberg-core` not installed
- [ ] No behavior change for tables that fit in memory
- [ ] Integration test: delete from large file → verify rows removed → verify no orphans after commit

---

## Issue 4: `[Fix] Streaming CoW overwrite via DataFusion`

### Summary

Same pattern as Issue 3, applied to `Transaction.overwrite()` when `overwrite_filter` requires file rewriting. Identical architecture, different call site.

### Tracks

Identical to Issue 3. The filter applied is `overwrite_filter` (keep rows NOT matching), then new data is appended.

### OOM Safety Guarantee

Same as Issue 3. The overwrite commit only happens after all files are successfully written. OOM at any point before commit → no snapshot created → no corruption.

### Acceptance Criteria

- [ ] `table.overwrite(df, overwrite_filter="year = 2020")` works on large tables
- [ ] Atomic: either all files replaced or none

---

## Issue 5: `[Feature] Equality delete read support`

### Summary

Remove the `ValueError` in `_plan_files_local()` and implement equality delete resolution. This is THE critical feature — it unblocks reading any V2 table with equality deletes (all Flink-written tables).

### Current Code (Hard Error)

```python
elif data_file.content == DataFileContent.EQUALITY_DELETES:
    raise ValueError("PyIceberg does not yet support equality deletes: ...")
```

### Track 1 (Immediate — Python-side DataFusion)

```python
# In ArrowScan, after reading data file batches:
def _resolve_equality_deletes(self, data_table, eq_delete_refs):
    ctx = SessionContext()  # with memory limit
    ctx.register_record_batches("data", [data_table.to_batches()])
    
    # Register all applicable equality delete files
    for i, ref in enumerate(eq_delete_refs):
        ctx.register_parquet(f"deletes_{i}", ref.delete_file.file_path)
    
    # Build UNION ALL of delete files projected to equality columns
    eq_cols = [schema.find_field(fid).name for fid in ref.equality_field_ids]
    union_sql = " UNION ALL ".join(
        f"SELECT {', '.join(eq_cols)} FROM deletes_{i}" 
        for i in range(len(eq_delete_refs))
    )
    
    # Anti-join: keep rows NOT in delete set
    join_cond = " AND ".join(f"d.{c} = e.{c}" for c in eq_cols)
    sql = f"SELECT d.* FROM data d LEFT ANTI JOIN ({union_sql}) e ON {join_cond}"
    
    return ctx.sql(sql).to_arrow_table()
```

### Track 2 (Long-term — Rust-side)

```python
from pyiceberg_core.execution import execute_equality_resolution

batches = execute_equality_resolution(
    data_file_paths=[task.file.file_path],
    eq_delete_file_paths=[ref.delete_file.file_path for ref in eq_refs],
    equality_field_names=eq_col_names,
    file_io_properties=self._table.io.properties,
    memory_limit="512MB",
)
# Returns list[RecordBatch] with deleted rows removed
```

### PyArrow Fallback (Small Delete Sets)

```python
def _resolve_equality_deletes_pyarrow(self, data_table, eq_delete_refs):
    """Works for small equality delete sets that fit in memory."""
    import pyarrow.compute as pc
    
    # Load all equality delete keys into memory
    all_delete_keys = []
    for ref in eq_delete_refs:
        delete_table = pq.read_table(ref.delete_file.file_path, columns=eq_col_names)
        all_delete_keys.append(delete_table)
    
    delete_keys = pa.concat_tables(all_delete_keys)  # ← OOMs if delete set too large
    
    # For single-column equality deletes: use pc.is_in
    if len(eq_col_names) == 1:
        col = eq_col_names[0]
        mask = pc.invert(pc.is_in(data_table[col], delete_keys[col]))
        return data_table.filter(mask)
    
    # For multi-column: build composite key and use is_in
    # ... (correct but O(|delete_keys|) memory)
```

### OOM Safety Guarantee

| Scenario | What Happens | Table Corruption? |
|----------|-------------|-------------------|
| OOM loading delete keys in PyArrow | `MemoryError` raised | **No** — this is a READ operation, no state modified |
| OOM during DataFusion anti-join | DataFusion spills (no OOM) | **N/A** |
| OOM during `concat_tables` | `MemoryError` raised | **No** — read-only operation |

**Why safe**: `scan().to_arrow()` is a **pure read**. It modifies nothing. An OOM simply means the user cannot read the data without more memory (or DataFusion). The table is completely untouched.

### Acceptance Criteria

- [ ] `table.scan().to_arrow()` returns correct results on tables with equality deletes
- [ ] Sequence number gating: deletes only apply to data committed before the delete
- [ ] PyArrow fallback works for tables with < 10K equality delete rows
- [ ] DataFusion path works for tables with 100M+ equality delete rows (512MB budget)
- [ ] Tables written by Flink are readable
- [ ] No `ValueError` thrown

---

## Issue 6: `[Feature] Table compaction (sort + rewrite)`

### Summary

Add `table.compact()` method that rewrites small/unsorted data files into optimally-sized, sorted output files.

### Track 1 (Immediate — Python-side DataFusion)

```python
def compact(self, *, target_file_size_bytes=256*1024*1024, memory_limit="512MB", 
            sort_order=None, file_filter=ALWAYS_TRUE):
    ctx = SessionContext()  # with memory limit configured
    
    # 1. Select files to compact
    files_to_compact = self._select_files_for_compaction(file_filter)
    
    # 2. Register all source files
    for i, f in enumerate(files_to_compact):
        ctx.register_parquet(f"file_{i}", f.file_path)
    
    # 3. UNION ALL + ORDER BY
    union = " UNION ALL ".join(f"SELECT * FROM file_{i}" for i in range(len(files_to_compact)))
    order_clause = f"ORDER BY {', '.join(sort_order)}" if sort_order else ""
    result = ctx.sql(f"SELECT * FROM ({union}) {order_clause}")
    
    # 4. Write result in target-size chunks
    new_files = self._write_batches_to_files(result.to_arrow_batches(), target_file_size_bytes)
    
    # 5. Atomic commit: replace old files with new
    with self.transaction() as tx:
        with tx.update_snapshot().overwrite() as snap:
            for old in files_to_compact:
                snap.delete_data_file(old)
            for new in new_files:
                snap.append_data_file(new)
```

### Track 2 (Long-term — Rust-side)

```python
from pyiceberg_core.execution import execute_compaction

result = execute_compaction(
    metadata_location=self.metadata_location,
    file_io_properties=self.io.properties,
    files_to_compact=[serialize(f) for f in files],
    target_file_size_bytes=target_file_size_bytes,
    sort_columns=sort_order,
    memory_limit=memory_limit,
)
# Commit using result.new_files
```

### PyArrow Fallback (Small Tables)

```python
def _compact_pyarrow(self, files_to_compact, sort_order, target_file_size_bytes):
    """In-memory compaction — works for tables that fit in RAM."""
    # Read all files into one table
    all_data = pa.concat_tables([
        pq.read_table(f.file_path) for f in files_to_compact
    ])  # ← OOMs if total data > RAM
    
    # Sort in memory
    if sort_order:
        all_data = all_data.sort_by(sort_order)
    
    # Write in target-size chunks
    new_files = self._write_batches_to_files(all_data.to_batches(), target_file_size_bytes)
    # ... commit
```

### OOM Safety Guarantee

| Scenario | What Happens | Table Corruption? |
|----------|-------------|-------------------|
| OOM during PyArrow `concat_tables` | `MemoryError` | **No** — no new files committed yet |
| OOM during PyArrow sort | `MemoryError` | **No** — no new files committed yet |
| OOM writing output files | Partial files on disk (orphans) | **No** — orphans never referenced |
| Commit fails (conflict) | `CommitFailedException` | **No** — OCC retry or abort; new files are orphans |

**Why safe**: Compaction is a **replace** operation. Old files remain valid until commit succeeds. New files are only referenced after atomic commit. If anything fails before commit, old files are still the source of truth and new files are orphans.

### Acceptance Criteria

- [ ] `table.compact()` produces correctly sorted, target-sized output files
- [ ] Works on 100GB+ with 512MB memory budget (DataFusion path)
- [ ] PyArrow fallback works for tables < 1GB
- [ ] Atomic: either all files replaced or none
- [ ] File statistics (min/max/count) correct in output manifests

---

## Issue 7: `[Feature] Orphan file deletion via anti-join`

### Summary

Add `table.maintenance.delete_orphan_files()` that finds and removes files in storage not referenced by any snapshot.

### Track 1 (Immediate — Python-side DataFusion)

```python
def delete_orphan_files(self, *, older_than=None, dry_run=False, memory_limit="512MB"):
    ctx = SessionContext()
    
    # 1. List all files in storage
    storage_paths = list(self.io.list_prefix(self.location()))
    
    # 2. Collect all valid paths from all snapshots
    valid_paths = set()
    for snapshot in self.snapshots():
        for manifest in snapshot.manifests(self.io):
            valid_paths.add(manifest.manifest_path)
            for entry in manifest.fetch_manifest_entry(self.io):
                valid_paths.add(entry.data_file.file_path)
    
    # 3. Register both as DataFusion tables and anti-join
    ctx.register_record_batches("storage", [[pa.record_batch({"path": storage_paths})]])
    ctx.register_record_batches("valid", [[pa.record_batch({"path": list(valid_paths)})]])
    
    orphans = ctx.sql("""
        SELECT s.path FROM storage s 
        LEFT ANTI JOIN valid v ON s.path = v.path
    """).to_arrow_table()["path"].to_pylist()
    
    # 4. Filter by age and delete
    if not dry_run:
        for path in orphans:
            self.io.delete(path)
    return orphans
```

### Track 2 (Long-term — Rust-side)

```python
from pyiceberg_core.execution import execute_antijoin_paths

orphans = execute_antijoin_paths(
    storage_paths=storage_paths,
    valid_paths=list(valid_paths),
    memory_limit=memory_limit,
)
```

### PyArrow Fallback

```python
def _orphan_files_pyarrow(self, storage_paths, valid_paths):
    """Set difference in memory — OOMs on tables with millions of files."""
    valid_set = set(valid_paths)  # ← OOMs if millions of paths
    return [p for p in storage_paths if p not in valid_set]
```

### OOM Safety Guarantee

| Scenario | What Happens | Table Corruption? |
|----------|-------------|-------------------|
| OOM building `valid_set` | `MemoryError` | **No** — no files deleted yet |
| OOM during DataFusion join | Spills to disk (no OOM) | **N/A** |
| Crash during file deletion | Some orphans deleted, some remain | **No** — orphans by definition are unreferenced; deleting some is safe, remainder cleaned on retry |

**Why safe**: Orphan file deletion only removes files that are NOT referenced by ANY snapshot. Even if the process crashes mid-deletion, the remaining orphans are still orphans. The table's metadata is never modified by this operation.

### Acceptance Criteria

- [ ] Correctly identifies orphan files (no false positives — never deletes valid files)
- [ ] Handles tables with 10M+ files (DataFusion path)
- [ ] `dry_run=True` returns list without deleting
- [ ] `older_than` filter prevents deleting recently-created files (race condition protection)

---

## Issue 8: `[Fix] Upsert via DataFusion hash join`

### Summary

Replace the O(n²) row-by-row Python comparison in `upsert_util.get_rows_to_update()` with a DataFusion hash join.

### Current Code (O(n²) and OOMs)

```python
# upsert_util.py — row-by-row comparison
for source_idx, target_idx in zip(source_indices, target_indices):
    source_row = source_table.slice(source_idx, 1)
    target_row = target_table.slice(target_idx, 1)
    for key in non_key_cols:
        if source_row[key][0].as_py() != target_row[key][0].as_py():
            to_update_indices.append(source_idx)
```

### Track 1 (Immediate — Python-side DataFusion)

```python
def get_rows_to_update_datafusion(source, target, join_cols, non_key_cols):
    ctx = SessionContext()
    ctx.register_record_batches("source", [source.to_batches()])
    ctx.register_record_batches("target", [target.to_batches()])
    
    # Inner join on key cols, filter where any non-key col differs
    join_cond = " AND ".join(f"s.{c} = t.{c}" for c in join_cols)
    diff_cond = " OR ".join(
        f"(s.{c} IS DISTINCT FROM t.{c})" for c in non_key_cols
    )
    
    sql = f"""
        SELECT s.* FROM source s
        INNER JOIN target t ON {join_cond}
        WHERE {diff_cond}
    """
    return ctx.sql(sql).to_arrow_table()
```

### Track 2 (Long-term — Rust-side)

Would be part of a general `execute_hash_join` function in `pyiceberg_core.execution`.

### PyArrow Fallback (Existing Code — Unchanged)

The current row-by-row implementation continues to work. It's O(n²) and accumulates results in memory, but for small upserts (< 100K rows) it completes in seconds.

### OOM Safety Guarantee

| Scenario | What Happens | Table Corruption? |
|----------|-------------|-------------------|
| OOM during join (PyArrow path) | `MemoryError` in `concat_tables` | **No** — no overwrite/append committed |
| OOM during DataFusion join | Spills to disk (no OOM) | **N/A** |
| OOM during write of update results | Partial files (orphans) | **No** — transaction not committed |

**Why safe**: `Transaction.upsert()` commits at the very end via `self.overwrite()` and `self.append()`. If the join phase OOMs, neither `overwrite` nor `append` is called, so no snapshot changes.

### Acceptance Criteria

- [ ] `table.upsert(df)` works for 1M+ row source × 10M+ row target
- [ ] Results identical to current implementation (same rows updated/inserted)
- [ ] O(n+m) time complexity (not O(n²))
- [ ] Existing PyArrow path unchanged for small inputs

---

## Issue 9: `[Feature] Equality-to-positional conversion`

### Summary

Convert accumulated equality delete files into positional deletes or deletion vectors, eliminating per-read anti-join cost.

### Track 1 (Immediate — Python-side DataFusion)

```python
def rewrite_equality_deletes(self, *, memory_limit="512MB"):
    ctx = SessionContext()
    
    # For each data file with applicable equality deletes:
    for task in scan_tasks_with_eq_deletes:
        # Register data file WITH row index
        ctx.register_parquet("data", task.file.file_path)
        
        # Register equality delete files
        for i, ref in enumerate(task.eq_delete_refs):
            ctx.register_parquet(f"del_{i}", ref.delete_file.file_path)
        
        # Inner join to find matching positions
        union_del = " UNION ALL ".join(...)
        sql = f"""
            SELECT d._file_path, d._row_index
            FROM data_with_row_index d
            INNER JOIN ({union_del}) e ON {join_cond}
        """
        positions = ctx.sql(sql).to_arrow_table()
        
        # Write as positional delete file (or DV)
        pos_delete_file = write_positional_deletes(positions)
    
    # Commit: remove eq delete files, add pos delete files
```

### Track 2 (Long-term — Rust-side)

Part of `execute_compaction` with delete resolution enabled.

### PyArrow Fallback

```python
def _eq_to_pos_pyarrow(self, data_file, eq_refs):
    """Load data + deletes into memory, find matching row indices."""
    data = pq.read_table(data_file.file_path)  # ← OOMs on large files
    delete_keys = load_all_eq_deletes(eq_refs)  # ← OOMs on large delete sets
    
    # Find matching indices
    mask = compute_match_mask(data, delete_keys, eq_cols)
    positions = pa.array(range(len(data))).filter(mask)
    
    return positions  # row indices to delete
```

### OOM Safety Guarantee

| Scenario | What Happens | Table Corruption? |
|----------|-------------|-------------------|
| OOM loading data/deletes | `MemoryError` | **No** — write path not reached, no commit |
| OOM during DataFusion join | Spills (no OOM) | **N/A** |
| Commit conflict | `CommitFailedException` | **No** — OCC retry; written files are orphans |

**Why safe**: This is a **replace** operation (remove equality delete files, add positional delete files). The table remains readable in its original state until the atomic commit succeeds. If anything fails, the old equality deletes remain valid.

### Acceptance Criteria

- [ ] After conversion, equality delete files are replaced by positional deletes
- [ ] Subsequent scans no longer require anti-join (only position filtering)
- [ ] Correctness: same rows are deleted before and after conversion
- [ ] Works for tables with GB-scale equality delete files

---

## Issue 10: `[Feature] Position delete compaction`

### Summary

Rewrite data files that have accumulated position deletes, producing clean files without delete markers.

### Track 1 (Immediate — Python-side DataFusion)

```python
def rewrite_position_deletes(self, *, memory_limit="512MB"):
    # For each data file with position deletes:
    for task in tasks_with_pos_deletes:
        ctx = SessionContext()
        ctx.register_parquet("data", task.file.file_path)
        
        # Load position delete indices
        positions_to_delete = load_position_deletes(task.delete_files)
        
        # Filter: keep rows NOT at deleted positions
        # Option A: DataFusion with row_number() + anti-join
        # Option B: PyArrow take() with inverted index (often fits in memory)
        
        filtered = apply_position_filter(data, positions_to_delete)
        new_file = write_data_file(filtered)
    
    # Commit: remove old data files + pos delete files, add new clean files
```

### PyArrow Fallback

Position deletes are typically small (just row indices). The PyArrow path works well here:

```python
def _rewrite_pos_deletes_pyarrow(self, data_file, delete_files):
    data = pq.read_table(data_file.file_path)  # ← OOMs only if file > RAM
    positions = load_positions(delete_files)     # Typically tiny (list of ints)
    
    # Create boolean mask
    mask = pa.array([True] * len(data))
    for pos in positions:
        mask[pos] = False
    
    return data.filter(mask)
```

### OOM Safety Guarantee

Same as Issue 3 (CoW rewrite). Old files remain valid until commit. OOM → no commit → no corruption.

### Acceptance Criteria

- [ ] After compaction, data files have no associated delete files
- [ ] Scan results identical before and after compaction
- [ ] Works with DataFusion for files > 1GB

---

## Issue 11: `[Feature] Full MoR compaction (join + sort + rewrite)`

### Summary

The composition of equality delete resolution + positional delete application + sort + rewrite. The "end boss" operation.

### Track 1 (Immediate — Python-side DataFusion)

```python
def compact_with_deletes(self, *, memory_limit="1GB", sort_order=None):
    ctx = SessionContext()  # with memory limit
    
    # Register all data files
    for i, f in enumerate(data_files):
        ctx.register_parquet(f"data_{i}", f.file_path)
    
    # Register all equality delete files
    for i, f in enumerate(eq_delete_files):
        ctx.register_parquet(f"eq_del_{i}", f.file_path)
    
    # Build plan: scan → filter pos deletes → anti-join eq deletes → sort → output
    data_union = " UNION ALL ".join(f"SELECT * FROM data_{i}" for i in ...)
    eq_union = " UNION ALL ".join(f"SELECT {eq_cols} FROM eq_del_{i}" for i in ...)
    
    sql = f"""
        SELECT d.* FROM ({data_union}) d
        LEFT ANTI JOIN ({eq_union}) e ON {join_cond}
        ORDER BY {sort_order}
    """
    # Note: position deletes handled as a WHERE NOT IN clause or pre-filter
    
    result_batches = ctx.sql(sql).to_arrow_batches()
    new_files = write_in_target_size_chunks(result_batches)
    
    # Atomic commit: replace all old files (data + all delete files) with new clean files
```

### Track 2 (Long-term — Rust-side)

This would compose `IcebergTableScan` → `FilterExec` (pos deletes) → `AntiHashJoinExec` (eq deletes) → `SortExec` → `IcebergWriteExec` entirely in Rust.

### PyArrow Fallback

Only feasible for very small tables. For any production table, this will OOM:

```python
def _compact_with_deletes_pyarrow(self, ...):
    # Load ALL data and ALL deletes into memory
    all_data = pa.concat_tables([pq.read_table(f.file_path) for f in data_files])
    all_eq_deletes = pa.concat_tables([pq.read_table(f.file_path) for f in eq_delete_files])
    
    # Apply anti-join in memory
    filtered = anti_join_pyarrow(all_data, all_eq_deletes, eq_cols)
    
    # Sort in memory
    sorted_data = filtered.sort_by(sort_order)
    
    # Write
    new_files = write_in_chunks(sorted_data)
```

### OOM Safety Guarantee

| Scenario | What Happens | Table Corruption? |
|----------|-------------|-------------------|
| OOM at any point before commit | `MemoryError` | **No** — old files remain the source of truth |
| DataFusion disk full during spill | `DataFusionError` → exception | **No** — no commit |
| Commit conflict (concurrent write) | `CommitFailedException` | **No** — retry or abort; new files are orphans |
| Process killed (SIGKILL) mid-write | Orphan data files on disk | **No** — never referenced; cleaned by orphan deletion |

**Why safe**: This is a **full table rewrite**. The old table state (data files + delete files) remains valid and readable by all concurrent readers until the moment the new snapshot is atomically committed. This is the fundamental Iceberg guarantee — snapshot isolation via OCC.

### Acceptance Criteria

- [ ] After full compaction, table has zero delete files
- [ ] Output files are sorted by specified sort order
- [ ] Output file sizes ≈ target_file_size_bytes
- [ ] Results identical to reading the un-compacted table
- [ ] Works on 100GB+ tables with 1GB memory budget

---

## Issue 12: `[Feature] Z-Order / Hilbert sorting`

### Summary

Add spatial clustering via Z-order (or Hilbert curve) interleaved-bit sort key during compaction.

### Track 1 (Immediate — Python-side DataFusion)

Requires computing a Z-order key as a UDF, then sorting:

```python
def compact_z_order(self, columns, *, memory_limit="1GB"):
    ctx = SessionContext()
    
    # Register UDF for z-order key computation
    @ctx.udf(return_type=pa.binary(), input_types=[...])
    def z_order_key(*cols):
        return compute_z_order_interleave(cols)
    
    # Register data files, compute key, sort, write
    sql = f"""
        SELECT * FROM data
        ORDER BY z_order_key({', '.join(columns)})
    """
    # ... same commit pattern as compaction
```

### PyArrow Fallback

Compute Z-order key via NumPy bit interleaving, then sort in memory:

```python
def _z_order_pyarrow(self, data, columns):
    keys = compute_z_order_keys_numpy(data, columns)  # O(N) memory for keys
    indices = pa.compute.sort_indices(keys)            # O(N) memory for sort
    return data.take(indices)                          # OOMs if N×R > RAM
```

### OOM Safety

Same as compaction (Issue 6). Pure rewrite operation; old files untouched until commit.

### Acceptance Criteria

- [ ] Output files achieve spatial clustering (measured by overlap reduction)
- [ ] Min/max column statistics in output manifests show tighter bounds per file
- [ ] Works for multi-column Z-order (2–6 dimensions typical)

---

## Issue 13: `[Feature] DV compaction`

### Summary

Merge accumulated Deletion Vectors (V3 Roaring bitmaps) and optionally rewrite data files with high deletion ratios.

### Track 1 (Immediate)

DV merge is cheap (Roaring OR operation). Only the data rewrite needs DataFusion:

```python
def compact_dvs(self, *, deletion_threshold=0.2):
    for data_file, dvs in files_with_dvs:
        merged_bitmap = merge_roaring_bitmaps(dvs)
        deletion_ratio = popcount(merged_bitmap) / data_file.record_count
        
        if deletion_ratio > deletion_threshold:
            # Rewrite: same as streaming CoW (Issue 3)
            new_file = rewrite_without_deleted_rows(data_file, merged_bitmap)
            # commit: replace data_file + dvs → new_file
        else:
            # Just merge DVs (metadata-only, no data rewrite)
            new_dv = write_merged_dv(merged_bitmap)
            # commit: replace old dvs → new_dv
```

### PyArrow Fallback

Bitmap merging is trivial (Roaring library handles it). Data rewrite falls back to the same pattern as Issue 3 (load file → filter → write).

### OOM Safety

Same as Issue 3. Bitmap operations are O(bitmap_size) which is typically < 1MB. Data rewrite follows the CoW safety model.

### Acceptance Criteria

- [ ] Merged DVs reduce the number of delete files per data file
- [ ] Data files with high deletion ratio are rewritten clean
- [ ] Scan results identical before and after

---

## Issue 14: `[Feature] Incremental compaction`

### Summary

Optimization of full compaction: merge already-sorted runs (data files known to be sorted by the table's sort order) using a k-way sort-preserving merge instead of full re-sort.

### Track 1 (Immediate — Python-side DataFusion)

DataFusion's `SortPreservingMergeExec` handles this natively:

```python
def compact_incremental(self, *, files_to_merge, memory_limit="512MB"):
    ctx = SessionContext()
    
    # Register each sorted file as a separate partition
    for i, f in enumerate(files_to_merge):
        ctx.register_parquet(f"part_{i}", f.file_path)
    
    # DataFusion's optimizer recognizes pre-sorted inputs and uses merge sort
    union = " UNION ALL ".join(f"SELECT * FROM part_{i}" for i in range(len(files_to_merge)))
    sql = f"SELECT * FROM ({union}) ORDER BY {sort_order}"
    
    # DataFusion uses SortPreservingMergeExec (O(N log k) not O(N log N))
    # ... write + commit
```

### PyArrow Fallback

PyArrow has no merge-sort primitive. Falls back to full sort:

```python
all_data = pa.concat_tables([pq.read_table(f) for f in files])
sorted_data = all_data.sort_by(sort_keys)  # Full sort, not merge
```

### OOM Safety

Same as compaction (Issue 6).

### Acceptance Criteria

- [ ] Leverages pre-existing sort order (fewer I/O passes than full sort)
- [ ] Output equivalent to full compaction
- [ ] Faster than full compaction for already-sorted inputs

---

## Summary: Safety Model

Every operation in this epic follows the same safety contract:

```
BEFORE operation: Table is in valid state S₀ (some snapshot)
DURING operation: New data files written to storage (orphans until commit)
ON SUCCESS: Atomic commit creates snapshot S₁ referencing new files
ON FAILURE (OOM/error/crash): No commit → table remains at S₀
                               Written files are orphans → cleaned by maintenance
```

This is Iceberg's fundamental guarantee. DataFusion's spill-to-disk prevents the failure case (OOM) from occurring, but even when it does occur (PyArrow path), the table is never corrupted. The user simply gets an error and can retry with more memory or with DataFusion installed.
