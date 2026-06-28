# Issue: Add `pyiceberg_core.execution` module for bounded-memory operations

## Title

`Python: Add execution module exposing bounded-memory DataFusion operations to PyIceberg`

---

## Is your feature request related to a problem or challenge?

PyIceberg (iceberg-python) needs to perform compute-heavy table operations — compaction, equality delete resolution, copy-on-write file rewrites, orphan file deletion — but has no path to execute these with bounded memory through the existing `pyiceberg-core` bindings.

**Current state of `pyiceberg-core`:**
- `pyiceberg_core.datafusion` — exposes `IcebergStaticTableProvider` as a read-only DataFusion `TableProvider` via PyCapsule FFI
- `pyiceberg_core.transform` — partition transform functions
- `pyiceberg_core.manifest` — manifest reading utilities

**What's missing:**
There is no way to trigger a full bounded-memory execution pipeline (scan → transform → write) from Python. The existing `TableProvider` FFI only supports reads. For write/compute operations, data would need to cross the Python↔Rust FFI boundary per-batch, which defeats bounded-memory guarantees (Python's address space holds the data, preventing Rust-side memory management).

**The consequence for PyIceberg:**
- `table.scan().to_arrow()` on tables with equality deletes raises `ValueError` (completely unreadable)
- `table.compact()` is not implemented (no bounded-memory sort)
- `Transaction.delete()` OOMs on large Parquet files (loads entire file into memory)
- Orphan file deletion OOMs on tables with millions of files

---

## Describe the solution you'd like

Add a new `execution` submodule to the Python bindings (`bindings/python/src/execution.rs`) that exposes operation-level functions via PyO3. Each function:

1. Accepts operation parameters from Python (file paths, filter expressions, memory limit)
2. Creates a bounded-memory DataFusion session internally (using `FairSpillPool`)
3. Constructs and executes a DataFusion plan entirely in Rust (GIL released)
4. Returns only metadata (new file paths, record counts) to Python — not bulk Arrow data

### Design principle: Operation-level FFI

The boundary crosses at the **operation** level, not the record level:

```python
# What we want (operation-level — all compute in Rust):
result = pyiceberg_core.execution.execute_compaction(
    metadata_location="s3://bucket/metadata/v3.metadata.json",
    file_io_properties={...},
    files_to_compact=[...],  # serialized DataFile JSON
    memory_limit="512MB",
)
# Returns: CompactionResult(new_files=[...], total_record_count=N)
# Python only gets back metadata for commit. Data never leaves Rust.
```

This ensures DataFusion's memory pool manages all data in Rust's address space, and Python threads are not blocked during execution (GIL released by Tokio).

### Proposed functions

```python
# pyiceberg_core.execution (type stubs)

def execute_cow_rewrite(
    metadata_location: str,
    file_io_properties: dict[str, str],
    files_to_rewrite: list[str],       # DataFile JSON
    filter_expression: str,            # Iceberg expression
    keep_matching: bool,
    memory_limit: str | None = None,   # e.g., "512MB"
) -> CowRewriteResult: ...

def execute_compaction(
    metadata_location: str,
    file_io_properties: dict[str, str],
    files_to_compact: list[str],       # DataFile JSON
    target_file_size_bytes: int | None = None,
    sort_columns: list[str] | None = None,
    memory_limit: str | None = None,
) -> CompactionResult: ...

def execute_equality_resolution(
    data_file_paths: list[str],
    eq_delete_file_paths: list[str],
    equality_field_names: list[str],
    file_io_properties: dict[str, str],
    memory_limit: str | None = None,
) -> list[RecordBatch]: ...

def execute_antijoin_paths(
    storage_paths: list[str],
    valid_paths: list[str],
    memory_limit: str | None = None,
) -> list[str]: ...
```

### Result types

```python
class CowRewriteResult:
    new_files: list[str]          # DataFile JSON for commit
    total_record_count: int
    total_file_size_bytes: int

class CompactionResult:
    new_files: list[str]          # DataFile JSON for commit
    total_record_count: int
    total_file_size_bytes: int
    input_files_count: int
```

### How PyIceberg uses this

Python handles orchestration (file selection, commit). Rust handles compute (sort, join, write):

```python
# In PyIceberg's table.compact():
from pyiceberg_core.execution import execute_compaction

# 1. Python selects files to compact (manifest-based planning)
files = self._select_files_for_compaction(filter)

# 2. Rust executes the sort + rewrite with bounded memory
result = execute_compaction(
    metadata_location=self.metadata_location,
    file_io_properties=self.io.properties,
    files_to_compact=[serialize(f) for f in files],
    sort_columns=self.sort_order(),
    memory_limit="512MB",
)

# 3. Python commits the replacement atomically
with self.transaction() as tx:
    tx.overwrite(old_files=files, new_files=result.new_files)
```

---

## Implementation approach

### Module structure

```rust
// bindings/python/src/execution.rs

#[pyclass] struct PyCowRewriteResult { ... }
#[pyclass] struct PyCompactionResult { ... }

#[pyfunction] fn execute_cow_rewrite(...) -> PyResult<PyCowRewriteResult> { ... }
#[pyfunction] fn execute_compaction(...) -> PyResult<PyCompactionResult> { ... }
#[pyfunction] fn execute_equality_resolution(...) -> PyResult<PyObject> { ... }
#[pyfunction] fn execute_antijoin_paths(...) -> PyResult<Vec<String>> { ... }

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> { ... }
```

### Registration in `lib.rs`

```rust
mod execution;

#[pymodule]
fn pyiceberg_core_rust(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    datafusion_table_provider::register_module(py, m)?;
    transform::register_module(py, m)?;
    manifest::register_module(py, m)?;
    execution::register_module(py, m)?;  // NEW
    Ok(())
}
```

### Internal execution pattern (for each function)

```rust
fn execute_compaction(...) -> PyResult<PyCompactionResult> {
    let rt = runtime();  // shared Tokio runtime (existing pattern)
    rt.block_on(async {
        // 1. Create bounded session (FairSpillPool + DiskManager)
        let ctx = create_bounded_session(BoundedSessionConfig::new(memory_bytes))?;

        // 2. Load table via StaticTable (no catalog needed — just metadata file)
        let file_io = FileIOBuilder::new(factory).with_props(props).build();
        let table = StaticTable::from_metadata_file(&path, ident, file_io).await?;

        // 3. Build DataFusion plan:
        //    IcebergTableScan(specific files) → SortExec → IcebergWriteExec
        //    (SortExec spills to disk automatically when FairSpillPool is exhausted)

        // 4. Execute plan (all data stays in Rust address space)
        let results = collect(plan, ctx.task_ctx()).await?;

        // 5. Extract DataFile metadata from IcebergWriteExec output
        //    Return to Python (only metadata crosses FFI, not bulk data)
        Ok(PyCompactionResult { new_files, total_record_count, ... })
    })
}
```

---

## Phased delivery

This can be delivered incrementally:

| Phase | Scope | Blocked on |
|:------|:------|:-----------|
| **Phase 1** | Module structure, `register_module`, result types, function signatures with `todo!()` bodies | Nothing |
| **Phase 2** | `execute_antijoin_paths` implementation (simplest — just register arrays, anti-join, collect) | Bounded session helper |
| **Phase 3** | `execute_equality_resolution` implementation (register Parquet files, anti-join, return Arrow batches) | Bounded session helper |
| **Phase 4** | `execute_cow_rewrite` implementation (scan → filter → write via IcebergWriteExec) | Bounded session + OverwriteAction/RewriteFiles for commit |
| **Phase 5** | `execute_compaction` implementation (scan → sort → write via IcebergWriteExec) | Bounded session + OverwriteAction/RewriteFiles for commit |

Phase 1 can ship immediately as a standalone PR to get early API feedback.

---

## Related issues

- [#2269](https://github.com/apache/iceberg-rust/issues/2269) — [EPIC] Implement Missing Write Actions (motivation: enable end-to-end native writes from Python)
- [#1607](https://github.com/apache/iceberg-rust/issues/1607) — Add RewriteFiles support (prerequisite for compaction/CoW commit in Phases 4-5)
- [#2186](https://github.com/apache/iceberg-rust/issues/2186) — CoW and MoR support (broader epic)
- [#1797](https://github.com/apache/iceberg-rust/issues/1797) — Reduce the need for iceberg-rust forks (this feature makes iceberg-rust directly usable from Python for production workloads)

### PyIceberg issues this unblocks

- [iceberg-python#1210](https://github.com/apache/iceberg-python/issues/1210) — Equality delete read support
- [iceberg-python#1092](https://github.com/apache/iceberg-python/issues/1092) — Data compaction
- [iceberg-python#3270](https://github.com/apache/iceberg-python/issues/3270) — Equality delete data correctness
- [iceberg-python#1200](https://github.com/apache/iceberg-python/issues/1200) — Orphan file deletion (OOM risk)

---

## Willingness to contribute

I can contribute this feature independently. I plan to deliver it in phases starting with the module structure (Phase 1).
