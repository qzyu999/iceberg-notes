# Pluggable Backend v12: Current Status and Remaining Steps

Branch: `pluggable-backend-discovery` (commit `c82ea540`)
Base: `main` @ `9d36e236`

---

## 1. Current State

```
16 files changed, 4,537 insertions(+), 0 existing files modified
79 passed, 1 skipped
```

### 1.1 What Exists on the Branch

| File | Lines | Purpose | Status |
|------|:---:|---|:---:|
| `protocol.py` | ~480 | ReadBackend, WriteBackend, ComputeBackend, PlanningBackend, Backends.resolve() | Complete |
| `_orchestrate.py` | ~200 | orchestrate_scan(), write_data_files(), helper functions | Complete |
| `planning.py` | ~250 | InMemoryPlanner + BoundedMemoryPlanner (DataFusion SQL join) | Complete |
| `engine.py` | ~190 | resolve_engine() with 3-axis resolution (read/write/compute) | Complete |
| `expression_to_sql.py` | ~215 | BooleanExpression → SQL WHERE with IS NOT DISTINCT FROM | Complete |
| `object_store.py` | ~224 | Credential bridging (S3/GCS/ADLS → backend config) | Complete |
| `materialize.py` | ~116 | materialize_to_parquet, materialize_batches_to_parquet | Complete |
| `metadata.py` | ~189 | Streaming generators for metadata enumeration | Complete |
| `backends/pyarrow_backend.py` | ~500 | PyArrowReadBackend + PyArrowWriteBackend + PyArrowComputeBackend | Complete |
| `backends/datafusion_backend.py` | ~420 | DataFusionReadBackend + DataFusionComputeBackend | Complete |
| `backends/duckdb_backend.py` | ~430 | DuckDBReadBackend + DuckDBComputeBackend | Complete |
| `backends/polars_backend.py` | ~320 | PolarsReadBackend + PolarsComputeBackend | Complete |
| `tests/test_backend_equivalence.py` | ~800 | 79 test cases across all backends | Complete |

### 1.2 What Does NOT Exist Yet

The branch does not modify `pyiceberg/table/__init__.py` or `pyiceberg/io/pyarrow.py`.
All code is purely additive. The wiring (replacing ArrowScan calls) is the remaining step.

---

## 2. Remaining Steps (From v11 §14)

| Step | Description | Status | Notes |
|:---:|---|:---:|---|
| 1 | Add `Backends.resolve()` to `protocol.py` | **Done** | Instantiates backends from engine resolution |
| 2 | Create `_orchestrate.py` | **Done** | orchestrate_scan + write_data_files + helpers |
| 3 | Replace `_to_arrow_via_file_scan_tasks` body | **Not done** | Requires modifying table/__init__.py |
| 4 | Replace `_to_arrow_batch_reader_via_file_scan_tasks` body | **Not done** | Same file |
| 5 | Replace `Transaction.delete` execution portion | **Not done** | Same file |
| 6 | Replace `Transaction.append` body | **Not done** | Same file |
| 7 | Update `Transaction.overwrite` and `dynamic_partition_overwrite` | **Not done** | Same file |
| 8 | Add deprecation warning to `ArrowScan.__init__` | **Not done** | Requires modifying io/pyarrow.py |
| 9 | Add proactive OOM warning + try/except (v11 §16) | **Not done** | Part of step 3 |
| 10 | Run `make test` (full suite) | **Not done** | Validates no regressions |
| 11 | Fix any failures | — | — |
| 12 | Add new integration tests | **Not done** | Tests for dispatch routing |
| 13 | Refactor upsert to use join_from_files (v11 §15.4) | **Not done** | Separate from steps 3-7 |
| 14 | Squash into one commit | — | — |

---

## 3. What Each Remaining Step Entails

### Step 3: Replace `_to_arrow_via_file_scan_tasks` (v11 §2 + §16)

Replace the 8-line ArrowScan call with:
- `Backends.resolve(scan.io.properties)`
- `_warn_if_large_result(tasks, schema, metadata)` (proactive OOM warning)
- `orchestrate_scan(backends, tasks, metadata, schema, filter)`
- `try: pa.Table.from_batches(...) except (MemoryError, ArrowMemoryError): raise with helpful message`

### Step 4: Replace `_to_arrow_batch_reader_via_file_scan_tasks`

Same as step 3 but without the `from_batches` materialization.
Returns `pa.RecordBatchReader.from_batches(schema, generator)` directly. Fully streaming.

### Step 5: Replace `Transaction.delete` execution portion (v11 §4)

Replace the per-file `ArrowScan.to_table` + `df.filter` + `_dataframe_to_data_files` with:
- `backends.read.read_parquet(path, schema, AlwaysTrue, props)` (streaming read)
- `backends.compute.filter(batches, complement_filter)` (streaming filter)
- `backends.write.write_partitioned(kept, location, schema, size, props, props)` (streaming write)
- Count rows via a `counting_iterator` wrapper (no list() materialization)

### Step 6: Replace `Transaction.append` body (v11 §5 + §15.2)

Replace `_dataframe_to_data_files(df)` with `write_data_files(backends, df, metadata, location, schema, props)`.
The `write_data_files` function in `_orchestrate.py` handles:
- Sort-on-write (if sort order + bounded-memory backend)
- Streaming RecordBatchReader writes
- pa.Table writes

### Step 7: Update overwrite/dynamic_partition_overwrite

Same as step 6 (share `write_data_files`). Commit semantics (overwrite vs append)
stay unchanged in the Transaction commit logic.

### Step 8: Deprecate ArrowScan

Add `warnings.warn(DeprecationWarning)` to `ArrowScan.__init__()` in `io/pyarrow.py`.

### Step 9: OOM safety (v11 §16)

Part of step 3. The proactive warning + try/except is embedded in
`_to_arrow_via_file_scan_tasks`.

### Step 13: Upsert refactoring (v11 §15.4)

Replace the per-batch loop + `concat_tables` + `rows_to_insert.filter(~expr)` with:
- `materialize_to_parquet(source_df)` → temp file
- `backends.compute.join_from_files([tmp], target_paths, cols, "inner")` → updates
- `backends.compute.join_from_files([tmp], target_paths, cols, "anti")` → inserts
- Stream results through `backends.write.write_partitioned`

This eliminates OOM points #2-5 from the current upsert (v11 §15.4).

---

## 4. Concerns Addressed (From v11 §15)

| Concern | Resolution | Implemented? |
|---------|-----------|:---:|
| Memory safety: no `list(batches)` in pipeline | `orchestrate_scan` is fully generator-based | Yes (in _orchestrate.py) |
| Naming: `execute_append` serving dual purpose | Renamed to `write_data_files` (writes only, caller handles commit) | Yes |
| MoR capability | Read side complete (anti_join, apply_positional_deletes). Write side needs `row_delta` commit. | Read: Yes. Write: Not yet. |
| Upsert OOM fix | Plan documented (join_from_files replaces per-batch loop) | Not yet (step 13) |
| Proactive OOM warning | `_warn_if_large_result` specified in v11 §16.3 | Not yet (step 9) |
| Try/except around materialization | Catches ArrowMemoryError on macOS/Windows/Linux-no-overcommit | Not yet (step 9) |
| IS NOT DISTINCT FROM for NULL equality | All SQL backends use IS NOT DISTINCT FROM in join conditions | Yes |
| PyArrow null_equals_null for anti-join | `_anti_join_tables(null_equals_null=True)` for equality deletes | Yes |

---

## 5. What the PR Delivers Once Steps 3-14 Are Complete

| Operation | Current behavior | After PR |
|-----------|-----------------|----------|
| `table.scan().to_arrow()` (no deletes) | Works via ArrowScan | Works via backends.read (same output, streaming pipeline) |
| `table.scan().to_arrow()` (positional deletes) | Works via ArrowScan (loads all deletes upfront) | Works via backends.compute.apply_positional_deletes (streaming) |
| `table.scan().to_arrow()` (equality deletes) | `ValueError` | Works via backends.compute.anti_join_from_files with spill |
| `table.delete(filter)` (CoW) | OOMs: loads full file per task | Streaming: read + filter + write per batch |
| `table.append(df)` | Works via _dataframe_to_data_files | Works via write_data_files (same output, optional sort-on-write) |
| `table.append(RecordBatchReader)` | Works (streaming, merged in #3335) | Works via write_data_files (same streaming path) |
| `table.overwrite(df)` | Works | Works via write_data_files |
| `table.upsert(df)` | OOMs: concat_tables + per-batch matching | Bounded: join_from_files + streaming write |
| Large result warning | None (silent OOM kill) | ResourceWarning before materialization |
| OOM error message | Process dies, no message | Catches ArrowMemoryError with alternatives |

---

## 6. Verification Plan

```bash
# After steps 3-14:

# Unit tests (execution module only)
uv run python -m pytest tests/execution/ -q
# Expected: 85+ passed (original 79 + ~6 new integration tests)

# Full test suite (validates no regressions)
make test
# Expected: all existing tests pass unchanged

# Lint
make lint
# Expected: passes

# Specific validation:
uv run python -c "
from pyiceberg.execution.protocol import Backends
b = Backends.resolve({})
print(f'Read: {type(b.read).__name__}')
print(f'Write: {type(b.write).__name__}')
print(f'Compute: {type(b.compute).__name__}')
print(f'Planning: {type(b.planning).__name__}')
print(f'Bounded memory: {b.supports_bounded_memory}')
"
# Expected output (without DataFusion):
# Read: PyArrowReadBackend
# Write: PyArrowWriteBackend
# Compute: PyArrowComputeBackend
# Planning: InMemoryPlanner
# Bounded memory: False
```

---

## 7. Next Session Instructions

Start a fresh conversation with:

> "Implement steps 3-14 from `pluggable_backend_discovery_v11.md` on the
> `pluggable-backend-discovery` branch in `iceberg-python`. The foundation
> is complete (16 files, 4,537 lines, `_orchestrate.py` and `Backends.resolve()`
> ready). Modify `table/__init__.py` to wire scan, delete, append, overwrite,
> and upsert through the pluggable backend. Add proactive OOM warning + try/except.
> Deprecate ArrowScan. Run `make test` to validate. Squash into one commit."

Provide files for context:
- `pyiceberg/execution/_orchestrate.py`
- `pyiceberg/execution/protocol.py`
- `pyiceberg/table/__init__.py` (lines 460-800, 2170-2210)
- `pyiceberg/io/pyarrow.py` (lines 1728-1770, ArrowScan class)
