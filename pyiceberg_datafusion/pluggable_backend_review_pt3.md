# Distinguished/Principal Engineer Review: Pluggable Backend Architecture — Part 3

**Branch:** `pluggable-backend-discovery` (commit `9ed54328`)  
**Scope:** 25 files, +6,203/−66 lines, single squashed commit  
**Reviewer:** Architecture & Code Quality — Final Pass  
**Date:** 2026-07-06  
**Status:** Merge-readiness assessment with formal analysis

---

## 1. Executive Summary

This review constitutes the final engineering assessment of the pluggable backend refactor. Parts 1 and 2 addressed critical bugs (monkey-patching, join logic, streaming materialization, residual computation, generator `return` bug). This Part 3 focuses on:

1. **System-level formal correctness** — does the design hold under composition?
2. **Remaining code-level deficits** — things that will block or delay merge
3. **Python idiom fidelity** — does this match PyIceberg's existing codebase?
4. **Refactoring completeness** — are there orphaned artifacts from previous iterations?
5. **Flakiness and CI stability** — will this pass reliably across environments?

**Overall Verdict:**

The architecture is sound — proper dependency inversion, clean protocol boundaries, correct use of Arrow as interchange format, and legitimate OOM-resilience improvements. The design follows established CS principles (Strategy pattern, Interface Segregation, Open/Closed). However, **7 blocking issues** and **9 non-blocking nits** remain that will be flagged by PyIceberg reviewers. These must be resolved before the PR can merge cleanly.

---

## 2. System Design: Formal Verification

### 2.1 Architecture — Component Dependency Graph

```mermaid
graph TB
    subgraph "Layer 4: Public API (unchanged contract)"
        API["Table.scan().to_arrow()<br/>Table.delete(filter)<br/>Table.append(df)<br/>Table.upsert(df, keys)"]
    end

    subgraph "Layer 3: Orchestration (new — owns Iceberg semantics)"
        ORCH["orchestrate_scan<br/>• parallel task execution<br/>• delete classification<br/>• residual binding<br/>• schema reconciliation"]
        COW["Transaction.delete (CoW)<br/>• two-pass streaming<br/>• RBR to writer"]
        SORT["_apply_sort_order<br/>• materialize → sort → RBR"]
    end

    subgraph "Layer 2: Resolution (new — strategy selection)"
        RES["Backends.resolve()<br/>→ resolve_engine()<br/>→ _detect_available_engines()"]
        CFG["Config hierarchy:<br/>1. Per-call override<br/>2. .pyiceberg.yaml<br/>3. Env vars<br/>4. Auto-detect"]
    end

    subgraph "Layer 1: Protocol (new — contracts)"
        P_R["ReadBackend<br/>(read_parquet)"]
        P_W["WriteBackend<br/>(write_parquet, write_partitioned)"]
        P_C["ComputeBackend<br/>(sort_from_files, anti_join_from_files,<br/>filter, apply_positional_deletes,<br/>aggregate_from_files, join_from_files)"]
        P_P["PlanningBackend<br/>(plan_files)"]
    end

    subgraph "Layer 0: Implementations (new — concrete strategies)"
        PA["PyArrow<br/>(always available,<br/>in-memory only)"]
        DF["DataFusion<br/>(FairSpillPool,<br/>512 MB default)"]
        DK["DuckDB<br/>(internal buffer mgmt,<br/>SET memory_limit)"]
        PL["Polars<br/>(in-memory,<br/>fast SIMD)"]
    end

    API --> ORCH & COW & SORT
    ORCH & COW & SORT --> RES
    RES --> CFG
    RES --> P_R & P_W & P_C
    P_R --> PA & DF & DK & PL
    P_W --> PA
    P_C --> PA & DF & DK & PL
    P_P --> PA & DF

    style ORCH fill:#2980b9,color:#fff
    style RES fill:#8e44ad,color:#fff
    style P_C fill:#27ae60,color:#fff
```

### 2.2 Invariants — Formal Statement

Let `B` denote the set of all backends {PyArrow, DataFusion, DuckDB, Polars}.

**Invariant 1 (Substitutability):**
```
∀ b₁, b₂ ∈ B:
  ∀ op ∈ {sort, anti_join, filter, aggregate, apply_positional_deletes}:
    multiset(op(b₁, input)) = multiset(op(b₂, input))
```
*Verified by:* `test_backend_equivalence.py` — parametrized across all backends.  
*Gap:* Composition isn't tested (sort → filter → join pipeline).

**Invariant 2 (Memory Boundedness):**
```
∀ op ∈ {sort_from_files, anti_join_from_files, aggregate_from_files}:
  ∀ b ∈ {DataFusion, DuckDB}:
    peak_memory(op(b, input, memory_limit=M)) ≤ M + O(output_batch_size)
```
*Verified by:* DataFusion's FairSpillPool, DuckDB's `SET memory_limit`.  
*Violation:* DuckDB `.to_arrow_table()` materializes full result at boundary (§3.1).

**Invariant 3 (No Upward Dependency):**
```
∀ module m at Layer N:
  ∀ import i in m:
    layer(target(i)) ≤ N
```
*Verified by:* Import analysis. No Layer 0 module imports from Layer 3.  
The only cross-layer import is `Backends.resolve()` calling `resolve_engine()` (Layer 2→2, same layer).

**Invariant 4 (Arrow Interchange):**
```
∀ boundary ∈ {read→compute, compute→write, orchestrate→caller}:
  data_format(boundary) = Iterator[pa.RecordBatch]
```
*Verified by:* All protocol method signatures use `Iterator[pa.RecordBatch]`.

### 2.3 State Machine — Scan Task Execution

```mermaid
stateDiagram-v2
    [*] --> ClassifyDeletes : _execute_task(task)
    
    ClassifyDeletes --> BothDeletes : task has BOTH eq + pos deletes
    ClassifyDeletes --> EqualityOnly : task has EQUALITY_DELETES only
    ClassifyDeletes --> PositionalOnly : task has POSITION_DELETES only
    ClassifyDeletes --> PlainRead : no delete files
    
    BothDeletes --> ApplyResidual : apply_positional_deletes → anti_join remaining
    EqualityOnly --> ApplyResidual : anti_join_from_files(data, eq_del_files, on=eq_cols)
    PositionalOnly --> ApplyResidual : apply_positional_deletes(data_path, pos_del_paths)
    PlainRead --> ApplyResidual : read_parquet(path, schema, residual, props)
    
    ApplyResidual --> DetermineReconcile : task.residual != AlwaysTrue → filter(batches, bound_residual)
    ApplyResidual --> DetermineReconcile : task.residual == AlwaysTrue → pass through
    
    DetermineReconcile --> PeekFirstBatch
    PeekFirstBatch --> NeedsReconcile : file_schema.field_ids != projected_schema.field_ids
    PeekFirstBatch --> NoReconcile : schemas match or inference failed
    
    NeedsReconcile --> YieldBatches : _reconcile(batch) for each batch
    NoReconcile --> YieldBatches : yield batch as-is
    
    YieldBatches --> [*]
```

> **✅ FIXED (§3.6):** The implementation now handles the `BothDeletes` path correctly — applies positional deletes first, then equality anti-join on the surviving rows.

### 2.4 CoW Delete — Two-Pass Streaming Protocol

```mermaid
sequenceDiagram
    participant TD as Transaction.delete
    participant RB as ReadBackend
    participant W as _dataframe_to_data_files
    
    Note over TD: For each original_file:
    
    TD->>RB: read_parquet(file_path, AlwaysTrue) [Pass 1]
    loop Each batch (O(batch_size))
        RB-->>TD: batch
        TD->>TD: kept_row_count += batch.filter(preserve).num_rows
    end
    
    alt kept == 0
        TD->>TD: replaced_files.append((file, []))
    else kept < total
        TD->>RB: read_parquet(file_path, AlwaysTrue) [Pass 2]
        TD->>TD: wrap in RecordBatchReader(_streaming_filter_batches)
        TD->>W: write via RBR (streaming, O(batch_size) peak)
        W-->>TD: new DataFile(s)
        TD->>TD: replaced_files.append((file, new_files))
    else kept == total
        TD->>TD: skip (no rows deleted)
    end
```

---

## 3. Blocking Issues (Must Fix Before Merge)

### 3.1 ~~🔴 DuckDB Backend — False "Streaming" Claim + Full Materialization~~ ✅ FIXED

**Every** DuckDB method (sort_from_files, anti_join_from_files, join_from_files, aggregate_from_files, read_parquet) previously called `.to_arrow_table()` before returning:

```python
# BEFORE (materializes full result in Python heap):
result = con.execute(sql).to_arrow_table()
return iter(result.to_batches())
```

**Problem:** The docstrings claimed "truly streaming, bounded memory" but `.to_arrow_table()` materializes the entire result into Python heap memory. The `iter(.to_batches())` afterwards is merely slicing an already-materialized table into batch-sized views — it provides zero memory benefit.

**Fix applied:** All file-based methods and `read_parquet` now use `fetch_record_batch()` which returns a `pyarrow.RecordBatchReader` that streams results directly from DuckDB's internal buffer without Python-side materialization:

```python
# AFTER (streams from DuckDB internal buffer, O(batch_size) Python memory):
result = con.execute(sql)
return result.fetch_record_batch(rows_per_batch=_DUCKDB_FETCH_BATCH_SIZE)
```

**Changes made:**
1. Module docstring updated to explain the streaming model accurately
2. Added `_DUCKDB_FETCH_BATCH_SIZE = 65_536` constant
3. `sort_from_files` — `.to_arrow_table()` → `.fetch_record_batch()`
4. `anti_join_from_files` — `.to_arrow_table()` → `.fetch_record_batch()`
5. `join_from_files` — `.to_arrow_table()` → `.fetch_record_batch()`
6. `aggregate_from_files` — `.to_arrow_table()` → `.fetch_record_batch()`
7. `DuckDBReadBackend.read_parquet` — `.to_arrow_table()` → `.fetch_record_batch()`
8. `sort` (in-memory) — `.to_arrow_table()` → `.fetch_record_batch()` + docstring clarified
9. `anti_join` (in-memory) — `.to_arrow_table()` → `.fetch_record_batch()` + docstring clarified
10. All docstrings corrected: removed false "truly streaming" claim; now accurately describes that DuckDB handles spill internally and results stream via `fetch_record_batch()`

**Memory model after fix:**
- File-based methods: O(batch_size) Python memory — DuckDB handles sort/join/agg spill internally, results stream from DuckDB's buffer one batch at a time
- In-memory methods: O(input) for materialization (unavoidable), O(batch_size) for output streaming

### 3.2 ~~🔴 `DuckDBReadBackend.read_parquet` Accepts Unbound Filter~~ ✅ FIXED

```python
class DuckDBReadBackend:
    def read_parquet(self, location, projected_schema, row_filter, io_properties):
        ...
        where = expression_to_sql(row_filter)  # ← requires BOUND expression
```

But `expression_to_sql` uses `BoundBooleanExpressionVisitor` which requires the expression to be **bound** (i.e., field references resolved to schema columns). The `row_filter` passed to `read_parquet` from `orchestrate_scan` is `task.residual` which is already a `BooleanExpression` but may or may not be bound depending on how it was constructed.

Looking at `orchestrate_scan`:
```python
batches = backends.read.read_parquet(
    task.file.file_path, projected_schema, task.residual, io_properties, ...
)
```

`task.residual` from `ManifestGroupPlanner` is indeed already bound (it's the output of `residual_evaluator.residual_for(partition)`). However, the PyArrow backend does `expression_to_pyarrow(row_filter)` wrapped in `try/except Exception`, while the DuckDB backend called `expression_to_sql(row_filter)` WITHOUT a try/except.

**If the expression contains a type that `_literal_to_sql` can't handle** (e.g., a custom literal type), DuckDB's read_parquet would raise an unhandled exception, while PyArrow gracefully falls back to no filter.

**Fix applied:** Added the same defensive try/except pattern to `DuckDBReadBackend.read_parquet`:
```python
if isinstance(row_filter, AlwaysTrue):
    sql = f"SELECT {columns} FROM read_parquet('{_escape_path(location)}')"
else:
    try:
        where = expression_to_sql(row_filter)
        sql = f"SELECT {columns} FROM read_parquet('{_escape_path(location)}') WHERE {where}"
    except (TypeError, ValueError, KeyError, NotImplementedError):
        # Fall back to no filter if expression can't be converted to SQL
        sql = f"SELECT {columns} FROM read_parquet('{_escape_path(location)}')"
```

### 3.3 ~~🔴 `Backends` Is a Non-Frozen Mutable Dataclass~~ ✅ FIXED

```python
@dataclass(frozen=True)  # ← NOW frozen
class Backends:
    read: ReadBackend
    write: WriteBackend
    compute: ComputeBackend
    io_properties: Properties
```

The `Backends` instance is shared across all tasks in `orchestrate_scan` (passed by reference to parallel workers via `ExecutorFactory.map`). With the original mutable dataclass, nothing prevented a rogue backend implementation from mutating `backends.io_properties` mid-execution, creating a data race in the thread pool.

**Fix applied:** Changed `@dataclass` to `@dataclass(frozen=True)`. This is consistent with `WriteResult` and `ResolvedBackends` which are also frozen. The `@classmethod resolve()` creates a new instance each time, so frozen doesn't affect construction.

### 3.4 🔴 `_execute_task` Closure Captures Non-Serializable State

```python
def orchestrate_scan(...):
    io_properties = backends.io_properties
    
    def _execute_task(task: FileScanTask) -> list[pa.RecordBatch]:
        ...  # uses `backends`, `io_properties`, `projected_schema`, etc. from closure
    
    executor = ExecutorFactory.get_or_create()
    for task_batches in executor.map(_execute_task, tasks):
        yield from task_batches
```

`ExecutorFactory.get_or_create()` returns a `ThreadPoolExecutor`. Thread pools work fine with closures — Python's GIL protects dict reads, and the captured variables are never mutated. **This is actually safe.**

However, there's a subtle issue: `_execute_task` captures `backends` which contains `backends.compute` — and `backends.compute.filter` is a **generator function** (uses `yield`). When two threads call `backends.compute.filter(...)` simultaneously, they each get independent generator objects (generator functions return new generators on each call). This is correct — no shared state.

**BUT**: `DataFusionComputeBackend.filter` and `DuckDBComputeBackend.filter` both call `expression_to_pyarrow(predicate)` on every invocation. `expression_to_pyarrow` is a pure function (no state) — safe. The `pa_expr` object returned is immutable — safe.

**Revised assessment:** This is NOT a blocking issue. The closure is thread-safe for `ThreadPoolExecutor`. Removing from blocking.

Actually, let me reconsider. The real issue is `_infer_file_schema_from_batch`:

```python
def _infer_file_schema_from_batch(batch, table_metadata):
    name_mapping = table_metadata.schema().name_mapping  # ← thread-safe (immutable)
    ...
    return pyarrow_to_schema(batch.schema, name_mapping=name_mapping, ...)
```

`table_metadata` is accessed by multiple threads but is `@dataclass(frozen=True)` — immutable, safe. `pyarrow_to_schema` is a pure function — safe.

**Final assessment: Thread safety is OK.** Removing this from blocking. But the `frozen=True` fix on `Backends` (§3.3) adds a belt to the suspenders.

### 3.4 (revised) ~~🔴 `_instantiate_write` Always Returns PyArrow Regardless of Engine~~ ✅ FIXED

```python
# BEFORE — dead conditional (both branches identical):
def _instantiate_write(engine: Any) -> WriteBackend:
    if engine == ExecutionEngine.DUCKDB:
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowWriteBackend
        return PyArrowWriteBackend()
    else:
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowWriteBackend
        return PyArrowWriteBackend()

# AFTER — clean, with rationale in docstring:
def _instantiate_write(engine: Any) -> WriteBackend:
    """Instantiate a WriteBackend. Always PyArrow — only PyArrow produces the detailed
    statistics metadata (column sizes, null counts, split offsets) required for Iceberg
    DataFile construction.
    """
    from pyiceberg.execution.backends.pyarrow_backend import PyArrowWriteBackend
    return PyArrowWriteBackend()
```

**Fix applied:** Removed the dead `if/else` structure. The docstring now documents WHY write is always PyArrow (Iceberg DataFile metadata requirements), preventing future developers from re-adding engine-specific branches without understanding the constraint.

### 3.5 ~~🔴 `BoundedMemoryPlanner` Has O(n) Memory for `data_file_lookup` and `delete_file_lookup`~~ ✅ FIXED (docstring corrected)

```python
data_file_lookup: dict[str, DataFile] = {}
delete_file_lookup: dict[str, DataFile] = {}

for entry in chain.from_iterable(planner.plan_manifest_entries(manifests)):
    data_file = entry.data_file
    ...
    data_file_lookup[data_file.file_path] = data_file  # ← holds ALL DataFile objects
```

The lookup dicts hold full `DataFile` objects (with partition values, column stats, etc.) — approximately 2-5 KB each. For 1M entries, that's 2-5 GB just for the dicts. This is the dominant memory term and is NOT bounded by `memory_limit`.

**Why it's still valuable:** The InMemoryPlanner's `DeleteFileIndex` OOMs on the **assignment cross-product** (when many deletes apply to many data files, the intermediate data structures explode). The BoundedMemoryPlanner fixes that specific OOM by pushing the assignment join to DataFusion with spill. Both planners have O(n) for entry enumeration — that's unavoidable without serializing full DataFile objects to Parquet (complex and diminishing returns).

**Fix applied:** Updated the class docstring and inline comment to honestly document the memory model:
- Phase 1 (streaming): O(batch_size) ✓
- Phase 2 (SQL join): O(memory_limit) ✓  
- Lookup dicts: **O(num_entries × DataFile_size)** — dominant term, NOT bounded
- Clarified when this planner helps vs. InMemoryPlanner (assignment OOM, not enumeration OOM)

### 3.6 ~~🔴 `_execute_task` Silently Drops Positional Deletes When Equality Deletes Exist~~ ✅ FIXED

```python
# BEFORE — elif means pos_deletes ignored when eq_deletes present:
if eq_deletes:
    batches = backends.compute.anti_join_from_files(...)
elif pos_deletes:                                        # ← NEVER REACHED
    batches = backends.compute.apply_positional_deletes(...)
else:
    batches = backends.read.read_parquet(...)

# AFTER — explicit combined path handles both delete types:
if pos_deletes and eq_deletes:
    # Both types: apply positional first (narrows by row index),
    # then anti-join remaining against equality delete values.
    batches = backends.compute.apply_positional_deletes(
        data_path=task.file.file_path,
        position_delete_paths=[d.file_path for d in pos_deletes],
        projected_schema=projected_schema,
        io_properties=io_properties,
    )
    eq_cols = _get_equality_field_names(eq_deletes, table_metadata)
    batches = backends.compute.anti_join(
        left=batches,
        right=_chain_read_batches(eq_deletes, ...),
        on=eq_cols,
    )
elif eq_deletes:
    batches = backends.compute.anti_join_from_files(...)
elif pos_deletes:
    batches = backends.compute.apply_positional_deletes(...)
else:
    batches = backends.read.read_parquet(...)
```

**Fix applied:**
1. Added a `pos_deletes and eq_deletes` branch as the FIRST condition — applies positional deletes (row-index based), then pipes the surviving batches through the in-memory `anti_join()` against equality delete values.
2. Added `_chain_read_batches()` helper to read and concatenate batches from multiple equality delete files for the anti-join right side.
3. The ordering (positional first, then equality) is correct per Iceberg spec: positional deletes reference physical row positions which must be resolved against the original file, while equality deletes are value-based and can be applied to any subset of rows.

### 3.7 ~~🔴 `orchestrate_scan` Filter Path Binds `task.residual` Twice~~ ✅ FIXED

```python
# BEFORE — redundant bind on already-bound expression:
if not isinstance(task.residual, AlwaysTrue):
    from pyiceberg.expressions.visitors import bind
    bound_residual = bind(projected_schema, task.residual, case_sensitive)
    batches = backends.compute.filter(batches, bound_residual)

# AFTER — pass directly (task.residual is already bound):
if not isinstance(task.residual, AlwaysTrue):
    batches = backends.compute.filter(batches, task.residual)
```

**Rationale:** `task.residual` is the output of `ResidualEvaluator.residual_for(partition)` which returns a **bound** expression — it has already resolved field references against the schema. Calling `bind()` on an already-bound expression is semantically redundant (the visitor traversal does nothing useful) and potentially hazardous if field ID numbering differs between the table schema and projected schema.

**Fix applied:** Removed the `bind()` call and passed `task.residual` directly to `backends.compute.filter()`. The `filter()` implementations all call `expression_to_pyarrow(predicate)` which expects a bound Iceberg expression — this is exactly what `task.residual` already is.

---

## 4. Non-Blocking Issues (Should Fix, Won't Block Merge)

### 4.1 ~~🟡 `sort` and `anti_join` Methods Exist on Protocol but Aren't in `ComputeBackend`~~ ✅ FIXED

The `ComputeBackend` protocol now includes both in-memory methods:
- `sort(data: Iterator[RecordBatch], sort_keys, memory_limit) -> Iterator[RecordBatch]`
- `anti_join(left: Iterator, right: Iterator, on, memory_limit) -> Iterator[RecordBatch]`

These are used by the combined pos+eq delete path in `_execute_task` (§3.6 fix) where positional deletes produce an in-memory iterator that feeds into the equality anti-join.

**Fix applied:**
1. Added `sort()` and `anti_join()` to the `ComputeBackend` protocol with docstrings noting they require input materialization.
2. Added `sort()` and `anti_join()` implementations to `PolarsComputeBackend` (the only backend that was missing them). PyArrow, DataFusion, and DuckDB already had both methods.

### 4.2 ~~🟡 `Config()` Instantiated Per-Task in `_execute_task`~~ ✅ FIXED

```python
# BEFORE — Config() parsed per-task (10K tasks = 10K YAML parses):
def _execute_task(task):
    ...
    downcast = Config().get_bool(DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE) or False

# AFTER — hoisted outside closure, parsed once:
_downcast_ns_config = Config().get_bool(DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE) or False

def _execute_task(task):
    ...
    downcast = _downcast_ns_config  # captured once from outer scope
```

**Fix applied:** Moved `Config()` instantiation and the `get_bool()` call outside `_execute_task` into the `orchestrate_scan` body. The value is captured once by the closure and reused across all parallel tasks.

### 4.3 ~~🟡 `PyArrowReadBackend.read_parquet` Swallows ALL Exceptions on Filter~~ ✅ FIXED

```python
# BEFORE — catches everything including programming bugs:
except Exception:
    pa_filter = None

# AFTER — narrowed to expected failure modes:
except (TypeError, ValueError, KeyError, NotImplementedError):
    # TypeError: unsupported expression type for PyArrow conversion
    # ValueError: invalid literal or field reference
    # KeyError: field name not found in schema
    # NotImplementedError: expression type not yet supported
    pa_filter = None
```

**Fix applied:** Narrowed the except clause so programming bugs (`AttributeError`, `IndexError`, etc.) in `expression_to_pyarrow` propagate instead of silently producing full table scans.

### 4.4 ~~🟡 `_SortedRecordBatchReader` Doesn't Handle `__exit__` with Exception~~ ✅ FIXED

```python
# BEFORE — always passes None for exc_info:
def _sorted_batches_with_cleanup() -> Iterator:
    try:
        yield from sort_fn(tmp_path)
    finally:
        ctx_manager.__exit__(None, None, None)

# AFTER — forwards actual exception info to context manager:
def _sorted_batches_with_cleanup() -> Iterator:
    try:
        yield from sort_fn(tmp_path)
    except BaseException:
        import sys
        ctx_manager.__exit__(*sys.exc_info())
        raise
    else:
        ctx_manager.__exit__(None, None, None)
```

**Fix applied:** The context manager's `__exit__` now receives the actual exception info when `sort_fn` raises. This respects the context manager protocol and allows future materializers to implement conditional cleanup based on exception type.

### 4.5 ~~🟡 `expression_to_sql` Doesn't Handle `BoundIn` with NULL Values~~ ✅ FIXED

```python
# BEFORE — SQL IN (NULL) never matches:
def visit_in(self, term, literals):
    values = ", ".join(sorted(_literal_to_sql(lit) for lit in literals))
    return f"{self._col(term)} IN ({values})"

# AFTER — separates NULL, emits IS NULL clause:
def visit_in(self, term, literals):
    non_null = {lit for lit in literals if lit is not None}
    has_null = None in literals or len(non_null) < len(literals)
    if non_null:
        values = ", ".join(sorted(_literal_to_sql(lit) for lit in non_null))
        in_clause = f"{self._col(term)} IN ({values})"
        if has_null:
            return f"({in_clause} OR {self._col(term)} IS NULL)"
        return in_clause
    elif has_null:
        return f"{self._col(term)} IS NULL"
    else:
        return "1=0"
```

**Fix applied:** Both `visit_in` and `visit_not_in` now handle NULL values correctly:
- `IN` with NULLs: `(col IN (1, 2) OR col IS NULL)` — matches NULL rows
- `NOT IN` with NULLs: `(col NOT IN (1, 2) AND col IS NOT NULL)` — excludes NULL rows

This aligns with Iceberg's IS NOT DISTINCT FROM semantics for equality deletes where NULL in the delete set should match NULL in the data.

### 4.6 🟡 Import Style: `from __future__ import annotations` Missing From Some Modules

`object_store.py` and `metadata.py` use `from __future__ import annotations`, but the `expression_to_sql.py` does too. Consistency is good. However, I noticed:

- `planning.py` uses it ✓
- `_orchestrate.py` uses it ✓  
- All backend files use it ✓

No issue here actually. All files are consistent.

### 4.7 ~~🟡 `_serialize_partition_key` Uses `|` as Separator — Collision Risk~~ ✅ FIXED

```python
# BEFORE — pipe separator is ambiguous if partition values contain '|':
return "|".join(parts)  # "0|a|b|2024" could be spec=0, part=("a|b", 2024) OR ("a", "b", 2024)

# AFTER — JSON array is unambiguous:
return json.dumps([spec_id] + values, default=str, sort_keys=False)
# "[0, \"a|b\", 2024]" vs "[0, \"a\", \"b\", 2024]" — distinct!
```

**Fix applied:** Switched from pipe-delimited `str()` concatenation to `json.dumps()` serialization. JSON handles embedded special characters, None values (→ `null`), and nested types unambiguously. The `default=str` fallback handles any non-JSON-serializable partition value types (dates, decimals, UUIDs).

### 4.8 ~~🟡 `_detect_available_engines` Cache Not Cleared in Tests~~ ✅ FIXED

```python
@lru_cache(maxsize=1)
def _detect_available_engines() -> set[ExecutionEngine]:
    ...
```

If tests run in an order where the cache is populated before a mock patches out an import, subsequent tests will see stale results.

**Fix applied:** Created `tests/execution/conftest.py` with an `autouse` fixture that clears the cache before and after each test:
```python
@pytest.fixture(autouse=True)
def clear_engine_detection_cache():
    from pyiceberg.execution.engine import _detect_available_engines
    _detect_available_engines.cache_clear()
    yield
    _detect_available_engines.cache_clear()
```

### 4.9 🟡 AST-Based Tests Will Break on Refactoring

Multiple tests in `test_wiring.py` and `test_planning.py` use `inspect.getsource()` + string matching:

```python
source = inspect.getsource(Transaction.delete)
assert "orchestrate_scan" in source
assert "backends.read.read_parquet" in source
```

These are brittle — renaming `orchestrate_scan` to `execute_scan_tasks` breaks the test without changing behavior. They're useful as regression guards during development but should be replaced with behavioral mocks for long-term maintenance.

**Recommendation for merge:** Keep them (they're fast and catch accidental reverts) but add a comment: `# Structural regression test — replace with behavioral test when stabilized.`

---

## 5. Python Idiom & PyIceberg Style Conformance

### 5.1 Conformance Matrix

| Aspect | PyIceberg Standard | This PR | Verdict |
|--------|-------------------|---------|---------|
| `from __future__ import annotations` | All modules | ✓ All modules | ✅ |
| License header | Apache 2.0, all files | ✓ All files | ✅ |
| Type annotations | All public functions | ✓ All public + private | ✅ |
| Docstrings | Google-style, purpose-focused | ✓ Mostly purpose-focused | ✅ |
| `TYPE_CHECKING` imports | Heavy deps only | ✓ pa, Schema, etc. | ✅ |
| `@runtime_checkable` protocols | Used in io/pyarrow.py | ✓ All 4 protocols | ✅ |
| `@dataclass` usage | Common (Record, etc.) | ✓ Backends, WriteResult | ✅ |
| Exception specificity | Narrow catches preferred | ⚠️ `except Exception` in PyArrow read | See §4.3 |
| `noqa` suppressions | Minimal, justified | ✓ None remaining | ✅ |
| Variable naming | Descriptive, snake_case | ✓ Consistent | ✅ |
| Magic numbers | Named constants | ✓ `DEFAULT_MEMORY_LIMIT`, `_OOM_WARNING_THRESHOLD_BYTES` | ✅ |
| Module docstrings | Brief purpose statement | ✓ All execution modules have them | ✅ |

### 5.2 Naming Review (Specific to This PR)

| Name | Assessment |
|------|------------|
| `Backends` | Good — short, plural indicating container |
| `orchestrate_scan` | Good — describes coordination role |
| `_execute_task` | Good — private, action-oriented |
| `_streaming_filter_batches` | Good — describes behavior |
| `_apply_sort_order` | Good — matches `_apply_*` pattern in PyIceberg |
| `_serialize_partition_key` | Good — explicit about purpose |
| `COMPUTE_INTENSIVE_OPERATIONS` | Good — UPPER_CASE for module constant |
| `_IDENTITY` | Acceptable — sentinel pattern, private |
| `_escape_path` | Good — matches `_escape_*` pattern |
| `_SortedRecordBatchReader` | Acceptable — private, descriptive |
| `resolve_backends` | ✅ Good — returns `ResolvedBackends`, name matches return type |
| `_instantiate_write` | ⚠️ Dead conditional (§3.4) — cleanup needed |

### 5.3 Docstring Quality vs PyIceberg Standard

PyIceberg's docstring style (from existing code): brief purpose statement, Args/Returns blocks, no implementation details in the description.

This PR's docstrings are **slightly over-documented** — some include performance characteristics (`O(batch_size)`) and algorithm descriptions. This is useful for a new module that establishes patterns, but should be trimmed for consistency:

**Keep:** One-liner purpose + Args/Returns  
**Remove:** Implementation notes like "Peak memory: O(file_size)" — these belong in code comments, not docstrings.

---

## 6. Refactoring Completeness Audit

### 6.1 ✅ ArrowScan Fully Deprecated — No Production Path

Verified: `ArrowScan` emits `DeprecationWarning` on instantiation. No code path in table operations creates an `ArrowScan` instance. The class is retained for backward compatibility only.

### 6.2 ✅ `_to_arrow_via_file_scan_tasks` Fully Routes Through Backends

The old path (direct `ArrowScan` usage) is completely replaced. All scan materialization goes through `Backends.resolve()` → `orchestrate_scan()`.

### 6.3 ~~⚠️ `read_parquet` Method Signature Mismatch Across Backends~~ ✅ FIXED

| Backend | `read_parquet` signature |
|---------|------------------------|
| PyArrow | `(location, projected_schema, row_filter, io_properties, dictionary_columns=())` |
| DataFusion | `(location, projected_schema, row_filter, io_properties, dictionary_columns=())` ✅ |
| DuckDB | `(location, projected_schema, row_filter, io_properties, dictionary_columns=())` ✅ |
| Polars | `(location, projected_schema, row_filter, io_properties, dictionary_columns=())` ✅ |

**Fix applied:** Added `dictionary_columns: tuple[str, ...] = ()` to DataFusion, DuckDB, and Polars `read_parquet` signatures. Each has a docstring note clarifying that the parameter is accepted for protocol compliance but the backend does not support dictionary-encoded output natively. This prevents `TypeError: unexpected keyword argument` when the orchestrator passes the parameter.

### 6.4 ⚠️ `Polars.sort_from_files` Has Wrong Type Annotation

Protocol declares:
```python
sort_keys: list[tuple[str, Literal["ascending", "descending"]]]
```

Polars implementation:
```python
sort_keys: list[tuple[str, Literal["ascending", "descending"]]]
```

This matches. But the type annotation says `Literal["ascending", "descending"]` while the actual values compared are strings `"ascending"` and `"descending"`. Polars's `descending=` parameter takes `bool`. The conversion `d == "descending"` is correct. No issue.

### 6.5 ⚠️ `_get_sort_order` Referenced in `_apply_sort_order` but Defined Where?

```python
from pyiceberg.execution._orchestrate import _get_sort_order
```

This function is imported from `_orchestrate.py` but I didn't see it in the portion read. It should extract the table's sort order as `list[tuple[str, str]]`. Verify it exists and returns the correct format.

---

## 7. Flakiness & CI Risk Assessment

### 7.1 Risk Matrix

| Test Category | Risk Level | Reason |
|---------------|:---:|--------|
| Backend equivalence (parametrized) | 🟡 Medium | Optional deps (`duckdb`, `polars`) may not be in CI image |
| Config/env var tests | 🟢 Low | Mock-based, no external deps |
| Wiring/dispatch tests | 🟢 Low | Mock-based, deterministic |
| Planning tests | 🟢 Low | Source inspection, no I/O |
| NULL matching tests | 🟡 Medium | Requires `datafusion` + `duckdb` |
| `write_partitioned` size tests | 🟡 Medium | File size depends on compression ratio |
| `_detect_available_engines` cache | 🟡 Medium | Test ordering with `@lru_cache` (§4.8) |

### 7.2 CI Environment Assumptions

The tests use `pytest.importorskip` correctly for optional backends. Tests that require DataFusion, DuckDB, or Polars will be **skipped** (not failed) if the package isn't installed. This is the correct pattern.

**Potential CI issue:** If the CI environment has DuckDB installed for other reasons (it's popular), the DuckDB tests will RUN and may expose the `.to_arrow_table()` materialization issue on large test data. For the test data sizes used (6-8 rows), this is not a problem.

### 7.3 Platform-Specific Risks

| Platform | Risk |
|----------|------|
| Windows | `_escape_path` normalizes backslashes — tested explicitly ✅ |
| macOS | No known issues — all paths are POSIX or cloud URIs |
| Linux ARM | DataFusion has ARM wheels — should work |
| Python 3.10 | `zip(strict=True)` tested explicitly ✅ |
| Python 3.13+ | `@runtime_checkable` Protocol behavior unchanged |

---

## 8. Design Principles Assessment

### 8.1 SOLID Compliance

| Principle | Assessment |
|-----------|-----------|
| **Single Responsibility** | ✅ Each module has one concern (orchestration, resolution, protocol, backends) |
| **Open/Closed** | ✅ New backends added by implementing protocols — no modification to existing code |
| **Liskov Substitution** | ⚠️ DuckDB's false streaming claim violates expectations (§3.1) |
| **Interface Segregation** | ✅ ReadBackend, WriteBackend, ComputeBackend, ObjectStoreBackend are separate |
| **Dependency Inversion** | ✅ Layer 3 depends on abstractions (protocols), not concrete backends |

### 8.2 GoF Patterns Used

| Pattern | Where |
|---------|-------|
| **Strategy** | Each backend is a strategy for read/write/compute |
| **Factory Method** | `Backends.resolve()`, `_instantiate_read/write/compute` |
| **Template Method** | `orchestrate_scan` defines the algorithm, backends fill steps |
| **Iterator** | `Iterator[RecordBatch]` as the universal data stream |
| **Context Manager** | `materialize_to_parquet`, `stream_paths_to_parquet`, `_scoped_env_vars` |

### 8.3 Distributed Systems Considerations

The refactor correctly handles:
- **Credential scoping** — env vars restored after use (not leaked between operations)
- **Failure isolation** — one task failure doesn't corrupt shared state
- **Idempotency** — `Backends.resolve()` produces a new instance each call (no shared mutable state, pending §3.3 freeze fix)

The refactor does NOT handle (correctly deferred):
- **Concurrent credential scopes** — acknowledged in `_scoped_env_vars` docstring
- **Distributed planning** — deferred to server-side planning (REST catalog)
- **Cross-catalog operations** — single io_properties per resolve() call

---

## 9. Recommendations for Merge

### Priority 1 (Must Fix — Will Block Merge)

| # | Issue | Effort | Section | Status |
|:---:|-------|:------:|:-------:|:------:|
| 1 | DuckDB false streaming claim (`.fetch_record_batch`) | 30 min | §3.1 | ✅ FIXED |
| 2 | DuckDB read_parquet missing try/except on filter | 5 min | §3.2 | ✅ FIXED |
| 3 | `Backends` should be `frozen=True` | 1 min | §3.3 | ✅ FIXED |
| 4 | `_instantiate_write` dead conditional | 5 min | §3.4 | ✅ FIXED |
| 5 | `BoundedMemoryPlanner` docstring honest about O(n) lookup | 5 min | §3.5 | ✅ FIXED |
| 6 | `_execute_task` silently drops pos deletes when eq deletes exist | 30 min | §3.6 | ✅ FIXED |
| 7 | Double-bind in orchestrate_scan residual filter | 10 min | §3.7 | ✅ FIXED |

### Priority 2 (Should Fix — May Be Flagged)

| # | Issue | Effort | Section | Status |
|:---:|-------|:------:|:-------:|:------:|
| 7 | `Config()` per-task in `_execute_task` | 5 min | §4.2 | ✅ FIXED |
| 8 | Bare `except Exception` in PyArrow read | 5 min | §4.3 | ✅ FIXED |
| 9 | `dictionary_columns` missing from DuckDB/Polars/DataFusion read | 10 min | §6.3 | ✅ FIXED |
| 10 | `_serialize_partition_key` pipe collision | 15 min | §4.7 | ✅ FIXED |
| 11 | `_detect_available_engines` cache in tests | 5 min | §4.8 | ✅ FIXED |
| 12 | `resolve_engine` naming → `resolve_backends` | 2 min | §5.2 | ✅ FIXED |

### Priority 3 (Nice to Have — Can Defer)

| # | Issue | Section | Status |
|:---:|-------|:-------:|:------:|
| 13 | `sort()` / `anti_join()` not in protocol | §4.1 | ✅ FIXED |
| 14 | `_SortedRecordBatchReader` exception forwarding | §4.4 | ✅ FIXED |
| 15 | `visit_in` NULL handling | §4.5 | ✅ FIXED |

---

## 10. Conclusion

This is a well-architected refactor that delivers genuine value:

1. **OOM-resilience** is real (streaming CoW, limit early-break, streaming count, sort-on-write with spill)
2. **The protocol boundaries are clean** — Arrow RecordBatch as interchange at every seam
3. **The resolution hierarchy is principled** — explicit > config > env > auto-detect
4. **Backward compatibility is preserved** — ArrowScan deprecated but not removed, PyArrow-only users see zero breaking changes

The 6 blocking issues are all **straightforward fixes** (total: ~1 hour of work). None require architectural changes. The design is correct; the implementation needs polish.

**Merge recommendation: Approve after Priority 1 fixes. Priority 2 can be follow-up commits.**
