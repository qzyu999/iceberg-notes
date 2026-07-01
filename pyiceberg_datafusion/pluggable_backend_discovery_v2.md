# Pluggable Backend Discovery: Implementation Review

Branch: `pluggable-backend-discovery` @ commit `4093a7d8`
Reviewer perspective: Principal Engineer, pre-merge code review.

---

## What Was Done

Created `pyiceberg/execution/` module with:
- `protocol.py` — `IOBackend`, `ComputeBackend`, `ExecutionBackend` Protocol definitions
- `engine.py` — `resolve_engine()` with explicit-over-implicit resolution
- `backends/pyarrow_backend.py` — PyArrow implementation (default fallback)
- `backends/datafusion_backend.py` — DataFusion implementation (bounded-memory)
- `backends/duckdb_backend.py` — DuckDB implementation (discovery validation)
- `test_backend_equivalence.py` — proves all three produce identical output for sort + anti-join

**Result:** All three backends pass the equivalence test (sort and anti-join produce identical output).

---

## Critical Review: Issues That Must Be Fixed Before PR

### CRITICAL: Style and Convention Violations

#### 1. Missing `from __future__ import annotations` consistency

The `backends/__init__.py` is missing it while all others have it. Minor but the repo
is strict about consistency.

#### 2. Type annotations use string quotes inconsistently

`protocol.py` correctly uses `TYPE_CHECKING` + string annotations. But `pyarrow_backend.py`
imports `pyarrow` at module level (line 28: `import pyarrow as pa`) — this is a
**hard dependency** that prevents importing the module when PyArrow isn't installed.
Wait — PyArrow IS always installed (required dep). This is fine.

However, `datafusion_backend.py` and `duckdb_backend.py` put `import pyarrow as pa`
inside method bodies (lazy import via `TYPE_CHECKING`). But then they DO `import pyarrow as pa`
inside each method call. This is correct for optional deps but creates repeated import
overhead per call. Should cache the import at class level on first use.

**Verdict:** Acceptable for discovery. For the real PR, follow the pattern in `pyarrow.py`
where `import pyarrow as pa` is at the top (since it's always available) and optional
deps are lazy.

#### 3. No `__all__` in `backends/__init__.py`

The repo convention is to have `__all__` in `__init__.py` when re-exporting.
`backends/__init__.py` has only a docstring — no exports. This is fine since backends
are imported explicitly, but should be noted.

#### 4. Docstring format

The repo uses Google-style docstrings (seen in `maintenance.py`):
```python
"""Return an ExpireSnapshots builder for snapshot expiration operations.

Returns:
    ExpireSnapshots builder for ...
"""
```

Our code uses the same style ✓. But some of our docstrings are too verbose for
protocol methods (multi-paragraph explanations in `protocol.py`). The repo convention
is concise docstrings. The protocol file is an exception (documentation IS the value),
so this is acceptable.

---

### CRITICAL: Architectural Deficiencies

#### 5. `list(data)` materializes the entire iterator — defeats streaming purpose

**The biggest problem in the implementation:**

```python
# datafusion_backend.py, line 164:
batches = list(data)  # ← THIS DEFEATS THE STREAMING CONTRACT
ctx.register_record_batches("sort_input", [batches])
```

The protocol defines `data: Iterator[pa.RecordBatch]` specifically for bounded memory.
But EVERY backend immediately calls `list(data)` to materialize all batches into RAM
before doing anything. This means:

- **PyArrow sort:** `list(data)` materializes → OOM before sort even starts
- **DataFusion sort:** `list(data)` materializes → OOM before DataFusion sees it
- **DuckDB sort:** `list(data)` materializes → same

**The streaming contract is violated by all three implementations.**

**Root cause:** DataFusion's `register_record_batches()` and DuckDB's `con.register()`
both require materialized data (list of batches or a Table). They cannot consume a
lazy iterator. This is a fundamental API limitation of both libraries' Python bindings.

**Fix required:** For the "streaming" promise to hold, the backend must either:
1. Use `register_parquet()` (read from file directly — truly streaming), or
2. Accept that `register_record_batches()` materializes (document this honestly), or
3. Write batches to a temp Parquet/IPC file and register THAT (spill before compute)

This is the **#1 issue** in the current implementation. The protocol promises streaming
but the implementation materializes. The discovery reveals that true streaming requires
the backend to control the file-read pipeline (i.e., `register_parquet()` not
`register_record_batches()`).

**Impact on protocol design:** The `ComputeBackend.sort(data: Iterator[RecordBatch], ...)`
signature may need to be reconsidered. If the backend can't consume an iterator without
materializing, perhaps the interface should accept file paths instead:

```python
def sort_files(
    self,
    file_paths: list[str],
    sort_keys: ...,
    io_properties: ...,
    memory_limit: ...,
) -> Iterator[pa.RecordBatch]:
```

This is a **discovery finding** — the protocol signature looks clean but the
implementation reveals it cannot be satisfied without materialization.

#### 6. `_expression_to_sql()` uses `isinstance` chain — not the visitor pattern

The existing `expression_to_pyarrow()` in `pyarrow.py` uses PyIceberg's
`BoundBooleanExpressionVisitor` (proper visitor pattern with double dispatch).
Our `_expression_to_sql()` uses a raw `isinstance` chain:

```python
def _convert(expr: BooleanExpression) -> str:
    if isinstance(expr, AlwaysTrue):
        return "1=1"
    elif isinstance(expr, AlwaysFalse):
        ...
```

**Problems:**
- Doesn't use PyIceberg's existing visitor infrastructure
- The expression classes imported (`EqualTo`, `GreaterThan`, etc.) are the UNBOUND
  versions. The actual expressions in `FileScanTask.residual` are BOUND (e.g.,
  `BoundEqualTo`, `BoundGreaterThan`). This code would fail at runtime on real data.
- Missing handling for `BoundReference` term access (`.ref().name` may not exist
  on unbound terms)

**Fix required:** Use `BoundBooleanExpressionVisitor[str]` like the existing PyArrow
converter does. Import the BOUND expression types. Test against actual bound expressions.

#### 7. `write_parquet()` return type is `dict` — should be a typed dataclass

```python
def write_parquet(...) -> "dict":
```

The repo uses typed dataclasses everywhere (`DataFile`, `ManifestEntry`, etc.). A raw
`dict` return is un-Pythonic and loses type safety. Should return a `WriteResult`
dataclass or directly return a `DataFile`.

#### 8. `PyArrowIOBackend.read_parquet()` uses `ds.dataset()` — wrong abstraction

```python
dataset = ds.dataset(location, format="parquet")
scanner = dataset.scanner(columns=columns, filter=pa_filter)
```

This bypasses PyIceberg's existing `FileIO` abstraction. The existing `ArrowScan` uses:
```python
with io.new_input(task.file.file_path).open() as fin:
    fragment = arrow_format.make_fragment(fin)
```

The backend should use the SAME FileIO path (which handles S3/GCS credentials via
PyArrowFileIO). Using `ds.dataset(location)` directly won't work for cloud storage
without separate credential configuration.

**Fix:** Accept `FileIO` as a parameter or use `io_properties` to construct the
appropriate filesystem.

---

### MODERATE: Logic Issues

#### 9. `PyArrowComputeBackend.anti_join()` composite key approach is fragile

```python
def _composite_key(table: pa.Table, cols: list[str]) -> pa.Array:
    arrays = [pc.cast(table.column(c), pa.string()) for c in cols]
    return pc.binary_join_element_wise(*arrays, "||")
```

This creates a composite key by concatenating string representations with `||`.
**This produces collisions:** `("a||b", "c")` has the same composite key as `("a", "b||c")`.
A separator like `\x00` (null byte) would be safer, or better yet, use struct-based
comparison (PyArrow supports struct arrays in `is_in` since v12).

#### 10. Empty iterator handling is inconsistent

Some methods return `iter([])` for empty input, others don't check. The behavior
should be consistent and documented: empty input → empty output, no errors.

#### 11. `DuckDBComputeBackend` uses `ANTI JOIN` syntax — should be `LEFT ANTI JOIN`

```python
sql = f"SELECT l.* FROM left_tbl l ANTI JOIN right_tbl r ON {join_cond}"
```

DuckDB's correct syntax for anti-join is `ANTI JOIN` (not `LEFT ANTI JOIN` like
DataFusion). This is correct for DuckDB but the inconsistency between backends in SQL
syntax highlights that the expression conversion must be backend-specific even for SQL.
✓ Actually correct — DuckDB does use just `ANTI JOIN`.

---

### MINOR: Cleanup Items

#### 12. `test_backend_equivalence.py` is in the repo root

Should be in `tests/execution/` following the repo's test directory structure.

#### 13. No `py.typed` marker for the new module

The repo has `pyiceberg/py.typed`. The new `execution/` module inherits this.
Not an issue but worth noting for type checker compatibility.

#### 14. `_detect_available_engines()` uses `lru_cache` — correct but test-unfriendly

Tests that mock import behavior will need to clear this cache. Should document this
or provide a `_reset_cache()` utility for testing.

#### 15. `resolve_engine()` returns `tuple[ExecutionEngine, ExecutionEngine]`

A named tuple or dataclass would be clearer:
```python
@dataclass
class ResolvedBackends:
    compute: ExecutionEngine
    io: ExecutionEngine
```

---

## Summary: The Discovery Findings

### What the Implementation Proves

1. ✅ **The protocol signatures generalize** — all three backends implement the same
   `sort()` and `anti_join()` methods with identical signatures.
2. ✅ **Output equivalence holds** — same input produces same output across backends.
3. ✅ **Engine detection works** — auto-detect + explicit override + fallback chain.
4. ✅ **Expression conversion to SQL is tractable** — ~120 lines covers all predicate types.

### What the Implementation Reveals (Deficiencies)

1. ❌ **Streaming is a lie** — all backends `list(data)` before processing. The
   `Iterator[RecordBatch]` input is immediately materialized. True streaming requires
   backends to control the read pipeline (file paths, not pre-read batches).

2. ❌ **Expression handling is wrong** — uses unbound expression types; should use
   the visitor pattern on bound expressions (like existing `expression_to_pyarrow`).

3. ❌ **FileIO integration missing** — backends bypass PyIceberg's FileIO abstraction
   for cloud storage. Must integrate with the existing credential management.

4. ⚠️ **Write return type untyped** — should be a proper dataclass, not `dict`.

5. ⚠️ **Composite key anti-join has collision risk** — needs a proper separator or
   struct-based approach.

### What Must Change for the Real PR

| Issue | Severity | Fix |
|-------|----------|-----|
| Streaming materialization (#5) | **Critical** | Redesign: pass file paths to backends for read ops, use `register_parquet()` |
| Expression handling (#6) | **Critical** | Rewrite using `BoundBooleanExpressionVisitor[str]` |
| FileIO integration (#8) | **Critical** | Accept `FileIO` or use `io_properties` to create filesystem |
| Write return type (#7) | Moderate | Define `WriteResult` dataclass |
| Composite key collision (#9) | Moderate | Use null-byte separator or struct comparison |
| Test location (#12) | Minor | Move to `tests/execution/` |
| Named return type (#15) | Minor | Use dataclass instead of tuple |

### The Key Discovery

**The most important finding:** The protocol's compute methods (`sort`, `anti_join`)
should NOT accept `Iterator[RecordBatch]` as input for the PRIMARY use case. They
should accept **file paths** so the backend controls the entire pipeline from disk.

The `Iterator[RecordBatch]` signature only makes sense for the SECONDARY use case:
user-provided in-memory data (e.g., upsert source dataframe).

This suggests the protocol needs TWO variants per operation:
```python
# Variant A: Backend reads from files (truly streaming, bounded memory)
def sort_files(self, file_paths: list[str], ...) -> Iterator[pa.RecordBatch]: ...

# Variant B: Backend computes on pre-materialized data (in-memory path)
def sort_batches(self, data: Iterator[pa.RecordBatch], ...) -> Iterator[pa.RecordBatch]: ...
```

Or alternatively, a union input type:
```python
def sort(self, source: list[str] | Iterator[pa.RecordBatch], ...) -> Iterator[pa.RecordBatch]: ...
```

This is the kind of insight Fowler's principle was designed to surface — by building
three implementations, we discovered that the clean-looking protocol has a fundamental
impedance mismatch with how engines actually work.
