# Track 1 & Track 2: How They Overlap, Swap, and Coexist

## The Core Difference (One Sentence Each)

- **Track 1**: PyIceberg calls `datafusion-python` (a Python package) to run DataFusion. Python orchestrates the plan, registers files, runs SQL, gets Arrow data back.
- **Track 2**: PyIceberg calls `pyiceberg_core.execution` (Rust via PyO3) with one function call. Rust does everything internally and returns only metadata.

Both use the same DataFusion engine underneath. The difference is which language drives it.

---

## Current State: PyIceberg Already Uses Track 2 Style

PyIceberg's existing DataFusion integration is **exclusively through `pyiceberg_core` (Rust via PyO3)**:

- `pyiceberg_core.datafusion.IcebergDataFusionTable` — Rust provides the TableProvider via PyCapsule FFI
- `pyiceberg_core.transform` — Rust does partition transforms (bucket, year, month, etc.)
- PyIceberg itself **never** creates a `datafusion.SessionContext` or orchestrates DataFusion from Python

The `from datafusion import SessionContext` line only appears in a **docstring example** and in **tests** — showing end users how to use the TableProvider they get back. PyIceberg's own internal code never imports or uses `datafusion-python` directly.

**However, `datafusion` is already a declared optional extra in `pyproject.toml`:**

```toml
pyiceberg-core = ["pyiceberg-core>=0.5.1,<0.10.0"]
datafusion = ["datafusion>=52,<53"]
```

So `pip install 'pyiceberg[datafusion]'` already works — users can already install it. It's technically available as an internal dependency if we chose to use it. The package is there; it's just not used internally today.

**What this means for track choice:**

- Track 2 is the **consistent** choice — extends the existing `pyiceberg_core` pattern
- Track 1 would introduce a new internal pattern (PyIceberg orchestrating DataFusion from Python) that doesn't exist today
- Track 1 is still **technically possible** given the `pyproject.toml` — nothing prevents it
- Maintainers may push back on Track 1 ("why orchestrate from Python when we have pyiceberg_core for this?")

---

## The Swap Is Trivial (Not a Refactor)

Regardless of which track you start with, **swapping is a one-function-body change**. The function signature, inputs, outputs, and all calling code remain identical.

**Track 1 body:**
```python
def resolve_equality_deletes(data_path, delete_paths, eq_cols, io_props, memory_limit):
    from datafusion import SessionContext, RuntimeEnvBuilder
    runtime = RuntimeEnvBuilder().with_fair_spill_pool(parse(memory_limit)).with_disk_manager_os()
    ctx = SessionContext(runtime=runtime)
    ctx.register_parquet("data", data_path)
    for i, p in enumerate(delete_paths):
        ctx.register_parquet(f"del_{i}", p)
    sql = f"SELECT d.* FROM data d LEFT ANTI JOIN (...) e ON ..."
    return ctx.sql(sql).to_arrow_table()
```

**Track 2 replacement (same signature, same return type):**
```python
def resolve_equality_deletes(data_path, delete_paths, eq_cols, io_props, memory_limit):
    from pyiceberg_core.execution import execute_equality_resolution
    return execute_equality_resolution(
        data_file_paths=[data_path],
        eq_delete_file_paths=delete_paths,
        equality_field_names=eq_cols,
        file_io_properties=io_props,
        memory_limit=memory_limit,
    )
```

**Why it's not a refactor:**
- Same inputs (file paths, column names, memory limit)
- Same output (Arrow table / RecordBatches)
- Callers (`table.scan()`, `table.compact()`) don't know or care which ran
- It's a leaf-level implementation detail, not an architectural boundary

**The only thing that would make it a refactor:** if you leaked Track 1 internals upward (exposed the `SessionContext` to callers, let other code depend on DataFusion-specific types). Keep the function boundary clean and it's always a 1:1 swap.

---

## What the Code Looks Like: Same Operation, Both Tracks

### Example: Equality Delete Resolution

**Track 1 (Python-side DataFusion):**

```python
# pyiceberg/execution/operations/equality_resolve.py

from datafusion import SessionContext, RuntimeEnvBuilder

def resolve_equality_deletes_track1(
    data_file_path: str,
    eq_delete_file_paths: list[str],
    equality_columns: list[str],
    memory_limit_bytes: int = 512 * 1024 * 1024,
) -> pa.Table:
    """Execute anti-join using datafusion-python directly."""

    # 1. Configure bounded session IN PYTHON
    runtime = (
        RuntimeEnvBuilder()
        .with_fair_spill_pool(memory_limit_bytes)
        .with_disk_manager_os()
    )
    ctx = SessionContext(runtime=runtime)

    # 2. Register Parquet files IN PYTHON
    ctx.register_parquet("data", data_file_path)
    for i, path in enumerate(eq_delete_file_paths):
        ctx.register_parquet(f"deletes_{i}", path)

    # 3. Build SQL IN PYTHON
    union_deletes = " UNION ALL ".join(
        f"SELECT {', '.join(equality_columns)} FROM deletes_{i}"
        for i in range(len(eq_delete_file_paths))
    )
    join_cond = " AND ".join(f"d.{c} = e.{c}" for c in equality_columns)
    sql = f"""
        SELECT d.* FROM data d
        LEFT ANTI JOIN ({union_deletes}) e ON {join_cond}
    """

    # 4. Execute (DataFusion runs in Rust, but orchestration is Python)
    result = ctx.sql(sql)

    # 5. Get results back as PyArrow table (zero-copy Arrow C Data Interface)
    return result.to_arrow_table()
```

**Track 2 (Rust-side via pyiceberg_core):**

```python
# pyiceberg/execution/operations/equality_resolve.py

def resolve_equality_deletes_track2(
    data_file_path: str,
    eq_delete_file_paths: list[str],
    equality_columns: list[str],
    file_io_properties: dict[str, str],
    memory_limit: str = "512MB",
) -> list[pa.RecordBatch]:
    """Execute anti-join entirely in Rust."""
    from pyiceberg_core.execution import execute_equality_resolution

    # ONE call. Everything happens in Rust. GIL released.
    return execute_equality_resolution(
        data_file_paths=[data_file_path],
        eq_delete_file_paths=eq_delete_file_paths,
        equality_field_names=equality_columns,
        file_io_properties=file_io_properties,
        memory_limit=memory_limit,
    )
```

**The dispatch layer that swaps between them:**

```python
# pyiceberg/execution/operations/equality_resolve.py

from pyiceberg.execution.engine import resolve_engine, ExecutionEngine

def resolve_equality_deletes(
    data_file_path: str,
    eq_delete_file_paths: list[str],
    equality_columns: list[str],
    file_io_properties: dict[str, str],
    memory_limit: str = "512MB",
) -> pa.Table:
    """Public API — dispatches to best available backend."""

    engine = resolve_engine("equality_delete_resolution")

    if engine == ExecutionEngine.DATAFUSION_RUST:
        # Track 2 available — use it
        batches = resolve_equality_deletes_track2(
            data_file_path, eq_delete_file_paths,
            equality_columns, file_io_properties, memory_limit,
        )
        return pa.Table.from_batches(batches)

    elif engine == ExecutionEngine.DATAFUSION_PYTHON:
        # Track 1 — datafusion-python available but not pyiceberg_core.execution
        return resolve_equality_deletes_track1(
            data_file_path, eq_delete_file_paths,
            equality_columns, parse_memory_limit(memory_limit),
        )

    else:
        # PyArrow fallback (small data only)
        return resolve_equality_deletes_pyarrow(
            data_file_path, eq_delete_file_paths, equality_columns,
        )
```

---

### Example: Compaction

**Track 1 (Python-side DataFusion):**

```python
def compact_track1(
    file_paths: list[str],
    sort_columns: list[str],
    target_file_size_bytes: int,
    memory_limit_bytes: int,
) -> list[pa.RecordBatch]:
    """Sort + rewrite via datafusion-python."""

    runtime = RuntimeEnvBuilder().with_fair_spill_pool(memory_limit_bytes).with_disk_manager_os()
    ctx = SessionContext(runtime=runtime)

    # Register all source files
    for i, path in enumerate(file_paths):
        ctx.register_parquet(f"file_{i}", path)

    # UNION ALL + ORDER BY
    union = " UNION ALL ".join(f"SELECT * FROM file_{i}" for i in range(len(file_paths)))
    order = ", ".join(sort_columns) if sort_columns else "1"
    sql = f"SELECT * FROM ({union}) ORDER BY {order}"

    result = ctx.sql(sql)

    # Get sorted batches back — Python writes them to new Parquet files
    return list(result.to_arrow_record_batch_reader())
```

**Track 2 (Rust-side via pyiceberg_core):**

```python
def compact_track2(
    metadata_location: str,
    file_io_properties: dict[str, str],
    files_to_compact: list[str],  # DataFile JSON
    sort_columns: list[str] | None,
    target_file_size_bytes: int,
    memory_limit: str,
) -> CompactionResult:
    """Sort + rewrite entirely in Rust. Returns new file metadata."""
    from pyiceberg_core.execution import execute_compaction

    return execute_compaction(
        metadata_location=metadata_location,
        file_io_properties=file_io_properties,
        files_to_compact=files_to_compact,
        target_file_size_bytes=target_file_size_bytes,
        sort_columns=sort_columns,
        memory_limit=memory_limit,
    )
```

**Key difference for compaction specifically:**

| Aspect | Track 1 | Track 2 |
|--------|---------|---------|
| Who reads Parquet files | DataFusion (via Python-registered paths) | DataFusion (via Rust FileIO) |
| Who sorts | DataFusion SortExec (same either way) | DataFusion SortExec (same either way) |
| Who writes new Parquet files | **Python** (iterates batches, calls PyArrow ParquetWriter) | **Rust** (IcebergWriteExec — handles target size splits, partition routing, file naming) |
| Who returns what | Arrow RecordBatches cross FFI → Python writes files → Python gets DataFile metadata | Rust writes files internally → returns DataFile JSON metadata only |
| Object store access | Must configure on DataFusion side (separate from PyIceberg's FileIO) | Uses Iceberg's FileIO (same config as PyIceberg) |

The Rust path is cleaner for compaction because **writing** is the hard part — target file size splitting, partition-aware routing, and generating correct DataFile metadata are all already implemented in `IcebergWriteExec`. With Track 1, you'd have to reimplement file-size-based splitting in Python.

---

## How the Engine Resolver Works

```python
# pyiceberg/execution/engine.py

import warnings
from enum import Enum, auto

class ExecutionEngine(Enum):
    DATAFUSION_RUST = auto()    # Track 2: pyiceberg_core.execution available
    DATAFUSION_PYTHON = auto()  # Track 1: datafusion-python available
    PYARROW = auto()            # Fallback: no DataFusion at all

def resolve_engine(operation: str) -> ExecutionEngine:
    """Detect best available engine. Preference: Rust > Python DF > PyArrow."""

    # Try Track 2 first (best performance)
    try:
        from pyiceberg_core.execution import execute_compaction  # noqa: F401
        return ExecutionEngine.DATAFUSION_RUST
    except (ImportError, AttributeError):
        pass

    # Try Track 1 (still bounded memory, just Python-orchestrated)
    try:
        import datafusion  # noqa: F401
        return ExecutionEngine.DATAFUSION_PYTHON
    except ImportError:
        pass

    # Fallback (works for small data, OOMs on large)
    warnings.warn(
        f"'{operation}' will use in-memory (PyArrow) execution. "
        f"For large tables: pip install 'pyiceberg[pyiceberg-core]' or 'pyiceberg[datafusion]'",
        UserWarning,
        stacklevel=3,
    )
    return ExecutionEngine.PYARROW
```

**Three install states and what they get:**

| Install | What's available | Engine used |
|---------|-----------------|-------------|
| `pip install pyiceberg` | PyArrow only | `PYARROW` (OOMs on large data) |
| `pip install 'pyiceberg[datafusion]'` | PyArrow + datafusion-python | `DATAFUSION_PYTHON` (Track 1) |
| `pip install 'pyiceberg[pyiceberg-core]'` | PyArrow + pyiceberg-core (includes DataFusion) | `DATAFUSION_RUST` (Track 2) |

---

## How They Get Built in Parallel (Timeline)

```
TIME ──────────────────────────────────────────────────────────────────────►

PYICEBERG (iceberg-python):
├── Engine resolver module ─────────── ships immediately
├── Track 1 implementations ────────── ships immediately (no upstream deps)
│   ├── equality_resolve_track1()
│   ├── compact_track1()
│   ├── cow_rewrite_track1()
│   └── antijoin_paths_track1()
├── Dispatch layer (resolve_engine) ── ships with Track 1
└── Track 2 swap-in ────────────────── ships when pyiceberg_core.execution is ready
    (just adds the DATAFUSION_RUST branch to resolve_engine)

ICEBERG-RUST:
├── PR 2: bounded session helper ───── can start NOW (no deps)
├── PR 3a: execution module stubs ──── can start NOW
├── PR 1: overwrite commit ─────────── blocked on OverwriteAction (#2185)
├── PR 3b: execute_antijoin_paths ──── after PR 2
├── PR 3c: execute_equality_res ────── after PR 2
├── PR 3d: execute_cow_rewrite ─────── after PR 1 + PR 2
└── PR 3e: execute_compaction ──────── after PR 1 + PR 2

EXTERNAL (others' work):
├── OverwriteAction (PR #2185) ─────── under review
└── RewriteFilesAction (#2244) ─────── in progress (MergingSnapshotProducer PR #2620)
```

**The key insight: Track 1 ships independently and immediately.** When Track 2 components land one by one, you swap them in behind the same dispatch layer. The user-facing API (`table.compact()`) never changes.

---

## The Swap-In Mechanics (Concrete)

### Day 1: Only Track 1 exists

```python
# resolve_engine returns DATAFUSION_PYTHON (or PYARROW)
# All operations use Track 1 implementations
```

### Day N: `execute_equality_resolution` lands in pyiceberg_core

```python
def resolve_engine(operation: str) -> ExecutionEngine:
    try:
        if operation in ("equality_delete_resolution",):
            from pyiceberg_core.execution import execute_equality_resolution  # noqa
            return ExecutionEngine.DATAFUSION_RUST
    except (ImportError, AttributeError):
        pass

    # ... fallthrough to Track 1 / PyArrow
```

Or simpler — just check once if the module exists, and use it for all operations that have implementations:

```python
_RUST_EXECUTION_AVAILABLE = False
try:
    import pyiceberg_core.execution as _rust_exec
    _RUST_EXECUTION_AVAILABLE = True
except ImportError:
    pass

# Then per-operation:
def resolve_equality_deletes(...):
    if _RUST_EXECUTION_AVAILABLE and hasattr(_rust_exec, 'execute_equality_resolution'):
        return _use_track2(...)
    elif _DATAFUSION_PYTHON_AVAILABLE:
        return _use_track1(...)
    else:
        return _use_pyarrow(...)
```

### Day M: All Track 2 operations land

Track 1 code stays in the codebase as the fallback for users who have `datafusion` installed but not `pyiceberg-core`. The dispatch is automatic — no user action needed.

---

## What's Shared Between Tracks (No Duplication)

These PyIceberg components are identical regardless of which track executes the compute:

| Component | Why it's shared |
|-----------|----------------|
| **File selection** (which files to compact/rewrite) | Manifest-based planning happens in Python either way |
| **Commit logic** (Transaction, overwrite, append) | Python Transaction API either way (Track 2 could eventually commit in Rust, but initially returns metadata for Python to commit) |
| **Engine resolution** | Same `resolve_engine()` dispatches to whichever is available |
| **User-facing API** (`table.compact()`, `table.delete()`) | Identical method signatures regardless of backend |
| **Memory limit parsing** | Both tracks understand "512MB" → bytes |
| **DataFile serialization** | Both tracks exchange DataFile metadata as JSON strings |

What's **duplicated** (unavoidably):

| Component | Track 1 (Python) | Track 2 (Rust) |
|-----------|-----------------|----------------|
| Session configuration | `RuntimeEnvBuilder().with_fair_spill_pool(...)` | `create_bounded_session(BoundedSessionConfig::new(...))` |
| File registration | `ctx.register_parquet("name", path)` | `FileIO + StaticTable + IcebergTableScan` |
| SQL/plan construction | Python string formatting | Rust plan builder or SQL |
| File writing (for compaction/CoW) | Python iterates batches + PyArrow ParquetWriter | Rust `IcebergWriteExec` |

The duplication is small and mechanical. The Track 1 implementations are ~20-50 lines each. They're cheap to maintain as fallbacks.

---

## Why Both Tracks Exist (Not Just One)

| Question | Answer |
|----------|--------|
| Why not just Track 2? | It requires iceberg-rust PRs that are blocked on upstream work (#2185, #2620). Could be months. |
| Why not just Track 1? | It works but has friction: object store config must be duplicated (DataFusion side + PyIceberg FileIO), and file writing in Python is less capable than `IcebergWriteExec` (no target-size splitting, no partition routing). |
| Why build both? | Track 1 ships now. Track 2 replaces it later for better performance. The dispatch layer makes this transparent to users. |
| Will Track 1 be removed? | No — it stays as the fallback for `pip install 'pyiceberg[datafusion]'` users who don't have pyiceberg-core. It's also simpler to debug (pure Python orchestration). |

---

## Performance Difference Between Tracks

For the same 10GB compaction with 512MB memory budget:

| Metric | Track 1 | Track 2 | Difference |
|--------|---------|---------|------------|
| Sort execution | Same (DataFusion SortExec in both) | Same | None |
| Spill-to-disk | Same (FairSpillPool in both) | Same | None |
| File I/O (read) | DataFusion reads via its object store registry | DataFusion reads via Iceberg FileIO | Negligible |
| File I/O (write) | Python iterates batches → PyArrow writes → multiple Python→Parquet calls | Rust IcebergWriteExec writes directly | **Track 2 faster** (no FFI per-batch for write) |
| GIL during execution | Released (datafusion-python handles this) | Released (Tokio runtime) | Same |
| Overhead of file registration | Per-file `register_parquet()` call from Python | Single plan construction in Rust | **Track 2 faster** for many files |
| Object store setup | Must configure `ObjectStoreUrl` separately | Uses existing FileIO properties | **Track 2 simpler** |

**Bottom line**: For reads/sorts/joins, performance is identical. For writes (compaction, CoW), Track 2 is moderately faster because it avoids the Python write loop. The difference is maybe 10-20% — not a 10x gap.

---

## Object Store Configuration: The Practical Friction of Track 1

The biggest pain point of Track 1 is that **DataFusion has its own object store registry** separate from PyIceberg's FileIO:

```python
# Track 1: Must configure object store TWICE
# PyIceberg's FileIO (for manifest reading, metadata):
table = catalog.load_table("db.events")  # uses FileIO internally

# DataFusion's object store (for Parquet reading):
ctx = SessionContext(runtime=runtime)
ctx.register_object_store("s3://bucket", AmazonS3Builder()
    .with_region("us-east-1")
    .with_access_key_id(...)
    .with_secret_access_key(...)
    .build()
)
ctx.register_parquet("data", "s3://bucket/data/file.parquet")
```

**Track 2 doesn't have this problem** — it uses `FileIOBuilder::new(factory).with_props(file_io_properties)` which accepts the same properties PyIceberg already has.

**Workaround for Track 1**: Extract object store config from PyIceberg's `table.io.properties` and translate to DataFusion's object store config. This is doable but annoying boilerplate.

---

## Summary: The Mental Model

```
┌─────────────────────────────────────────────────────────────────┐
│  User calls: table.compact(memory_limit="512MB")                │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│  Engine Resolver: what's installed?                              │
│                                                                 │
│  pyiceberg_core.execution available? → Track 2 (Rust)           │
│  datafusion-python available?        → Track 1 (Python DF)      │
│  neither?                            → PyArrow fallback          │
└────────┬──────────────────────┬─────────────────────┬───────────┘
         │                      │                     │
         ▼                      ▼                     ▼
┌────────────────┐  ┌─────────────────────┐  ┌───────────────────┐
│ Track 2 (Rust) │  │ Track 1 (Python DF) │  │ PyArrow fallback  │
│                │  │                     │  │                   │
│ One FFI call   │  │ Configure session   │  │ Load all to RAM   │
│ Rust: scan →   │  │ Register files      │  │ Sort in memory    │
│   sort → write │  │ Run SQL             │  │ Write from Python │
│ Returns: meta  │  │ Get Arrow batches   │  │                   │
│                │  │ Write from Python   │  │ OOMs on large     │
│ Best perf      │  │ Good perf           │  │ data              │
└────────────────┘  └─────────────────────┘  └───────────────────┘
         │                      │                     │
         ▼                      ▼                     ▼
┌─────────────────────────────────────────────────────────────────┐
│  Same commit path: Python Transaction API                       │
│  tx.overwrite(old_files=[...], new_files=[...])                 │
└─────────────────────────────────────────────────────────────────┘
```

**Track 1 and Track 2 produce identical results.** They share the same user API, the same commit path, and the same engine resolution layer. The only difference is internal execution efficiency and how much boilerplate lives in Python vs. Rust.

Build Track 1 now. Swap in Track 2 components as they land. Users never notice the transition.
