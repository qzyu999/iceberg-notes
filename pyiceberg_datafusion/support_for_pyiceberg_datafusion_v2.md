# Support for PyIceberg DataFusion Integration (v2)

[qzyu999@gmail.com](mailto:qzyu999@gmail.com)

---

## Introduction

Apache PyIceberg faces a class of compute-heavy operations that cannot be implemented correctly under bounded memory using PyArrow alone. These operations—equality delete resolution, data compaction, upsert, orphan file deletion—are fundamental to production Iceberg table management but require execution-engine capabilities (spill-to-disk, hash joins, external sorts) that PyArrow structurally cannot provide.

Apache DataFusion, a Rust-based query execution framework built on Arrow, provides exactly these capabilities. This document proposes using DataFusion as PyIceberg's **internal compute library** for all data-intensive operations. The design follows a DuckDB-style UX: users optionally configure a memory budget. DataFusion is used when installed; PyArrow remains the fallback for small data.

This integration is not optional for feature parity with Java Iceberg. Java delegates compute to Spark/Flink. Python has no equivalent unless we build one.

### Architectural Principle: Semantic Ownership

This design follows the principle that PyIceberg must retain ownership of all Iceberg semantics — scan planning, delete resolution, commit protocol — and delegate only raw compute (sort, join, filter) to external libraries. This is the same relationship PyIceberg already has with PyArrow: PyIceberg decides *what* to compute; the library handles the *how*.

This principle was articulated during [community discussion on #3554](https://github.com/apache/iceberg-python/issues/3554#issuecomment-4819685614) and reflects a deliberate architectural boundary: PyIceberg's feature set should not be limited by the pace of any external Rust or Java implementation. By keeping all Iceberg logic in Python, PyIceberg evolves independently while leveraging DataFusion purely for bounded-memory execution.

### Key Pain Points Addressed

- **Data Correctness:** Tables with equality deletes (all Flink-written tables) are completely unreadable by PyIceberg today—a hard `ValueError` is thrown.
- **Out-of-Memory Crashes:** Copy-on-write deletes, upserts, and orphan file deletion all OOM on production-scale data.
- **Missing Maintenance:** Compaction, delete compaction, and sorted writes are not implemented.
- **Production Gap:** Without these capabilities, PyIceberg remains a development tool incapable of production table maintenance.

---

## The Separation Principle

### Formal Decomposition

Any Iceberg table operation `Op` decomposes into two orthogonal concerns:

```
Op = Semantics(Op) ∘ Compute(Op)
```

**Semantics(Op)** — The *what* (Iceberg-spec-aware decisions):
- Which files to read (manifest filtering, partition pruning)
- Which delete files apply to which data files (DeleteFileIndex, sequence number gating)
- What constitutes a valid commit (OCC, snapshot isolation)
- How to route output to partitions (partition spec evaluation)
- Which files to replace atomically (snapshot semantics)

**Compute(Op)** — The *how* (data transformation mechanics):
- Sort N records by key K → external merge sort
- Anti-join relation R against relation S on columns C → hash join
- Filter stream by predicate P → streaming filter

**Theorem (Separation):** `Semantics(Op)` and `Compute(Op)` are independently substitutable. Correctness depends on Semantics being correct; feasibility (completion without OOM) depends on Compute being bounded-memory. Changing the compute backend does not alter semantic correctness.

This is the same factoring Java Iceberg uses: Java owns semantics, Spark/Flink provides compute. We follow the identical pattern: PyIceberg owns semantics, DataFusion provides compute.

### The Interface

```
PyIceberg → DataFusion: (file paths | Arrow batches) × operation descriptor × memory budget
DataFusion → PyIceberg: Arrow batches (result)
```

DataFusion is a **pure function on Arrow data**. It receives data, transforms it, returns data. It has no Iceberg awareness. This is identical to how PyArrow is used today — `pq.read_table(path)`, `pa.Table.sort_by(keys)` — except DataFusion can spill to disk.

---

## Two Distinct Roles of DataFusion in PyIceberg

DataFusion serves two **independent, orthogonal** roles in PyIceberg. These must not be conflated, especially by stakeholders sensitive to "picking favorites" among query engines.

### Role A: Multi-Engine Query Connector (Existing, Unchanged)

The existing `__datafusion_table_provider__` lets end users query Iceberg tables via DataFusion SQL. This is the standard **multi-engine interop pattern** — the same pattern as `to_duckdb()`, `to_ray()`, `to_daft()`, `to_pandas()`.

```python
# User-facing: "I want to query this Iceberg table with DataFusion"
from datafusion import SessionContext
ctx = SessionContext()
ctx.register_table("events", iceberg_table)  # PyCapsule FFI → iceberg-rust TableProvider
ctx.sql("SELECT * FROM events WHERE year = 2024")
```

**How it works internally:**
1. `iceberg_table.__datafusion_table_provider__()` imports `pyiceberg_core.datafusion.IcebergDataFusionTable`
2. Rust's `IcebergStaticTableProvider` (from iceberg-rust) handles scan planning, partition pruning
3. DataFusion executes the query, Rust reads the Parquet files
4. Results return to Python via Arrow C Data Interface

**Packages required:** `datafusion-python` + `pyiceberg-core`
**Iceberg semantics owned by:** iceberg-rust (Rust)
**Status:** Merged, working, unchanged by this proposal

### Role B: Internal Compute Engine (This Proposal, New)

This proposal uses DataFusion **internally** as a compute library — the same way PyIceberg uses PyArrow internally today. Users never interact with DataFusion directly. It is an implementation detail behind `table.compact()`, `table.delete()`, etc.

```python
# Internal PyIceberg code — NOT user-facing
from datafusion import SessionContext, RuntimeEnvBuilder

runtime = RuntimeEnvBuilder().with_fair_spill_pool(512_000_000).with_disk_manager_os()
ctx = SessionContext(runtime=runtime)
ctx.register_parquet("data", file_path)
sorted_result = ctx.sql("SELECT * FROM data ORDER BY timestamp")
```

**Packages required:** `datafusion-python` only (no `pyiceberg-core` needed)
**Iceberg semantics owned by:** PyIceberg Python code
**Status:** This proposal

### Why This Is NOT "Picking DataFusion as THE Query Engine"

PyIceberg remains engine-agnostic for user queries (Role A). Users can query via DataFusion, DuckDB, Polars, Ray, or any engine that supports the PyCapsule/Arrow protocol.

What this IS: PyIceberg choosing an **internal execution strategy** for its own maintenance operations — the same way:
- Java Iceberg uses Spark Actions internally for compaction
- PostgreSQL uses its own internal sort/join algorithms
- CPython uses its own garbage collector

A library's internal implementation details are orthogonal to its external interop interfaces.

### Package and Import Boundaries

| | Role A (existing) | Role B (this proposal) |
|--|---|---|
| **pip extras needed** | `[datafusion]` + `[pyiceberg-core]` | `[datafusion]` only |
| **Imports from `datafusion`** | `SessionContext` | `SessionContext`, `RuntimeEnvBuilder` |
| **Imports from `pyiceberg_core`** | `pyiceberg_core.datafusion.IcebergDataFusionTable` | **None** |
| **DataFusion APIs used** | `register_table()` (PyCapsule FFI) | `register_parquet()`, `register_record_batches()` |
| **Who reads files** | iceberg-rust (Rust, via TableProvider) | DataFusion directly (with object store bridge) |
| **Who owns Iceberg semantics** | iceberg-rust | PyIceberg Python code |
| **DataFusion's Iceberg awareness** | Full (schema, partitions, predicates) | **Zero** (just sees Parquet/Arrow) |

These two roles share the `datafusion-python` pip package but use completely different APIs with zero code overlap.

---

## Why DataFusion — And Why Not Pluggable

### The Elimination Argument

The requirements for the internal compute engine form a conjunction of hard constraints:

```
Required = Apache-licensed
         ∧ Arrow-native (zero-copy interop with PyArrow)
         ∧ Embeddable as library (not a server/database)
         ∧ Single-node execution
         ∧ Spill-to-disk for sort, join, aggregate
         ∧ Python bindings exist and are maintained
```

| Engine | Apache | Arrow-native | Embeddable | Spill | Python | Verdict |
|--------|:---:|:---:|:---:|:---:|:---:|---|
| **DataFusion** | ✅ | ✅ | ✅ | ✅ | ✅ | **Only candidate** |
| DuckDB | ❌ (GPL ext.) | ❌ (copies) | ⚠️ (DB) | ✅ | ✅ | License + format |
| Polars | ⚠️ (MIT) | ✅ | ✅ | ❌ | ✅ | No spill |
| Spark | ✅ | ⚠️ | ❌ (JVM) | ✅ | ✅ | Cannot embed |
| Velox | ⚠️ | ✅ | ✅ | ✅ | ❌ | No Python |
| PyArrow | ✅ | ✅ | ✅ | ❌ | ✅ | No spill (the problem) |

DataFusion is the only library satisfying all constraints. This is not a preference — it is an elimination.

### Why "Pluggable" Is the Anti-Pattern

Making the compute backend pluggable is justified when: (1) multiple viable options exist, (2) the abstraction cost is low, (3) users benefit from choosing. All three are false here:

1. **One candidate exists.** A pluggable interface over one implementation is dead code.
2. **The abstraction cost is prohibitive.** Each engine has completely different APIs for memory configuration, data registration, SQL dialect, object store setup, error handling, and result format. The "pluggable interface" becomes its own query engine — the inner platform anti-pattern.
3. **Users never see or choose the engine.** They call `table.compact()`. The engine is hidden behind Information Hiding (Parnas, 1972).

The existing `to_X()` interop methods (`to_duckdb()`, `to_ray()`, `to_daft()`) already serve users who want to analyze data with their preferred engine. That's a different concern — user query preference vs. internal maintenance implementation.

**If an alternative emerges**, `pyiceberg/execution/compute.py` is a clean substitution point. Building pluggable infrastructure now for alternatives that don't exist violates YAGNI.

### CS Principles Underlying This Decision

| Principle | Application |
|-----------|-------------|
| **Separation of Concerns** (Dijkstra, 1974) | Semantics and Compute are orthogonal, independently substitutable |
| **Information Hiding** (Parnas, 1972) | DataFusion is invisible at the public API boundary; swappable without API changes |
| **Dependency Inversion** (Martin, 1996) | PyIceberg depends on abstract capability (bounded-memory sort/join), not concrete implementation |
| **YAGNI** (Beck, ~1999) | No pluggable abstraction for alternatives that don't exist |
| **Uniform Asymptotic Design** | Every code path handles worst-case scale; no branching on assumed data size |

---

## Background

### The Current State of DataFusion in PyIceberg

| Component | Location | Status | Description |
|:---|:---|:---|:---|
| `__datafusion_table_provider__` | `pyiceberg/table/__init__.py` | **Merged** ✅ | Exposes table as DataFusion `TableProvider` via PyCapsule FFI |
| `IcebergDataFusionTable` | `pyiceberg_core.datafusion` (Rust) | **Merged** ✅ | Static read-only `TableProvider` backed by `IcebergStaticTableProvider` |
| `pyiceberg-core` optional extra | `pyproject.toml` | **Available** ✅ | `pyiceberg-core>=0.5.1,<0.10.0` |
| `datafusion` optional extra | `pyproject.toml` | **Available** ✅ | `datafusion>=52,<53` |
| DataFusion round-trip test | `tests/table/test_datafusion.py` | **Merged** ✅ | Basic: append → register → query → verify |
| Transform delegation | `pyiceberg/transforms.py` | **Merged** ✅ | `pyiceberg_core.transform` for bucket/year/month/etc. |

### What Does NOT Exist (What This Proposal Adds)

| Capability | Impact |
|:---|:---|
| Engine resolution layer | No automatic dispatch between DataFusion and PyArrow |
| Bounded-memory session configuration | No `FairSpillPool`, no `DiskManager`, no memory limit |
| Object store bridge | DataFusion can't access S3/GCS without credential translation |
| Compute functions (sort, anti-join, filter) | No reusable bounded-memory primitives |
| Any equality delete support | Hard `ValueError` in `_plan_files_local()` |

### The Hard Error

```python
# pyiceberg/table/__init__.py (current behavior)
elif data_file.content == DataFileContent.EQUALITY_DELETES:
    raise ValueError(
        "PyIceberg does not yet support equality deletes: "
        "https://github.com/apache/iceberg/issues/6568"
    )
```

### MoR Prerequisite Chain

| Feature | Issue | PR | Status | Relevance |
|:---|:---|:---|:---|:---|
| Concurrency Safety Validations | #819 | #1935, #1938, #2050, #3049 | **Merged** ✅ | Foundation for commit retry |
| Commit Retry with Conflict Validation | #3319 | #3320 | **In Progress** ⏳ | Required for safe compaction commits |
| REPLACE API (metadata-only) | #3130 | #3131 | **Blocked** 🛑 (on #3320) | Required for compaction commit path |
| Data Files Compaction | #1092 | #3124 | **Blocked** 🛑 (on #3131) | End goal: uses DataFusion for sort + rewrite |
| DeleteFileIndex (positional) | N/A | #2918 | **Merged** ✅ | Foundation for equality delete indexing |
| DeleteFileIndex (equality) | #3270 | #3285 | **In Progress** ⏳ | Index plumbing — prerequisite for resolution |

---

## What Is Apache DataFusion?

Apache DataFusion is an extensible query execution framework written in Rust, built on Apache Arrow. It is **not** a database—it is an embeddable execution engine that applications use to run SQL and dataframe operations with full control over memory, parallelism, and I/O.

### How DataFusion Works

```
SQL / DataFrame API
    → LogicalPlan (relational algebra)
        → Optimizer (predicate pushdown, join reorder)
            → PhysicalPlan (ExecutionPlan tree)
                → Execution (Tokio async, multi-threaded, GIL released)
                    → Stream[RecordBatch] (output)
```

**The key capability**: When a physical operator (sort, hash join, aggregate) needs more memory than its budget allows, it **spills intermediate state to local disk** and continues. Managed by `FairSpillPool` which divides available memory evenly among concurrent operators.

### Memory Management Architecture

1. **MemoryPool** (trait): Tracks total usage, fails `try_grow()` when limit reached
2. **MemoryReservation** (RAII): Each operator holds a reservation; on failure → spill
3. **SpillManager** → **DiskManager**: Writes Arrow IPC to temp files, reads back during merge

We use **`FairSpillPool`** — divides memory evenly among spillable operators:
```
∀i ∈ spillable_operators: reservation_i ≤ (Pool_size - Unspillable) / |spillable_operators|
```

This prevents starvation when multiple operators (sort + join) run concurrently in a single plan.

---

## The OOM Problem: Comprehensive Inventory

| # | Operation | Current Status | OOM Pattern | DataFusion Solution |
|:---|:---|:---|:---|:---|
| 1 | **Equality delete reads** | ❌ Hard `ValueError` | Anti-join requires all delete keys in memory | Grace Hash Join with spill |
| 2 | **CoW delete (file rewrite)** | ✅ Works but OOMs | Loads entire Parquet file (~1GB) into memory | Streaming `FilterExec` pipeline |
| 3 | **Upsert (MERGE INTO)** | ✅ Works but O(n²) + OOM | Row-by-row comparison; `concat_tables` accumulation | Hash join with spill |
| 4 | **Data compaction** | ❌ Not implemented | External sort of arbitrarily large datasets | External merge sort with spill |
| 5 | **Orphan file deletion** | ⏳ In progress (#1200) | LEFT ANTI JOIN of millions of paths | Hash anti-join with spill |
| 6 | **Eq-to-positional conversion** | ❌ Not implemented | INNER JOIN of data × eq_deletes | Hash join with spill |
| 7 | **Full MoR compaction** | ❌ Not implemented | Compose: join + sort + rewrite | Pipelined anti-join → sort → write |
| 8 | **Dynamic partition overwrite** | ✅ Works but OOM risk | Full materialization for partition detection | Hash aggregate with spill |
| 9 | **Sort-order enforcement** | ⚠️ Partial | Full sort before write | External merge sort |

### The OOM Code Paths Today

```python
# Equality deletes — hard error
raise ValueError("PyIceberg does not yet support equality deletes")

# CoW delete — full file materialization
for original_file in files:
    df = ArrowScan(...).to_table(tasks=[original_file])  # FULL FILE IN MEMORY
    filtered_df = df.filter(preserve_row_filter)

# Upsert — O(n²) row-by-row Python loop
for source_idx, target_idx in zip(source_indices, target_indices):
    source_row = source_table.slice(source_idx, 1)  # per-row slice
    target_row = target_table.slice(target_idx, 1)
    for key in non_key_cols:
        if source_row[key][0].as_py() != target_row[key][0].as_py():
            to_update_indices.append(source_idx)
```

---

## Design: DuckDB-Style UX

### Core Principle

Users never choose an engine. They optionally configure a memory budget. The system automatically uses DataFusion when installed and falls back to PyArrow when not.

```python
# Zero-config — just works
table.compact()
table.delete("category = 'spam'")
result = table.scan().to_arrow()  # transparently resolves equality deletes

# Power user — tune the budget
table.compact(memory_limit="2GB")
```

### Engine Resolution

```python
# pyiceberg/execution/engine.py
from enum import Enum, auto
from functools import lru_cache

class ExecutionEngine(Enum):
    DATAFUSION = auto()   # Bounded memory via spill-to-disk
    PYARROW = auto()      # In-memory only (fallback)

@lru_cache(maxsize=1)
def _probe_backend() -> ExecutionEngine:
    try:
        import datafusion  # noqa: F401
        return ExecutionEngine.DATAFUSION
    except ImportError:
        return ExecutionEngine.PYARROW

def resolve_engine(operation: str) -> ExecutionEngine:
    engine = _probe_backend()
    if engine == ExecutionEngine.PYARROW:
        warnings.warn(
            f"'{operation}' will use in-memory (PyArrow) execution which may OOM. "
            f"For bounded-memory execution: pip install 'pyiceberg[datafusion]'",
            UserWarning, stacklevel=2,
        )
    return engine
```

**Note:** We check `import datafusion` (the Python compute package), NOT `import pyiceberg_core`. The `pyiceberg-core` package is for transforms and the TableProvider connector (Role A) — unrelated to bounded-memory compute (Role B).

### Install Path

```
pip install 'pyiceberg[datafusion]'    # Enables bounded-memory compute
```

This extra already exists in `pyproject.toml`: `datafusion = ["datafusion>=52,<53"]`.

---

## Architecture

### The Boundary

```
┌─────────────────────────────────────────────────────────────┐
│  PyIceberg (Python) — ALL Iceberg semantics                 │
│  • Scan planning (manifest filtering, partition pruning)    │
│  • DeleteFileIndex (sequence number gating)                 │
│  • File selection (compaction bin-packing)                  │
│  • Partition routing, schema reconciliation                 │
│  • Transaction + commit (OCC, atomic snapshot)              │
│  • File writing (_dataframe_to_data_files)                 │
└────────────────────────┬────────────────────────────────────┘
                         │ file paths / Arrow batches
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  DataFusion (via datafusion-python) — compute only          │
│  • sort(files, keys, memory_limit) → sorted batches         │
│  • anti_join(left, right, cols, memory_limit) → filtered    │
│  • filter(file, predicate) → matching rows                  │
│  Knows NOTHING about Iceberg.                               │
└─────────────────────────────────────────────────────────────┘
```

### Consistency with Semantic Ownership Principle

The design principle states DataFusion should be used "much like how PyArrow is used today." This means DataFusion reads files directly — the same way PyArrow does:

```python
# How PyArrow is used today:
pq.read_table(file_path)  # PyArrow reads the file, not PyIceberg

# How DataFusion will be used:
ctx.register_parquet("data", file_path)  # DataFusion reads the file, not PyIceberg
```

"PyIceberg handles everything" means making Iceberg-semantic decisions (which files, what logic, how to commit) — not doing byte-level I/O. The compute library reads files directly; PyIceberg tells it *where* to read and *what* to compute.

### Object Store Bridge

DataFusion needs object store credentials to read from S3/GCS/ADLS. PyIceberg already has these in its `FileIO` properties. The bridge translates:

```python
# pyiceberg/execution/object_store.py
def configure_object_store(ctx: SessionContext, io_properties: dict[str, str]) -> None:
    """One-time translation of PyIceberg FileIO props → DataFusion object store config."""
    if any(k.startswith("s3.") for k in io_properties):
        _configure_s3(ctx, io_properties)
    elif any(k.startswith("gcs.") for k in io_properties):
        _configure_gcs(ctx, io_properties)
    # Local filesystem: no config needed
```

### Design Principle: Always Let DataFusion Read

DataFusion handles all file reading for all operations. No branching on assumed data size. When data is small, no spill occurs (negligible overhead). When data is large, spill activates transparently.

This eliminates the class of "works in dev, OOMs in prod" bugs caused by size-prediction logic.

### Metadata Streaming

For operations that enumerate large metadata sets (orphan file deletion: all paths across all snapshots), the semantic layer streams manifest entries through a generator → temp Parquet file → register with DataFusion. Python memory stays at O(batch_size) regardless of total metadata volume.

```python
# Stream metadata to temp Parquet — constant memory, any scale
for entry in manifest_generator:
    buffer.append(entry.file_path)
    if len(buffer) >= batch_size:
        writer.write_batch(pa.record_batch([pa.array(buffer)], names=["path"]))
        buffer.clear()
# Then: ctx.register_parquet("valid_paths", tmp_file)
```

New operations use this streaming pattern from day one. Existing operations (scan planning, compaction file selection) can be migrated incrementally as follow-up work — they are partition-scoped and fit in memory for typical tables.

---

## Module Layout

```
pyiceberg/execution/
├── __init__.py          # Re-exports ExecutionEngine, resolve_engine
├── engine.py            # resolve_engine() — checks `import datafusion`
├── session.py           # create_bounded_session() — FairSpillPool + DiskManager
├── object_store.py      # Translate FileIO props → DataFusion object store
└── compute.py           # sort_batches(), anti_join(), filter_parquet()
```

### `session.py` — Bounded Session Creation

```python
def create_bounded_session(memory_limit: str | int | None = None) -> SessionContext:
    """Create a DataFusion SessionContext with bounded memory and spill-to-disk."""
    from datafusion import SessionContext, RuntimeEnvBuilder

    limit_bytes = parse_memory_limit(memory_limit)  # "512MB" → 536_870_912
    runtime = RuntimeEnvBuilder().with_fair_spill_pool(limit_bytes).with_disk_manager_os()
    return SessionContext(runtime=runtime)
```

### `compute.py` — Pure Compute Functions

```python
def anti_join(left: pa.Table, right: pa.Table, on: list[str],
              memory_limit: str | None = None) -> pa.Table:
    """LEFT ANTI JOIN with bounded memory (Grace Hash Join with spill)."""
    ctx = create_bounded_session(memory_limit)
    ctx.register_record_batches("left_tbl", [[b for b in left.to_batches()]])
    ctx.register_record_batches("right_tbl", [[b for b in right.to_batches()]])
    join_cond = " AND ".join(f"l.{c} = r.{c}" for c in on)
    return ctx.sql(f"SELECT l.* FROM left_tbl l LEFT ANTI JOIN right_tbl r ON {join_cond}").to_arrow_table()

def sort_batches(data: pa.Table, sort_keys: list[str],
                 memory_limit: str | None = None) -> pa.Table:
    """External merge sort with bounded memory."""
    ctx = create_bounded_session(memory_limit)
    ctx.register_record_batches("data", [[b for b in data.to_batches()]])
    return ctx.sql(f"SELECT * FROM data ORDER BY {', '.join(sort_keys)}").to_arrow_table()

def filter_parquet(file_path: str, predicate_sql: str,
                   memory_limit: str | None = None,
                   io_properties: dict | None = None) -> pa.Table:
    """Streaming filter of a Parquet file with O(batch_size) memory."""
    ctx = create_bounded_session(memory_limit)
    if io_properties:
        configure_object_store(ctx, io_properties)
    ctx.register_parquet("source", file_path)
    return ctx.sql(f"SELECT * FROM source WHERE {predicate_sql}").to_arrow_table()
```

---

## Relationship to Java Iceberg: Semantic Parity vs. Algorithmic Freedom

### The Three Categories

| Category | Rule | Examples |
|----------|------|---------|
| **Spec semantics** (MUST match) | Iceberg spec invariants — violating produces incorrect tables | Sequence number gating, equality delete semantics, partition spec eval |
| **Strategy heuristics** (SHOULD match) | Implementation choices users expect — not spec-mandated but behavioral | BinPack file selection, target file size defaults, z-order bit interleaving formula |
| **Execution algorithms** (MAY differ) | Compute mechanism — depends on deployment model | Grace Hash Join vs. broadcast join, local merge sort vs. distributed shuffle |

Java uses distributed algorithms (Spark shuffle, broadcast join). We use single-node bounded-memory algorithms (DataFusion external merge sort, Grace Hash Join with spill). Same relational semantics, different execution model.

**Key exception: Z-Order.** The bit-interleaving formula IS the algorithm. Must be bitwise-identical to Java for cross-engine compatibility of clustered file layouts.

---

## OOM Safety Guarantee

Every operation follows the same safety contract:

```
BEFORE: Table is in valid state S₀
DURING: New data files written to storage (orphans until commit)
ON SUCCESS: Atomic commit → snapshot S₁
ON FAILURE: No commit → table remains at S₀ (new files are orphans)
```

Iceberg's OCC guarantees that a mid-operation failure (OOM, disk full, network error) cannot corrupt table state. This holds regardless of compute backend.

---

## Speed-of-Light Analysis

### FFI Overhead

Arrow C Data Interface is zero-copy. For 10GB of data at 8192 rows/batch:
```
T_ffi = 12,500 pointer handoffs × ~50ns each ≈ 0.6 ms
T_read = 10GB / 7GB/s (NVMe) ≈ 1,400 ms
T_ffi / T_read = 0.00043 (0.04% — negligible)
```

### Write Path

PyIceberg writes output files via PyArrow's `ParquetWriter` (same as today). This is ~30% slower than Rust's `IcebergWriteExec` for the encoding step, but dominated by I/O (S3 ~100MB/s per stream). Acceptable tradeoff for architectural independence.

### Formal Performance Bound

For compute-bound operations (sort, join): `T_actual / T_theoretical_min → 1` as data size grows. Both the Python-orchestrated and hypothetical Rust-orchestrated approaches use identical DataFusion execution. The difference is only in the write path, which is I/O-bound.

---

## Implementation Roadmap

### Dependency Graph

```
Foundation (1, 2, 3) ─┬─→ Upsert fix (4)
                      ├─→ Equality delete reads (5) → Eq-to-Pos (9)
                      ├─→ Orphan file deletion (6)
                      ├─→ CoW delete/overwrite (7)
                      └─→ Compaction (8) → Pos delete compact (10) → Z-Order (11)
```

### Ordered Checklist

| # | Title | Depends On | Blockers |
|:---|:---|:---|:---|
| 1 | Engine resolution module | Nothing | None |
| 2 | Bounded-session helpers | Nothing | None |
| 3 | Object store bridge | Nothing | None |
| 4 | Upsert via hash join | 1, 2 | None |
| 5 | Equality delete resolution | 1, 2, 3 | #3285 (DeleteFileIndex) |
| 6 | Orphan file deletion | 1, 2 | None |
| 7 | Streaming CoW delete/overwrite | 1, 2, 3 | None |
| 8 | Table compaction | 1, 2, 3 | #3131 (REPLACE API) |
| 9 | Eq-to-positional conversion | 5 | None |
| 10 | Position delete compaction | 8 | None |
| 11 | Z-Order sorting | 8 | None |
| 12 | Full MoR compaction | 5, 8, 10 | None |

**Key observation: No iceberg-rust blockers.** All work lives in this repo. Items 4 and 6 can start immediately with zero upstream dependencies.

### Parallelizable Work

After foundation (1-3), items 4–8 are all independent and can be developed in parallel.

---

## Feature Interactions

### With Commit Retry (#3319 / #3320)

DataFusion-powered operations produce new data files then commit atomically. If the commit fails due to concurrent modification, the retry mechanism allows re-validation without re-executing compute.

### With REPLACE API (#3130 / #3131)

Compaction uses REPLACE (remove old files, add new files atomically). The REPLACE API provides the commit path that DataFusion sort results flow into.

### With DeleteFileIndex (#2918 / #3285)

The equality delete resolver is triggered by `DeleteFileIndex`. When the index identifies applicable delete files for a data file, it passes both to DataFusion for anti-join resolution.

### With Schema Evolution

Equality deletes reference columns by `equality_ids` (field IDs). PyIceberg reconciles schemas (project both data and delete files to common columns) before handing to DataFusion. DataFusion never sees field IDs — it sees matching column names.

### With V3 Deletion Vectors

DV compaction reuses the streaming CoW rewrite infrastructure. DVs themselves are O(KB) bitmaps; only the data rewrite needs DataFusion's bounded-memory execution.

---

## Open Questions

1. **Object store config completeness**: Does `datafusion-python` expose all necessary object store configuration (STS tokens, custom endpoints, assume-role)? Need to audit.

2. **Streaming vs. batch resolution**: Should equality deletes be resolved per-file (small anti-joins, many invocations) or per-partition (larger anti-joins, fewer invocations)?

3. **Memory limit default**: 512MB (safe on 8GB machines) or percentage of available RAM? Configurable via `.pyiceberg.yaml`?

4. **datafusion-python version stability**: The `RuntimeEnvBuilder` API and object store registration API — are they stable across minor versions?

---

## Goals

- **Equality delete reads**: `table.scan().to_arrow()` returns correct results on all V2 tables
- **Bounded-memory compaction**: `table.compact()` operates on 100GB+ with 512MB budget
- **No existing regressions**: All tests pass without `datafusion` installed
- **Production-ready maintenance**: Orphan deletion, delete compaction handle 10M+ file tables
- **Zero-config UX**: Everything works with sensible defaults

## Non-Goals

- **Distributed execution**: Single-node only. DataFusion provides vertical scaling via disk.
- **Replace PyArrow for I/O**: PyArrow remains for Parquet writing, schema operations, streaming reads.
- **Force dependency**: DataFusion is never required. PyArrow fallback always exists.
- **Pluggable engine abstraction**: Not needed (one viable candidate, users don't choose).

---

## Related Issues

### PyIceberg (iceberg-python)

| Issue/PR | Title | Relevance |
|:---|:---|:---|
| [#3554](https://github.com/apache/iceberg-python/issues/3554) | EPIC: Integrate DataFusion as execution engine | This proposal |
| [#1078](https://github.com/apache/iceberg-python/issues/1078) | MoR support epic | Umbrella |
| [#1210](https://github.com/apache/iceberg-python/issues/1210) / [#3270](https://github.com/apache/iceberg-python/issues/3270) | Equality delete reads | Critical feature |
| [PR #3285](https://github.com/apache/iceberg-python/pull/3285) | DeleteFileIndex for equality deletes | Prerequisite |
| [#3356](https://github.com/apache/iceberg-python/issues/3356) | Execution path isolation | Architectural alignment |
| [#1092](https://github.com/apache/iceberg-python/issues/1092) | Data compaction | End goal |
| [#1200](https://github.com/apache/iceberg-python/issues/1200) | Orphan file deletion | End goal |
| [#3319](https://github.com/apache/iceberg-python/issues/3319) / [PR #3320](https://github.com/apache/iceberg-python/pull/3320) | Commit retry | Prerequisite for compaction |
| [#3130](https://github.com/apache/iceberg-python/issues/3130) / [PR #3131](https://github.com/apache/iceberg-python/pull/3131) | REPLACE API | Prerequisite for compaction |
| [#2918](https://github.com/apache/iceberg-python/pull/2918) | DeleteFileIndex (positional) | Merged foundation |

### datafusion-python

| Issue | Title | Relevance |
|:---|:---|:---|
| [#1217](https://github.com/apache/datafusion-python/issues/1217) | FFI boundary stability | PyCapsule protocol reliability |

### iceberg-rust (informational — no contributions needed)

| Issue | Title | Note |
|:---|:---|:---|
| [#2716](https://github.com/apache/iceberg-rust/issues/2716) | Bounded-memory execution for pyiceberg-core | **To be closed** — superseded by this approach |
| [#2717](https://github.com/apache/iceberg-rust/issues/2717) | Bounded-memory session helper | **To be closed** |
| [#2718](https://github.com/apache/iceberg-rust/issues/2718) | `pyiceberg_core.execution` module | **To be closed** |

---

## References

- `pyiceberg_datafusion/datafusion_direction.md` — Architectural pivot analysis
- `pyiceberg_datafusion/issue_3554_updated_description_v2.md` — Updated GitHub issue body
- `pyiceberg_datafusion/tracking.md` — Full operations and dependency tracking
