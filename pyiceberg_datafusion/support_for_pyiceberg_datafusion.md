# Support for PyIceberg DataFusion Integration

[qzyu999@gmail.com](mailto:qzyu999@gmail.com)

---

## Introduction

Apache PyIceberg faces a class of compute-heavy operations that cannot be implemented correctly under bounded memory using PyArrow alone. These operations—equality delete resolution, data compaction, upsert, orphan file deletion—are fundamental to production Iceberg table management but require execution-engine capabilities (spill-to-disk, hash joins, external sorts) that PyArrow structurally cannot provide.

Apache DataFusion, a Rust-based query execution framework built on Arrow, provides exactly these capabilities. This document proposes a comprehensive integration of DataFusion into PyIceberg as an optional, automatically-resolved execution backend for all compute-heavy workloads. The design follows a DuckDB-style UX: users configure a memory budget, not an execution strategy. DataFusion is used when available; PyArrow remains the fallback for small data.

This integration is not optional for feature parity with Java Iceberg. Java delegates compute to Spark/Flink. Python has no equivalent unless we build one.

### Key Pain Points Addressed

- **Data Correctness:** Tables with equality deletes (all Flink-written tables) are completely unreadable by PyIceberg today—a hard `ValueError` is thrown.
- **Out-of-Memory Crashes:** Copy-on-write deletes, upserts, and orphan file deletion all OOM on production-scale data.
- **Missing Maintenance:** Compaction, delete compaction, and sorted writes are not implemented.
- **Production Gap:** Without these capabilities, PyIceberg remains a development tool incapable of production table maintenance.

---

## Background

### The Current State of DataFusion in PyIceberg

PyIceberg already has a partial DataFusion integration via `pyiceberg-core` (Rust bindings from `iceberg-rust`):

| Component | Location | Status | Description |
|:---|:---|:---|:---|
| `__datafusion_table_provider__` | `pyiceberg/table/__init__.py` | **Merged** ✅ | Exposes table as DataFusion `TableProvider` via PyCapsule FFI |
| `IcebergDataFusionTable` | `pyiceberg_core.datafusion` (Rust) | **Merged** ✅ | Static read-only `TableProvider` backed by `IcebergStaticTableProvider` |
| `pyiceberg-core` optional extra | `pyproject.toml` | **Available** ✅ | `pyiceberg-core>=0.5.1,<0.10.0` |
| `datafusion` optional extra | `pyproject.toml` | **Available** ✅ | `datafusion>=52,<53` |
| DataFusion round-trip test | `tests/table/test_datafusion.py` | **Merged** ✅ | Basic: append → register → query → verify |
| Transform delegation | `pyiceberg/transforms.py` | **Merged** ✅ | `pyiceberg_core.transform` for bucket/year/month/etc. |

### What Does NOT Exist

| Capability | Impact |
|:---|:---|
| Engine resolution layer | No automatic dispatch between DataFusion and PyArrow |
| Memory-configured execution | SessionContext uses defaults (no spill) |
| Write-capable provider via FFI | Only read-only `IcebergStaticTableProvider` exposed |
| Delete file resolution | `IcebergDataFusionTable` ignores delete files entirely |
| `pyiceberg_core.execution` module | No FFI entry point for bounded-memory operations |
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

### Related Background: MoR Prerequisite Chain

The DataFusion integration builds on top of ongoing MoR and maintenance work. Key prerequisites:

| Feature | Issue | PR | Status | Relevance |
|:---|:---|:---|:---|:---|
| Concurrency Safety Validations | #819 | #1935, #1938, #2050, #3049 | **Merged** ✅ | Foundation for commit retry |
| Commit Retry with Conflict Validation | #3319 | #3320 | **In Progress** ⏳ | Required for RowDelta and safe compaction commits |
| REPLACE API (metadata-only) | #3130 | #3131 | **Blocked** 🛑 (on #3320) | Required for compaction commit path |
| Data Files Compaction | #1092 | #3124 | **Blocked** 🛑 (on #3131) | End goal: uses DataFusion for sort + rewrite |
| Metadata Compaction | #270 | #1661 | **Blocked** 🛑 (on #3131) | End goal: manifest merge |
| DeleteFileIndex (positional) | N/A | #2918 | **Merged** ✅ | Foundation for equality delete indexing |
| DeleteFileIndex (equality) | #3270 | #3285 | **In Progress** ⏳ | Index plumbing without resolution |

---

## What Is Apache DataFusion?

Apache DataFusion is an extensible query execution framework written in Rust, built on Apache Arrow. It is **not** a database—it is an embeddable execution engine that applications use to run SQL and dataframe operations with full control over memory, parallelism, and I/O.

### Why DataFusion Specifically?

| Requirement | DataFusion Property |
|:---|:---|
| Arrow-native (zero-copy with PyArrow) | Operates directly on Arrow `RecordBatch` |
| Spill-to-disk (the core feature) | `FairSpillPool` + `DiskManager` for bounded memory |
| GIL bypass | Rust + Tokio: true parallelism |
| Iceberg integration exists | `iceberg-datafusion` crate already implements `TableProvider` |
| Apache ecosystem | Same governance as Iceberg and Arrow (Apache 2.0) |
| Already partially integrated | `pyiceberg-core` bridges Rust↔Python via PyO3 |

### Why Not Alternatives?

| Alternative | Why Insufficient |
|:---|:---|
| **DuckDB** | Database (not composable library), GPL-licensed extensions, no Iceberg `TableProvider`, copies data at boundary |
| **Polars** | No spill-to-disk for joins/sorts, no `TableProvider` extensibility, not Apache-licensed |
| **Spark** | Requires JVM + cluster, 10s startup, cannot embed in a Python library |
| **Velox** | No Python bindings, no Iceberg integration, not Apache-licensed |
| **PyArrow** | No memory management, no spill-to-disk, no join operators, no query planning |
| **Ray** | Massive dependency, cluster-oriented, no built-in external-memory operators |

### How DataFusion Works

DataFusion transforms SQL/DataFrame operations into streaming physical execution plans:

```
SQL / DataFrame API
    → LogicalPlan (relational algebra)
        → Optimizer (predicate pushdown, join reorder)
            → PhysicalPlan (ExecutionPlan tree)
                → Execution (Tokio async, multi-threaded)
                    → Stream[RecordBatch] (output)
```

**The key capability**: When a physical operator (sort, hash join, aggregate) needs more memory than its budget allows, it **spills intermediate state to local disk** and continues. This is managed by `FairSpillPool` which divides available memory evenly among concurrent operators. The result: any operation completes with bounded memory, using disk as overflow.

### Memory Management Architecture

DataFusion's memory system has three layers:

1. **MemoryPool** (trait): Tracks total usage, fails `try_grow()` when limit reached
2. **MemoryReservation** (RAII): Each operator holds a reservation; on failure → spill
3. **SpillManager** → **DiskManager**: Writes Arrow IPC to temp files, reads back during merge

Three pool implementations:
- `UnboundedMemoryPool` — no limit (default, dangerous)
- `GreedyMemoryPool` — first-come-first-served hard limit
- **`FairSpillPool`** — divides memory evenly among spillable operators (our choice)

---

## The OOM Problem: Comprehensive Inventory

### Operations Blocked or Degraded by Memory Constraints

| # | Operation | Current Status | OOM Scenario | DataFusion Solution |
|:---|:---|:---|:---|:---|
| 1 | **Equality delete reads** | ❌ Hard `ValueError` | Anti-join requires all delete keys in memory | Grace Hash Join with spill |
| 2 | **CoW delete (file rewrite)** | ✅ Works but OOMs | Loads entire Parquet file (~1GB) into memory | Streaming `FilterExec` pipeline |
| 3 | **Upsert (MERGE INTO)** | ✅ Works but O(n²) + OOM | Row-by-row comparison; `concat_tables` accumulation | Hash join with spill |
| 4 | **Data compaction** | ❌ Not implemented | External sort of arbitrarily large datasets | External merge sort with spill |
| 5 | **Orphan file deletion** | ⏳ In progress (#1200) | LEFT ANTI JOIN of millions of paths | Hash anti-join with spill |
| 6 | **Eq-to-positional conversion** | ❌ Not implemented | INNER JOIN of data × eq_deletes for position extraction | Hash join with spill |
| 7 | **Full MoR compaction** | ❌ Not implemented | Compose: join + sort + rewrite | Pipelined anti-join → sort → write |
| 8 | **Dynamic partition overwrite** | ✅ Works but OOM risk | Full materialization for partition detection | Hash aggregate with spill |
| 9 | **Sort-order enforcement** | ⚠️ Partial | Full sort before write | External merge sort |

### The OOM Code Paths Today

**Equality deletes** — hard error:
```python
# pyiceberg/table/__init__.py:~2190
raise ValueError("PyIceberg does not yet support equality deletes")
```

**CoW delete** — full file materialization:
```python
# Transaction.delete()
for original_file in files:
    df = ArrowScan(...).to_table(tasks=[original_file])  # FULL FILE IN MEMORY
    filtered_df = df.filter(preserve_row_filter)
```

**Upsert** — O(n²) row-by-row Python comparison:
```python
# upsert_util.py
for source_idx, target_idx in zip(source_indices, target_indices):
    source_row = source_table.slice(source_idx, 1)  # per-row slice!
    target_row = target_table.slice(target_idx, 1)
    for key in non_key_cols:
        if source_row[key][0].as_py() != target_row[key][0].as_py():
            to_update_indices.append(source_idx)
```

---

## Design: Automatic Engine Resolution (DuckDB-Style UX)

### Core Principle

Users never choose an engine. They configure a memory budget. The system automatically uses DataFusion when available and falls back to PyArrow when not.

```python
# User experience — zero config needed
table.compact()
table.delete("category = 'spam'")
result = table.scan().to_arrow()  # transparently resolves equality deletes

# Power user — tune the budget
table.compact(memory_limit="2GB")
```

### Engine Resolution Logic

```python
# pyiceberg/execution/engine.py (new module)
def resolve_engine(operation: str) -> ExecutionEngine:
    try:
        import pyiceberg_core
        return ExecutionEngine.DATAFUSION
    except ImportError:
        warnings.warn(
            f"'{operation}' will use in-memory (PyArrow) execution. "
            f"For large tables: pip install 'pyiceberg[pyiceberg-core]'",
            stacklevel=3,
        )
        return ExecutionEngine.PYARROW
```

### Why All Operations Can Be "Auto" (Not "Required")

Every operation has a correct PyArrow fallback for small data. If it OOMs, no corruption occurs because:

1. **Reads are side-effect-free.** An OOM during a scan modifies nothing.
2. **Writes use atomic commits.** Iceberg's OCC guarantees no snapshot is committed until success. Partial writes become orphans.
3. **PyArrow is correct for small data.** 100 equality delete rows? `pc.is_in()` handles it in 1MB.

The user's recourse on OOM is clear: `pip install 'pyiceberg[pyiceberg-core]'`.

### Decision Matrix

| Operation | PyArrow Fallback? | Engine Policy |
|:---|:---|:---|
| Equality delete reads | Yes (small delete sets) | Auto |
| Compaction | Yes (small tables) | Auto |
| Eq-to-positional conversion | Yes (small tables) | Auto |
| Orphan file deletion | Yes (small file counts) | Auto |
| CoW delete/overwrite | Yes (with warning) | Auto |
| Upsert | Yes (with warning) | Auto |
| Append / simple scan | Yes (no warning) | No change |

---

## Architecture: What Needs to Change

### Layer 1: New Python Module (`pyiceberg/execution/`)

```
pyiceberg/execution/
├── __init__.py
├── engine.py              # resolve_engine()
└── operations/
    ├── cow_rewrite.py     # Streaming CoW via DataFusion or PyArrow
    ├── compact.py         # Compaction via DataFusion
    ├── equality_resolve.py # Anti-join resolution
    ├── upsert.py          # Hash join + partition route
    └── orphan_delete.py   # Path anti-join
```

**Key property**: Zero DataFusion imports at module level. All imports are lazy.

### Layer 2: Rust FFI Module (`pyiceberg_core.execution`)

New functions exposed to Python via PyO3:

```python
def execute_cow_rewrite(
    metadata_location: str,
    file_io_properties: dict[str, str],
    files_to_rewrite: list[str],
    filter_expression: str,
    keep_matching: bool,
    memory_limit: str | None = None,
) -> CowRewriteResult: ...

def execute_compaction(
    metadata_location: str,
    file_io_properties: dict[str, str],
    files_to_compact: list[str],
    target_file_size_bytes: int,
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
```

### Layer 3: Minimal iceberg-rust Changes

| Change | Description | Status |
|:---|:---|:---|
| Memory-configurable session | `create_bounded_session()` with `FairSpillPool` | New (needed) |
| `IcebergOverwriteCommitExec` | Atomic file-replace commit for compaction/CoW | New (needed) |
| Expose execution module via PyO3 | Register new submodule in `pyiceberg-core` | New (needed) |

### Layer 4: Minimal Existing Code Changes

```python
# pyiceberg/table/__init__.py — ONLY these touchpoints change:

# 1. Remove ValueError, index equality deletes
elif data_file.content == DataFileContent.EQUALITY_DELETES:
    delete_index.add_delete_file(manifest_entry, partition_key=data_file.partition)

# 2. Add DataFusion dispatch in Transaction.delete()
if delete_snapshot.rewrites_needed:
    engine = resolve_engine("cow_delete")
    if engine == ExecutionEngine.DATAFUSION:
        return self._cow_delete_datafusion(...)

# 3. New methods (purely additive):
def compact(self, ...) -> None: ...
def rewrite_position_deletes(self, ...) -> None: ...
def delete_orphan_files(self, ...) -> list[str]: ...
```

**Total impact**: < 50 lines modified in existing files, ~200 lines added in new modules.

---

## Two-Track Integration Strategy

Given that iceberg-rust's native MoR support (#2186, #2205) is not yet complete, we pursue two parallel tracks:

### Track 1: Python-Side DataFusion (Immediate)

Use `datafusion-python` directly. No iceberg-rust changes needed.

```python
from datafusion import SessionContext, RuntimeEnvBuilder

# Create memory-bounded session
runtime = RuntimeEnvBuilder().with_fair_spill_pool(512_000_000).with_disk_manager_os()
ctx = SessionContext(runtime=runtime)

# Register files identified by PyIceberg's scan planning
ctx.register_parquet("data", "s3://bucket/data/file-001.parquet")
ctx.register_parquet("deletes", "s3://bucket/data/eq-del-001.parquet")

# Execute anti-join with spill-to-disk
result = ctx.sql("SELECT d.* FROM data d LEFT ANTI JOIN deletes e ON d.id = e.id")
```

**Advantages**: Works today, no upstream dependencies, full spill support.
**Disadvantages**: No Iceberg-aware partition pruning, object store config needed separately.

### Track 2: Rust-Side DataFusion (Long-term, Optimal)

Execute entire plans in Rust below the GIL via `pyiceberg_core.execution`.

```python
from pyiceberg_core.execution import execute_compaction

result = execute_compaction(
    metadata_location=self.metadata_location,
    file_io_properties=self.io.properties,
    files_to_compact=[serialize(f) for f in files],
    target_file_size_bytes=256_000_000,
    sort_columns=["timestamp", "id"],
    memory_limit="512MB",
)
```

**Advantages**: Full Iceberg-aware execution, no FFI data transfer overhead, optimal performance.
**Disadvantages**: Requires new Rust code in `pyiceberg-core` and `iceberg-rust`.

### Track Equivalence

Both tracks produce identical results for all inputs. The difference is performance and architectural cleanliness. Track 1 can be deprecated once Track 2 is complete.

---

## Existing Issues and PRs (Cross-Repository)

### PyIceberg (iceberg-python)

| Issue/PR | Title | Relevance |
|:---|:---|:---|
| [#1078](https://github.com/apache/iceberg-python/issues/1078) | MoR support epic | Umbrella for all MoR read/write |
| [#1210](https://github.com/apache/iceberg-python/issues/1210) | Equality delete read support | Primary blocking issue |
| [#3270](https://github.com/apache/iceberg-python/issues/3270) | Equality delete support (data correctness) | Continuation of #1210 |
| [PR #3285](https://github.com/apache/iceberg-python/pull/3285) | DeleteFileIndex for equality deletes | WIP: index plumbing without resolution |
| [PR #2918](https://github.com/apache/iceberg-python/pull/2918) | DeleteFileIndex for positional deletes | **Merged** — foundation for #3285 |
| [#3356](https://github.com/apache/iceberg-python/issues/3356) | Execution path isolation | Keep DataFusion cleanly separated |
| [#3122](https://github.com/apache/iceberg-python/discussions/3122) | PyArrow materialization limits | Documents OOM patterns |
| [PR #2676](https://github.com/apache/iceberg-python/pull/2676) | PyArrow OOM mitigation | Related to worker materialization |
| [#1818](https://github.com/apache/iceberg-python/issues/1818) | V3 (deletion vectors) tracking | DV read/write |
| [#3319](https://github.com/apache/iceberg-python/issues/3319) | Commit retry + conflict validation | Prerequisite for RowDelta |
| [PR #3320](https://github.com/apache/iceberg-python/pull/3320) | Commit retry implementation | In progress |
| [#3130](https://github.com/apache/iceberg-python/issues/3130) | REPLACE API | Required for compaction commit |
| [PR #3131](https://github.com/apache/iceberg-python/pull/3131) | REPLACE API implementation | Blocked on #3320 |
| [#1092](https://github.com/apache/iceberg-python/issues/1092) | Data compaction | Blocked on #3131 |
| [#1200](https://github.com/apache/iceberg-python/issues/1200) | Orphan file deletion | In-progress, OOM risk |
| [PR #2075](https://github.com/apache/iceberg-python/pull/2075) | DataFusion-related scan work | Historical context |
| [PR #2928](https://github.com/apache/iceberg-python/pull/2928) | DataFusion-related integration | Historical context |

### iceberg-rust

| Issue | Title | Relevance |
|:---|:---|:---|
| [#2186](https://github.com/apache/iceberg-rust/issues/2186) | MoR scan-side delete reconciliation | Long-term: native delete resolution in TableProvider |
| [#2205](https://github.com/apache/iceberg-rust/issues/2205) | Equality delete reader | Rust-native anti-join |
| [#2201](https://github.com/apache/iceberg-rust/issues/2201) | Positional delete reader | Rust-native pos delete |
| [#1530](https://github.com/apache/iceberg-rust/issues/1530) | Delete file support in scan | Core primitive |
| [#2269](https://github.com/apache/iceberg-rust/issues/2269) | DataFusion write actions (MERGE/UPDATE) | Write path through DataFusion |

### datafusion-python

| Issue | Title | Relevance |
|:---|:---|:---|
| [#1217](https://github.com/apache/datafusion-python/issues/1217) | FFI bus error / segfault | PyCapsule boundary stability |

---

## Implementation Roadmap

### Dependency Chain

```
Foundation (1, 2) ─┬─→ CoW Delete (3) → CoW Overwrite (4)
                   ├─→ Equality Delete Reads (5) → Eq-to-Pos (9)
                   ├─→ Orphan File Deletion (7)
                   ├─→ Compaction (6) → Pos Delete Compact (10) → Z-Order (12)
                   └─→ Upsert (8)

Equality Reads (5) + Compaction (6) + Pos Compact (10) → Full MoR Compaction (11)
```

### Ordered Issue List

| # | Title | Depends On | Category | iceberg-rust Change? |
|:---|:---|:---|:---|:---|
| 1 | Engine resolution module | Nothing | Infra | No |
| 2 | `pyiceberg_core.execution` Rust stubs | Nothing | Infra | Yes (minor) |
| 3 | Streaming CoW delete | 1, 2 | Fix existing OOM | No (Track 1) |
| 4 | Streaming CoW overwrite | 3 | Fix existing OOM | No |
| 5 | Equality delete read support | 1, 2 | **Critical new feature** | No (Track 1) |
| 6 | Table compaction | 1, 2 | New feature | No (Track 1) |
| 7 | Orphan file deletion | 1, 2 | New feature | No |
| 8 | Upsert via hash join | 1, 2 | Fix existing O(n²) | No |
| 9 | Eq-to-positional conversion | 5 | New feature | No |
| 10 | Position delete compaction | 6 | New feature | No |
| 11 | Full MoR compaction | 5, 6, 10 | Composite | Yes (OverwriteCommitExec) |
| 12 | Z-Order sorting | 6 | New feature | No |
| 13 | DV compaction | 3 | V3 feature | No |
| 14 | Incremental compaction | 6 | Optimization | No |

### Parallelizable Work

After foundation (1, 2) is laid, issues **3–8** are all independent and can be developed in parallel by different contributors.

---

## OOM Safety Guarantee

Every operation in this integration follows the same safety contract:

```
BEFORE operation: Table is in valid state S₀
DURING operation: New data files written to storage (orphans until commit)
ON SUCCESS:       Atomic commit → snapshot S₁ (references new files)
ON FAILURE:       No commit → table remains at S₀ (new files are orphans)
```

**Iceberg's optimistic concurrency control guarantees that a mid-operation OOM can never corrupt table state.** Written-but-uncommitted files are orphans, invisible to all readers, and cleaned up by maintenance operations.

---

## Why PyIceberg Cannot Remain "Metadata-Only"

### The Category Error

There is a misconception that PyIceberg should be a thin metadata client. This fails because:

1. **Table maintenance IS compute**: Compaction, orphan deletion, and delete compaction read/rewrite data files. You cannot delegate these to Spark without reimplementing Iceberg's entire commit protocol.

2. **`table.scan().to_arrow()` is a correctness contract**: For V2 tables with equality deletes, this contract is currently violated. The scan is not delegatable—it IS PyIceberg's responsibility.

3. **No external engine provides write-back**: Polars/DuckDB/Daft can read Iceberg but cannot commit results back. Only PyIceberg has the full catalog + commit + metadata stack.

### The Production Reality

- Tables accumulate equality deletes continuously (Flink CDC)
- Without compaction, Read Amplification Factor grows unbounded
- Without orphan deletion, storage costs grow unbounded
- Without equality delete resolution, Python apps cannot read the data

Java Iceberg solves this by coupling with Spark. PyIceberg must solve it by coupling with DataFusion.

---

## Feature Interactions

### With Commit Retry (#3319 / #3320)

DataFusion-powered operations (compaction, CoW rewrite) produce new data files then commit atomically. If the commit fails due to concurrent modification, the retry mechanism from #3320 allows re-validation and re-commit without re-executing the expensive compute phase.

### With REPLACE API (#3130 / #3131)

Compaction uses the REPLACE data operation (remove old files, add new files atomically). The REPLACE API (#3131) provides the Python-side commit path that DataFusion results flow into.

### With DeleteFileIndex (#2918 / #3285)

The DataFusion equality resolver is triggered by `DeleteFileIndex`. When the index identifies applicable equality delete files for a data file, it passes the file paths to DataFusion for anti-join resolution rather than attempting in-memory materialization.

### With Schema Evolution

Equality deletes reference columns by `equality_ids` (field IDs). The DataFusion resolver must project both data and delete files to the same schema using Iceberg's field-ID-based schema reconciliation before executing the join.

### With V3 Deletion Vectors

DV compaction (merging bitmaps + rewriting high-deletion-ratio files) reuses the streaming CoW rewrite infrastructure from issue #3. DVs themselves are O(KB) in memory; only the data rewrite needs DataFusion's bounded-memory execution.

---

## Open Questions

1. **Version pinning**: `pyiceberg-core` depends on specific `datafusion` versions. How do we manage the version matrix across releases?

2. **Object store configuration**: Track 1 (Python-side DataFusion) needs object store access configured separately from PyIceberg's `FileIO`. Should we bridge `FileIO` properties into DataFusion's object store registry?

3. **Streaming vs. batch resolution**: Should equality deletes be resolved per-file (small anti-joins, many invocations) or per-partition (larger anti-joins, fewer invocations)?

4. **Memory limit default**: What's the right default `memory_limit`? Options: 50% of available RAM, fixed 512MB, configurable via `.pyiceberg.yaml`.

5. **Track 1 → Track 2 migration**: When iceberg-rust #2186 is complete, how do we deprecate Track 1 without breaking users?

---

## Goals

- **Equality delete reads**: `table.scan().to_arrow()` returns correct results on all V2 tables
- **Bounded-memory compaction**: `table.compact()` operates on 100GB+ with 512MB budget
- **No existing regressions**: All tests pass without `pyiceberg-core` installed
- **Production-ready maintenance**: Orphan deletion, delete compaction handle 10M+ file tables
- **Zero-config UX**: Everything "just works" with sensible defaults

## Non-Goals

- **Distributed execution**: Single-node only. DataFusion provides vertical scaling via disk.
- **Replace PyArrow**: PyArrow remains for streaming I/O, Parquet read/write, schema operations.
- **Force opt-in**: DataFusion is never required. PyArrow fallback always exists.

---

## References

### Design Documents

- `pyiceberg_datafusion/datafusion_status.md` — Current state of DataFusion in iceberg-rust and iceberg-python
- `pyiceberg_datafusion/datafusion_ux.md` — UX design, engine resolution, full OOM feature inventory
- `pyiceberg_datafusion/datafusion_epic.md` — GitHub epic with all issue references
- `pyiceberg_datafusion/datafusion_issues.md` — Detailed issue breakdown with implementation tracks
- `pyiceberg_datafusion/datafusion_in_depth.md` — DataFusion architecture deep dive

### Key Upstream Issues

- [PyIceberg MoR Epic #1078](https://github.com/apache/iceberg-python/issues/1078)
- [Equality Delete Support #1210](https://github.com/apache/iceberg-python/issues/1210)
- [DeleteFileIndex for Equality #3285](https://github.com/apache/iceberg-python/pull/3285)
- [Execution Path Isolation #3356](https://github.com/apache/iceberg-python/issues/3356)
- [iceberg-rust MoR #2186](https://github.com/apache/iceberg-rust/issues/2186)
- [iceberg-rust Equality Reader #2205](https://github.com/apache/iceberg-rust/issues/2205)
- [DataFusion FFI Stability #1217](https://github.com/apache/datafusion-python/issues/1217)
