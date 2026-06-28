# Updated Issue #3554 Description

## Proposed Edit (replaces original body after Kevin's feedback)

---

### Feature Request / Improvement

# Problem

PyIceberg cannot perform several operations at production scale due to unbounded memory requirements in the PyArrow execution path:

- Tables with equality deletes are unreadable (hard `ValueError`)
- CoW deletes OOM on large Parquet files (~1GB)
- CoW overwrite OOMs (same pattern as delete)
- Upsert uses O(n²) row-by-row comparison
- Compaction not implemented (requires external sort, infeasible in-memory for large tables)
- Orphan file deletion OOMs (LEFT ANTI JOIN of millions of paths)

These block PyIceberg from achieving feature parity with Java Iceberg for V2/V3 tables.

# Proposed Solution

Use `datafusion-python` (already a declared optional extra: `pip install 'pyiceberg[datafusion]'`) as an internal compute library for data-intensive operations. DataFusion provides spill-to-disk execution (bounded memory) for sorts, joins, and filters — the operations that cause OOM today.

**The boundary (per @kevinjqliu's [feedback](https://github.com/apache/iceberg-python/issues/3554#issuecomment-4819685614)):**

- **PyIceberg owns all Iceberg semantics** — scan planning, delete file resolution logic, partition routing, commit protocol, file selection
- **DataFusion is a pure compute library** — it receives Arrow data / Parquet file paths, executes sort/join/filter with bounded memory, returns Arrow data. It knows nothing about Iceberg (manifests, snapshots, partitions, catalogs).

This is the same pattern as PyArrow today: PyIceberg decides *what* to compute, hands the work to a library that does the heavy lifting, and handles the results. The difference is DataFusion can spill to disk when memory is exhausted, while PyArrow cannot.

# Two Distinct Roles of DataFusion in PyIceberg

This epic introduces a **second, independent role** for DataFusion alongside the existing integration. These serve fundamentally different purposes and should not be conflated:

## Role A: Query Engine Connector (existing, unchanged)

The existing `__datafusion_table_provider__` lets end users query Iceberg tables via DataFusion SQL — the same way PyIceberg could expose tables to DuckDB, Polars, or any other engine. This is the **multi-engine interop** pattern that all Iceberg bindings support (Java has Spark/Flink connectors, Rust has DataFusion TableProvider, etc.).

```python
# User-facing: "I want to query this Iceberg table with DataFusion"
ctx = SessionContext()
ctx.register_table("events", iceberg_table)  # PyCapsule FFI → iceberg-rust TableProvider
ctx.sql("SELECT * FROM events WHERE year = 2024")
```

- **Purpose:** Let users choose their query engine
- **Iceberg semantics owned by:** iceberg-rust (via `pyiceberg-core`)
- **Packages required:** `datafusion-python` + `pyiceberg-core`
- **Status:** Merged, working, unchanged by this epic

## Role B: Internal Compute Engine (this epic, new)

This epic uses DataFusion **internally** as a compute library — the same way PyIceberg uses PyArrow internally today. Users never see or interact with DataFusion directly. It's an implementation detail behind `table.compact()`, `table.delete()`, etc.

```python
# Internal: "PyIceberg needs to sort 10GB of data without OOMing"
# User never writes this — it's inside PyIceberg's implementation
ctx = SessionContext(runtime=RuntimeEnvBuilder().with_fair_spill_pool(512_000_000))
ctx.register_parquet("data", file_path)
sorted_result = ctx.sql("SELECT * FROM data ORDER BY timestamp")
```

- **Purpose:** Give PyIceberg bounded-memory compute for its own operations
- **Iceberg semantics owned by:** PyIceberg (Python)
- **Packages required:** `datafusion-python` only (no `pyiceberg-core`)
- **Status:** This epic

## Why This Distinction Matters

**This is NOT "picking DataFusion as PyIceberg's query engine."** PyIceberg remains engine-agnostic for user queries (Role A). Users can query via DataFusion, DuckDB, Polars, or any engine that supports the TableProvider/PyCapsule protocol.

What this IS: **PyIceberg choosing an internal execution strategy for its own maintenance operations** — the same way Java Iceberg uses Spark Actions internally for compaction, or the same way PostgreSQL uses its own internal sort/join algorithms rather than delegating to an external engine.

The key CS principle: a library's **internal implementation details** (how it sorts, joins, filters data) are orthogonal to its **external interop interfaces** (how it exposes data to other systems).

# The CS Principles Behind This Design

This architecture follows established computer science and software engineering principles. Framing the decision in these terms helps all parties understand why this is the correct boundary — independent of any specific technology choice.

## Principle 1: Separation of Concerns (Dijkstra, 1974)

An operation `Op` decomposes into two orthogonal concerns:

```
Op = Semantics(Op) ∘ Compute(Op)
```

- **Semantics** (what): Iceberg-spec decisions — which files, which deletes apply, how to commit
- **Compute** (how): Data transformation — sort, join, filter with bounded memory

These are independently substitutable. Changing the compute backend (PyArrow → DataFusion) doesn't alter semantic correctness. This is the same separation Java Iceberg uses: Java owns semantics, Spark/Flink provides compute.

## Principle 2: Information Hiding (Parnas, 1972)

DataFusion is an **implementation detail hidden behind PyIceberg's public API**. Users call `table.compact()` — they don't know or care whether sort happens via PyArrow, DataFusion, or hand-rolled merge sort. The decision to use DataFusion is invisible at the API boundary.

This means:
- If a better compute library emerges, we can swap it without API changes
- Users don't couple to DataFusion's behavior or version
- The optional dependency remains truly optional — fallback to PyArrow always works

## Principle 3: Dependency Inversion (Martin, 1996)

PyIceberg depends on an **abstract capability** (sort N records with M memory budget), not on a concrete implementation. DataFusion happens to be the only Arrow-native, Apache-licensed, embeddable library that provides spill-to-disk today. If alternatives emerge, the `compute.py` module has a clean substitution boundary.

## Principle 4: Uniform Asymptotic Behavior (Algorithm Design)

Every code path is designed for the worst case (unbounded data). The same algorithm handles 100 rows and 10 billion rows — no branching on assumed size, no "fast path for small data." When data is small, the bounded-memory algorithm has negligible overhead (no spill occurs). When data is large, it spills gracefully. This eliminates an entire class of "works in dev, OOMs in prod" bugs.

## Why DataFusion Specifically — And Why Not "Pluggable"

A natural objection: "Isn't choosing DataFusion an opinionated lock-in? Shouldn't the compute backend be pluggable, so users or contributors can substitute DuckDB, Polars, etc.?"

This is a reasonable instinct (the k8s "everything is configurable" philosophy), but it's the wrong pattern here. Making the compute backend pluggable is an **anti-pattern** in this context, for rigorous engineering reasons:

### The Elimination Argument

The requirement for the internal compute engine is a conjunction of hard constraints:

```
Required = Apache-licensed
         ∧ Arrow-native (zero-copy interop with PyArrow)
         ∧ Embeddable as library (not a server/database)
         ∧ Single-node (not distributed)
         ∧ Spill-to-disk for sort, join, aggregate
         ∧ Python bindings exist
         ∧ Actively maintained
```

Apply these constraints as a filter:

| Engine | Apache-licensed | Arrow-native | Embeddable | Spill-to-disk | Python bindings | Verdict |
|--------|:---:|:---:|:---:|:---:|:---:|---|
| **DataFusion** | ✅ | ✅ | ✅ | ✅ | ✅ | **Satisfies all** |
| DuckDB | ❌ (GPL extensions) | ❌ (copies at boundary) | ⚠️ (database) | ✅ | ✅ | License + format mismatch |
| Polars | ❌ (MIT, not Apache — fine legally but not ASF ecosystem) | ✅ | ✅ | ❌ (no spill for joins/sorts) | ✅ | Missing core capability |
| Spark | ✅ | ⚠️ | ❌ (requires JVM cluster) | ✅ | ✅ | Cannot embed |
| Velox | ❌ (Apache-adjacent but Meta-controlled) | ✅ | ✅ | ✅ | ❌ | No Python bindings |
| PyArrow | ✅ | ✅ | ✅ | ❌ | ✅ | Missing core capability (the problem we're solving) |

**DataFusion is the only library in existence that satisfies all constraints.** This is not a preference — it's an elimination. There is no decision to make; there is only one candidate.

### Why "Pluggable" Is the Anti-Pattern Here

Making something pluggable is justified when:
1. Multiple viable options exist with different tradeoffs for different users (**false** — only one option exists)
2. The abstraction cost is low (**false** — see below)
3. Users benefit from choosing (**false** — users call `table.compact()`, never see the engine)

**The abstraction cost is prohibitive.** A pluggable compute interface would need to abstract over:
- Session creation and memory configuration (each engine does this differently)
- Data registration (TableProvider vs. register_parquet vs. DuckDB's `read_parquet`)
- SQL dialect or DataFrame API (DataFusion SQL vs. DuckDB SQL vs. Polars expressions)
- Object store configuration (each engine has its own credential handling)
- Result format (RecordBatch stream vs. Arrow Table vs. engine-specific types)
- Spill configuration (FairSpillPool vs. DuckDB's memory_limit vs. nonexistent in Polars)
- Error handling and retry semantics

This abstraction becomes either:
- So thin it's useless (each engine requires different code anyway), or
- So thick it's its own query engine (the **inner platform anti-pattern** — building a query engine interface to abstract over query engines)

Both outcomes are worse than the direct approach.

### The Correct CS Framing: This Is an Implementation Detail, Not a User Choice

The choice of internal compute engine is analogous to:
- CPython choosing reference counting + generational GC (not pluggable)
- PostgreSQL choosing its own sort/hash-join implementations (not pluggable)
- Linux choosing CFS for process scheduling (not user-configurable per-process)
- Java Iceberg choosing to implement `BinPackStrategy` directly rather than delegating to a "pluggable sort provider"

These are **engineering decisions** made by project maintainers based on constraints. They are hidden behind stable APIs. Users never see or interact with them. Making them pluggable would add complexity without user benefit.

**The final word:** If a second Apache-licensed, Arrow-native, embeddable, spill-capable Python library emerges, the substitution point is clean (`pyiceberg/execution/compute.py` — swap the internals). But building a pluggable abstraction layer *now* for alternatives that *don't exist* violates YAGNI (You Aren't Gonna Need It) and adds maintenance burden for zero benefit.

### Relationship to PyIceberg's Existing Multi-Engine Interop

PyIceberg already provides multi-engine **read** access via output formatters:

```python
scan().to_arrow()          # → pa.Table
scan().to_duckdb("name")   # → DuckDBPyConnection
scan().to_ray()            # → ray.data.Dataset
scan().to_daft()           # → daft.DataFrame
scan().to_pandas()         # → pd.DataFrame
__datafusion_table_provider__  # → DataFusion PyCapsule
```

These are all **one-directional (Iceberg → Engine)** and **read-only**. No external engine writes back to Iceberg through these interfaces. They exist so users can *query* Iceberg data in their preferred engine.

A natural question: "Could we extend this pattern to let users *choose* which engine performs compute-heavy ops? E.g., `table.compact(engine='duckdb')` or `table.compact(engine='polars')`?"

**This is the wrong abstraction for three reasons:**

**Reason 1: The existing `to_X()` interop serves a fundamentally different purpose.**

The `to_X()` methods exist because different users have different *query* preferences (SQL vs. DataFrame, DuckDB vs. Polars). They're choosing how to *analyze* data. Compute-heavy maintenance ops (compact, delete resolution) aren't user analysis — they're internal table management. The user doesn't care *how* compaction sorts data; they care that it completes correctly.

The analogy: SQLite lets you query with different client libraries (Python, C, Go), but you don't choose which sorting algorithm SQLite uses internally. Those are different layers.

**Reason 2: None of the interop engines can actually do the job.**

| Engine | Can sort with spill? | Can hash-join with spill? | Can read Parquet with memory budget? | Suitable? |
|--------|:---:|:---:|:---:|:---:|
| DuckDB | ✅ | ✅ | ✅ | ❌ (GPL extensions, copies at Arrow boundary) |
| Polars | ❌ | ❌ | ❌ | ❌ (no spill-to-disk) |
| Ray | ⚠️ (distributed overhead) | ⚠️ | ⚠️ | ❌ (cluster framework, not embeddable bounded-memory engine) |
| Daft | ❌ | ❌ | ❌ | ❌ (no spill) |
| pandas | ❌ | ❌ | ❌ | ❌ (pure in-memory) |

Making the interface pluggable across engines that can't satisfy the core requirement (bounded-memory execution) creates a false promise. Users would select an engine, hit OOM, and not understand why.

**Reason 3: The abstraction cost exceeds the implementation cost.**

A pluggable `ComputeEngine` interface would need to specify:
- How to configure memory limits (each engine differs completely)
- How to register data (SQL table names? file paths? DataFrame objects?)
- How to express operations (SQL strings? method calls? expression trees?)
- How to handle errors and partial results
- How to manage temp state and cleanup

This interface would be harder to design, implement, test, and maintain than just writing the DataFusion implementation directly. It's the **inner platform effect** — building infrastructure to abstract over a problem that has exactly one solution.

**The clear separation:**

| Layer | Purpose | Pluggable? | Why |
|-------|---------|:---:|-----|
| `scan().to_X()` | User chooses query engine for analysis | ✅ Yes | Multiple valid choices, user preference matters |
| `table.compact()` internals | PyIceberg's internal execution strategy | ❌ No | One viable option, user doesn't care, hidden behind API |

This is the standard layering: user-facing interfaces are flexible and multi-engine; internal implementation details are opinionated and optimal.

# UX

DuckDB-style: users optionally configure a memory budget. No engine selection, no execution strategy configuration.

```python
# Zero-config — just works with default 512MB budget
table.compact()
table.delete("category = 'spam'")
result = table.scan().to_arrow()  # transparently resolves equality deletes

# Power user — tune the budget
table.compact(memory_limit="2GB")
```

The memory budget is configured via Python's `datafusion.RuntimeEnvBuilder().with_fair_spill_pool()` — no Rust utilities needed.

When `datafusion-python` is not installed, existing PyArrow behavior is unchanged (works for small data, OOMs on large).

# Architecture

```
┌─────────────────────────────────────────────────────────┐
│  PyIceberg (Python) — ALL Iceberg semantics             │
│  • Scan planning (manifest filtering, partition pruning)│
│  • DeleteFileIndex (sequence number gating)             │
│  • File selection (compaction bin-packing)              │
│  • Partition routing, schema reconciliation             │
│  • Transaction + commit (OCC)                           │
│  • File writing (_dataframe_to_data_files)             │
└────────────────────────┬────────────────────────────────┘
                         │ file paths / Arrow batches
                         ▼
┌─────────────────────────────────────────────────────────┐
│  DataFusion (via datafusion-python) — compute only      │
│  • sort(files, keys, memory_limit) → sorted batches     │
│  • anti_join(data_files, delete_files, cols) → filtered │
│  • filter(file, predicate) → matching rows              │
│  Knows NOTHING about Iceberg.                           │
└─────────────────────────────────────────────────────────┘
```

# Implementation

All work lives in this repo (`iceberg-python`). No iceberg-rust changes needed.

New module: `pyiceberg/execution/`

```
pyiceberg/execution/
├── __init__.py          # Re-exports
├── engine.py            # resolve_engine() — detects datafusion availability
├── session.py           # create_bounded_session() — FairSpillPool config
├── object_store.py      # Translate FileIO props → DataFusion object store config
└── compute.py           # sort_batches(), anti_join(), filter_parquet()
```

## Checklist

### Foundation (no blockers)
- [ ] Engine resolution module (`pyiceberg/execution/engine.py`)
- [ ] Bounded-session helpers (`pyiceberg/execution/session.py`) — configures `RuntimeEnvBuilder.with_fair_spill_pool()`
- [ ] Object store bridge (`pyiceberg/execution/object_store.py`) — translates FileIO properties

### Operations (after foundation)
- [ ] Upsert fix — replace O(n²) row comparison with DataFusion hash join
- [ ] Equality delete resolution — DataFusion LEFT ANTI JOIN (blocked on #3285)
- [ ] Orphan file deletion — DataFusion anti-join of path arrays
- [ ] Streaming CoW delete/overwrite — DataFusion reads + filters file, PyIceberg writes output
- [ ] Table compaction — DataFusion sorts, PyIceberg writes (blocked on #3131 for commit)
- [ ] Position delete compaction
- [ ] Sort-order enforcement on write
- [ ] Z-Order / Hilbert sorting
- [ ] Full MoR compaction (equality resolution + sort + rewrite)
- [ ] Dynamic partition overwrite (bounded memory)

# Related Issues

## PyIceberg
- #1078 (MoR support epic)
- #1210 / #3270 (equality delete reads)
- #3356 (execution path isolation)
- #1092 (data compaction)
- #1200 (orphan file deletion)
- #3285 (`DeleteFileIndex` for equality deletes — prerequisite for eq delete resolution)
- #3319 / #3320 (commit retry — prerequisite for safe compaction commits)
- #3130 / #3131 (`REPLACE` API — prerequisite for compaction commit)
- #1818 (V3 tracking, DV compaction)
- #2918 (`DeleteFileIndex` for positional deletes, merged)

## datafusion-python
- [#1217](https://github.com/apache/datafusion-python/issues/1217) (FFI boundary stability)

---

## What Changed From the Original Description

| Original | Updated |
|----------|---------|
| Two tracks (Python DF + Rust pyiceberg_core.execution) | Single approach: `datafusion-python` as compute library |
| Track 2 preferred long-term | Track 2 removed entirely |
| Engine resolver checks `pyiceberg_core.execution` | Engine resolver checks `import datafusion` |
| Install: `pip install 'pyiceberg[pyiceberg-core]'` | Install: `pip install 'pyiceberg[datafusion]'` |
| Requires iceberg-rust contributions | Zero iceberg-rust changes |
| References `pyiceberg_core.execution` module, bounded session helper, IcebergOverwriteCommitExec | All removed |
| Design doc linked (Google Doc) | Can keep link or remove — the approach in the doc is partially superseded |
| Ambiguous about whether this "picks" DataFusion as THE engine | Explicit: Role A (multi-engine interop) unchanged, Role B (internal compute) is an opinionated implementation detail |

## iceberg-rust Issues to Close

These should be closed with a comment referencing this pivot:

- [#2716](https://github.com/apache/iceberg-rust/issues/2716) — EPIC: Bounded-memory execution operations for pyiceberg-core
- [#2717](https://github.com/apache/iceberg-rust/issues/2717) — Bounded-memory session helper
- [#2718](https://github.com/apache/iceberg-rust/issues/2718) — `pyiceberg_core.execution` module

Suggested closing comment:
> Closing — per architectural feedback on [iceberg-python#3554](https://github.com/apache/iceberg-python/issues/3554#issuecomment-4819685614), the execution layer will use `datafusion-python` directly from PyIceberg's Python code rather than adding a `pyiceberg_core.execution` Rust module. This keeps all Iceberg semantics in PyIceberg and uses DataFusion purely as a compute library (same pattern as PyArrow). No iceberg-rust changes needed.

