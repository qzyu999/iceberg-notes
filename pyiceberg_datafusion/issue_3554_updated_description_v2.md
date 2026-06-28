# Issue #3554 — Updated Description (v2)

This is the proposed replacement for the issue body on
https://github.com/apache/iceberg-python/issues/3554, reflecting the refined
architectural direction established in community discussion.

---

### Feature Request / Improvement

# Problem

PyIceberg cannot perform several critical operations at production scale. The root cause
is that PyArrow — the current compute backend — provides no memory management, no
spill-to-disk, and no bounded-memory join or sort operators. Operations that are trivial
in Java Iceberg (which delegates to Spark/Flink) are either missing or OOM-prone:

| Operation | Status | Failure mode |
|-----------|--------|--------------|
| Read tables with equality deletes | Hard `ValueError` | Completely unreadable |
| CoW delete/overwrite | OOMs on large files | Loads entire Parquet file (~1GB) into memory |
| Upsert | O(n²) + OOM | Row-by-row Python comparison, `concat_tables` accumulation |
| Compaction | Not implemented | Requires external sort (infeasible in-memory) |
| Orphan file deletion | OOMs | LEFT ANTI JOIN of millions of paths |
| Position/equality delete compaction | Not implemented | Requires hash join + sort |
| Sort-order enforcement on write | Partial | Full sort before write |

These block PyIceberg from feature parity with Java Iceberg for V2/V3 tables.

# Solution

Use Apache DataFusion (`pip install 'pyiceberg[datafusion]'` — already a declared
optional extra) as PyIceberg's internal compute library for data-intensive operations.
DataFusion provides spill-to-disk sort, join, and filter with a configurable memory
budget. When not installed, existing PyArrow behavior is unchanged.

# Architectural Boundary

Based on [community discussion](https://github.com/apache/iceberg-python/issues/3554#issuecomment-4819685614), this proposal follows the **semantic ownership** principle: PyIceberg retains full ownership of Iceberg semantics and delegates only raw compute to DataFusion — the same relationship it already has with PyArrow.

**PyIceberg owns:**
- Scan planning (manifest filtering, partition pruning)
- Delete file resolution logic (DeleteFileIndex, sequence number gating)
- File selection (compaction bin-packing, orphan detection)
- Partition routing, schema reconciliation
- Transaction + commit (OCC, atomic snapshot)
- File writing (`_dataframe_to_data_files`)

**DataFusion provides:**
- `sort(data, keys, memory_limit)` — external merge sort with spill
- `anti_join(left, right, cols, memory_limit)` — Grace Hash Join with spill
- `filter(file, predicate)` — streaming filter with O(batch_size) memory

DataFusion knows nothing about Iceberg. It receives file paths or Arrow batches,
executes the requested operation with bounded memory, and returns Arrow results.
PyIceberg makes all Iceberg-semantic decisions.

# How This Relates to Existing DataFusion Integration

PyIceberg already uses DataFusion in a different, orthogonal role. These two roles
must not be conflated:

| | Role A: Query connector (existing) | Role B: Internal compute (this epic) |
|--|------|------|
| **What it does** | Lets users query Iceberg tables via DataFusion SQL | Gives PyIceberg bounded-memory sort/join/filter |
| **Who uses it** | End users write SQL/DataFrame queries | PyIceberg's internal implementation (invisible to users) |
| **Who owns Iceberg semantics** | iceberg-rust (via `pyiceberg-core` TableProvider) | PyIceberg Python code |
| **Packages needed** | `datafusion` + `pyiceberg-core` | `datafusion` only |
| **API surface** | `ctx.register_table()` + PyCapsule FFI | `ctx.register_parquet()` / `register_record_batches()` |
| **Changed by this epic** | No | Yes (new) |

Role A is the standard multi-engine interop pattern (like `to_duckdb()`, `to_ray()`,
`to_daft()`). It remains unchanged.

Role B is an **internal implementation detail** — users call `table.compact()` and never
know DataFusion exists underneath. This is the same pattern as PyArrow today: PyIceberg
uses it as a library, not as an exposed interface.

# Why DataFusion (and Why Not Pluggable)

This is an opinionated engineering decision, not an arbitrary preference. The
requirements for the internal compute engine are:

- Apache-licensed (ASF ecosystem compatibility)
- Arrow-native (zero-copy interop with PyArrow — no serialization)
- Embeddable as a library (not a server/database)
- Spill-to-disk for sort, join, and aggregate (the core requirement)
- Python bindings exist and are maintained

DataFusion is the **only** library satisfying all constraints. DuckDB has GPL-licensed
extensions and copies at the Arrow boundary. Polars has no spill-to-disk. Spark
requires a JVM. Velox has no Python bindings.

**Why not make it pluggable?** A pluggable `ComputeEngine` abstraction adds complexity
without benefit:
1. No alternatives exist (one candidate = no choice to make)
2. The abstraction surface is enormous (memory config, SQL dialect, data registration, object store — each engine differs completely)
3. Users never see or choose the engine — it's hidden behind `table.compact()`
4. The existing `to_X()` methods already serve users who want to query with their preferred engine

If an alternative emerges, `pyiceberg/execution/compute.py` is a clean substitution point.
Building pluggable infrastructure now for alternatives that don't exist is premature
abstraction (YAGNI).

# User Experience

DuckDB-style: sensible default memory budget, optional override via existing config
mechanisms. No new initialization step required.

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("prod", uri="...")
table = catalog.load_table("db.events")

# Everything just works — default 512MB budget, spills to disk if needed
table.compact()                          # new method
table.delete("status = 'expired'")       # existing method, no longer OOMs on large files
df = table.scan().to_arrow()             # existing method, now resolves equality deletes
```

Memory budget is configurable through PyIceberg's existing config hierarchy:

```yaml
# .pyiceberg.yaml
execution:
  memory-limit: 1GB
```
```bash
# Environment variable
export PYICEBERG_EXECUTION__MEMORY_LIMIT=2GB
```

Most users never configure this — the default handles typical workloads. When
`datafusion` is not installed, PyArrow fallback is used unchanged. The install path:

```
pip install 'pyiceberg[datafusion]'
```

# Implementation

All code lives in this repo. No iceberg-rust changes needed.

```
pyiceberg/execution/
├── __init__.py          # Re-exports
├── engine.py            # resolve_engine() — checks `import datafusion`
├── session.py           # create_bounded_session() — FairSpillPool + DiskManager
├── object_store.py      # Translate FileIO props → DataFusion object store config
└── compute.py           # sort_batches(), anti_join(), filter_parquet()
```

## Checklist

### Foundation (no blockers)
- [ ] Engine resolution module
- [ ] Bounded-session helpers (configures `RuntimeEnvBuilder.with_fair_spill_pool()`)
- [ ] Object store bridge (translate FileIO properties to DataFusion)

### Operations
- [ ] Upsert — replace O(n²) row comparison with hash join
- [ ] Equality delete resolution — LEFT ANTI JOIN (needs #3285)
- [ ] Orphan file deletion — anti-join of path arrays
- [ ] Streaming CoW delete/overwrite — DataFusion filters, PyIceberg writes
- [ ] Table compaction — DataFusion sorts, PyIceberg writes (needs #3131 for commit)
- [ ] Position delete compaction
- [ ] Sort-order enforcement on write
- [ ] Z-Order / Hilbert sorting
- [ ] Full MoR compaction
- [ ] Dynamic partition overwrite (bounded memory)

# Related Issues

**PyIceberg:**
#1078 (MoR epic) · #1210 / #3270 (equality deletes) · #3356 (execution isolation) ·
#1092 (compaction) · #1200 (orphan deletion) · #3285 (DeleteFileIndex eq deletes) ·
#3319 / #3320 (commit retry) · #3130 / #3131 (REPLACE API) · #1818 (V3/DV) ·
#2918 (DeleteFileIndex positional, merged)

**datafusion-python:**
[#1217](https://github.com/apache/datafusion-python/issues/1217) (FFI stability)

---

*Updated to reflect architectural direction from
[this comment](https://github.com/apache/iceberg-python/issues/3554#issuecomment-4819685614).
iceberg-rust issues #2716, #2717, #2718 will be closed — no Rust-side changes needed
under this approach.*
