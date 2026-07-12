# PyIceberg Pluggable Execution Backend — PR Plan

## Prime Directive

**Do as much as needed and nothing else.** The PR adds a pluggable execution
interface with DataFusion as a spill-to-disk backend. Existing operations that
are currently OOM-prone become OOM-resilient. New operations that naturally fall
out of the interface (equality deletes, sort-on-write) are included because they
require zero additional architecture — just routing through already-needed
protocol methods. No code is added for features that aren't exercised by a
concrete code path in this PR.

---

## Branches

| Branch | Purpose | Status |
|--------|---------|--------|
| `pluggable-backend-discovery` | Full API surface exploration (Fowler's principle) | ✅ Complete — 24K lines |
| `pluggable-backend-init` | PR-ready: trimmed to only what's needed | 🚧 Needs trimming from discovery |

Both branch from `origin/main` at `2c755232`.

---

## Operations Analysis: What DataFusion Improves

### Existing operations made OOM-resilient

| Operation | Current OOM Risk | With This PR |
|-----------|-----------------|--------------|
| **CoW delete on large files** | O(file_size) — `ArrowScan.to_table([task])` materializes entire file | **O(batch_size)** — two-pass streaming: count pass + filter-stream-to-writer pass |
| **Scan planning (>100K delete entries)** | O(num_entries) — all entries in Python dict (DeleteFileIndex) | **O(memory_limit)** — BoundedMemoryPlanner uses DataFusion SQL join with spill-to-disk |
| **CoW delete (statistics short-circuit)** | Reads every file regardless of column bounds | **Zero I/O for classifiable files** — strict/inclusive metrics evaluators skip/drop files without reading |

### New operations enabled by the interface (zero extra architecture)

| Operation | Why It Belongs | OOM Model |
|-----------|---------------|-----------|
| **Equality delete resolution** | Requires `anti_join` protocol method — same method used by any future MoR consumer. Removes the existing `raise ValueError`. | O(memory_limit) — DataFusion Grace Hash Join spills to disk |
| **Sort-on-write** | Requires `sort_from_files` protocol method. DataFusion external merge sort. Iceberg spec says sort is advisory — skip gracefully without DF. | O(memory_limit) — DataFusion spills sorted runs |

### Operations NOT changed (already streaming or inherently bounded)

| Operation | Why No Change Needed |
|-----------|---------------------|
| `to_arrow_batch_reader()` | Already O(batch_size) streaming |
| `scan().count()` | Streaming count, never materializes |
| `append(RecordBatchReader)` | Already streaming microbatched writes |
| Positional delete resolution | O(num_positions) PyArrow set fallback; **O(memory_limit) with DataFusion** — SQL anti-join spills to disk |
| `to_arrow()` full materialization | User explicitly asked for full Table — can't avoid, warning exists |

### Operations NOT in scope (would add code for features that don't exist yet)

| Operation | Status in PyIceberg | Why Excluded |
|-----------|--------------------|----|
| Compaction / RewriteDataFiles | Does not exist | New feature requiring new orchestration |
| Orphan file deletion | Does not exist | New feature requiring storage listing |
| MoR write path (RowDelta) | Falls back to CoW with warning | New write mode, separate PR |
| Partitioned streaming writes | Not supported for RecordBatchReader | Tracked in #2152, separate concern |
| Incremental changelog scan | Does not exist | New scan type |

---

## What Ships in the PR

### Structural (the pluggable interface)

| Component | Purpose |
|-----------|---------|
| `protocol.py` | ReadBackend, WriteBackend, ComputeBackend protocols |
| `engine.py` | Resolution: config/env/auto-detect → PyArrow or DataFusion |
| `_orchestrate.py` | Scan dispatch: read + delete resolution + filter + schema reconciliation |
| `backends/pyarrow_backend.py` | Default: behavior-identical to today's ArrowScan |
| `backends/datafusion_backend.py` | Spill-to-disk compute via FairSpillPool |
| `object_store.py` | Credential bridge for DataFusion (env var scoping) |
| `expression_to_sql.py` | Iceberg BooleanExpression → SQL WHERE (for DF read path) |
| `_sql_helpers.py` | Shared sort direction utility |
| `planning.py` | InMemoryPlanner (wrapper) + BoundedMemoryPlanner (DF SQL join) |

### OOM-resilience improvements to existing operations

| Component | What Changes |
|-----------|-------------|
| `table/__init__.py` CoW delete | Two-pass streaming for large files + stats short-circuit |
| `table/__init__.py` scan planning | Auto-switch to BoundedMemoryPlanner above threshold |
| `table/delete_file_index.py` | Sequence number gating for equality deletes |

### New capabilities (fall naturally from the interface)

| Component | What's New |
|-----------|-----------|
| `_orchestrate.py` equality branch | anti_join routing for equality delete files |
| `table/__init__.py` sort-on-write | `_apply_sort_order` with DF sort_from_files |
| `_sorted_reader.py` | Lifecycle-managed temp file for sort-on-write streaming |
| `materialize.py` | Temp Parquet helper (used by sort-on-write + bounded planner) |

### Deprecation

| Component | Change |
|-----------|--------|
| `io/pyarrow.py` ArrowScan | DeprecationWarning on instantiation, still functional |

---

## What Gets Trimmed from Discovery Branch

| Item | Lines | Reason |
|------|-------|--------|
| `backends/duckdb_backend.py` | ~360 | Third backend — proves nothing new |
| `backends/polars_backend.py` | ~300 | Fourth backend — proves nothing new |
| `metadata.py` | ~200 | Orphan file deletion prep — feature doesn't exist |
| `ObjectStoreBackend` / `ReadAndListBackend` protocols | ~60 | Only needed by orphan deletion |
| `PlanningBackend` protocol | ~30 | InMemory/Bounded planners are concrete, not pluggable |
| `aggregate_from_files()` protocol method | ~20 | Not called by any PR code path |
| `join_from_files()` protocol method | ~20 | Not called (anti_join_from_files is the used path) |
| DuckDB/Polars engine.py enum variants + registry | ~80 | No backends to register |
| DuckDB/Polars test files (~20 files) | ~5000 | No backends to test |
| Tests for removed features | ~3000 | Dead code |

**Expected final size: ~5,000–6,000 lines total (source + tests)**

---

## Regression Contract

- ALL existing `tests/` must pass unchanged (except ArrowScan deprecation warnings)
- `table.scan().to_arrow()` produces identical output before and after
- `table.scan().to_arrow_batch_reader()` produces identical output
- `table.scan().count()` produces identical output
- `Transaction.delete(filter)` produces identical committed state
- `Transaction.append(df)` produces identical data files
- `Transaction.overwrite(df, filter)` produces identical committed state
- `Transaction.upsert(df, join_cols)` produces identical result
- Performance: ≤5ms overhead per scan from the new dispatch layer

---

## PR Description

```
Core: Add pluggable execution backend with DataFusion for OOM-resilient operations

Closes #XXXX

## Rationale

PyIceberg's scan and delete operations materialize large intermediate data in
memory (CoW delete reads entire files, scan planning accumulates all delete entries).
This PR introduces a pluggable compute backend that routes these operations through
DataFusion's spill-to-disk execution when installed, making them OOM-resilient.

## What Changes

**Architecture:**
- ReadBackend, WriteBackend, ComputeBackend protocols (pyiceberg.execution.protocol)
- PyArrow backend (default, behavior-identical to ArrowScan)
- DataFusion backend (optional, spill-to-disk via FairSpillPool)
- Auto-detection: `pip install 'pyiceberg[datafusion]'` promotes DF as compute backend

**Existing operations improved:**
- CoW delete: O(batch_size) streaming for large files (was O(file_size))
- CoW delete: statistics-based short-circuit skips files without reading
- Scan planning: bounded-memory planner for tables with >100K delete entries

**New operations enabled:**
- Equality delete support (anti-join with IS NOT DISTINCT FROM semantics)
- Sort-on-write (best-effort, skipped gracefully without DataFusion)

**Deprecated:**
- ArrowScan (still functional, emits DeprecationWarning)

## User-Facing Changes

- Tables with equality delete files can now be read (previously raised ValueError)
- CoW deletes on large files no longer OOM (streaming rewrite)
- `pip install 'pyiceberg[datafusion]'` enables bounded-memory compute
- ArrowScan deprecated (use table.scan().to_arrow())
- No breaking changes to any public API
```

---

## Trimming Checklist (work remaining on `pluggable-backend-init`)

- [ ] Delete `backends/duckdb_backend.py`, `backends/polars_backend.py`
- [ ] Delete `metadata.py`
- [ ] Remove `ObjectStoreBackend`, `ReadAndListBackend`, `PlanningBackend` from protocol.py
- [ ] Remove `aggregate_from_files`, `join_from_files` from ComputeBackend
- [ ] Remove DuckDB/Polars from engine.py (enum, registry, detection)
- [ ] Remove DuckDB/Polars from `__init__.py` exports
- [ ] Delete all DuckDB/Polars test files
- [ ] Consolidate remaining tests to ~15 focused files
- [ ] Trim configuration.md (remove DuckDB/Polars sections)
- [ ] Verify all existing tests pass
- [ ] Run ArrowScan parity tests on Linux (skipped on Windows)

---

## Upstream Issues Filed

| Issue | Repo | Purpose | Status |
|-------|------|---------|--------|
| [#1624](https://github.com/apache/datafusion-python/issues/1624) | datafusion-python | Per-session object store config (credential scoping without env vars) | Open |
| [#23472](https://github.com/apache/datafusion/issues/23472) | datafusion (core) | Expose per-file Parquet `FileMetaData` from write operations (`ParquetSink`) | Open — filed by us |

### Why these matter for PyIceberg

**#1624 (credential scoping):** Currently DataFusion reads cloud credentials from
`os.environ`, requiring a global lock (`_scoped_env_vars`) that serializes concurrent
DataFusion operations. Once per-session object store config lands, credentials can be
injected into `RuntimeConfig` directly — eliminating the lock and enabling true parallel
DataFusion operations + lazy `execute_stream()` for end-to-end streaming.

**#23472 (write metadata):** Currently PyArrow is the ONLY write backend because it's the
only way to get the detailed column statistics (column_sizes, null_counts, lower_bounds,
upper_bounds, split_offsets) required for Iceberg's `DataFile` manifest entries. Once
DataFusion exposes `FileMetaData` from its `ParquetSink`, a DataFusion write backend
becomes possible — enabling single-pass CoW delete (read → filter → write all in
DataFusion SQL with predicate pushdown and spill-to-disk, zero Python batch loop).

### Impact on current PR

Neither issue blocks the initial PR. The current architecture correctly works around both:
- Credential scoping: `_scoped_env_vars` with lock (correct, serialized for different creds)
- Write metadata: PyArrow `ParquetWriter` with `metadata_collector` (correct, always available)

Both workarounds are encapsulated behind the pluggable interface — when upstream fixes
land, only the backend implementations change. User-facing API is unaffected.
