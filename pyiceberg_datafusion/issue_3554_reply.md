# GitHub Reply to #3554 (responding to Kevin's pluggable backend comment)

---

Thanks @kevinjqliu — I explored this direction in depth. Here's what I found:

## The Key Insight: Read/Write vs. Compute Are Different Problems

All candidate libraries (PyArrow, DataFusion, DuckDB, Polars) produce **identical Arrow output** for Parquet reads. The Parquet spec defines a deterministic decode algorithm — given the same file, same projection, same filter, any correct implementation produces the same RecordBatches. Reading is I/O-bound (94% network time for S3), so swapping who reads doesn't produce a user-visible improvement.

The real differentiator is **compute** (sort, join, filter). Only DataFusion and DuckDB can spill to disk for bounded-memory execution. PyArrow and Polars structurally cannot.

## Proposed Architecture: Three Independent Axes

```
Operation = Semantics (always PyIceberg) × IO (pluggable) × Compute (pluggable w/ capability gate)
```

All three communicate via Arrow RecordBatch (zero-copy exchange, [verified across 25 library permutations](https://github.com/qzyu999/iceberg-notes/blob/main/pyiceberg_datafusion/arrow_interop_test.py)).

**IOBackend** — who reads/writes Parquet:
- PyArrow (default, works everywhere)
- DataFusion, DuckDB, Polars (all produce same Arrow output)
- Future: cuDF for GPU-accelerated decode

**ComputeBackend** — who sorts/joins/filters:
- DataFusion (recommended: Apache 2.0, per-session memory control, Arrow-native)
- DuckDB (capable but BSL license for S3 extension)
- PyArrow (fallback: works for small data, OOMs on large)

Mix-and-match works: DuckDB reads → DataFusion sorts → PyArrow writes. Arrow is the glue.

## Implementation Plan

**Phase 1 (immediate):** Build DataFusion compute functions behind clean Arrow-in/Arrow-out signatures. No protocol extraction yet — delivers bounded-memory operations NOW. Zero refactoring of existing code.

**Phase 2 (after proven):** Extract `IOBackend` + `ComputeBackend` protocols by generalizing from the two concrete implementations (PyArrow + DataFusion). Refactor `pyiceberg/io/pyarrow.py` behind the interface.

**Phase 3 (community):** Others contribute DuckDB/Polars/cuDF backends.

## Why This Order

1. Phase 1 delivers user value immediately (OOM fix ships)
2. The interface emerges from real implementation, not speculative design
3. The existing 3K-line `pyarrow.py` monolith gets refactored with the benefit of knowing what the interface actually needs to be

## Design Doc

Full analysis (Arrow interop stack, IPC spill mechanics, per-library memory bounds, API design, distributed/GPU considerations):
[Support for PyIceberg Pluggable Backend Architecture](https://docs.google.com/document/d/1p3Imyhlw_KZq9asP6Wz9VFj9sny1hcqelY9LC0c6J0Y/edit?usp=sharing)

For Phase 1 — should I start with the engine resolution module, or would you prefer to see the `IOBackend`/`ComputeBackend` protocol definitions first?
