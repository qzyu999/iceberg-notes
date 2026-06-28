# Issue: Add bounded-memory DataFusion session helper to `iceberg-datafusion`

## Title

`DataFusion: Add bounded-memory session utility with spill-to-disk support`

---

## Is your feature request related to a problem or challenge?

The `iceberg-datafusion` crate has no built-in way to configure memory-bounded execution. All physical operators (`IcebergTableScan`, `IcebergWriteExec`, `IcebergCommitExec`) rely entirely on whatever `SessionContext` the caller provides, and there is no utility to construct a session with spill-to-disk capabilities.

This means:

1. **Applications embedding `iceberg-datafusion` must know DataFusion internals** to configure `FairSpillPool`, `DiskManager`, and `RuntimeEnv` correctly. This is non-trivial boilerplate that every consumer reimplements.

2. **The default behavior is unbounded memory.** Without explicit configuration, DataFusion uses `UnboundedMemoryPool` — sorts and joins consume unlimited memory and OOM on large datasets instead of spilling to disk.

3. **Downstream consumers like `pyiceberg-core` need bounded execution** for operations such as compaction (external merge sort), equality delete resolution (hash anti-join), and copy-on-write rewrites. These operations process arbitrarily large data and must complete within a configurable memory budget.

DataFusion already provides production-grade external-memory operators (`SortExec`, `HashJoinExec`, `GroupedHashAggregateExec`) that spill to disk automatically — but only when the session's `MemoryPool` is configured to reject allocations at a threshold. Without a bounded pool, spill never triggers.

---

## Describe the solution you'd like

Add a `session` module to `iceberg-datafusion` (`crates/integrations/datafusion/src/session.rs`) that provides:

### 1. `BoundedSessionConfig` struct

A configuration struct for bounded-memory sessions:

```rust
pub struct BoundedSessionConfig {
    /// Maximum memory budget in bytes. Operators spill to disk beyond this.
    pub memory_limit_bytes: usize,
    /// Number of partitions for parallel execution. Defaults to available CPUs.
    pub target_partitions: Option<usize>,
    /// Rows per RecordBatch. Defaults to 8192.
    pub batch_size: Option<usize>,
    /// Optional directory for spill files. None = OS temp directory.
    pub spill_directory: Option<String>,
}
```

With a `parse_memory_limit()` helper for human-readable strings ("512MB", "2GB").

### 2. `create_bounded_session()` function

```rust
pub fn create_bounded_session(config: BoundedSessionConfig) -> Result<SessionContext, DataFusionError>
```

This constructs a `SessionContext` with:
- **`FairSpillPool`** — divides available memory evenly among concurrent spillable operators (sort, join, aggregate). Prevents any single operator from starving others.
- **`DiskManager`** — manages temp file lifecycle for spill data (Arrow IPC format, zero-deserialization on read-back).
- **`target_partitions`** derived from available CPUs (or overridden).
- **`batch_size`** of 8192 (DataFusion's default, balances overhead vs. granularity).

### Why `FairSpillPool` over `GreedyMemoryPool`

Iceberg operations often compose multiple memory-hungry operators in a single plan (e.g., compaction = HashJoin for delete resolution + SortExec for ordering + write). `FairSpillPool` ensures each operator gets a fair share:

| Scenario | GreedyMemoryPool | FairSpillPool |
|----------|-----------------|---------------|
| Sort alone (512MB budget) | Works | Works |
| Sort + HashJoin (512MB) | First operator grabs all memory; second may OOM | Each gets 256MB, both spill cooperatively |
| Sort + HashJoin + Aggregate | Starvation possible | Each gets ~170MB |

### Memory guarantee

For any plan `P` executed on the returned session:
```
∀t ∈ execution_time: resident_memory(t) ≤ memory_limit_bytes + ε
```
Where ε is framework overhead (Tokio stacks, metadata — typically 10-50MB).

---

## Example usage

```rust
use iceberg_datafusion::session::{BoundedSessionConfig, create_bounded_session};

// Create a session with 512MB memory budget
let config = BoundedSessionConfig::new(512 * 1024 * 1024);
let ctx = create_bounded_session(config)?;

// Operations on this session spill to disk when memory is exhausted:
// - SortExec: external merge sort (write sorted runs, k-way merge)
// - HashJoinExec: Grace Hash Join (partition both sides, process per-partition)
// - GroupedHashAggregateExec: spill partial aggregation state

ctx.register_parquet("data", "s3://bucket/table/data.parquet").await?;
let result = ctx.sql("SELECT * FROM data ORDER BY timestamp").await?;
// ^ If data > 512MB, intermediate sorted runs spill to disk automatically
```

---

## Motivation from downstream consumers

### pyiceberg-core (Python bindings)

PyIceberg needs bounded-memory execution for:
- **Table compaction** — sort + rewrite arbitrarily large datasets within a memory budget
- **Equality delete resolution** — hash anti-join of data × delete files (both potentially larger than RAM)
- **Copy-on-write rewrites** — streaming filter through files larger than available memory
- **Orphan file deletion** — anti-join of storage listing × valid paths (millions of entries)

Without this utility, every downstream consumer reimplements the same `RuntimeEnvBuilder` + `FairSpillPool` + `DiskManager` boilerplate.

### datafusion-comet and other engines

Any Rust application embedding `iceberg-datafusion` for large-scale operations faces the same problem. A standard session helper eliminates duplicated configuration code.

---

## Scope

- **New file:** `crates/integrations/datafusion/src/session.rs`
- **New public API:** `BoundedSessionConfig`, `create_bounded_session()`
- **Modification:** Add `pub mod session;` to `lib.rs`
- **No changes to existing behavior** — purely additive
- **No new dependencies** — uses types already in the `datafusion` workspace dependency

---

## Related issues

- [#1797](https://github.com/apache/iceberg-rust/issues/1797) — Community discussion about making iceberg-rust more embeddable/usable
- [#2269](https://github.com/apache/iceberg-rust/issues/2269) — [EPIC] Implement Missing Write Actions (write operations need bounded execution for large tables)
- [#2711](https://github.com/apache/iceberg-rust/issues/2711) — DataFusion non-append InsertOp is silently committed as append (broader gap in DataFusion integration completeness)

---

## Willingness to contribute

I can contribute this feature independently.
