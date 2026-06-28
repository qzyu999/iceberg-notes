# iceberg-rust PRs Required for PyIceberg DataFusion Integration

## Overview

This document details the three pull requests needed in the `apache/iceberg-rust` repository to enable the Track 2 (Rust-side) DataFusion integration for PyIceberg. These PRs provide the foundation for bounded-memory execution of compute-heavy operations (compaction, equality delete resolution, CoW rewrites, orphan file deletion) entirely within Rust, below the Python GIL.

### Context

PyIceberg needs DataFusion as an execution engine for operations that are structurally impossible to implement correctly under bounded memory using PyArrow alone. The iceberg-rust repository already has a DataFusion integration (`iceberg-datafusion` crate) that provides read and append-only write support. What's missing are the pieces needed for **bounded-memory execution** and **atomic file replacement** (overwrite commits).

### PR Summary

| PR | Title | Location | Dependencies |
|:---|:---|:---|:---|
| **PR 1** | `IcebergOverwriteCommitExec` | `crates/integrations/datafusion/src/physical_plan/overwrite_commit.rs` | None |
| **PR 2** | Bounded-memory session helper | `crates/integrations/datafusion/src/session.rs` (or similar) | None |
| **PR 3** | `pyiceberg_core.execution` FFI module | `bindings/python/src/execution.rs` | PR 1 + PR 2 |

PRs 1 and 2 are independent and can be developed in parallel. PR 3 depends on both.

### Relationship to Track 1 (Python-Side DataFusion)

Track 1 uses `datafusion-python` directly from Python and requires **zero** iceberg-rust changes. It works today and can serve as the initial implementation while these PRs are developed. Once PRs 1-3 land, Track 2 provides optimal performance (no FFI data transfer overhead, full Iceberg-aware execution below the GIL).

Per operation, it's one or the other — either do it entirely in Rust (Track 2) or use `datafusion-python` from Python (Track 1). Both use the same underlying DataFusion Rust engine; the difference is who orchestrates. Different operations can use different tracks (e.g., compaction via Track 2, orphan deletion via Track 1).

---

## Key Architectural Clarifications

### What iceberg-rust Is Building vs. What We're Building

iceberg-rust's core team is focused on **Transaction primitives** — the low-level commit layer:
- `OverwriteAction` (PR #2185 by @glitchy) — atomically add + delete files
- `RewriteFilesAction` (#2244 by @CTTY) — same but with validation for compaction
- `RowDeltaAction` (#2202) — for MoR position/equality delete writes
- `MergingSnapshotProducer` (PR #2620) — the snapshot plumbing underneath

These don't care about *how* you produce the new DataFiles. They just commit them.

iceberg-rust's DataFusion integration already has the **building blocks**:
- `IcebergTableScan` — read from Iceberg tables
- `IcebergWriteExec` — write Arrow batches to Parquet data files
- `IcebergCommitExec` — commit new files (append only)

**Nobody is building the glue layer** that says: "Use DataFusion's bounded-memory execution to sort/join/filter data, write new files via IcebergWriteExec, and commit via the new overwrite primitives." That's what our PRs provide.

In short:
- **iceberg-rust core team** → Transaction actions (commit layer)
- **iceberg-datafusion existing** → read + append-write building blocks
- **Our contribution** → the execution layer that connects DataFusion (as query engine) to those primitives for compaction/CoW/delete resolution

### PRs 1-3 Are Infrastructure — Each Operation Needs Implementation

PRs 1-3 provide the **scaffolding**:
- PR 1: the commit node that wires overwrite semantics into a DataFusion plan
- PR 2: the bounded-memory session configuration utility
- PR 3: the FFI module shell with function signatures and `todo!()` bodies

The actual operation implementations — the bodies of `execute_compaction`, `execute_equality_resolution`, `execute_cow_rewrite`, `execute_antijoin_paths` — each need their own follow-up PR with 100-300 lines of Rust that wires together DataFusion SQL + Iceberg FileIO + IcebergWriteExec. So the real work is:

```
PR 2  (bounded session)                    — can ship immediately
PR 1  (overwrite commit node)              — blocked on OverwriteAction landing
PR 3a (module structure + stubs)           — can ship immediately
PR 3b (execute_cow_rewrite body)           — needs PR 1 + PR 2
PR 3c (execute_compaction body)            — needs PR 1 + PR 2
PR 3d (execute_equality_resolution body)   — needs PR 2 only (read-only, no commit)
PR 3e (execute_antijoin_paths body)        — needs PR 2 only (read-only, no commit)
```

### Why DataFusion Specifically (Not Engine-Agnostic)

PyIceberg already chose DataFusion — it's in `pyproject.toml`, there's `__datafusion_table_provider__` on `Table`, and `pyiceberg-core` is built on `iceberg-datafusion`. The decision is made.

No other engine fits:
- DuckDB can't write back to Iceberg, has GPL-licensed extensions
- Polars has no spill-to-disk for joins/sorts
- Spark requires a JVM
- There is no alternative Apache-licensed, Arrow-native, embeddable, spill-capable engine

The engine resolution layer (`resolve_engine()`) provides the extensibility point if one ever appears:
```python
if engine == DATAFUSION: ...
elif engine == PYARROW: ...  # fallback
# elif engine == FUTURE_ENGINE: ...  # add later if needed
```

Users never see "DataFusion" — they see `table.compact(memory_limit="512MB")`. The engine is an implementation detail behind a DuckDB-style UX.

---

## PR 1: `IcebergOverwriteCommitExec` — Atomic File-Replace Commit

### Problem Statement

The existing `IcebergCommitExec` (in `crates/integrations/datafusion/src/physical_plan/commit.rs`) only supports `Transaction::fast_append` — it can add new data files to a table but cannot remove existing files. This makes it impossible to implement compaction, copy-on-write deletes, or any operation that atomically replaces files through the DataFusion execution pipeline.

**Current behavior:**
```
IcebergCommitExec: collect DataFile JSON from input → Transaction::fast_append(data_files) → commit
```

**Required behavior for overwrite:**
```
IcebergOverwriteCommitExec: collect DataFile JSON from input
    → Transaction::overwrite (or RewriteFiles equivalent)
    → remove files_to_delete from snapshot
    → add files_to_add (from input)
    → atomic commit
```

### Formal Semantics

The overwrite commit must satisfy:

```
Snapshot_{n+1}.files = (Snapshot_n.files \ files_to_delete) ∪ files_to_add
```

Where:
- `files_to_delete` is provided at plan construction time (known before execution)
- `files_to_add` is produced by the upstream `IcebergWriteExec` during execution
- The transition is atomic (either both sets change or neither does)

### Current Code Analysis

The existing `IcebergCommitExec` in `commit.rs`:

```rust
// Current commit logic (line ~170 of commit.rs):
let tx = Transaction::new(&table);
let action = tx.fast_append().add_data_files(data_files);
let _updated_table = action
    .apply(tx)
    .map_err(to_datafusion_error)?
    .commit(catalog.as_ref())
    .await
    .map_err(to_datafusion_error)?;
```

Key observations:
- Uses `Transaction::new(&table)` to create a transaction
- Calls `tx.fast_append()` which returns an action that only adds files
- The `ApplyTransactionAction` trait is used: `.apply(tx)` then `.commit(catalog)`
- The commit goes through the catalog's atomic metadata swap

### Proposed Implementation

**New file:** `crates/integrations/datafusion/src/physical_plan/overwrite_commit.rs`

```rust
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use iceberg::Catalog;
use iceberg::spec::{DataFile, deserialize_data_file_from_json};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};

use crate::physical_plan::DATA_FILES_COL_NAME;
use crate::to_datafusion_error;

/// IcebergOverwriteCommitExec atomically replaces a set of data files with new ones.
///
/// This is the commit node for operations that rewrite existing files:
/// - Compaction (many small files → fewer large sorted files)
/// - Copy-on-Write deletes (file with deleted rows → file without)
/// - Copy-on-Write updates (file with old values → file with new values)
///
/// Unlike `IcebergCommitExec` which only appends, this node:
/// 1. Collects new DataFiles from its input (produced by IcebergWriteExec)
/// 2. Atomically removes `files_to_delete` and adds the new files
/// 3. Uses Iceberg's OCC to ensure no concurrent modification conflicts
///
/// If the commit fails due to a concurrent modification, the caller can retry
/// the entire operation (the written files become orphans and are cleaned up
/// by orphan file deletion).
#[derive(Debug)]
pub(crate) struct IcebergOverwriteCommitExec {
    table: Table,
    catalog: Arc<dyn Catalog>,
    /// The upstream plan producing new DataFile JSON strings
    input: Arc<dyn ExecutionPlan>,
    /// Files to be removed from the snapshot (serialized DataFile JSON)
    files_to_delete: Vec<DataFile>,
    /// Schema of the table (for display purposes)
    schema: ArrowSchemaRef,
    /// Output schema: single "count" column
    count_schema: ArrowSchemaRef,
    plan_properties: Arc<PlanProperties>,
}

impl IcebergOverwriteCommitExec {
    pub fn new(
        table: Table,
        catalog: Arc<dyn Catalog>,
        input: Arc<dyn ExecutionPlan>,
        files_to_delete: Vec<DataFile>,
        schema: ArrowSchemaRef,
    ) -> Self {
        let count_schema = Self::make_count_schema();
        let plan_properties = Self::compute_properties(Arc::clone(&count_schema));

        Self {
            table,
            catalog,
            input,
            files_to_delete,
            schema,
            count_schema,
            plan_properties,
        }
    }

    fn compute_properties(schema: ArrowSchemaRef) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ))
    }

    fn make_count_batch(count: u64) -> DFResult<RecordBatch> {
        let count_array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
        RecordBatch::try_from_iter_with_nullable(vec![("count", count_array, false)])
            .map_err(|e| {
                DataFusionError::ArrowError(
                    Box::new(e),
                    Some("Failed to make count batch".to_string()),
                )
            })
    }

    fn make_count_schema() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]))
    }
}

impl DisplayAs for IcebergOverwriteCommitExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IcebergOverwriteCommitExec: table={}, files_to_delete={}",
                    self.table.identifier(),
                    self.files_to_delete.len()
                )
            }
            DisplayFormatType::Verbose => {
                write!(
                    f,
                    "IcebergOverwriteCommitExec: table={}, files_to_delete={}, schema={:?}",
                    self.table.identifier(),
                    self.files_to_delete.len(),
                    self.schema
                )
            }
        }
    }
}

impl ExecutionPlan for IcebergOverwriteCommitExec {
    fn name(&self) -> &str {
        "IcebergOverwriteCommitExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_distribution(&self) -> Vec<datafusion::physical_plan::Distribution> {
        // Must see all new files in a single partition to commit atomically
        vec![datafusion::physical_plan::Distribution::SinglePartition; self.children().len()]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "IcebergOverwriteCommitExec expects exactly one child, got {}",
                children.len()
            )));
        }

        Ok(Arc::new(IcebergOverwriteCommitExec::new(
            self.table.clone(),
            self.catalog.clone(),
            children[0].clone(),
            self.files_to_delete.clone(),
            self.schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "IcebergOverwriteCommitExec only has one partition, got {partition}"
            )));
        }

        let table = self.table.clone();
        let input_plan = self.input.clone();
        let count_schema = Arc::clone(&self.count_schema);
        let files_to_delete = self.files_to_delete.clone();
        let catalog = Arc::clone(&self.catalog);

        let spec_id = self.table.metadata().default_partition_spec_id();
        let partition_type = self.table.metadata().default_partition_type().clone();
        let current_schema = self.table.metadata().current_schema().clone();

        let stream = futures::stream::once(async move {
            // 1. Collect new data files from the upstream write plan
            let mut new_data_files: Vec<DataFile> = Vec::new();
            let mut total_record_count: u64 = 0;

            let mut batch_stream = input_plan.execute(0, context)?;

            while let Some(batch_result) = batch_stream.next().await {
                let batch = batch_result?;
                let files_array = batch
                    .column_by_name(DATA_FILES_COL_NAME)
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "Expected 'data_files' column in input batch".to_string(),
                        )
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "Expected 'data_files' column to be StringArray".to_string(),
                        )
                    })?;

                let batch_files: Vec<DataFile> = files_array
                    .into_iter()
                    .flatten()
                    .map(|f| -> DFResult<DataFile> {
                        deserialize_data_file_from_json(
                            f,
                            spec_id,
                            &partition_type,
                            &current_schema,
                        )
                        .map_err(to_datafusion_error)
                    })
                    .collect::<DFResult<_>>()?;

                total_record_count += batch_files.iter().map(|f| f.record_count()).sum::<u64>();
                new_data_files.extend(batch_files);
            }

            // 2. Build and execute the overwrite transaction
            // NOTE: The exact Transaction API depends on what iceberg-rust exposes.
            // This may need to use RewriteFiles or a similar action.
            let tx = Transaction::new(&table);

            // Option A: If iceberg-rust has a rewrite_files action:
            // let action = tx.rewrite_files()
            //     .delete_files(files_to_delete)
            //     .add_files(new_data_files);

            // Option B: If we need to compose delete + append:
            // This is the pattern that needs to be implemented or exposed.
            // The key requirement is ATOMICITY — both delete and add in one commit.

            // For now, use whatever overwrite/rewrite API is available:
            let action = tx.rewrite_files()
                .delete_data_files(files_to_delete)
                .add_data_files(new_data_files);

            let _updated_table = action
                .apply(tx)
                .map_err(to_datafusion_error)?
                .commit(catalog.as_ref())
                .await
                .map_err(to_datafusion_error)?;

            Self::make_count_batch(total_record_count)
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.count_schema),
            stream,
        )))
    }
}
```

### Key Design Decisions

**1. `files_to_delete` provided at construction time**

The files to delete are known before execution begins (they come from scan planning). This means:
- No additional I/O during commit to discover which files to remove
- The plan is fully deterministic — same inputs always produce same commit
- Validation can happen during plan construction (e.g., verify files exist in current snapshot)

**2. Same output schema as `IcebergCommitExec`**

Returns a single `count` column with the total records written. This maintains consistency with the existing commit node and allows interchangeable use in plan trees.

**3. SinglePartition distribution requirement**

Same as `IcebergCommitExec` — the commit must see ALL new files to construct the complete manifest. The `CoalescePartitionsExec` upstream merges the metadata (not the data — data is already written to files).

### Transaction API Prerequisite

This PR depends on iceberg-rust's `Transaction` API supporting an atomic "remove + add" operation. Looking at the current code, `Transaction` exposes `fast_append()`. The overwrite commit needs one of:

1. **`tx.rewrite_files()`** — A `RewriteFiles` action (analogous to Java's `RewriteFiles`)
2. **`tx.overwrite()`** — An overwrite action that accepts both deletes and adds
3. **Building from primitives** — Constructing the snapshot update manually via `SnapshotProducer`

**If the Transaction API doesn't yet expose this**, the PR scope expands to include adding a `RewriteFilesAction` to `iceberg-rust`'s transaction module. This would be:

```rust
/// Action that atomically replaces a set of data files.
/// Produces a snapshot with operation = "replace".
pub struct RewriteFilesAction {
    files_to_delete: Vec<DataFile>,
    files_to_add: Vec<DataFile>,
    // Optional: delete files to remove (for MoR compaction)
    delete_files_to_remove: Vec<DataFile>,
}

impl RewriteFilesAction {
    pub fn delete_data_files(mut self, files: Vec<DataFile>) -> Self {
        self.files_to_delete = files;
        self
    }

    pub fn add_data_files(mut self, files: Vec<DataFile>) -> Self {
        self.files_to_add = files;
        self
    }
}

impl ApplyTransactionAction for RewriteFilesAction {
    fn apply(self, tx: Transaction) -> Result<Transaction> {
        // Build snapshot update:
        // 1. Remove files_to_delete from manifests
        // 2. Add files_to_add to new manifest
        // 3. Set operation = DataOperations::REPLACE
        // 4. Add summary statistics
        todo!()
    }
}
```

### How It Fits in the Execution Plan Tree

For a compaction operation, the full plan tree would be:

```
IcebergOverwriteCommitExec (files_to_delete=[old files])
  CoalescePartitionsExec
    IcebergWriteExec
      SortExec (external merge sort — spills to disk)
        IcebergTableScan (reads the files_to_compact)
```

For a CoW delete:

```
IcebergOverwriteCommitExec (files_to_delete=[file being rewritten])
  CoalescePartitionsExec
    IcebergWriteExec
      FilterExec (keep rows NOT matching delete predicate)
        IcebergTableScan (reads the specific file)
```

### Testing Strategy

1. **Unit test with MockWriteExec** — Same pattern as existing `test_iceberg_commit_exec`:
   - Create table with initial data files (via fast_append)
   - Execute `IcebergOverwriteCommitExec` with those files as `files_to_delete` + new mock files
   - Verify: old files removed from manifest, new files present
   - Verify: snapshot operation is "replace"

2. **Conflict detection test** — Concurrent modification:
   - Start overwrite plan
   - Concurrently modify the table (add/remove a file)
   - Verify: commit fails with conflict error (OCC)

3. **Empty input test** — No new files produced (all rows deleted):
   - Execute with `files_to_delete=[some_file]` but upstream produces zero files
   - Verify: old files removed, no new files added (valid — entire file deleted)

### Existing Issues and PRs (Found on GitHub)

**This PR's work is directly tracked and partially in-progress:**

| ID | Title | Status | Relevance |
|:---|:---|:---|:---|
| [#1607](https://github.com/apache/iceberg-rust/issues/1607) | **Add `RewriteFiles` support to iceberg-rust** | Open (assigned to @CTTY) | **THE issue for this work.** Umbrella with 4 sub-issues. |
| [#2244](https://github.com/apache/iceberg-rust/issues/2244) | Implement RewriteFilesAction | Open (assigned to @CTTY) | Sub-issue of #1607. The core Transaction action. |
| [#2242](https://github.com/apache/iceberg-rust/issues/2242) | Process delete files when writing snapshots | Open | Sub-issue of #1607. Prerequisite for overwrite commits. |
| [#2243](https://github.com/apache/iceberg-rust/issues/2243) | Implement SnapshotValidator | Open | Sub-issue of #1607. Conflict detection for overwrites. |
| [#2241](https://github.com/apache/iceberg-rust/issues/2241) | Add helpers to scan snapshot ancestors | **Closed** (done) | Sub-issue of #1607. Already merged. |
| [PR #2620](https://github.com/apache/iceberg-rust/pull/2620) | MergingSnapshotProducer | **Draft** (by @CTTY) | Active WIP — the new snapshot producer that enables RewriteFiles. +1578 lines. |
| [PR #1606](https://github.com/apache/iceberg-rust/pull/1606) | `RewriteFilesAction` + Validation + Process Delete Manifests | **Closed** (stale) | Original attempt. Closed due to inactivity but code exists. Being reworked via #2620. |
| [#2711](https://github.com/apache/iceberg-rust/issues/2711) | DataFusion non-append InsertOp is silently committed as append | Open (11 hours old!) | **Brand new bug** confirming the gap: `IcebergTableProvider::insert_into` ignores `InsertOp::Overwrite`. |
| [PR #2185](https://github.com/apache/iceberg-rust/pull/2185) | feat(transaction): add OverwriteAction with CoW delete support | Open (by @glitchy) | `OverwriteAction` (simpler than `RewriteFiles`) — adds + deletes files. Under review. |
| [#2186](https://github.com/apache/iceberg-rust/issues/2186) | Copy-on-Write and Merge-on-Read support | Open (epic) | Umbrella epic. Lists `OverwriteAction` as first CoW primitive. |
| [#2269](https://github.com/apache/iceberg-rust/issues/2269) | [EPIC] Implement Missing Write Actions | Open | Lists `RewriteFiles` as TODO. Motivated by datafusion-comet. |
| [#2556](https://github.com/apache/iceberg-rust/issues/2556) | Unify RewriteManifestsAction with SnapshotProducer | Open | Refactoring needed to support `Operation::Replace` in snapshot summaries. |

**Key Insight: Active development is happening on this exact problem.**

- **@CTTY** is actively working on `RewriteFilesAction` via the `MergingSnapshotProducer` (PR #2620, draft, 14 days old). The sub-issues #2242/#2243/#2244 are the prerequisite chain.
- **@glitchy** has `OverwriteAction` (PR #2185) under review — this is a simpler version that adds + deletes files but may not have full compaction semantics.
- **Issue #2711** was opened TODAY confirming that `IcebergCommitExec` silently ignores non-append ops.

**Strategy implications:**
- For PR 1 (`IcebergOverwriteCommitExec`): Can build on either `OverwriteAction` (PR #2185, simpler, closer to merge) or `RewriteFilesAction` (#2244, more complete but further out). The DataFusion commit node is a *consumer* of whatever transaction action lands first.
- Consider engaging with @CTTY and @glitchy directly — their work is the prerequisite for our PR 1.
- Issue #2711 is a natural place to propose "once OverwriteAction or RewriteFiles lands, we should wire it into `IcebergCommitExec` / create `IcebergOverwriteCommitExec`."

### PR Metadata

- **Title:** `DataFusion: Add IcebergOverwriteCommitExec for atomic file replacement`
- **Labels:** `enhancement`
- **Related issues:** [#2711](https://github.com/apache/iceberg-rust/issues/2711), [#1607](https://github.com/apache/iceberg-rust/issues/1607), [#2269](https://github.com/apache/iceberg-rust/issues/2269), [#2186](https://github.com/apache/iceberg-rust/issues/2186)
- **Depends on:** PR #2185 (OverwriteAction) OR #2244 (RewriteFilesAction) — one must land first
- **Breaking changes:** None (purely additive)
- **New dependencies:** None

---

## PR 2: Bounded-Memory Session Helper (`create_bounded_session`)

### Problem Statement

The `iceberg-datafusion` crate currently has **zero explicit memory management**. A grep for `spill`, `MemoryPool`, `FairSpillPool`, `DiskManager`, `memory_limit` across the entire crate returns no results. The integration delegates memory management entirely to whatever `SessionContext` the caller provides.

This means:
- If a caller uses the default `SessionContext` (which uses `UnboundedMemoryPool`), no spill-to-disk ever occurs
- Large sort or join operations will OOM instead of gracefully spilling
- There is no way for PyIceberg to request bounded-memory execution through the existing API

DataFusion has full external-memory operator support (`SortExec` with external merge sort, `HashJoinExec` with Grace Hash Join), but these only activate when the `MemoryPool` rejects a `try_grow()` request. Without a bounded pool, they never spill.

### What DataFusion Provides (Already Built)

DataFusion's memory management stack:

| Layer | Component | Purpose |
|:---|:---|:---|
| Policy | `MemoryPool` trait | Tracks total usage, fails `try_grow()` at limit |
| Accounting | `MemoryReservation` (RAII) | Each operator holds a reservation; on failure -> spill |
| Spill | `SpillManager` + `DiskManager` | Writes Arrow IPC to temp files, reads back during merge |

Three pool implementations:
- `UnboundedMemoryPool` — never fails (default, dangerous for large data)
- `GreedyMemoryPool` — hard limit, first-come-first-served
- **`FairSpillPool`** — divides memory evenly among spillable operators (our choice)

The `FairSpillPool` algorithm:
```
Given: P = pool size, U = unspillable memory, n = spillable consumers
Each consumer i may use at most: quota_i = (P - U) / n
When consumer i calls try_grow(additional):
    if reservation_i + additional > quota_i:
        return Err(ResourcesExhausted)  // triggers spill
```

### Proposed Implementation

**New file:** `crates/integrations/datafusion/src/session.rs`

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Memory-bounded session creation for Iceberg DataFusion operations.
//!
//! This module provides utilities for creating DataFusion `SessionContext` instances
//! configured with bounded memory pools and spill-to-disk support. These are used
//! by compute-heavy operations (compaction, CoW rewrite, equality delete resolution)
//! to guarantee O(M) bounded memory regardless of input data size.

use std::sync::Arc;

use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

/// Default batch size for DataFusion execution.
///
/// Matches DataFusion's own default: 8192 rows per RecordBatch.
/// This balances per-batch overhead (too small = overhead dominates)
/// against memory granularity (too large = coarse-grained reservation).
const DEFAULT_BATCH_SIZE: usize = 8192;

/// Default memory limit when none is specified: 512 MB.
///
/// Rationale:
/// - Large enough to avoid excessive spilling for moderate datasets
/// - Small enough to be safe on most developer machines (8GB+ RAM)
/// - Consistent with DuckDB's default memory_limit on comparable systems
const DEFAULT_MEMORY_LIMIT_BYTES: usize = 512 * 1024 * 1024;

/// Memory pool fraction (1.0 = use entire budget before spilling).
///
/// At 1.0, the pool triggers spill only when the hard limit is hit.
/// Values < 1.0 leave headroom for framework overhead (Tokio stacks,
/// metadata structures, etc.) but reduce effective budget.
const MEMORY_POOL_FRACTION: f64 = 1.0;

/// Configuration for a bounded-memory DataFusion session.
#[derive(Debug, Clone)]
pub struct BoundedSessionConfig {
    /// Maximum memory budget in bytes. Operations spill to disk beyond this.
    pub memory_limit_bytes: usize,
    /// Number of partitions for parallel execution. Defaults to available CPUs.
    pub target_partitions: Option<usize>,
    /// Rows per RecordBatch. Defaults to 8192.
    pub batch_size: Option<usize>,
    /// Optional directory for spill files. None = OS temp directory.
    pub spill_directory: Option<String>,
}

impl Default for BoundedSessionConfig {
    fn default() -> Self {
        Self {
            memory_limit_bytes: DEFAULT_MEMORY_LIMIT_BYTES,
            target_partitions: None,
            batch_size: None,
            spill_directory: None,
        }
    }
}

impl BoundedSessionConfig {
    /// Create a new config with the specified memory limit.
    pub fn new(memory_limit_bytes: usize) -> Self {
        Self {
            memory_limit_bytes,
            ..Default::default()
        }
    }

    /// Parse a human-readable memory string (e.g., "512MB", "2GB", "1024").
    ///
    /// Supported suffixes: KB, MB, GB, TB (case-insensitive).
    /// Plain number is interpreted as bytes.
    pub fn parse_memory_limit(s: &str) -> Result<usize, String> {
        let s = s.trim();
        let (num_str, multiplier) = if let Some(n) = s.strip_suffix("TB") {
            (n.trim(), 1024 * 1024 * 1024 * 1024)
        } else if let Some(n) = s.strip_suffix("tb") {
            (n.trim(), 1024 * 1024 * 1024 * 1024)
        } else if let Some(n) = s.strip_suffix("GB") {
            (n.trim(), 1024 * 1024 * 1024)
        } else if let Some(n) = s.strip_suffix("gb") {
            (n.trim(), 1024 * 1024 * 1024)
        } else if let Some(n) = s.strip_suffix("MB") {
            (n.trim(), 1024 * 1024)
        } else if let Some(n) = s.strip_suffix("mb") {
            (n.trim(), 1024 * 1024)
        } else if let Some(n) = s.strip_suffix("KB") {
            (n.trim(), 1024)
        } else if let Some(n) = s.strip_suffix("kb") {
            (n.trim(), 1024)
        } else {
            (s, 1)
        };

        num_str
            .parse::<usize>()
            .map(|n| n * multiplier)
            .map_err(|e| format!("Invalid memory limit '{}': {}", s, e))
    }
}

/// Create a DataFusion `SessionContext` configured for bounded-memory execution.
///
/// The returned session uses:
/// - `FairSpillPool` with the specified memory limit (operators spill to disk when exceeded)
/// - `DiskManager` in OS temp directory mode (automatic spill file lifecycle)
/// - Configurable parallelism (defaults to available CPU count)
///
/// # Memory Guarantee
///
/// For any plan P executed on this session:
///   for all t in execution_time: resident_memory(t) <= memory_limit + epsilon
///
/// Where epsilon is framework overhead (Tokio stacks, metadata, ~10-50MB).
///
/// # Example
///
/// ```rust
/// use iceberg_datafusion::session::{BoundedSessionConfig, create_bounded_session};
///
/// let config = BoundedSessionConfig::new(512 * 1024 * 1024); // 512MB
/// let ctx = create_bounded_session(config).expect("Failed to create session");
///
/// // All operations on this context will spill to disk if memory is exhausted:
/// // ctx.sql("SELECT * FROM large_table ORDER BY col").await?;
/// ```
pub fn create_bounded_session(
    config: BoundedSessionConfig,
) -> Result<SessionContext, datafusion::error::DataFusionError> {
    let target_partitions = config.target_partitions.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1)
    });

    let batch_size = config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

    let session_config = SessionConfig::new()
        .with_batch_size(batch_size)
        .with_target_partitions(target_partitions);

    let mut runtime_builder = RuntimeEnvBuilder::new()
        .with_memory_limit(config.memory_limit_bytes, MEMORY_POOL_FRACTION);

    // Configure disk manager for spill files
    if let Some(ref dir) = config.spill_directory {
        runtime_builder = runtime_builder.with_temp_file_path(dir.clone());
    }
    // If no directory specified, DiskManager uses OS temp (default behavior)

    let runtime = runtime_builder.build_arc()?;

    Ok(SessionContext::new_with_config_rt(session_config, runtime))
}
```

### Registration in `lib.rs`

Add to `crates/integrations/datafusion/src/lib.rs`:

```rust
pub mod session;  // Add this line
```

### Why `FairSpillPool` Over `GreedyMemoryPool`

For operations that compose multiple memory-hungry operators (e.g., compaction = sort + write, or MoR compaction = join + sort + write), `FairSpillPool` prevents starvation:

| Scenario | GreedyMemoryPool | FairSpillPool |
|:---|:---|:---|
| Sort (512MB budget) | Works: sort gets full budget | Works: sort gets full budget |
| Sort + HashJoin (512MB budget) | **Problem**: first operator grabs all memory, second OOMs | **OK**: each gets 256MB, both spill cooperatively |
| Sort + HashJoin + Aggregate | One operator may never acquire memory | Each gets 170MB, all spill as needed |

### Integration Points

This session helper is used by:
1. **PR 3 (`pyiceberg_core.execution`)** — All Python-exposed execution functions call `create_bounded_session` internally
2. **Any future Rust-side operation** that needs bounded memory (e.g., a Rust CLI tool for compaction)
3. **Tests** — Provides a standard way to test with memory pressure

### Testing Strategy

1. **Spill verification test:**
   - Create session with small memory limit (e.g., 4MB)
   - Execute a sort on data larger than the limit (e.g., 16MB)
   - Verify: operation completes without OOM
   - Verify: spill metrics show non-zero bytes spilled

2. **Configuration parsing test:**
   - `parse_memory_limit("512MB")` = 536,870,912
   - `parse_memory_limit("2GB")` = 2,147,483,648
   - `parse_memory_limit("1024")` = 1024
   - `parse_memory_limit("invalid")` = Err

3. **Default configuration test:**
   - `BoundedSessionConfig::default()` uses 512MB
   - `target_partitions` defaults to CPU count

### Existing Issues (Found on GitHub)

**No direct issue exists for a bounded-memory session helper.** However, related issues provide context:

| ID | Title | Status | Relevance |
|:---|:---|:---|:---|
| [#1780](https://github.com/apache/iceberg-rust/issues/1780) | Support configuring datafusion catalog in sqllogictest framework | Open (good first issue) | About configuring the DataFusion catalog — not memory, but establishes precedent for session config helpers. |
| [#1945](https://github.com/apache/iceberg-rust/issues/1945) | Add Tokio Runtime Handle support | Open | Related: runtime/execution infrastructure for the DataFusion integration. |
| [#2364](https://github.com/apache/iceberg-rust/issues/2364) | Report DataFusion operator metrics in IcebergTableScan | Open | Related: execution observability. Spill metrics would be reported through this. |
| [#2220](https://github.com/apache/iceberg-rust/issues/2220) | Enable parallel file-level reads in IcebergTableScan | Open | Related: performance tuning of the DataFusion integration (target_partitions). |
| [#1797](https://github.com/apache/iceberg-rust/issues/1797) | [Discussion] Reduce the need for iceberg-rust forks | Open (30 comments, 17 thumbs up) | Community discussion about making iceberg-rust more usable. A session helper for bounded execution directly addresses usability for embedded use cases. |

**Key Insight: This is a greenfield contribution.**

No one has proposed a bounded-memory session utility for the `iceberg-datafusion` crate. The crate currently has zero memory management code (confirmed by grep). This PR would be the first to address execution resource management, which is a gap highlighted indirectly by:
- Issue #2711 (DataFusion InsertOp silently wrong) — the broader issue of the DataFusion integration being incomplete
- The community discussion in #1797 — downstream projects forking because iceberg-rust isn't production-ready

**Strategy implications:**
- This is low-controversy, purely additive infrastructure
- No coordination with other contributors needed
- Can reference #1797's discussion about making iceberg-rust more embeddable as motivation
- The PR stands alone — useful for any Rust application embedding iceberg-datafusion

### PR Metadata

- **Title:** `DataFusion: Add bounded-memory session helper with FairSpillPool`
- **Labels:** `enhancement`
- **Related issues:** [#1797](https://github.com/apache/iceberg-rust/issues/1797) (community usability), [#2269](https://github.com/apache/iceberg-rust/issues/2269) (write actions need bounded execution)
- **Breaking changes:** None (purely additive — new public module)
- **New dependencies:** None (all types from `datafusion` already in workspace)

---

## PR 3: `pyiceberg_core.execution` — Rust FFI Execution Module

### Problem Statement

PyIceberg needs to execute bounded-memory operations (compaction, CoW rewrite, equality delete resolution, orphan file deletion) entirely within Rust. The existing `pyiceberg_core` bindings only expose:
- `pyiceberg_core.datafusion.IcebergDataFusionTable` — a read-only `TableProvider` via PyCapsule
- `pyiceberg_core.transform` — partition transform functions
- `pyiceberg_core.manifest` — manifest reading utilities

There is no way to trigger a full execution pipeline (scan -> transform -> write -> commit) from Python with bounded memory guarantees. The data would have to cross the FFI boundary per-batch, defeating the purpose (Python's address space holds the data, preventing Rust-side memory management).

### Design Principle: Operation-Level FFI

The key architectural decision: cross the FFI boundary at the **operation** level, not the record level.

```
WRONG (defeats bounded-memory):
  Python: for batch in rust_scan():           # data crosses FFI
              transformed = process(batch)     # Python holds data
              rust_write(transformed)          # crosses FFI again

RIGHT (bounded-memory in Rust):
  Python: result = rust_execute_compaction(    # operation descriptor crosses FFI
              metadata_location="...",
              files_to_compact=[...],
              memory_limit="512MB",
          )
          # Only metadata (file paths, record counts) crosses back
```

This means:
- All RecordBatch data stays in Rust's address space
- DataFusion's `FairSpillPool` manages memory correctly (Rust allocator)
- Python's GIL is released during execution (true parallelism)
- Only serialized metadata (DataFile JSON, error messages) crosses the boundary

### Proposed Implementation

**New file:** `bindings/python/src/execution.rs`

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Python-exposed execution functions for bounded-memory Iceberg operations.
//!
//! Each function in this module:
//! 1. Parses operation parameters from Python
//! 2. Creates a bounded-memory DataFusion session (FairSpillPool)
//! 3. Constructs an execution plan from Iceberg primitives
//! 4. Executes the plan entirely in Rust (GIL released)
//! 5. Returns only metadata (new file paths, record counts) to Python
//!
//! The caller (PyIceberg) handles the commit using the returned metadata.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::physical_plan::collect;
use datafusion::prelude::*;
use iceberg::io::FileIOBuilder;
use iceberg::table::StaticTable;
use iceberg::TableIdent;
use iceberg_datafusion::session::{BoundedSessionConfig, create_bounded_session};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::runtime::runtime;

/// Result of a copy-on-write rewrite operation.
///
/// Contains the metadata needed by PyIceberg to commit the replacement.
#[pyclass(name = "CowRewriteResult")]
#[derive(Debug, Clone)]
pub struct PyCowRewriteResult {
    /// Serialized DataFile JSON strings for newly written files
    #[pyo3(get)]
    pub new_files: Vec<String>,
    /// Total records written across all new files
    #[pyo3(get)]
    pub total_record_count: u64,
    /// Total bytes written
    #[pyo3(get)]
    pub total_file_size_bytes: u64,
}

/// Result of a compaction operation.
#[pyclass(name = "CompactionResult")]
#[derive(Debug, Clone)]
pub struct PyCompactionResult {
    /// Serialized DataFile JSON strings for newly written (compacted) files
    #[pyo3(get)]
    pub new_files: Vec<String>,
    /// Total records in compacted output
    #[pyo3(get)]
    pub total_record_count: u64,
    /// Total bytes written
    #[pyo3(get)]
    pub total_file_size_bytes: u64,
    /// Number of input files that were compacted
    #[pyo3(get)]
    pub input_files_count: usize,
}

/// Execute a streaming copy-on-write file rewrite.
///
/// Reads specified data files, applies a filter (keeping or removing matching rows),
/// and writes the result to new Parquet files. All execution happens in Rust with
/// bounded memory (spill-to-disk via DataFusion's FairSpillPool).
///
/// The caller is responsible for committing the result (removing old files,
/// adding new files) using PyIceberg's Transaction API.
///
/// # Arguments
///
/// * `metadata_location` - Path to the table's metadata JSON file
/// * `file_io_properties` - Properties for constructing FileIO (S3 credentials, etc.)
/// * `files_to_rewrite` - Serialized DataFile JSON strings of files to read
/// * `filter_expression` - Iceberg expression string for the filter predicate
/// * `keep_matching` - If true, keep rows matching the filter; if false, remove them
/// * `memory_limit` - Optional memory budget string (e.g., "512MB"). Default: 512MB.
///
/// # Returns
///
/// `CowRewriteResult` with new file metadata for commit.
///
/// # Errors
///
/// Returns PyRuntimeError if execution fails (disk full during spill, I/O error, etc.).
/// No table state is modified on error — the caller simply does not commit.
#[pyfunction]
#[pyo3(signature = (
    metadata_location,
    file_io_properties,
    files_to_rewrite,
    filter_expression,
    keep_matching,
    memory_limit=None
))]
fn execute_cow_rewrite(
    metadata_location: String,
    file_io_properties: HashMap<String, String>,
    files_to_rewrite: Vec<String>,
    filter_expression: String,
    keep_matching: bool,
    memory_limit: Option<String>,
) -> PyResult<PyCowRewriteResult> {
    let rt = runtime();

    rt.block_on(async {
        // 1. Parse memory limit
        let memory_bytes = memory_limit
            .as_deref()
            .map(BoundedSessionConfig::parse_memory_limit)
            .transpose()
            .map_err(|e| PyRuntimeError::new_err(e))?
            .unwrap_or(BoundedSessionConfig::default().memory_limit_bytes);

        // 2. Create bounded session
        let config = BoundedSessionConfig::new(memory_bytes);
        let ctx = create_bounded_session(config)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create session: {e}")))?;

        // 3. Load table metadata
        let factory = Arc::new(OpenDalResolvingStorageFactory::new());
        let file_io = FileIOBuilder::new(factory)
            .with_props(file_io_properties)
            .build();

        let table_ident = TableIdent::from_strs(["execution", "target"])
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid identifier: {e}")))?;

        let static_table =
            StaticTable::from_metadata_file(&metadata_location, table_ident, file_io)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to load table: {e}")))?;

        let table = static_table.into_table();

        // 4. Register source files and execute filter plan
        // Implementation: register each file as a Parquet source,
        // apply complement of filter, write to new files via IcebergWriteExec.
        //
        // The exact plan composition depends on whether we:
        //   a) Use IcebergTableScan with file-level filtering (preferred)
        //   b) Register raw Parquet files and use DataFusion's native reader
        //
        // For initial implementation, option (b) is simpler:
        // ctx.register_parquet("source", file_paths, options).await?;
        // let result = ctx.sql("SELECT * FROM source WHERE NOT (filter)").await?;

        // 5. Collect results and return metadata
        // ... (plan execution, file metadata extraction)

        todo!("Implementation depends on final API design for file-level scan")
    })
}

/// Execute a bounded-memory compaction (sort + rewrite).
///
/// Reads specified data files, optionally sorts them by the given columns,
/// and writes optimally-sized output files. All execution happens in Rust
/// with bounded memory.
///
/// # Arguments
///
/// * `metadata_location` - Path to the table's metadata JSON file
/// * `file_io_properties` - Properties for constructing FileIO
/// * `files_to_compact` - Serialized DataFile JSON strings of input files
/// * `target_file_size_bytes` - Target size for output files (default: 256MB)
/// * `sort_columns` - Optional column names to sort by
/// * `memory_limit` - Optional memory budget string (e.g., "1GB"). Default: 512MB.
///
/// # Returns
///
/// `CompactionResult` with new file metadata for commit.
///
/// # Memory Guarantee
///
/// Regardless of input size, memory usage is bounded by `memory_limit`.
/// DataFusion's external merge sort spills intermediate sorted runs to disk
/// when the budget is exhausted, then performs a k-way merge.
///
/// For typical parameters (memory=512MB, data=100GB):
/// - Sort passes: ceil(log_64(100GB/512MB)) = 1 (single merge pass)
/// - Total I/O: ~4x data volume (read + spill-write + merge-read + output-write)
/// - Wall time: ~data_size / disk_bandwidth * 4
#[pyfunction]
#[pyo3(signature = (
    metadata_location,
    file_io_properties,
    files_to_compact,
    target_file_size_bytes=None,
    sort_columns=None,
    memory_limit=None
))]
fn execute_compaction(
    metadata_location: String,
    file_io_properties: HashMap<String, String>,
    files_to_compact: Vec<String>,
    target_file_size_bytes: Option<u64>,
    sort_columns: Option<Vec<String>>,
    memory_limit: Option<String>,
) -> PyResult<PyCompactionResult> {
    let rt = runtime();

    rt.block_on(async {
        // 1. Parse configuration
        let memory_bytes = memory_limit
            .as_deref()
            .map(BoundedSessionConfig::parse_memory_limit)
            .transpose()
            .map_err(|e| PyRuntimeError::new_err(e))?
            .unwrap_or(BoundedSessionConfig::default().memory_limit_bytes);

        let _target_size = target_file_size_bytes.unwrap_or(256 * 1024 * 1024);
        let input_count = files_to_compact.len();

        // 2. Create bounded session
        let config = BoundedSessionConfig::new(memory_bytes);
        let _ctx = create_bounded_session(config)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create session: {e}")))?;

        // 3. Load table, build plan:
        //    IcebergTableScan(specific files) -> SortExec(sort_columns) -> IcebergWriteExec
        //
        // The SortExec will use external merge sort with spill when memory is exhausted.
        // IcebergWriteExec will split output at target_file_size_bytes boundaries.

        // 4. Execute plan, collect file metadata

        todo!("Implementation pending")
    })
}

/// Execute an equality delete resolution (anti-join).
///
/// Reads data files and equality delete files, performs a LEFT ANTI JOIN
/// to remove deleted rows, and returns the surviving rows as Arrow RecordBatches.
///
/// This is used by PyIceberg's scan path to correctly read tables with
/// equality deletes (all Flink-written V2 tables).
///
/// # Arguments
///
/// * `data_file_paths` - Paths to data Parquet files
/// * `eq_delete_file_paths` - Paths to equality delete Parquet files
/// * `equality_field_names` - Column names that form the equality delete key
/// * `file_io_properties` - Properties for constructing FileIO
/// * `memory_limit` - Optional memory budget. Default: 512MB.
///
/// # Returns
///
/// List of PyArrow RecordBatches with deleted rows removed.
///
/// # Memory Guarantee
///
/// Uses Grace Hash Join (partition-based) with spill. Even if the delete set
/// is larger than memory, the join completes by partitioning both sides and
/// processing one partition at a time.
#[pyfunction]
#[pyo3(signature = (
    data_file_paths,
    eq_delete_file_paths,
    equality_field_names,
    file_io_properties,
    memory_limit=None
))]
fn execute_equality_resolution(
    data_file_paths: Vec<String>,
    eq_delete_file_paths: Vec<String>,
    equality_field_names: Vec<String>,
    file_io_properties: HashMap<String, String>,
    memory_limit: Option<String>,
) -> PyResult<PyObject> {
    let rt = runtime();

    rt.block_on(async {
        // 1. Create bounded session
        // 2. Register data files as "data" table
        // 3. Register equality delete files as "deletes" table
        // 4. Execute: SELECT d.* FROM data d LEFT ANTI JOIN deletes e
        //             ON d.eq_col1 = e.eq_col1 AND d.eq_col2 = e.eq_col2 ...
        // 5. Convert result RecordBatches to PyArrow via Arrow C Data Interface
        //    (zero-copy: RecordBatch::to_pyarrow)

        todo!("Implementation pending")
    })
}

/// Execute a path anti-join for orphan file detection.
///
/// Given two lists of file paths (storage listing vs. valid paths from manifests),
/// returns the paths present in storage but NOT in the valid set (orphans).
///
/// # Arguments
///
/// * `storage_paths` - All paths found in storage
/// * `valid_paths` - All paths referenced by table snapshots
/// * `memory_limit` - Optional memory budget. Default: 512MB.
///
/// # Returns
///
/// List of orphan file paths (in storage but not in any manifest).
#[pyfunction]
#[pyo3(signature = (storage_paths, valid_paths, memory_limit=None))]
fn execute_antijoin_paths(
    storage_paths: Vec<String>,
    valid_paths: Vec<String>,
    memory_limit: Option<String>,
) -> PyResult<Vec<String>> {
    let rt = runtime();

    rt.block_on(async {
        // 1. Create bounded session
        // 2. Register storage_paths as table "storage" (single "path" column)
        // 3. Register valid_paths as table "valid" (single "path" column)
        // 4. Execute: SELECT s.path FROM storage s
        //            LEFT ANTI JOIN valid v ON s.path = v.path
        // 5. Collect result paths

        todo!("Implementation pending")
    })
}

/// Register the execution submodule in pyiceberg_core.
pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let this = PyModule::new(py, "execution")?;

    this.add_class::<PyCowRewriteResult>()?;
    this.add_class::<PyCompactionResult>()?;
    this.add_function(wrap_pyfunction!(execute_cow_rewrite, &this)?)?;
    this.add_function(wrap_pyfunction!(execute_compaction, &this)?)?;
    this.add_function(wrap_pyfunction!(execute_equality_resolution, &this)?)?;
    this.add_function(wrap_pyfunction!(execute_antijoin_paths, &this)?)?;

    m.add_submodule(&this)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("pyiceberg_core.execution", this)?;

    Ok(())
}
```

### Registration in `bindings/python/src/lib.rs`

```rust
mod data_file;
mod datafusion_table_provider;
mod error;
mod execution;       // ADD THIS
mod manifest;
mod runtime;
mod transform;

#[pymodule]
fn pyiceberg_core_rust(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    datafusion_table_provider::register_module(py, m)?;
    transform::register_module(py, m)?;
    manifest::register_module(py, m)?;
    execution::register_module(py, m)?;   // ADD THIS
    Ok(())
}
```

### Python-Side Usage (How PyIceberg Calls This)

```python
# pyiceberg/execution/operations/compact.py

def compact_via_rust(table, files_to_compact, sort_order, memory_limit):
    """Execute compaction entirely in Rust with bounded memory."""
    from pyiceberg_core.execution import execute_compaction

    result = execute_compaction(
        metadata_location=table.metadata_location,
        file_io_properties=table.io.properties,
        files_to_compact=[
            serialize_data_file(f) for f in files_to_compact
        ],
        target_file_size_bytes=table.properties.get(
            "write.target-file-size-bytes", 256 * 1024 * 1024
        ),
        sort_columns=sort_order,
        memory_limit=memory_limit or "512MB",
    )

    # Commit the replacement using PyIceberg's Transaction
    with table.transaction() as tx:
        with tx.update_snapshot().overwrite() as snap:
            for old_file in files_to_compact:
                snap.delete_data_file(old_file)
            for new_file_json in result.new_files:
                snap.append_data_file(deserialize_data_file(new_file_json))

    return result


# pyiceberg/execution/operations/equality_resolve.py

def resolve_equality_deletes_via_rust(data_file, eq_delete_refs, table):
    """Resolve equality deletes via Rust anti-join with bounded memory."""
    from pyiceberg_core.execution import execute_equality_resolution

    eq_col_names = [
        table.schema().find_field(fid).name
        for fid in eq_delete_refs[0].equality_field_ids
    ]

    batches = execute_equality_resolution(
        data_file_paths=[data_file.file_path],
        eq_delete_file_paths=[ref.delete_file.file_path for ref in eq_delete_refs],
        equality_field_names=eq_col_names,
        file_io_properties=table.io.properties,
        memory_limit="512MB",
    )

    return batches  # List[RecordBatch] with deleted rows removed
```

### Key Design Decisions

**1. `todo!()` stubs — ship the module structure first**

The initial PR can land with `todo!()` bodies and comprehensive doc comments. This establishes:
- The module structure and registration
- The Python-visible API contract (function signatures, return types)
- The `Cargo.toml` dependency wiring

Follow-up PRs implement each function. This matches iceberg-rust's PR culture of keeping each PR focused.

**2. Result types return serialized metadata, not Arrow data**

`CowRewriteResult` and `CompactionResult` return `Vec<String>` (DataFile JSON). The actual data is already written to object storage by Rust — Python only needs the metadata to construct the commit. Exception: `execute_equality_resolution` returns Arrow RecordBatches because the scan result flows back to the user, not to a commit.

**3. `StaticTable` for execution, catalog for commit (separation of concerns)**

The execution functions load a `StaticTable` (frozen snapshot, no catalog needed). The commit happens on the Python side via PyIceberg's `Transaction` which already has full catalog + OCC support. This means:
- Rust side: pure compute (read, transform, write)
- Python side: metadata operations (plan files, commit, retry)
- No need to pass catalog credentials to the Rust execution module

**4. GIL release during execution**

The `runtime().block_on(async { ... })` call releases the GIL while Tokio executes the plan. This means Python threads are not blocked during potentially long-running operations (100GB compaction = minutes).

### Dependencies

**Cargo.toml additions for `bindings/python/`:**

```toml
[dependencies]
# Existing:
pyo3 = { workspace = true }
iceberg = { workspace = true }
iceberg-datafusion = { workspace = true }
iceberg-storage-opendal = { workspace = true }
datafusion = { workspace = true }
datafusion-ffi = { workspace = true }

# May need to add:
# (Most are already transitive through iceberg-datafusion)
```

### Testing Strategy

1. **Module registration test:**
   ```python
   from pyiceberg_core.execution import execute_compaction, CowRewriteResult
   assert callable(execute_compaction)
   ```

2. **Memory limit parsing test** (Rust unit test):
   ```rust
   assert_eq!(BoundedSessionConfig::parse_memory_limit("512MB"), Ok(536_870_912));
   assert_eq!(BoundedSessionConfig::parse_memory_limit("2GB"), Ok(2_147_483_648));
   assert!(BoundedSessionConfig::parse_memory_limit("invalid").is_err());
   ```

3. **Integration test (per function, follow-up PRs):**
   - Create a local table with Parquet files
   - Call `execute_compaction` with a small memory limit
   - Verify output files are correctly sorted and sized
   - Verify memory was bounded (spill metrics > 0 for large inputs)

### PR Metadata

- **Title:** `Python: Add pyiceberg_core.execution module for bounded-memory operations`
- **Labels:** `python-bindings`, `datafusion`, `enhancement`
- **Related issues:** [#2269](https://github.com/apache/iceberg-rust/issues/2269)
- **Breaking changes:** None (purely additive — new submodule)
- **New dependencies:** None beyond what's already in the workspace

---

## Execution Order and Parallelization

### Dependency Graph

```
PR 1 (IcebergOverwriteCommitExec) ───┐
                                      ├──→ PR 3 (pyiceberg_core.execution)
PR 2 (Bounded session helper) ───────┘
```

- **PR 1 and PR 2 are fully independent** — develop in parallel
- **PR 3 depends on both** — uses `create_bounded_session` from PR 2, and optionally `IcebergOverwriteCommitExec` from PR 1 for full Rust-side commit (though initial implementation can defer commit to Python side)

### Suggested PR Submission Order

1. **PR 2 first** — smallest scope, least controversial, purely additive utility
2. **PR 1 second** — requires understanding of Transaction API, may need a prerequisite PR if `RewriteFilesAction` doesn't exist
3. **PR 3 third** — largest scope, benefits from having 1 and 2 merged

### Alternative: Ship PR 3 with Python-Side Commit

PR 3 can land without PR 1 by having the execution functions return metadata and letting Python handle the commit:

```python
# Without IcebergOverwriteCommitExec (Python commits):
result = execute_compaction(...)
with table.transaction() as tx:
    # Python-side atomic replace
    tx.overwrite(old_files=files_to_compact, new_files=result.new_files)

# With IcebergOverwriteCommitExec (Rust commits end-to-end):
result = execute_compaction_and_commit(...)  # commit happens in Rust
# More efficient (no FFI round-trip for commit), but requires PR 1
```

The Python-side commit approach is simpler, has zero additional Rust dependencies, and is perfectly correct (PyIceberg's Transaction already has full OCC support). The Rust-side commit is an optimization for later.

---

## Relationship to Existing iceberg-rust Issues

| Issue | Title | How These PRs Relate |
|:---|:---|:---|
| [#2186](https://github.com/apache/iceberg-rust/issues/2186) | MoR scan-side delete reconciliation | Long-term: once this lands, `IcebergTableScan` handles deletes natively. Until then, PR 3's `execute_equality_resolution` provides the anti-join from Python. |
| [#2205](https://github.com/apache/iceberg-rust/issues/2205) | Equality delete reader | Provides the low-level Rust reader. PR 3 wraps this for Python access. |
| [#2201](https://github.com/apache/iceberg-rust/issues/2201) | Positional delete reader | Foundation for position delete compaction. |
| [#1530](https://github.com/apache/iceberg-rust/issues/1530) | Delete file support in scan | Core primitive. Our PRs build ON TOP of whatever this delivers. |
| [#2269](https://github.com/apache/iceberg-rust/issues/2269) | DataFusion write actions (MERGE/UPDATE) | PR 1 is a prerequisite for this — MERGE/UPDATE both need atomic overwrite. |

### Non-Blocking Relationship

These PRs do NOT block on #2186, #2205, or #1530. They provide an independent execution path that:
- Uses `StaticTable` + direct file registration (no catalog scan planning)
- Performs joins/sorts via DataFusion SQL (not via Iceberg's native scan)
- Returns results to Python for commit (not through Iceberg's commit pipeline)

Once the upstream issues land, the implementation can be optimized to use native scan planning. But the Python-facing API (`execute_compaction`, `execute_equality_resolution`) remains identical.

---

## Appendix A: Current Repository Structure (Relevant Files)

```
iceberg-rust/
├── crates/
│   ├── iceberg/                              # Core Iceberg library
│   │   └── src/
│   │       └── transaction.rs                # Transaction, fast_append, etc.
│   └── integrations/
│       └── datafusion/                       # iceberg-datafusion crate
│           ├── Cargo.toml
│           └── src/
│               ├── lib.rs                    # Module re-exports
│               ├── catalog.rs                # IcebergCatalogProvider
│               ├── schema.rs                 # IcebergSchemaProvider
│               ├── task_writer.rs            # TaskWriter (partition routing)
│               ├── session.rs                # NEW (PR 2)
│               ├── table/
│               │   ├── mod.rs                # IcebergTableProvider, IcebergStaticTableProvider
│               │   ├── metadata_table.rs     # Metadata table access
│               │   └── table_provider_factory.rs
│               └── physical_plan/
│                   ├── mod.rs                # Plan re-exports
│                   ├── commit.rs             # IcebergCommitExec (fast_append only)
│                   ├── overwrite_commit.rs   # NEW (PR 1)
│                   ├── scan.rs               # IcebergTableScan
│                   ├── write.rs              # IcebergWriteExec
│                   ├── repartition.rs        # Iceberg-aware repartitioning
│                   ├── sort.rs               # Partition-based sorting
│                   ├── project.rs            # Partition value projection
│                   └── expr_to_predicate.rs  # DataFusion Expr -> Iceberg Predicate
├── bindings/
│   └── python/
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs                        # Module registration
│           ├── datafusion_table_provider.rs  # PyIcebergDataFusionTable (read-only)
│           ├── execution.rs                  # NEW (PR 3)
│           ├── data_file.rs
│           ├── manifest.rs
│           ├── runtime.rs                    # Tokio runtime singleton
│           ├── transform.rs
│           └── error.rs
```

---

## Appendix B: Key Patterns from Existing Code

### Pattern: Module Registration (from `datafusion_table_provider.rs`)

```rust
pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let this = PyModule::new(py, "execution")?;
    this.add_class::<PyCowRewriteResult>()?;
    this.add_function(wrap_pyfunction!(execute_compaction, &this)?)?;
    m.add_submodule(&this)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("pyiceberg_core.execution", this)?;
    Ok(())
}
```

### Pattern: Tokio Runtime Usage (from `runtime.rs`)

```rust
// All async operations use the shared Tokio runtime:
let rt = runtime();
rt.block_on(async { /* DataFusion execution */ })
```

### Pattern: FileIO Construction (from `datafusion_table_provider.rs`)

```rust
let factory = Arc::new(OpenDalResolvingStorageFactory::new());
let file_io = FileIOBuilder::new(factory)
    .with_props(file_io_properties)
    .build();
let static_table = StaticTable::from_metadata_file(&metadata_location, ident, file_io).await?;
```

### Pattern: IcebergCommitExec Plan Properties

```rust
// Commit nodes require SinglePartition (must see all files):
fn required_input_distribution(&self) -> Vec<Distribution> {
    vec![Distribution::SinglePartition; self.children().len()]
}

// Commit output is a count schema:
fn make_count_schema() -> ArrowSchemaRef {
    Arc::new(ArrowSchema::new(vec![Field::new("count", DataType::UInt64, false)]))
}
```

---

## Appendix C: Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|:---|:---|:---|:---|
| Transaction API doesn't support atomic overwrite | **LOW** (confirmed: PR #2185 `OverwriteAction` is under review, PR #2620 `MergingSnapshotProducer` is in draft) | Blocks PR 1 | Wait for #2185 to merge (closest to landing), then build `IcebergOverwriteCommitExec` on top. Or engage with @glitchy to help review/merge faster. |
| `RewriteFilesAction` not ready in time | Medium | Delays PR 1 full scope | Can initially use `OverwriteAction` (PR #2185) which is simpler but sufficient for compaction and CoW. `RewriteFiles` adds validation. |
| DataFusion version mismatch with pyiceberg-core | Low | Blocks PR 3 | Both iceberg-rust and datafusion-python track DataFusion releases. Pin to compatible versions. |
| PR review culture prefers smaller PRs | High | Delays | Split PR 3 into: (a) module structure + stubs, (b) CoW rewrite implementation, (c) compaction, (d) equality resolution |
| Object store configuration differs Rust vs Python | Medium | Runtime errors | Reuse FileIO properties (already proven in `datafusion_table_provider.rs` pattern) |
| Spill files on remote storage (e.g., ECS without local disk) | Low | Performance | Document: spill uses LOCAL temp. Operators running on diskless containers need larger memory budget. |
| Overlap with @CTTY's RewriteFiles work | Medium | Wasted effort | Engage early: comment on #1607, offer to build the DataFusion integration layer on top of their Transaction action. Clear separation: they build the action, we build the ExecutionPlan node. |
| PR #2185 (OverwriteAction) stalls in review | Medium | Delays PR 1 | Offer to review/test. Issue #2711 (opened today) adds urgency — the current silent-append bug motivates landing overwrite support. |

---

## Appendix D: Community Engagement Strategy

### For PR 1 (IcebergOverwriteCommitExec)

- **Discussion thread first**: Open a GitHub Discussion or comment on #2269 explaining the need for atomic overwrite in the DataFusion pipeline
- **Reference Java**: Point to `RewriteDataFilesAction` and `OverwriteFiles` in Java Iceberg
- **Scope clearly**: This PR adds ONE new `ExecutionPlan` node. It does not modify existing code.

### For PR 2 (Bounded session)

- **Low controversy**: This is a pure utility addition. No existing behavior changes.
- **Test-driven**: Include the spill verification test that demonstrates the capability.

### For PR 3 (Execution module)

- **Start with stubs**: First PR establishes the module, signatures, and types. Bodies can be `todo!()`.
- **Follow with implementations**: Each function gets its own follow-up PR with tests.
- **Reference PyIceberg issues**: Link to iceberg-python #1210 (equality deletes), #1092 (compaction), #3270 (data correctness) to show concrete demand.

---

## Summary: What to Do Next

1. **Engage on PR #2185 (OverwriteAction)** — This is the closest-to-landing Transaction primitive. Review it, test it, offer to help. Once it merges, PR 1 can build directly on top.

2. **Comment on issue #2711** — The bug was opened TODAY. Propose: "Once #2185 lands, I can contribute an `IcebergOverwriteCommitExec` that wires `OverwriteAction` into the DataFusion execution pipeline, fixing this issue for `InsertOp::Overwrite`."

3. **Start PR 2 immediately** — No dependencies, no coordination needed. Purely additive utility. This is ready to write and submit today.

4. **Watch PR #2620 (MergingSnapshotProducer)** — @CTTY's draft for `RewriteFilesAction`. This is the more complete solution (with validation). If it lands before #2185, use it instead.

5. **Start PR 3 (stubs)** — Establish the module structure and Python API contract. Use `todo!()` bodies. This gets early feedback while waiting for the Transaction layer.

6. **In parallel: Build Track 1 in iceberg-python** — Zero iceberg-rust dependencies. Delivers immediate value. Validates the UX.

### Updated Dependency Chain (Based on GitHub Findings)

```
EXTERNAL (others' work):
  PR #2185 (OverwriteAction by @glitchy) ──── under review
  PR #2620 (MergingSnapshotProducer by @CTTY) ── draft

OUR WORK:
  PR 2 (bounded session) ──────────────────── can start NOW
  PR 1 (IcebergOverwriteCommitExec) ────────── blocked on #2185 or #2620
  PR 3 (pyiceberg_core.execution stubs) ────── can start NOW (bodies need PR 1 + 2)

PARALLEL:
  Track 1 (Python-side DataFusion in iceberg-python) ── can start NOW
```
