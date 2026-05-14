# PyIceberg Maintenance Architecture: First-Principles Analysis

## 1. What Maintenance Means Architecturally in Iceberg

Maintenance in Iceberg is **distributed metadata state evolution under immutable snapshot constraints**. It is NOT simple file management — it is the controlled mutation of a table's physical layout while preserving logical correctness, transactional isolation, and concurrent access guarantees.

### The Three Pillars of Maintenance

| Pillar | Definition | Key Invariant |
|--------|-----------|---------------|
| **Planning** | Identifying which files/manifests to rewrite, based on size heuristics, partition layout, or delete file accumulation | Must be snapshot-isolated: plan against a fixed snapshot ID |
| **Execution** | Physically reading old files, transforming data (compaction, sort, delete application), writing new files | Must be idempotent and restartable; no metadata side-effects |
| **Commit Coordination** | Atomically replacing old file references with new ones in table metadata, with conflict detection and retry | Must validate no concurrent conflicts (new deletes, new data in affected partitions) |

### The Maintenance Operation Taxonomy

```
Maintenance Operations
├── Data File Compaction (RewriteDataFiles)
│   ├── BinPack strategy (size-based grouping)
│   ├── Sort strategy (global/partition sort)
│   └── Z-Order strategy (multi-dimensional clustering)
├── Delete File Compaction (RewritePositionDeleteFiles)
├── Manifest Compaction (RewriteManifests)
├── Snapshot Lifecycle Management
│   ├── ExpireSnapshots (metadata + optional file cleanup)
│   └── ManageSnapshots (branch/tag CRUD, rollback)
├── Orphan File Cleanup (DeleteOrphanFiles)
├── Dangling Delete Removal (RemoveDanglingDeleteFiles)
├── Equality Delete Conversion (ConvertEqualityDeleteFiles)
└── Table Path Rewrite (RewriteTablePath)
```

---

## 2. How Java Iceberg Implements Maintenance Internally

### 2.1 The `SnapshotProducer` Hierarchy (Core Commit Engine)

The entire maintenance commit path flows through a single, deeply layered class hierarchy:

```
SnapshotProducer<T> (abstract)
  ├── commit()          — retry loop with exponential backoff via Tasks.foreach()
  ├── apply()           — produces List<ManifestFile> for new snapshot
  ├── cleanAll()        — cleanup on failure
  └── cleanUncommitted()— cleanup partial manifests
      │
      ├── MergingSnapshotProducer<T> (abstract)
      │   ├── ManifestFilterManager    — filters manifests to remove deleted files
      │   ├── ManifestMergeManager     — bin-packs manifests by target size
      │   ├── validateAddedDataFiles() — conflict detection for concurrent appends
      │   ├── validateNoNewDeletesForDataFiles() — conflict detection for concurrent deletes
      │   ├── validateAddedDVs()       — conflict detection for concurrent DVs
      │   └── validationHistory()      — walks snapshot ancestry for conflict window
      │       │
      │       ├── BaseRewriteFiles     — implements RewriteFiles (data file compaction commit)
      │       │   └── validates: no new deletes for rewritten files, data files still exist
      │       ├── BaseOverwriteFiles   — implements OverwriteFiles
      │       ├── BaseRowDelta         — implements RowDelta (MoR deletes)
      │       ├── BaseReplacePartitions
      │       ├── MergeAppend          — manifest-merging append
      │       └── StreamingDelete      — implements DeleteFiles
      │
      ├── FastAppend              — simple manifest addition (no merge)
      ├── BaseRewriteManifests    — manifest compaction (no data rewrite)
      └── RemoveSnapshots         — snapshot expiration + file cleanup
```

**Critical design insight**: `SnapshotProducer.commit()` is the **universal commit loop**. It calls `Tasks.foreach(ops).retry(N).exponentialBackoff(...).onlyRetryOn(CommitFailedException.class).run(...)`. On retry, it calls `apply()` again against refreshed metadata. This is why `apply()` must be **re-entrant** — it may be called multiple times.

### 2.2 The Actions Layer (Orchestration Engine)

Above the core commit primitives, Java has a full **Actions** framework for orchestrating complex multi-phase maintenance:

```
Actions API (api/src/main/java/org/apache/iceberg/actions/)
├── Action<ThisT, R>                    — base interface: execute() → R
├── SnapshotUpdate<ThisT, R>            — action that produces snapshots
├── RewriteDataFiles                    — full compaction orchestration API
│   ├── binPack() / sort() / zOrder()   — strategy selection
│   ├── filter(Expression)              — partition/file filtering
│   ├── partial-progress config         — incremental commit support
│   └── Result { rewriteResults(), rewriteFailures() }
├── RewritePositionDeleteFiles          — position delete compaction
├── DeleteOrphanFiles                   — orphan file discovery + deletion
├── RemoveDanglingDeleteFiles           — stale delete file cleanup
├── ConvertEqualityDeleteFiles          — eq-delete → pos-delete conversion
└── RewriteManifests                    — manifest-level compaction
```

The **core actions implementation** layer provides engine-agnostic orchestration:

```
Core Actions (core/src/main/java/org/apache/iceberg/actions/)
├── FileRewritePlanner<I, T, F, G>           — plans which files to rewrite
│   └── SizeBasedFileRewritePlanner          — size-threshold + bin-packing
│       ├── BinPackRewriteFilePlanner        — data file bin-pack planning
│       └── BinPackRewritePositionDeletePlanner
├── FileRewriteRunner<I, T, F, G>            — executes the actual rewrite
├── RewriteDataFilesCommitManager            — handles commit + cleanup
│   ├── commitFileGroups(Set<RewriteFileGroup>)
│   ├── commitOrClean()                      — commit with cleanup on failure
│   └── CommitService (async commit batching via BaseCommitService)
└── RewritePositionDeletesCommitManager      — same for delete files
```

### 2.3 The Commit Retry + Validation Contract

Java's `SnapshotProducer.commit()` (lines ~380-420 of SnapshotProducer.java):
```java
Tasks.foreach(ops)
    .retry(numRetries)
    .exponentialBackoff(minWait, maxWait, totalRetryTime, 2.0)
    .onlyRetryOn(CommitFailedException.class)
    .run(taskOps -> {
        Snapshot newSnapshot = apply();           // re-entrant!
        taskOps.commit(base, newSnapshot.metadata());
    });
```

`BaseRewriteFiles` then calls these validations inside `apply()`:
1. `validateDataFilesExist()` — the files being replaced must still exist
2. `validateNoNewDeletesForDataFiles()` — no new deletes applied to rewritten files since `startingSnapshotId`
3. `validateAddedDVs()` — no concurrent DV additions

This forms the **optimistic concurrency control** contract for all maintenance operations.

### 2.4 The Transaction Model

Java `BaseTransaction` orchestrates multi-operation transactions:
- Maintains a `List<PendingUpdate>` of staged operations
- On `commitTransaction()`, uses `Tasks.foreach(ops).retry(...)` with `applyUpdates()` that **re-applies all pending updates** against refreshed metadata on retry
- Tracks `deletedFiles` for cleanup, distinguishing committed vs uncommitted files
- `TransactionTableOperations` provides an in-memory view of evolving metadata

---

## 3. What Maintenance Capabilities Currently Exist in PyIceberg

### 3.1 Current State Summary

| Capability | Status | Implementation Quality |
|-----------|--------|----------------------|
| **ExpireSnapshots** | ✅ Implemented | Basic: by-id, by-ids, older-than. **No file cleanup.** |
| **ManageSnapshots** | ✅ Implemented | Solid: create/remove branch/tag, rollback, set-current |
| **Delete (copy-on-write)** | ✅ Implemented | Functional but ad-hoc: inline in `Transaction.delete()` |
| **Overwrite** | ✅ Implemented | Functional: predicate-based overwrite with rewrite |
| **Append (fast + merge)** | ✅ Implemented | Good: `_FastAppendFiles` + `_MergeAppendFiles` with manifest merging |
| **Manifest Merge** | ✅ Implemented | `_ManifestMergeManager` with bin-packing |
| **Validation Framework** | ✅ Implemented | `validate.py`: added/deleted data files, new delete files, sequence numbers |
| **RewriteDataFiles** | ❌ Missing | No compaction action exists |
| **RewriteManifests** | ❌ Missing | No manifest-only compaction |
| **RewritePositionDeleteFiles** | ❌ Missing | No delete file compaction |
| **DeleteOrphanFiles** | ❌ Missing | No orphan detection/cleanup |
| **RemoveDanglingDeleteFiles** | ❌ Missing | No dangling delete cleanup |
| **ConvertEqualityDeleteFiles** | ❌ Missing | No eq-delete conversion |
| **Commit Retry** | ❌ Missing | Single-shot commit, no retry loop |
| **File Cleanup on Expire** | ❌ Missing | Metadata-only expiration |
| **RewriteFiles primitive** | ❌ Missing | No `REPLACE` snapshot operation |

### 3.2 Detailed Code-Level Analysis

#### `MaintenanceTable` — The Entry Point (46 lines)

```python
# pyiceberg/table/maintenance.py — THE ENTIRE FILE
class MaintenanceTable:
    tbl: Table
    def __init__(self, tbl: Table) -> None:
        self.tbl = tbl
    def expire_snapshots(self) -> ExpireSnapshots:
        return ExpireSnapshots(transaction=Transaction(self.tbl, autocommit=True))
```

This is a **stub**. The entire maintenance surface is a single method returning `ExpireSnapshots`.

#### `ExpireSnapshots` — Metadata-Only (lines 1032-1134 of snapshot.py)

The current implementation:
- Removes snapshot entries from metadata via `RemoveSnapshotsUpdate`
- Protects branch/tag heads from expiration
- Supports `by_id()`, `by_ids()`, `older_than(datetime)`
- **Does NOT delete any physical files** (data files, manifests, manifest lists)

This is a **critical gap**. In production, snapshot expiration without file cleanup causes unbounded storage growth.

#### `_SnapshotProducer` — The Core (lines 94-389 of snapshot.py)

PyIceberg's snapshot producer:
- Manages `_added_data_files` and `_deleted_data_files`
- Writes manifest files and manifest lists
- Produces `Summary` with metrics
- Supports branch-targeting via `_target_branch`
- Has `_build_delete_files_partition_predicate()` for manifest pruning

**What's missing vs Java's `SnapshotProducer`:**
1. No `commit()` retry loop — `_commit()` returns `UpdatesAndRequirements`, delegated to `Transaction._apply()` which does a single-shot `_do_commit()`
2. No `cleanAll()` / `cleanUncommitted()` — no cleanup of partially written manifests on failure
3. No re-entrant `apply()` — cannot be retried against refreshed metadata
4. No `deleteWith()` callback for tracking files to clean up

#### `_OverwriteFiles` — File Replacement (lines 585-672)

This is the closest thing to `RewriteFiles` but operates differently:
- Uses `Operation.OVERWRITE` (not `Operation.REPLACE`)
- Matches deleted files by identity (`entry.data_file in self._deleted_data_files`)
- Rewrites manifests to exclude deleted entries
- **No validation** against concurrent modifications

#### `Transaction` — No Retry, No Re-apply (lines 209-280)

```python
def commit_transaction(self) -> None:
    self._table._do_commit(updates=self._updates, requirements=self._requirements)
```

Compare to Java's `BaseTransaction.commitSimpleTransaction()` which:
1. Uses `Tasks.foreach(ops).retry(N).exponentialBackoff(...)`
2. On retry, calls `applyUpdates()` which **re-applies all pending updates** against fresh metadata
3. Tracks and cleans uncommitted files

PyIceberg's commit is fire-and-forget with no retry.

#### Validation Framework — Present but Unused for Maintenance

`validate.py` (358 lines) implements:
- `_validation_history()` — walks snapshot ancestry
- `_added_data_files()` / `_deleted_data_files()` — finds conflicting entries
- `_validate_added_data_files()` — detects concurrent appends
- `_validate_no_new_delete_files()` — detects concurrent delete files
- `_validate_no_new_deletes_for_data_files()` — detects deletes affecting specific data files

These are **correct implementations** of Java's conflict detection validators. However, they are **not wired into any maintenance operation**. They exist but are orphaned.

---

## 4. Architectural Gaps Preventing Parity

### Gap 1: No `REPLACE` Snapshot Operation

Java's `Operation.REPLACE` is the semantic anchor for all compaction operations. It means "data files were added and removed without changing table data." PyIceberg's `Operation` enum has `REPLACE` defined but **no snapshot producer uses it**. Without this, there is no way to express a compaction commit.

### Gap 2: No `RewriteFiles` Primitive

Java's `BaseRewriteFiles` extends `MergingSnapshotProducer` and:
1. Accepts files to delete + files to add
2. Sets operation to `REPLACE`
3. Validates from a starting snapshot (`validateFromSnapshot`)
4. Validates no new deletes for rewritten files
5. Validates data files still exist
6. Supports `dataSequenceNumber()` for sequence number preservation

PyIceberg has **nothing equivalent**. The `_OverwriteFiles` producer is the closest but uses `OVERWRITE` semantics and lacks validation.

### Gap 3: No Commit Retry Loop

This is the single most critical infrastructure gap. Every maintenance operation in Java relies on `SnapshotProducer.commit()` with retry-on-conflict semantics. PyIceberg's `Transaction.commit_transaction()` is a single attempt that throws on any `CommitFailedException`.

Without retry:
- Any concurrent write causes permanent failure
- Multi-step compaction jobs cannot recover from transient conflicts
- Production maintenance is unreliable under any write load

### Gap 4: No File Cleanup Infrastructure

Java's `SnapshotProducer` tracks all written files via `deleteWith()` callbacks and provides `cleanAll()` for failure cleanup. Java's `RemoveSnapshots` has `FileCleanupStrategy` (incremental vs reachable) for physical file deletion.

PyIceberg has neither. Failed operations leave orphaned manifest files. Expired snapshots leave orphaned data files.

### Gap 5: No Actions Framework

Java's Actions layer provides:
- `FileRewritePlanner` — which files need compaction, grouping by partition
- `SizeBasedFileRewritePlanner` — bin-packing with min/max/target size thresholds
- `RewriteDataFilesCommitManager` — partial-progress commits, abort/cleanup
- `BaseCommitService` — async commit batching

PyIceberg has nothing in this space. There is no way to:
- Identify which partitions need compaction
- Group files into rewrite tasks
- Execute compaction with partial progress
- Clean up on partial failure

### Gap 6: No MergingSnapshotProducer Equivalent

Java's `MergingSnapshotProducer` is the workhorse that:
- Manages separate `DataFileFilterManager` and `DeleteFileFilterManager`
- Handles manifest merging via `ManifestMergeManager`
- Provides all conflict validation hooks
- Supports both data and delete manifest evolution in a single commit

PyIceberg's `_SnapshotProducer` is a simpler base. The `_MergeAppendFiles` handles manifest merging for appends only, but there's no equivalent for complex operations that add AND remove files with full validation.

---

## 5. Proposed Architectural Refactors and Foundational Abstractions

### Phase 0: Foundation — Commit Retry Infrastructure (PREREQUISITE)

**Everything else depends on this.** Without retry semantics, no maintenance operation is production-safe.

```python
# Proposed: pyiceberg/table/commit.py
class CommitRetry:
    """Retry loop for table commits with exponential backoff."""

    @staticmethod
    def run(
        table: Table,
        build_updates: Callable[[TableMetadata], UpdatesAndRequirements],
        num_retries: int = 4,
        min_wait_ms: int = 100,
        max_wait_ms: int = 60000,
    ) -> None:
        """
        Execute a commit with retry.

        build_updates is called with current metadata and must return
        (updates, requirements). On CommitFailedException, the table
        is refreshed and build_updates is called again.
        """
```

This must also integrate with `Transaction` so that `commit_transaction()` retries by re-applying all staged updates against refreshed metadata, mirroring Java's `BaseTransaction.commitSimpleTransaction()`.

### Phase 1: `RewriteFiles` Snapshot Producer

```python
# Proposed: new class in pyiceberg/table/update/snapshot.py
class _RewriteFiles(_SnapshotProducer["_RewriteFiles"]):
    """Replace data files atomically (Operation.REPLACE).

    Used for compaction: removes old files, adds new files,
    validates no concurrent conflicts since starting snapshot.
    """
    _starting_snapshot_id: int | None
    _use_starting_sequence_number: bool

    def validate_from_snapshot(self, snapshot_id: int) -> _RewriteFiles: ...
    def data_sequence_number(self, seq_num: int) -> _RewriteFiles: ...

    def _validate(self) -> None:
        """Run all conflict validations before commit."""
        # Uses existing validate.py functions:
        # - _validate_no_new_deletes_for_data_files()
        # - _validate_deleted_data_files() (files still exist)

    def _existing_manifests(self) -> list[ManifestFile]: ...
    def _deleted_entries(self) -> list[ManifestEntry]: ...
```

This wires the existing `validate.py` functions into a proper commit path.

### Phase 2: File Cleanup Infrastructure

```python
# Proposed: pyiceberg/table/cleanup.py
class FileCleanupStrategy(ABC):
    """Strategy for cleaning up unreferenced files."""
    @abstractmethod
    def cleanup(self, table: Table, expired_snapshots: set[int]) -> CleanupResult: ...

class IncrementalFileCleanup(FileCleanupStrategy):
    """Delete files only referenced by expired snapshots (incremental diff)."""

class ReachableFileCleanup(FileCleanupStrategy):
    """Compute full set of reachable files and delete everything else."""
```

Integrate into `ExpireSnapshots`:
```python
class ExpireSnapshots(UpdateTableMetadata["ExpireSnapshots"]):
    def clean_expired_files(self, strategy: FileCleanupStrategy = IncrementalFileCleanup()) -> ExpireSnapshots: ...
    def delete_with(self, delete_func: Callable[[str], None]) -> ExpireSnapshots: ...
```

### Phase 3: Compaction Planning Framework

```python
# Proposed: pyiceberg/table/actions/planner.py
class FileRewritePlanner(ABC):
    """Determines which files to rewrite and how to group them."""
    @abstractmethod
    def plan(self, table: Table, filter: BooleanExpression) -> list[RewriteFileGroup]: ...

class SizeBasedPlanner(FileRewritePlanner):
    """Bin-pack files based on size thresholds."""
    target_file_size: int      # Target output file size
    min_file_size: int         # Files below this are candidates (default: 75% of target)
    max_file_size: int         # Files above this are candidates (default: 180% of target)
    min_input_files: int       # Min files in group to trigger rewrite (default: 5)
    max_group_size: int        # Max bytes per group (default: 100GB)
```

### Phase 4: Compaction Commit Coordinator

```python
# Proposed: pyiceberg/table/actions/commit_manager.py
class RewriteCommitManager:
    """Manages commits for rewrite operations with cleanup."""

    def commit_file_groups(self, groups: list[RewriteFileGroup]) -> None:
        """Commit all groups in a single RewriteFiles operation."""

    def commit_or_clean(self, groups: list[RewriteFileGroup]) -> None:
        """Commit, cleaning up written files on failure."""

    def abort_file_group(self, group: RewriteFileGroup) -> None:
        """Clean up files written for a failed group."""
```

### Phase 5: `RewriteDataFiles` Action

```python
# Proposed: pyiceberg/table/actions/rewrite_data_files.py
class RewriteDataFiles:
    """Full compaction orchestration action."""

    def bin_pack(self) -> RewriteDataFiles: ...
    def filter(self, expr: BooleanExpression) -> RewriteDataFiles: ...
    def option(self, key: str, value: str) -> RewriteDataFiles: ...
    def execute(self) -> RewriteResult: ...
```

### Phase 6: Orphan File Cleanup

```python
# Proposed: pyiceberg/table/actions/delete_orphan_files.py
class DeleteOrphanFiles:
    """Find and delete files not referenced by any valid snapshot."""

    def location(self, path: str) -> DeleteOrphanFiles: ...
    def older_than(self, timestamp_ms: int) -> DeleteOrphanFiles: ...
    def delete_with(self, func: Callable[[str], None]) -> DeleteOrphanFiles: ...
    def execute(self) -> OrphanFileResult: ...
```

---

## 6. Implementation Roadmap

### Priority 1: Transaction Safety (Weeks 1-3)
- [ ] Implement `CommitRetry` with exponential backoff
- [ ] Wire retry into `Transaction.commit_transaction()`
- [ ] Add manifest cleanup on commit failure (`cleanAll()` equivalent)
- [ ] Add file tracking via `delete_with()` callbacks in `_SnapshotProducer`

### Priority 2: RewriteFiles Primitive (Weeks 3-5)
- [ ] Add `_RewriteFiles` snapshot producer with `Operation.REPLACE`
- [ ] Wire `validate.py` functions into validation hooks
- [ ] Support `validate_from_snapshot()` and `data_sequence_number()`
- [ ] Expose via `UpdateSnapshot.rewrite()` and `Transaction.new_rewrite()`
- [ ] Add comprehensive tests for concurrent conflict scenarios

### Priority 3: File Cleanup on Expire (Weeks 5-7)
- [ ] Implement `IncrementalFileCleanup` strategy
- [ ] Implement `ReachableFileCleanup` strategy
- [ ] Wire into `ExpireSnapshots` with configurable cleanup level
- [ ] Handle manifest list, manifest, and data file deletion
- [ ] Add safety checks (e.g., `older_than` threshold for orphan safety)

### Priority 4: Compaction Action (Weeks 7-11)
- [ ] Implement `SizeBasedPlanner` with bin-packing
- [ ] Implement `RewriteCommitManager` with cleanup
- [ ] Implement `RewriteDataFiles` action (bin-pack strategy first)
- [ ] Add partial-progress support (commit groups incrementally)
- [ ] Wire into `MaintenanceTable` as `table.maintenance.rewrite_data_files()`

### Priority 5: Advanced Maintenance (Weeks 11-15)
- [ ] Implement `DeleteOrphanFiles` action
- [ ] Implement `RemoveDanglingDeleteFiles` action
- [ ] Implement `RewriteManifests` (manifest-only compaction)
- [ ] Implement `RewritePositionDeleteFiles` (delete file compaction)

---

## 7. Capability Matrix

| Capability | Java Iceberg | PyIceberg | Gap Severity |
|-----------|-------------|-----------|--------------|
| Commit retry with exponential backoff | `Tasks.foreach().retry()` in `SnapshotProducer` + `BaseTransaction` | ❌ Single-shot commit | **CRITICAL** |
| Manifest cleanup on failure | `cleanAll()` + `cleanUncommitted()` | ❌ None | **CRITICAL** |
| `REPLACE` operation (compaction commit) | `BaseRewriteFiles` | ❌ Not used | **CRITICAL** |
| Conflict validation (concurrent deletes) | `validateNoNewDeletesForDataFiles()` | ✅ Exists in `validate.py` but **unwired** | HIGH |
| Conflict validation (concurrent appends) | `validateAddedDataFiles()` | ✅ Exists in `validate.py` but **unwired** | HIGH |
| Conflict validation (concurrent DVs) | `validateAddedDVs()` | ❌ None | MEDIUM |
| Data file compaction action | `RewriteDataFiles` + `SizeBasedFileRewritePlanner` | ❌ None | **CRITICAL** |
| Compaction planning (bin-pack) | `BinPackRewriteFilePlanner` | ❌ None | HIGH |
| Compaction commit manager | `RewriteDataFilesCommitManager` | ❌ None | HIGH |
| Partial-progress commits | `BaseCommitService` (async batching) | ❌ None | MEDIUM |
| Snapshot expiration (metadata) | `RemoveSnapshots` | ✅ `ExpireSnapshots` (basic) | LOW |
| Snapshot expiration (file cleanup) | `FileCleanupStrategy` (incremental/reachable) | ❌ None | **CRITICAL** |
| Orphan file cleanup | `DeleteOrphanFiles` action | ❌ None | HIGH |
| Dangling delete cleanup | `RemoveDanglingDeleteFiles` | ❌ None | MEDIUM |
| Manifest compaction | `BaseRewriteManifests` | ❌ None (but manifest merge exists for appends) | MEDIUM |
| Position delete compaction | `RewritePositionDeleteFiles` | ❌ None | MEDIUM |
| Equality delete conversion | `ConvertEqualityDeleteFiles` | ❌ None | LOW |
| Snapshot management (branch/tag) | `SnapshotManager` | ✅ `ManageSnapshots` | NONE |
| Manifest merging on append | `MergeAppend` + `ManifestMergeManager` | ✅ `_MergeAppendFiles` + `_ManifestMergeManager` | NONE |
| Sort-based compaction | Spark `SortStrategy` | ❌ None | MEDIUM |
| Z-Order compaction | Spark `ZOrderStrategy` | ❌ None | LOW |
| Table path rewrite | `RewriteTablePath` | ❌ None | LOW |

---

## 8. Key Architectural Observations

### 8.1 PyIceberg's Validation Code is Ready — It Just Needs Wiring

The `validate.py` module is a **faithful port** of Java's conflict detection logic. The functions `_validate_added_data_files`, `_validate_no_new_delete_files`, and `_validate_no_new_deletes_for_data_files` correctly walk snapshot ancestry and filter manifest entries. They just aren't called from any maintenance operation.

### 8.2 The `_OverwriteFiles` Producer is NOT a Replacement for `RewriteFiles`

`_OverwriteFiles` uses `Operation.OVERWRITE` which semantically means "data changed." `Operation.REPLACE` means "physical layout changed, data unchanged." This distinction matters for:
- Snapshot expiration (REPLACE snapshots can be skipped in some cleanup strategies)
- Conflict detection (different validation rules apply)
- Audit/lineage tracking

### 8.3 The Transaction Model Lacks Re-entrancy

Java's `BaseTransaction.applyUpdates()` re-applies ALL pending updates against refreshed metadata. PyIceberg's `Transaction` accumulates `_updates` as a tuple but `commit_transaction()` sends them all at once with no ability to re-derive them from fresh metadata. This means:
- Snapshot IDs computed during staging may conflict with concurrent operations
- Manifest file paths computed during staging may be stale
- The entire update chain may need to be discarded and rebuilt

### 8.4 PyIceberg's `_SnapshotProducer._commit()` Returns Data, Doesn't Execute

In Java, `SnapshotProducer.commit()` is the method that actually executes the commit with retries. In PyIceberg, `_commit()` just returns `UpdatesAndRequirements` which are then staged into the transaction. The actual commit happens later in `Transaction.commit_transaction()`. This separation makes retry semantics harder because the snapshot producer doesn't control the commit lifecycle.

### 8.5 No File Tracking for Cleanup

Java tracks every file written during an operation via the `deleteWith(Consumer<String>)` callback on `SnapshotProducer`. On failure, `cleanAll()` deletes all written-but-uncommitted manifests and manifest lists. PyIceberg writes files directly to the FileIO and never tracks them. A failed commit leaves orphaned manifest files that are never cleaned up.

---

## 9. Summary

PyIceberg's maintenance subsystem is in an **early-stage, metadata-only** state. The foundational abstractions for production-grade maintenance — commit retry, file cleanup, conflict-validated rewrite operations, and compaction planning — are all absent. However, several critical building blocks exist:

1. **Validation framework** (`validate.py`) — complete and correct, needs wiring
2. **Manifest merging** (`_ManifestMergeManager`) — functional for appends
3. **Snapshot management** (`ManageSnapshots`) — complete
4. **Operation enum** — `REPLACE` is defined, just unused

The path to parity requires building from the bottom up: commit retry infrastructure first, then `RewriteFiles` primitive, then file cleanup, then compaction actions. Each layer depends on the one below it. Attempting to implement compaction without commit retry would produce a system that fails under any concurrent write load — exactly the production scenario where compaction is most needed.
