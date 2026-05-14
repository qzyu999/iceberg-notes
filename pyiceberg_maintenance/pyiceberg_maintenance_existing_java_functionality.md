# Java Iceberg Maintenance Architecture: Complete Reference for PyIceberg Parity

> This document provides a rigorous, source-level breakdown of every Java Iceberg maintenance subsystem, its internal mechanics, and the precise mapping to PyIceberg's current state. All class references point to the `apache/iceberg` repository (`core/src/main/java/org/apache/iceberg/`).

---

## 1. The Commit Producer Hierarchy

The entire maintenance commit path flows through `SnapshotProducer<T>`, which owns the universal retry loop.

### 1.1 `SnapshotProducer<T>` — The Universal Commit Engine

**File:** `SnapshotProducer.java` (933 lines)

**Core contract:**

| Method | Role | Re-entrancy |
|--------|------|-------------|
| `commit()` | Retry loop via `Tasks.foreach(ops).retry(N).exponentialBackoff(...)` | Called once per operation |
| `apply()` | Refreshes metadata, runs `validate()`, calls `apply(meta, snapshot)` to produce manifests, writes manifest list, returns `Snapshot` | **Must be re-entrant** — called on each retry |
| `validate(meta, snapshot)` | Hook for subclass conflict detection | Called inside `apply()` before manifest generation |
| `cleanAll()` | Deletes all manifest lists + calls `cleanUncommitted(EMPTY_SET)` | Called on non-retryable failure |
| `cleanUncommitted(committed)` | Abstract — subclass deletes manifests not in `committed` set | Called after successful commit |

**Retry loop internals** (lines 458–550):

```
Tasks.foreach(ops)
  .retry(COMMIT_NUM_RETRIES)                    // default: 4
  .exponentialBackoff(MIN_WAIT, MAX_WAIT, TOTAL) // 100ms, 60s, 1800s
  .onlyRetryOn(CommitFailedException.class)
  .run(taskOps -> {
      Snapshot newSnapshot = apply();            // re-entrant!
      TableMetadata updated = buildFrom(base).setBranchSnapshot(newSnapshot, branch).build();
      taskOps.commit(base, updated.withUUID());  // CAS operation
  });
```

**Post-commit cleanup** (lines 514–542): After successful commit, refreshes metadata, finds the committed snapshot by ID, then:
1. Calls `cleanUncommitted(committedManifests)` to delete orphaned manifests
2. Deletes unused manifest lists from retry attempts

**On failure** (lines 504–512): If `strictCleanup` is false OR exception is `CleanableFailure`, calls `cleanAll()` to delete all generated artifacts.

### 1.2 `MergingSnapshotProducer<T>` — The Manifest Management Layer

**File:** `MergingSnapshotProducer.java` (51,981 bytes)

Extends `SnapshotProducer` and adds:

| Capability | Method | Purpose |
|-----------|--------|---------|
| Manifest filtering | `ManifestFilterManager` | Removes deleted file entries from existing manifests |
| Manifest merging | `ManifestMergeManager` | Bin-packs small manifests into target-sized ones |
| Conflict detection | `validateAddedDataFiles()` | Detects concurrent data file additions |
| Conflict detection | `validateNoNewDeletesForDataFiles()` | Detects concurrent row-level deletes targeting rewritten files |
| Conflict detection | `validateNoNewDeleteFiles()` | Detects concurrent delete file additions |
| Conflict detection | `validateAddedDVs()` | Detects concurrent Deletion Vector additions |
| Conflict detection | `validateDeletedDataFiles()` | Detects concurrent data file deletions |
| History walking | `validationHistory()` | Walks snapshot ancestry from `startingSnapshotId` to `parent` |

**Subclasses (all extend MergingSnapshotProducer):**

| Class | Operation | Validation in `validate()` |
|-------|-----------|---------------------------|
| `BaseRewriteFiles` | `REPLACE` | `validateNoNewDeletesForDataFiles` for replaced data files |
| `BaseOverwriteFiles` | `OVERWRITE` | `validateAddedDataFiles`, `validateNoNewDeleteFiles`, `validateDeletedDataFiles` |
| `BaseRowDelta` | `OVERWRITE`/`DELETE`/`APPEND` | `validateAddedDataFiles`, `validateNoNewDeleteFiles`, `validateNoNewDeletesForDataFiles`, `validateAddedDVs` |
| `BaseReplacePartitions` | `OVERWRITE`/`APPEND` | `validateAddedDataFiles` (dynamic overwrite mode) |
| `MergeAppend` | `APPEND` | None (inherits base) |
| `StreamingDelete` | `DELETE` | None |

**Direct SnapshotProducer subclasses (bypass MergingSnapshotProducer):**

| Class | Operation | Notes |
|-------|-----------|-------|
| `FastAppend` | `APPEND` | No manifest merging, just adds new manifest |
| `BaseRewriteManifests` | `REPLACE` | Manifest-level compaction, no data rewrite |
| `RemoveSnapshots` | N/A (not a snapshot) | Snapshot expiration with its own retry loop |

---

## 2. `BaseRewriteFiles` — The Compaction Commit Primitive

**File:** `BaseRewriteFiles.java` (157 lines)

This is the **single most important class** for PyIceberg compaction parity.

### 2.1 API Surface

```java
class BaseRewriteFiles extends MergingSnapshotProducer<RewriteFiles> implements RewriteFiles {
    // State
    private final DataFileSet replacedDataFiles = DataFileSet.create();
    private Long startingSnapshotId = null;

    // Operation type
    protected String operation() { return DataOperations.REPLACE; }

    // File management
    RewriteFiles deleteFile(DataFile dataFile)    // tracks in replacedDataFiles + calls delete()
    RewriteFiles deleteFile(DeleteFile deleteFile) // calls delete() only
    RewriteFiles addFile(DataFile dataFile)        // calls add()
    RewriteFiles addFile(DeleteFile deleteFile)    // calls add()

    // Sequence number control
    RewriteFiles dataSequenceNumber(long seq)      // sets sequence number for new data files

    // Validation anchor
    RewriteFiles validateFromSnapshot(long snapshotId)  // sets startingSnapshotId
}
```

### 2.2 Validation Logic

```java
protected void validate(TableMetadata base, Snapshot parent) {
    validateReplacedAndAddedFiles();  // Preconditions: must delete something, types must match
    if (!replacedDataFiles.isEmpty()) {
        // Core safety check: no new row-level deletes for the files being rewritten
        validateNoNewDeletesForDataFiles(base, startingSnapshotId, replacedDataFiles, parent);
    }
}
```

**Why `validateNoNewDeletesForDataFiles`?** If a concurrent writer adds a position delete targeting file X, and compaction replaces file X with file X' (which doesn't know about the delete), the delete is lost. This validation walks the snapshot history from `startingSnapshotId` to `parent` and checks for any new delete files whose referenced data files overlap with `replacedDataFiles`.

### 2.3 The `RewriteFiles` API Interface

**File:** `api/.../RewriteFiles.java` (188 lines)

Key contract from the Javadoc:
> "The new state of the table after each rewrite must be logically equivalent to the original table state."
> "Commit conflicts will be resolved by applying the changes to the new latest snapshot and reattempting the commit. If any of the deleted files are no longer in the latest snapshot when reattempting, the commit will throw a ValidationException."

---

## 3. The Actions Layer — Orchestration Above the Commit Primitive

### 3.1 Architecture Overview

```
User API
  └── RewriteDataFiles (action interface)
        └── BaseRewriteDataFiles (engine-agnostic orchestration)
              ├── FileRewritePlanner → SizeBasedFileRewritePlanner → BinPackRewriteFilePlanner
              ├── FileRewriteRunner (engine-specific: Spark/Flink implement this)
              └── RewriteDataFilesCommitManager
                    ├── commitFileGroups() → table.newRewrite().validateFromSnapshot(startId)
                    ├── abortFileGroup() → deletes written files on failure
                    └── CommitService extends BaseCommitService (async partial-progress)
```

### 3.2 `SizeBasedFileRewritePlanner` — File Selection

**File:** `actions/SizeBasedFileRewritePlanner.java` (383 lines)

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `target-file-size-bytes` | From `write.target-file-size-bytes` | Target output file size |
| `min-file-size-bytes` | 75% of target | Files smaller than this are candidates |
| `max-file-size-bytes` | 180% of target | Files larger than this are candidates |
| `min-input-files` | 5 | Minimum files in a group to trigger rewrite |
| `max-file-group-size-bytes` | 100 GB | Maximum data per group |
| `rewrite-all` | false | Force rewrite of all files |

**Algorithm:**
1. Filter files outside `[min, max]` size range
2. Bin-pack filtered files into groups of `max-file-group-size-bytes`
3. Filter groups: keep only those with `>= min-input-files` OR total size `> target-file-size-bytes`

### 3.3 `RewriteDataFilesCommitManager` — Commit Orchestration

**File:** `actions/RewriteDataFilesCommitManager.java` (181 lines)

```java
public void commitFileGroups(Set<RewriteFileGroup> fileGroups) {
    DataFileSet rewrittenDataFiles = DataFileSet.create();
    DataFileSet addedDataFiles = DataFileSet.create();
    DeleteFileSet danglingDVs = DeleteFileSet.create();

    for (RewriteFileGroup group : fileGroups) {
        rewrittenDataFiles.addAll(group.rewrittenFiles());
        addedDataFiles.addAll(group.addedFiles());
        danglingDVs.addAll(group.danglingDVs());
    }

    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshotId);
    if (useStartingSequenceNumber) {
        rewrite.dataSequenceNumber(table.snapshot(startingSnapshotId).sequenceNumber());
    }

    rewrittenDataFiles.forEach(rewrite::deleteFile);
    addedDataFiles.forEach(rewrite::addFile);
    danglingDVs.forEach(rewrite::deleteFile);
    rewrite.commit();  // triggers SnapshotProducer.commit() retry loop
}
```

**Error handling:**
- `CommitStateUnknownException` → **never clean up** (files may have been committed)
- `CleanableFailure` → delete all added files via `abortFileGroup()`
- Other exceptions → rethrow without cleanup

### 3.4 `BaseCommitService` — Async Partial-Progress

**File:** `actions/BaseCommitService.java` (257 lines)

A single-threaded executor that commits file groups as they complete:
- `offer(group)` — enqueue completed rewrite for commit
- Batches groups by `rewritesPerCommit` count
- On failure: logs error, continues (partial-progress)
- On close: waits up to 120 minutes for pending commits, then aborts remaining

---

## 4. Snapshot Expiration — `RemoveSnapshots`

**File:** `RemoveSnapshots.java` (472 lines)

### 4.1 Two-Phase Architecture

**Phase 1: Metadata update** (lines 188–271, inside its own retry loop)
1. Compute retained refs (main always retained; others by `max-ref-age-ms`)
2. For each branch, compute snapshots to retain by `min-snapshots-to-keep` and `max-snapshot-age-ms`
3. Retain unreferenced snapshots newer than `defaultExpireOlderThan`
4. Build updated `TableMetadata` removing expired snapshots
5. Commit via `ops.commit(base, updated)` with retry

**Phase 2: File cleanup** (lines 384–407, AFTER metadata commit)
```java
FileCleanupStrategy cleanupStrategy =
    incrementalCleanup
        ? new IncrementalFileCleanup(io, deleteExec, planExec, deleteFunc)
        : new ReachableFileCleanup(io, deleteExec, planExec, deleteFunc);
cleanupStrategy.cleanFiles(base, current, cleanupLevel);
```

### 4.2 `FileCleanupStrategy` — Abstract Cleanup Interface

**File:** `FileCleanupStrategy.java` (166 lines)

```java
abstract class FileCleanupStrategy {
    abstract void cleanFiles(TableMetadata before, TableMetadata after, CleanupLevel level);
    protected void deleteFiles(Set<String> paths, String fileType);  // bulk or per-file
}
```

### 4.3 `IncrementalFileCleanup` — Fast Path

**File:** `IncrementalFileCleanup.java` (339 lines)

**When used:** Simple main-branch-only expiration (no non-main snapshots, no specified IDs).

**Algorithm:**
1. Identify expired snapshot IDs
2. Compute ancestor IDs and cherry-pick source IDs
3. For each valid snapshot's manifests: if manifest was written by expired ancestor AND has deleted files → scan for files to delete
4. For each expired snapshot's manifests: if not in valid manifests → mark manifest for deletion; if from ancestor with deletes → scan; if from non-ancestor with adds → revert (delete added files)
5. Delete: data files, manifests, manifest lists, statistics files

### 4.4 `ReachableFileCleanup` — Safe Path

**File:** `ReachableFileCleanup.java` (231 lines)

**When used:** Complex cases (specified snapshot IDs, non-main branches).

**Algorithm:**
1. Collect manifests from expired snapshots (deletion candidates)
2. Prune candidates by removing manifests still referenced by valid snapshots
3. For remaining candidates: read all file paths → these are deletable
4. Cross-reference against files in current manifests → remove any still-live files
5. Delete: data files, manifests, manifest lists, statistics files

### 4.5 `CleanupLevel` Enum

| Level | Behavior |
|-------|----------|
| `ALL` | Delete data files + manifests + manifest lists + statistics |
| `MANIFEST_AND_LIST` | Delete manifests + manifest lists only (skip data files) |
| `NONE` | Metadata-only expiration, no file deletion |

---

## 5. Manifest Compaction — `BaseRewriteManifests`

**File:** `BaseRewriteManifests.java` (387 lines)

### 5.1 Key Design

- Extends `SnapshotProducer` directly (NOT `MergingSnapshotProducer`)
- Operation: `REPLACE` (no logical data change)
- Uses `clusterBy(Function<DataFile, Object>)` to re-partition manifest entries
- Uses `rewriteIf(Predicate<ManifestFile>)` to filter which manifests to rewrite

### 5.2 Safety Invariant

```java
private void validateFilesCounts() {
    int createdManifestsFilesCount = activeFilesCount(newManifests + addedManifests + rewrittenAddedManifests);
    int replacedManifestsFilesCount = activeFilesCount(rewrittenManifests + deletedManifests);
    if (created != replaced) throw ValidationException("...");
}
```

The total number of active files must remain identical before and after rewrite.

---

## 6. `BaseRowDelta` — Merge-on-Read Mutations

**File:** `BaseRowDelta.java` (195 lines)

### 6.1 Operation Determination

```java
protected String operation() {
    if (addsDataFiles() && !addsDeleteFiles() && !deletesDataFiles()) return APPEND;
    if (addsDeleteFiles() && !addsDataFiles()) return DELETE;
    return OVERWRITE;
}
```

### 6.2 Validation (the most comprehensive of any producer)

```java
protected void validate(TableMetadata base, Snapshot parent) {
    // 1. Validate referenced data files still exist
    validateDataFilesExist(base, startingSnapshotId, referencedDataFiles, !validateDeletes, filter, parent);
    // 2. No concurrent data file additions in conflict partition
    validateAddedDataFiles(base, startingSnapshotId, conflictDetectionFilter, parent);
    // 3. No concurrent delete file additions
    validateNoNewDeleteFiles(base, startingSnapshotId, conflictDetectionFilter, parent);
    // 4. No new deletes targeting explicitly removed data files
    validateNoNewDeletesForDataFiles(base, startingSnapshotId, filter, removedDataFiles, parent);
    // 5. No conflicting file-level and position deletes
    validateNoConflictingFileAndPositionDeletes();
    // 6. No concurrent Deletion Vectors
    validateAddedDVs(base, startingSnapshotId, conflictDetectionFilter, parent);
}
```

---

## 7. PyIceberg Current State Mapping

### 7.1 What Exists

| Java Component | PyIceberg Equivalent | Status | Reference |
|---------------|---------------------|--------|-----------|
| `SnapshotProducer.commit()` retry loop | `Transaction.commit_transaction()` | **PR #3320** (open) | @lawofcycles |
| `validate()` hook | `_validate_concurrency()` | **PR #3320** (open) | @lawofcycles |
| `_refresh_for_retry()` | `_refresh_for_retry()` | **PR #3320** (open) | @lawofcycles |
| `FastAppend` | `_FastAppendFiles` | ✅ Merged | Existing |
| `MergeAppend` | `_MergeAppendFiles` | ✅ Merged | Existing |
| `BaseOverwriteFiles` | `_OverwriteFiles` | ✅ Merged | Existing |
| `StreamingDelete` | `_DeleteFiles` | ✅ Merged | Existing |
| Validation functions | `validate.py` | ✅ Merged | #1935, #1938, #2050, #3049 |
| `ExpireSnapshots` (metadata-only) | `expire_snapshots()` | ✅ Merged | PR #1880 |
| `write.metadata.delete-after-commit` | Implemented | ✅ Merged | PR #1607 |

### 7.2 What Is In-Flight

| Java Component | PyIceberg PR | Status | Owner |
|---------------|-------------|--------|-------|
| `BaseRewriteFiles` | PR #3131 | Open | @qzyu999 |
| Retry + validation wiring | PR #3320 | Open | @lawofcycles |
| `table.maintenance.compact()` | PR #3124 | Open (blocked on #3131) | @qzyu999 |

### 7.3 What Is Missing

| Java Component | PyIceberg Issue | Gap Description |
|---------------|----------------|-----------------|
| `FileCleanupStrategy` | None | No physical file cleanup after snapshot expiration |
| `IncrementalFileCleanup` | None | No incremental cleanup strategy |
| `ReachableFileCleanup` | None | No reachable cleanup strategy |
| `BaseRewriteManifests` | #270 (PR #1661 closed/stale) | No manifest compaction |
| `DeleteOrphanFiles` action | #1200 (PR #1958 closed/stale) | No orphan file discovery |
| `BaseRowDelta` | #1808 | No MoR write support |
| `SizeBasedFileRewritePlanner` | Part of #1092 | No size-based planning |
| `RewriteDataFilesCommitManager` | None | No partial-progress commit orchestration |
| `BaseCommitService` | None | No async commit service |
| `BinPackRewriteFilePlanner` | None | No bin-pack planning |
| `RewritePositionDeleteFiles` | None | No position delete compaction |
| `ConvertEqualityDeleteFiles` | #3270 | No eq-delete conversion |
| `RemoveDanglingDeleteFiles` | None | No stale delete cleanup |
| `validateAddedDVs()` | None | No DV conflict detection |
| `CleanupLevel` enum | None | No granular cleanup control |

---

## 8. Interaction Analysis: Existing PRs

### 8.1 Dependency Graph

```
PR #3320 (Commit Retry + Validation)
  │
  ├──► PR #3131 (RewriteFiles / REPLACE producer)
  │      │
  │      └──► PR #3124 (table.maintenance.compact())
  │             │
  │             └──► Future: SizeBasedPlanner, CommitManager
  │
  ├──► Future: FileCleanupStrategy (for ExpireSnapshots)
  │
  └──► Future: BaseRowDelta (MoR writes)
```

### 8.2 PR #3320 ↔ PR #3131 Integration

**PR #3320** introduces:
- `_refresh_for_retry()` — resets snapshot ID, UUID, clears cached manifests
- `_validate_concurrency()` — calls `validate.py` functions during retry
- `_rebuild_snapshot_updates()` — strips snapshot updates and re-executes producers

**PR #3131** must:
1. Override `_validate_concurrency()` to call `_validate_no_new_deletes_for_data_files()` for replaced files
2. Inherit `_refresh_for_retry()` from base (data files survive retry, only manifests regenerate)
3. Set `operation = Operation.REPLACE` to enable correct conflict resolution semantics

**Current design divergence in PR #3131:** The PR currently does NOT wire validation. The `_RewriteFiles` producer must gain a `_validate_concurrency()` override that mirrors `BaseRewriteFiles.validate()`:

```python
def _validate_concurrency(self, base_table: Table, parent_snapshot: Optional[Snapshot]) -> None:
    if self._replaced_data_files:
        _validate_no_new_deletes_for_data_files(
            table=base_table,
            starting_snapshot=parent_snapshot,
            conflict_detection_filter=None,  # check all partitions
            data_files=self._replaced_data_files,
            parent_snapshot=parent_snapshot,
        )
```

### 8.3 PR #3124 ↔ PR #3131 Relationship

**PR #3124** (`table.maintenance.compact()`) currently:
- Reads entire table via `.to_arrow()` (in-memory)
- Uses `table.overwrite()` (Operation.OVERWRITE, not REPLACE)
- No planning heuristics

**After PR #3131 merges**, PR #3124 should be redesigned to:
1. Use `table.replace(files_to_delete, files_to_add)` instead of `table.overwrite()`
2. Add basic size-based planning (filter files below target size)
3. Write new files externally, then pass `DataFile` references to the replace API

### 8.4 PR #1880 (ExpireSnapshots) — Missing Cleanup

PR #1880 is merged but metadata-only. The critical gap:

```python
# Current (merged):
table.expire_snapshots()  # Updates metadata, removes snapshot references

# Missing:
# After metadata commit, physically delete:
#   - Data files only referenced by expired snapshots
#   - Manifest files only referenced by expired snapshots
#   - Manifest list files of expired snapshots
#   - Statistics files of expired snapshots
```

This requires implementing `FileCleanupStrategy` with at least the `IncrementalFileCleanup` algorithm.

---

## 9. The ACID Invariants

### 9.1 Why Compaction Cannot Corrupt Data

**Theorem:** Given immutable data files and atomic CAS metadata commits, `REPLACE` operations are mathematically safe.

**Proof sketch:**
1. Data files are immutable once written (content-addressed by path)
2. `REPLACE` writes new files BEFORE attempting commit
3. Commit is atomic CAS: either old→new succeeds, or fails entirely
4. On CAS failure: old files remain valid, new files are orphans (cleaned up)
5. On CAS success: old files become unreferenced (cleaned up by `ExpireSnapshots`)
6. Concurrent failures result in "safe aborts" — never in partial state

### 9.2 The Conflict Detection Contract

| Scenario | Java Validation | Outcome |
|----------|----------------|---------|
| Compaction + concurrent append | No conflict (different files) | Both succeed |
| Compaction + concurrent delete on SAME file | `validateNoNewDeletesForDataFiles` fails | `ValidationException` — compaction aborts |
| Compaction + concurrent delete on DIFFERENT file | No conflict | Both succeed |
| Two concurrent compactions on SAME files | `failMissingDeletePaths()` — deleted file not found | `ValidationException` |
| Two concurrent compactions on DIFFERENT files | No conflict | Both succeed |

### 9.3 Sequence Number Semantics for REPLACE

When compaction rewrites files, the new data files can use either:
- **Inherited sequence number** (`dataSequenceNumber(startingSeq)`): Prevents new equality deletes from being applied to compacted files that predate them. Used by `useStartingSequenceNumber` in `RewriteDataFilesCommitManager`.
- **New sequence number** (default): The new files get the commit's sequence number. Simpler but may cause equality delete conflicts.

---

## 10. Implementation Roadmap for PyIceberg

### Phase 0: Foundation (PR #3320)
- [x] Commit retry loop in `Transaction.commit_transaction()`
- [x] `_refresh_for_retry()` lifecycle hook
- [x] `_validate_concurrency()` lifecycle hook
- [x] Wire `validate.py` functions into `_DeleteFiles` and `_OverwriteFiles`
- [ ] **Merge PR #3320**

### Phase 1: REPLACE Primitive (PR #3131)
- [ ] `_RewriteFiles` snapshot producer with `Operation.REPLACE`
- [ ] Override `_validate_concurrency()` with `validateNoNewDeletesForDataFiles`
- [ ] `table.replace(files_to_delete, files_to_add)` API
- [ ] `transaction.replace(files_to_delete, files_to_add)` API
- [ ] Sequence number control via `data_sequence_number` parameter

### Phase 2: Basic Compaction (PR #3124 redesign)
- [ ] `SizeBasedPlanner`: filter files by `[min_size, max_size]` thresholds
- [ ] Bin-packing into groups of `max-file-group-size-bytes`
- [ ] `table.maintenance.compact()` using `table.replace()` internally
- [ ] In-memory rewrite via PyArrow (MVP)

### Phase 3: Snapshot Expiration with Cleanup
- [ ] `FileCleanupStrategy` abstract base
- [ ] `IncrementalFileCleanup`: diff-based cleanup for simple main-branch cases
- [ ] `ReachableFileCleanup`: reference-counting for complex multi-branch cases
- [ ] `CleanupLevel` enum: `ALL`, `MANIFEST_AND_LIST`, `NONE`
- [ ] Wire into existing `expire_snapshots()` as post-commit phase

### Phase 4: Manifest Compaction
- [ ] `_RewriteManifests` snapshot producer
- [ ] `clusterBy()` and `rewriteIf()` APIs
- [ ] File count invariant validation
- [ ] `table.maintenance.rewrite_manifests()`

### Phase 5: Orphan File Cleanup
- [ ] `all_known_files()` via manifest/metadata scanning
- [ ] Storage listing via `FileIO`
- [ ] Set difference → orphan candidates
- [ ] Age-based safety filter (default: 3 days)
- [ ] `table.maintenance.delete_orphan_files()`

### Phase 6: Advanced Features
- [ ] `BaseRowDelta` — MoR write support
- [ ] `validateAddedDVs()` — Deletion Vector conflict detection
- [ ] `RewriteDataFilesCommitManager` — partial-progress commits
- [ ] `BaseCommitService` — async commit orchestration
- [ ] Distributed execution support (Ray/Dask integration points)
