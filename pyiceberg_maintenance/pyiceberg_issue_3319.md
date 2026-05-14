# Add commit retry with data conflict validation

**State:** open
**Created by:** @lawofcycles
**Created at:** 2026-05-02 07:15:53.000 UTC

### Feature Request / Improvement
## Summary

This proposal adds two capabilities to PyIceberg's write path, matching the behavior of Java Iceberg's `SnapshotProducer.commit()`.

1. Automatic retry with exponential backoff when a catalog commit fails due to a concurrent transaction (`CommitFailedException`)
2. Data conflict validation during retry to detect incompatible concurrent modifications (`ValidationException`)

These two capabilities work together. The retry loop refreshes table metadata and re-runs validation on each attempt. If validation passes, the commit is retried with regenerated manifests. If validation detects a real data conflict, the operation aborts without retry.

## Motivation

When multiple processes write to the same Iceberg table concurrently, PyIceberg fails immediately with `CommitFailedException` regardless of whether the writes actually conflict. This happens because `AssertRefSnapshotId` detects that the branch head has moved, but no retry or conflict analysis is attempted.

In practice, many concurrent write scenarios are safe to retry automatically. For example, concurrent appends never conflict with each other, and streaming writes rarely conflict with compaction operations targeting older partitions. Java Iceberg handles these cases transparently through its retry loop in `SnapshotProducer.commit()`.

Through a series of community contributions (#1935, #1938, #2050, #3049), PyIceberg already has the individual validation functions in `pyiceberg/table/update/validate.py`, built as the foundation for this work. This proposal completes the picture by integrating them into the commit flow alongside automatic retry.

## Background

### Java's commit flow

Java's `SnapshotProducer.commit()` works as follows:

```
commit():
    retry(CommitFailedException, exponential_backoff):
        apply():
            refresh()                  # Load latest metadata from catalog
            validate(base, parent)     # Check for data conflicts (subclass override)
            manifests = apply(...)     # Generate manifests
        ops.commit(base, updated)      # Atomic commit to catalog (CAS)
```

On each retry attempt, `apply()` is called again, which refreshes metadata, re-runs validation against the latest snapshot, and regenerates manifests. Previous attempt's uncommitted manifests are cleaned up.

Three outcomes are possible:

| Catalog commit | Validation on retry | Result |
|---|---|---|
| Succeeds | (not executed) | Commit complete |
| Fails (CommitFailedException) | Passes | Retry succeeds |
| Fails (CommitFailedException) | Fails (ValidationException) | Abort |

### PyIceberg's current commit flow

```
_SnapshotProducer._commit()     → generates manifests, returns (updates, requirements)
Transaction._stage()            → accumulates updates
Transaction.commit_transaction() → Table._do_commit() → Catalog.commit_table()
```

Manifest generation and catalog commit happen in separate methods. `_SnapshotProducer` does not have access to the catalog commit step, and `Transaction.commit_transaction()` does not have access to the snapshot producers that generated the manifests.

### Existing validation functions

Through a series of community contributions (#1935, #1938, #2050, #3049), the individual validation functions have been implemented in `pyiceberg/table/update/validate.py` as building blocks for concurrency safety. These were created as sub-tasks of #819 with the explicit goal of eventually integrating them into the write path. This proposal is the next step: wiring these functions into the commit flow.

| PyIceberg function | Java equivalent | Called by |
|---|---|---|
| `_validate_added_data_files` | [`MergingSnapshotProducer.validateAddedDataFiles`](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java) | [`BaseOverwriteFiles.validate()`](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/BaseOverwriteFiles.java), [`BaseRowDelta.validate()`](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/BaseRowDelta.java) |
| `_validate_deleted_data_files` | `MergingSnapshotProducer.validateDeletedDataFiles` | `BaseOverwriteFiles.validate()` |
| `_validate_no_new_delete_files` | `MergingSnapshotProducer.validateNoNewDeleteFiles` | `BaseOverwriteFiles.validate()`, `BaseRowDelta.validate()` |
| `_validate_no_new_deletes_for_data_files` | `MergingSnapshotProducer.validateNoNewDeletesForDataFiles` | `BaseOverwriteFiles.validate()`, `BaseRewriteFiles.validate()`, `BaseRowDelta.validate()` |

In Java, these are invoked from within `SnapshotProducer.apply()`, which is called on each retry attempt inside `SnapshotProducer.commit()`. Spark enables them by calling `validateNoConflictingData()` / `validateNoConflictingDeletes()` in [`SparkWrite.java`](https://github.com/apache/iceberg/blob/main/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java) based on the isolation level.

## Design

### Retry location

The retry loop lives in `Transaction.commit_transaction()`. When a catalog commit fails with `CommitFailedException`, the Transaction refreshes table metadata and re-executes each snapshot producer to regenerate manifests against the updated state.

This design follows from how PyIceberg structures its commit flow. A single `Transaction` can contain multiple snapshot producers executed sequentially. For example, `Transaction.delete()` first runs a `_DeleteFiles` producer (for whole-file deletes), then conditionally runs an `_OverwriteFiles` producer (for partial rewrites). Both are staged in the Transaction and committed together as one atomic catalog update. The retry mechanism needs to rebuild all of them as a unit.

To make this possible, snapshot producers register themselves with the Transaction when they commit. On retry, the Transaction re-executes each producer in registration order.

### Retry flow

```python
class Transaction:
    _snapshot_producers: list[_SnapshotProducer]

    def commit_transaction(self) -> Table:
        if not self._updates:
            return self._table

        num_retries = ...  # from commit.retry.num-retries
        for attempt in range(num_retries + 1):
            try:
                self._table._do_commit(self._updates, self._requirements)
                return self._table
            except CommitFailedException:
                if attempt == num_retries:
                    raise
                self._table.refresh()
                self._rebuild_snapshot_updates()
                sleep(backoff_with_jitter(attempt))

    def _rebuild_snapshot_updates(self):
        # Keep non-snapshot updates (e.g. SetPropertiesUpdate)
        self._updates = tuple(
            u for u in self._updates
            if not isinstance(u, (AddSnapshotUpdate, SetSnapshotRefUpdate))
        )
        self._requirements = tuple(
            r for r in self._requirements
            if not isinstance(r, AssertRefSnapshotId)
        )
        # Re-execute each producer in order
        for producer in self._snapshot_producers:
            producer._cleanup_uncommitted()
            producer._refresh_for_retry()
            producer._validate_concurrency()  # raises ValidationException if conflict
            updates, requirements = producer._commit()
            self._stage(updates, requirements)
```

### Producer refresh

On retry, each snapshot producer resets its state to generate new manifests against the updated metadata:

```python
class _SnapshotProducer:
    def _refresh_for_retry(self):
        self._parent_snapshot_id = (
            snapshot.snapshot_id
            if (snapshot := self._transaction.table_metadata.snapshot_by_name(self._target_branch))
            else None
        )
        self._snapshot_id = self._transaction.table_metadata.new_snapshot_id()
        self._manifest_num_counter = itertools.count(0)
        self.commit_uuid = uuid.uuid4()
```

`_added_data_files` and `_deleted_data_files` are preserved across retries. Data files written to storage are reusable since their content does not depend on the snapshot ID. Only manifests (which reference the snapshot ID and parent) need regeneration.

`_DeleteFiles` additionally clears its `_compute_deletes` cached property, which depends on `_parent_snapshot_id`.

### Producer registration

Snapshot producers register with the Transaction when `commit()` is called:

```python
class _SnapshotProducer:
    def commit(self) -> None:
        self._transaction._register_snapshot_producer(self)
        self._transaction._apply(*self._commit())
```

This preserves the existing `_commit()` → `_apply()` → `_stage()` call chain. The producer reference is used only during retry.

### Validation

Each snapshot producer subclass implements `_validate_concurrency()`.

**_FastAppendFiles / _MergeAppendFiles:** No validation needed. Appends do not conflict with any operation because Iceberg has no primary key constraint.

**_OverwriteFiles and _DeleteFiles:**

```python
def _validate_concurrency(self):
    if self._starting_snapshot is None:
        return
    if isolation_level == IsolationLevel.SERIALIZABLE:
        _validate_added_data_files(...)
    if conflict_detection_filter is not None:
        _validate_no_new_delete_files(...)
        _validate_deleted_data_files(...)
    if self._deleted_data_files:
        _validate_no_new_deletes_for_data_files(...)
```

Both `_OverwriteFiles` and `_DeleteFiles` use the same set of validation functions. The user's overwrite/delete filter (`_predicate`) is used as the `conflict_detection_filter`, which enables file-level metrics-based conflict detection. The filter is evaluated against each data file's column statistics (min/max, null count) using `_InclusiveMetricsEvaluator`. Files that cannot contain matching rows are excluded from conflict checks. For partitioned tables with conditions on partition columns, this effectively provides partition-level filtering because file statistics align with partition boundaries.

When `_predicate` is `AlwaysFalse()` (no row filter), `conflict_detection_filter` is set to `None` and `_validate_no_new_delete_files` / `_validate_deleted_data_files` are skipped. This matches Java's `BaseOverwriteFiles.validate()`, which skips these checks when `rowFilter() == AlwaysFalse()` to avoid treating all files as conflicts. `_validate_no_new_deletes_for_data_files` still runs if `_deleted_data_files` is non-empty, as it checks specific files rather than a filter range.

For example, when a user calls `table.delete("category == 'old'")`, only concurrent changes to files whose column statistics overlap with `category == 'old'` are considered conflicts.

### Conflict detection filter limitation

In Java Iceberg, Spark passes the scan's filter expressions as the `conflictDetectionFilter`, enabling fine-grained conflict detection even for operations without an explicit row filter. PyIceberg's `_OverwriteFiles` does not currently carry scan filter information.

This means that for filter-less full-table overwrites (`table.overwrite(df)` with `ALWAYS_TRUE`), the conflict detection filter effectively becomes `AlwaysTrue`, treating any concurrent change to the table as a conflict. In practice:

- Operations with explicit filters (e.g. `table.delete("category == 'old'")`) work well. Concurrent changes to different partitions are correctly identified as non-conflicting and retried successfully.
- Filter-less full-table overwrites are more conservative than Spark. A concurrent append to a completely different partition would cause `ValidationException`, whereas Spark could determine it is safe through its scan filter.

This is a safe-side limitation: data integrity is never compromised. The only effect is that some operations that Spark would retry successfully will instead fail with `ValidationException` in PyIceberg. Concurrent full-table overwrites are uncommon in practice, so the real-world impact is expected to be small. Improving this would require passing scan filter information into `_OverwriteFiles`, which can be done as a follow-up.

### Isolation levels

A new `IsolationLevel` enum is introduced:

```python
class IsolationLevel(str, Enum):
    SERIALIZABLE = "serializable"
    SNAPSHOT = "snapshot"
```

Read from table properties:
- `write.delete.isolation-level` (default: serializable)
- `write.update.isolation-level` (default: serializable)
- `write.merge.isolation-level` (default: serializable)

The difference: under serializable isolation, `_validate_added_data_files` is called to detect conflicting appends to the same partition. Under snapshot isolation, this check is skipped, allowing concurrent appends to the same partition as long as no deletes conflict.

### Retry configuration

Read from table properties, matching Java defaults:

- `commit.retry.num-retries` (default: 4)
- `commit.retry.min-wait-ms` (default: 100)
- `commit.retry.max-wait-ms` (default: 60000)
- `commit.retry.total-timeout-ms` (default: 1800000)

### Manifest cleanup

Uncommitted manifests from failed attempts are cleaned up on a best-effort basis. Each producer tracks the paths of manifests it writes, and `_cleanup_uncommitted()` deletes them before regeneration.

## Scope

### Included

- Retry loop in `Transaction.commit_transaction()`
- `IsolationLevel` enum and table property reading
- Validation integration for `_OverwriteFiles` and `_DeleteFiles`
- `commit.retry.*` table property support
- Uncommitted manifest cleanup
- Unit tests and integration tests

### Not included

- RowDelta (Merge-on-Read is not yet supported in PyIceberg)
- `_RewriteFiles` validation (follow-up after #3131 merges)
- 
## References

- Java: [SnapshotProducer.java](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/SnapshotProducer.java), [BaseOverwriteFiles.java](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/BaseOverwriteFiles.java)
- Spark: [SparkWrite.java](https://github.com/apache/iceberg/blob/main/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java)
- Iceberg spec: [Commit Conflict Resolution and Retry](https://iceberg.apache.org/spec/#commit-conflict-resolution-and-retry)
- Blog: [Manage concurrent write conflicts in Apache Iceberg on the AWS Glue Data Catalog](https://aws.amazon.com/blogs/big-data/manage-concurrent-write-conflicts-in-apache-iceberg-on-the-aws-glue-data-catalog/)



