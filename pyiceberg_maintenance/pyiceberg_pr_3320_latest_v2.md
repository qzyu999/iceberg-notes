# PR #3320: Commit Retry and Validation for PyIceberg — Principal Engineer Review

> **Reviewer:** Principal Engineer deep review  
> **Date:** 2026-06-14  
> **PR:** https://github.com/apache/iceberg-python/pull/3320  
> **Issue:** https://github.com/apache/iceberg-python/issues/3319  
> **Branch:** `feat/commit-retry-and-validation` (fetched as `pr-3320`)  
> **Base:** `d99e4633` (main at time of fork)  
> **Java Reference:** `apache/iceberg` repo (`SnapshotProducer.java`, `MergingSnapshotProducer.java`, `BaseOverwriteFiles.java`, `StreamingDelete.java`, `TableProperties.java`)

---

## 1. Summary of Changes

| File | +/- | Purpose |
|------|-----|---------|
| `pyiceberg/table/__init__.py` | +120/-7 | Retry loop in `Transaction._commit_transaction()`, property reads, `_rebuild_snapshot_updates()`, manifest cleanup |
| `pyiceberg/table/update/snapshot.py` | +121/-2 | `_SnapshotProducer` gains retry lifecycle methods, `_validate_concurrency()`, manifest tracking, `commit()` override |
| `pyiceberg/table/update/validate.py` | +16/-7 | Fix `_validation_history` boundary semantics (from_snapshot exclusion) |
| `pyiceberg/table/snapshots.py` | +7 | New `IsolationLevel` enum |
| `mkdocs/docs/configuration.md` | +20 | User-facing documentation for retry + isolation properties |
| `tests/table/test_commit_retry.py` | +684 | New comprehensive test suite |
| `tests/integration/test_writes/test_optimistic_concurrency.py` | +27/-33 | Update integration tests to expect new behavior |
| `tests/table/test_validate.py` | +16/-9 | Fix existing validation tests for corrected boundary semantics |
| **Total** | **+971/-40** | |

---

## 2. Architectural Design Review

### 2.1 How Java Does It (Reference Architecture)

In Java, the retry-validation contract is:

1. **`SnapshotProducer.commit()`** wraps the entire commit in `Tasks.foreach(ops).retry(N).exponentialBackoff(...).onlyRetryOn(CommitFailedException.class).run(...)`.
2. **Inside the retry lambda**, `apply()` is called which does: `refresh()` → `runValidations(parentSnapshot)` → build manifests → write manifest list → return new `Snapshot`.
3. **Each retry** calls `apply()` again from scratch — the producer re-reads table metadata, re-computes manifests, re-validates against the new parent snapshot. This is a **full re-execution per retry**.
4. **Validation is per-subclass**: `BaseOverwriteFiles.validate()` does data-file and delete-file conflict detection. `StreamingDelete.validate()` only validates files-to-delete-exist (no isolation-level-based conflict detection at the core library level — that's pushed to engine integrations like Spark).
5. **Isolation levels** are applied at the **engine layer** (Spark's `SparkRowLevelOperationBuilder` reads `write.delete.isolation-level` and passes it down to set `validateNoConflictingData()` / `validateNoConflictingDeletes()` on the `OverwriteFiles` or `RowDelta` operation).

### 2.2 How This PR Does It (Python Architecture)

In this PR:

1. **`Transaction._commit_transaction()`** wraps the `_do_commit` call in a for-loop with exponential backoff + jitter.
2. **On `CommitFailedException`**, the transaction calls `self._table.refresh()` then `self._rebuild_snapshot_updates()` which strips old `AddSnapshotUpdate`/`SetSnapshotRefUpdate`/`AssertRefSnapshotId` and re-calls each producer's `_refresh_for_retry()` → `_validate_concurrency()` → `_commit()`.
3. **Validation** is consolidated in `_SnapshotProducer._validate_concurrency()` with **isolation level driven by table properties** — this is a design choice that differs from Java.
4. **`_FastAppendFiles`** overrides `_validate_concurrency()` as a no-op (appends never conflict).

### 2.3 Key Architectural Divergence from Java

**This is the most significant design decision in the PR:**

In Java, the isolation level and which validations to run are **explicitly set by the caller** (engine integration). The core `DeleteFiles` operation doesn't auto-validate based on table properties. In this PR, PyIceberg auto-reads `write.delete.isolation-level` / `write.update.isolation-level` from table properties and applies validation rules directly inside `_SnapshotProducer._validate_concurrency()`.

**Is this appropriate?** Yes, arguably. PyIceberg doesn't have a Spark-like engine intermediary — it's the end-user API directly. Moving validation into the producer is the right call for a library that's both the catalog client and the query engine. Java can afford to push this to Spark because Java's `OverwriteFiles` interface has explicit `.validateNoConflictingData()` method calls. PyIceberg has no such intermediate API surface.

**Risk:** This design means ALL deletes/overwrites in PyIceberg always validate. In Java, you can choose to skip validation by not calling `validateNoConflictingData()`. The PR's approach is safer (fail-closed) but less flexible. This is acceptable for a first implementation.

---

## 3. Critical Issues (MUST fix before merge)

### 3.1 ❌ `_validation_history` boundary fix has a semantic inversion risk

**Location:** `pyiceberg/table/update/validate.py`, lines 63-90

The PR changes the boundary semantics of `_validation_history`:

```python
if from_snapshot.snapshot_id == to_snapshot.snapshot_id:
    return [], set()
```

And adds early break:
```python
for snapshot in ancestors_between(from_snapshot, to_snapshot, table.metadata):
    if snapshot.snapshot_id == from_snapshot.snapshot_id:
        last_snapshot = snapshot
        break
```

And changes the final check from:
```python
if last_snapshot is not None and last_snapshot.snapshot_id != from_snapshot.snapshot_id:
```
to:
```python
if last_snapshot is None or last_snapshot.snapshot_id != from_snapshot.snapshot_id:
```

**Problem:** The change to `last_snapshot is None or ...` is correct if `ancestors_between` can return an empty iterator for a valid ancestry (e.g., if `from_snapshot` is `to_snapshot`'s direct parent and there are no intermediate snapshots). But there's a subtle issue: if `ancestors_between` returns an empty iterator because the ancestry is broken (not because they're adjacent), the old code would NOT raise (it would skip the check), but the NEW code WILL raise `"No matching snapshot found."`.

**Assessment:** This is actually the **correct** behavior — a broken ancestry chain should raise. The old code had a bug (silently accepting disconnected snapshots). The fix is correct.

**However**, the docstring says "from_snapshot is excluded from results" but the function parameter names are confusing:

```python
def _validation_history(
    table: Table,
    from_snapshot: Snapshot,   # This is actually the BOUNDARY/STOP snapshot (newer, exclusive)
    to_snapshot: Snapshot,     # This is actually the START snapshot (older, inclusive)
    ...
```

Looking more carefully at the callers:

```python
_validation_history(table, parent_snapshot, starting_snapshot, ...)
```

So `from_snapshot=parent_snapshot` (the CURRENT tip) and `to_snapshot=starting_snapshot` (the older base). The walk goes from `to_snapshot` BACKWARDS toward `from_snapshot`. So `from_snapshot` is the **stop point** (exclusive) and `to_snapshot` is the **start point** (inclusive). The naming is **inverted from intuition** ("from" = stop, "to" = start) which is confusing but matches the existing convention.

**Verdict:** The logic is correct but the naming is a pre-existing confusion. Not a blocker.

### 3.2 ❌ Missing `_starting_snapshot_id` initialization in `_SnapshotProducer.__init__`

**Looking at the diff**, the PR adds:
```python
self._starting_snapshot_id = self._parent_snapshot_id
```

This IS in the `__init__` method (after `_parent_snapshot_id` is set). The class-level annotation `_starting_snapshot_id: int | None` is also added. Field is properly declared and initialized. **Not an issue.**

### 3.3 ❌ `_validate_concurrency` uses `self._predicate` but may encounter `AlwaysFalse()`

**Location:** `_SnapshotProducer._validate_concurrency()`:

```python
conflict_detection_filter = self._predicate if self._predicate != AlwaysFalse() else None
```

**Analysis:** For `_FastAppendFiles`, this method is overridden as a no-op, so it's fine. For `_OverwriteFiles`, the predicate includes the partition-based filter from deleted files. For `_DeleteFiles`, the predicate is set by `delete_by_predicate()`.

When `delete()` is called with a predicate that doesn't produce any actual deletes (no matching files), `_compute_deletes` returns empty entries, `_commit()` returns `(), ()`, and no snapshot is produced — so `_validate_concurrency()` is never called on retry. This is correct.

For the overwrite created during partial file rewrites, the predicate is properly set via `delete_by_predicate()` call and `_starting_snapshot_id` is inherited from the delete producer. **This is correct.**

### 3.4 ~~❌~~ ✅ Multi-producer parent snapshot chain on retry — NOT A BUG

**Location:** `Transaction._rebuild_snapshot_updates()`:

```python
for producer in self._snapshot_producers:
    producer._refresh_for_retry()
    producer._validate_concurrency()
    updates, requirements = producer._commit()
    self._stage(updates, requirements)
```

**Initial concern:** If a `Transaction.delete()` produces BOTH a `_DeleteFiles` and an `_OverwriteFiles` producer (for partial file rewrites), both producers calling `_refresh_for_retry()` would seemingly get the SAME `_parent_snapshot_id` (the refreshed branch tip), breaking the snapshot chain.

**Analysis:** After careful examination, `Transaction.table_metadata` is a **computed property** (`update_table_metadata(self._table.metadata, self._updates)`) that reflects ALL staged updates in real-time. The loop processes producers **sequentially**:

1. Producer 1 (`_DeleteFiles`) calls `_refresh_for_retry()` → reads `table_metadata` (no staged snapshots yet) → gets parent = refreshed tip
2. Producer 1's `_commit()` + `_stage()` adds `AddSnapshotUpdate(X)` + `SetSnapshotRefUpdate(X, main)` to `self._updates`
3. Now `table_metadata` reflects snapshot X as the branch tip
4. Producer 2 (`_OverwriteFiles`) calls `_refresh_for_retry()` → reads `table_metadata` (now includes X) → gets parent = **X** ✅

The chain is preserved correctly because `_refresh_for_retry()` reads `self._transaction.table_metadata` which is recomputed with each staged update. The sequential staging within the loop ensures the correct parent snapshot propagation.

**Verdict:** NOT a bug. The design is subtle but correct. The `table_metadata` property's dynamic computation is the key enabler.

---

## 4. Moderate Issues (SHOULD fix)

### 4.1 ⚠️ `_isolation_level_property` is set as an instance attribute, not in `__init__`

**Location:** `_SnapshotProducer.__init__()`:

```python
self._isolation_level_property: str = TableProperties.WRITE_DELETE_ISOLATION_LEVEL
```

This is fine as a default. But it's set as `self._isolation_level_property: str = ...` — a type-annotated assignment in `__init__`. The class-level declarations only list:
```python
_written_manifests: list[str]
_uncommitted_manifests: list[str]
```

But NOT `_isolation_level_property`. This is inconsistent with the class's declaration style. It should be declared at the class level:

```python
_isolation_level_property: str
```

**Severity:** LOW — style inconsistency, not a bug. But PyIceberg tends to declare all instance attributes at the class level.

### 4.2 ⚠️ `random.uniform` in retry loop without seeding consideration

**Location:** `Transaction._commit_transaction()`:

```python
jitter = random.uniform(0, 0.25 * wait)
time.sleep((wait + jitter) / 1000.0)
```

The jitter factor is 0-25% of the wait time. This is reasonable. However, Java's exponential backoff has a fixed factor of `2.0`:

```java
.exponentialBackoff(min, max, total, 2.0 /* exponential */)
```

The Python implementation also uses base-2 exponential (`min_wait_ms * (2**attempt)`), which matches. The 25% jitter range is a common pattern and acceptable.

**One concern:** `random.uniform` uses the global PRNG state which is shared across threads. In a multi-threaded scenario (multiple tables committing concurrently), this is not thread-safe in CPython (though the GIL makes it practically safe). Consider `random.Random()` instance if thread safety is ever a concern. Not a blocker.

### 4.3 ⚠️ `commit()` method override in `_SnapshotProducer` shadows the base class

**Location:** `_SnapshotProducer.commit()`:

```python
def commit(self) -> None:
    self._transaction._register_snapshot_producer(self)
    self._transaction._apply(*self._commit())
```

The base class `UpdateTableMetadata` has:
```python
def commit(self) -> None:
    self._transaction._apply(*self._commit())
```

The PR **overrides** `commit()` to add producer registration. This is fine, but it means the override must always call `_apply()` — it doesn't call `super().commit()`. This is a minor violation of the Liskov substitution principle. If anyone adds logic to the base `commit()` in the future, the override won't pick it up.

**Fix:** Call `super().commit()` and add registration before:
```python
def commit(self) -> None:
    self._transaction._register_snapshot_producer(self)
    super().commit()
```

**Severity:** LOW — cosmetic, reduces maintenance risk.

### 4.4 ⚠️ Java has `write.merge.isolation-level` — Python PR only has delete and update

Java defines THREE isolation level properties:
- `write.delete.isolation-level`
- `write.update.isolation-level`  
- `write.merge.isolation-level`

The PR only defines two (`WRITE_DELETE_ISOLATION_LEVEL`, `WRITE_UPDATE_ISOLATION_LEVEL`). This is acceptable because PyIceberg doesn't have a MERGE operation as a distinct concept (Spark MERGE INTO), but should be documented as a known gap for completeness.

### 4.5 ⚠️ `_starting_snapshot_id` propagation for the overwrite in `Transaction.delete()`

**Location:** `Transaction.delete()` → overwrite path:

```python
overwrite_snapshot._starting_snapshot_id = delete_snapshot._starting_snapshot_id
```

This correctly propagates the starting snapshot from the delete to the overwrite. Good design — both producers validate from the same base point.

However, in the `Transaction.overwrite()` method (not `delete()`):

```python
self.delete(
    delete_filter=delete_filter,
    snapshot_properties=snapshot_properties,
    branch=branch,
    _isolation_level_property=TableProperties.WRITE_UPDATE_ISOLATION_LEVEL,
)
```

The internal `delete()` call creates a `_DeleteFiles` producer that sets `_starting_snapshot_id = _parent_snapshot_id` (from `__init__`). Then if a partial delete triggers an overwrite, that overwrite also gets the correct starting snapshot. But the **subsequent append** in `Transaction.overwrite()`:

```python
with self._append_snapshot_producer(snapshot_properties, branch=branch) as append_files:
    append_files.commit_uuid = append_snapshot_commit_uuid
    ...
```

This append gets a NEW `_starting_snapshot_id` from its own `__init__` (which will be the parent at that point in the transaction). Since `_FastAppendFiles` overrides `_validate_concurrency()` as a no-op, this doesn't cause validation issues. But it means on retry, the append might pick up a different parent than expected.

**Assessment:** Not a bug due to the no-op override, but worth noting as a potential future issue if append validation is ever added.

---

## 5. Minor Issues (Nits)

### 5.1 Unrelated formatting change in `DataScan`

The diff includes a formatting change to `DataScan._residual_evaluator_factory()`:

```python
-        return lambda datafile: (
-            residual_evaluator_of(
-                spec=spec,
-                expr=self.row_filter,
-                case_sensitive=self.case_sensitive,
-                schema=self.table_metadata.schema(),
-            )
+        return lambda datafile: residual_evaluator_of(
+            spec=spec,
+            expr=self.row_filter,
+            case_sensitive=self.case_sensitive,
+            schema=self.table_metadata.schema(),
         )
```

This is purely a **cosmetic reformatting** (the outer parens are grouping, not a tuple — no trailing comma). It doesn't change behavior but should be in a separate commit/PR per PyIceberg convention ("One concern per PR"). Minor violation.

### 5.2 `_case_sensitive` parameter threading in `_build_delete_files_partition_predicate`

The diff adds `self._case_sensitive` to a call:

```python
self.delete_by_predicate(
    self._transaction._build_partition_predicate(
        partition_records=partition_records, schema=self.schema(), spec=self.spec(spec_id)
    ),
    self._case_sensitive,
)
```

Previously `delete_by_predicate` was called without the second positional arg, defaulting to `case_sensitive=True`. Now it explicitly passes `self._case_sensitive`. This is correct and improves consistency. However, this is a behavior change for case-insensitive operations and should be called out in the PR description.

### 5.3 Imports inside methods

```python
def _validate_concurrency(self) -> None:
    from pyiceberg.table import TableProperties
    from pyiceberg.table.snapshots import IsolationLevel
    from pyiceberg.table.update.validate import (...)
```

These imports are inside the method to avoid circular imports. This is acceptable in Python but adds per-call overhead. Since `_validate_concurrency()` is only called during commits (infrequent), this is fine.

### 5.4 `_snapshot_producers: list[Any]` typing

**Location:** `Transaction.__init__()`:

```python
self._snapshot_producers: list[Any] = []
```

Using `Any` here loses type safety. Since `_SnapshotProducer` is the actual type, this should be:

```python
from __future__ import annotations
# ...
self._snapshot_producers: list[_SnapshotProducer[Any]] = []
```

Or import conditionally with `TYPE_CHECKING`. This is a minor typing issue.

### 5.5 `_register_snapshot_producer` typing

```python
def _register_snapshot_producer(self, producer: Any) -> None:
```

Same issue — `producer` should be typed as `_SnapshotProducer[Any]`.

### 5.6 Test file uses `from typing import Any` for monkey-patching

The test file extensively uses `Any` typing for the monkey-patching closures. This is acceptable in test code.

---

## 6. Java Parity Analysis

| Java Feature | Python PR Status | Parity | Notes |
|-------------|-----------------|--------|-------|
| `Tasks.foreach().retry(N).exponentialBackoff()` | `for attempt in range(num_retries + 1)` + manual sleep | ✅ Equivalent | Python adds jitter (Java doesn't in the default config — jitter comes from the `Tasks` util) |
| `.onlyRetryOn(CommitFailedException.class)` | `except CommitFailedException:` | ✅ | |
| `COMMIT_NUM_RETRIES` property | `COMMIT_NUM_RETRIES` | ✅ | Same name, same default (4) |
| `COMMIT_MIN_RETRY_WAIT_MS` property | `COMMIT_MIN_RETRY_WAIT_MS` | ✅ | Same name, same default (100) |
| `COMMIT_MAX_RETRY_WAIT_MS` property | `COMMIT_MAX_RETRY_WAIT_MS` | ✅ | Same name, same default (60000) |
| `COMMIT_TOTAL_RETRY_TIME_MS` property | `COMMIT_TOTAL_RETRY_TIME_MS` | ✅ | Same name, same default (1800000) |
| `IsolationLevel` enum | `IsolationLevel(str, Enum)` | ✅ | |
| `write.delete.isolation-level` | `WRITE_DELETE_ISOLATION_LEVEL` | ✅ | |
| `write.update.isolation-level` | `WRITE_UPDATE_ISOLATION_LEVEL` | ✅ | |
| `write.merge.isolation-level` | ❌ Not implemented | ⚠️ | PyIceberg has no MERGE concept |
| `SnapshotProducer.apply()` per retry (full re-execution) | `_refresh_for_retry()` + `_validate_concurrency()` + `_commit()` | ⚠️ | Python separates validation from manifest generation |
| `SnapshotProducer.refresh()` | `self._table.refresh()` | ✅ | |
| `cleanUncommitted(Set<ManifestFile> committed)` | `_cleanup_uncommitted()` + `_clean_all_uncommitted()` | ⚠️ | Java passes the committed set and diffs; Python tracks written vs uncommitted lists |
| `validate()` called by each subclass | `_validate_concurrency()` on base class | ⚠️ | Different design — Python uses table-property-driven validation in base, Java uses caller-specified validation in subclasses |
| `BaseOverwriteFiles.validate()` — explicit `validateNewDataFiles`/`validateNewDeletes` flags | Implicit based on isolation level + predicate presence | ⚠️ | Python's approach is less granular but more automatic |
| `StreamingDelete.validate()` — only `validateFilesToDeleteExist` | `_validate_concurrency()` does full conflict detection | ⚠️ | Python is MORE strict than Java core (Java pushes this to Spark) |
| Manifest list cleanup (delete unused lists from failed attempts) | `_cleanup_uncommitted_manifests()` | ⚠️ | Java cleans manifest lists AND manifests; Python only cleans manifests |
| `CommitMetrics` tracking | ❌ Not implemented | ⚠️ | No commit metrics/timing |
| `strictCleanup` flag | ❌ Not implemented | ⚠️ | Minor — Java has a flag to control cleanup behavior |
| `CommitStateUnknownException` handling | ❌ Not implemented | ⚠️ | Java has special handling for "commit state unknown" |

### Key Parity Observations:

1. **Python is stricter than Java core.** Java's `StreamingDelete` doesn't validate conflicts at all at the library level — that's Spark's job. Python auto-validates based on table properties. This is the RIGHT choice for a direct-user library but worth documenting.

2. **Manifest LIST cleanup is missing.** Java's `commit()` method tracks `manifestLists` (the manifest list files) and deletes unused ones from failed attempts. Python only tracks and cleans manifests (the individual manifest files). The manifest list files from failed retry attempts will leak. This should be a TODO.

3. **`CommitStateUnknownException`** is not handled. If the catalog returns an ambiguous response (e.g., timeout after the commit may or may not have succeeded), the Python implementation will retry, potentially double-committing. Java catches this explicitly and re-throws without retry.

---

## 7. Code Quality Assessment

### 7.1 Does it match PyIceberg style?

| Aspect | Assessment |
|--------|-----------|
| Type annotations | ⚠️ Uses `Any` where `_SnapshotProducer` type would be better |
| Docstrings | ✅ All new public/protected methods have docstrings |
| Naming conventions | ✅ Follows `_private_method` convention |
| Import style | ✅ Conditional imports for circular deps |
| Error handling | ✅ Proper exception re-raise, logging for cleanup failures |
| Test structure | ✅ Tests use `catalog` fixture, proper isolation |
| Magic numbers | ✅ All constants are named in `TableProperties` |

### 7.2 Potential Flakiness

1. **`time.sleep()` in retry loop** — Tests that verify retry count use `patch.object` on `_rebuild_snapshot_updates`, which bypasses the sleep. No test actually sleeps. Good.

2. **`test_uncommitted_manifests_tracked_correctly`** — Checks `uncommitted_count_during_rebuild > 0` which depends on internal state during rebuild. If the implementation changes how manifests are tracked, this test becomes brittle. It's testing implementation details rather than behavior.

3. **`test_concurrent_partial_deletes_on_different_partitions_succeed`** — This relies on the partition filter correctly identifying non-overlapping partitions. If the inclusive projection algorithm changes, this test could fail. Low risk.

4. **No concurrency testing** — All tests simulate concurrency by loading two table references and committing sequentially. This doesn't test actual thread-safety. Acceptable for unit tests, but should be noted.

### 7.3 Vibe Coding Red Flags

1. **The `_cleanup_uncommitted_manifests()` vs `_clean_all_uncommitted()` naming** is confusing. One cleans previous-attempt manifests, the other cleans everything. The names don't make this distinction clear. A comment or rename would help (`_cleanup_stale_manifests_after_success` vs `_cleanup_all_manifests_on_abort`).

2. **The `except Exception:` block** in `_commit_transaction()`:
```python
except Exception:
    for producer in self._snapshot_producers:
        producer._clean_all_uncommitted()
    raise
```
This catches ALL exceptions including `KeyboardInterrupt` (via `BaseException` → no, `Exception` doesn't catch `BaseException`). Actually, `Exception` is correct here — it catches `ValidationException` (which is the expected abort path) and any unexpected errors, but not `KeyboardInterrupt`/`SystemExit`. Good.

3. **No `finally` block for cleanup** — If `_rebuild_snapshot_updates()` raises (e.g., `ValidationException` during `_validate_concurrency()`), the manifests from the current attempt are NOT cleaned. The `except Exception:` only catches the outer exception, but `_rebuild_snapshot_updates()` is called INSIDE the try block... let me trace through:

```python
try:
    for attempt in range(num_retries + 1):
        try:
            self._table._do_commit(...)
            self._cleanup_uncommitted_manifests()
            break
        except CommitFailedException:
            ...
            self._table.refresh()
            self._rebuild_snapshot_updates()  # This can raise ValidationException
except Exception:
    for producer in self._snapshot_producers:
        producer._clean_all_uncommitted()
    raise
```

If `_rebuild_snapshot_updates()` raises `ValidationException`, it's caught by the OUTER `except Exception:`, which calls `_clean_all_uncommitted()`. This is correct!

Inside `_rebuild_snapshot_updates()`, the producers call `_commit()` which writes NEW manifests and manifest lists. If `_validate_concurrency()` passes for the first producer but the second producer's `_validate_concurrency()` fails, the first producer's newly written manifests are on `_written_manifests` but haven't been moved to `_uncommitted_manifests`. So `_clean_all_uncommitted()` cleans both lists (`itertools.chain(self._uncommitted_manifests, self._written_manifests)`), which is correct.

**Actually this is well-designed.** The `_clean_all_uncommitted()` method intentionally chains both lists.

---

## 8. Test Coverage Assessment

| Scenario | Covered? | Test |
|----------|----------|------|
| Append + Append (retry succeeds) | ✅ | `test_commit_retry_on_commit_failed` |
| Delete + Delete (same data, fails) | ✅ | `test_concurrent_delete_delete_raises_validation_exception` |
| Append + Delete (serializable, fails) | ✅ | `test_concurrent_append_delete_raises_validation_exception` |
| Delete + Append (retry succeeds) | ✅ | `test_concurrent_delete_append_retries_successfully` |
| Overwrite + Overwrite (fails) | ✅ | `test_concurrent_overwrite_overwrite_raises_validation_exception` |
| Overwrite + Append (succeeds) | ✅ | `test_concurrent_overwrite_append_retries_successfully` |
| Snapshot isolation (append+delete OK) | ✅ | `test_snapshot_isolation_allows_concurrent_append_delete` |
| Retry exhaustion | ✅ | `test_retry_exhaustion_raises_commit_failed` |
| Non-snapshot updates preserved | ✅ | `test_rebuild_snapshot_updates_preserves_non_snapshot_updates` |
| Manifest tracking | ✅ | `test_uncommitted_manifests_tracked_correctly` |
| Partition-scoped deletes (no conflict) | ✅ | `test_concurrent_deletes_on_different_partitions_succeed` |
| Partial deletes (CoW, different partitions) | ✅ | `test_concurrent_partial_deletes_on_different_partitions_succeed` |
| Update isolation level routing | ✅ | `test_overwrite_uses_update_isolation_level` |
| Missing parent snapshot validation | ✅ | `test_validate_concurrency_raises_on_missing_parent_snapshot` |
| Missing starting snapshot validation | ✅ | `test_validate_concurrency_raises_on_missing_starting_snapshot` |
| `_refresh_for_retry` state reset | ✅ | `test_refresh_for_retry_resets_producer_state` |
| `_compute_deletes` cache clearing | ✅ | `test_delete_files_refresh_clears_compute_deletes_cache` |
| Cleanup on validation abort | ✅ | `test_clean_all_uncommitted_on_validation_exception` |
| Multi-producer starting snapshot | ✅ | `test_mixed_delete_overwrite_starts_from_catalog_snapshot` |
| Total timeout exceeded | ❌ | Not tested |
| Multiple retries (not just one) | ❌ | Not tested |
| Retry with max_wait_ms cap | ❌ | Not tested |
| Branch (non-main) retry | ❌ | Not tested |
| `_OverwriteFiles._refresh_for_retry()` | ❌ | Not tested (`_OverwriteFiles` doesn't override but should it?) |

**Missing test gaps** are non-critical but worth noting. The total timeout test would need `time.monotonic` mocking.

---

## 9. Specific Technical Deficits

### 9.1 Manifest LIST file leakage

Java tracks manifest list files separately (`manifestLists`) and deletes unused ones after commit:
```java
for (String manifestList : manifestLists) {
    if (!saved.manifestListLocation().equals(manifestList)) {
        deleteFile(manifestList);
    }
}
```

The Python PR tracks and cleans individual manifest files (`_written_manifests`, `_uncommitted_manifests`) but does NOT clean up manifest LIST files from failed attempts. Each retry creates a new manifest list file (via `_commit()` → `write_manifest_list()`), and only the last successful one is referenced. The others leak.

**TODO:** Track manifest list file paths and clean up unused ones on success.

### 9.2 `_OverwriteFiles` doesn't override `_refresh_for_retry()`

`_DeleteFiles` overrides `_refresh_for_retry()` to clear `_compute_deletes` cache. But `_OverwriteFiles` doesn't override it, even though it has state that depends on the parent snapshot (its `_existing_manifests()` reads from the branch tip).

Looking at `_OverwriteFiles._existing_manifests()`:
```python
if snapshot := self._transaction.table_metadata.snapshot_by_name(name=self._target_branch):
    for manifest_file in snapshot.manifests(io=self._io):
```

After `_refresh_for_retry()`, `_parent_snapshot_id` is updated but the transaction metadata is also refreshed (via `self._table.refresh()` in the retry loop). So `snapshot_by_name` will return the new tip. This means `_existing_manifests()` will correctly read from the refreshed state. **Not a bug.**

But `_OverwriteFiles._deleted_entries()` uses `self._parent_snapshot_id`:
```python
if self._parent_snapshot_id is not None:
    previous_snapshot = self._transaction.table_metadata.snapshot_by_id(self._parent_snapshot_id)
```

After `_refresh_for_retry()`, this is updated to the new tip. The deleted data files (`self._deleted_data_files`) are from the ORIGINAL commit. The question is: do the same data files still exist in the new parent? If a concurrent commit deleted some of them, this would try to re-delete them, which would fail to find them in manifests.

**This is a subtle concern** — on retry, `_OverwriteFiles` re-scans manifests looking for `self._deleted_data_files`, but those files might have been deleted by the concurrent commit. The scan would fail to find them and produce incorrect manifests.

**Assessment:** This is an edge case that would only trigger if an overwrite retries after a concurrent operation deleted the same files. In practice, this would be caught by `_validate_concurrency()` which calls `_validate_deleted_data_files()` and would raise `ValidationException` before reaching `_commit()`. So the validation catches it. **Not a bug in practice, but architecturally fragile.**

### 9.3 No `CommitStateUnknownException` handling

If the REST catalog returns an ambiguous response (e.g., HTTP timeout), the Python code currently either succeeds or raises some HTTP error (not `CommitFailedException`). If it raises a non-`CommitFailedException` error, the retry won't trigger and the outer `except Exception:` will clean up manifests. This is safe.

But if the implementation ever raises `CommitFailedException` for an ambiguous state (which it shouldn't — that class implies definite failure), the retry could double-commit. This is a future risk, not a current one.

**TODO:** Consider adding `CommitStateUnknownException` for parity.

---

## 10. Comparison with Java: What's Missing vs What's Extra

### Python has that Java core doesn't:
- **Auto-validation based on table properties** — Java requires explicit `.validateNoConflictingData()` calls from the engine layer. Python auto-detects conflicts based on isolation level. This is BETTER for a user-facing library.
- **Jitter in backoff** — Java's `Tasks.exponentialBackoff()` doesn't add jitter by default (though the total timeout acts as a ceiling). Python adds 0-25% jitter. This is a best practice.

### Java has that Python PR doesn't:
- **Manifest list file cleanup** (see 9.1)
- **Commit metrics/timing** — Java tracks `commitMetrics().attempts()`, `commitMetrics().totalDuration()`
- **`CommitStateUnknownException`** handling
- **Per-retry `apply()` re-execution** — Java recomputes manifests on each retry. Python reuses the same manifest content (just reassigning snapshot IDs/UUIDs). This means Python won't pick up new manifest merge opportunities on retry. Acceptable for now.
- **`write.merge.isolation-level`** — N/A for PyIceberg
- **`cleanupAfterCommit()` override point** — Java allows subclasses to opt out of post-commit cleanup
- **`strictCleanup` flag** — controls whether cleanup happens on non-`CleanableFailure` exceptions

---

## 11. Verdict and Recommendations

### Overall Assessment: **LGTM**

The PR is well-designed, thoroughly tested, and achieves its goal of adding optimistic concurrency control to PyIceberg. The architecture choice of auto-validation via table properties is appropriate for a direct-user library. The retry logic correctly implements exponential backoff with jitter and total timeout. The multi-producer retry path (which was the biggest concern) is correct due to the computed `table_metadata` property reflecting staged updates in real-time.

### Must Fix Before Merge:

No critical blocking issues found. The multi-producer scenario (Issue 3.4) was analyzed in depth and found to be correct — `Transaction.table_metadata` is a computed property that reflects staged updates, so sequential producer processing in the retry loop correctly chains snapshot parent IDs.

### Should Fix:

1. **Issue 9.1:** Add manifest list file cleanup tracking. Add `_written_manifest_lists: list[str]` and clean up unused ones on success.
2. **Issue 4.1:** Add class-level declaration for `_isolation_level_property`.
3. **Issue 4.3:** Call `super().commit()` instead of duplicating the base logic.
4. **Issue 5.1:** Move the `DataScan` formatting change to a separate commit.
5. **Nit 5.4/5.5:** Improve type annotations from `Any` to `_SnapshotProducer`.

### Should Document (in PR description or as TODOs):

6. This auto-validates based on table properties (differs from Java where engine sets validation).
7. Manifest list cleanup is missing (TODO for follow-up).
8. `CommitStateUnknownException` not handled (TODO for follow-up).
9. `write.merge.isolation-level` not supported (N/A for PyIceberg).
10. The `_case_sensitive` parameter change in `_build_delete_files_partition_predicate` is a behavior change.

### Integration Test Changes Are Correct:

The update from `CommitFailedException` to `ValidationException` in `test_optimistic_concurrency.py` is the RIGHT fix — these tests were previously testing the "dumb" behavior (fail on any concurrent change) and now correctly test the "smart" behavior (fail only on actual conflicts, retry on non-conflicts).

---

## 12. Summary Table

| Category | Count |
|----------|-------|
| Critical issues | 0 (initial concern about multi-producer was disproven) |
| Moderate issues | 5 |
| Minor nits | 6 |
| Missing Java parity items | 5 (most are TODOs) |
| Test coverage gaps | 5 (all non-critical) |
| **Overall quality** | **HIGH** — production-ready |

The PR author has done solid work understanding the Java implementation's principles and adapting them for Python's different architectural constraints. The test suite is comprehensive and the documentation additions are clear. The subtle design choice of `table_metadata` as a computed property that reflects staged updates makes the multi-producer retry path correct without needing explicit inter-producer coordination. This is ready to merge.

---

## 13. Post-Review Check (2026-06-21): Nits Still Outstanding

After the PR author pushed their latest changes (manifest list tracking, CommitWindow refactoring, branch fixes), I ran the tests locally — **all pass**. The manifest list fix and branch-aware retry are correctly implemented.

However, three minor nits from the original review remain unaddressed. None are correctness bugs or blockers, but they're worth calling out for a follow-up or quick fix before merge.

---

### Nit A: `_isolation_level_property` missing from class-level declarations

**What it is:**

Every other instance field on `_SnapshotProducer` is declared at the class level (above `__init__`) as a type annotation:

```python
class _SnapshotProducer(UpdateTableMetadata[U], Generic[U]):
    commit_uuid: uuid.UUID
    _io: FileIO
    _operation: Operation
    _snapshot_id: int
    _parent_snapshot_id: int | None
    _starting_snapshot_id: int | None
    _added_data_files: list[DataFile]
    _manifest_num_counter: itertools.count[int]
    _deleted_data_files: set[DataFile]
    _compression: AvroCompressionCodec
    _target_branch: str | None
    _predicate: BooleanExpression
    _case_sensitive: bool
    _commit_window: CommitWindow | None
    _written_manifests: list[str]
    _uncommitted_manifests: list[str]
    _written_manifest_lists: list[str]
    # ← _isolation_level_property is NOT here
```

But `_isolation_level_property` is only set inside `__init__`:
```python
self._isolation_level_property: str = TableProperties.WRITE_DELETE_ISOLATION_LEVEL
```

**Why it matters:**

This is a PyIceberg style convention. Class-level declarations serve as documentation — they tell you "these are all the fields this class uses" without having to read through `__init__`. When one field breaks the pattern, a reader scanning the class-level declarations won't know `_isolation_level_property` exists. They'll only discover it by reading the full `__init__` or finding it used in `_validate_concurrency()`.

It's also relevant for static analysis tools (mypy, Pyright) — class-level annotations are the canonical way to declare instance fields in Python dataclass-style code. Without it, the type is only inferred from the assignment.

**Fix (one line):**

Add to the class-level declarations:
```python
_isolation_level_property: str
```

---

### Nit B: `commit()` should call `super().commit()` instead of duplicating base logic

**What it is:**

The base class `UpdateTableMetadata` defines:
```python
class UpdateTableMetadata:
    def commit(self) -> None:
        self._transaction._apply(*self._commit())
```

The `_SnapshotProducer` subclass overrides it:
```python
class _SnapshotProducer(UpdateTableMetadata[U], Generic[U]):
    def commit(self) -> None:
        self._transaction._register_snapshot_producer(self)
        self._transaction._apply(*self._commit())  # ← duplicated from base
```

**Why it matters:**

This violates the "don't repeat yourself" principle for inheritance. If someone later adds logic to the base `UpdateTableMetadata.commit()` (e.g., logging, validation, metrics), the `_SnapshotProducer` override won't pick it up — because it copies the implementation rather than calling through.

Today this works fine. But 6 months from now, someone adds a commit hook to the base class, wonders why it doesn't fire for snapshot producers, and spends 30 minutes debugging before finding this override.

**Fix:**
```python
def commit(self) -> None:
    self._transaction._register_snapshot_producer(self)
    super().commit()  # ← delegates to base, which calls _apply(*self._commit())
```

Functionally identical today, but future-proof.

---

### Nit C: `list[Any]` typing for `_snapshot_producers` and `_register_snapshot_producer`

**What it is:**

In `Transaction.__init__()`:
```python
self._snapshot_producers: list[Any] = []
```

And:
```python
def _register_snapshot_producer(self, producer: Any) -> None:
    """Register a snapshot producer for retry support."""
    self._snapshot_producers.append(producer)
```

**Why it matters:**

`Any` tells the type checker "I give up, don't check anything about this object." That means:
- If you typo `producer._refresh_for_rety()` (missing `r`), mypy won't catch it.
- If you pass a `str` instead of a producer, mypy won't complain.
- IDE autocompletion on items in `_snapshot_producers` gives you nothing — no suggestions for `._refresh_for_retry()`, `._validate_concurrency()`, etc.

The actual type is `_SnapshotProducer[Any]`. The reason `Any` was used is probably to avoid a circular import — `_SnapshotProducer` is in `pyiceberg.table.update.snapshot` and `Transaction` is in `pyiceberg.table`. But Python has a standard pattern for this:

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyiceberg.table.update.snapshot import _SnapshotProducer
```

The `TYPE_CHECKING` block is only evaluated by type checkers (mypy/Pyright) and IDEs, never at runtime. So it doesn't cause circular import issues at runtime, but gives you full type safety and autocompletion during development.

**Fix:**

In `pyiceberg/table/__init__.py`, add to the existing `TYPE_CHECKING` block:
```python
if TYPE_CHECKING:
    from pyiceberg.table.update.snapshot import _SnapshotProducer
```

Then change:
```python
self._snapshot_producers: list[_SnapshotProducer[Any]] = []

def _register_snapshot_producer(self, producer: _SnapshotProducer[Any]) -> None:
```

---

### Summary: Nit Status Table

| Nit | Original Section | Status After Latest Push | Severity |
|-----|-----------------|--------------------------|----------|
| `_isolation_level_property` not at class level | §4.1 | ❌ Still present | Low (style) |
| `commit()` doesn't call `super()` | §4.3 | ❌ Still present | Low (maintenance risk) |
| `list[Any]` typing | §5.4 / §5.5 | ❌ Still present | Low (type safety) |
| `DataScan` formatting change | §5.1 | ✅ No longer applicable (upstream refactored) | — |
| `_case_sensitive` behavior change | §5.2 | ⚠️ Still present, correct but undocumented | Very low |

**Recommendation:** Mention these as "nits for follow-up" in the review comment. None are blocking. The first three are one-liner fixes that could be addressed in a quick follow-up commit or squashed in before merge if the author is willing.
