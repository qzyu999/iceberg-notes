# PR #3320 Review Comment: Manifest List File Leakage on Retry

## The Problem

Manifest **files** (individual `.avro` files listing data files) are correctly tracked and cleaned up on retry. But manifest **list** files (`snap-{id}-0-{uuid}.avro`, which reference the manifest files) are written to storage without being tracked — so when a retry happens, the old manifest list file becomes an orphan that is never deleted.

---

## Full Flow: A Commit That Fails Once Then Succeeds

### Setup

User calls `tbl.append(df)`. Under the hood, a `_FastAppendFiles` producer is created, its `commit()` stages updates, and eventually `Transaction.commit_transaction()` is called.

---

### STEP 1: First attempt — build the snapshot

The producer's `_commit()` is called (either directly from `commit()` on the first attempt, or from `_rebuild_snapshot_updates()` on retry). It first builds manifests:

**`snapshot.py` line 277-278 — entry point:**
```python
def _commit(self) -> UpdatesAndRequirements:
    new_manifests = self._manifests()          # ← Calls new_manifest_output() internally
```

Inside `_manifests()`, individual manifest files are created via `new_manifest_output()`:

**`snapshot.py` line 363-368 — manifest files are written and TRACKED:**
```python
def new_manifest_output(self) -> OutputFile:
    location_provider = self._transaction._table.location_provider()
    file_name = _new_manifest_file_name(num=..., commit_uuid=self.commit_uuid)
    file_path = location_provider.new_metadata_location(file_name)
    self._written_manifests.append(file_path)   # ← TRACKED ✅
    return self._io.new_output(file_path)
```

After `_manifests()` returns, we have:
```
_written_manifests = ["s3://bucket/metadata/{uuid1}-m0.avro"]
```

Now back in `_commit()`, it creates the manifest LIST file:

**`snapshot.py` line 282-298 — manifest list is written but NOT tracked:**
```python
    file_name = _new_manifest_list_file_name(
        snapshot_id=self._snapshot_id,       # e.g. 123
        attempt=0,
        commit_uuid=self.commit_uuid,        # e.g. uuid1
    )
    location_provider = self._transaction._table.location_provider()
    manifest_list_file_path = location_provider.new_metadata_location(file_name)
    # ↑ Creates path "s3://bucket/metadata/snap-123-0-{uuid1}.avro"
    # ↑ This path is NEVER appended to _written_manifests or any other list ❌

    with write_manifest_list(
        format_version=self._transaction.table_metadata.format_version,
        output_file=self._io.new_output(manifest_list_file_path),  # ← Written to storage!
        snapshot_id=self._snapshot_id,
        parent_snapshot_id=self._parent_snapshot_id,
        sequence_number=next_sequence_number,
        avro_compression=self._compression,
    ) as writer:
        writer.add_manifests(new_manifests)
```

**State after Step 1:**
```
In storage:
  s3://bucket/metadata/{uuid1}-m0.avro          ← manifest file
  s3://bucket/metadata/snap-123-0-{uuid1}.avro  ← manifest LIST file

Tracked in Python:
  _written_manifests = ["{uuid1}-m0.avro"]      ← manifest tracked ✅
  (nothing tracks snap-123-0-{uuid1}.avro)      ← manifest list NOT tracked ❌
```

---

### STEP 2: Attempt the catalog commit — it FAILS

**`table/__init__.py` line 1099-1106:**
```python
for attempt in range(num_retries + 1):
    try:
        self._table._do_commit(           # ← Sends updates to catalog (REST/Glue/etc)
            updates=self._updates,
            requirements=self._requirements,
        )
        self._cleanup_uncommitted_manifests()   # ← Only reached on SUCCESS (not reached here)
        break
```

The catalog responds: "branch main has changed: expected id X, got Y" → raises `CommitFailedException`.

---

### STEP 3: Prepare for retry

**`table/__init__.py` line 1107-1117:**
```python
    except CommitFailedException:
        elapsed_ms = (time.monotonic() - start_time) * 1000
        if attempt == num_retries or not self._snapshot_producers or elapsed_ms >= total_timeout_ms:
            raise                             # ← Give up if exhausted

        wait = min(min_wait_ms * (2**attempt), max_wait_ms)
        jitter = random.uniform(0, 0.25 * wait)
        time.sleep((wait + jitter) / 1000.0)  # ← Exponential backoff with jitter

        self._table.refresh()                 # ← Re-read table metadata from catalog
        self._rebuild_snapshot_updates()      # ← Rebuild all snapshot producers for retry
```

`_rebuild_snapshot_updates()` calls `_refresh_for_retry()` on each producer:

**`snapshot.py` line 396-405 — resets producer state for a fresh attempt:**
```python
def _refresh_for_retry(self) -> None:
    self._uncommitted_manifests.extend(self._written_manifests)
    #   ↑ Moves manifests to "uncommitted" list for later cleanup ✅
    #   _uncommitted_manifests is now ["{uuid1}-m0.avro"]
    
    self._written_manifests.clear()
    #   ↑ Ready for the next attempt's manifests
    
    self._parent_snapshot_id = (
        snapshot.snapshot_id if (snapshot := self._transaction.table_metadata.snapshot_by_name(self._target_branch)) else None
    )   # ← Points to new branch tip after refresh
    
    self._snapshot_id = self._transaction.table_metadata.new_snapshot_id()   # ← e.g. 456
    self._manifest_num_counter = itertools.count(0)
    self.commit_uuid = uuid.uuid4()   # ← e.g. uuid2
```

**State after Step 3:**
```
In storage (unchanged):
  s3://bucket/metadata/{uuid1}-m0.avro          ← still exists
  s3://bucket/metadata/snap-123-0-{uuid1}.avro  ← still exists

Tracked in Python:
  _uncommitted_manifests = ["{uuid1}-m0.avro"]  ← queued for cleanup ✅
  _written_manifests = []                       ← cleared for new attempt

  snap-123-0-{uuid1}.avro                       ← NO REFERENCE TO IT ANYWHERE ❌
                                                   It was never in _written_manifests,
                                                   so it was never moved to _uncommitted_manifests.
                                                   It's forgotten.
```

---

### STEP 4: Second attempt — `_commit()` runs again with new IDs

Same as Step 1 but with `uuid2` and `snapshot_id=456`:

```
New files written to storage:
  s3://bucket/metadata/{uuid2}-m0.avro          ← new manifest
  s3://bucket/metadata/snap-456-0-{uuid2}.avro  ← new manifest list

Tracked:
  _written_manifests = ["{uuid2}-m0.avro"]      ← tracked ✅
  snap-456-0-{uuid2}.avro                       ← not tracked (same gap, but this one will be committed so it's fine)
```

---

### STEP 5: Second attempt SUCCEEDS

**`table/__init__.py` line 1100-1106:**
```python
        self._table._do_commit(...)          # ← Succeeds this time!
        self._cleanup_uncommitted_manifests() # ← Now called!
        break
```

**`table/__init__.py` line 1128-1131 — calls each producer's cleanup:**
```python
def _cleanup_uncommitted_manifests(self) -> None:
    """Clean up manifests from failed retry attempts after a successful commit."""
    for producer in self._snapshot_producers:
        producer._cleanup_uncommitted()
```

**`snapshot.py` line 377-384 — deletes the OLD manifest files:**
```python
def _cleanup_uncommitted(self) -> None:
    """Delete manifest files from failed retry attempts."""
    for path in self._uncommitted_manifests:        # ← Contains ["{uuid1}-m0.avro"]
        try:
            self._io.delete(path)                    # ← Deletes {uuid1}-m0.avro ✅
        except Exception:
            logger.warning("Failed to delete uncommitted manifest: %s", path, exc_info=True)
    self._uncommitted_manifests.clear()
```

---

### FINAL STATE

| File | Was Tracked? | Was Cleaned? | Final Status |
|------|-------------|-------------|-------------|
| `{uuid1}-m0.avro` (attempt 1 manifest) | ✅ via `_written_manifests` → `_uncommitted_manifests` | ✅ Deleted in Step 5 | **Gone** (clean) |
| `{uuid2}-m0.avro` (attempt 2 manifest) | ✅ in `_written_manifests` | N/A — committed snapshot uses it | **In use** (correct) |
| `snap-123-0-{uuid1}.avro` (attempt 1 manifest LIST) | ❌ Never tracked | ❌ Never deleted | **ORPHAN** 💀 |
| `snap-456-0-{uuid2}.avro` (attempt 2 manifest LIST) | ❌ Never tracked | N/A — committed snapshot references it | **In use** (correct) |

The orphaned `snap-123-0-{uuid1}.avro` will sit in the metadata directory forever. With default settings (4 retries), a contentious table could accumulate up to 4 orphaned manifest list files per commit. Over time with high-concurrency workloads this quietly grows.

---

## The Gap: Why It Happens

**`new_manifest_output()` (line 367)** appends to `_written_manifests`:
```python
self._written_manifests.append(file_path)   # ← manifests tracked
```

**`_commit()` (line 288)** does NOT append `manifest_list_file_path` to anything:
```python
manifest_list_file_path = location_provider.new_metadata_location(file_name)
# ← This is just a local variable. Never saved anywhere for cleanup.
```

So when `_refresh_for_retry()` runs, it moves `_written_manifests` → `_uncommitted_manifests` for later deletion. But the manifest list file was never IN `_written_manifests`, so it never gets moved, and never gets cleaned.

---

## Java Reference

Java handles this with a dedicated tracking list ([SnapshotProducer.java](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/SnapshotProducer.java)):

**Declaration (line 114):**
```java
private final List<String> manifestLists = Lists.newArrayList();
```

**Track every manifest list written (line 322, inside apply()):**
```java
manifestLists.add(manifestList.location());
```

**After successful commit — clean all but the one that was used (line 553-556):**
```java
Snapshot saved = ops.refresh().snapshot(newSnapshotId.get());
for (String manifestList : manifestLists) {
    if (!saved.manifestListLocation().equals(manifestList)) {
        deleteFile(manifestList);
    }
}
```

**On abort — clean everything (line 604-609, cleanAll()):**
```java
protected void cleanAll() {
    for (String manifestList : manifestLists) {
        deleteFile(manifestList);
    }
    manifestLists.clear();
    cleanUncommitted(EMPTY_SET);
}
```

---

## Proposed Fix

### 1. Add a tracking field to `_SnapshotProducer`

```python
# Class-level declaration (add after _uncommitted_manifests):
_written_manifest_lists: list[str]

# In __init__ (add after self._uncommitted_manifests = []):
self._written_manifest_lists = []
```

### 2. Track the manifest list file in `_commit()`

```python
def _commit(self) -> UpdatesAndRequirements:
    ...
    manifest_list_file_path = location_provider.new_metadata_location(file_name)
    self._written_manifest_lists.append(manifest_list_file_path)   # ← ADD THIS
    
    with write_manifest_list(
        ...
        output_file=self._io.new_output(manifest_list_file_path),
        ...
    ) as writer:
        writer.add_manifests(new_manifests)
    ...
```

### 3. Clean up manifest lists after successful commit

```python
def _cleanup_uncommitted(self) -> None:
    """Delete manifest files and manifest lists from failed retry attempts."""
    for path in self._uncommitted_manifests:
        try:
            self._io.delete(path)
        except Exception:
            logger.warning("Failed to delete uncommitted manifest: %s", path, exc_info=True)
    self._uncommitted_manifests.clear()

    # Delete all manifest lists except the last one (which the committed snapshot references)
    if len(self._written_manifest_lists) > 1:
        for path in self._written_manifest_lists[:-1]:
            try:
                self._io.delete(path)
            except Exception:
                logger.warning("Failed to delete uncommitted manifest list: %s", path, exc_info=True)
        self._written_manifest_lists = self._written_manifest_lists[-1:]
```

### 4. Clean up manifest lists on abort

```python
def _clean_all_uncommitted(self) -> None:
    """Clean up all manifests and manifest lists on abort."""
    for path in itertools.chain(self._uncommitted_manifests, self._written_manifests):
        try:
            self._io.delete(path)
        except Exception:
            logger.warning("Failed to delete uncommitted manifest: %s", path, exc_info=True)
    for path in self._written_manifest_lists:
        try:
            self._io.delete(path)
        except Exception:
            logger.warning("Failed to delete uncommitted manifest list: %s", path, exc_info=True)
    self._uncommitted_manifests.clear()
    self._written_manifests.clear()
    self._written_manifest_lists.clear()
```

### 5. No change needed to `_refresh_for_retry()`

Unlike manifests, we don't need to move manifest list paths between lists. We just accumulate all of them in `_written_manifest_lists` across retries and clean all-but-last on success (or clean all on abort).

---

## Test Case

```python
def test_manifest_list_cleanup_on_retry(catalog: Catalog) -> None:
    """Verify that manifest list files from failed retry attempts are cleaned up."""
    catalog.create_namespace("default")
    schema = Schema(NestedField(1, "x", LongType(), required=False))
    catalog.create_table("default.manifest_list_cleanup_test", schema=schema)

    import pyarrow as pa
    from typing import Any
    from unittest.mock import patch

    df = pa.table({"x": [1, 2, 3]})

    tbl = catalog.load_table("default.manifest_list_cleanup_test")
    tbl.append(df)

    # Create two references to simulate concurrency
    tbl1 = catalog.load_table("default.manifest_list_cleanup_test")
    tbl2 = catalog.load_table("default.manifest_list_cleanup_test")

    # tbl1 commits first, causing tbl2's first attempt to fail
    tbl1.append(df)

    # Track all deletes performed during tbl2's commit (which will retry)
    deleted_paths: list[str] = []
    original_delete = tbl2.io.delete

    def tracking_delete(path: str) -> None:
        deleted_paths.append(path)
        original_delete(path)

    with patch.object(tbl2.io, "delete", side_effect=tracking_delete):
        tbl2.append(df)

    # Verify that manifest list files from the failed attempt were cleaned up
    manifest_list_deletes = [p for p in deleted_paths if "snap-" in p and p.endswith(".avro")]
    assert len(manifest_list_deletes) >= 1, (
        f"Expected at least one orphaned manifest list to be cleaned up, "
        f"but only these paths were deleted: {deleted_paths}"
    )


def test_manifest_list_cleanup_on_abort(catalog: Catalog) -> None:
    """Verify that all manifest lists are cleaned up when commit aborts with ValidationException."""
    catalog.create_namespace("default")
    schema = Schema(NestedField(1, "x", LongType(), required=False))
    catalog.create_table("default.manifest_list_abort_test", schema=schema)

    import pyarrow as pa
    from typing import Any
    from unittest.mock import patch

    from pyiceberg.exceptions import ValidationException

    df = pa.table({"x": [1, 2, 3]})

    tbl = catalog.load_table("default.manifest_list_abort_test")
    tbl.append(df)

    tbl1 = catalog.load_table("default.manifest_list_abort_test")
    tbl2 = catalog.load_table("default.manifest_list_abort_test")

    # tbl1 deletes, which will cause tbl2's delete to fail validation (conflicting deletes)
    tbl1.delete("x == 1")

    # Track all deletes
    deleted_paths: list[str] = []
    original_delete = tbl2.io.delete

    def tracking_delete(path: str) -> None:
        deleted_paths.append(path)
        original_delete(path)

    with patch.object(tbl2.io, "delete", side_effect=tracking_delete):
        with pytest.raises(ValidationException):
            tbl2.delete("x == 1")

    # On abort, ALL manifest lists should be cleaned (not just old ones)
    manifest_list_deletes = [p for p in deleted_paths if "snap-" in p and p.endswith(".avro")]
    assert len(manifest_list_deletes) >= 1, (
        f"Expected manifest list cleanup on abort, "
        f"but only these paths were deleted: {deleted_paths}"
    )
```

---

## Summary

| What | Tracked today? | Cleaned today? | After fix |
|------|---------------|---------------|-----------|
| Manifest files (`{uuid}-m0.avro`) | ✅ `_written_manifests` | ✅ via `_uncommitted_manifests` | No change needed |
| Manifest list files (`snap-{id}-0-{uuid}.avro`) | ❌ | ❌ Orphaned | ✅ via `_written_manifest_lists` |
