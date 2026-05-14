# Feature: Add metadata-only replace API to Table for REPLACE snapshot operations

**State:** open
**Created by:** @qzyu999
**Created at:** 2026-03-09 22:15:11.000 UTC

### Feature Request / Improvement

## Description
This issue proposes implementing a metadata-only `replace` API in PyIceberg, enabling orchestrators to submit a set of `DataFile`s to delete and a set of `DataFile`s to append in a single atomic transaction. 

This functionality is critical for maintenance operations such as data compaction (the "small files" problem), ensuring the logical state of the table remains unaltered while physical data layout is optimized.

### Background
In a current PR (#3124, part of #1092), PyIceberg's `replace` semantics are tightly coupled with PyArrow dataframes (`def replace(self, df: pa.Table)`). This approach introduces several architectural flaws:
1. **Coupling Physical Serialization with Metadata:** It forces a `.parquet` write serialization hook directly into the snapshot commit transaction, increasing the risk of schema degradation and blocking network topologies.
2. **Missing `Operation.REPLACE`:** The current system uses primitives that log as `APPEND` or `OVERWRITE`, muddying the table history and complicating snapshot expiry/maintenance.
3. **Java Inconsistency:** This severely drifts from Java Iceberg's native `org.apache.iceberg.RewriteFiles` specification, which strictly isolates the builder into accepting purely `DataFile` pointers.

### Proposed Solution
To fix this and achieve logical equivalence, we must implement an exact port of Java's `RewriteFiles` builder API into PyIceberg's native `_SnapshotProducer` engine.

1. **Introduce `_RewriteFiles` Snapshot Producer:** 
   Add a new `_RewriteFiles` class that specifically targets replacing existing files. This class will implement:
   - `_deleted_entries()`: To find the existing target files and re-emit them as `DELETED` entries, defensively keeping their ancestral `sequence_number`s completely intact for time travel compatibility.
   - `_existing_manifests()`: To scavenge unchanged manifests natively, skipping deep rewrites and only mutating manifests impacted by the deleted files.

2. **Builder Hook Implementation:**
   Implement `UpdateSnapshot().replace()` which configures the transaction with `Operation.REPLACE`.

3. **Expose Shorthands on Table & Transaction:**
   Add `replace` APIs on both `Table` and `Transaction` taking `Iterable[DataFile]` arguments to elegantly wrap the snapshot mutation:
   ```python
   def replace(
       self,
       files_to_delete: Iterable[DataFile],
       files_to_add: Iterable[DataFile],
       snapshot_properties: dict[str, str] = EMPTY_DICT,
       branch: str | None = MAIN_BRANCH,
   ) -> None:
       ...
   ```

### Notable canges
- `replace()` API implemented on both `Table` and `Transaction` using `Iterable[DataFile]`.
- PyArrow `.parquet` write logic decoupled from the metadata transaction.
- `_RewriteFiles` correctly copies ancestral `sequence_number` pointers for `DELETED` and `EXISTING` manifest entries.
- Snapshots committed via the `replace()` hook possess a Summary containing `operation=Operation.REPLACE`.
- Unit tests pass simulating data file swaps and summary verifications.

### Related Java API
Inspired heavily by Java's builder interface: https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/RewriteFiles.java

### AI Disclosure
AI was used to help understand the code base and draft code changes. All code changes have been thoroughly reviewed, ensuring that the code changes are in line with a broader understanding of the codebase.

- Worth deeper review after AI-assistance:
- The `test_invalid_operation()` in `tests/table/test_snapshots.py` previously used `Operation.REPLACE` as a value to test invalid operations, but with this change `Operation.REPLACE` becomes valid. In place I just put a dummy Operation.
- The `_RewriteFiles` in `pyiceberg/table/update/snapshot.py` overrides the `_deleted_entries` and `_existing_manifests` functions. I sought to test this thoroughly that it was done correctly. I am thinking it's possible to improve the test suite to make this more rigorous. I am open to suggestions on how that could be done.


