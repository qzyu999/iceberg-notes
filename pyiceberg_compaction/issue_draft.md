# Feature: Add metadata-only replace API to Table for REPLACE snapshot operations

## Description
This issue proposes implementing a metadata-only `replace` API in PyIceberg, enabling orchestrators to submit a set of `DataFile`s to delete and a set of `DataFile`s to append in a single atomic transaction. 

This functionality is critical for maintenance operations such as data compaction (the "small files" problem), format conversion, and relocating data files, ensuring the logical state of the table remains unaltered while physical data layout is optimized.

### Background
Currently, PyIceberg's `replace` semantics are tightly coupled with PyArrow dataframes (`def replace(self, df: pa.Table)`). This approach introduces several architectural flaws:
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

### Acceptance Criteria
- [ ] `replace()` API implemented on both `Table` and `Transaction` using `Iterable[DataFile]`.
- [ ] PyArrow `.parquet` write logic decoupled from the metadata transaction.
- [ ] `_RewriteFiles` correctly copies ancestral `sequence_number` pointers for `DELETED` and `EXISTING` manifest entries.
- [ ] Snapshots committed via the `replace()` hook possess a Summary containing `operation=Operation.REPLACE`.
- [ ] Unit tests pass simulating data file swaps and summary verifications.

### Related Java API
Inspired heavily by Java's builder interface: https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/RewriteFiles.java
