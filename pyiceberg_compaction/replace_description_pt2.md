# PyIceberg vs Java Iceberg Transaction APIs

To implement a clean `.replace()` (or `.rewrite()`) API in PyIceberg for compaction—one that respects the `REPLACE` operation semantics exactly—it is crucial to understand how transactions are modeled in both the Java reference implementation and PyIceberg.

## 1. The Java Iceberg Implementation (`org.apache.iceberg.Transaction`)

In Java, an Iceberg `Table` and a `Transaction` are fundamentally interfaces that act as factories for **Snapshot Update APIs**. 

When you want to modify table data, you don't directly manipulate the transaction buffer. Instead, you call a specific builder method on the `Transaction` (or `Table`) interface:

*   `txn.newAppend()` $\rightarrow$ Returns an `AppendFiles` builder.
*   `txn.newRewrite()` $\rightarrow$ Returns a `RewriteFiles` builder.
*   `txn.newOverwrite()` $\rightarrow$ Returns an `OverwriteFiles` builder.

### The `RewriteFiles` API
The `RewriteFiles` interface is designed *specifically* for `REPLACE` operations:
```java
public interface RewriteFiles extends SnapshotUpdate<RewriteFiles> {
    RewriteFiles rewriteFiles(Set<DataFile> filesToDelete, Set<DataFile> filesToAdd);
    // ...
}
```
Internally, the `RewriteFiles` implementation implicitly guarantees that the operation type generated in the manifest list will be explicitly set to `DataOperations.REPLACE`. Because the API is typed and specific, the caller does not (and cannot) try to "fake" an overwrite as a replace.

## 2. The PyIceberg Implementation (`pyiceberg.table.Transaction`)

PyIceberg follows a very similar architectural pattern. The `Transaction` object (in `pyiceberg.table.__init__.py`) encapsulates staging `TableUpdate`s and `TableRequirement`s.

Just like Java, PyIceberg relies on specific internal **Snapshot Producers** (found in `pyiceberg/table/update/snapshot.py`) to handle the different operation types:
*   `_FastAppendFiles` (produces `APPEND` snapshots)
*   `_OverwriteFiles` (produces `OVERWRITE` snapshots)
*   `_DeleteFiles` (produces `DELETE` snapshots)

Currently, when you call `txn.overwrite()` (or `table.overwrite()`), PyIceberg dynamically instantiates an `_OverwriteFiles` snapshot producer:
```python
# From PyIceberg Table/Transaction overwrite()
with self.update_snapshot(snapshot_properties=snapshot_properties).overwrite() as overwrite_snapshot:
    # overwrite_snapshot is an instance of _OverwriteFiles
    # ...
```
By definition, `_OverwriteFiles` hardcodes its underlying operation as `OVERWRITE`. Passing `snapshot_properties={"snapshot-type": "replace"}` does not change the core engine-level operation logged in the table metadata—it merely adds a cosmetic tag.

## 3. How to Bridge the Gap for Compaction

To properly implement the reviewer's feedback and match the Java implementation, PyIceberg needs its own equivalent of Java's `RewriteFiles`.

**Step A: Create the Snapshot Producer (`pyiceberg/table/update/snapshot.py`)**
We need a new class, e.g., `_RewriteFiles(SnapshotProducer)`, parallel to `_OverwriteFiles`. The key distinction is that `_RewriteFiles` will explicitly map to the `DataOperation.REPLACE` string rather than `OVERWRITE`.

It will expose methods to define the input and output bounds of the compaction:
```python
class _RewriteFiles(_SnapshotProducer["_RewriteFiles"]):
    def rewrite_files(self, files_to_delete: Iterable[DataFile], files_to_add: Iterable[DataFile]) -> None:
        # Add to local tracking sets
        for f in files_to_delete:
            self.delete_data_file(f)
        for f in files_to_add:
            self.append_data_file(f)
```

**Step B: Expose `.rewrite()` on `UpdateSnapshot`**
Update the factory class `UpdateSnapshot` to instance this new producer:
```python
def rewrite(self, commit_uuid: uuid.UUID | None = None) -> _RewriteFiles:
    return _RewriteFiles(
        # ... standard parameters
    )
```

**Step C: Expose `.rewrite()` (or `.replace()`) on `Transaction` and `Table`**
Finally, add a method to `pyiceberg.table.Transaction` (and mirror it on `Table`) that allows users to leverage this natively, passing in the files to logically delete and the new, compacted DataFrame or DataFiles to append. 

```python
def rewrite_files(self, files_to_delete: Iterable[DataFile], df: pa.Table, snapshot_properties=...) -> None:
    # 1. Convert df to new DataFiles
    # 2. Open the snapshot producer
    with self.update_snapshot(snapshot_properties=snapshot_properties).rewrite() as rewrite_snapshot:
        rewrite_snapshot.rewrite_files(files_to_delete, new_data_files)
```

### Conclusion
By making these structural additions, PyIceberg will naturally mirror Java Iceberg's `Transaction.newRewrite()`. The resulting snapshot will mathematically prove to downstream readers that data has merely been rearranged, fully resolving the core requirement of the `REPLACE` specification.
