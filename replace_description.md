# The REPLACE Operation in Apache Iceberg

## Definition and Core Semantics
In Apache Iceberg, the `REPLACE` data operation is defined with a specific, highly rigorous guarantee: **files are removed and replaced without changing the logical data in the table.**

According to the Java API (`org.apache.iceberg.DataOperations`):
*   `OVERWRITE`: New data is added to overwrite existing data (a logical change to the table's state).
*   `REPLACE`: Files are removed and replaced, without changing the data in the table (a physical change only, implemented by `RewriteFiles`).

## Why the Distinction Matters
Iceberg's architectural guarantees—specifically concurrency control and incremental reading—depend entirely on the rigor of the snapshot operation type.

### 1. Incremental Reads (Time Travel & Streaming)
When a downstream consumer reads an Iceberg table incrementally to process new changes, it analyzes the snapshot history.
*   If a snapshot is marked as `REPLACE`, the reader knows that the underlying files were strictly restructured (e.g., compacted from 10 small files to 1 large file) but no new logical records were inserted, updated, or deleted. The reader can safely **ignore** this snapshot.
*   If you mistakenly use `OVERWRITE` for a compaction job, downstream consumers will incorrectly perceive the compacted files as *new* data, leading to duplicate processing.

### 2. Conflict Resolution
During optimistic concurrency control, Iceberg uses the operation type to determine if two concurrent commits conflict.
*   An `OVERWRITE` or `DELETE` might violently conflict with another concurrent operation modifying the same partition.
*   Because `REPLACE` strictly promises no logical changes, Iceberg's commit protocol can often safely re-apply a `REPLACE` operation alongside other concurrent data modifications (provided the specific files being replaced haven't been deleted).

## Context: The PyIceberg Feedback
In the provided code snippet:
```python
# Overwrite the table atomically (REPLACE operation)
with self.tbl.transaction() as txn:
    txn.overwrite(arrow_table, snapshot_properties={"snapshot-type": "replace", "replace-operation": "compaction"})
```
While you are attempting to tag the snapshot properties with `"snapshot-type": "replace"`, you are utilizing the `txn.overwrite()` API. In Iceberg's metadata, the fundamental action recorded for this snapshot will be `OVERWRITE`, not `REPLACE`.

### The Required Implementation
To maintain absolute rigor with the Iceberg specification, PyIceberg cannot overload `.overwrite()` for compaction. It must introduce a standalone API (e.g., `txn.replace()` or `txn.rewrite()`) that constructs a snapshot explicitly flagged with the `replace` action. This ensures the engine unequivocally signals to all consumers and transaction managers that the table's logical data remains completely unmodified.
