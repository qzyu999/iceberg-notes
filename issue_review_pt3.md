# Issue Review Part 3: Inspiration from the Java Iceberg Implementation

The modifications proposed for the PyIceberg `replace()` API are not arbitrary; they are strictly derived from the reference Java Iceberg implementation, which serves as the ultimate source of truth for the Iceberg specification.

This document rigorously maps the Java architecture to the newly proposed PyIceberg architecture, explaining how the Python bindings now safely mirror Java's design patterns for file replacement.

---

## 1. The Core Iceberg Promise: Logical Equivalence 

Both Java's and Python's `replace` semantics are utilized predominately by advanced maintenance processes—such as data compaction pipelines, file format converters, or partitioning optimizers.

In Java Iceberg, the API documentation for `RewriteFiles` strictly mandates:
> *Note that the new state of the table after each rewrite must be logically equivalent to the original table state.*

The original PyIceberg implementation was fundamentally incompatible with this law. By accepting a `df: pa.Table` and passing it through PyArrow serialization, PyIceberg injected computational data-writing directly into the table metadata commit process. This created an unacceptable risk of data skew, corruption, and schema conversion drift. 

The new PyIceberg implementation guarantees exact logical parity by delegating the physical file writing to *external orchestrators* (like compaction scripts), and strictly only ingesting resulting `Iterable[DataFile]`s for commit—exactly modelling Java.

---

## 2. Comparing the Architectural APIs

### Java Iceberg API: `org.apache.iceberg.RewriteFiles`

In Java, snapshot mutations are performed via the `SnapshotUpdate` Builder pattern. For replacing files, the specification exposes `RewriteFiles.java`.

The Java builder interface exposes pure metadata mutations:
```java
// Java: Add a new data file
default RewriteFiles addFile(DataFile dataFile)

// Java: Remove a data file from the current table state
default RewriteFiles deleteFile(DataFile dataFile)
```

Furthermore, it offers a shorthand to bundle them:
```java
// Java: Add a rewrite that replaces one set of data files with another set
default RewriteFiles rewriteFiles(Set<DataFile> filesToDelete, Set<DataFile> filesToAdd)
```

### New PyIceberg API: `UpdateSnapshot` and `Table`

The new PyIceberg implementation perfectly maps these semantics into Pythonic constructs.

First, we implemented the builder capability on `UpdateSnapshot` by introducing a dedicated `_RewriteFiles` class (equivalent to Java's `RewriteFiles` interface):
```python
# Python: The new Builder Method
def replace(self) -> _RewriteFiles:
    return _RewriteFiles(
        operation=Operation.REPLACE, # Exactly identifying it as a Rewrite Operation
        ...
    )
```

Under the hood, `_RewriteFiles` implements the granular file queuing, analogous to Java's interface:
```python
# Python: _RewriteFiles queues up the specific files natively
replace_snapshot.delete_data_file(file_to_delete) # Maps to deleteFile()
replace_snapshot.append_data_file(data_file)      # Maps to addFile()
```

Finally, we expose the shorthand on the `Table` class, providing the exact ergonomic equivalent to Java's `rewriteFiles()` shorthand:
```python
# Python: Shorthand directly mirroring Java's rewriteFiles()
def replace(
    self,
    files_to_delete: Iterable[DataFile],
    files_to_add: Iterable[DataFile],
    ...
) -> None:
```

---

## 3. Sequence Number and Metadata Hygiene

In Iceberg V2, sequence numbers dictate the time-travel legitimacy of delete files relative to data files. 

When a `DataFile` is logically deleted (replaced) during a rewrite, the new manifest must record it with a status of `DELETED`, but **it must not lose its ancestral sequence number**.

In Java, the `ManifestWriter` handles migrating existing and deleted entries to new clean manifests while retaining these sequence markers. The new PyIceberg implementation of `_RewriteFiles.py` explicitly achieves this by migrating Java's schema protocol into Python:

```python
# PyIceberg duplicating sequence numbers to assure Java Protocol Parity
ManifestEntry.from_args(
    status=ManifestEntryStatus.DELETED,  # or EXISTING
    snapshot_id=entry.snapshot_id,
    sequence_number=entry.sequence_number,            # CRITICAL
    file_sequence_number=entry.file_sequence_number,  # CRITICAL
    data_file=entry.data_file,
)
```

## Summary
The PyIceberg `.replace()` redesign correctly lifts the design invariants from Java's `org.apache.iceberg.RewriteFiles`:

1.  **Separation of Concerns**: Data generation is externalized. The API strictly only accepts standard Iceberg metadata pointers (`DataFile`).
2.  **Builder Parity**: The Python operation leverages `_RewriteFiles`, mapping exactly to Java's builder queuing pattern.
3.  **Operation Integrity**: The new code ensures the commit registers strictly as `Operation.REPLACE`, adhering to Java `SnapshotSummary` conventions.
4.  **Metadata Preservation**: It rigorously replicates `ManifestEntry` sequence numbers, guaranteeing full Iceberg V2 Time-Travel compliance across languages.
