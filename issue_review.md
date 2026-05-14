## 1. Context & The Core Issue

In PR #3124, the objective was to implement a mechanism for table data compaction. The approach taken was to introduce a new `replace` method to the PyIceberg `Table` API:

```python
def replace(self, df: pa.Table, files_to_delete: Iterable[DataFile], snapshot_properties: dict[str, str] = EMPTY_DICT, branch: str | None = MAIN_BRANCH) -> None:
```

This method intercepts a pyarrow `Table` containing the compacted data, creates new `DataFile`s from it, effectively deletes the `files_to_delete` references, and commits the result as a `REPLACE` snapshot (via `_RewriteFiles`).

Reviewer @kevinjqliu raises two primary concerns:
1. **Separation of Concerns:** The implementation of the `replace` API itself, and the actual utility for "table compaction" using it, should be separated into two distinct Pull Requests for easier review and rigor.
2. **Data Equivalence Guarantee:** Iceberg semantics heavily rely on the `REPLACE` snapshot operation guaranteeing that **the underlying table data has not logically changed**. It exists merely for restructuring (e.g., bin-packing, sorting). By exposing a core API that accepts a `pa.Table`, PyIceberg trusts the caller implicitly. If a user maliciously or accidentally alters the DataFrame rows (adds, removes, or mutates data) and calls `table.replace(df)`, the fundamental "no data change" contract of a `REPLACE` snapshot is violated.

---

## 2. Java Iceberg vs. PyIceberg Deep Dive

### Java Iceberg Implementation
To understand how the reference implementation handles this, we can look at `BaseRewriteFiles.java` and `RewriteFiles.java` in the core Java Iceberg library. Java uses a "Builder" pattern to construct the replacement operation:

```java
table.newRewrite()
     .deleteFile(oldFile1)
     .deleteFile(oldFile2)
     .addFile(newCompactedFile)
     .commit();
```
- **Mechanism:** The Java API does **not** accept raw dataframes or RDDs/Datasets. It strictly processes pre-written `DataFile` objects (and `DeleteFile`s). The Iceberg Java core trusts that the external caller (such as the Spark `RewriteDataFilesAction`) has already run a distributed job to safely read the target data, write it unchanged precisely into the new DataFiles, and is now merely submitting the metadata swap.

### Current PyIceberg PR Approach
- **Signature:** PyIceberg's proposed `replace()` method natively takes a `pa.Table` (DataFrame).
- **Mismatch:** By pushing the actual file-writing logic into the `replace()` table API boundary, PyIceberg treats the `REPLACE` operation as an end-user data manipulation function (like `.append()` or `.overwrite()`). This creates a significant footgun: since users can supply any DataFrame they want, they can easily cause data loss or corruption under the guise of an Operation that explicitly promises down-stream systems (like materialized views) that the data equivalent remains identical.

---

## 3. Architectural Proposal

To resolve this reliably and mimic the robust abstractions of Java Iceberg, we must structurally decouple the "Data Writing/Compaction process" from the "Metadata Snapshot Replace process".

### Proposed Re-Architecture

**1. Fix the Metadata-Level Replace API (`replace`)**
Instead of the `replace(df)` method directly interacting with PyArrow, PyIceberg should expose a lower-level metadata-swap method that mirrors Java's `RewriteFiles` capability while retaining PyIceberg's simpler "Shorthand API" design.
* `def replace(self, files_to_delete: Iterable[DataFile], files_to_add: Iterable[DataFile]) -> None`
* This method expects fully materialized Iceberg `DataFile`s. The table registers the new data files against the deleted ones and commits the `Operation.REPLACE` snapshot. 

**2. Isolate the PyArrow Logic in Maintenance Utilities**
The problem of handling the PyArrow `Table` logic belongs in a higher-level maintenance class (e.g., `MaintenanceTable.compact()`).
* The maintenance utility reads the target `files_to_delete`.
* It does the bin-packing or merging into a `pa.Table` internally.
* It uses `FileIO` to physically write the new `DataFile`s to storage.
* It calls `table.replace(files_to_delete, files_to_add)` to finalize.
* **Result:** The user never directly injects an arbitrary DataFrame into a `REPLACE` operation. The PyIceberg library internally enforces the data equivalence.

---

## 4. Should PyIceberg drop Shorthand methods for Java's Builder Pattern?

**The Builder Pattern in Java:** Java Iceberg deprecated passing huge `Set<DataFile>` batches (like `rewriteFiles(Set deletes, Set adds)`) in favor of standardizing on `.addFile()` and `.deleteFile()`. This allows Java to scale better because you don't need to hold the entire collection in memory; you can stream files into the transaction builder as they are produced by parallel Spark tasks.

**Is it necessary for PyIceberg?**
Currently, PyIceberg `Table` utilizes "Shorthand APIs" (`append`, `overwrite`, `delete`) rather than Builders (like `newAppend().appendFile().commit()`).
* Because Python (and specifically PyArrow/pandas manipulation) operates primarily on single, in-memory instances at this point in PyIceberg's lifecycle, the memory footprint of passing an `Iterable[DataFile]` is trivial compared to the actual dataframe itself.
* The internal `UpdateSnapshot` / `_RewriteFiles` classes in PyIceberg *do* actually use the builder pattern under the hood (`replace_snapshot.delete_data_file(f)` \-\> `replace_snapshot.append_data_file(f)` \-\> commit).

Therefore, PyIceberg does not *need* to expose the Builder pattern to the top-level API users yet. A shorthand `table.replace(files_to_delete, files_to_add)` remains clean, Pythonic, perfectly backwards-compatible with standard PyIceberg API norms, and is highly scalable simply because the backend loops through the provided explicit `Iterable`s using generators.

---

## 5. Scoped Plan of Action

We can cleanly address the reviewer's request by splitting the work into two PRs:

### PR 1: Core `replace` API Addition
1. **Change Signature:** Modify the proposed `table.replace()` method to drop the PyArrow bindings and exclusively accept Iterables of `DataFile`s.
2. **Internal Plumbing:** Back this method up with the `_RewriteFiles` snapshot producer (which is already implemented correctly in the PR).
3. **Testing:** Write catalog-level tests ensuring that providing mock `DataFile`s correctly commits an `Operation.REPLACE` snapshot.

### PR 2: Compaction Implementation
1. **Implement Private File Writer:** In the `MaintenanceTable.compact()` routine, write an internal function that securely scans the `files_to_delete`, structures the PyArrow `Table`, and dumps it to object storage as new Parquet files (`files_to_add`).
2. **Commit via API:** Call `table.replace(files_to_delete, files_to_add)`.
3. **Validation:** In the future, if stricter mechanisms are required, PyIceberg could checksum row counts between `files_to_delete` and `files_to_add`, but keeping the dataframe abstraction out of the Table API implicitly protects naive users.
