<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# The Apache Iceberg Master Specification & Implementation Matrix

This document provides a comprehensive, bird's-eye view analysis of the Apache Iceberg ecosystem. It connects the structural dots that make Iceberg both highly performant and architecturally complex, traces the evolution of features across Table Specifications (**V1, V2, V3, and V4**), and outlines the exact implementation status, engineering trade-offs, and strategic rationales comparing **Java (`/iceberg/`)** and **PyIceberg (`/iceberg-python/`)**.

---

## 1. Demystifying the Complexity: How the Dots Connect

To understand why Iceberg is designed the way it is, one must view it not as a file format, but as a **layered state machine** built over immutable object storage. Iceberg solves three fundamental database problems on data lakes:
1. **Schema Evolution without Side Effects**
2. **Partition Evolution without Data Rewriting**
3. **Row-Level Mutations (Updates and Deletes) without Massive Write Amplification**

These problems are solved by decoupling logical structure from physical data. The following diagram illustrates how these layers connect at runtime:

```
                  +-----------------------------------+
                  |        Catalog / Pointer          |
                  | (Holds current table-metadata.json) |
                  +-----------------+-----------------+
                                    |
                                    v
                  +-----------------+-----------------+
                  |       Table Metadata Log          |
                  |  (List of active Snapshot IDs)    |
                  +-----------------+-----------------+
                                    |
                                    v
                  +-----------------+-----------------+
                  |        Manifest List File         |
                  |  (Active manifest files + stats)  |
                  +-----------------+-----------------+
                                    |
                                    v
                  +-----------------+-----------------+
                  |         Manifest File             |
                  | (Lists Data/Delete files + stats) |
                  +-----------------+-----------------+
                                    |
            +-----------------------+-----------------------+
            |                                               |
            v                                               v
+-----------+-----------+                       +-----------+-----------+
|    Physical Data      |                       |    Physical Delete    |
| (Immutable Parquet)   |                       |  (Pos/Eq Logs or DVs) |
+-----------------------+                       +-----------------------+
```

### The Architectural Anchors of Complexity

*   **Unique Field IDs (Schema Layer)**: Instead of mapping query columns by string names (which break if a user renames, drops, or reorders fields), Iceberg assigns a unique, immutable integer ID (Field ID) to every column. Schema evolution is simply a metadata mapping of Field IDs; physical data files never need to be rewritten.
*   **Partition Specs & Hidden Partitioning (Partition Layer)**: Query engines do not partition by directory names like Hive. Iceberg stores partition transforms (such as `day(timestamp_col)` or `bucket(16, id)`) in a Partition Spec. When partition strategies change (Partition Evolution), the table metadata increments the Spec ID. Historical data remains under the old Spec ID, and new data is written under the new Spec ID. The client automatically merges both specs during query scans.
*   **Sequence Numbers & File Inheritance (State Layer)**: In Merge-on-Read (MoR) tables, delete files must only apply to data files that were written *before* the delete occurred. Iceberg tracks this by assigning a monotonically increasing Sequence Number to every snapshot. When a delete file is written, it is assigned a sequence number $S_{\text{delete}}$. A query engine will only apply this delete to data files whose sequence number $S_{\text{data}} < S_{\text{delete}}$. This prevents deleting concurrently appended data.

---

## 2. Table Specifications Matrix (V1 vs. V2 vs. V3 vs. V4)

The Table Specification defines the capabilities and constraints of the metadata layer. The matrix below traces the exact physical and metadata layout requirements across versions:

| Feature Area | V1 (Format Version 1) | V2 (Format Version 2) | V3 (Format Version 3) | V4 (Future / Under Draft) |
| :--- | :--- | :--- | :--- | :--- |
| **Primary Focus** | Analytical Copy-on-Write (CoW) tables. | Row-Level Mutations using Merge-on-Read (MoR). | Storage Optimization, Cryptographic Encryption, and Deletion Vectors. | Views integration, multi-table transactions, and advanced indexing. |
| **Row-Level Deletes** | **Bypassed**: Only supports CoW (entire data file must be rewritten). | **Supported**: Equality Delete and Positional Delete Parquet files. | **Supported**: Deletion Vectors (compressed Roaring Bitmaps stored in Puffin). | **Supported**: Further optimization of Deletion Vectors and indexes. |
| **Sequence Numbers** | **Absent**: Files do not have transaction sequence numbers. | **Required**: Added to manifest entries to order appends and deletes. | **Required**: Used for advanced metadata tracking. | **Required**: State synchronization across multi-table views. |
| **Puffin Blob Storage** | **Bypassed**: Metadata is strictly JSON and Parquet. | **Bypassed**: Metadata remains unchanged. | **Required**: Used to store serialized roaring bitmaps and statistics. | **Required**: Enhanced statistics and indexing layouts. |
| **Object-Level Encryption** | **Absent**: Encryption must be managed by the storage layer. | **Absent**: Same as V1. | **Supported**: Direct key-based metadata and data encryption. | **Supported**: Fine-grained column-level access controls. |
| **Views Support** | **Absent**: Handled entirely by external query engines. | **Absent**: Same as V1. | **Supported**: Decoupled View Specifications are unified. | **Required**: Native multi-dialect view definitions. |
| **Partition Fields** | Fields must be optional/nullable in the schema. | Partition fields can be required or optional. | Partition schemas are further generalized. | Advanced geographic and hierarchical partition specs. |

---

## 3. Binding & Implementation Matrix (Java vs. PyIceberg)

This matrix compares the exact support status and strategic Python engineering rationale for every major operation across spec versions.

### 💡 Concept Mapping & Plain English Explanations
*   **Supported**: Fully implemented, tested, and ready for production use.
*   **Planned**: Active PR open or scheduled on the community roadmap.
*   **Bypassed**: Intentionally skipped or not supported because it introduces unnecessary complexity or JVM-centric overhead that Python does not require.

| Operation / Feature | Spec Version | Java (`/iceberg/`) | PyIceberg (`/iceberg-python/`) | Python-Specific Strategic Rationale & Mechanics |
| :--- | :---: | :---: | :---: | :--- |
| **Schema Evolution** | V1 / V2 / V3 | **Supported** | **Supported** | Resolved entirely in metadata by mapping Field IDs to PyArrow schemas during table scans. |
| **Partition Evolution** | V1 / V2 / V3 | **Supported** | **Supported** | Decodes partition specs and evaluates multi-spec splits in Python before pushing projection bounds to PyArrow. |
| **Copy-on-Write (CoW) Writes** | V1 / V2 | **Supported** | **Supported** | Employs PyArrow C++ datasets to write Parquet files directly, avoiding Python GIL and JVM memory overhead. |
| **Merge-on-Read (MoR) Reads** | V2 (Pos/Eq) | **Supported** | **Supported** | Reads Parquet delete logs and applies positional masks to PyArrow tables during scanning. |
| **Merge-on-Read (MoR) Reads** | V3 (DV) | **Supported** | **Supported** | Utilizes `pyroaring` to read roaring bitmaps inside Puffin blobs and masks PyArrow memory zero-copy. |
| **Merge-on-Read (MoR) Writes** | V2 (Positional) | **Supported** | **Bypassed** | **Intentionally skipped**. Writing positional delete Parquet files ($M \times 64$ bits) requires intensive sorting and memory buffering in Python. The community bypassed this in favor of V3 Deletion Vectors. |
| **Merge-on-Read (MoR) Writes** | V2 (Equality) | **Supported** | **Bypassed** | **Intentionally skipped for writes**. Python clients are not streaming engines (like Flink) and do not write high-frequency equality delete logs. |
| **Merge-on-Read (MoR) Writes** | V3 (DV) | **Supported** | **Planned** | **PR #2822 (Resurrect Candidate)**. Serializes PyArrow row selection masks into Puffin roared bitmaps, achieving optimal Shannon entropy bounds on disk. |
| **Commit Conflict Validation**| V2 / V3 | **Supported** | **Planned** | **PR #3320**. Crucial safety bedrock. Implements optimistic concurrency control validation rules (`validateDeletedFiles`, etc.) inside Python retry loops. |
| **REPLACE Snapshot API** | V1 / V2 / V3 | **Supported** | **Planned** | **PR #3131**. The core metadata transaction primitive that allows PyIceberg to swap old data files with compacted data files. |
| **Bin-Pack Data Compaction** | V1 / V2 | **Supported** | **Planned** | **PR #3124**. Groups fragmented files into $128\text{MB}-512\text{MB}$ blocks via PyArrow C++ Dataset streaming without JVM garbage collection sweeps. |
| **Sort / Z-Order Compaction** | V1 / V2 | **Supported** | **Planned** | **Future Strategy**. Re-clusters row records on disk via multidimensional space-filling curves. Accelerates DuckDB/PyArrow read queries via dictionary and min/max row-group skipping. |
| **Snapshot Expiration** | V1 / V2 | **Supported** | **Supported** | **Metadata Only (PR #1880)**. Removes historical snapshot logs from metadata, but physical file pruning is still planned. |
| **Physical File Cleanup** | V1 / V2 / V3 | **Supported** | **Planned** | **Future Strategy**. Deletes unreferenced files from S3/GCS. Requires strict `gc.enabled` checks and default 24-hour grace periods (`olderThan`) to avoid deleting uncommitted concurrent writes. |
| **Delete Orphan Files** | V1 / V2 / V3 | **Supported** | **Planned** | **PR #3361 / PR #1958**. Identifies unreferenced files in S3. Requires safety grace periods to protect active concurrent writers. |
| **RewriteManifests API** | V1 / V2 / V3 | **Supported** | **Planned** | **PR #1661 (Resurrect Candidate)**. Compacts tiny manifest files. Requires automatic clustering by partition spec and `manifest.target-size-bytes` bounds. |

---

## 4. Visualizing the Read & Write Pipelines

To connect the architectural dots, the workflows below trace how query engines resolve logical records under various specifications.

### 4.1 The Read Path: Resolving Deleted Records

A reader wants to query `my_table` under snapshot $S_k$. The query engine must resolve logical active rows by applying deletes matching the format specification:

```
                    +------------------------------------+
                    |  Read manifest-list for Snapshot   |
                    +-----------------+------------------+
                                      |
                                      v
                    +-----------------+------------------+
                    |  Identify active Manifest Files    |
                    +-----------------+------------------+
                                      |
                                      v
                    +-----------------+------------------+
                    |  Load physical Data File (Parquet) |
                    +-----------------+------------------+
                                      |
         +----------------------------+----------------------------+
         | (Spec V2 Positional)       | (Spec V2 Equality)         | (Spec V3 Deletion Vector)
         v                            v                            v
+--------+--------+          +--------+--------+          +--------+--------+
| Read Parquet    |          | Read Parquet    |          | Read compressed |
| positional      |          | equality delete |          | roaring bitmap  |
| delete file     |          | criteria        |          | from Puffin     |
+--------+--------+          +--------+--------+          +--------+--------+
         |                            |                            |
         | (Locate row positions)     | (Eval attribute matches)   | (Zero-copy bitmask)
         v                            v                            v
+--------+----------------------------+----------------------------+--------+
| Filter out deleted row indices: ActiveRows = { r in d | Psi(d, r) == 0 }  |
+-------------------------------------+-------------------------------------+
                                      |
                                      v
                       +--------------+---------------+
                       | Return logical Active Table  |
                       +------------------------------+
```

---

## 5. Architectural Guide for Pull Request Reviewers

When reviewing code in the PyIceberg write or maintenance path, enforce the following invariants to prevent catalog corruption and data loss.

### 5.1 The Four Commit Safety Assertions
Any PR implementing `RowDelta` or `RewriteFiles` must override `_validate_concurrency()` and enforce the following assertions during optimistic concurrency control (OCC) retries:

1.  **Existence Invariant (`validateDataFilesExist`)**:
    Ensure that any delete files added by the transaction target data files that are still active in the catalog. If a concurrent transaction dropped the target data file, the commit must abort.
2.  **No Double Delete (`validateDeletedFiles`)**:
    Verify that the files our transaction is attempting to remove from metadata have not been concurrently deleted by another transaction.
3.  **Serializable Appends (`validateNoConflictingAppends`)**:
    If our transaction is performing a delete matching a filter predicate under `SERIALIZABLE` isolation, assert that no concurrent transaction has appended new data files matching that predicate.
4.  **Vector Integrity (`validateAddedDVs`)**:
    Verify that no concurrent transaction has rewritten or compacted the data files targeted by our newly added Deletion Vectors.

### 5.2 Storage Integrity Safety Gates
Any physical cleanup PR (Orphan File Cleanup or Snapshot Expiration) must implement these two core safety checks:

```python
# Strict Safety Gate checks before calling FileIO.delete()
def execute_physical_cleanup(table: Table, paths_to_delete: list[str]) -> None:
    # 1. Respect GC-Enabled table property
    gc_enabled = property_as_bool(
        table.metadata.properties,
        TableProperties.GC_ENABLED,
        TableProperties.GC_ENABLED_DEFAULT  # Default: True
    )
    if not gc_enabled:
        logger.warning("Physical storage cleanup bypassed: gc.enabled is set to False")
        return

    # 2. Enforce safety grace period (default 24 hours) for orphan detection
    now_ms = current_time_millis()
    grace_period_ms = property_as_long(
        table.metadata.properties,
        TableProperties.HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS,
        TableProperties.HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS_DEFAULT  # Default: 24h (86400000ms)
    )
    
    safe_paths = []
    for path in paths_to_delete:
        mtime_ms = table.io.get_modification_time_ms(path)
        if (now_ms - mtime_ms) > grace_period_ms:
            safe_paths.append(path)
            
    # Safely proceed with physical deletion
    table.io.delete_files(safe_paths)
```
