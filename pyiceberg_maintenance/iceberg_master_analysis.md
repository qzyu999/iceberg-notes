# The Apache Iceberg Master Specification, Architecture & Implementation Matrix

This master document serves as a comprehensive, first-principles guide designed to connect all the architectural dots of the Apache Iceberg ecosystem. It demystifies Iceberg's underlying complexity, explains *why* the format is designed this way, details the evolution of features across Table Specifications (**V1, V2, V3, and V4**), and contrasts the capabilities, status, and strategic design rationales of **Java (`/iceberg/`)** and **PyIceberg (`/iceberg-python/`)**.

---

## 1. Demystifying the Complexity: "Why So Complex?"

At first glance, Apache Iceberg can feel overwhelmingly complex with its nested layers of metadata files (Metadata JSON, Manifest Lists, Manifests), column Field IDs, Spec IDs, and transaction Sequence Numbers. 

This complexity is not accidental. It is the minimal necessary engineering cost required to solve a fundamental distributed systems problem: **How do you build a transactional, ACID-compliant, petabyte-scale relational database engine on top of a highly distributed, eventually consistent, immutable object storage service (like AWS S3, Google Cloud Storage, or Azure ADLS) without a centralized database server?**

In a traditional database (e.g., PostgreSQL):
* There is a centralized server coordinating all transactions.
* There is a single active write-ahead log (WAL) on local storage.
* An active memory manager locks pages and coordinates concurrent readers and writers.

In a data lake:
* There is **no central database server**; multiple completely independent query engines (Spark, Flink, Trino, PyIceberg, DuckDB) read and write to the same storage concurrently.
* **Storage is immutable**: You cannot modify a Parquet file in-place; you can only overwrite it or write a new file.
* **Storage primitives are weak**: Object stores do not support atomic directory renames. Renaming a directory in S3 is actually an $O(N)$ network operation where every file is copied to a new key and then deleted. If this process fails halfway, the table is left in a corrupted, partially migrated state.

To guarantee atomic, high-performance operations without these server-side primitives, Iceberg replaces directory-based tracking (used by legacy systems like Apache Hive) with **explicit, metadata-driven file tracking**.

### 1.1 The Five Layers of the Iceberg State Tree

Iceberg structures its metadata as a hierarchical tree of immutable state files. To read or write to a table, a client traverses this tree from top to bottom:

```
                            +-----------------------------------+
                            |          Catalog / Pointer        |
                            | (Atomically swaps active metadata)|
                            +-----------------+-----------------+
                                              |
                                              v
                            +-----------------+-----------------+
                            |       Table Metadata JSON         |
                            | (Records specs, schemas & snaps)  |
                            +-----------------+-----------------+
                                              |
                                              v
                            +-----------------+-----------------+
                            |        Manifest List File         |
                            | (Lists active manifests + stats)  |
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

1. **Catalog Layer (The Anchor)**: A light-weight catalog (REST catalog, Nessie, Glue, JDBC, or Hive Metastore) maintains a single atomic pointer to the current `table-metadata.json` file. Committing a transaction is reduced to a single atomic operation: swapping this pointer to point to the new `table-metadata.json` file.
2. **Table Metadata Layer (The Timeline)**: An immutable JSON file that contains the table schema, partition specifications, custom table properties, and a complete chronological log of "Snapshots" representing previous commits.
3. **Manifest List Layer (The Snapshot Index)**: When a query starts, it reads a Manifest List file unique to that snapshot. This file lists all the Manifest Files active in that snapshot. Crucially, the Manifest List stores partition-level min/max statistics for every manifest file. This allows query engines to skip reading entire manifest files if they don't match the query filter (Manifest Pruning).
4. **Manifest Layer (The File Index)**: A manifest file is an Avro file that lists individual physical data files and delete files. For every file, it records the physical URI, partition values, row counts, and column-level statistics (min/max values, null counts, NaN counts). This enables query engines to prune down to the exact subset of physical data files matching a query *without* ever opening the Parquet files themselves.
5. **Physical Storage Layer**: The immutable physical data files (Parquet, ORC, Avro) and delete files (position deletes, equality deletes, or deletion vectors) stored in S3/GCS/ADLS.

---

## 2. Connecting the Dots: The Foundational Mechanics

Iceberg achieves its ultimate performance guarantees—such as zero-copy schema evolution, partition evolution without data rewrites, and concurrent multi-writer safety—through three core architectural mechanisms.

### Dot #1: Schema Evolution via Unique Field IDs
In Hive and other older formats, column resolution was performed by **name** or **position**. If you renamed a column, old data files failed to load because the schema in the file did not match. If you reordered columns, query engines misaligned data types.

Iceberg solves this by assigning a **unique, immutable integer ID (Field ID)** to every single field in the schema. 
* Column names are merely metadata labels.
* Under the hood, Iceberg only cares about the Field ID.
* If a column `user_email` (Field ID 5) is renamed to `email`, Iceberg simply updates the metadata label. The underlying physical files (where Field ID 5 was written) remain untouched. When reading old files, Iceberg maps Field ID 5 directly to the new `email` variable.

### Dot #2: Hidden Partitioning & Spec Evolution via Spec IDs
Query engines should not force users to understand the physical partitioning layout of the data lake. In Hive, tables were partitioned by directory paths (e.g., `/year=2026/month=05/`). Users had to explicitly filter by `year` and `month` columns in their SQL queries to avoid full-table scans. Furthermore, if partition strategies changed, the entire table had to be physically rewritten.

Iceberg introduces **Hidden Partitioning** and **Partition Spec Evolution**:
* **Hidden Partitioning**: The user queries a column directly (e.g., `WHERE event_time >= '2026-05-19'`). Iceberg reads the Partition Spec, applies the corresponding transform (e.g., `days(event_time)`), and automatically locates the correct physical partition files without user intervention.
* **Partition Spec Evolution**: If the table layout changes from partitioning by `month(event_time)` (Spec ID 0) to `day(event_time)` (Spec ID 1), Iceberg simply increments the active Spec ID in the metadata. Old files remain under Spec ID 0, and new files are written under Spec ID 1. During a table scan, Iceberg automatically evaluates the query filter against both Spec ID 0 and Spec ID 1 files, merging the results seamlessly without requiring any historical data to be rewritten.

### Dot #3: Concurrency Control via Sequence Numbers
In Merge-on-Read (MoR) tables, updates and deletes are written as separate "delete files" that must be merged with data files at read time. 

But consider this concurrent scenario:
1. **At Time 1 ($S_0$ - Snapshot 0)**: We have data file `d1.parquet` written under Sequence Number `1`.
2. **At Time 2**: Writer A wants to delete rows inside `d1.parquet`. It reads `d1.parquet` and writes a delete file `delete.parquet`.
3. **At Time 2 (Concurrent)**: Writer B appends new rows to the table in a new data file `d2.parquet`.
4. **At Time 3**: Both writers attempt to commit.

If the delete file `delete.parquet` was simply applied to *all* active data files, it could mistakenly delete rows in the newly appended `d2.parquet`. 

Iceberg prevents this by using **Sequence Numbers** (a logical database clock):
* Every snapshot commit increments the table's sequence number.
* When a data file is added, it inherits the sequence number of that commit snapshot ($S_{\text{data}}$).
* When a delete file is added, it is stamped with the sequence number of its commit snapshot ($S_{\text{delete}}$).
* **The Gating Rule**: A delete file $f_{\text{delete}}$ can **only** be applied to a data file $d_{\text{data}}$ if:
  $$S_{\text{data}} < S_{\text{delete}}$$
* Since `d2.parquet` was written concurrently/after the delete file was conceived, its sequence number is greater than or equal to $S_{\text{delete}}$, preventing the delete file from being incorrectly applied to it.

---

## 3. Copy-on-Write (CoW) vs. Merge-on-Read (MoR) Mechanics

Understanding the trade-offs between write amplification (WAF) and read amplification (RAF) is critical for choosing the right table format.

| Metric / Dimension | Copy-on-Write (CoW) | MoR - V2 Positional Deletes | MoR - V2 Equality Deletes | MoR - V3 Deletion Vectors (DV) |
| :--- | :--- | :--- | :--- | :--- |
| **Write Speed (for Updates/Deletes)** | **Slow**: Must rewrite entire 128MB–512MB data files containing mutated rows. | **Fast**: Appends a small Parquet file listing row indices. | **Fast**: Appends a small Parquet file listing key rules (e.g., `id = 12`). | **Fastest**: Appends highly compressed Roaring Bitmaps directly. |
| **Query Read Latency** | **Fastest (Read-Optimized)**: Pure sequential scans. No overhead. | **Slow**: Client must build a memory index of row positions to mask data. | **Slowest**: Client must perform join-like matches on equality keys at runtime. | **Very Fast**: Client performs zero-copy bitmask operations on Arrow memory. |
| **Write Amplification (WAF)** | Extremely High ($O(\text{File Size})$) | Very Low ($O(\text{Deleted Positions})$) | Very Low ($O(\text{Delete Rules})$) | Minimal ($O(\text{Bitsets})$) |
| **Read Amplification (RAF)** | 1.0 (No redundant read IO) | Higher (Must read data file + positional delete Parquet) | High (Must read data file + evaluate rules) | Near 1.0 (Bitmaps are tiny and stored in fast Puffin blobs) |
| **Memory Footprint at Read-Time** | Low | High (Requires mapping arrays of long integers) | Very High (Requires hashing lookup keys) | Low (Roaring Bitmaps are highly compact) |

---

## 4. The Master Specification & Implementation Matrix

The following master matrix analyzes all major Iceberg operations, comparing Table Specification versions (**V1, V2, V3, V4**) against the **Java (`/iceberg/`)** and **PyIceberg (`/iceberg-python/`)** bindings.

### 💡 Concept Key
* **Supported**: Fully implemented, thoroughly tested, and ready for production use.
* **Planned**: Active community development, draft PRs, or scheduled on the roadmap.
* **Bypassed**: Intentionally skipped or not supported because it represents a JVM-centric paradigm, introducing unnecessary streaming complexity or memory management overhead that Python does not require.

| Operation / Feature Area | Spec V1 | Spec V2 | Spec V3 | Spec V4 (Draft) | Java Support | PyIceberg Support | Python-Specific Strategic Rationale & Mechanics |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **Schema Evolution** | yes | yes | yes | yes | **Supported** | **Supported** | Decodes metadata maps and applies projected schemas to PyArrow datasets during scans, executing entirely in C++. |
| **Partition Spec Evolution** | yes | yes | yes | yes | **Supported** | **Supported** | Resolves multi-spec partition bounds on-the-fly and pushes filter projections down to Arrow datasets. |
| **Copy-on-Write (CoW) Writes**| yes | yes | yes | yes | **Supported** | **Supported** | Employs PyArrow C++ Dataset APIs to write structured Parquet files directly, completely bypassing the Python GIL. |
| **Merge-on-Read (MoR) Reads** | N/A | yes | yes | yes | **Supported** | **Supported** | Reads Positional Deletes (V2) or Deletion Vectors (V3 roaring bitmaps inside Puffin blobs) and applies logical masks to Arrow memory. |
| **MoR Writes (Equality)** | N/A | yes | yes | yes | **Supported** | **Bypassed** | **Intentionally skipped for writes**. Python clients are not streaming ingestion engines (like Flink) and do not write high-frequency equality delete logs. |
| **MoR Writes (Positional)** | N/A | yes | yes | yes | **Supported** | **Bypassed** | **Intentionally skipped for writes**. Writing positional deletes requires massive memory buffers and global offset sorting in Python. Bypassed in favor of high-performance V3 DVs. |
| **MoR Writes (Deletion Vectors)**| N/A | N/A | yes | yes | **Supported** | **Planned** | **PR #2822**. Directly serializes Arrow selection bitmasks into roaring bitmaps inside Puffin blobs, avoiding the sorting overhead of positional deletes. |
| **Commit Concurrency Validation**| N/A | yes | yes | yes | **Supported** | **Planned** | **PR #3320**. The optimistic concurrency control safety bedrock. Implements robust validation assertions (`validateDeletedFiles`, etc.) inside retries. |
| **REPLACE Snapshot API** | yes | yes | yes | yes | **Supported** | **Planned** | **PR #3131**. The core metadata transaction primitive that allows PyIceberg to atomic-swap old data files with compacted data files. |
| **Bin-Pack Data Compaction** | yes | yes | yes | yes | **Supported** | **Planned** | **PR #3124**. Consolidates small files ($<128\text{MB}$) into large ones ($128\text{MB}–512\text{MB}$) using PyArrow C++ streaming datasets, avoiding JVM garbage collection. |
| **Sort / Z-Order Compaction** | yes | yes | yes | yes | **Supported** | **Planned** | Re-clusters records along multi-dimensional space-filling curves, enabling downstream analytics tools (DuckDB, Ray) to skip row groups. |
| **Snapshot Expiration** | yes | yes | yes | yes | **Supported** | **Supported** | **Metadata only (PR #1880)**. Safely prunes expired snapshot logs from `table-metadata.json`. Physical S3/GCS file deletion is planned. |
| **Physical Storage Pruning** | yes | yes | yes | yes | **Supported** | **Planned** | Deletes unreferenced physical files. Requires strict safety gating (checking `gc.enabled` and enforcing 24-hour grace periods). |
| **Delete Orphan Files** | yes | yes | yes | yes | **Supported** | **Planned** | **PR #1958**. Scans S3/GCS directories and removes files not referenced in metadata. Protects active transactions using grace periods. |
| **RewriteManifests API** | yes | yes | yes | yes | **Supported** | **Planned** | **PR #1661**. Consolidates highly fragmented manifest files into standardized `manifest.target-size-bytes` blocks. |

---

## 5. Architectural Guide for Pull Request Reviewers

When reviewing code in the PyIceberg write, maintenance, or compaction pipelines, enforce the following safety rules to prevent catalog corruption and silent data loss.

### 5.1 The Four Commit Safety Assertions
Any pull request that implements `RowDelta` writes or compaction `RewriteFiles` must override `_validate_concurrency()` and assert the following four conditions during optimistic concurrency control (OCC) transaction commit retries:

1. **Existence Invariant (`validateDataFilesExist`)**:
   Verify that any delete files added by the transaction target physical data files that are still active in the catalog. If a concurrent transaction deleted or compacted a target data file, our transaction must abort.
2. **No Double Delete (`validateDeletedFiles`)**:
   Assert that the data files our transaction is attempting to remove from metadata have not been concurrently deleted by another transaction.
3. **Serializable Appends (`validateNoConflictingAppends`)**:
   If the transaction performs a delete matching a filter predicate under `SERIALIZABLE` isolation, assert that no concurrent transaction has appended new data files matching that predicate.
4. **Vector Integrity (`validateAddedDVs`)**:
   Assert that no concurrent transaction has rewritten or compacted the data files targeted by our newly added Deletion Vectors.

### 5.2 Storage Integrity Safety Gates
Any physical storage pruner (Snapshot Expiration or Delete Orphan Files) **must** implement these two safety check gates before executing physical file deletions:

```python
# Absolute Safety Gate checks before invoking table.io.delete()
def execute_physical_cleanup_safely(table: Table, files_to_delete: list[str]) -> None:
    # 1. GC Gating: Verify if physical deletion is allowed by metadata configuration
    gc_enabled = property_as_bool(
        table.metadata.properties,
        TableProperties.GC_ENABLED,
        TableProperties.GC_ENABLED_DEFAULT  # Default: True
    )
    if not gc_enabled:
        logger.warning("Physical storage cleanup bypassed: gc.enabled is set to False")
        return

    # 2. Grace Period Gating: Protect active concurrent writes (default 24 hours)
    now_ms = current_time_millis()
    grace_period_ms = property_as_long(
        table.metadata.properties,
        TableProperties.HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS,
        TableProperties.HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS_DEFAULT  # Default: 24h (86400000ms)
    )
    
    safe_files = []
    for filepath in files_to_delete:
        modification_time_ms = table.io.get_modification_time_ms(filepath)
        if (now_ms - modification_time_ms) > grace_period_ms:
            safe_files.append(filepath)
        else:
            logger.info(f"File skipped (within safety grace period): {filepath}")
            
    # Atomic execution of safe physical deletions
    table.io.delete_files(safe_files)
```

By enforcing these guidelines, reviewers protect the structural integrity of Apache Iceberg tables across both JVM and Python environments.
