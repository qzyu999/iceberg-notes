# Proposal for `rewrite_data_files` being added to PyIceberg

## Table of Contents
- [Config](#config)
- [PySpark process for compaction](#pyspark-process-for-compaction)
    - [Compaction Parameters](#compaction-parameters)
    - [Compaction Code [Rewrite Data Files Implementation (Java/Spark)]](#compaction-code-rewrite-data-files-implementation-javaspark)
        - [Architecture Overview](#architecture-overview)
        - [1. Planning Phase](#1-planning-phase)
        - [2. Execution Phase](#2-execution-phase)
        - [3. Commit Phase](#3-commit-phase)
        - [Key Differences for pyiceberg](#key-differences-for-pyiceberg)
        - [Files for Reference](#files-for-reference)
- [Steps for PyIceberg to Adopt this Process](#steps-for-pyiceberg-to-adopt-this-process)

## Config
- PySpark reference version: v3.5 (latest major stable version)
- PyIceberg reference version: latest (v0.11.0)

## PySpark process for compaction
To run compaction in PySpark, you can use the following code:
```sql
spark.sql("CALL catalog.system.rewrite_data_files(table => 'db.table')")
```

However, there are various parameters that can be added such as follows:
```sql
spark.sql("""
    CALL catalog.system.rewrite_data_files(
        table => 'db.events',
        strategy => 'sort',
        sort_order => 'zorder(event_id, user_id)',
        where => 'event_date = "2024-01-01"',
        options => map(
            'target-file-size-bytes', '268435456', -- 256MB
            'partial-progress.enabled', 'true',
            'max-concurrent-file-group-rewrites', '10'
        )
    )
""")
```

### Compaction Parameters
The top-level procedure arguments are as follows:
- `table` (Required, string): The name of the table to rewrite.
- `strategy` (Optional, string): Use binpack (default, combines small files without reordering) or sort (compacts and sorts data for faster queries).
- `where` (Optional, string): A SQL-like filter to rewrite only specific partitions or subsets of data (e.g., 'date > "2023-01-01"').
- `sort_order` (Optional, string): Only for the sort strategy. Specifies column sorting (e.g., 'id ASC NULLS LAST') or Z-Ordering (e.g., 'zorder(col1, col2)').
- `options` (Optional, map): A key-value map for the detailed settings listed below.
    - **Comprehensive Options Map (`options => map(...)`)**
        - These granular settings are passed inside the options map to control performance and file size targets.
        - **File Size & Selection Targets**
            - `target-file-size-bytes`: The desired size of output files (Default: 512 MB or the table's write.target-file-size-bytes).
            - `min-file-size-bytes`: Files smaller than this are candidates for compaction (Default: 75% of target).
            - `max-file-size-bytes`: Files larger than this are candidates for splitting (Default: 180% of target).
            - `min-input-files`: Minimum number of files required in a group to trigger a rewrite (Default: 5).
            - `rewrite-all`: If set to 'true', forces a rewrite of all files regardless of size. 
        - **Execution & Parallelism**
            - `max-concurrent-file-group-rewrites`: Number of file groups rewritten simultaneously in parallel (Default: 5).
            - `max-file-group-size-bytes`: The max amount of data processed in a single Spark task group (Default: 10 GB).
            - `rewrite-job-order`: Determines which data is compacted first. Options: bytes-asc, bytes-desc, files-asc, files-desc, or none. 
        - **Reliability & Progress**
            - `partial-progress.enabled`: If 'true', commits completed groups even if others fail, preventing a total rollback on large jobs (Default: 'false').
            - `partial-progress.max-commits`: Max number of intermediate commits to perform (Default: 10).
            - `partial-progress.max-failed-commits`: Determines the "tolerance" of the job. If multiple mini-commits fail (due to write conflicts), the job will only fail entirely once this threshold is hit.
            - `use-starting-sequence-number`: When 'true', uses the original sequence number for rewritten files to avoid conflicts with concurrent deletes. 
        - **Advanced Delete & Partitioning Options**
            - `delete-file-threshold`: Forces a rewrite of a data file if it is associated with more than this number of delete files, even if the file size is otherwise "healthy."
            - `delete-ratio-threshold`: The fraction of rows in a data file that are marked as deleted. If a file has 30% or more deleted rows, Iceberg will rewrite it to reclaim space.
            - `output-spec-id`: If you have Evolved your Partition Spec (e.g., changed partitioning from Days to Months), setting this allows you to physically move old data into the new partitioning scheme during the compaction process.
            - `remove-dangling-deletes`: Cleans up delete files that no longer point to any active data (often caused by manual cleanups or edge cases).

### Compaction Code [Rewrite Data Files Implementation (Java/Spark)]
This document provides a breakdown of how `rewrite_data_files` is implemented in the Iceberg Java/Spark codebase to serve as a reference for `pyiceberg`.

#### Architecture Overview

The `rewrite_data_files` action follow a three-phase approach: **Planning**, **Execution**, and **Committing**.

#### 1. Planning Phase
The goal is to identify which files need rewriting and group them into manageable "file groups".

*   **Main Class**: [BinPackRewriteFilePlanner.java](iceberg/core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java)
*   **Logic**:
    1.  **Filtering**: Selects files based on the table's current snapshot and user-provided filters.
    2.  **Grouping**: Groups files by partition.
    3.  **Bin-Packing**: Within each partition, files are grouped into "bins" using a bin-packing algorithm. It aims to combine small files into larger ones (up to `target-file-size-bytes`) or split large files.
    4.  **Thresholds**: Decides if a file needs rewriting based on size, the number of delete files associated with it, or the delete ratio.

#### 2. Execution Phase
The goal is to actually read the data from the identified file groups and write it out into new, optimized files.

*   **Action Class**: [RewriteDataFilesSparkAction.java](iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java)
*   **Runner Class**: [SparkBinPackFileRewriteRunner.java](iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackFileRewriteRunner.java)
*   **Workflow**:
    1.  The action creates a `groupId` (UUID) for each file group.
    2.  It stages the tasks (files to be read) in a `ScanTaskSetManager`.
    3.  It triggers a Spark job that reads the staged tasks using the `iceberg` Spark source.
    4.  The Spark job writes the data out using the `iceberg` Spark sink.
    5.  A `FileRewriteCoordinator` keeps track of the new files generated by the Spark job.
    6.  The runner then fetches the list of new files from the coordinator.

#### 3. Commit Phase
The goal is to atomically replace the old files with the new files in the Iceberg table metadata.

*   **Commit Manager**: [RewriteDataFilesCommitManager.java](iceberg/core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java)
*   **Logic**:
    1.  Uses the Iceberg `Table` API: `table.newRewrite()`.
    2.  `rewrite.deleteFile(oldFile)` for each file that was rewritten.
    3.  `rewrite.addFile(newFile)` for each newly created file.
    4.  Calls `rewrite.commit()` to finalize the transaction.
    5.  Support for **Partial Progress**: If enabled, it can commit smaller batches of file groups independently, allowing the operation to make progress even if some groups fail or conflict.

#### Key Differences for pyiceberg

When implementing this in `pyiceberg`, you will likely need to:
1.  **Planning**: Implementation of the bin-packing algorithm in Python (likely using `pyiceberg`'s manifest reading capabilities).
2.  **Execution**: Instead of Spark, use a local engine (like PyArrow/DuckDB) or a distributed engine (like Ray/Dask) to read the source files and write new Parquet/Avro/ORC files.
3.  **Committing**: Use the `pyiceberg` Table API to perform a `REPLACE` operation (metadata update).

#### Files for Reference
- [RewriteDataFilesSparkAction.java](iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java) - High-level orchestration.
- [BinPackRewriteFilePlanner.java](iceberg/core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java) - Core planning logic.
- [SparkBinPackFileRewriteRunner.java](iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackFileRewriteRunner.java) - Spark execution runner.
- [RewriteDataFilesCommitManager.java](iceberg/core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java) - Commit coordination.

## Steps for PyIceberg to Adopt this Process
