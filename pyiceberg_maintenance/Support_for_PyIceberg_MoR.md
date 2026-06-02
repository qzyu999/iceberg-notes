Support for PyIceberg Merge-on-Read (MoR)

[qzyu999@gmail.com](mailto:qzyu999@gmail.com)

# Introduction

Apache Iceberg supports two primary mechanisms for row-level updates and deletions: Copy-on-Write (CoW) and Merge-on-Read (MoR). Currently, PyIceberg primarily supports CoW, which requires rewriting entire data files even if only a single row is modified. While CoW is suitable for batch processing, it introduces severe write amplification for streaming, Change Data Capture (CDC), and high-frequency micro-batch workloads. (Note: Even within batch contexts, PyIceberg's current CoW implementation faces specific operational challenges, such as the lack of native upsert support and single-node memory constraints, detailed in Appendix A).

MoR shifts this compute burden from write-time to read-time by writing small delta files (delete files) alongside the base data files. To fully support the Iceberg specification and serve as a robust query engine for data lakes, PyIceberg must be able to resolve these delete files during scan planning and execution. This implementation needs to consider both the V2 specification (Parquet-based Position and Equality deletes) and the V3 specification (Puffin-based Deletion Vectors).

## Key pain-points

- **Write Amplification & Latency:** Ingesting CDC data or frequent updates via CoW in PyIceberg is practically infeasible due to the massive I/O overhead of rewriting large Parquet files.  
- **Incomplete Read Compatibility:** Data engineered by other engines (like Flink or Spark) often utilizes MoR. PyIceberg currently cannot accurately read tables where V2 delete files or V3 Deletion Vectors are present, risking stale or incorrect data exposure to downstream analytical Python applications.  
- **Single-Node Memory Bottlenecks:** Evaluating delete files—particularly Equality Deletes—requires joining delta files against base data. In Python's typical single-node environment, naive in-memory evaluations can easily trigger out-of-memory (OOM) crashes.

# 

# Background

This section outlines recent architectural updates, establishing the context necessary because the implementation of Merge-on-Read (MoR) deeply interacts with these various mechanisms. This will provide a foundation for the **Feature Interactions** section later in the document.

## The MaintenanceTable API, Snapshot Expiration, and Orphan File Deletion

With [refactor: consolidate snapshot expiration into MaintenanceTable · Issue \#2142 · apache/iceberg-python](https://github.com/apache/iceberg-python/issues/2142) and [PR \#2143](https://github.com/apache/iceberg-python/pull/2143) PyIceberg refactors towards a cleaner setup where maintenance tasks are to be consolidated into a dedicated MaintenanceTable class. The first addition to it is the work from [Support Snapshot Expiration Operation · Issue \#516 · apache/iceberg-python](https://github.com/apache/iceberg-python/issues/516) and [PR \#1880](https://github.com/apache/iceberg-python/pull/1880) a limited snapshot expiration (compared to the Java implementation) that only deletes certain metadata files while leaving orphaned data/metadata files, lacks advanced retention strategies (e.g., retainLast()), and is single-threaded. Later, PyIceberg gained the ability to automate this metadata pruning via support for support for the write.metadata.delete-after-commit.enabled and write.metadata.previous-versions-max table properties from [Remove old metadata files \#1199 \- apache/iceberg-python](https://github.com/apache/iceberg-python/issues/1199) and [PR \#1607](https://github.com/apache/iceberg-python/pull/1607). There is however [Remove deleted data files with expire\_snapshots · Issue \#2604 · apache/iceberg-python](https://github.com/apache/iceberg-python/issues/2604) that is still pending which pushes to extend the existing expire\_snapshots to also remove the referenced data files. These are the initial maintenance operations of PyIceberg.

From snapshot expiration comes the baggage of orphan files (e.g., physical objects such as Parquete data files and Avro manifests). Orphan files are primarily generated from three situations: failed or interrupted writes, snapshot expiration not deleting storage, and storage path or authority alterations on the underlying URI such as S3 endpoints. [Delete orphan files \#1200 \- apache/iceberg-python](https://github.com/apache/iceberg-python/issues/1200) exists, however it remains open as there are currently two PR’s that reference it which remain unmerged, [PR \#1958](https://github.com/apache/iceberg-python/pull/1958) and [PR \#3361](https://github.com/apache/iceberg-python/pull/3361). The implementation of this feature has been hindered by several architectural and performance hurdles. A primary pain point is the cost of listing directories on object stores like S3; exhaustive directory scans yield massive paged responses that severely degrade performance. To circumvent this without bloating the project with new dependencies like OpenDAL, the community had to determine the best way to utilize PyArrow's FileSystem or extend existing IO classes. Furthermore, safely identifying orphan files requires meticulously cross-referencing all storage paths against the complete set of valid files—including metadata logs, historical lineage, branches, and tags—to prevent accidental data loss. This proved technically challenging in earlier iterations like [PR \#1958](https://github.com/apache/iceberg-python/pull/1958), which stalled due to excruciatingly slow recursive directory scans and bugs where URI scheme prefixes were stripped during file globbing. More recently, [PR \#3361](https://github.com/apache/iceberg-python/pull/3361) attempts to overcome these API blockers by natively adding a `list` method directly to PyIceberg's `FileIO` while adopting a `remove_orphan_files` nomenclature to align with Spark and Trino, intentionally distinguishing the maintenance task from row-level SQL deletes. The below table shows the status of these features.

| Feature / Objective | Issue | PR(s) | Current Status | Notes / Dependencies |
| :---- | :---- | :---- | :---- | :---- |
| **Limited Snapshot Expiration** | \#516 | \#1880 | **Merged** ✅ | Deletes metadata only. Single-threaded and leaves behind orphaned data files. |
| **Automate Metadata Pruning** | \#1199 | \#1607 | **Merged** ✅ | Enables table properties (write.metadata.delete-after-commit.enabled, etc.) to auto-delete old metadata files. |
| MaintenanceTable **Refactor** | \#2142 | \#2143 | **Merged** ✅ | Creates the foundational class dedicated to consolidating maintenance tasks. |
| **Full Snapshot Expiration** | \#2604 | N/A | **Pending** ⏳ | Extends the limited expiration (\#516) to physically remove the referenced data files as well. |
| **Delete Orphan Files** | \#1200 | \#1958 *(Stalled)* \#3361 *(Active)* | **In Progress** ⏳ | Highly complex due to S3 listing costs. PR \#3361 attempts to unblock this by natively adding a list method to FileIO. |

## Commit Retries, REPLACE Operation API, and Compaction

Apache Iceberg runs under the paradigm of Optimistic Concurrency Control (OCC) whereby a Compare and Swap (CAS) operation, the engine executes atomic swaps at the catalog level ensuring that race conditions between reads and writes don’t pollute the underlying data. As of now, PyIceberg is able to follow these rules, but it doesn’t yet allow for automated retries when for example newly proposed snapshots can be updated rather than outright rejected. [Support IsolationLevels and Concurrency Safety Validation Checks · Issue \#819 · apache/iceberg-python](https://github.com/apache/iceberg-python/issues/819) has been the main issue tracking this, where the bedrock validate functions have all been merged in: [PR \#1935](https://github.com/apache/iceberg-python/pull/1935), [PR \#1938](https://github.com/apache/iceberg-python/pull/1938), [PR \#2050](https://github.com/apache/iceberg-python/pull/2050), and [PR \#3049](https://github.com/apache/iceberg-python/pull/3049). However, a final combination of all these into a single commit retry with conflict validation is still in progress in [Add commit retry with data conflict validation · Issue \#3319 · apache/iceberg-python](https://github.com/apache/iceberg-python/issues/3319) and [PR \#3320](https://github.com/apache/iceberg-python/pull/3320).

With the completion of [PR \#3320](https://github.com/apache/iceberg-python/pull/3320), that makes room for [Feature: Add metadata-only replace API to Table for REPLACE snapshot operations · Issue \#3130 · apache/iceberg-python](https://github.com/apache/iceberg-python/issues/3130) and [PR \#3131](https://github.com/apache/iceberg-python/pull/3131), which adds write ability for the REPLACE data operation (the only data operation without write ability currently in PyIceberg), utilized during data compaction (`RewriteFiles`) and manifest optimization (`RewriteManifests`). Once complete, then [Support data files compaction · Issue \#1092 · apache/iceberg-python](https://github.com/apache/iceberg-python/issues/1092) and [PR \#3124](https://github.com/apache/iceberg-python/pull/3124) along with [Support metadata compaction · Issue \#270 · apache/iceberg-python · GitHub](https://github.com/apache/iceberg-python/issues/270) and [PR \#1661](https://github.com/apache/iceberg-python/pull/1661) can proceed. The below table makes the dependency chain clear.

| Feature / Objective | Issue | PR(s) | Current Status | Blocks / Unblocks |
| :---- | :---- | :---- | :---- | :---- |
| **Concurrency Safety Validation Checks** | \#819 | \#1935, \#1938, \#2050, \#3049 | **Merged** ✅ | Unblocked Issue \#3319 |
| **Commit Retries & Conflict Validation** | \#3319 | \#3320 | **In Progress** ⏳ | Blocks the REPLACE API (PR \#3131) |
| REPLACE **API (Metadata-only)** | \#3130 | \#3131 | **Blocked** 🛑 *(Waiting on \#3320)* | Blocks Data & Metadata Compactions |
| **Data Files Compaction** | \#1092 | \#3124 | **Blocked** 🛑 *(Waiting on \#3131)* | N/A *(End Goal)* |
| **Metadata Compaction** | \#270 | \#1661 | **Blocked** 🛑 *(Waiting on \#3131)* | N/A *(End Goal)* |

# Merge-on-Read (MoR)

With Apache Iceberg V2, potentially expensive Copy-on-Write (CoW) operations where 100+ MB Parquet files can be rewritten simply due to deleting or updating a single row is overcome through Merge-on-Read (MoR). While CoW remains optimal for read-time queries, writes through MoR simplify to the addition of small “delete files” (updates simply involve a delete file followed by an append file for the new records). This greatly benefits high-throughput streaming pipelines, Change Data Capture (CDC) workloads, and regulatory data privacy compliance operations.

## Positional Deletes, Equality Deletes, and Delete Compaction

V2 delete files come in two variations: positional deletes and equality deletes. Positional deletes are highly specific, recording the exact physical file path and the precise row index of the deleted record. While positional deletes offer high read performance, they place a massive burden on the writer. To generate a positional delete, the writer must already know the exact physical location of a row, which usually requires executing a full read operation before writing (e.g., `DELETE FROM table WHERE status = 'inactive'`). Streaming compute engines, such as Apache Flink, cannot tolerate this read-before-write latency. Instead, Flink issues Equality Deletes. An equality delete simply states a logical condition (e.g., `account_id = 999`), and the query engine is responsible for evaluating this condition across the entire dataset at read time.

The work for MoR in PyIceberg first began with [Python: Table scan returning deleted data · Issue \#6568 · apache/iceberg](https://github.com/apache/iceberg/issues/6568) and [PR \#6775 (Java)](https://github.com/apache/iceberg/pull/6775), an issue and PR under Java Iceberg before the separate iceberg-python repo was created. Then, with [\[Feat\] Support Merge-on-Read mode for Deletes · Issue \#1078 · apache/iceberg-python](https://github.com/apache/iceberg-python/issues/1078), the initial attempt began for full V2 MoR capability. Still unfinished, there has even been mention of skipping the full V2 implementation and going for V3’s Deletion Vectors (DVs) in PyIceberg. [Support producing positional deletes · Issue \#1808 · apache/iceberg-python](https://github.com/apache/iceberg-python/issues/1808) was added to support the writing side of positional deletes, however there’s no associated PR and has since been automatically marked stale. Support for equality delete files in PyIceberg first comes up in [\[feature request\] Support reading equality delete files · Issue \#1210 · apache/iceberg-python](https://github.com/apache/iceberg-python/issues/1210) and its associated [PR \#2255](https://github.com/apache/iceberg-python/pull/2255) which has been closed by geruh possibly due to the PR size, then later again in [Equality Delete support · Issue \#3270 · apache/iceberg-python](https://github.com/apache/iceberg-python/issues/3270) which seems to be a duplicate issue but simply highlights the fact that the lack of read support for equality delete files leads to data correctness problems. [PR \#3285](https://github.com/apache/iceberg-python/pull/3285) references [issue \#3270](https://github.com/apache/iceberg-python/issues/3270), and works to add support for equality deletes to the existing `DeleteFileIndex` (merged in [PR \#2918](https://github.com/apache/iceberg-python/pull/2918)) but still needs review.

In summary, PyIceberg currently only supports reads for positional deletes which are optimized via `DeleteFileIndex`. Furthermore, reading equality deletes lead to the issue of needing to potentially load millions of delete files into memory which would lead to an OOM crash (as noted in [PR \#3285](https://github.com/apache/iceberg-python/pull/3285)). Although writing equality deletes would technically be feasible, that brings the issue of a UX where a user can write changes, but then immediately upon querying the results get inaccurate data (no issue or PR either). There seems to be space though for [issue \#1808](https://github.com/apache/iceberg-python/issues/1808), where a `RowDelta` API would need to be built out which itself doesn’t suffer from the memory constraints of equality deletes. It’s worth mentioning that no delete file compaction issue or PR has yet been created. Delete file compaction in V2 would consist of two components, the first being converting equality deletes to positional deletes and the second being merging small positional deletes. This too however suffers from the memory issue we see with reading equality deletes. The below table shows these issues and PRs.

| Format Evolution & MoR | Operation / Topic | Issue | PR | Owner | Status Definition |
| :---- | :---- | :---- | :---- | :---- | :---- |
| **V2 Mechanics** | Support Merge-on-Read (MoR) | \#1078 | N/A | N/A | **Missing:** Epic issue open; fundamental MoR write support lacking. |
| **V2 Read Path** | Support reading positional delete files | \#6568 | \#6775 | Fokko | **Merged** |
| **V2 Write Path** | Support writing positional deletes | \#1808 | N/A | N/A | **Missing:** Closed by bot; technical implementation for MoR stalled. |
| **V2 Read Path** | Support reading equality delete files | \#1210 | \#2255 | geruh | **In Progress:** Open PR for equality delete read capabilities (OOM issues). |
| **V2 Write Path** | Support writing equality deletes | N/A | N/A | N/A | **Missing:** No issue tracking support for writing equality deletes (OOM issues). |
| **Optimization with DeleteFileIndex** | Add DeleteFileIndex for positional deletes | N/A | \#2918 (related to \#2255) | geruh | **Merged** |
| **Data Correctness** | Support reading equality delete files | \#3270 | N/A | rambleraptor | **Missing:** Open issue addressing critical data correctness risks. |
| **Optimization with DeleteFileIndex** | Support reading equality deletes with DeleteFileIndex | \#3270 | \#3285 | rambleraptor | **In Progress:** Open PR adding index plumbing; avoids PyArrow crashes. Still avoids actual reading of equality delete files (OOM issues). |
| **Optimization with Compaction** | Delete file compaction | N/A | N/A | N/A | **Missing:** Needs new issue tracking equality-to-positional conversion and merging of many positional delete files. |

## Deletion Vectors (DVs)

V3 brings Deletion Vectors (DVs) which solve the performance issues inherent in V2 positional deletes. Instead of writing cumbersome, serialized Avro or Parquet delete files containing millions of \[file\_path, row\_index\] tuples, DVs utilize Roaring Bitmaps. A single, highly compressed bitmap can efficiently represent millions of deleted rows for a specific data file. This architectural shift drastically reduces storage I/O, minimizes memory footprint during the planning phase, and significantly lowers CPU overhead during read-time data reconciliation. Equality deletes themselves aren’t directly impacted by DVs, but by doing equality-to-positional conversion those converted files can be stored as DVs. This practice is encouraged to keep inefficient equality delete files optimized.

Read support for DVs has been completed in [Support Deletion Vectors \#1549 \- apache/iceberg-python](https://github.com/apache/iceberg-python/issues/1549) and [PR \#1516](https://github.com/apache/iceberg-python/pull/1516). An issue for write support for DVs was opened in [Iceberg Deletion Vector Write Support · Issue \#2261 · apache/iceberg-python](https://github.com/apache/iceberg-python/issues/2261), but has been closed as stale. There are two associated PRs, [PR \#2193](https://github.com/apache/iceberg-python/pull/2193) and [PR \#2822](https://github.com/apache/iceberg-python/pull/2822), both of which have been closed as stale also. These seem to be independent implementations, where neither look to entirely solve write support, but are instead building out fundamental components that would make it possible. The V3 tracking issue, [V3 Tracking issue · Issue \#1818 · apache/iceberg-python](https://github.com/apache/iceberg-python/issues/1818), also includes these. The below table shows these issues and PRs.

| V3 Specification Features | Operation / Topic | Issue | PR | Owner | Status Definition |
| :---- | :---- | :---- | :---- | :---- | :---- |
| **V3 Read Path** | Deletion vector read support | \#1549 | \#1516 | Fokko | **Merged** |
| **V3 Write Path** | Deletion vector write support | \#2261 | \#2193, \#2822 | yingjianwu98, rambleraptor | **Missing:** Closed by bot; requires review. |
| **Meta-Tracking** | V3 Tracking Issue | \#1818 | \#1516, \#2822 | Fokko, rambleraptor | **Open:** High-level epic tracking all V3 specification components. |

The issue with writing DVs is fortunately different from what is faced with reading equality deletes. The simple approach to reading equality deletes in PyIceberg would typically require loading the entire unbounded set of delete values into a hashset via PyArrow in the single node’s RAM and streaming the data through it, essentially performing an anti-join which can lead to OOM errors. The Java implementation has worked to optimize this through metadata pushdowns, multi-threaded task splitting, and multi-node join mechanics to distribute the memory footprint in a query engine such as Apache Spark.

DVs, while similarly suffering from needing to first read the underlying data files to locate the exact row positions of deleted records (the "read-before-write" penalty), completely avoid the OOM traps of equality deletes. With DVs, PyIceberg can chunk through the data, apply the logical filters, extract the file paths with their deleted row indices, and update the DV. The only object in memory is the DV itself, which through extreme compression can track millions of deleted rows in a few MB of RAM.

## The V4 Horizon

With V4 in development in the Java implementation of Iceberg, we can look ahead to see if there are any potential game-changers with respect to how MoR works. Namely, the bottleneck we face is reading equality deletes for MoR, along with other OOM-related issues such as deleting orphan files that make single-node implementation within Iceberg bindings difficult. V4 looks to bring: single-file commits, storing metadata in Parquet, column statistics as first-class, and relative paths. Although they may improve MoR efficiency, the OOM issues remain unresolved at a high-level glance. Although these features could require deep rework and/or adaptation of existing or potential V2/V3 implementations, they don’t fundamentally create breakthroughs that would unblock current issues.

## Apache Arrow, Apache DataFusion, and the OOM Problem

Currently various features in PyIceberg are impacted by the OOM problem where a single-node PyIceberg instance is incapable of handling certain operations due to memory constraints. PyArrow, the Python library for Apache Arrow, is the execution layer for PyIceberg. Apache Arrow is the in-memory columnar format the way Apache Parquet is the on-disk columnar format. Utilizing Arrow provides various benefits such as: zero-copy serialization, vectorized execution, Parquet synergy, and Python ecosystem interoperability. Yet, finding parity with Java Iceberg remains a challenge largely due to the lack of integration with a distributed query engine such as Apache Spark or Trino. PyArrow alone can’t solve this, but with the help of Apache DataFusion we can potentially break this barrier.

Apache DataFusion is a fast Rust-based query engine that integrates deeply with Arrow arrays and executes SQL/DataFrame operations to bypass Python’s Global Interpreter Lock (GIL). With DataFusion, Iceberg bindings can accomplish what previously is either unrealistic or impossible with Arrow alone. With Java Iceberg, typically these memory-bound operations are distributed across nodes to prevent crashes. Using DataFusion, the query engine on a single node keeps track of the load on the RAM and will spill to disk if it breaks a threshold, allowing it to scale vertically as opposed to Spark’s horizontal scalability. This is the key property that PyArrow doesn’t allow PyIceberg to do in its current state. This would allow all those operations in PyIceberg to continue, as long as there’s enough space on disk for the operations to succeed. This in itself doesn’t differ dramatically from how Spark operates, in that it continues as long as there are enough resources to continue.

We must first consider whether PyArrow alone can resolve the OOM issues we are facing. As mentioned for reading equality deletes, the issue is that of comparing data files to an in-memory hashset. However, we could still potentially batch the hashset as well, leading to something more akin to a O(NM) time complexity while ensuring neither the dataset nor the hashset are loaded entirely into RAM. With DataFusion, we have the ability to for example partition the data and hashset into buckets, spill the data to temporary disk space, and then join by bucket in-memory for a true O(N+M) time complexity.

Furthermore, DataFusion has other desirable attributes that help to solve the various OOM issues. For example, with orphan file deletion, the operation is fundamentally a massive `LEFT ANTI JOIN` between the complete list of physical objects in storage (e.g., S3 paths) and the valid files tracked across Iceberg’s metadata lineage. If you attempt this by manually chunking through PyArrow, you introduce severe operational risk; if a script drops a batch or runs out of memory mid-execution, it loses global state and risks permanently deleting live data. DataFusion natively parses this as a relational algebra plan. It tracks the global state, executes the cross-reference, and gracefully spills to disk when the file lists exceed RAM, mathematically guaranteeing zero data loss without crashing.

Also, with UPSERTs and data compaction (`RewriteFiles`), the engine must perform complex, holistic operations like shuffles, groupings, and sorts to route incoming records to their correct physical partitions. PyArrow lacks a disk-spilling shuffle engine, meaning it will aggressively buffer these unpartitioned records into RAM until the OS kills the process. DataFusion inherently monitors its memory consumption; when it approaches the RAM threshold during a heavy sort or group-by phase, it writes intermediate sorted runs to disk and merges them later. This architectural capability allows a single-node PyIceberg instance to handle massive Write/Merge operations safely, scaling vertically via disk capacity rather than being hopelessly bottlenecked by physical memory. Ultimately, migrating from a pure compute library like PyArrow to a fully-fledged execution engine like DataFusion is not just a performance optimization—it is a functional prerequisite for bringing full V2/V3 lifecycle management to PyIceberg.

The main focus for this document, of those issues affected by OOM errors, is that of reading equality delete files. This issue alone leads to inaccurate results when querying a table with equality delete files with PyIceberg. The `DeleteFileIndex` has already been integrated into positional delete files, and (unmerged) work has been done to integrate `DeleteFileIndex` with equality delete files. The way we can integrate DataFusion into the `DeleteFileIndex` is to shift its role from an in-memory materialization index to a lazy execution plan builder. Currently, for positional deletes, the `DeleteFileIndex` maps specific data files to their corresponding delete files so PyArrow can apply row masks. For equality deletes, instead of having PyIceberg immediately read the indexed delete files into a physical PyArrow hashset—which triggers the OOM crash—the `DeleteFileIndex` can be used to construct a DataFusion `LogicalPlan`.

Specifically, when a table scan is initiated, the `DeleteFileIndex` would look up which equality delete files overlap with the target data files based on sequence numbers and partition specs. Instead of loading those delete rows, PyIceberg would pass the file paths of both the data files and the matching equality delete files directly to DataFusion as lazy Parquet scanners. DataFusion then constructs an optimized execution graph, treating the equality delete file as the right-side table in a native `LEFT ANTI JOIN`.

By intercepting the process at the `DeleteFileIndex` stage, we ensure that the evaluation of logical delete conditions (e.g., `account_id = 999`) is entirely offloaded to DataFusion’s multi-threaded, disk-spilling execution engine. The data files and equality delete files are streamed in parallel, and if the volume of equality deletes exceeds physical RAM, DataFusion's Grace Hash Join mechanism automatically partitions and spills the keys to disk. This preserves the engine's zero-copy alignment with the underlying Arrow memory structures while completely eliminating the risk of data inaccuracy or single-node memory exhaustion during the read path.

- Understand DeleteFileIndex  
- Understand the Iceberg-Rust issues/PRs and how they relate, how can it be imported to pyiceberg?  
- Ensure the pyiceberg-core and datafusion-python bridge is hardened in the FFI via TableProvider  
- Don't force opt-in 

| Operation / Topic | Repository | Issue / PR | Status Definition |
| :---- | :---- | :---- | :---- |
| **PyArrow Execution Limits** | iceberg-python | \#3122, \#2676 | **Open:** Highlights PyArrow's worker materialization traps and limitations in streaming massive data without OOMs. |
| **FFI Boundary Stability** | datafusion-python | \#1217 | **Open:** Fatal bus errors/segfaults at the FFI boundary between pyiceberg\_core and DataFusion. Needs ABI hardening. |
| **Execution Path Isolation** | iceberg-python | \#3356 | **Open:** Decoupling pyiceberg-core (DataFusion TableProvider) from standard pyarrow dependencies to prevent bloat. |
| **Rust MoR Primitives** | iceberg-rust | \#2186 | **Open:** Core epic tracking scan-side delete reconciliation and RowDeltaAction native to Rust. Required before DataFusion can process Iceberg MoR tasks. |
| **DataFusion Write Actions** | iceberg-rust | \#2269 | **Open:** Epic tracking native Iceberg writes (MERGE/UPDATE) from Arrow batches through DataFusion to eliminate JVM dependencies. |

- Arrow  
  - [https://github.com/apache/iceberg-python/pull/2676](https://github.com/apache/iceberg-python/pull/2676)  
  - [https://github.com/apache/iceberg-python/pull/1995](https://github.com/apache/iceberg-python/pull/1995)  
  - [https://github.com/apache/iceberg-python/discussions/3122](https://github.com/apache/iceberg-python/discussions/3122)  
  - 

Can the equality delete anti-join be batched via PyArrow?

- [https://www.google.com/search?q=antijoin\&rlz=1C5CHFA\_enUS907US907\&sourceid=chrome\&ie=UTF-8\&amc=1\&aep=42\&cud=0\&qsubts=1780174987358\&source=chrome.crn.rb\&ccb=1\&cs=0\&hl=en-US\&biw=1920\&bih=968\&mstk=AUtExfDeZmJGbBrFFuhtlgCl8EwOwC1iY2OYp31pwGEFBNEG9LnHJN2nWjjR6OP2LKz8b8YAxUiTJ8YD1TEUh0YrHbe-BPYY4USU3yEzhEGgpmo2jRWueeDof1\_mmjXLLVM2m1wdG-rOX00x8o6UJ6ShRKE5ioTTUz019l4gBMCDHIqdCDXPRIkJ48R\_YJAxnODh3EzynjIXJpeI0JS4ZlGF\_xlgx\_Tb4nrXfcmjo3sAsNTtF6q-i0Twi16rxvhGDkt\_zKHOPmyXjp2wsA\&csuir=1\&mtid=jVAbatnVF\_mkkPIPgeH-4As\&udm=50](https://www.google.com/search?q=antijoin&rlz=1C5CHFA_enUS907US907&sourceid=chrome&ie=UTF-8&amc=1&aep=42&cud=0&qsubts=1780174987358&source=chrome.crn.rb&ccb=1&cs=0&hl=en-US&biw=1920&bih=968&mstk=AUtExfDeZmJGbBrFFuhtlgCl8EwOwC1iY2OYp31pwGEFBNEG9LnHJN2nWjjR6OP2LKz8b8YAxUiTJ8YD1TEUh0YrHbe-BPYY4USU3yEzhEGgpmo2jRWueeDof1_mmjXLLVM2m1wdG-rOX00x8o6UJ6ShRKE5ioTTUz019l4gBMCDHIqdCDXPRIkJ48R_YJAxnODh3EzynjIXJpeI0JS4ZlGF_xlgx_Tb4nrXfcmjo3sAsNTtF6q-i0Twi16rxvhGDkt_zKHOPmyXjp2wsA&csuir=1&mtid=jVAbatnVF_mkkPIPgeH-4As&udm=50)

Can we allow spill-to-disk with DataFusion?  
[https://github.com/apache/iceberg-python/pull/2075](https://github.com/apache/iceberg-python/pull/2075)  
[https://github.com/apache/iceberg-python/pull/2928](https://github.com/apache/iceberg-python/pull/2928)  
[https://github.com/apache/datafusion-python/issues/1217](https://github.com/apache/datafusion-python/issues/1217)  
[https://github.com/apache/iceberg-python/issues/3356](https://github.com/apache/iceberg-python/issues/3356)  
[https://github.com/apache/iceberg-rust/issues/1530](https://github.com/apache/iceberg-rust/issues/1530)  
[https://github.com/apache/iceberg-rust/issues/2186](https://github.com/apache/iceberg-rust/issues/2186)  
[https://github.com/apache/iceberg-rust/issues/2201](https://github.com/apache/iceberg-rust/issues/2201)  
[https://github.com/apache/iceberg-rust/issues/2205](https://github.com/apache/iceberg-rust/issues/2205)

# Feature Interactions

Merge-on-Read fundamentally alters the data lifecycle within PyIceberg by decoupling logical row mutations from physical data file rewrites. Because this shift impacts the core read, write, and scan planning paths, the MoR implementation cannot exist in isolation. We must carefully design these changes to integrate smoothly with existing table capabilities and ongoing feature development, particularly in the following areas:

## Retry Commit

Need more test cases for \#3320 for MoR integration.

## Table Maintenance & Compaction

Integrating MoR will rely heavily on the recently implemented whole-table compaction strategy and the metadata-only replace API. As delete files accumulate, read performance degrades. The compaction strategy must be extended to merge these deltas into new base files seamlessly.

## Schema Evolution

Equality deletes rely on specific column schemas. If a column is dropped or renamed, the MoR read path must correctly resolve the schema of the equality delete file against the current table schema.

## Row Lineage

…

## Upsert

…

## Relative Paths

…

# Major Hurdles

What questions are blocking PR’s etc.?  
What can be leapfrogged entirely with V3 DVs?

# Goals

**Comprehensive Read Compatibility (V2 & V3):** Implement read paths for V2 Position/Equality Deletes and V3 Deletion Vectors, allowing PyIceberg to accurately query MoR tables generated by external JVM-based engines.  
**Full V2 MoR Write Support (Position & Equality):** Implement write paths for both V2 Position Deletes and V2 Equality Deletes. **Because many enterprise environments and existing data pipelines (such as Flink and Spark streaming jobs) are currently locked into the V2 specification, PyIceberg must provide full V2 write parity. This ensures PyIceberg can serve as a complete, standalone mutation engine without forcing downstream consumers into premature V3 table upgrades.**  
**V3 MoR Write Support:** Implement parsing and writing for Puffin files and Roaring Bitmaps to support V3 Deletion Vectors (DVs) for highly efficient local row-level mutations.  
**Arrow-Native Execution:** Leverage PyArrow's compute functionalities to apply delete masks efficiently to `RecordBatch` streams without loading full datasets into native Python objects.  
**Delete File Compaction:** Extend the existing whole-table compaction routines to apply existing DVs, Position Deletes, and Equality Deletes, rewriting clean data files in-memory, and use the metadata-only replace API to update table state.

# Non-Goals

**Distributed Compaction:** MoR compaction and maintenance operations will remain scoped to single-node execution. PyIceberg will focus on the single-node memory efficiency of these tasks rather than distributed orchestration.

# Proposed Changes

These proposed changes are derived from the formal mathematical and logical constraints defined in Appendix B: Formal Specification and Implementation Mapping of MoR.  
**Manifest Evaluator Updates:** Modify `DataScan` to identify and fetch associated delete files (Parquet or Puffin) from the manifest lists during the planning phase.  
**Puffin & Roaring Bitmap Integration:** Introduce parsing for Puffin sidecar files using Rust bindings to ensure strict memory safety and optimized evaluation of V3 Deletion Vectors.  
**Delete Application Engine (Read Path):**

* *Position Deletes & DVs:* Create an anti-join or boolean mask filter using PyArrow compute to strip deleted rows based on their file offsets.  
* *Equality Deletes:* Implement a memory-aware PyArrow join that filters rows matching the specified equality column values.

**V2 Position Delete Writer:** Create a specialized writer that buffers `file_path` and `pos` tuples during mutation operations, flushing them to standard Parquet files and registering them correctly in new delete manifests.  
**V2 Equality Delete Writer:** Implement an API for CDC-style logical deletions. This writer will accept PyArrow tables or record batches containing the deleted row values, extract the subset of columns defined by the `equality_ids`, write them to a Parquet delete file, and register the specific `equality_ids` in the manifest metadata.  
**V3 DV Writers:** Implement the logic to serialize deleted row positions into a Roaring Bitmap, wrap it in a Puffin file, and commit it via a new snapshot for V3 tables.

# Open Questions

- **Equality Delete Memory Limits (Read Path):** What is the specific memory threshold or row count limit we should enforce before failing an Equality Delete read operation to prevent single-node OOM errors?  
- **Equality Delete Compaction Heuristics:** Since writing Equality Deletes heavily degrades read performance over time, should the PyIceberg write API trigger warnings, or should we introduce a heuristic threshold that automatically prompts the user to run a compaction job when too many equality delete files accumulate?  
- **Rust Integration Overhead:** For parsing Puffin files and evaluating Roaring Bitmaps, should we implement this purely in Python first for simplicity, or immediately implement Rust bindings to guarantee memory safety and performance from day one?  
- **Write Version Routing:** Should `write.delete.mode` automatically detect the table format version, or should users have explicit override controls to force a specific delete file format?

# References

# Appendix A: Current State and Limitations of CoW in PyIceberg

While Copy-on-Write (CoW) serves as the default write mechanism in PyIceberg, relying on it exclusively presents several challenges that further necessitate the adoption of MoR:

* **Lack of Native Upsert (`MERGE INTO`) Support:** PyIceberg does not yet have a high-level API for seamless upserts. Users attempting CDC or record updates must manually orchestrate the reading of target files, filtering records in PyArrow, and appending/overwriting partitions.  
  * [Merge into / Upsert \#402 \- apache/iceberg-python](https://github.com/apache/iceberg-python/issues/402)  
  * [PR \#1660](https://github.com/apache/iceberg-python/pull/1660)  
  * [Issue during Upsert · Issue \#1759 · apache/iceberg-python](https://github.com/apache/iceberg-python/issues/1759)  
  * [PR \#1830](https://github.com/apache/iceberg-python/pull/1830)  
  * [\[Proposal\] New “update” method for Iceberg tables/transactions. · Issue \#2391](https://github.com/apache/iceberg-python/issues/2391)  
* **Memory & Compute Bottlenecks:** CoW forces the engine to pull entire data files into memory to apply a single row update. In a single-node Python environment, rewriting heavy Parquet files aggressively consumes RAM, frequently leading to OOM crashes during heavy mutation workloads.  
  * [Upsert in PyIceberg: Use Cases, Trade Offs, and Strategy · apache iceberg-python · Discussion \#3118 · GitHub](https://github.com/apache/iceberg-python/discussions/3118)  
* **Concurrency and Commit Conflicts:** Frequent CoW operations on the same table (e.g., from parallel Python workers or agentic processes) lead to high rates of optimistic concurrency control (OCC) failures. Because entire files are being replaced rather than small delta files being appended, the likelihood of concurrent transactions failing to commit is significantly higher without MoR.  
  * [Support IsolationLevels and Concurrency Safety Validation Checks · Issue \#819 · apache/iceberg-python](https://github.com/apache/iceberg-python/issues/819)  
  * [Merge snapshots into 1 under transaction of multiple operations · Issue \#2201 · apache/iceberg-python](https://github.com/apache/iceberg-python/issues/2201)

# Appendix B: Formal Specification and Implementation Mapping of MoR

To ensure the PyIceberg codebase accurately reflects the Apache Iceberg specification, we must define the mathematical invariants of Merge-on-Read. By establishing the formal Reconstitution Operator, we can directly map these logical constraints to our physical PyArrow and Rust execution paths.

#### **1\. The Logical Reconstitution Operator ()**

In a Merge-on-Read architecture, the logical state of a table T at a given snapshot is not simply the sum of its data files. It is the union of all active data files (D) passed through a Reconstitution Operator (), which filters out deleted rows based on the active delete files (F).  
T=d D(d)  
For a single data file d, consisting of rows r, the operator applies the relevant Position Deletes (FP), Equality Deletes (FE), and V3 Deletion Vectors (V):  
(d)=r d | 𝖵𝖺𝗅𝗂𝖽P(r,FP) ⋀ 𝖵𝖺𝗅𝗂𝖽E(r,FE) ⋀ 𝖵𝖺𝗅𝗂𝖽V(r,V) 

#### **2\. V2 Sequence Applicability Invariants**

Evaluating 𝖵𝖺𝗅𝗂𝖽P and 𝖵𝖺𝗅𝗂𝖽E requires strict enforcement of sequence number gating during scan planning. If sequence numbers are not rigorously evaluated, PyIceberg risks either dropping valid data or surfacing deleted records.  
Let 𝖲𝖾𝗊(d) be the sequence number of the data file, and 𝖲𝖾𝗊(f) be the sequence number of the delete file. A delete file only applies to a data file if the sequence constraints are met.

**Position Delete Gating:**  
𝖠𝗉𝗉𝗅𝗒(fPd)𝖲𝖾𝗊(d)𝖲𝖾𝗊(fP)  
Because position deletes target exact file coordinates, they can be safely written in the same transaction as the data file they mutate (e.g., during a batch MERGE INTO).

**Equality Delete Gating:**  
𝖠𝗉𝗉𝗅𝗒(fEd)𝖲𝖾𝗊(d)\<𝖲𝖾𝗊(fE)  
Equality deletes use a strict inequality. Continuous streaming architectures, such as Flink pipelines writing CDC events, frequently emit a new data row and a corresponding equality delete in the exact same transaction. If PyIceberg evaluated this with a  operator, the equality delete would instantly suppress the newly inserted row from the same commit. The strict \< ensures equality deletes only apply to data committed prior to the mutation transaction.

#### **3\. V3 Deletion Vector Evaluation Model**

Under the V3 specification, the evaluation of position deletes shifts from row-by-row coordinate matching to bitwise mask application via Deletion Vectors (DVs) stored in Puffin files.  
If a data file d has an associated Deletion Vector vV, the validity of a row at positional index i is defined strictly by the binary state of the Roaring Bitmap:  
𝖵𝖺𝗅𝗂𝖽V(ri,v)𝖡𝗂𝗍(v,i)=0

#### **4\. Code Mapping & Execution**

Defining MoR mathematically allows us to map the theoretical Reconstitution Operator () directly into our PyIceberg architecture:

* **Scan Planning (The Gating Logic):** The \< and  sequence invariants dictate how DataScan builds its manifest evaluator. Before any data is read, the planner must discard any fP or fE files that fail the sequence invariant for a given data file d.  
* **V2 Equality Execution (Memory-Aware Joins):** The 𝖵𝖺𝗅𝗂𝖽E condition resolves to an anti-join. In PyIceberg, this translates to utilizing PyArrow's compute functions (pc.is\_null(pc.index\_in())) to filter the RecordBatch streams against the loaded equality delete keys.  
* **V3 DV Execution (Rust Bindings):** The 𝖵𝖺𝗅𝗂𝖽V bitwise evaluation requires maximum single-node memory efficiency. Implementing a Rust-backed Roaring Bitmap evaluator allows PyIceberg to parse the Puffin file, extract the bitmask, and yield a dense boolean array. PyArrow's pc.filter then applies this array to the physical data block, achieving O(1) memory overhead compared to V2 Parquet delete file evaluation.

