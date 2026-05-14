# Nomenclature and Architectural Extensibility: `_RewriteFiles` vs `.replace()`

When reviewing the PyIceberg implementation of the compaction API, a critical architectural decision was made regarding the nomenclature of the internal mechanics versus the public-facing API. Specifically, we implemented an internal `_RewriteFiles` producer class that is exposed to the user via the `table.replace()` and `transaction.replace()` execution methods.

This document outlines the reasoning behind this split, its alignment with the Java Iceberg specifications, and how it lays the critical architectural groundwork for highly-requested, missing PyIceberg maintenance features.

## 1. Action APIs vs. Data Operations

To understand this design, one must understand the strict boundary Apache Iceberg draws between **Actions** and **Operations**:

*   **The Action (What is happening physically):** Iceberg describes the physical process of taking existing files, reading them, and writing them back out in a more optimal layout as *Rewriting*. This is a mechanical filesystem action.
*   **The Operation (What happened logically):** Iceberg describes the metadata result of this action to downstream consumers (like Flink or Spark streaming) as a *Replacement*. It signals: *"The physical files backing this table were replaced, but the logical rows inside remain exactly the same."*

By naming our internal snapshot producer `_RewriteFiles`, we are accurately describing its mechanical purpose: it takes a list of `files_to_delete` and a new payload of data, and rewrites the physical manifest mapping. 

By exposing this to the user via `table.replace()`, we perfectly align the PyIceberg developer experience with the Iceberg Specification. The user is explicitly invoking the `Operation.REPLACE` pipeline, fulfilling the reviewer's request to "create the `.replace()` first".

## 2. Parity with Java Iceberg's Maintenance Architecture

In the Java Iceberg implementation, the `org.apache.iceberg.Transaction` interface exposes a `newRewrite()` method which returns a `RewriteFiles` API object. This object is specifically hardcoded to generate an `Operation.REPLACE` snapshot. 

Our PyIceberg implementation achieves perfect parity with this design:
*   Java: `Transaction.newRewrite()` -> returns `RewriteFiles` -> commits `DataOperations.REPLACE`
*   PyIceberg: `UpdateSnapshot.replace()` -> returns `_RewriteFiles` -> commits `Operation.REPLACE`

We hide the `_RewriteFiles` class as an internal implementation detail, but preserve its mechanical role exactly as Java intended.

## 3. Extensibility: Laying the Groundwork for Missing PyIceberg Features

The introduction of the `_RewriteFiles` pipeline is not just about data compaction; it is the foundational prerequisite for implementing several major maintenance features currently missing from PyIceberg.

### A. RewriteManifests (Metadata Compaction)
Currently, PyIceberg can compact *Data Files*, but it has no mechanism for compacting *Metadata Manifests*. Over time, tables endure "metadata bloat" where hundreds of tiny manifest files drastically slow down query planning. 

In Java Iceberg, `RewriteManifests` is a dedicated maintenance action. It reads all small manifest files and rewrites them into larger, optimized manifest files. **Because this does not alter the logical data of the table, this action also utilizes the `Operation.REPLACE` pipeline.**

By merging the `_RewriteFiles` class and proving the `Operation.REPLACE` commit pathway works seamlessly in PyIceberg, we have built the exact transactional infrastructure required to implement `RewriteManifests` in the near future.

### B. DeleteOrphanFiles
As tables are compacted, rewritten, or aborted midway through transactions, "orphan files" (physical parquet files sitting in the object store that are no longer referenced by any Iceberg snapshot) accumulate and cost money.

Java Iceberg tackles this via the `DeleteOrphanFiles` action. Implementing this in PyIceberg deeply relies on understanding the strict boundaries between physical filesystem actions and logical table state. 

While `DeleteOrphanFiles` doesn't produce an Iceberg snapshot (because it only deletes unreferenced garbage), the `_RewriteFiles` pipeline we built establishes the standard for how PyIceberg separates tracking *physical files* from *logical records*. When we compact data, the old files become "orphans" once their snapshots expire. Building rigorous compaction now ensures our future garbage collection algorithms have mathematically sound snapshot histories to traverse.
