@kevinjqliu Thanks for the detailed review! I completely agree that we should be generating an `Operation.REPLACE` snapshot rather than an `OVERWRITE` for compaction since we are not materially changing the logical dataset. 

I've refactored the compaction run to properly use a `.replace()` API, mirroring the rigorous operation boundaries set by the Java Iceberg implementation. 

To give some more context on the changes in this PR:

### 1. Mirroring Java's `RewriteFiles` Implementation
In the Java Iceberg implementation, `Transaction.newRewrite()` returns a `RewriteFiles` API object that is hardcoded to commit a `DataOperations.REPLACE` snapshot. To achieve exact parity in PyIceberg, I created a new internal producer class called `_RewriteFiles` (inheriting from `UpdateSnapshot`). This internal producer identically handles the physical mechanism of swapping original files out for consolidated ones while guaranteeing the `Operation.REPLACE` property is assigned to the snapshot. I then exposed this pipeline natively to the user via `table.replace()` and `txn.replace()`.

### 2. Code Architecture mimicking `OverwriteFiles`
The architecture for `_RewriteFiles` closely mimics the existing `_OverwriteFiles` logic. By separating the operation into its own class, we avoid contaminating the original `overwrite` branch and correctly adhere to the Action vs Operation split. `table.maintenance.compact()` now simply collects the existing `DataFile` references via a table scan, and passes them cleanly into `with table.transaction() as txn: txn.replace(..., files_to_delete=...)`.

### 3. Laying the Groundwork for Missing PyIceberg Features
By proving out the `Operation.REPLACE` commit pathway via `_RewriteFiles`, this PR lays down the immediate transactional blueprint needed to add two major missing maintenance tasks to PyIceberg in the near future:
* **`RewriteManifests` (Metadata Compaction):** Java Iceberg uses the `REPLACE` pipeline to compact tiny metadata manifests into larger optimized ones since the logic is unchanged. The foundation structure added here enables `RewriteManifests` to be natively supported very soon.
* **`DeleteOrphanFiles`:** As we successfully log soft-deletes (`files_to_delete`) during our compactions, this explicit mapping lays the exact structural dependency needed to eventually run garbage-collection against unreferenced parquet blob files once the `REPLACE` snapshot expires.

Let me know if there's any other feedback you have on this approach!
