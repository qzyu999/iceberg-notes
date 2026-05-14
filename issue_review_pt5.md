# Issue Review Part 5: Applied Architecture and Compaction

To truly understand **why** the `_RewriteFiles` class exists and how `_deleted_entries` and `_existing_manifests` work, we need to transition away from Pythonic inheritance theory and look at a real-world Apache Iceberg Data Lake.

Let's demystify exactly what this Pull Request enables natively by walking through **Data Compaction**.

---

## The Real World Problem: The "Small Files" Issue

Imagine we have an Iceberg table tracking `user_clicks` partitioned by day.

Throughout `2026-03-09`, our streaming ingestion pipeline runs every 5 minutes, appending 100 rows per batch.
Because Iceberg provides ACID transactions, every 5-minute batch creates a brand new tiny `.parquet` file in our S3 bucket.

By the end of the day, we have:
- **288** tiny `.parquet` files for `2026-03-09`.
- **1** massive manifest file tracking all 288 files.

**The Problem:** Querying this table tomorrow will be incredibly slow. The query engine (like Presto, Trino, or duckdb) has to make 288 separate S3 network requests just to read one day of data.

**The Solution:** Compaction. We need to read those 288 tiny files, merge them into 1 big, highly optimized 1GB `.parquet` file, and update the Iceberg table metadata. *We do not want to alter the actual data.*

---

## How `replace()` Solves Compaction

Let's walk through the compaction process and see exactly how the new PyIceberg engine we just built handles it.

### 1. The Compactor (External to this PR)
An external script (like a Ray cluster or an Airflow DAG) runs a `compact()` routine:
1. It queries the Iceberg table for all files in `2026-03-09`. It gets a list of 288 `DataFile` objects.
2. It downloads the 288 files, merges them, and uploads **1 new optimized file** to S3: `s3://bucket/optim_2026-03-09.parquet`.
3. It creates a new `DataFile` object representing this new file in memory.

### 2. The Final Commit (`table.replace()`)
The compactor script must now tell the Iceberg catalog to swap the files.

```python
# The external script executes this shorthand
table.replace(
    files_to_delete=[tiny_file_1, tiny_file_2, ..., tiny_file_288],
    files_to_add=[big_optimized_file_1]
)
```

This triggers our `UpdateSnapshot().replace()` builder inside `pyiceberg/table/update/snapshot.py`. It queues up the 288 tiny files in `self._deleted_data_files` and the 1 big file in `self._added_data_files`.

When the Python context manager exits, `_SnapshotProducer.commit()` is called. 

Now, the engine must write the actual `.avro` Manifest files that define the new Iceberg snapshot. It does this exactly in 3 steps.

---

## Step 1: `_SnapshotProducer` Handles The New Data Automatically

Because `_SnapshotProducer` is a robust parent class, it inherently knows how to handle the `self._added_data_files` list. 

We don't have to code this in `_RewriteFiles`. The producer automatically creates a new manifest (e.g., `snap-4444-added.avro`) that simply says:
- `[ADDED] s3://bucket/optim_2026-03-09.parquet`

---

## Step 2: `_deleted_entries()` Tracks the Ancestry

Iceberg cannot just logically forget the 288 tiny files. 

If we ever want to do a "Time Travel" query to see what the table looked like *yesterday*, those 288 files must still exist in the historical manifests. However, for the *current* state of the table, we must explicitly mark them as `DELETED`.

But here's the catch: We cannot just create fake `[DELETED]` manifest entries. In Iceberg V2, sequence numbers dictate the order of operations. A deleted entry must retain its **exact original sequence number** so the query engines know precisely at what point in history it was replaced.

**This is what `_deleted_entries()` does for us.**

Our overridden `_deleted_entries()` function receives the 288 `tiny_file_x` objects we asked to delete.
1. It opens up the CURRENT snapshot of the table.
2. It searches the manifests looking for the original, ancestral entries corresponding to those 288 files.
3. Once it finds them, it copies their exact `sequence_number`s.
4. It emits 288 copied entries, but explicitly sets their status to `ManifestEntryStatus.DELETED`.

`_SnapshotProducer` takes those 288 modified entries and writes them into a new manifest (e.g., `snap-4444-deleted.avro`).

---

## Step 3: `_existing_manifests()` Saves What We Didn't Touch

Now imagine our Iceberg table also has a manifest tracking `2026-03-08` (yesterday's data). We didn't touch this data during our compaction.

If we blindly built a new snapshot with only the `added.avro` and `deleted.avro` files, we would wipe out all of the data from the rest of the year! A snapshot must point to **all** active data.

**This is what `_existing_manifests()` does for us.**

It evaluates every single manifest active in the table prior to our compression task.
- If it encounters the manifest for `2026-03-08`, it says: *"Wait, none of the 288 deleted files are in here. This manifest is completely clean!"* It simply copies the pointer (the file path) of that manifest directly to the new snapshot, **bypassing any rewrite entirely to save massive amounts of I/O network calls.**
- If it encounters the old manifest for `2026-03-09` that housed the 288 tiny files alongside 2 files we *didn't* delete, it says: *"This manifest is dirty. It contains deleted files."* It forcibly reads the old manifest, filters out the 288 tiny files, and writes a *new* split manifest containing only the 2 surviving files marked as `[EXISTING]`. 

`_SnapshotProducer` takes these salvaged, clean manifests and bundles them with the added and deleted manifests.

---

## The Grand Finale

`_SnapshotProducer` zips `snap-4444-added.avro`, `snap-4444-deleted.avro`, and our salvaged clean manifests into one cohesive `manifest-list.avro`. 

It tags the entire transaction with `Operation.REPLACE`.

The transaction commits to the catalog.

We have successfully compacted 288 tiny files into 1 optimized file, preserved the ancestral sequence numbers for time travel logic, and avoided downloading or rewriting entire gigabytes of untouched historical data. 

**This PR achieves the exact, native Iceberg orchestration engine from Java, ported faithfully into Python.**
