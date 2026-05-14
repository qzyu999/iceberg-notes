# Issue Review Part 4: The Snapshot Producer Lifecycle

The logic flow in PyIceberg's metadata transaction model relies heavily on the **Context Manager Pattern** combined with **Inheritance**. 

At first glance, it appears as though `UpdateSnapshot.replace()` simply returns an empty `_RewriteFiles` class and does nothing. To understand how the transaction actually writes to disk, we must look at how Python executes the `with` statement blocks and the hidden machinery inside the `_SnapshotProducer` base class.

Here is a step-by-step breakdown of exactly how `table.replace()` becomes a committed Iceberg Snapshot.

---

### Step 1: Initiating the Context Manager

When you call `table.replace()`, it forwards the execution to `transaction.replace()`. Inside the transaction, we see this block:

```python
with self.update_snapshot().replace() as replace_snapshot:
    for file_to_delete in files_to_delete:
        replace_snapshot.delete_data_file(file_to_delete)

    for data_file in files_to_add:
        replace_snapshot.append_data_file(data_file)
```

1. **`update_snapshot()`** returns an `UpdateSnapshot` factory object.
2. **`.replace()`** instantiates and returns exactly one object: our new `_RewriteFiles` class instance.
3. The `with` statement calls the `__enter__()` method on the `_RewriteFiles` object. Because `_RewriteFiles` inherits from `_SnapshotProducer`, it inherits this method cleanly. At this stage, `replace_snapshot` is assigned to be our newly instantiated `_RewriteFiles` object.

### Step 2: The Staging Phase (Builder Pattern)

Inside the `with` block, we execute the iteration.

```python
replace_snapshot.delete_data_file(file_to_delete)
replace_snapshot.append_data_file(data_file)
```

These methods are **not** defined in `_RewriteFiles`. They are inherited from `_SnapshotProducer`. 

If you look inside `_SnapshotProducer`, you will see what these methods do:
```python
# From pyiceberg.table.update.snapshot._SnapshotProducer
def delete_data_file(self, data_file: DataFile) -> _SnapshotProducer[U]:
    self._deleted_data_files.add(data_file)
    return self

def append_data_file(self, data_file: DataFile) -> _SnapshotProducer[U]:
    self._added_data_files.append(data_file)
    return self
```

**What happens here? Absolutely nothing is written to disk.** 
The snapshot producer is acting as a "Builder". It is merely accumulating the `DataFile` references into standard Python `set`s and `list`s stored in its memory (`self._deleted_data_files` and `self._added_data_files`).

### Step 3: Triggering the Execution (The `__exit__` Method)

The magic happens the moment the `with` block completes. 

In Python, exiting a `with` block automatically triggers the object's `__exit__()` method. Because `_RewriteFiles` inherits from `_SnapshotProducer`, it executes `_SnapshotProducer`'s `__exit__` method.

Let's look at `_SnapshotProducer`'s `__exit__`:

```python
# From pyiceberg.table.update.snapshot._SnapshotProducer
def __exit__(self, ...) -> None:
    self.commit()
```
The `__exit__` method immediately calls `self.commit()`.

### Step 4: The Core Engine (`self.commit()`)

Inside `_SnapshotProducer`, the `self.commit()` method is an orchestrator. It calls several internally constructed methods to build the actual Iceberg snapshot. 

One of the first things `commit()` does is call `self._manifests()`.

```python
# Inside _SnapshotProducer
def _manifests(self) -> list[ManifestFile]:
    # 1. Writes a new manifest containing EVERYTHING in self._added_data_files
    added_manifests = executor.submit(_write_added_manifest) 
    
    # 2. Asks the subclass for Deleted Entries and writes a delete manifest
    delete_manifests = executor.submit(_write_delete_manifest)  
    
    # 3. Asks the subclass what existing files to keep
    existing_manifests = executor.submit(self._existing_manifests) 

    return added_manifests.result() + delete_manifests.result() + existing_manifests.result()
```

### Step 5: Polymorphism Executes Our Custom Code

Notice in Step 4 that `_SnapshotProducer` needs to know:
- Which entries are deleted? `_write_delete_manifest` searches for them by calling `self._deleted_entries()`.
- Which existing manifests should be kept? It calls `self._existing_manifests()`.

**This is why `_RewriteFiles` only implements `_deleted_entries()` and `_existing_manifests()`!**

Because `_RewriteFiles` overrides these two abstract methods, when `_SnapshotProducer` tries to build the final list of manifests during the `.commit()` cycle, Python invokes our custom logic.

1. `_SnapshotProducer` takes all the files we passed to `append_data_file` and writes them into a brand new `added_data_files.avro` manifest automatically.
2. It then calls our `_RewriteFiles._deleted_entries()`. Our custom code uses the files we passed to `delete_data_file` to search the previous snapshot, find the exact ancestral `ManifestEntry` items, copy their sequence numbers, and return them marked as `DELETED`.
3. It calls our `_RewriteFiles._existing_manifests()`. Our custom code safely copies over all the old manifests, while aggressively scrubbing out the deleted entries to ensure the table stays clean.
4. `_SnapshotProducer` gathers all three of these newly generated manifest-lists.

### Step 6: The Final Transaction Commit

Once `_SnapshotProducer` has the complete set of new/updated manifests, it wraps them into a `Snapshot` object, evaluates the delta metric summaries (where it will see `Operation.REPLACE`), and binds this new `Snapshot` to the `self._transaction` we passed into its constructor.

Finally, when the overarching outermost `with self.transaction() as tx:` block concludes, the transaction pushes the new snapshot pointer to the Iceberg Catalog (e.g., AWS Glue, REST, or SQL).

### Summary

The `_RewriteFiles` class appears empty because it is tightly coupled to the **Context Manager Lifecycle**.
1. `table.replace()` creates the class.
2. The `with` block queues up lists of `DataFile`s in memory.
3. Exiting the `with` block triggers a massive chain reaction via `__exit__ -> commit -> _manifests`.
4. The base class automatically writes the newly queued appended files.
5. The base class invokes our custom overridden functions (`_deleted_entries` and `_existing_manifests`) to correctly stitch together the exact historical Iceberg layout.
