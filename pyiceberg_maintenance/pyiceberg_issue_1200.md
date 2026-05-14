# Delete orphan files

**State:** open
**Created by:** @sungwy
**Created at:** 2024-09-24 16:03:45.000 UTC

Introduce a new API to delete orphan files for a given table

Feature reference: https://iceberg.apache.org/docs/1.5.1/maintenance/#delete-orphan-files

---

### Comment by @omkenge at 2024-10-29 17:53:54.000 UTC

Hi @sungwy 
I would like to work on this ..
Can I ?

---

### Comment by @sungwy at 2024-10-29 18:58:32.000 UTC

Hey sure thing! I'll assign it to you @omkenge 

---

### Comment by @omkenge at 2024-11-21 18:28:29.000 UTC

`Orphan File Deletion in Iceberg Tables`
Here's a step-by-step breakdown of the logic behind the process:
1. List All Files in Storage
2. Extract Referenced Files from Table Metadata
3. Identify Orphan Files
By comparing the list of all files in storage with the list of files referenced by the Iceberg table, the script identifies orphan files.
These are files that exist in storage but are not part of the current table metadata.
The comparison is performed by subtracting the set of referenced files from the set of all files in storage.
4. Delete Orphan Files

What is your opinion on this ?
@kevinjqliu @Fokko @sungwy 

---

### Comment by @kevinjqliu at 2024-11-23 20:10:18.000 UTC

That looks generally correct to me. There are a few caveats though. This assumes that the entire iceberg table (metadata and data files) is in a single location and that no other files should exist. 

I think a good first step is to figure out all the files belonging to an iceberg table. Given a table, return all metadata and data file paths, including historical lineage, branches, and tags.

---

### Comment by @ndrluis at 2024-11-24 22:05:25.000 UTC

@omkenge I believe you will need to wait for the merge of #1285. In the meantime, I will work on the partition statistics over the next few weeks. Before that, I believe we will be tracking all the files in the metadata (this needs to be double-checked). With that, you will be able to verify what could be removed.

Another point is the filesystem that will be responsible for scanning the directory. FileIO is not how we solve this, so we will need to use something else. Perhaps OpenDAL would be a good candidate. As a reference, you can see that the [Java implementation uses the Hadoop filesystem](https://github.com/apache/iceberg/blob/main/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/DeleteOrphanFilesSparkAction.java#L356

```java
    actualFileIdentDS.sparkSession().sparkContext().register(conflicts);
```).

---

### Comment by @omkenge at 2025-01-20 06:03:16.000 UTC

Hello @ndrluis 
I think #1285  is now merged can I start working on this issue.

---

### Comment by @ndrluis at 2025-01-20 11:51:22.000 UTC

Hello @omkenge, you can start development, but please note that we need the partition statistics. I'll start working on this feature this week. The merge for the orphan files removal implementation will be blocked until we have these statistics, but you can begin the development work.

---

### Comment by @omkenge at 2025-01-28 18:39:59.000 UTC

Hello @ndrluis @kevinjqliu 
Could you plz help me on OpenDal how we can use and integrate this. It will very  helpfull for me.
and another thing I just extract the data file from snapshot with 
`metrics=table.inspect.files()`
`file_paths = metrics.column("file_path").to_pylist()`
is this correct way 

---

### Comment by @ndrluis at 2025-01-31 16:58:01.000 UTC

Hi @omkenge, I don’t have direct experience with OpenDAL, but my suggestion is based on how [iceberg-rust is currently using it](https://github.com/search?q=repo%3Aapache%2Ficeberg-rust%20opendal&type=code).

For the implementation, I’d recommend aligning with the Java implementation as a reference. Check out these two key files:

[DeleteOrphanFilesSparkAction.java](https://github.com/apache/iceberg/blob/main/spark/v3.3/spark/src/main/java/org/apache/iceberg/spark/actions/DeleteOrphanFilesSparkAction.java)
[RemoveOrphanFilesProcedure.java](https://github.com/apache/iceberg/blob/main/spark/v3.3/spark/src/main/java/org/apache/iceberg/spark/procedures/RemoveOrphanFilesProcedure.java)

---

### Comment by @Fokko at 2025-02-03 09:56:16.000 UTC

I think we want to avoid depending directly on OpenDal, since that's another dependency. FileIO officially doesn't support listing of directories because listing of a directory doesn't perform well on object stores. This will result in a paged response that potentially has a lot of pages.

A catalog might provide a more powerful way of cleaning up orphan files by leveraging [S3 Inventory lists](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html), but I don't think that's a good implementation for the client itself. Similar to the Java implementation where we rely on the underlying filesystem, I think we can do something similar in PyIceberg by using the [Arrow FileSystem to list the files](https://arrow.apache.org/docs/python/filesystems.html#listing-files).

---

### Comment by @omkenge at 2025-02-06 16:51:14.000 UTC

Hello @Fokko 
Here is the small Implementation 
1. List Data Files in S3
We use PyArrow’s S3FileSystem to retrieve file paths from the given table location:
      
      
        def list_data_files_from_table(table_location: str) -> set:
            if not table_location.startswith("s3://"):
                raise ValueError("Table location must start with 's3://'")
        
            base = table_location.rstrip("/")
            data_location = f"{base}/data" if not base.endswith("/data") else base
        
            s3 = fs.S3FileSystem(
                region="eu-central-1",
                endpoint_override="127.0.0.1:9000",
                access_key="admin",
                secret_key="password",
                scheme="http"
            )
        
            bucket, prefix = data_location[5:].split("/", 1)
            selector = fs.FileSelector(f"{bucket}/{prefix}", recursive=True)
            
            file_infos = s3.get_file_info(selector)
            return {f"s3://{info.path}" for info in file_infos if info.type == fs.FileType.File}
2. Extract Metadata-Tracked Files
Using PyIceberg, we retrieve file paths stored in the table metadata:
  ```
def extract_metadata_files(table) -> set:
      metadata_table = table.inspect.files()
      return set(metadata_table.column("file_path").to_pylist())

```
3. Identify Orphan Files
```
def find_orphan_files(table_location, table):
    s3_files = list_data_files_from_table(table_location)
    metadata_files = extract_metadata_files(table)
    
    orphan_files = s3_files - metadata_files  # Files in S3 but not in metadata
    return orphan_files
```



---

### Comment by @kevinjqliu at 2025-02-06 22:43:05.000 UTC

> Extract Metadata-Tracked Files

we might want to use all_files and all_metadata_files. `files` only gets the data files for the current snapshot

---

### Comment by @omkenge at 2025-02-21 12:55:29.000 UTC

@kevinjqliu 
I tried this works for me 
But I do not know how to create s3 file system or how to support other file storage in same logic .Could you plz help me on this 
```
import os
import pyarrow.fs as fs
from pyiceberg.catalog import load_catalog

def list_files_from_table_subdir(table_location: str, subdir: str) -> set:
    """
    List all files under the specified subdirectory (e.g. "data" or "metadata")
    for a table whose location is an S3 URI.
    
    Args:
        table_location (str): The base table location (must start with "s3://").
        subdir (str): The subdirectory to list.
    
    Returns:
        set: A set of S3 URIs for all files under the subdirectory.
    """
    if not table_location.startswith("s3://"):
        raise ValueError("Table location must start with 's3://'")
    
    # Remove any trailing slash and ensure the full location points to the subdir.
    base = table_location.rstrip("/")
    full_location = base if base.endswith(f"/{subdir}") else f"{base}/{subdir}"
    
    # Create an S3FileSystem with the required credentials.
    s3 = fs.S3FileSystem(
         region="eu-central-1",
         endpoint_override="127.0.0.1:9000",
         access_key="admin",
         secret_key="password",
         scheme="http"
    )
    
    # Remove the "s3://" prefix and split into bucket and prefix.
    bucket, prefix = full_location[5:].split("/", 1)
    
    # List files recursively under the given prefix.
    selector = fs.FileSelector(f"{bucket}/{prefix}", recursive=True)
    file_infos = s3.get_file_info(selector)
    
    # Return the full S3 URI for each file.
    return {f"s3://{info.path}" for info in file_infos if info.type == fs.FileType.File}

def extract_all_metadata_files(table) -> set:
    """
    Extract all metadata-related files from an Iceberg table by combining:
      - Files from the snapshot inspection (column "file_path")
      - Metadata log files (column "file")
      - Manifest list files from snapshots (column "manifest_list")
    
    Args:
        table: An Iceberg table object.
        
    Returns:
        set: A set of all metadata file paths.
    """
    # Extract file paths from the current snapshot.
    metadata_table = table.inspect.files()
    table_files = set(metadata_table.column("file_path").to_pylist())

    # Extract metadata log files.
    metadata_manifest = table.inspect.metadata_log_entries()
    manifest_files = set(metadata_manifest.column("file").to_pylist())

    # Extract manifest list files from snapshots.
    metadata_snapshot = table.inspect.snapshots()
    snapshot_manifests = set(metadata_snapshot.column("manifest_list").to_pylist())

    # Combine all sets into one.
    all_metadata_files = table_files.union(manifest_files).union(snapshot_manifests)
    return all_metadata_files

def collect_table_files(table) -> list:
    """
    Collect files from the table's base location by listing both the "metadata"
    and "data" subdirectories, then combine them in a single list.
    
    Args:
        table: An Iceberg table object with .location().
    
    Returns:
        list: A list of S3 URIs for all metadata and data files.
    """
    base_location = table.location()
    
    # List files in the "metadata" subdirectory.
    metadata_files = list_files_from_table_subdir(base_location, "metadata")
    
    # List files in the "data" subdirectory.
    data_files = list_files_from_table_subdir(base_location, "data")
    
    # Combine both sets into a single list.
    all_files = list(metadata_files.union(data_files))
    return all_files

def find_orphan_files(table_location: str, table) -> set:
    """
    Identify orphan files that exist in the S3 "data" subdirectory but are not
    referenced in the snapshot's metadata.
    
    Args:
        table_location (str): The base table location (must start with "s3://").
        table: An Iceberg table object.
    
    Returns:
        set: A set of orphan file URIs.
    """
    # List data files from the S3 "data" subdirectory.
    s3_data_files = list_files_from_table_subdir(table_location, "data")
    
    # Extract metadata files from the current snapshot.
    metadata_files = extract_all_metadata_files(table)
    
    # Orphan files are those in S3 data that are not present in the metadata.
    orphan_files = s3_data_files - metadata_files
    return orphan_files

def delete_orphan_files(table, dry_run=True) -> set:
    """
    Delete orphan files from the table's S3 "data" subdirectory.
    
    If dry_run is True, only prints the files that would be deleted without
    actually deleting them.
    
    Args:
        table: An Iceberg table object.
        dry_run (bool): Whether to perform a dry run.
    
    Returns:
        set: The set of orphan file URIs that were (or would be) deleted.
    """
    table_location = table.location()
    orphan_files = find_orphan_files(table_location, table)
    
    if dry_run:
        print("Dry Run: The following orphan files would be deleted:")
        for file_uri in orphan_files:
            print(file_uri)
    else:
        s3 = fs.S3FileSystem(
            region="eu-central-1",
            endpoint_override="127.0.0.1:9000",
            access_key="admin",
            secret_key="password",
            scheme="http"
        )
        for file_uri in orphan_files:
            relative_path = file_uri[5:]
            try:
                s3.delete_file(relative_path)
                print(f"Deleted {file_uri}")
            except Exception as e:
                print(f"Failed to delete {file_uri}: {e}")
    return orphan_files


if __name__ == '__main__':
    # Configure the catalog using your S3 settings.
    catalog = load_catalog(
        "local",
        **{
            "uri": "http://127.0.0.1:8181",
            "s3.endpoint": "http://127.0.0.1:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.region": "eu-central-1",
            "s3.path-style-access": "true",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )
    
    # Load an existing table.
    table = catalog.load_table("om.students")
    table_location = table.location()
    
    # Collect and combine files from both metadata and data subdirectories.
    combined_files = collect_table_files(table)
    print("Combined files:")
    print(combined_files)
    
    # Extract all metadata files (snapshot, metadata log, manifest list).
    metadata_files = extract_all_metadata_files(table)
    print("\nExtracted metadata files:")
    print(metadata_files)
    
    # Identify orphan files (files in S3 data not referenced in metadata).
    orphan_files = find_orphan_files(table_location, table)
    print("\nOrphan files:")
    print(orphan_files)
    
    # Execute the delete orphan files action in dry-run mode.
    print("\nExecuting Delete Orphan Files (Dry Run):")
    delete_orphan_files(table, dry_run=True)
    
    # To actually delete the orphan files, set dry_run=False.
    # Uncomment the line below to perform actual deletion.
    # delete_orphan_files(table, dry_run=False)

```

---

### Comment by @kevinjqliu at 2025-03-04 00:12:45.000 UTC

> But I do not know how to create s3 file system or how to support other file storage in same logic .Could you plz help me on this

take a look at `load_file_io` and how its used 
https://github.com/apache/iceberg-python/blob/5c68ad81d144f6ab1855807fd3a133e944f9b0a6/pyiceberg/io/__init__.py#L340

```python
def load_file_io(properties: Properties = EMPTY_DICT, location: Optional[str] = None) -> FileIO:
```

---

### Comment by @jayceslesar at 2025-04-29 18:54:12.000 UTC

Looks like the following will also work directly from a table object:

```py
from pyiceberg import catalog
from pyarrow.fs import FileSelector
#
CATALOG = catalog.load_catalog(**{"type": "glue"})

table = CATALOG.load_table("my_table_name")

scheme, netloc, path = table.io.parse_location(table.location())
fs = table.io.fs_by_scheme(scheme, netloc)
selector = FileSelector(path, recursive=True)
files = fs.get_file_info(selector)
print(files)
```

Edit: 
~~Note that it does take a LOOOOOOONG time if your table has many files~~

Actually not that bad, one of my iceberg tables has ~1m files and it took just around 4mins for this method to recursively capture everything in that directory


I believe that this is platform agnostic?


Basically can just get the difference in that output against (all_manifests + files for every snapshot)

Realistically it makes sense to make a new method on the `InspectTable` called `all_files` or whatever that combines every path we can find in the manifests as well as the files call for every snapshot

---

### Comment by @github-actions[bot] at 2026-03-17 00:27:42.000 UTC

This issue has been automatically marked as stale because it has been open for 180 days with no activity. It will be closed in next 14 days if no further activity occurs. To permanently prevent this issue from being considered stale, add the label 'not-stale', but commenting on the issue is preferred when possible.

---

