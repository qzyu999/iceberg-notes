# Support data files compaction

**State:** open
**Created by:** @sungwy
**Created at:** 2024-08-22 18:21:11.000 UTC

Introduce an API to compact data files. The first version of the API will do the following:
- take a predicate expression as input parameter to find data files matching the filter that will be re-written
- group data files by partitions and rewrite them using the same bin-packing constraints of the writer


---

### Comment by @sungwy at 2024-09-24 17:34:12.000 UTC

Unassigning to work on other near-term priorities

---

### Comment by @github-actions[bot] at 2025-03-24 00:19:41.000 UTC

This issue has been automatically marked as stale because it has been open for 180 days with no activity. It will be closed in next 14 days if no further activity occurs. To permanently prevent this issue from being considered stale, add the label 'not-stale', but commenting on the issue is preferred when possible.

---

### Comment by @zbs at 2025-06-01 05:22:54.000 UTC

Is there any way to trigger compaction? The literature says that it's optimal to compact delete files back into data files to improve read space, and AFAICT there's no way to do this in PyIceberg.

Incidentally, is there a way to control whether your catalog uses copy-on-write vs. merge-on-read?

---

### Comment by @yingjianwu98 at 2025-06-26 21:07:25.000 UTC

@sungwy 

Since my task https://github.com/apache/iceberg-python/issues/1931#issuecomment-3002159502 is depending on the DeleteFileIndex so I am not going to work on it for now until the DeleteFileIndex task is complete.

At the mean time, wondering if I can take this task if you haven't started working on this? Thanks!

---

### Comment by @github-actions[bot] at 2025-12-24 00:21:52.000 UTC

This issue has been automatically marked as stale because it has been open for 180 days with no activity. It will be closed in next 14 days if no further activity occurs. To permanently prevent this issue from being considered stale, add the label 'not-stale', but commenting on the issue is preferred when possible.

---

### Comment by @github-actions[bot] at 2026-01-08 00:22:40.000 UTC

This issue has been closed because it has not received any activity in the last 14 days since being marked as 'stale'

---

### Comment by @qzyu999 at 2026-02-23 20:26:23.000 UTC

Hi @kevinjqliu, is it possible for me to take a look at this issue? Doesn't look like anyone is currently working on it.

CC: @sungwy, I am thinking you can also respond to this

---

### Comment by @kevinjqliu at 2026-02-26 05:04:37.000 UTC

feel free to start to work on it. might be a good idea to outline some ideas before proceeding. 

For example, i think we can model this similar to `rewrite_data_files` https://iceberg.apache.org/docs/nightly/spark-procedures/#rewrite_data_files

---

### Comment by @qzyu999 at 2026-03-03 04:53:42.000 UTC

Hi @kevinjqliu, thanks for the guidance. I've taken a look at the Java/Spark code (referencing v3.5), and I can see that there are quite a few options and nuances to the existing Java implementation. I believe for this issue, the goal is to focus more on some sort of MVP as described by @sungwy. Checking with the existing codebase from the latest iceberg-python main branch, I went ahead and checked what we have and what is missing for a potential MVP.

We already have major components such as:
- `table.scan(filter).plan_files()` (in `pyiceberg/table/__init__.py` for filtering based on a predicate as described in the OP by @sungwy)
- Expression types (in `pyiceberg/expressions`)
- `ListPacker` (in `pyiceberg/utils/bin_packing.py` for bin-packing the files together into manageable sizes)
- various `pyarrow` functions to handle a single-node/python-only MVP setup (located in `pyiceberg/io/pyarrow.py`)
- _OverwriteFiles for the commit (in `pyiceberg/table/update/snapshot.py`)
- MaintenanceTable class (in `pyiceberg/table/maintenance.py`) where we can add a `rewrite_data_files(table, row_filter)` function

I've tested some code locally, and I'm able to do the following:
1. generate a partitioned table with many small files added across multiple snapshots
2. then scan the table given a predicate filter to find all the needed data files
  2a. This also is able to handle the edge case (as seen in `groupByPartition()` from `BinPackRewriteFilePlanner.java`) where old partition specs after schema evolution changes needs to manage where old data files potentially from previous partitions need to be handled as a separate partition group to be rewritten specifically.
3. process each partition (similar to `planFileGroups` in `BinPackRewriteFilePlanner.java`) using constant vars seen in the Java version of `SizeBasedFileRewritePlanner` - this effectively creates the list of all records across partitions that need to be rewritten AND it works in the case of selecting a specific set of rows using `row_filter` rather than automatically rewriting everything
  3a. filter files by size
  3b. bin-pack the files
  3c. filter groups (like in `filterFileGroups` from `BinPackRewriteFilePlanner.java`)
4. create a list of both the old data files and the new data files (based on the bin-packing etc.)
5. create a transaction to 1) delete the old files and 2) append the new files in a single commit

There's one major nuance for this first MVP, which is that there's a `expectedOutputFiles()` in `SizeBasedFileRewritePlanner.java`, where it has an algorithm to handle the "remainder" problem (e.g., we may potentially write 10 large files and 1 small file rather than 10 files where 1 file is slightly larger than in the former case). PyIceberg apparently utilizes a `bin_pack_arrow_table()` within `iceberg-python/pyiceberg/io/pyarrow.py`, which is doing more of a real-time optimization rather than a planned optimization that potentially is more optimal. However, I feel that for this initial MVP it's not needed.

What are your thoughts, is it okay to proceed and create a PR for this?

---

### Comment by @kevinjqliu at 2026-03-05 02:55:49.000 UTC

Thanks for taking the time to look into this @qzyu999! I think this is on the right track. 

Looking at the `rewrite_data_files` implementation in spark, theres a lot of bells and whistles (probably added over time). For the pyiceberg implementation, It might be useful to scope the feature down as much as possible; just to create a harness and we can improve it over time. 

What do you think about first handling the case for compaction of a whole table? That way to don't have to deal with `filter` and matching data files. 

Im thinking something like `table.maintenance.compact()`, which will rewrite the table using the `REPLACE` operation. 
For the actual data files, we can take a shortcut and just binpack by reading the table and writing it out again. This should produce the desired file size specified by `write.target-file-size-bytes` (which the write path already uses) 

WDYT? 

---

### Comment by @qzyu999 at 2026-03-06 05:42:22.000 UTC

Hi @kevinjqliu, thanks for the suggestion, I think you definitely make sense regarding starting simple. I added a PR: 5c8dc67, where I add `compact()`  to the existing `MaintenanceTable` class. I've also added some additional tests. Please let me know what you think about the changes.

Edit: I added a commit, 2774bd3, to address linter issues.

---

