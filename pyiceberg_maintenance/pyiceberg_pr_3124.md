# feat: Add table.maintenance.compact() for full-table data file compaction

**State:** open
**Created by:** @qzyu999
**Created at:** 2026-03-06 05:39:43.000 UTC

Closes #1092

# Rationale for this change
This introduces a simplified, whole-table compaction strategy via the MaintenanceTable API (`table.maintenance.compact()`).

Key implementation details:
- Reads the entire table state into memory via `.to_arrow()`.
  - Note: This initial implementation uses an in-memory Arrow-based rewrite strategy. Future iterations can extend this to support streaming or distributed rewrites for larger-than-memory datasets.
- Uses `table.overwrite()` to rewrite data, leveraging PyIceberg's target file bin-packing (`write.target-file-size-bytes`) natively.
- Ensures atomicity by executing within a table transaction.
- Explicitly sets `snapshot-type: replace` and `replace-operation: compaction` to ensure correct metadata history for downstream engines.
- Includes a guard to safely ignore compaction requests on empty tables.

## Are these changes tested?
Includes full Pytest coverage in `tests/table/test_maintenance.py`.

## Are there any user-facing changes?
Yes. This PR adds a new compact() method to the TableMaintenance API, allowing users to perform file compaction on existing Iceberg tables.

Example usage:
```Python
table = catalog.load_table("default.my_table")
# Merges small files into larger ones based on table properties
table.maintenance.compact()
```
<!-- In the case of user-facing changes, please add the changelog label. -->

Edit: It looks like I'm not able to add the changelog label, hopefully someone with permissions can do so.


---

### Comment by @EnyMan at 2026-03-19 14:51:25.000 UTC

I have been working on similar functionality for a while as part of my upsert optimization efforts. https://github.com/EnyMan/iceberg-python/blob/rewrite-data-files/pyiceberg/table/maintenance.py#L47

```python
    def rewrite_data_files(self) -> RewriteDataFiles:
```, we had used it extensively in our production environment. (10K+ rewrites) It should be basically a clone of the Java version, and I was planning on creating a PR, but I never got to it until now, and now I see there is already some work being done on it. But i use Operation.OVERWRITE operation instead of replace.

---

### Comment by @qzyu999 at 2026-03-19 17:24:49.000 UTC

> I have been working on similar functionality for a while as part of my upsert optimization efforts. https://github.com/EnyMan/iceberg-python/blob/rewrite-data-files/pyiceberg/table/maintenance.py#L47

```python
    def rewrite_data_files(self) -> RewriteDataFiles:
```, we had used it extensively in our production environment. (10K+ rewrites) It should be basically a clone of the Java version, and I was planning on creating a PR, but I never got to it until now, and now I see there is already some work being done on it. But i use Operation.OVERWRITE operation instead of replace.

Hi @EnyMan, thanks for sharing your work! I took a look at your code, IIUC it seems that it's taking the new files and adding them and getting the old files and deleting them, an `Operation.OVERWRITE` as you mentioned. I had done something similarly in the beginning, but I now believe there is a flaw to that from the Java perspective:
- `OVERWRITE` means new data is added to overwrite existing data
- `REPLACE` means files are moved and replaced without changing the data in the table

This has impacts for time travel and conflict resolution.
- If a snapshot is marked as `REPLACE`, the reader knows that the underlying files were strictly restructured (e.g., compacted from 10 small files to 1 large file) but no new logical records were inserted, updated, or deleted. The reader can safely ignore this snapshot.
- If you use `OVERWRITE` for a compaction job, downstream processes may incorrectly perceive the compacted files as new data, potentially leading to duplicate processing.
- During optimistic concurrency control, Iceberg uses the operation type to determine if two concurrent commits conflict. Because `REPLACE` strictly promises no logical changes, Iceberg's commit protocol can often safely re-apply a REPLACE operation alongside other concurrent data modifications (provided the specific files being replaced haven't been deleted).

For reasons that I believe are related to the above examples, @kevinjqliu requested we first implement the `Operation.REPLACE` functionality (#3130, #3131), and then come back to this issue/PR and complete the redesign. I saw that your code seems to have lots of those additional features that exist in Java's compaction function. As mentioned in #1092, the initial version of PyIceberg's can first start with the basic harness and iterate towards the level of completion that your implementation has in future issues/PR's. Following this logic, I believe once #3130 and #1092 are completed, your code would be quite valuable for quickly implementing compaction and adding those additional features to PyIceberg.

* Insights were assisted with AI



---

### Comment by @EnyMan at 2026-03-20 21:55:31.000 UTC

@qzyu999 thanks for the detailed explanation, I now understand why the `REPLACE` is used. Feel free to use any of my code if you want, for future work on the rewrite action, as I can't promise to have the capacity to contribute/engage, but who knows.

---

### Comment by @kevinjqliu at 2026-03-25 23:20:35.000 UTC

Thanks for the details @EnyMan great to see you guys are already running that in production! I see https://github.com/EnyMan/iceberg-python/pull/2, we can use that as a reference for future implementation 😄 

For `REPLACE` vs `OVERWRITE`, `replace` an optimization as described above. Its guaranteeing that no data is changed, only metadata and metadata structure. I think iceberg-go is using `OVERWRITE` for its compaction path. But we can use this opportunity to add `replace` to the codebase

---

