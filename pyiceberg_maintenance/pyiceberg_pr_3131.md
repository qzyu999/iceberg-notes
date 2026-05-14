# feat: Add metadata-only replace API to Table for REPLACE snapshot operations

**State:** open
**Created by:** @qzyu999
**Created at:** 2026-03-09 22:23:54.000 UTC

<!--
Thanks for opening a pull request!
-->

<!-- In the case this PR will resolve an issue, please replace ${GITHUB_ISSUE_ID} below with the actual Github issue id. -->
Closes #3130

# Rationale for this change
In a current PR (#3124, part of #1092), the proposed `replace()` API accepts a PyArrow dataframe (`pa.Table`), forcing the table engine to physically serialize data during a metadata transaction commit. This couples execution with the catalog, diverges from Java Iceberg's native `RewriteFiles` builder behavior, and fails to register under `Operation.REPLACE`. 

This PR redesigns `table.replace()` and `transaction.replace()` to accept `Iterable[DataFile]` inputs. By externalizing physical data writing (e.g., compaction via Ray), the new explicit metadata-only `_RewriteFiles` SnapshotProducer can natively swap snapshot pointers in the manifests, perfectly inheriting ancestral sequence numbers for `DELETED` entries to ensure time-travel equivalence.

## Are these changes tested?
**Yes.**

Fully exhaustive test coverage has been added to `tests/table/test_replace.py`. The suite validates:
1. Context manager executions tracking valid history growth (`len(table.history())`).
2. Snapshot summary bindings asserting strict `Operation.REPLACE` tags.
3. Accurate evaluation of delta-metrics (added/deleted files and records tracking perfectly).
4. **Low-level serialization:** Bypassed high-level discard filters on `manifest.fetch_manifest_entry(discard_deleted=False)` to natively assert that `status=DELETED` overrides are accurately preserving avro sequence numbers.
5. Idempotent edge cases where `replace([], [])` successfully short-circuits the commit loop without mutating history.

## Are there any user-facing changes?
**Yes.**

The method signature for `Table.replace()` and `Transaction.replace()` has been updated from the original PR #3124. 
It no longer accepts a PyArrow DataFrame (`df: pa.Table`). Instead, it now requests two arguments:
`files_to_delete: Iterable[DataFile]` and `files_to_add: Iterable[DataFile]`, following the convention seen in the Java implementation.

<!-- In the case of user-facing changes, please add the changelog label. -->
*(Please add the `changelog` label)*

## AI Disclosure
AI was used to help understand the code base and draft code changes. All code changes have been thoroughly reviewed, ensuring that the code changes are in line with a broader understanding of the codebase.

- Worth deeper review after AI-assistance:
- The `test_invalid_operation()` in `tests/table/test_snapshots.py` previously used `Operation.REPLACE` as a value to test invalid operations, but with this change `Operation.REPLACE` becomes valid. In place I just put a dummy Operation.
- The `_RewriteFiles` in `pyiceberg/table/update/snapshot.py` overrides the `_deleted_entries` and `_existing_manifests` functions. I sought to test this thoroughly that it was done correctly. I am thinking it's possible to improve the test suite to make this more rigorous. I am open to suggestions on how that could be done.

---

### Comment by @qzyu999 at 2026-03-28 03:13:12.000 UTC

Hi @kevinjqliu, apologies for the delay, thank you so much for taking the time to review the PR again, I understand that you are quite busy. I've addressed all your points in the latest set of tests within 33aaef0817b1366d80bcd8194a0c2ca5ba6f46f2. I've thoroughly expanded the tests to integrate those requirements across a broad set of tests.

There are two minor issues I noticed however:
- Requirement: If the difference is due to prior soft-deletes, confirm those delete files account for it
  - This would require however that the `_RewriteFiles` be scoped to handle Delete Manifests, but currently it's only set to handle Data Files. Handling Delete Manifests would make it so that we could potentially do `REPLACE` operations on deleted files. For example the purpose of this PR is to allow for compaction of data files, but we could in theory also compact delete files for the use case that someone has run many delete operations on many small files.
  - I think this is definitely something to work on, but perhaps not in this PR. The Java code seems to handle this well. I am thinking that after we merge this `REPLACE`, we can next work on the data compaction issue. Then after that we can come back to work on _RewriteFiles for Delete Manifests and work on metadata compaction afterwards.
- Another more minor issue I noticed is that from running the tests and doing `fast_append()` on files that are `DataFileContent.POSITION_DELETES`, they're not yet being labeled properly as `ManifestContent.DELETES`. IIUC this is due to the fact that `_SnapshotProducer._manifests()` (which `fast_append` relies on under the hood) currently defaults to creating standard `ManifestContent.DATA` writers. It doesn't yet inspect the incoming file's content type to route `POSITION_DELETES` into a dedicated `ManifestContent.DELETES` writer. I worked around this in my test by scanning the manifest entries directly rather than relying on the manifest's label, but I just wanted to flag it for the roadmap for when we build out full Merge-on-Read write support.

---

### Comment by @qzyu999 at 2026-04-16 04:22:56.000 UTC

Hi @geruh, thanks for the awesome feedback, I've responded to each of your replies, PTAL.

---

### Comment by @qzyu999 at 2026-04-16 21:29:55.000 UTC

Hi @geruh, thanks again for the helpful feedback, I've responded to each of your review comments and updated the code accordingly, with the exception on the note about naming convention for rewrite/replace as that is pending a response from @kevinjqliu, PTAL.

---

# feat: Add metadata-only replace API to Table for REPLACE snapshot operations

**State:** open
**Created by:** @qzyu999
**Created at:** 2026-03-09 22:23:54.000 UTC

<!--
Thanks for opening a pull request!
-->

<!-- In the case this PR will resolve an issue, please replace ${GITHUB_ISSUE_ID} below with the actual Github issue id. -->
Closes #3130

# Rationale for this change
In a current PR (#3124, part of #1092), the proposed `replace()` API accepts a PyArrow dataframe (`pa.Table`), forcing the table engine to physically serialize data during a metadata transaction commit. This couples execution with the catalog, diverges from Java Iceberg's native `RewriteFiles` builder behavior, and fails to register under `Operation.REPLACE`. 

This PR redesigns `table.replace()` and `transaction.replace()` to accept `Iterable[DataFile]` inputs. By externalizing physical data writing (e.g., compaction via Ray), the new explicit metadata-only `_RewriteFiles` SnapshotProducer can natively swap snapshot pointers in the manifests, perfectly inheriting ancestral sequence numbers for `DELETED` entries to ensure time-travel equivalence.

## Are these changes tested?
**Yes.**

Fully exhaustive test coverage has been added to `tests/table/test_replace.py`. The suite validates:
1. Context manager executions tracking valid history growth (`len(table.history())`).
2. Snapshot summary bindings asserting strict `Operation.REPLACE` tags.
3. Accurate evaluation of delta-metrics (added/deleted files and records tracking perfectly).
4. **Low-level serialization:** Bypassed high-level discard filters on `manifest.fetch_manifest_entry(discard_deleted=False)` to natively assert that `status=DELETED` overrides are accurately preserving avro sequence numbers.
5. Idempotent edge cases where `replace([], [])` successfully short-circuits the commit loop without mutating history.

## Are there any user-facing changes?
**Yes.**

The method signature for `Table.replace()` and `Transaction.replace()` has been updated from the original PR #3124. 
It no longer accepts a PyArrow DataFrame (`df: pa.Table`). Instead, it now requests two arguments:
`files_to_delete: Iterable[DataFile]` and `files_to_add: Iterable[DataFile]`, following the convention seen in the Java implementation.

<!-- In the case of user-facing changes, please add the changelog label. -->
*(Please add the `changelog` label)*

## AI Disclosure
AI was used to help understand the code base and draft code changes. All code changes have been thoroughly reviewed, ensuring that the code changes are in line with a broader understanding of the codebase.

- Worth deeper review after AI-assistance:
- The `test_invalid_operation()` in `tests/table/test_snapshots.py` previously used `Operation.REPLACE` as a value to test invalid operations, but with this change `Operation.REPLACE` becomes valid. In place I just put a dummy Operation.
- The `_RewriteFiles` in `pyiceberg/table/update/snapshot.py` overrides the `_deleted_entries` and `_existing_manifests` functions. I sought to test this thoroughly that it was done correctly. I am thinking it's possible to improve the test suite to make this more rigorous. I am open to suggestions on how that could be done.

---

### Comment by @qzyu999 at 2026-03-28 03:13:12.000 UTC

Hi @kevinjqliu, apologies for the delay, thank you so much for taking the time to review the PR again, I understand that you are quite busy. I've addressed all your points in the latest set of tests within 33aaef0817b1366d80bcd8194a0c2ca5ba6f46f2. I've thoroughly expanded the tests to integrate those requirements across a broad set of tests.

There are two minor issues I noticed however:
- Requirement: If the difference is due to prior soft-deletes, confirm those delete files account for it
  - This would require however that the `_RewriteFiles` be scoped to handle Delete Manifests, but currently it's only set to handle Data Files. Handling Delete Manifests would make it so that we could potentially do `REPLACE` operations on deleted files. For example the purpose of this PR is to allow for compaction of data files, but we could in theory also compact delete files for the use case that someone has run many delete operations on many small files.
  - I think this is definitely something to work on, but perhaps not in this PR. The Java code seems to handle this well. I am thinking that after we merge this `REPLACE`, we can next work on the data compaction issue. Then after that we can come back to work on _RewriteFiles for Delete Manifests and work on metadata compaction afterwards.
- Another more minor issue I noticed is that from running the tests and doing `fast_append()` on files that are `DataFileContent.POSITION_DELETES`, they're not yet being labeled properly as `ManifestContent.DELETES`. IIUC this is due to the fact that `_SnapshotProducer._manifests()` (which `fast_append` relies on under the hood) currently defaults to creating standard `ManifestContent.DATA` writers. It doesn't yet inspect the incoming file's content type to route `POSITION_DELETES` into a dedicated `ManifestContent.DELETES` writer. I worked around this in my test by scanning the manifest entries directly rather than relying on the manifest's label, but I just wanted to flag it for the roadmap for when we build out full Merge-on-Read write support.

---

### Comment by @qzyu999 at 2026-04-16 04:22:56.000 UTC

Hi @geruh, thanks for the awesome feedback, I've responded to each of your replies, PTAL.

---

### Comment by @qzyu999 at 2026-04-16 21:29:55.000 UTC

Hi @geruh, thanks again for the helpful feedback, I've responded to each of your review comments and updated the code accordingly, with the exception on the note about naming convention for rewrite/replace as that is pending a response from @kevinjqliu, PTAL.

---

