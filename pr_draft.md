Feature: Add metadata-only replace API to Table for REPLACE snapshot operations

Fixes #3130

<!--
Thanks for opening a pull request!
-->

<!-- In the case this PR will resolve an issue, please replace ${GITHUB_ISSUE_ID} below with the actual Github issue id. -->
Closes #3130

# Rationale for this change
Currently, PyIceberg's `replace()` API accepts a PyArrow dataframe (`pa.Table`), forcing the table engine to physically serialize data during a metadata transaction commit. This couples execution with the catalog, diverges from Java Iceberg's native `RewriteFiles` builder behavior, and fails to register under `Operation.REPLACE`. 

This PR redesigns `table.replace()` and `transaction.replace()` to accept `Iterable[DataFile]` inputs. By externalizing physical data writing (e.g., compaction via Ray/Spark), the new explicit metadata-only `_RewriteFiles` SnapshotProducer can natively swap snapshot pointers in the manifests, perfectly inheriting ancestral sequence numbers for `DELETED` entries to ensure time-travel equivalence.

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

The method signature for `Table.replace()` and `Transaction.replace()` has been updated. 
It no longer accepts a PyArrow DataFrame (`df: pa.Table`). Instead, it now requests two arguments:
`files_to_delete: Iterable[DataFile]` and `files_to_add: Iterable[DataFile]`.

*(Please add the `changelog` label)*
