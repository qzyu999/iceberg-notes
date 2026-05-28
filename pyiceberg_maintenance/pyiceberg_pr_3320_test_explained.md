# PyIceberg PR #3320 — Concurrency Validation & Commit Retry: Gap Tests Explained

This document provides a clear, plain-English breakdown of the 16 additional gap tests (**G1–G16**) introduced to verify PyIceberg's concurrency validation and commit retry mechanisms. 

It explains why each test is necessary, how to understand it in layman's terms without complex mathematics, and why each test has its specific risk designation.

---

## High-Risk Gaps (🔴 HIGH PRIORITY)
These gaps cover complex, multi-stage transaction scenarios where properties or states are delegated between different internal builders. Failure in these paths directly leads to silent data corruption, lost updates, or incorrect transaction failures in production environments.

### G7: Dual-Producer Retry in Copy-on-Write (`test_cow_rewrite_retry_refreshes_both_producers`)
* **Why it makes sense to add:** 
  A partial delete (deleting only some rows in a file) is a complex two-stage process. First, it registers a `_DeleteFiles` step to remove the old files, and then it registers an `_OverwriteFiles` step to write the new files containing the surviving rows. Since our new retry loop is designed at the whole-transaction level specifically to orchestrate multiple snapshot updates atomically, we must explicitly test that *both* steps are refreshed and retried together when a concurrent collision occurs.
* **Layman's term analogy:** 
  Imagine you are correcting a draft of a book. The correction involves two steps: ripping out the old page (Delete) and gluing in the corrected page (Overwrite). If someone else edits the book while you are working, you have to restart. This test makes sure you remember to restart *both* steps (ripping out and gluing) on your new attempt, rather than just doing one of them.
* **Risk Designation (🔴 HIGH):** 
  This is the only built-in operation in PyIceberg that queues multiple snapshot builders in a single transaction. If a retry fails to refresh both builders in sequence, it could result in a corrupted table state (e.g., deleting the old file but forgetting to append the rewritten file, leading to silent data loss).

### G8: Isolation Level Propagation in CoW Rewrite (`test_cow_rewrite_inherits_isolation_level_property`)
* **Why it makes sense to add:** 
  When a delete operation triggers a Copy-on-Write rewrite, it creates a separate `_OverwriteFiles` builder. We must ensure that this underlying overwrite builder inherits the correct isolation level property (like `write.update.isolation-level`) from the parent transaction context. If the isolation level property is lost during delegation, the database might fall back to a default level, causing unexpected transaction failures.
* **Layman's term analogy:** 
  If a manager (the delete operation) gives strict instructions to an assistant (the overwrite builder) on how to handle conflicts, the assistant must follow those exact rules. If the instructions are lost when handed off, the assistant will revert to default behavior, potentially rejecting valid work.
* **Risk Designation (🔴 HIGH):** 
  If the isolation property isn't correctly propagated during delegation, rewrite transactions will fall back to the default `serializable` level and fail unnecessarily when concurrent appends occur, defeating the entire purpose of having relaxed snapshot isolation levels.

### G16: Dynamic Partition Overwrite Isolation Routing (`test_dynamic_partition_overwrite_uses_update_isolation_level`)
* **Why it makes sense to add:** 
  Dynamic Partition Overwrites (DPO) replace entire partitions with new data. Because DPO is fundamentally an update/overwrite operation, it should route through the `write.update.isolation-level` property (which defaults to `snapshot` to allow concurrent appends), rather than defaulting to the stricter `write.delete.isolation-level`.
* **Layman's term analogy:** 
  If you are replacing a window in a house (DPO), you want it treated as a home improvement project (relaxed rules allowing other people to paint the walls concurrently) rather than a demolition project (strict rules where no one else is allowed on the property).
* **Risk Designation (🔴 HIGH):** 
  If DPO is routed to the wrong isolation level property, ETL pipelines performing partition overwrites will fail whenever a concurrent append occurs, causing widespread pipeline failures.

---

## Medium-Risk Gaps (🟡 MEDIUM PRIORITY)
These gaps cover the safety limits, timing correctness, and state cleanliness of the retry loop. While they are unlikely to cause data corruption, bugs here can lead to infinite loops, resource leaks, or performance bottlenecks.

### G2: Custom Backoff Parameters (`test_retry_respects_custom_backoff_parameters`)
* **Why it makes sense to add:** 
  Users can configure how long a transaction should sleep before retrying using `commit.retry.min-wait-ms` and `commit.retry.max-wait-ms`. We must verify that the retry loop actually honors these custom bounds instead of ignoring them.
* **Layman's term analogy:** 
  If you tell someone to wait at least 5 minutes before calling you back, they shouldn't call you back after 5 seconds.
* **Risk Designation (🟡 MEDIUM):** 
  If the retry loop ignores wait bounds, it could result in "thundering herd" issues where PyIceberg repeatedly floods a congested catalog with retry requests, causing it to crash or stall.

### G3: Total Timeout Exceeded (`test_total_timeout_terminates_retry`)
* **Why it makes sense to add:** 
  There is a `commit.retry.total-timeout-ms` property that acts as a maximum safety timer. If the cumulative time spent retrying exceeds this threshold, the loop must terminate and raise a clean failure.
* **Layman's term analogy:** 
  If you've been waiting in line at a store for over an hour, you should eventually give up and leave, rather than standing there forever.
* **Risk Designation (🟡 MEDIUM):** 
  Without a working timeout mechanism, a highly congested table could trap a PyIceberg process in an infinite retry loop, hanging production applications indefinitely.

### G6: Resetting Manifest Counters (`test_merge_append_retry_resets_manifest_counter`)
* **Why it makes sense to add:** 
  When `_MergeAppendFiles` prepares a commit, it names its manifest files sequentially using an internal counter. If a commit fails and retries, this counter must be reset to zero so that we do not generate an ever-growing sequence of unnecessary manifest files or cause name collisions.
* **Layman's term analogy:** 
  If you write a draft of a letter, number the pages 1, 2, 3, and then decide to throw the draft away and start over, you should start numbering the new pages from 1 again, not 4.
* **Risk Designation (🟡 MEDIUM):** 
  Failing to reset the counter can lead to file path discrepancies, duplicate files, or orphaned manifest files lying around in object storage (like S3/GCS), cluttering the catalog.

### G11: Exponential Backoff Formula (`test_backoff_bounded_by_max_wait`)
* **Why it makes sense to add:** 
  On each retry attempt, the sleep time should grow exponentially (doubling each time) but must be capped by `max-wait-ms` (plus a small random "jitter" to prevent multiple clients from retrying at the exact same millisecond). We need to verify that this mathematical growth is properly bounded.
* **Layman's term analogy:** 
  If a phone line is busy, you wait 1 minute, then 2, then 4, then 8... but you shouldn't eventually wait 10 hours. You should cap your waiting interval at a reasonable maximum (like 10 minutes) and add a few random seconds of delay so you don't call at the exact same moment as someone else.
* **Risk Designation (🟡 MEDIUM):** 
  An uncapped backoff formula could lead to absurdly long sleep times on later retry attempts, making the application appear frozen.

### G12: Rebuild Idempotency (`test_rebuild_snapshot_updates_is_idempotent`)
* **Why it makes sense to add:** 
  If a transaction fails and retries multiple times, the `_rebuild_snapshot_updates()` method is called repeatedly. We must verify that calling this method multiple times is completely safe and does not double-stage updates or duplicate requirements.
* **Layman's term analogy:** 
  If you tell someone to "clear the table and set it for dinner," doing it twice in a row shouldn't result in two plates and two forks stacked on top of each other. The second run should be a clean, identical setup.
* **Risk Designation (🟡 MEDIUM):** 
  If rebuilding is not clean (idempotent), multiple retries can result in duplicate snapshots, duplicate metadata requirements, or duplicate table updates, leading to invalid metadata commits.

---

## Low-Risk Gaps (🟢 LOW PRIORITY)
These gaps cover edge cases, defensive programming boundaries, and optimization paths. While important for complete code coverage, the risk of production failure or data corruption in these paths is low because they are heavily guarded by surrounding logic.

### G1: Partitioned Append Commutativity (`test_concurrent_append_append_partitioned`)
* **Why it makes sense to add:** 
  Two concurrent appends should never conflict, regardless of whether a table is partitioned or unpartitioned. We already test this on unpartitioned tables; adding it for partitioned tables ensures the partitioning scheme doesn't introduce bugs.
* **Layman's term analogy:** 
  Two delivery drivers arriving at a warehouse to drop off boxes should both succeed, regardless of whether the warehouse is divided into specific aisles (partitioned) or just one big open room (unpartitioned).
* **Risk Designation (🟢 LOW):** 
  The core append logic is shared, so partition-specific bugs here are highly unlikely. It is a good sanity check.

### G4: Concurrent Deletes on Same Partition, Different Rows (`test_concurrent_delete_same_partition_different_rows`)
* **Why it makes sense to add:** 
  Under Copy-on-Write, if two concurrent transactions delete *different* rows that happen to reside in the *same* partition files, they must conflict and reject each other. This is because both operations are trying to rewrite the exact same physical files, and one rewrite would overwrite the other's changes.
* **Layman's term analogy:** 
  If two people are editing the same document at the same time to change different sentences, they cannot both save their work independently without merging, or one will overwrite the other's changes.
* **Risk Designation (🟢 LOW):** 
  The file-level safety checks are already robustly covered elsewhere, so the risk of this failing silently is very low.

### G5: FastAppend Concurrency Validation is a No-Op (`test_fast_append_validate_concurrency_is_noop`)
* **Why it makes sense to add:** 
  Appends do not conflict with anything because they only add new data. Therefore, `_FastAppendFiles._validate_concurrency()` should always be a simple, safe no-op that never throws an exception. We must verify this invariant holds.
* **Layman's term analogy:** 
  Adding a new book to a library shelf (Append) never conflicts with someone else dropping off a book on another shelf—so there should be no security guard stopping you.
* **Risk Designation (🟢 LOW):** 
  This is a defensive guard clause assertion. The chance of a bug here is minimal, but having it explicitly verified keeps the API contracts clean.

### G9: Operations on Empty Table (`test_concurrent_appends_on_empty_table`)
* **Why it makes sense to add:** 
  We must verify that concurrent writes on an brand new table (which has no parent snapshot history yet) do not crash during the conflict-checking phase.
* **Layman's term analogy:** 
  If two people arrive at a newly opened store with no customer history, the checkout counter should handle them both perfectly without crashing because "previous history" is empty.
* **Risk Designation (🟢 LOW):** 
  The codebase has simple guard checks for `None` snapshots, so it is highly unlikely to fail but important for complete coverage.

### G10: Manifest Cleanup with IO Failures (`test_clean_all_uncommitted_with_io_failures`)
* **Why it makes sense to add:** 
  If a transaction aborts and we try to clean up uncommitted manifest files, but our storage system (S3/GCS) experiences a temporary IO warning/failure, that storage warning must not swallow or mask the original transaction failure.
* **Layman's term analogy:** 
  If you drop a glass and it breaks, and then you stub your toe while trying to sweep up the glass, the primary issue is still the broken glass, not the stubbed toe.
* **Risk Designation (🟢 LOW):** 
  Cleanup is a best-effort, logging-only operation. The code wraps these in standard `try-except` blocks, making failures extremely visible but harmless to the main exception flow.

### G13: Mixed Transaction Updates (`test_mixed_updates_not_duplicated_on_retry`)
* **Why it makes sense to add:** 
  If a single transaction does a mix of metadata changes (like setting a table property) and data changes (like appending data), we must verify that on retry, the metadata changes are preserved but *not duplicated* in the transaction's stage list.
* **Layman's term analogy:** 
  If a transaction says "rename the table and append data," retrying after a collision should still result in the table being renamed *once*, not multiple times.
* **Risk Designation (🟢 LOW):** 
  The transaction rebuild filter successfully separates snapshot updates from metadata property updates, so this is a solid sanity check.

### G14: AlwaysFalse Predicate (`test_always_false_predicate_skips_filter_validation`)
* **Why it makes sense to add:** 
  If a delete transaction is run with a filter that matches nothing (represented internally by `AlwaysFalse()`), the conflict detection filter should be ignored, skipping validation checks entirely because nothing is targeted.
* **Layman's term analogy:** 
  If you are told to "go to the store and buy nothing," you don't need to check if the store has items in stock before going.
* **Risk Designation (🟢 LOW):** 
  This is a optimization guard clause that prevents unnecessary database scans when query filters are trivially empty.

### G15: Format Version Gating (`test_format_version_1_skips_delete_file_validation`)
* **Why it makes sense to add:** 
  Table Format Version 1 (V1) does not support Merge-on-Read (MoR) or delete files. We must ensure that V1 tables completely skip all delete-file checks to avoid crashing on older tables.
* **Layman's term analogy:** 
  If an older model car (V1) doesn't have a dashboard screen, the diagnostic tool shouldn't crash trying to check the screen's firmware version; it should just skip the check.
* **Risk Designation (🟢 LOW):** 
  Format version gates are well-established across PyIceberg, making this a highly predictable and low-risk verification.
