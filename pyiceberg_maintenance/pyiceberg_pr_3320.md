# Add commit retry and concurrency validation for writes

**State:** open
**Created by:** @lawofcycles
**Created at:** 2026-05-03 09:17:58.000 UTC

Closes #3319
Closes #819
Closes #269

# Rationale for this change

PyIceberg currently fails immediately with `CommitFailedException` when a concurrent transaction commits first, regardless of whether the writes actually conflict. Java Iceberg handles this transparently through its retry loop in `SnapshotProducer.commit()`.

This PR adds automatic commit retry with exponential backoff and data conflict validation to PyIceberg, matching Java Iceberg's behavior. On `CommitFailedException`, the retry loop refreshes table metadata, re-runs validation, and regenerates manifests. If validation detects a real data conflict, the operation aborts with `ValidationException` instead of retrying.

The retry loop is placed in `Transaction.commit_transaction()` rather than in individual snapshot producers. This is necessary because `Transaction.delete()` uses two producers (`_DeleteFiles` + `_OverwriteFiles`) that must be committed atomically. Retrying at the producer level would break this atomicity.

Validation behavior follows Java's `BaseOverwriteFiles.validate()`, using the existing validation functions from `validate.py` that were contributed through #1935, #1938, #2050, and #3049.

## Are these changes tested?
Yes. Unit tests and integration tests covering retry success, `ValidationException` abort, retry exhaustion, isolation levels, partition-level conflict detection, manifest cleanup, and producer state reset.

## Are there any user-facing changes?
Yes. Previously, all concurrent write conflicts resulted in `CommitFailedException`.

 Now:
- Compatible concurrent writes (e.g. concurrent appends) are retried automatically and succeed transparently
- Incompatible concurrent writes (e.g. concurrent deletes on the same data) raise `ValidationException` instead of `CommitFailedException`

The following new table properties are supported.
- `commit.retry.num-retries` (default: 4)
- `commit.retry.min-wait-ms` (default: 100)
- `commit.retry.max-wait-ms` (default: 60000)
- `write.delete.isolation-level` (default: serializable)
- `write.update.isolation-level` (default: serializable)

---

### Comment by @lawofcycles at 2026-05-04 11:38:41.000 UTC

## Benchmark results

This PR brings three capabilities to PyIceberg's write path.

1. Transparent retry for concurrent writes. Users no longer need to implement retry logic around `table.append()` or `table.delete()`.
2. Data conflict validation. Incompatible concurrent modifications (e.g. concurrent deletes on the same data) are detected and rejected with `ValidationException`, preventing silent data corruption.
3. Efficient retry via data file reuse. On retry, only manifests are regenerated. Data files already written to S3 are reused, avoiding redundant Parquet writes.

To validate (3), I benchmarked concurrent appends using the NYC Yellow Taxi dataset (2024-01, 2.9M rows, 19 columns) with Glue Data Catalog + S3.

### Before vs After

Without this PR, concurrent appends fail immediately with `CommitFailedException`. Only one writer succeeds per batch, regardless of parallelism.

| Workers | Before (no retry) | After (this PR) |
|--------:|-------------------:|----------------:|
|       2 |              50.0% |          100.0% |
|       4 |              25.0% |          100.0% |
|       8 |              12.5% |          100.0% |

(N workers x 10 batches x 1K rows, `commit.retry.num-retries=10`, `commit.retry.min-wait-ms=500`)

### Internal retry vs user-side retry

Compared the internal retry (this PR) against a user-side retry that catches `CommitFailedException` and re-does `load_table` + `append` from scratch. Both use the same backoff parameters (retries=15, min-wait=500ms).

| Workers | Internal retry | User-side retry | Speedup |
|--------:|---------------:|----------------:|--------:|
|       2 |            33s |             46s |    1.4x |
|       4 |            68s |             87s |    1.3x |
|       8 |           167s |            299s |    1.8x |
|      16 |           399s |            588s |    1.5x |

(3 batches per worker, ~370K-1.5M rows per batch depending on worker count)

Internal retry is faster because it reuses data files already written to S3 and only regenerates manifests on retry. User-side retry rewrites Parquet files on every attempt.

Interestingly, internal retry actually performs *more* retries than user-side retry (88 vs 50 total retries at 8 workers), because the shorter retry window increases commit attempt density. Despite more retries, the total time is lower because each retry is much cheaper.

### Tuning `commit.retry.min-wait-ms`

Tested different `min-wait-ms` values with 8 workers to find the optimal backoff for Glue.

| min-wait-ms | Total time | Total retries |
|------------:|-----------:|--------------:|
|         100 |       158s |            78 |
|         500 |       126s |           115 |
|        1000 |       238s |            67 |
|        2000 |       235s |            63 |
|        3000 |       206s |            41 |

The default (100ms, matching Java Iceberg) works reasonably well, but 500ms is optimal for Glue. Too short causes contention storms, too long wastes time waiting. The optimal value depends on the catalog's commit latency.

---

