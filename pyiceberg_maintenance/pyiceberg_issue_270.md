# Support metadata compaction

**State:** open
**Created by:** @Fokko
**Created at:** 2024-01-16 12:04:11.000 UTC

### Feature Request / Improvement

Add support for compaction. This rewrites the existing manifests into a single one, reducing the number of calls to the object store. This should follow the Java configuration keys:

- `commit.manifest-merge.enabled`: Controls whether to automatically merge manifests on writes.
- `commit.manifest.min-count-to-merge`: Minimum number of manifests to accumulate before merging.
- `commit.manifest.target-size-bytes`: Target size when merging manifest files.

---

### Comment by @HonahX at 2024-01-30 17:38:28.000 UTC

I am interested in taking this if no one has started working on it.

---

### Comment by @HonahX at 2024-02-27 07:48:07.000 UTC

Based on offline discussion with @Fokko, I will first focus on implementing the `MergeAppend` which supports these keys
- `commit.manifest-merge.enabled`
- `commit.manifest.min-count-to-merge`
- `commit.manifest.target-size-bytes`

The MergeAppend will become the default append method since `commit.manifest-merge.enabled` is default to `True`.
The PR for MergeAppend is https://github.com/apache/iceberg-python/pull/363

BTW, it seems `rewrite_manifest` operations only depends on the `commit.manifest.target-size-bytes`. Shall we update the description to reflect this?


---

### Comment by @github-actions[bot] at 2024-08-26 00:15:50.000 UTC

This issue has been automatically marked as stale because it has been open for 180 days with no activity. It will be closed in next 14 days if no further activity occurs. To permanently prevent this issue from being considered stale, add the label 'not-stale', but commenting on the issue is preferred when possible.

---

### Comment by @github-actions[bot] at 2024-09-09 00:17:17.000 UTC

This issue has been closed because it has not received any activity in the last 14 days since being marked as 'stale'

---

### Comment by @amitgilad3 at 2025-02-06 16:33:22.000 UTC

Hey, was wondering if there are no blockers if i can try to implement rewrite manifests??

---

### Comment by @kevinjqliu at 2025-02-06 22:48:10.000 UTC

sure thing @amitgilad3 
Based on the conversation above, it looks like some of the components are already implemented 

---

### Comment by @ZENOTME at 2025-03-11 11:20:32.000 UTC

Hi, recently I'm trying to investigate support rewrite manifest in iceberg-rust. And the design of iceberg-rust is following iceberg-python, basically, but for now, rewrite manifest is not supported in iceberg-python so I have to refer to the implementation of iceberg-java. In iceberg-java, the rewrite manifest is based on SnapshotProducer and I find that the design of SnapshotProducer between iceberg-java and python is a little different. In iceberg-python, SnapshotProducer is a more "fine grained" abstract, e.g. it provides the summary implementation, `add_data_file` interface. But in iceberg-java, the SnapshotProducer needs the child type to implement the summary. Which means that we can't directly implement rewrite manifest based on SnapshotProducer. In iceberg-python design, I can think of two ways to implement rewrite manifest:
1. Don't base on SnapshotProducer
2. Change the SnapshotProducer design to similar to java, and implement the rewrite manifest based on SnapshotProducer

I'm interested in which design iceberg-python will choice and as a refer for iceberg-rust. 

---

### Comment by @kevinjqliu at 2025-03-11 17:17:41.000 UTC

Hi @ZENOTME thanks for bringing this up. In pyiceberg, `_SnapshotProducer` defines the general structure of "things that are changed to produce a new snapshot." 

The `_DeleteFiles`, `_FastAppendFiles`, and `_OverwriteFiles` follow this pattern. https://grep.app/search?f.repo=apache%2Ficeberg-python&q=%28_SnapshotProducer

I think we can implement metadata compaction by overriding the behaviors of https://github.com/apache/iceberg-python/blob/b86d7d5885c1f9feec86cbffcb818738e41cd6c1/pyiceberg/table/update/snapshot.py#L197-L199

```python
        added_manifests = executor.submit(_write_added_manifest)
        delete_manifests = executor.submit(_write_delete_manifest)
        existing_manifests = executor.submit(self._existing_manifests)
```

---

### Comment by @kevinjqliu at 2025-03-11 17:19:35.000 UTC

Looks like @amitgilad3  has already started a PR for Rewrite manifests in #1661


---

### Comment by @ZENOTME at 2025-03-11 17:39:40.000 UTC

> Looks like @amitgilad3 has already started a PR for Rewrite manifests in https://github.com/apache/iceberg-python/pull/1661

Thanks @kevinjqliu! It's a good reference.

---

### Comment by @kevinjqliu at 2025-03-11 18:31:17.000 UTC

feel free to help review the PR :) i haven't gotten to it yet 

---

### Comment by @zschumacher at 2025-06-05 19:05:37.000 UTC

Are there any updates here?

---

### Comment by @github-actions[bot] at 2025-12-17 00:21:23.000 UTC

This issue has been automatically marked as stale because it has been open for 180 days with no activity. It will be closed in next 14 days if no further activity occurs. To permanently prevent this issue from being considered stale, add the label 'not-stale', but commenting on the issue is preferred when possible.

---

### Comment by @github-actions[bot] at 2025-12-31 00:22:45.000 UTC

This issue has been closed because it has not received any activity in the last 14 days since being marked as 'stale'

---

### Comment by @etolbakov at 2026-01-05 09:44:48.000 UTC

Hi folks,
Could this ticket be reopened, the metadata compaction functionality is very much needed 🙏?
cc @Fokko 

---

### Comment by @Fokko at 2026-01-05 10:55:56.000 UTC

@etolbakov Certainly, thanks for pinging me 👍 

---

