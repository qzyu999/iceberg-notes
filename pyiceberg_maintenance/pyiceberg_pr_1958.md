# feat: delete orphaned files

**State:** closed
**Created by:** @jayceslesar
**Created at:** 2025-04-29 22:42:05.000 UTC

Closes #1200 

# Rationale for this change
Ability to do more table maintenance from pyiceberg (iceberg-python?)

# Are these changes tested?
Added a test!

# Are there any user-facing changes?
Yes, this is a new method on the `Table` class.


---

### Comment by @kevinjqliu at 2025-05-04 01:21:41.000 UTC

a meta question, wydt of moving the orphan file function to its own file/namespace, similar to how to use `.inspect`. 

i like the idea of having all the table maintenance functions together, similar to delta table's [optimize](https://delta-io.github.io/delta-rs/api/delta_table/#deltalake.DeltaTable.optimize)

---

### Comment by @jayceslesar at 2025-05-04 16:53:12.000 UTC

> a meta question, wydt of moving the orphan file function to its own file/namespace, similar to how to use `.inspect`.
> 
> i like the idea of having all the table maintenance functions together, similar to delta table's [optimize](https://delta-io.github.io/delta-rs/api/delta_table/#deltalake.DeltaTable.optimize)


I think that makes sense -- would https://github.com/apache/iceberg-python/pull/1880 end up there too?

Also ideally there is a CLI that exposes all the maintenance actions too right?

I think moving things to a new `OptimizeTable` class in a new namespace `optimize.py` makes a lot of sense, can be modeled very similar to the `InspectTable` and generally makes things cleaner -- I think it still makes sense to have the `all_known_files` inside of inspect though, and can still use that in the new `OptimizeTable`

---

### Comment by @Fokko at 2025-05-13 14:42:37.000 UTC

> i like the idea of having all the table maintenance functions together, similar to delta table's [optimize](https://delta-io.github.io/delta-rs/api/delta_table/#deltalake.DeltaTable.optimize)

That's a good point. However, I think we should be able to either run them separate as well. For example, delete orphan files won't affect the speed of the table, so it is more of a maintenance feature to reduce object storage costs. Delete orphan files can also be pretty costly because of the list operation, ideally you would delegate this to the catalog that uses, for example, s3 inventory.

---

### Comment by @jayceslesar at 2025-06-24 12:35:57.000 UTC

@Fokko we probably also want pyiceberg to have some idea about https://iceberg.apache.org/spec/#delete-formats right? Is it currently aware of those files?

---

### Comment by @Fokko at 2025-06-24 14:44:08.000 UTC

@jayceslesar I believe the merge-on-read delete files (positional deletes, equality deletes, and deletion vectors) are returned by the all-files. The only part that's missing is the partition statistics files.

---

### Comment by @jayceslesar at 2025-06-24 15:35:22.000 UTC

> @jayceslesar I believe the merge-on-read delete files (positional deletes, equality deletes, and deletion vectors) are returned by the all-files. The only part that's missing is the partition statistics files.

Sounds good, I will add the partition statistics files when that is merged!

---

### Comment by @aammar5 at 2025-07-10 15:30:08.000 UTC

Once issue I've found with this PR is that the catalog properties need to propagate to `PyArrowFileIO(properties=...)` otherwise endpoint/authentication/etc to things like s3 simply fail ... 

---

### Comment by @jayceslesar at 2025-09-22 21:01:29.000 UTC

Going to get around adding tests for both types of FileIO... @Fokko @kevinjqliu anything else you think we need here?

---

### Comment by @ForeverAngry at 2025-11-10 15:53:59.000 UTC

@jayceslesar how's this coming? Let me know if i can help with anything. Id like to use this in prod as well!

---

### Comment by @github-actions[bot] at 2026-03-17 00:28:27.000 UTC

This pull request has been marked as stale due to 30 days of inactivity. It will be closed in 1 week if no further activity occurs. If you think that's incorrect or this pull request requires a review, please simply write any comment. If closed, you can revive the PR at any time and @mention a reviewer or discuss it on the dev@iceberg.apache.org list. Thank you for your contributions.

---

### Comment by @github-actions[bot] at 2026-03-25 00:30:13.000 UTC

This pull request has been closed due to lack of activity. This is not a judgement on the merit of the PR in any way. It is just a way of keeping the PR queue manageable. If you think that is incorrect, or the pull request requires review, you can revive the PR at any time.

---

### Comment by @Zevrap-81 at 2026-04-15 12:34:37.000 UTC

Are there any blockers for this PR?

---

### Comment by @jayceslesar at 2026-04-15 14:11:08.000 UTC

Needs to be brought up to date. I can eventually take a look

---

