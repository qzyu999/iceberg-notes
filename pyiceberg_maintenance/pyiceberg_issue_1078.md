# [Feat] Support Merge-on-Read mode for Deletes

**State:** open
**Created by:** @sungwy
**Created at:** 2024-08-20 15:52:52.000 UTC

### Feature Request / Improvement

Similar to Spark, we would like to implement Merge-on-Read mode of deleting, by creating delete files that encode position and equality delete markers that can be applied within a given partition.

https://iceberg.apache.org/spec/?h=delete+files#row-level-deletes

---

### Comment by @github-actions[bot] at 2025-02-17 00:18:54.000 UTC

This issue has been automatically marked as stale because it has been open for 180 days with no activity. It will be closed in next 14 days if no further activity occurs. To permanently prevent this issue from being considered stale, add the label 'not-stale', but commenting on the issue is preferred when possible.

---

### Comment by @Fokko at 2025-02-17 10:19:39.000 UTC

Or we can jump directly to deletion vectors 😏 

---

### Comment by @piyushdubey at 2025-09-26 23:07:15.000 UTC

Reviving this thread. Is the support for Writing Position Deletes added to PyIceberg or not yet?

@Fokko, @sungwy 

---

### Comment by @Thomas-X at 2026-01-14 12:52:05.000 UTC

Apologies for the necro-post, but is this supported yet? Writing to a normal sized iceberg table is painfully slow when forced to do CoW

---

### Comment by @kevinjqliu at 2026-01-25 17:14:25.000 UTC

i think we have all the necessary pieces to write v2 position delete, lets see if we can get this feature out. 

We should think about what the default should be. If the table/file is small, it would be faster to just rewrite instead of writing delete files

---

### Comment by @qzyu999 at 2026-05-17 04:08:53.000 UTC

Is this basically the same feature request for V2 positional deletes as tracked in #1808? Also, with V3 read support merged (#1516) and Deletion Vector write support underway (#2822), what is the long-term vision for maintaining both V2 and V3 architectures for MoR? Because V2 is highly integrated into stable enterprise pipelines while V3 is the future standard, it would make a lot of sense for PyIceberg to bridge and maintain both capabilities.

---

