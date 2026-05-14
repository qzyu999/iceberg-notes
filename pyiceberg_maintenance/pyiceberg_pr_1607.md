# Implement `write.metadata.delete-after-commit.enabled` to clean up old metadata files

**State:** closed
**Created by:** @kaushiksrini
**Created at:** 2025-02-04 14:17:43.000 UTC

Implements property `write.metadata.delete-after-commit.enabled` from https://iceberg.apache.org/docs/1.5.1/maintenance/#remove-old-metadata-files.

Closes #1199 

---

### Comment by @kaushiksrini at 2025-02-07 21:40:54.000 UTC

@Fokko thanks! used context managers and added to the documentation

---

### Comment by @kaushiksrini at 2025-02-09 17:19:01.000 UTC

@kevinjqliu thanks for the review! I moved the logic after the table is committed 

---

### Comment by @kaushiksrini at 2025-02-12 04:27:22.000 UTC

@Fokko thanks for the review! and @kevinjqliu thanks for making the changes + review!

---

### Comment by @kevinjqliu at 2025-02-12 04:38:41.000 UTC

Thanks @kaushiksrini I applied the simple test changes via github. Thanks @Fokko for the review 

---

