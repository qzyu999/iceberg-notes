# Write Deletion Vectors

**State:** closed
**Created by:** @rambleraptor
**Created at:** 2025-12-08 22:56:22.000 UTC

<!--
Thanks for opening a pull request!
-->

<!-- In the case this PR will resolve an issue, please replace ${GITHUB_ISSUE_ID} below with the actual Github issue id. -->
Part of #2261

# Rationale for this change
This adds a PuffinWriter for writing deletion vectors.

Right now, it's just the writer class + some round trip tests (where we read + write the same file) to sanity check that the PuffinWriter works as expected. Writing Puffin files is very complex, so I wanted to make sure we all agreed on the writing semantics before using this elsewhere.

Let me know your thoughts on this (or if it's too granular)

## Are these changes tested?
Unit tests included

## Are there any user-facing changes?

<!-- In the case of user-facing changes, please add the changelog label. -->


---

### Comment by @glesperance at 2025-12-11 02:54:12.000 UTC

Hey @rambleraptor, I was working on a DV implementation before discovering this PR. Since review is already underway, I'd rather contribute here than duplicate effort.

I've added a Spark interoperability test: glesperance@c25fe312

This verifies pyiceberg can read Spark-written DVs. Combined with your existing round-trip tests, this confirms format compatibility... ie if the same reader handles both, Spark can read ours too.

This may be redundant with your existing .bin fixture tests, though I believe those test the raw bitmap format rather than full Puffin DVs with the Java wrapper (length + magic + CRC). Let me know if I'm wrong on that.

Happy to PR to your fork if you think it's pertinent -or- feel free to cherry pick the commit as you see fit.

---

### Comment by @rambleraptor at 2025-12-11 22:42:37.000 UTC

@glesperance Thanks so much! I patched in your commit and I'll push it up along with my changes. Your name should appear in the commit log + PR. Let me know if you don't see it.

---

### Comment by @rambleraptor at 2025-12-12 00:46:39.000 UTC

PR comments have been addressed.

@geruh it looks like your work on DeleteFileIndexes will be very useful for determing offsets + lengths on the blobs!

---

### Comment by @glesperance at 2025-12-16 15:45:54.000 UTC

Really excited to see this moving forward.

@rambleraptor Thanks for the opportunity to contribute and for handling the updates. On full DV support, have you started on the delete/manifest writers for v3 and the MOR logic?

I’ve got a working PoC with some tests but it’ll certainly need more polish before it's PR-ready. 
I also need to rebase it on top of #2822 (this) and  #2180

---

### Comment by @github-actions[bot] at 2026-03-18 00:30:30.000 UTC

This pull request has been marked as stale due to 30 days of inactivity. It will be closed in 1 week if no further activity occurs. If you think that's incorrect or this pull request requires a review, please simply write any comment. If closed, you can revive the PR at any time and @mention a reviewer or discuss it on the dev@iceberg.apache.org list. Thank you for your contributions.

---

### Comment by @github-actions[bot] at 2026-03-27 00:31:14.000 UTC

This pull request has been closed due to lack of activity. This is not a judgement on the merit of the PR in any way. It is just a way of keeping the PR queue manageable. If you think that is incorrect, or the pull request requires review, you can revive the PR at any time.

---

