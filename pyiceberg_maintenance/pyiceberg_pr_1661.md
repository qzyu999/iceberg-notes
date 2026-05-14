# Rewrite manifests 

**State:** closed
**Created by:** @amitgilad3
**Created at:** 2025-02-13 20:21:24.000 UTC

This is an initial implementation of rewrite manifests, aiming to mimic the Java implementation as closely as possible. I’ve tried to follow the same structure and logic, but there are still some areas that might need refinement.

I’m looking for feedback and suggestions on:
	•	Whether the approach aligns well with the existing design.
	•	Any gaps or optimizations that could improve performance.
	•	How best to proceed with completing this feature.

Would love any insights or guidance on the next steps! Thanks in advance for the review! 🙌

---

### Comment by @Fokko at 2025-04-17 13:39:59.000 UTC

@amitgilad3 gentle ping, are you still interested in working on this?

---

### Comment by @amitgilad3 at 2025-04-17 14:04:39.000 UTC

Hey @Fokko , yes i am very interested in finishing this, must of missed this (sorry) , will look at this later today :)

---

### Comment by @Fokko at 2025-05-16 21:16:58.000 UTC

Looks like the CI is sad 😞 

```
tests/integration/test_writes/test_rewrite_manifests.py:154: error: "rewrite_manifests" of "Table" does not return a value (it only ever returns None)  [func-returns-value]
tests/integration/test_writes/test_rewrite_manifests.py:227: error: "rewrite_manifests" of "Table" does not return a value (it only ever returns None)  [func-returns-value]
```

---

### Comment by @amitgilad3 at 2025-05-17 14:41:29.000 UTC

Hey @Fokko -  just wanted to say thanks for reviewing (really appreciate it) .
I fixed all your comments and added a test to test v1 -> v2 , hope all pass now.
let me know if i have more work todo :)

---

### Comment by @amitgilad3 at 2025-06-03 12:13:40.000 UTC

@Fokko  gentle ping, was wondering if we still have any blockers ?

---

### Comment by @sungwy at 2025-07-25 13:15:32.000 UTC

Hi @amitgilad3 - sorry for the delayed turn around. There's a question in slack regarding this PR, and I wanted to take a stab at helping merging this in, in Fokko's absence.

Are you still interested in working on this PR? Would you be available to resolve the conflicts?

---

### Comment by @Anton-Tarazi at 2025-09-24 03:48:16.000 UTC

Wondering what the status of this PR is, happy to re-create if you're no longer able to work on it @amitgilad3 

---

### Comment by @amitgilad3 at 2025-09-24 20:46:59.000 UTC

Hey @sungwy , @Anton-Tarazi  i missed the messages , will fix this week all issues 

---

### Comment by @aammar5 at 2025-10-15 18:45:04.000 UTC

I have a local rebased version of this PR, if it's needed ...

---

### Comment by @ForeverAngry at 2025-11-10 20:44:52.000 UTC

@amitgilad3 any updates on this? Im happy to help if need :) 

---

### Comment by @github-actions[bot] at 2026-03-17 00:28:09.000 UTC

This pull request has been marked as stale due to 30 days of inactivity. It will be closed in 1 week if no further activity occurs. If you think that's incorrect or this pull request requires a review, please simply write any comment. If closed, you can revive the PR at any time and @mention a reviewer or discuss it on the dev@iceberg.apache.org list. Thank you for your contributions.

---

### Comment by @github-actions[bot] at 2026-03-25 00:29:53.000 UTC

This pull request has been closed due to lack of activity. This is not a judgement on the merit of the PR in any way. It is just a way of keeping the PR queue manageable. If you think that is incorrect, or the pull request requires review, you can revive the PR at any time.

---

