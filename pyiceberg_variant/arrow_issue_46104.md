# GH-45937: [C++][Parquet] Logical type definition for variant

**State:** closed
**Created by:** @neilechao
**Created at:** 2025-04-10 23:02:08.000 UTC

### Describe the enhancement requested

Initial logical type definition and arrow extension for Variant

### Component(s)

C++, Parquet

---

### Comment by @jayceslesar at 2025-06-10 21:45:16.000 UTC

This issue can be closed right?

---

### Comment by @wgtmac at 2025-06-19 02:07:27.000 UTC

I think you are right. Let me close it.

---

### Comment by @amoeba at 2025-07-07 23:55:37.000 UTC

Hi @wgtmac, I noticed this issue doesn't have a milestone and it should. It looks to me like the merge script was used to do the merge of https://github.com/apache/arrow/pull/45375 but I'm not 100% sure.

Since it looks like this the PR merged after the 20.0.0 maint branch was created, it wasn't included in the 20.0.0 release so it's fine to include in 21.0.0. We can see it's not on `maint-20.0.0 below`:

```sh
❯ git branch --contains 68f1a0f4e0bae3ef436925ecf23b613b6ac9234b
  main
  maint-21.0.0
```

Also, https://github.com/apache/arrow/issues/45937 is in the 21.0.0 milestone but it would better if we removed the parent issue from any milestones and put this issue in 21.0.0. Any objections?


---

### Comment by @wgtmac at 2025-07-08 03:10:27.000 UTC

@amoeba Yes, it is not included in the 20.0.0. I have removed 21.0.0 milestone from the parent issue.

---

### Comment by @amoeba at 2025-07-08 03:16:21.000 UTC

Thanks @wgtmac!

---

