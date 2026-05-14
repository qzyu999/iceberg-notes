# Equality Delete support

**State:** open
**Created by:** @rambleraptor
**Created at:** 2026-04-23 23:39:11.000 UTC

One of the missing v2 features in PyIceberg is reading from equality delete files. 

I believe PyIceberg needs support for reading these files or else we run into situations where people are querying deleted data. Happy to be disproven on this!

---

### Comment by @geruh at 2026-04-24 08:51:42.000 UTC

I'm in favor of adding support, @rambleraptor! I've been meaning to open an issue for this but keep getting sidetracked.

This is a gap beyond just v2. Any table with equality deletes is affected, which can come from streaming use cases like Flink or direct usage of the Java core library (v3+ can have equality deletes). We should be able to read them, but I'd avoid writing them in favor of delete vectors.

One of the main benefits would be rewriting equality deletes to positional deletes. I know there was a PR in deltacat to do this in the past: https://github.com/ray-project/deltacat/issues/471

I think a good path forward would be to start by indexing the equality deletes so they can be mapped into file scan tasks. Then follow up with the reads using PyArrow, similar to what was done in #2255. That way we avoid a massive PR.

What do you think?

---

### Comment by @rambleraptor at 2026-04-24 22:33:10.000 UTC

I think that sounds like a great plan. I did the index part at #3285. It also includes some plumbing to ensure that equality deletes are ignored in the PyArrow reading in order to avoid crashing.

---

