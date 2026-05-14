# Bridging Your Understanding: Java Top-Down vs. Issue #1092

## The Short Answer

**Yes, it's the same operation.** But the issue is asking you to build **the inner kernel** of the full Java pipeline — not the outer shell.

Think of it this way:

```
┌──────────────────────────────────────────────────────────────────────┐
│                                                                      │
│  spark.sql("CALL catalog.system.rewrite_data_files(                  │
│      table => 'db.events',                                           │
│      strategy => 'sort',                          ← NOT in scope     │
│      sort_order => 'zorder(event_id, user_id)',   ← NOT in scope     │
│      where => 'event_date = ...',                 ← IN SCOPE         │
│      options => map(                                                 │
│          'target-file-size-bytes', '268435456',   ← IN SCOPE (implicit)
│          'partial-progress.enabled', 'true',      ← NOT in scope     │
│          'max-concurrent-file-group-rewrites',    ← NOT in scope     │
│      )                                                               │
│  )")                                                                 │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

The issue says: **"The first version of the API..."**  — i.e., they're explicitly scoping this as a starting point, not the full Java feature set.

---

## What You Studied vs. What's Being Asked

Here's where your confusion likely comes from. You traced this full chain:

```
SparkProcedures → RewriteDataFilesProcedure → SparkActions
  → RewriteDataFilesSparkAction
    → BinPackRewriteFilePlanner (or Sort/ZOrder planner)
    → SparkBinPackFileRewriteRunner (using full Spark distributed engine)
    → RewriteDataFilesCommitManager (with partial progress, retry, etc.)
```

That's **~2000+ lines of Java** across 9 files, with:
- 3 rewrite strategies (bin-pack, sort, z-order)
- Partial progress with multi-commit batching
- Concurrent file group rewrites with thread pools
- Delete-aware compaction (delete file thresholds, delete ratio)
- Output spec evolution
- Starting sequence number handling
- Dangling delete cleanup
- Rewrite job ordering

**The issue is NOT asking for all of that.** It's asking for this:

```
MaintenanceTable.rewrite_data_files(filter)
  → table.scan(filter).plan_files()           ← already exists
  → group by partition                         ← trivial
  → filter files by size                       ← new, simple
  → bin-pack into groups                       ← already exists (ListPacker)
  → for each group: read → write               ← already exists (ArrowScan + writer)
  → commit: delete old, add new                ← already exists (_OverwriteFiles)
```

---

## Why They Feel Different

| Aspect | Your top-down research | Issue #1092 |
|---|---|---|
| **Perspective** | "How does the full Spark CALL work?" | "What's the minimal viable compaction?" |
| **Engine** | Spark (distributed, JVM) | PyArrow (single-node, Python) |
| **Strategy** | Configurable (bin-pack/sort/z-order) | Bin-pack only (hardcoded) |
| **Options** | 15+ tunable parameters | Uses existing writer defaults |
| **Commit** | Partial progress, retry, multi-commit | Single atomic commit |
| **Parallelism** | Thread pool, concurrent groups | Sequential |
| **Entry point** | SQL stored procedure system | Python method on `MaintenanceTable` |

The Java code has all those layers because it's a **production-grade, multi-tenant, distributed system**. The issue is asking for the **algorithmic core** — the part that actually matters for correctness — without the operational complexity.

---

## The Connection: Same Algorithm, Different Packaging

Your research wasn't wasted. Here's how to think about it:

```
┌─ JAVA FULL IMPLEMENTATION ────────────────────────────────────────┐
│                                                                    │
│  ┌─ OUTER SHELL (NOT in issue scope) ───────────────────────────┐ │
│  │  • SQL procedure parsing (RewriteDataFilesProcedure)         │ │
│  │  • Strategy selection (binpack/sort/zorder)                  │ │
│  │  • Thread pool / concurrent execution                       │ │
│  │  • Partial progress / multi-commit batching                 │ │
│  │  • Rewrite job ordering                                     │ │
│  │  • Spark SQL read/write (SparkBinPackFileRewriteRunner)      │ │
│  │                                                              │ │
│  │  ┌─ INNER KERNEL (this IS the issue scope) ───────────────┐  │ │
│  │  │                                                         │  │ │
│  │  │  1. Scan table with filter → get file list              │  │ │
│  │  │  2. Group files by partition                            │  │ │
│  │  │  3. Filter: keep files outside [min, max] size range    │  │ │
│  │  │  4. Bin-pack files into groups (ListPacker)             │  │ │
│  │  │  5. Filter groups: need ≥ min_input_files               │  │ │
│  │  │  6. For each group: read data → write new files         │  │ │
│  │  │  7. Atomic commit: delete old files, add new files      │  │ │
│  │  │                                                         │  │ │
│  │  └─────────────────────────────────────────────────────────┘  │ │
│  └──────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

The issue is saying: **"Build the inner kernel. The outer shell can come later."**

And the beautiful thing is: pyiceberg already has most of the inner kernel pieces built for other features (writing, scanning, bin-packing). You're essentially **wiring existing components together in a new way**.

---

## Concrete Example: What Changes

**Java (what you studied):**
```java
// 1. User calls via SQL stored procedure
spark.sql("CALL catalog.system.rewrite_data_files(table => 'db.table', where => '...')");

// 2. This triggers ~200 lines of procedure parsing
// 3. Creates RewriteDataFilesSparkAction (~460 lines)
// 4. Which creates BinPackRewriteFilePlanner (~350 lines)  
// 5. Which creates SparkBinPackFileRewriteRunner (~70 lines, but extends 2 base classes)
// 6. Which uses RewriteDataFilesCommitManager (~180 lines)
// Total: ~1200+ lines of new Java code
```

**PyIceberg (what the issue wants):**
```python
# 1. User calls via Python API
table.maintenance.rewrite_data_files(filter=EqualTo("date", "2024-01-01"))

# 2. This calls: table.scan(filter).plan_files()            ← EXISTS
# 3. Groups by partition                                     ← 5 lines  
# 4. Filters files by size                                   ← 10 lines
# 5. Bin-packs using ListPacker                              ← EXISTS
# 6. Reads with ArrowScan, writes with _dataframe_to_data_files  ← EXISTS
# 7. Commits with _OverwriteFiles                            ← EXISTS
# Total: ~80-100 lines of new Python code
```

The reason it's so much less code isn't because it's less correct — it's because **pyiceberg already built reusable primitives** (ListPacker, ArrowScan, _OverwriteFiles) that the Java side baked directly into each action class.

---

## Your Research IS Valuable

Even though the issue scope is small, your top-down understanding is critical because:

1. **You know the destination.** When Phase 2/3 PRs come (sort strategy, partial progress, concurrent rewrites), you already know the architecture.

2. **You know the edge cases.** The Java code handles things like "what if `min_file_size` is 75% of `target_file_size`?" — you understand *why* those defaults exist.

3. **You can design for extensibility.** By keeping the planner and runner as separate concerns (even if Phase 1 only has one of each), you're setting up for the full feature set later.

4. **You can write better tests.** Knowing how the Java code validates correctness helps you write test cases that cover the same scenarios.

---

## TL;DR

| Question | Answer |
|---|---|
| Is it the same CALL? | **Same operation, smaller scope.** The issue wants the core algorithm without the Spark/distributed/multi-strategy packaging. |
| Why does it feel different? | You studied **top-down** (user-facing SQL → internals). The issue describes **bottom-up** (core algorithm first → add features later). |
| Was my research wasted? | **No.** You now understand the full architecture, which means you can build Phase 1 in a way that naturally extends to the full feature set. |
| What's actually new code? | ~80-100 lines: group by partition, filter files by size, filter groups, orchestrate read→write→commit. Everything else already exists in pyiceberg. |
