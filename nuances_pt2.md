# Nuance: The Remainder Problem in File Rewriting

> This document explores `expectedOutputFiles()`, `inputSplitSize()`, and `writeMaxFileSize()`
> in `SizeBasedFileRewritePlanner.java` — three interconnected methods that solve the problem
> of producing undersized "remainder" files during compaction.

---

## Table of Contents

- [The Problem: Tiny Remainder Files](#the-problem-tiny-remainder-files)
- [The Three Methods That Solve It](#the-three-methods-that-solve-it)
  - [expectedOutputFiles() — How Many Files Should We Produce?](#expectedoutputfiles--how-many-files-should-we-produce)
  - [inputSplitSize() — How Big Should Each Read Split Be?](#inputsplitsize--how-big-should-each-read-split-be)
  - [writeMaxFileSize() — How Big Can a Single Output File Be?](#writemaxfilesize--how-big-can-a-single-output-file-be)
- [How These Three Methods Work Together](#how-these-three-methods-work-together)
- [Worked Examples](#worked-examples)
- [How This Flows Through Spark's Execution](#how-this-flows-through-sparks-execution)
- [How PyIceberg Handles This Today](#how-pyiceberg-handles-this-today)
- [What We Need for Phase 1](#what-we-need-for-phase-1)
- [Decision Matrix for expectedOutputFiles](#decision-matrix-for-expectedoutputfiles)

---

## The Problem: Tiny Remainder Files

Imagine you have 10.1 GB of data to compact, with a target file size of 1 GB.

**Naive approach**: Divide 10.1 GB into 1 GB chunks → you get **11 files**:
- 10 files × 1 GB each = 10 GB
- 1 file × 0.1 GB = 100 MB ← **this is the remainder file**

That 100 MB file is a problem. It's way below the minimum file size threshold (75% of 1 GB = 750 MB). If we run compaction again, this file will be flagged as "too small" and trigger _another_ rewrite cycle. We've created the exact problem we were trying to solve.

**Better approach**: Produce **10 files** of ~1.01 GB each. Each file is slightly larger than target (1% bigger), which is well within the acceptable range (max = 180% of target = 1.8 GB). No remainder file. No future rewrite needed.

This is what `expectedOutputFiles()` does.

---

## The Three Methods That Solve It

### expectedOutputFiles() — How Many Files Should We Produce?

**File**: [SizeBasedFileRewritePlanner.java, lines 234-258](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java)

```java
protected int expectedOutputFiles(long inputSize) {
    if (inputSize < targetFileSize) {
        return 1;  // CASE 0: Input is smaller than one target file
    }

    long numFilesWithRemainder = LongMath.divide(inputSize, targetFileSize, RoundingMode.CEILING);
    long numFilesWithoutRemainder = LongMath.divide(inputSize, targetFileSize, RoundingMode.FLOOR);
    long avgFileSizeWithoutRemainder = inputSize / numFilesWithoutRemainder;

    if (LongMath.mod(inputSize, targetFileSize) > minFileSize) {
        // CASE 1: The remainder is big enough on its own
        return (int) numFilesWithRemainder;

    } else if (avgFileSizeWithoutRemainder
        < Math.min(1.1 * targetFileSize, (double) writeMaxFileSize())) {
        // CASE 2: Distributing the remainder keeps files within 10% of target
        return (int) numFilesWithoutRemainder;

    } else {
        // CASE 3: Can't distribute because it would bloat files past the limit
        return (int) numFilesWithRemainder;
    }
}
```

### Line-by-line explanation

#### Early return: `inputSize < targetFileSize`
If the entire group is smaller than one target file, just produce 1 file. No remainder problem possible.

#### The two candidate counts

```java
long numFilesWithRemainder = LongMath.divide(inputSize, targetFileSize, RoundingMode.CEILING);
long numFilesWithoutRemainder = LongMath.divide(inputSize, targetFileSize, RoundingMode.FLOOR);
```

For 10.1 GB input with 1 GB target:
- `numFilesWithRemainder` = ⌈10.1 / 1.0⌉ = **11** (ceiling division)
- `numFilesWithoutRemainder` = ⌊10.1 / 1.0⌋ = **10** (floor division)

The question is: should we produce 11 files or 10?

#### Average size without remainder

```java
long avgFileSizeWithoutRemainder = inputSize / numFilesWithoutRemainder;
```

If we produce 10 files: 10.1 GB / 10 = **1.01 GB per file**.

#### Decision: Three cases

**Case 1: `remainder > minFileSize`** (the remainder is big enough to be a valid file)

```java
if (LongMath.mod(inputSize, targetFileSize) > minFileSize)
```

`remainder = 10.1 % 1.0 = 0.1 GB = 100 MB`. `minFileSize = 0.75 GB = 750 MB`.  
Is `100 MB > 750 MB`? **No.** → Move to Case 2.

But if the input were 10.8 GB: `remainder = 0.8 GB = 800 MB > 750 MB` → **Yes! Keep the remainder as its own file. Produce 11 files.**

The reasoning: if the remainder is big enough to pass the "too small" filter on its own, there's no reason to squeeze it into other files. Let it be its own file.

**Case 2: `avgWithoutRemainder < min(1.1 * target, writeMaxFileSize)`** (distributing adds ≤ 10%)

```java
else if (avgFileSizeWithoutRemainder < Math.min(1.1 * targetFileSize, (double) writeMaxFileSize()))
```

For our 10.1 GB example: `avgWithoutRemainder = 1.01 GB`. `1.1 * target = 1.1 GB`. `writeMaxFileSize = 1.38 GB` (see below).  
Is `1.01 GB < min(1.1 GB, 1.38 GB) = 1.1 GB`? **Yes.** → **Round down. Produce 10 files of ~1.01 GB each.**

The reasoning: each file is only 1% bigger than target. That's fine. No remainder file needed.

**Case 3: fallback** (distributing would bloat files too much)

If neither Case 1 nor Case 2 applies, keep the remainder file. This happens when the input is just barely above a multiple of target, but there are so few files that distributing would push each file past 110% of target.

For example: 2.3 GB input with 1 GB target:
- `numFilesWithRemainder = 3`, `numFilesWithoutRemainder = 2`
- `avgWithoutRemainder = 2.3 / 2 = 1.15 GB`
- Is `1.15 GB < 1.1 GB`? **No.** → Case 3: produce 3 files.

---

### inputSplitSize() — How Big Should Each Read Split Be?

**File**: [SizeBasedFileRewritePlanner.java, lines 211-218](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java)

```java
protected long inputSplitSize(long inputSize) {
    long estimatedSplitSize = (inputSize / expectedOutputFiles(inputSize)) + SPLIT_OVERHEAD;
    if (estimatedSplitSize < targetFileSize) {
        return targetFileSize;
    } else {
        return Math.min(estimatedSplitSize, writeMaxFileSize());
    }
}
```

#### What this does

This is a **Spark-specific** planning concept. Spark reads data in "splits" — each split becomes one Spark task, and each task typically produces one output file. By controlling the split size, you control how much data each Spark task processes.

`estimatedSplitSize = totalInput / expectedOutputFiles + 5KB overhead`

For our 10.1 GB → 10 files example: `estimatedSplitSize = 10.1 GB / 10 + 5 KB ≈ 1.01 GB`

The clamps ensure the split is at least `targetFileSize` (don't produce files smaller than target) and at most `writeMaxFileSize()` (don't exceed the maximum output size).

#### SPLIT_OVERHEAD

```java
private static final long SPLIT_OVERHEAD = 5L * 1024; // 5 KB
```

A tiny buffer (5 KB) added to each split to account for metadata overhead — things like Parquet row group headers, page headers, column chunk metadata, etc. This prevents a split from falling just barely short of the target size.

#### Why this matters for Spark

```
SparkBinPackFileRewriteRunner.doRewrite():

    spark.read()
        .option(SparkReadOptions.SPLIT_SIZE, group.inputSplitSize())  ← uses this
        .load(groupId);

    spark.write()
        .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, group.maxOutputFileSize())  ← and this
        .save(groupId);
```

Spark's Iceberg reader uses `SPLIT_SIZE` to decide how to combine input files into read splits. If `SPLIT_SIZE = 1.01 GB`, then Spark will try to pack input files into ~1.01 GB reading tasks. Each task produces one output file ~1.01 GB in size.

---

### writeMaxFileSize() — How Big Can a Single Output File Be?

**File**: [SizeBasedFileRewritePlanner.java, lines 260-279](file:///Users/jaredyu/Desktop/open_source/iceberg/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java)

```java
/**
 * Estimates a larger max target file size than the target size used in
 * task creation to avoid creating tiny remainder files.
 *
 * While we create tasks that should all be smaller than our target size,
 * there is a chance that the actual data will end up being larger than
 * our target size due to various factors of compression, serialization,
 * which are outside our control. If this occurs, instead of making a
 * single file that is close in size to our target, we would end up
 * producing one file of the target size, and then a small extra file
 * with the remaining data.
 *
 * For example, if our target is 512 MB, we may generate a rewrite task
 * that should be 500 MB. When we write the data we may find we actually
 * have to write out 530 MB. If we use the target size while writing, we
 * would produce a 512 MB file and an 18 MB file. If instead we use a
 * larger size estimated by this method, then we end up writing a single file.
 */
protected long writeMaxFileSize() {
    return (long) (targetFileSize + ((maxFileSize - targetFileSize) * 0.5));
}
```

#### The calculation

With defaults (target = 512 MB, max = 922 MB):

```
writeMaxFileSize = 512 + (922 - 512) * 0.5
                 = 512 + 410 * 0.5
                 = 512 + 205
                 = 717 MB
```

This is the **write-time** cap. When Spark's writer is producing an output file, it doesn't stop at `targetFileSize` (512 MB) — it stops at `writeMaxFileSize` (717 MB). This gives files room to grow slightly past the target due to compression unpredictability, without producing a tiny overflow file.

#### Why not just use maxFileSize (922 MB)?

Because that's too permissive. A 922 MB file is almost double the target. The 50% midpoint (717 MB) allows some overflow while keeping files reasonably close to target.

#### With 1 GB target (the example we've been using):

```
maxFileSize = 1.80 GB
writeMaxFileSize = 1.0 + (1.80 - 1.0) * 0.5
                 = 1.0 + 0.40
                 = 1.40 GB
```

---

## How These Three Methods Work Together

```
                   expectedOutputFiles(inputSize)
                              │
                              ▼
                   "Should we produce N or N+1 files?"
                              │
              ┌───────────────┼───────────────┐
              │               │               │
         Case 1:          Case 2:          Case 3:
    remainder > min     distribute ok    can't distribute
    → N+1 files         → N files        → N+1 files
              │               │               │
              └───────┬───────┘               │
                      │                       │
                      ▼                       │
              inputSplitSize(inputSize)        │
              = inputSize / N + 5KB            │
              clamped to [target, writeMax]     │
                      │                       │
                      ▼                       │
              Spark read: SPLIT_SIZE ─────────┘
              Each split ≈ inputSplitSize
                      │
                      ▼
              Spark write: TARGET_FILE_SIZE_BYTES = writeMaxFileSize
              Each output file ≤ writeMaxFileSize
                      │
                      ▼
              Result: N output files, each ≈ inputSize/N
              No tiny remainder files!
```

---

## Worked Examples

### Example 1: 10.1 GB input, 1 GB target

| Step | Calculation | Result |
|---|---|---|
| `numFilesWithRemainder` | ⌈10.1 / 1.0⌉ | 11 |
| `numFilesWithoutRemainder` | ⌊10.1 / 1.0⌋ | 10 |
| `remainder` | 10.1 % 1.0 | 0.1 GB |
| `minFileSize` | 1.0 × 0.75 | 0.75 GB |
| Case 1: `0.1 > 0.75`? | No | → try Case 2 |
| `avgWithoutRemainder` | 10.1 / 10 | 1.01 GB |
| `1.1 × target` | 1.1 × 1.0 | 1.10 GB |
| `writeMaxFileSize` | 1.0 + (1.8 - 1.0) × 0.5 | 1.40 GB |
| Case 2: `1.01 < min(1.10, 1.40)`? | Yes (1.01 < 1.10) | → **10 files** |
| `inputSplitSize` | 10.1 / 10 + 5KB | ~1.01 GB |
| **Output** | 10 files × ~1.01 GB | ✅ No remainder |

### Example 2: 10.8 GB input, 1 GB target

| Step | Calculation | Result |
|---|---|---|
| `remainder` | 10.8 % 1.0 | 0.8 GB |
| `minFileSize` | 0.75 GB | |
| Case 1: `0.8 > 0.75`? | **Yes** | → **11 files** |
| `inputSplitSize` | 10.8 / 11 + 5KB | ~0.98 GB |
| **Output** | 10 files × ~1 GB + 1 file × ~0.8 GB | ✅ Remainder is large enough |

### Example 3: 2.3 GB input, 1 GB target

| Step | Calculation | Result |
|---|---|---|
| `numFilesWithRemainder` | ⌈2.3 / 1.0⌉ | 3 |
| `numFilesWithoutRemainder` | ⌊2.3 / 1.0⌋ | 2 |
| `remainder` | 2.3 % 1.0 | 0.3 GB |
| Case 1: `0.3 > 0.75`? | No | → try Case 2 |
| `avgWithoutRemainder` | 2.3 / 2 | 1.15 GB |
| Case 2: `1.15 < 1.10`? | **No** (1.15 > 1.10) | → **Case 3: 3 files** |
| `inputSplitSize` | 2.3 / 3 + 5KB | ~0.77 GB |
| **Output** | 3 files × ~0.77 GB | 1 small file, but distributing would exceed 110% |

### Example 4: 500 MB input, 1 GB target

| Step | Calculation | Result |
|---|---|---|
| `inputSize < targetFileSize`? | 500 MB < 1 GB? **Yes** | → **1 file** |
| **Output** | 1 file × 500 MB | Input too small for multiple files |

### Example 5: 5.05 GB input, 512 MB target (realistic defaults)

| Step | Calculation | Result |
|---|---|---|
| `numFilesWithRemainder` | ⌈5.05 GB / 512 MB⌉ = ⌈10.06⌉ | 11 |
| `numFilesWithoutRemainder` | ⌊10.06⌋ | 10 |
| `remainder` | 5.05 GB % 512 MB | 30 MB |
| `minFileSize` | 512 × 0.75 = 384 MB | |
| Case 1: `30 MB > 384 MB`? | No | → try Case 2 |
| `avgWithoutRemainder` | 5.05 GB / 10 | 505 MB |
| `1.1 × 512 MB` | 563 MB | |
| `writeMaxFileSize` | 512 + (922 - 512) × 0.5 = 717 MB | |
| Case 2: `505 MB < min(563, 717)`? | Yes (505 < 563) | → **10 files** |
| `inputSplitSize` | 5.05 GB / 10 + 5KB | ~505 MB |
| **Output** | 10 files × ~505 MB | ✅ Only 1% below target, no remainder |

---

## How This Flows Through Spark's Execution

The methods connect to the runner at [SparkBinPackFileRewriteRunner.java, lines 42-62](file:///Users/jaredyu/Desktop/open_source/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackFileRewriteRunner.java):

```java
protected void doRewrite(String groupId, RewriteFileGroup group) {
    // 1. Read with computed split size — controls how data is batched
    Dataset<Row> scanDF = spark().read()
        .format("iceberg")
        .option(SparkReadOptions.SCAN_TASK_SET_ID, groupId)
        .option(SparkReadOptions.SPLIT_SIZE, group.inputSplitSize())     // ← from inputSplitSize()
        .option(SparkReadOptions.FILE_OPEN_COST, "0")
        .load(groupId);

    // 2. Write with computed max file size — controls output file boundaries
    scanDF.write()
        .format("iceberg")
        .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupId)
        .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, group.maxOutputFileSize())  // ← writeMaxFileSize()
        .option(SparkWriteOptions.DISTRIBUTION_MODE, distributionMode(group).modeName())
        .option(SparkWriteOptions.OUTPUT_SPEC_ID, group.outputSpecId())
        .mode("append")
        .save(groupId);
}
```

The chain:
1. **Planner**: `expectedOutputFiles(10.1 GB) → 10`
2. **Planner**: `inputSplitSize(10.1 GB) → 1.01 GB`
3. **Planner**: `writeMaxFileSize() → 1.4 GB`
4. **Runner (read)**: Spark reads and packs input into ~1.01 GB splits
5. **Runner (write)**: Each Spark task writes one file, capped at 1.4 GB
6. **Result**: 10 files of ~1.01 GB, no tiny remainder

---

## How PyIceberg Handles This Today

PyIceberg's writer uses `bin_pack_arrow_table()` at [pyiceberg/io/pyarrow.py, line 2740-2753](file:///Users/jaredyu/Desktop/open_source/iceberg-python/pyiceberg/io/pyarrow.py):

```python
def bin_pack_arrow_table(tbl: pa.Table, target_file_size: int) -> Iterator[list[pa.RecordBatch]]:
    from pyiceberg.utils.bin_packing import PackingIterator

    avg_row_size_bytes = tbl.nbytes / tbl.num_rows
    target_rows_per_file = max(1, int(target_file_size / avg_row_size_bytes))
    batches = tbl.to_batches(max_chunksize=target_rows_per_file)
    bin_packed_record_batches = PackingIterator(
        items=batches,
        target_weight=target_file_size,
        lookback=len(batches),  # check ALL previous bins
        weight_func=lambda x: x.nbytes,
        largest_bin_first=False,
    )
    return bin_packed_record_batches
```

### How PyIceberg's approach differs from Java

| Aspect | Java (Spark) | Python (PyArrow) |
|---|---|---|
| **Splitting mechanism** | `SPLIT_SIZE` option on Spark reader | `tbl.to_batches(max_chunksize=...)` |
| **Output sizing** | `TARGET_FILE_SIZE_BYTES` on Spark writer | `target_weight` in `PackingIterator` |
| **Remainder handling** | `expectedOutputFiles()` pre-computes optimal file count | `PackingIterator` does real-time bin-packing |
| **Split size calculation** | `inputSplitSize()` divides total by expected files | `avg_row_size * target_rows` estimates per-batch |

### Does PyIceberg have the remainder problem?

**Partially, but differently.** PyIceberg's `PackingIterator` uses a **real-time bin-packing** approach rather than pre-planning:

1. Arrow splits the table into record batches of `target_rows_per_file` rows each
2. `PackingIterator` packs these batches into bins of total weight ≤ `target_file_size`
3. Each bin becomes one output file

The `lookback=len(batches)` is key — unlike the planning-phase `ListPacker` (lookback=1), the **write-phase** packer checks ALL previous bins. This means if a small remainder batch is created, the packer can try to fit it into any previous bin that has room.

### Example: 10.1 GB → PyIceberg's approach

1. `avg_row_size = 10.1 GB / N_rows`
2. `target_rows = 1 GB / avg_row_size`
3. Arrow creates batches: 10 batches × ~1 GB each + 1 batch × ~0.1 GB
4. `PackingIterator` processes batches:
   - Bins 1-10: each gets one ~1 GB batch
   - Batch 11 (0.1 GB): PackingIterator checks ALL 10 previous bins for room
   - Since each bin has weight ~1 GB and target_weight is 1 GB, none have room
   - **Result: 11 bins, the last one is only 0.1 GB** ← remainder problem!

So yes, **PyIceberg CAN produce tiny remainder files** in certain cases, because its bin-packing doesn't have the Java planner's "round-down-and-distribute" logic.

### When it works anyway

In practice, PyIceberg often avoids the problem because:
- `avg_row_size` is an estimate — batches don't land exactly at `target_file_size`
- Record batches have variable sizes due to column encoding
- The `lookback=len(batches)` sometimes finds room in earlier bins

But it's not guaranteed.

---

## What We Need for Phase 1

### Option A: Do nothing (simplest)

For Phase 1, we can accept that PyIceberg might produce occasional remainder files. The existing `bin_pack_arrow_table()` handles most cases acceptably. The remainder file, while suboptimal, won't cause data loss or corruption — it'll just be small.

**Pros**: Zero new code for this aspect  
**Cons**: Might produce a small file that triggers re-compaction on the next run

### Option B: Port expectedOutputFiles() to Python

We could pre-compute the optimal file count and adjust the target size passed to `bin_pack_arrow_table`:

```python
def expected_output_files(input_size, target_size, min_size, max_size):
    """Mirror of SizeBasedFileRewritePlanner.expectedOutputFiles()"""
    if input_size < target_size:
        return 1

    num_with_remainder = -(-input_size // target_size)   # ceiling division
    num_without_remainder = input_size // target_size     # floor division
    avg_without = input_size / num_without_remainder
    remainder = input_size % target_size
    write_max = target_size + (max_size - target_size) * 0.5

    if remainder > min_size:
        return num_with_remainder          # remainder is big enough
    elif avg_without < min(1.1 * target_size, write_max):
        return num_without_remainder       # distribute remainder
    else:
        return num_with_remainder          # can't distribute

# Then adjust the target for writing:
n_files = expected_output_files(total_input, target_file_size, min_file_size, max_file_size)
adjusted_target = total_input / n_files  # larger than original target if distributing
# Pass adjusted_target to bin_pack_arrow_table instead of target_file_size
```

**Pros**: Exact parity with Java  
**Cons**: Adds ~20 lines, and the benefit is marginal for Phase 1

### Option C: Adjust writeMaxFileSize only

A simpler version: just use `writeMaxFileSize()` as the target for `bin_pack_arrow_table()` instead of `targetFileSize`. This gives files room to absorb remainder data without the full `expectedOutputFiles` calculation.

```python
write_max = target_file_size + (max_file_size - target_file_size) * 0.5
# Use write_max as target in bin_pack_arrow_table
```

**Pros**: 2 lines of code, handles the compression-overshoot problem  
**Cons**: Doesn't handle the pre-planning optimization of expectedOutputFiles

### Recommendation

**Phase 1: Option A** (do nothing). The small-file problem from remainders is self-correcting — the next compaction run will pick them up. The existing `bin_pack_arrow_table` is good enough.

**Phase 2: Option B** (port the full logic). When we're polishing the implementation, adding `expected_output_files()` brings exact Java parity and avoids pointless re-compaction cycles.

---

## Decision Matrix for expectedOutputFiles

Here's a reference table showing what `expectedOutputFiles()` returns for various inputs, assuming target = 1 GB:

| Input Size | Remainder | Remainder > min (750 MB)? | Avg w/o remainder | Avg < 1.1 GB? | Decision | Output |
|---|---|---|---|---|---|---|
| 500 MB | — | — | — | — | `< target` | 1 file |
| 1.5 GB | 500 MB | No | 1.5 GB | No (1.5 > 1.1) | Case 3: keep | 2 files |
| 2.0 GB | 0 | No | 1.0 GB | Yes (1.0 < 1.1) | Case 2: round down | 2 files |
| 2.05 GB | 50 MB | No | 1.025 GB | Yes (1.025 < 1.1) | Case 2: distribute | 2 files |
| 2.3 GB | 300 MB | No | 1.15 GB | No (1.15 > 1.1) | Case 3: keep | 3 files |
| 2.8 GB | 800 MB | **Yes** | — | — | Case 1: big enough | 3 files |
| 5.05 GB | 30 MB | No | 505 MB | Yes | Case 2: distribute | 10 files |
| 10.1 GB | 100 MB | No | 1.01 GB | Yes | Case 2: distribute | 10 files |
| 10.8 GB | 800 MB | **Yes** | — | — | Case 1: big enough | 11 files |
| 10.95 GB | 950 MB | **Yes** | — | — | Case 1: big enough | 11 files |
| 11.0 GB | 0 | No | 1.0 GB | Yes | Case 2: round down | 11 files |

---

## Summary

| Method | Purpose | Formula | With defaults (512 MB target) |
|---|---|---|---|
| `expectedOutputFiles()` | Avoid tiny remainder files | 3-case decision tree based on remainder size vs. distribution impact | Distributes if avg stays < 563 MB |
| `inputSplitSize()` | Tell Spark how big each read split should be | `inputSize / expectedOutputFiles + 5KB`, clamped to `[target, writeMax]` | ~512 MB per split |
| `writeMaxFileSize()` | Allow files to grow past target to absorb compression overshoot | `target + (max - target) * 0.5` | 717 MB |

**Key takeaway**: These three methods form a coordinated system. `expectedOutputFiles` decides the file count, `inputSplitSize` ensures the reader batches accordingly, and `writeMaxFileSize` gives the writer headroom. In PyIceberg, `bin_pack_arrow_table` handles splitting differently (real-time bin-packing rather than pre-planning), so the exact same logic isn't needed, but the remainder problem exists and can be addressed in Phase 2 for exact Java parity.
