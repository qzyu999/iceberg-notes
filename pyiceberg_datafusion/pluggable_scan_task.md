# Pluggable Backend: Scan Task Analysis and Operation Universe

## 1. The Current Scan Task Flow in PyIceberg

### 1.1 The Pipeline (Status Quo)

The current data path in PyIceberg is a linear pipeline from scan planning through
to Arrow output. The boundary between "Iceberg semantics" and "data execution" is
implicit and interleaved:

```mermaid
graph TD
    subgraph "Scan Planning (PyIceberg — Iceberg semantics)"
        A["DataScan.plan_files()"]
        B["ManifestPlanner<br/>• Read manifests (Avro)<br/>• Partition pruning (skip non-matching manifests)<br/>• Column stats filtering (skip non-matching row groups)<br/>• Delete file association (which deletes apply to which data)"]
        C["Output: Iterable[FileScanTask]<br/>Each task = DataFile + set[DeleteFile] + residual filter"]
    end

    subgraph "Data Execution (PyArrow — currently hardcoded)"
        D["_to_arrow_via_file_scan_tasks()"]
        E["ArrowScan(table_metadata, io, schema, filter, ...)"]
        F["ArrowScan.to_table(tasks)<br/>• For each FileScanTask:<br/>  1. Open Parquet file (via FileIO)<br/>  2. Apply projection (column pruning)<br/>  3. Apply filter (predicate pushdown)<br/>  4. Read delete files<br/>  5. Apply positional deletes<br/>  6. Yield RecordBatches"]
        G["pa.Table (materialized result)"]
    end

    A --> B --> C --> D --> E --> F --> G
```

### 1.2 The Exact Code Path

```python
# User calls:
table.scan().to_arrow()

# Which calls:
class DataScan:
    def to_arrow(self):
        return _to_arrow_via_file_scan_tasks(
            self, self.projection(), self.plan_files(), ...
        )

# Which calls:
def _to_arrow_via_file_scan_tasks(scan, projected_schema, tasks, ...):
    from pyiceberg.io.pyarrow import ArrowScan
    return ArrowScan(
        scan.table_metadata, scan.io, projected_schema,
        scan.row_filter, scan.case_sensitive, scan.limit, ...
    ).to_table(tasks)  # ← THIS is where PyArrow takes over

# Inside ArrowScan.to_table():
def to_table(self, tasks: Iterable[FileScanTask]) -> pa.Table:
    # For each task:
    #   1. pq.ParquetFile(task.file.file_path)  ← PyArrow reads
    #   2. Scanner.to_batches(filter=...)       ← PyArrow filters
    #   3. _apply_positional_deletes(...)       ← PyArrow computes
    #   4. Accumulate into pa.Table             ← PyArrow materializes
    return pa.concat_tables(all_batches)
```

### 1.3 The Boundary Today

```
SCAN PLANNING (PyIceberg):
  Input:  Table metadata + snapshot + row filter + partition filter
  Output: Iterable[FileScanTask] — each task says "read this file, apply these deletes"
  This is PURE Iceberg semantics. No data is read. Only metadata is consulted.

DATA EXECUTION (PyArrow — hardcoded):
  Input:  Iterable[FileScanTask] + projected schema + residual filter
  Output: pa.Table or Iterator[RecordBatch]
  This reads actual Parquet data, applies filters, resolves deletes.
```

**The `FileScanTask` is the handoff point.** Scan planning produces tasks; data execution
consumes them. This is where the pluggable interface naturally sits.

### 1.4 What a FileScanTask Contains

```python
class FileScanTask:
    file: DataFile           # path, format, size, partition values, column stats
    delete_files: set[DataFile]  # associated delete files (positional + equality)
    residual: BooleanExpression  # filter that couldn't be pushed to partition pruning
```

This is a **complete execution instruction**: "read file X, apply deletes Y, filter
by predicate Z." Any backend receiving this has everything it needs to produce the
correct output.

### 1.5 Server-Side Scan Planning: How It Fits

PyIceberg already supports **pluggable scan planning** — not just pluggable execution.
The scan planning step itself can be performed by different planners:

```python
# In DataScan.plan_files():
def plan_files(self) -> Iterable[FileScanTask]:
    if self._should_use_server_side_planning():
        return self._plan_files_server_side()   # REST catalog does the planning
    return self._plan_files_local()             # PyIceberg does it locally
```

This is tracked in [#2303 (Pluggable Scan Planning)](https://github.com/apache/iceberg-python/issues/2303)
and [#2775 (Server-Side Scan Planning)](https://github.com/apache/iceberg-python/issues/2775).

**Three scan planners exist or are planned:**

| Planner | Where it runs | Status | Output |
|---------|--------------|--------|--------|
| **Python local** | PyIceberg client (reads manifests) | ✅ Working | `Iterable[FileScanTask]` |
| **REST server-side** | REST catalog server | ✅ Partially merged | `Iterable[FileScanTask]` |
| **iceberg-rust** | Rust (via `pyiceberg-core`) | Proposed (#2303) | `Iterable[FileScanTask]` |

**Critical observation: ALL planners produce the same output type (`FileScanTask`).**

This means the pluggable execution backend is **completely decoupled from the choice of
scan planner.** Whether scan planning happens in Python, on the REST server, or in Rust —
the output is always `Iterable[FileScanTask]`, and the execution backend consumes that
identically.

```mermaid
graph TD
    subgraph "Pluggable Scan Planning (upstream of us)"
        SP_PY["Python local planner<br/>(reads manifests directly)"]
        SP_REST["REST server-side planner<br/>(server does manifest scanning)"]
        SP_RUST["iceberg-rust planner<br/>(Rust does manifest scanning)"]
    end

    subgraph "THE HANDOFF POINT"
        FST["Iterable[FileScanTask]<br/>(same type regardless of who planned)"]
    end

    subgraph "Pluggable Execution Backend (our work)"
        EB_PA["PyArrowBackend<br/>(read + compute in PyArrow)"]
        EB_DF["DataFusionBackend<br/>(read/compute with spill)"]
        EB_DDB["DuckDBBackend<br/>(read/compute via DuckDB)"]
    end

    SP_PY --> FST
    SP_REST --> FST
    SP_RUST --> FST
    FST --> EB_PA
    FST --> EB_DF
    FST --> EB_DDB
```

### 1.6 Why This Is a Smooth Interaction (Not a Complex Mess)

The two pluggable axes (scan planning and execution) interact cleanly because of
**the FileScanTask contract**:

1. **Planners promise:** "I'll give you correct `FileScanTask`s — right files, right
   deletes, right residual filter. How I determined them is my business."

2. **Backends promise:** "Give me `FileScanTask`s and I'll read the data, apply deletes,
   and return correct Arrow output. How you planned them is your business."

Neither side needs to know about the other. They communicate exclusively through
`FileScanTask` — a data structure defined by PyIceberg's `table/__init__.py`.

**No conflict with server-side planning:**
- Server-side planning makes the *planning* faster (server reads manifests, not client)
- Our pluggable backend makes the *execution* more capable (bounded memory, spill)
- They address different performance bottlenecks:
  - Planning bottleneck = manifest scanning latency (solved by server-side)
  - Execution bottleneck = OOM on data compute (solved by pluggable backend)

**They compose freely:**
```
Server-side planning + DataFusion execution  → fast planning + bounded compute
Python local planning + DataFusion execution → simple + bounded compute
REST planning + PyArrow execution            → fast planning + in-memory compute
```

Any combination works because `FileScanTask` is the stable contract between them.

### 1.7 The Metadata OOM Problem Revisited

One subtlety: **local scan planning** itself reads manifests into memory. For the
normal scan path (partition-pruned), this is bounded. But for operations like orphan
file deletion that enumerate ALL manifests across ALL snapshots, the planning phase
can OOM before execution even begins.

Server-side planning helps here — the server handles the manifest scanning, and the
client only receives the resulting `FileScanTask` list (which is much smaller than
the full manifest content).

For local planning with large metadata, our streaming pattern (Section 4 of this doc)
applies: iterate manifests as a generator, never materialize the full set.

---

## 2. The Pluggable Boundary: Formal Definition

### 2.1 The Separation Theorem (Restated)

```
Operation(Op) = ScanPlan(Op) ∘ Execute(Op)

Where:
  ScanPlan : TableMetadata × Filter × Projection → Iterable[FileScanTask]
             (Iceberg semantics — ALWAYS in PyIceberg)

  Execute  : Iterable[FileScanTask] × Schema → Iterator[RecordBatch]
             (Data execution — PLUGGABLE)
```

**Theorem:** `ScanPlan` and `Execute` are independently substitutable. The correctness
of the output depends on `ScanPlan` producing correct tasks (right files, right deletes,
right residuals). The *feasibility* (OOM-free execution) depends on `Execute` being
bounded-memory. Changing the executor doesn't change correctness; it changes scale.

### 2.2 The Interface at the Boundary

```python
class ExecutionBackend(Protocol):
    """Consumes FileScanTasks and produces Arrow RecordBatches."""

    def execute_scan(
        self,
        tasks: Iterable[FileScanTask],
        projected_schema: Schema,
        row_filter: BooleanExpression,
        io_properties: dict[str, str],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]: ...
```

This is the **minimum viable interface for reads**. The backend receives:
- Tasks (what to read, what deletes apply)
- Schema (what columns to project)
- Filter (residual predicate after partition pruning)
- IO properties (how to access storage)
- Memory limit (how much RAM to use before spilling)

And returns: streaming Arrow RecordBatches.

---

## 3. The Universe of Operations (Java Iceberg Parity)

### 3.1 Java Iceberg's Operation Taxonomy

Java Iceberg defines operations across three categories:

**Category A: Read Operations (scan + filter + resolve deletes)**
```
TableScan → FileScanTask → Read data + apply deletes → output
```

**Category B: Write/Mutation Operations (modify table state)**
```
AppendFiles, OverwriteFiles, DeleteFiles, RowDelta, RewriteFiles, ReplacePartitions
```

**Category C: Maintenance Actions (bulk data manipulation)**
```
RewriteDataFiles, RewriteManifests, RewritePositionDeleteFiles,
ConvertEqualityDeleteFiles, DeleteOrphanFiles, ExpireSnapshots,
DeleteReachableFiles, RemoveDanglingDeleteFiles, ComputeTableStats
```

### 3.2 Complete Operation Map: What Each Touches

| # | Operation | Scan Planning | Read Data | Compute | Write Data | Commit | Java Location |
|---|-----------|:---:|:---:|:---:|:---:|:---:|---|
| 1 | **Table scan** (read) | ✅ Manifests, pruning | ✅ Parquet → Arrow | ✅ Delete resolution | ❌ | ❌ | `TableScan` |
| 2 | **Append** | ❌ | ❌ | ❌ | ✅ Arrow → Parquet | ✅ fast_append | `AppendFiles` |
| 3 | **Overwrite** | ✅ Find affected files | ✅ Read for CoW rewrite | ✅ Filter | ✅ Write new files | ✅ overwrite | `OverwriteFiles` |
| 4 | **Delete** (CoW) | ✅ Find affected files | ✅ Read files to rewrite | ✅ Filter complement | ✅ Write rewritten files | ✅ overwrite | `DeleteFiles` (CoW mode) |
| 5 | **Upsert** | ✅ Find matching files | ✅ Read target for join | ✅ Hash join (source × target) | ✅ Write updated + new | ✅ overwrite + append | `MergeInto` (Spark) |
| 6 | **Compaction** | ✅ Select files to compact | ✅ Read all selected files | ✅ Sort (external merge) | ✅ Write sorted output | ✅ rewrite_files | `RewriteDataFiles` |
| 7 | **Equality delete resolution** | ✅ Match deletes to data | ✅ Read data + deletes | ✅ Anti-join | ❌ (read-path only) | ❌ | `EqualityDeleteFilter` |
| 8 | **Position delete compaction** | ✅ Find files with pos deletes | ✅ Read data + pos deletes | ✅ Filter by position | ✅ Write clean files | ✅ rewrite_files | `RewritePositionDeleteFiles` |
| 9 | **Eq-to-pos conversion** | ✅ Match eq deletes to data | ✅ Read data + eq deletes | ✅ Inner join (find positions) | ✅ Write pos delete files | ✅ row_delta | `ConvertEqualityDeleteFiles` |
| 10 | **Orphan file deletion** | ✅ All snapshots (metadata) | ❌ (paths only) | ✅ Anti-join (paths) | ❌ | ❌ (storage delete) | `DeleteOrphanFiles` |
| 11 | **Expire snapshots** | ✅ All snapshots (metadata) | ❌ | ✅ Set difference (paths) | ❌ | ✅ Remove snapshot refs | `ExpireSnapshots` |
| 12 | **Rewrite manifests** | ✅ Read manifests | ❌ (metadata only) | ❌ | ❌ (write manifests) | ✅ rewrite_manifests | `RewriteManifests` |
| 13 | **Z-Order sort** | ✅ Select files | ✅ Read all files | ✅ Sort (Z-order key) | ✅ Write sorted output | ✅ rewrite_files | `RewriteDataFiles` (Z-Order) |
| 14 | **Sort-order enforce** | ❌ (write path) | ❌ | ✅ Sort before write | ✅ Write sorted | ✅ append | Write path |
| 15 | **Dynamic partition overwrite** | ✅ Detect partitions | ✅ Read for partition detect | ✅ Hash aggregate | ✅ Write partitioned | ✅ replace_partitions | `ReplacePartitions` |
| 16 | **Remove dangling deletes** | ✅ Cross-reference metadata | ❌ | ✅ Set diff (metadata) | ❌ | ✅ Remove delete files | `RemoveDanglingDeleteFiles` |
| 17 | **Compute table stats** | ✅ All data files | ✅ Read all files | ✅ NDV sketches | ❌ (write Puffin) | ✅ statistics | `ComputeTableStats` |

### 3.3 Operation Classification by Backend Requirement

```
LEGEND:
  S = Scan Planning (always PyIceberg)
  R = Read Data (IOBackend)
  C = Compute (ComputeBackend — may need spill)
  W = Write Data (IOBackend)
  X = Commit (always PyIceberg)
```

| Operation | Needs | Backend capability required |
|-----------|-------|---------------------------|
| Table scan | S+R+C | ComputeBackend with anti-join (for eq deletes) |
| Append | W+X | IOBackend only (no compute needed) |
| Overwrite / Delete (CoW) | S+R+C+W+X | ComputeBackend with filter (streaming) |
| Upsert | S+R+C+W+X | ComputeBackend with hash join + spill |
| Compaction | S+R+C+W+X | ComputeBackend with sort + spill |
| Orphan file deletion | S+C | ComputeBackend with anti-join (paths only, no data read) |
| Expire snapshots | S+C+X | ComputeBackend with set diff (metadata scale) |
| Rewrite manifests | S+X | No backend needed (metadata-only) |
| Position delete compaction | S+R+C+W+X | ComputeBackend with filter + spill |
| Eq-to-pos conversion | S+R+C+W+X | ComputeBackend with join + spill |
| Z-Order sort | S+R+C+W+X | ComputeBackend with sort + spill + UDF |
| Compute table stats | S+R+C+X | ComputeBackend with aggregate (sketches) |

### 3.4 How Deep the Abstraction Goes

The pluggable interface needs to abstract **four layers** below scan planning:

```mermaid
graph TB
    subgraph "Layer 0: Scan Planning (ALWAYS PyIceberg)"
        SP["ScanPlanner<br/>ManifestPlanner / REST planner<br/>Output: FileScanTask[]"]
    end

    subgraph "Layer 1: File I/O (IOBackend)"
        READ["read_parquet(path, schema, filter)<br/>→ Iterator[RecordBatch]"]
        WRITE["write_parquet(batches, path)<br/>→ DataFile metadata"]
        STATS["collect_statistics(path)<br/>→ ColumnStatistics"]
    end

    subgraph "Layer 2: Compute (ComputeBackend)"
        SORT["sort(batches, keys, memory_limit)<br/>→ Iterator[RecordBatch]"]
        JOIN["anti_join / hash_join(left, right, cols, memory_limit)<br/>→ Iterator[RecordBatch]"]
        FILTER["filter(batches, predicate)<br/>→ Iterator[RecordBatch]"]
        AGG["aggregate(batches, grouping, funcs)<br/>→ Iterator[RecordBatch]"]
    end

    subgraph "Layer 3: Delete Resolution (ComputeBackend — specialized)"
        POS_DEL["apply_positional_deletes(data_batches, pos_delete_batches)<br/>→ Iterator[RecordBatch]"]
        EQ_DEL["resolve_equality_deletes(data_batches, eq_delete_batches, cols)<br/>→ Iterator[RecordBatch]"]
    end

    subgraph "Layer 4: Object Store Access"
        OBJ["ObjectStoreAccess<br/>S3/GCS/ADLS/local filesystem<br/>Credential management"]
    end

    SP -->|"FileScanTask[]"| READ
    SP -->|"FileScanTask[]"| POS_DEL
    SP -->|"FileScanTask[]"| EQ_DEL
    READ --> SORT
    READ --> JOIN
    READ --> FILTER
    SORT --> WRITE
    JOIN --> WRITE
    FILTER --> WRITE
    POS_DEL --> FILTER
    EQ_DEL --> JOIN
    READ --> OBJ
    WRITE --> OBJ
```

### 3.5 The Special Case: Orphan File Deletion

Orphan file deletion is unique — it deals with **object listing**, not data files:

```python
# Orphan deletion flow:
# 1. List ALL objects in storage prefix (storage listing)
# 2. Enumerate ALL valid file paths across ALL snapshots (manifest scanning)
# 3. Anti-join: orphans = storage_paths \ valid_paths
# 4. Delete orphans from storage

# The "data" here is just path strings — not Parquet content.
# But the SCALE can be millions of paths → still needs bounded-memory anti-join.
```

This doesn't use `IOBackend.read_parquet()` at all — it uses `ComputeBackend.anti_join()`
on string arrays. The scan planning phase enumerates valid paths from manifests
(the metadata OOM problem — needs streaming).

### 3.6 The Special Case: Metadata-Only Operations

Some operations touch only metadata, not data files:

| Operation | Data touched | Backend needed? |
|-----------|-------------|:---:|
| Rewrite manifests | Manifest files (Avro, KB-MB each) | ❌ No backend — just rewrite Avro files |
| Expire snapshots (metadata) | Snapshot refs | ❌ No backend — remove refs from metadata |
| Remove dangling deletes | Manifest entries | ❌ Cross-reference metadata, no data read |

These stay entirely in PyIceberg with no backend involvement.

---

## 4. The Scan Planning OOM Problem

### 4.1 Where Metadata Materializes

Scan planning itself can OOM for operations that enumerate large metadata sets:

| Operation | Metadata enumerated | Scale | OOM Risk |
|-----------|-------------------|-------|:---:|
| Normal scan | Manifests for selected partitions | O(partitions hit) | Low |
| Orphan deletion | ALL file paths across ALL snapshots | O(total_files × snapshots) | **High** |
| Expire snapshots | File paths in expired vs retained | O(total_files) | **High** |
| Compaction file selection | Files in target partitions | O(files_in_partition) | Low |
| Full table stats | All data files | O(total_files) | Medium |

### 4.2 What Java Iceberg Does (And Why We Can Do Better)

Java Iceberg does NOT solve this problem. The Spark driver node holds all manifest
entries and file metadata in memory. For extremely large tables, users hit Spark
driver OOM with errors like:

```
java.lang.OutOfMemoryError: Java heap space
  at org.apache.iceberg.BaseTableScan.planFiles(...)
```

The standard fix in the Java ecosystem is operational: "bump `spark.driver.memory`
to 16GB." This is documented as a known limitation — the planning/coordination node
must be large enough to hold the full metadata set.

**We can do better.** Python's generator model makes streaming metadata natural and
nearly free. There is no reason to accept the same limitation Java has.

### 4.3 The Streaming Approach: Always, Not Conditionally

**Principle:** Apply the streaming pattern to ALL new operations unconditionally.
The overhead for small metadata (100 files) is negligible (microseconds to yield 100
items). The benefit for large metadata (10M files) is the difference between working
and OOM. Since we cannot predict metadata scale, we design for the limit.

This is the same principle from Section 5.2 of `datafusion_direction.md`: never branch
on assumed data size.

```python
# THE PATTERN (used by ALL new operations):
def _iter_metadata(table, operation_filter) -> Iterator[MetadataEntry]:
    """Generator — O(1) memory per yield, regardless of total entries."""
    for snapshot in table.snapshots():
        for manifest in snapshot.manifests(table.io):
            for entry in manifest.fetch_manifest_entry(table.io):
                if operation_filter(entry):
                    yield entry  # O(1) memory — never accumulates

# Consuming the generator (two options):

# Option A: Direct streaming into compute backend
batches = _batch_iterator(_iter_metadata(table, filter), batch_size=8192)
ctx.register_record_batches("metadata", batches)

# Option B: Stream to temp Parquet (for operations needing random access)
tmp_path = _stream_to_parquet(_iter_metadata(table, filter), schema)
ctx.register_parquet("metadata", tmp_path)
```

### 4.4 Rollout Strategy

| Scope | Approach | Risk |
|-------|----------|------|
| **ALL new operations** (orphan deletion, expire snapshots, compaction, etc.) | Streaming from day one | Zero — no existing behavior to break |
| **Existing scan planning** (`_plan_files_local`, `DataScan.plan_files()`) | Migrate incrementally as follow-up PRs | Low — partition pruning already bounds most scans |

For existing scan planning, the migration is straightforward: replace list accumulation
with generator yields. The `ManifestPlanner` already iterates manifests — it just
currently materializes results into a list. Converting to a generator is a small,
reviewable change per-subsystem.

### 4.5 Formal Memory Guarantee

**Theorem (Streaming Metadata Bound):** With the generator pattern applied universally:

```
M_planning(Op) = O(batch_size × entry_size)  for ALL operations
               ≈ O(8192 × 500B) = 4MB       regardless of table scale
```

Compare to Java Iceberg:
```
M_planning_java(Op) = O(total_files × entry_size)  [unbounded, OOMs at scale]
```

PyIceberg achieves asymptotically better memory behavior for scan planning than
Java Iceberg — by choosing streaming over materialization. This is a genuine
architectural advantage of the Python implementation.

---

## 5. The Complete Pluggable Interface

### 5.1 Formal Protocol Definitions

Based on the operation universe analysis, the complete interface is:

```python
class IOBackend(Protocol):
    """Layer 1: Who reads/writes Parquet files."""

    def read_parquet(
        self,
        location: str,
        projected_schema: Schema,
        row_filter: BooleanExpression,
        io_properties: dict[str, str],
    ) -> Iterator[pa.RecordBatch]: ...

    def write_parquet(
        self,
        batches: Iterator[pa.RecordBatch],
        location: str,
        schema: Schema,
        write_properties: dict[str, str],
        io_properties: dict[str, str],
    ) -> DataFile: ...

    def collect_statistics(
        self,
        location: str,
        schema: Schema,
        io_properties: dict[str, str],
    ) -> dict[int, ColumnStatistics]: ...

    def list_objects(
        self,
        prefix: str,
        io_properties: dict[str, str],
    ) -> Iterator[str]: ...  # For orphan file deletion


class ComputeBackend(Protocol):
    """Layer 2: Who does sort/join/filter/aggregate on Arrow data."""

    @property
    def supports_bounded_memory(self) -> bool: ...

    def sort(
        self,
        data: Iterator[pa.RecordBatch],
        sort_keys: list[tuple[str, str]],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]: ...

    def anti_join(
        self,
        left: Iterator[pa.RecordBatch],
        right: Iterator[pa.RecordBatch],
        on: list[str],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]: ...

    def hash_join(
        self,
        left: Iterator[pa.RecordBatch],
        right: Iterator[pa.RecordBatch],
        on: list[str],
        join_type: Literal["inner", "left", "right", "outer"],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]: ...

    def filter(
        self,
        data: Iterator[pa.RecordBatch],
        predicate: BooleanExpression,
    ) -> Iterator[pa.RecordBatch]: ...

    def aggregate(
        self,
        data: Iterator[pa.RecordBatch],
        group_by: list[str],
        aggregations: list[tuple[str, str]],  # (column, function)
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]: ...


class ExecutionBackend(Protocol):
    """Layer 3 (composite): Executes complete scan tasks using IO + Compute."""

    def execute_scan(
        self,
        tasks: Iterable[FileScanTask],
        projected_schema: Schema,
        row_filter: BooleanExpression,
        io_properties: dict[str, str],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]: ...
```

### 5.2 Why Three Protocols (Not One)

**Separation of Concerns (Dijkstra):** IO, Compute, and composite execution are
independent concerns. A backend might implement IO but not compute (Polars reads
but can't spill). Another might implement compute but not IO (DataFusion for
sort only, PyArrow for read/write).

**Interface Segregation (Martin):** Clients should not depend on methods they don't
use. `table.append()` only needs `IOBackend.write_parquet()`. Forcing it to depend
on `ComputeBackend.sort()` would be unnecessary coupling.

**Composition:** The `ExecutionBackend` composes `IOBackend` + `ComputeBackend` for
operations that need both (scan with delete resolution). Simple operations use
individual protocols directly.

---

## 6. How Each Operation Maps to the Interface

### 6.1 Detailed Operation → Protocol Mapping

| Operation | IOBackend methods used | ComputeBackend methods used | Notes |
|-----------|----------------------|---------------------------|-------|
| **Scan (no deletes)** | `read_parquet` | `filter` | Simple: read + filter residual |
| **Scan (pos deletes)** | `read_parquet` (data + delete files) | `filter` (by position) | PyIceberg determines which deletes apply |
| **Scan (eq deletes)** | `read_parquet` (data + delete files) | `anti_join` | The critical OOM operation |
| **Append** | `write_parquet` | — | No compute needed |
| **Delete (CoW)** | `read_parquet` + `write_parquet` | `filter` (complement) | Stream: read → filter → write |
| **Overwrite** | `read_parquet` + `write_parquet` | `filter` | Same as delete |
| **Upsert** | `read_parquet` + `write_parquet` | `hash_join` (inner for updates) + `anti_join` (for inserts) | Most complex operation |
| **Compaction** | `read_parquet` (many files) + `write_parquet` | `sort` | External merge sort with spill |
| **Orphan deletion** | `list_objects` | `anti_join` (paths) | No Parquet read — just path strings |
| **Expire snapshots** | — | `anti_join` (paths) | Metadata paths only |
| **Pos delete compaction** | `read_parquet` + `write_parquet` | `filter` (exclude positions) | Streaming filter |
| **Eq-to-pos conversion** | `read_parquet` + `write_parquet` | `hash_join` (find positions) | Join to discover positions |
| **Z-Order sort** | `read_parquet` + `write_parquet` | `sort` (with UDF for Z-key) | Same as compaction + computed key |
| **Compute stats** | `read_parquet` | `aggregate` (NDV sketches) | Full table scan + aggregation |

### 6.2 Coverage Verification

Every operation in Java Iceberg's action set can be expressed as a composition of:
- `IOBackend.{read_parquet, write_parquet, list_objects, collect_statistics}`
- `ComputeBackend.{sort, anti_join, hash_join, filter, aggregate}`
- PyIceberg semantics (scan planning, commit protocol)

**Theorem (Completeness):** The protocol set `{IOBackend, ComputeBackend}` is
sufficient to implement all operations in Java Iceberg's action set, given that
scan planning and commit remain in PyIceberg.

**Proof:** By construction — the table in 6.1 maps every Java operation to a
combination of protocol methods. No operation requires a method not in the protocol. ∎

---

## 7. Implementation Strategy

### 7.1 The Fowler Principle Applied Aggressively

> "When you have two or three implementations of something, then you can see what
> the interface should be. When you have one implementation, you're just guessing."

Rather than building one backend (DataFusion) and hoping the interface is correct,
we build **three backends in the first PR**: PyArrow, DataFusion, and DuckDB. This
gives us enough implementations to derive the correct interface from empirical
observation rather than speculation.

The cost of building all three upfront is low because:
- PyArrow: already exists — we're just extracting it behind the protocol
- DataFusion: the primary new implementation (~200 lines of compute functions)
- DuckDB: structurally similar to DataFusion (SQL-based, Arrow-native, ~150 lines)

### 7.2 The Single PR: Three Backends, One Protocol

```
pyiceberg/execution/
├── __init__.py              # Re-exports
├── engine.py                # resolve_backend() — auto-detect what's installed
├── protocol.py              # IOBackend + ComputeBackend Protocol definitions
├── session.py               # Shared utilities (memory limit parsing, etc.)
├── backends/
│   ├── __init__.py
│   ├── pyarrow_backend.py   # Extract from existing pyarrow.py monolith
│   ├── datafusion_backend.py # New: DataFusion for IO + compute (spill-capable)
│   └── duckdb_backend.py    # New: DuckDB for IO + compute (spill-capable)
└── object_store.py          # Translate FileIO props → backend-specific config
```

### 7.3 Why Three (Not Two) Before Extracting the Interface

With two implementations, the "common interface" might be an accident of similarity
between two specific libraries. With three, patterns that hold across all three are
genuinely general:

| Test | Two impls (PyArrow + DataFusion) | Three impls (+ DuckDB) |
|------|---|---|
| `sort(data, keys, memory_limit)` | Both accept this signature ✓ | DuckDB also accepts ✓ → **the signature is correct** |
| `anti_join(left, right, cols, memory_limit)` | Both accept ✓ | DuckDB also accepts ✓ → **correct** |
| Expression format | PyArrow: `pc.Expression`, DF: SQL string | DuckDB: also SQL → **SQL string is the right abstraction for the interface** |
| Object store config | PyArrow: `pyarrow.fs`, DF: `RuntimeEnvBuilder` | DuckDB: `SET s3_region` → **each backend needs its own, not shared** |

The third implementation validates (or refutes) interface decisions made based on
the first two. It catches over-fitting to PyArrow+DataFusion specifics.

### 7.4 Concrete Implementation Plan

**PR 1: Foundation + Three Backends (the interface-defining PR)**

| Component | Work | Lines (est.) |
|-----------|------|:---:|
| `protocol.py` | Define `IOBackend` + `ComputeBackend` + `ExecutionBackend` protocols | ~80 |
| `engine.py` | Auto-detect installed backends, resolve preferred | ~60 |
| `pyarrow_backend.py` | Extract `ArrowScan` read logic + `write_file` + sort/filter into protocol | ~300 (moved, not new) |
| `datafusion_backend.py` | `SessionContext` + `register_parquet` + SQL for sort/join/filter | ~200 |
| `duckdb_backend.py` | `duckdb.connect()` + `read_parquet` + SQL for sort/join/filter | ~150 |
| `object_store.py` | FileIO props → backend-specific object store config | ~100 |
| Tests | Each backend produces identical output for same input | ~300 |

Total new code: ~900 lines. Moved code: ~300 lines (from `pyarrow.py` monolith).

**PR 2: Wire into existing operations**

Hook the backend resolution into `DataScan.to_arrow()`, `Transaction.delete()`,
`Transaction.upsert()`. Existing behavior unchanged when `engine.py` resolves to
PyArrow (which it does by default if nothing else is installed).

**PR 3+: New operations using the backend**

`table.compact()`, `table.delete_orphan_files()`, etc. — these use the protocol
from day one and benefit from whichever backend is available.

### 7.5 The Backend Resolution Logic

```python
# pyiceberg/execution/engine.py

def resolve_backend(operation: str) -> tuple[IOBackend, ComputeBackend]:
    """Resolve the best available backends for the given operation.

    Priority (highest first):
      1. DataFusion (if installed) — bounded memory, Apache 2.0
      2. DuckDB (if installed) — bounded memory, BSL caveat for S3
      3. PyArrow (always available) — fallback, in-memory only
    """
    try:
        from pyiceberg.execution.backends.datafusion_backend import (
            DataFusionIOBackend, DataFusionComputeBackend
        )
        return DataFusionIOBackend(), DataFusionComputeBackend()
    except ImportError:
        pass

    try:
        from pyiceberg.execution.backends.duckdb_backend import (
            DuckDBIOBackend, DuckDBComputeBackend
        )
        return DuckDBIOBackend(), DuckDBComputeBackend()
    except ImportError:
        pass

    from pyiceberg.execution.backends.pyarrow_backend import (
        PyArrowIOBackend, PyArrowComputeBackend
    )
    warnings.warn(
        f"'{operation}' using PyArrow (in-memory only, may OOM on large data). "
        f"Install 'pyiceberg[datafusion]' or 'pyiceberg[duckdb]' for bounded-memory execution.",
        UserWarning, stacklevel=3,
    )
    return PyArrowIOBackend(), PyArrowComputeBackend()
```

### 7.6 Why This Approach Is Sound

**Fowler's principle (satisfied):** Three implementations → the shared interface is
derived from observation, not guessed from one.

**Open-Closed principle (satisfied):** Adding a fourth backend (Polars, cuDF, Ray)
requires only implementing the protocol — no changes to existing code.

**Dependency Inversion (satisfied):** PyIceberg's table operations depend on the
abstract `Protocol`, not on concrete backend classes.

**YAGNI (satisfied):** We're not building speculative infrastructure. DuckDB is a
concrete, working library we can test today. It validates the interface while also
being useful to DuckDB users.

---

## 8. The First PR: What It Must Prove

### 8.1 Requirements for Correctness

The first PR (three backends + protocol) must demonstrate:

1. **Functional equivalence:** For identical input, ALL THREE backends produce
   identical output (same rows, same values, same order for ordered ops).

2. **Bounded memory (DataFusion + DuckDB):** Both spill-capable backends complete
   within configured `memory_limit` for inputs that OOM the PyArrow backend.

3. **Protocol generality:** The protocol definitions naturally fit all three
   implementations without any backend needing special-case accommodations.
   This proves the interface is correct.

4. **Streaming contract:** All interfaces use `Iterator[pa.RecordBatch]` (not
   `pa.Table`), ensuring bounded memory through the entire pipeline regardless
   of backend.

5. **No semantic coupling:** The backend functions know nothing about Iceberg.
   They receive Arrow data, perform compute, return Arrow data.

6. **Capability declaration:** PyArrow backend declares
   `supports_bounded_memory = False`. DataFusion and DuckDB declare `True`.
   Operations that require bounded memory only dispatch to capable backends.

### 8.2 Evidence for Interface Correctness (Three Implementations)

For each protocol method, the PR provides three working implementations:

| Protocol method | PyArrow impl | DataFusion impl | DuckDB impl |
|----------------|---|---|---|
| `read_parquet(path, schema, filter)` | `pq.ParquetFile` + `Scanner` | `ctx.register_parquet()` + SQL | `duckdb.read_parquet()` |
| `write_parquet(batches, path)` | `pq.ParquetWriter` | Delegates to PyArrow | `duckdb.write_parquet()` or delegate |
| `sort(data, keys, limit)` | `pa.Table.sort_by()` | `ctx.sql("ORDER BY")` | `con.sql("ORDER BY")` |
| `anti_join(left, right, cols, limit)` | `pc.is_in()` (limited) | `ctx.sql("LEFT ANTI JOIN")` | `con.sql("LEFT ANTI JOIN")` |
| `filter(data, predicate)` | `table.filter(pc.Expression)` | `ctx.sql("WHERE ...")` | `con.sql("WHERE ...")` |

If the same protocol signature works for all three with no special-casing → the
protocol is correct by construction.

### 8.3 Documentation for Future Backend Contributors

The PR includes `pyiceberg/execution/BACKENDS.md` documenting:
- Protocol definitions with full type signatures
- Contract for each method (what inputs mean, what output guarantees)
- `memory_limit` semantics (MUST honor if `supports_bounded_memory`, best-effort otherwise)
- How to register a new backend
- Testing requirements (must pass the equivalence test suite)
- Example: skeleton for a new backend implementation

### 8.4 The Test Suite

```python
# tests/execution/test_backend_equivalence.py

@pytest.fixture(params=["pyarrow", "datafusion", "duckdb"])
def backend(request):
    """Parametrize all tests across all available backends."""
    ...

def test_sort_produces_same_output(backend, sample_data):
    """All backends sort identically."""
    result = backend.compute.sort(sample_data, keys=["id"], memory_limit=None)
    expected = sorted_reference(sample_data, keys=["id"])
    assert_arrow_equal(result, expected)

def test_anti_join_produces_same_output(backend, left_data, right_data):
    """All backends anti-join identically."""
    result = backend.compute.anti_join(left_data, right_data, on=["id"])
    expected = anti_join_reference(left_data, right_data, on=["id"])
    assert_arrow_equal(result, expected)

def test_bounded_memory_sort(spill_capable_backend, large_data):
    """Spill-capable backends complete within memory_limit."""
    # large_data > memory_limit → must spill
    result = spill_capable_backend.compute.sort(
        large_data, keys=["ts"], memory_limit=64_000_000  # 64MB
    )
    assert result is not None  # completed without OOM
```

---

## 9. Speed-of-Light Analysis

### 9.1 The Overhead of the Pluggable Layer

```
T_dispatch = O(1) — one Python attribute lookup to select backend
T_operation = O(N/D) — dominated by I/O or compute
T_dispatch / T_operation ≈ 10⁻⁸ (negligible)
```

### 9.2 Streaming Everywhere

The `Iterator[pa.RecordBatch]` contract ensures:
```
M_pipeline = O(batch_size) for streaming operations (filter, read)
M_stateful = O(memory_limit) for stateful operations (sort, join)
M_total = max(M_pipeline, M_stateful) = O(memory_limit)
```

### 9.3 End-to-End Memory Bound

```
M_total(Op) = M_scan_planning + M_execution
            = O(batch_size)    + O(memory_limit)     [with streaming metadata]
            = O(memory_limit)                        [since memory_limit >> batch_size]
```

This holds for ALL operations, ALL table sizes, with NO branching on assumed scale.

---

## 10. Summary

| Layer | Responsibility | Pluggable? | Examples |
|-------|---------------|:---:|---|
| **Scan Planning** | Which files, which deletes, which filter | ❌ Always PyIceberg | ManifestPlanner, DeleteFileIndex |
| **IO Backend** | Read/write Parquet, list objects | ✅ Pluggable | PyArrow, DataFusion, DuckDB |
| **Compute Backend** | Sort, join, filter, aggregate | ✅ Pluggable (with capability gate) | DataFusion (spill), PyArrow (fallback) |
| **Commit** | Atomic snapshot update | ❌ Always PyIceberg | Transaction, OCC |

The `FileScanTask` is the handoff point between scan planning and execution.
Everything above it (manifest reading, partition pruning, delete file matching)
is Iceberg semantics — stays in PyIceberg. Everything below it (reading Parquet,
computing joins, writing output) is pluggable via the `IOBackend` + `ComputeBackend`
protocols.
