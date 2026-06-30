# Pluggable Backend Discovery: iceberg-python Codebase Analysis

Branch: `pluggable-backend-discovery` (iceberg-python)
Base: `main` @ commit `9d36e236` (Bump to Java Iceberg 1.11.0)

---

## 1. Current Architecture: The Exact Data Path

### 1.1 The Read Pipeline (scan → Arrow output)

```mermaid
graph TD
    subgraph "User API"
        A["table.scan().to_arrow()"]
    end

    subgraph "Scan Planning (engine-agnostic, PyIceberg semantics)"
        B["DataScan.plan_files()"]
        C{"Server-side planning?"}
        D["_plan_files_server_side()<br/>(REST catalog does planning)"]
        E["_plan_files_local()<br/>(ManifestGroupPlanner)"]
        F["Output: Iterable[FileScanTask]"]
    end

    subgraph "Handoff (the boundary)"
        G["_to_arrow_via_file_scan_tasks()"]
    end

    subgraph "Data Execution (PyArrow-hardcoded)"
        H["ArrowScan(table_metadata, io, schema, filter, limit)"]
        I["ArrowScan.to_table(tasks)<br/>OR .to_record_batches(tasks)"]
        J["For each FileScanTask:<br/>1. io.new_input(path).open()<br/>2. PyArrow fragment + Scanner<br/>3. Predicate pushdown<br/>4. Positional delete application<br/>5. Schema projection"]
        K["Output: pa.Table / Iterator[RecordBatch]"]
    end

    A --> B --> C
    C -->|yes| D --> F
    C -->|no| E --> F
    F --> G --> H --> I --> J --> K
```

### 1.2 The Write Pipeline (Arrow → Parquet files)

```mermaid
graph TD
    subgraph "User API"
        A["table.append(df) / table.overwrite(df)"]
    end

    subgraph "Write Orchestration (table/__init__.py)"
        B["Transaction.append() / overwrite()"]
        C["Calls _dataframe_to_data_files()"]
    end

    subgraph "Data Writing (PyArrow-hardcoded, pyarrow.py)"
        D["_dataframe_to_data_files(table_metadata, df, io)"]
        E{"df type?"}
        F["RecordBatchReader → bin_pack_record_batches()"]
        G["pa.Table → bin_pack_arrow_table() / _determine_partitions()"]
        H["write_file(io, table_metadata, tasks: Iterator[WriteTask])"]
        I["For each WriteTask:<br/>1. Project to file schema<br/>2. pq.ParquetWriter<br/>3. Collect statistics<br/>4. Return DataFile metadata"]
    end

    subgraph "Commit (engine-agnostic)"
        J["Transaction.commit() → snapshot update"]
    end

    A --> B --> C --> D --> E
    E -->|RecordBatchReader| F --> H
    E -->|pa.Table| G --> H
    H --> I --> J
```

### 1.3 The Delete Resolution Pipeline

```mermaid
graph TD
    subgraph "During ArrowScan.to_record_batches()"
        A["_read_all_delete_files(io, tasks)<br/>Reads ALL delete files upfront → dict[file_path, list[ChunkedArray]]"]
        B["For each FileScanTask:<br/>_task_to_record_batches()"]
        C["If positional deletes exist for this file:<br/>_combine_positional_deletes(positions, start, end)<br/>→ indices to KEEP"]
        D["batch.take(keep_indices)"]
        E["Apply residual filter if present"]
    end

    subgraph "NOT SUPPORTED"
        F["Equality deletes → raise ValueError"]
    end

    A --> B --> C --> D --> E
    B -.->|"equality deletes"| F
```

---

## 2. The Exact Coupling Points (What Uses PyArrow)

### 2.1 File: `pyiceberg/io/pyarrow.py` (3,100+ lines)

| Line Range | Responsibility | PyArrow APIs Used | Pluggable? |
|-----------|---------------|-------------------|:---:|
| 246-390 | `PyArrowFileIO` + `PyArrowFile` | `pyarrow.fs.*` | Already abstract (FileIO ABC) |
| 699-710 | `schema_to_pyarrow()` | `pa.Schema`, visitor pattern | Shared infra (all backends need Arrow Schema) |
| 853-1069 | Expression → PyArrow filter | `pc.Expression`, `pc.field()`, `pc.scalar()` | **Backend-specific** |
| 1120-1165 | `_read_deletes()` | `ds.Scanner.from_fragment().to_table()`, `pc.filter` | **Backend-specific** |
| 1167-1220 | `pyarrow_to_schema()` | `pa.Schema` visitor | Shared infra |
| 1616-1727 | `_task_to_record_batches()` | `ds.Scanner.from_fragment()`, `pq.ParquetFile` equivalent | **Core read — must be abstracted** |
| 1728-1870 | `ArrowScan` class | Orchestrates tasks → batches | **The execution layer to replace** |
| 2198-2500 | `StatsAggregator`, statistics | Parquet metadata access | **Backend-specific** |
| 2617-2700 | `write_file()` | `pq.ParquetWriter` | **Core write — must be abstracted** |
| 2869-2970 | `_dataframe_to_data_files()` | `pa.Table`, `pa.RecordBatchReader` | **Write orchestration — must be abstracted** |

### 2.2 File: `pyiceberg/table/__init__.py` (key coupling points)

| Line | What | Coupling |
|------|------|---------|
| 2186-2215 | `_to_arrow_via_file_scan_tasks()`, `_to_arrow_batch_reader_via_file_scan_tasks()` | Import `ArrowScan` from pyarrow.py — **the bridge functions to replace** |
| 2285-2315 | `DataScan.to_arrow()`, `.to_arrow_batch_reader()` | Call the bridge functions |
| 460-800 | `Transaction.append()`, `.overwrite()`, `.delete()`, `.upsert()` | Call `_dataframe_to_data_files()` from pyarrow.py |
| 2721-2755 | `WriteTask` dataclass | Uses `pa.RecordBatch` in type — Arrow FORMAT coupling (permanent, correct) |

### 2.3 Other files with PyArrow coupling

| File | Nature |
|------|--------|
| `pyiceberg/table/upsert_util.py` | Uses `pa.Table`, `pc.is_in()` for key matching — **compute coupling** |
| `pyiceberg/table/inspect.py` | Uses `pa.table()` for metadata inspection results — lightweight |
| `pyiceberg/table/deletion_vector.py` | Uses `pa.ChunkedArray` for DV representation — Arrow format coupling |
| `pyiceberg/transforms.py` | Uses `pc.*` for transform computation OR delegates to `pyiceberg_core` |
| `pyiceberg/expressions/visitors.py` | Uses `pc.*` for expression evaluation — **backend-specific** |

---

## 3. The Natural Interface Boundary: FileScanTask

### 3.1 FileScanTask as the Contract

```python
@dataclass(init=False)
class FileScanTask(ScanTask):
    file: DataFile           # What to read (path, format, partition, stats)
    delete_files: set[DataFile]  # What deletes apply (positional only currently)
    residual: BooleanExpression  # Filter after partition pruning
```

This is already:
- **Engine-agnostic** — no PyArrow types in it
- **Complete** — contains everything a backend needs to produce correct output
- **Serializable** — can cross process boundaries (REST scan planning already serializes it)
- **The same structure regardless of who planned it** (local, REST, or Rust)

### 3.2 What a Backend Receives

To execute a scan, a backend needs:
1. `Iterable[FileScanTask]` — what to read and what deletes apply
2. `Schema` (Iceberg) — what columns to project
3. `BooleanExpression` (Iceberg) — residual filter
4. `FileIO` properties — how to access storage (S3 credentials, etc.)
5. `TableMetadata` — for schema reconciliation context
6. Optional: `memory_limit` — for bounded-memory execution

And returns:
- `Iterator[pa.RecordBatch]` — the Arrow data (streaming)

### 3.3 What a Write Backend Receives

To execute a write:
1. `Iterator[pa.RecordBatch]` — data to write
2. `Schema` (Iceberg) — target schema
3. `TableMetadata` — for properties (target file size, compression, etc.)
4. `FileIO` properties — where to write
5. Partition routing info — which partition each batch belongs to

And returns:
- `Iterable[DataFile]` — metadata about written files (for commit)

---

## 4. Engine API Discovery

### 4.1 DataFusion (datafusion-python)

```python
from datafusion import SessionContext, RuntimeEnvBuilder

# Session setup (bounded memory)
runtime = RuntimeEnvBuilder().with_fair_spill_pool(512_000_000).with_disk_manager_os()
ctx = SessionContext(runtime=runtime)

# READ: Register Parquet file, apply filter, project columns
ctx.register_parquet("data", "s3://bucket/file.parquet")
result = ctx.sql("SELECT col1, col2 FROM data WHERE col1 > 5 ORDER BY col2")
arrow_table = result.to_arrow_table()
# Or streaming: result.execute_stream() → RecordBatchStream

# COMPUTE: Sort
ctx.register_record_batches("input", [batches])
sorted_result = ctx.sql("SELECT * FROM input ORDER BY key")

# COMPUTE: Anti-join (equality delete resolution)
ctx.register_record_batches("data", [data_batches])
ctx.register_record_batches("deletes", [delete_batches])
resolved = ctx.sql("SELECT d.* FROM data d LEFT ANTI JOIN deletes e ON d.id = e.id")

# COMPUTE: Filter
ctx.register_record_batches("data", [batches])
filtered = ctx.sql("SELECT * FROM data WHERE status != 'deleted'")

# WRITE: DataFusion doesn't have a direct "write Parquet" from Python
# → Delegate to PyArrow ParquetWriter (or use Rust-side IcebergWriteExec via pyiceberg-core)

# OBJECT STORE: Configured via RuntimeEnvBuilder or environment
# Supports S3, GCS, ADLS, local via object_store crate (Apache 2.0)
```

**Key APIs:**
- `SessionContext(runtime=...)` — per-session memory isolation
- `RuntimeEnvBuilder().with_fair_spill_pool(N)` — explicit memory budget
- `ctx.register_parquet(name, path)` — read from storage
- `ctx.register_record_batches(name, batches)` — register existing Arrow data
- `ctx.sql(query)` — execute SQL
- `.to_arrow_table()` / `.execute_stream()` — get Arrow output

**Capabilities:** ✅ Sort+spill, ✅ Join+spill, ✅ Filter (streaming), ✅ Aggregate+spill
**Limitations:** No direct Parquet write from Python API (delegate to PyArrow)
**License:** Apache 2.0 (including object store)
**Memory model:** Per-session `FairSpillPool` — explicit, configurable, isolated

### 4.2 DuckDB (duckdb-python)

```python
import duckdb

# Session setup
con = duckdb.connect()
con.execute("SET memory_limit = '2GB'")
con.execute("SET temp_directory = '/tmp/duckdb_spill'")

# READ: Read Parquet with pushdown
result = con.execute("""
    SELECT col1, col2 FROM read_parquet('s3://bucket/file.parquet')
    WHERE col1 > 5
""")
arrow_table = result.to_arrow_table()  # was fetch_arrow_table(), deprecated

# COMPUTE: Sort
con.register("input", arrow_table)
sorted_result = con.execute("SELECT * FROM input ORDER BY key").to_arrow_table()

# COMPUTE: Anti-join
con.register("data_tbl", data_table)
con.register("deletes_tbl", delete_table)
resolved = con.execute("""
    SELECT d.* FROM data_tbl d
    LEFT ANTI JOIN deletes_tbl e ON d.id = e.id
""").to_arrow_table()

# WRITE: DuckDB can write Parquet
con.execute("COPY (SELECT * FROM input) TO 'output.parquet' (FORMAT PARQUET)")

# OBJECT STORE: httpfs extension (BSL license!)
con.execute("SET s3_region = 'us-east-1'")
con.execute("SET s3_access_key_id = '...'")
con.execute("SET s3_secret_access_key = '...'")
```

**Key APIs:**
- `duckdb.connect()` — create connection
- `con.execute("SET memory_limit = '2GB'")` — memory config (connection-wide)
- `con.register(name, arrow_table)` — register Arrow data
- `con.execute(sql).to_arrow_table()` — execute + get Arrow
- `read_parquet(path)` — direct Parquet read in SQL

**Capabilities:** ✅ Sort+spill, ✅ Join+spill, ✅ Filter, ✅ Aggregate, ✅ Write Parquet
**Limitations:** Connection-wide memory (not per-query), BSL license for S3
**License:** Core: MIT. httpfs (S3/GCS): **Business Source License** (non-open-source)
**Memory model:** Connection-wide `SET memory_limit` — less granular than DataFusion

### 4.3 Polars

```python
import polars as pl

# READ: Lazy scan with pushdown
lf = pl.scan_parquet("s3://bucket/file.parquet")
result = lf.filter(pl.col("col1") > 5).select(["col1", "col2"]).collect()
arrow_table = result.to_arrow()

# COMPUTE: Sort (in-memory only, no spill)
sorted_df = df.sort("key")

# COMPUTE: Anti-join (in-memory only)
resolved = data_df.join(deletes_df, on="id", how="anti")

# WRITE: Write Parquet
df.write_parquet("output.parquet")

# OBJECT STORE: Built-in cloud support
# Configured via storage_options={"aws_access_key_id": "...", ...}
```

**Key APIs:**
- `pl.scan_parquet(path, storage_options={})` — lazy Parquet scan
- `.filter()`, `.select()`, `.sort()`, `.join()` — lazy expressions
- `.collect()` — execute the lazy plan
- `.to_arrow()` — convert to Arrow

**Capabilities:** ✅ Sort (in-memory), ✅ Join (in-memory), ✅ Filter (streaming in lazy mode), ✅ Write
**Limitations:** ❌ No spill-to-disk for sort or join. Large data OOMs.
**License:** MIT
**Memory model:** No configurable limit. Uses all available RAM.

### 4.4 cuDF (RAPIDS)

```python
import cudf

# READ: GPU-accelerated Parquet read
gdf = cudf.read_parquet("file.parquet")

# COMPUTE: GPU sort
sorted_gdf = gdf.sort_values("key")

# COMPUTE: GPU anti-join
resolved = data_gdf.merge(deletes_gdf, on="id", how="leftanti")

# EXCHANGE: GPU → CPU Arrow
cpu_table = sorted_gdf.to_arrow()

# LIMITATIONS:
# - Requires NVIDIA GPU + CUDA
# - Data must fit in GPU VRAM (8-80GB)
# - No spill to system RAM or SSD equivalent to DataFusion
# - S3 access via separate cudf.io or fsspec
```

**Capabilities:** ✅ Sort (GPU), ✅ Join (GPU), ✅ Filter (GPU), ✅ Arrow interop
**Limitations:** ❌ Hardware-dependent, ❌ VRAM-limited (no spill), ❌ Not pip-installable on all systems
**License:** Apache 2.0
**Memory model:** GPU VRAM. No configurable CPU memory budget.

### 4.5 Ray (ray.data)

```python
import ray

# READ: Distributed Parquet read
ds = ray.data.read_parquet("s3://bucket/table/")

# COMPUTE: Distributed sort (across workers)
sorted_ds = ds.sort("key")

# COMPUTE: No native anti-join
# Must convert to Arrow/pandas per-block and use another library

# EXCHANGE: To Arrow
arrow_table = ds.to_arrow_refs()  # distributed Arrow blocks
```

**Key insight:** Ray is NOT a compute backend — it's a **distribution layer**. It
distributes work across machines. Per-worker compute still needs PyArrow/DataFusion/DuckDB.
It doesn't implement sort/join itself for single-node bounded memory.

**Capabilities:** ✅ Distributed reads, ⚠️ Sort (distributed, not bounded-memory per-node)
**Limitations:** ❌ Not a single-node compute engine, ❌ No anti-join primitive
**Role:** Orchestration layer ABOVE our backend protocol (see Section 6 of pluggable_scan_task.md)

---

## 5. Protocol Derivation: What's Common Across All Engines

### 5.1 Read Operation

Every engine accepts:
- A file path (string)
- Column projection (list of column names or indices)
- A filter predicate (each engine's own format)

And returns: Arrow RecordBatch/Table

```python
# The common signature (derived from all engines):
def read_parquet(
    location: str,
    projected_columns: list[str],
    filter_expression: BooleanExpression,  # Iceberg expression, converted per-backend
    io_properties: dict[str, str],         # Credentials, region, endpoint
) -> Iterator[pa.RecordBatch]:
```

**Per-backend variation:** Expression format
- DataFusion: SQL string (`"col1 > 5 AND col2 = 'x'"`)
- DuckDB: SQL string (same)
- Polars: `pl.col("col1") > 5` expression objects
- PyArrow: `pc.field("col1") > pc.scalar(5)` expression objects
- cuDF: Boolean mask or query string

**Resolution:** The protocol accepts `BooleanExpression` (Iceberg's own format). Each
backend implements its own converter: `expression_to_sql()`, `expression_to_polars()`, etc.
This is a small per-backend cost (~50-100 lines each for SQL-based engines).

### 5.2 Write Operation

Every engine accepts:
- Arrow data (RecordBatch/Table)
- Output path (string)
- Write properties (compression, row group size, etc.)

And returns: File metadata (size, statistics)

```python
def write_parquet(
    batches: Iterator[pa.RecordBatch],
    location: str,
    schema: Schema,
    write_properties: dict[str, str],
    io_properties: dict[str, str],
) -> DataFile:
```

**Per-backend variation:** Minimal. All write Arrow → Parquet.
**Note:** DataFusion doesn't have a Python-side write API. It delegates to PyArrow.
This is fine — write is I/O-bound, not compute-bound. PyArrow writing is sufficient.

### 5.3 Compute Operations

| Operation | Common signature | Per-backend conversion |
|-----------|-----------------|----------------------|
| Sort | `sort(data: Iterator[RecordBatch], keys: list[str], memory_limit: int)` | SQL ORDER BY vs. `.sort_by()` vs. `.sort_values()` |
| Anti-join | `anti_join(left: Iterator[RB], right: Iterator[RB], on: list[str], memory_limit: int)` | SQL LEFT ANTI JOIN vs. `.join(how="anti")` vs. `pc.is_in()` |
| Filter | `filter(data: Iterator[RB], predicate: BooleanExpression)` | SQL WHERE vs. `.filter()` vs. expression |
| Hash join | `hash_join(left: Iterator[RB], right: Iterator[RB], on: list[str], join_type: str, memory_limit: int)` | SQL JOIN vs. `.join()` |

### 5.4 The Capability Declaration

```python
class ComputeBackend(Protocol):
    @property
    def supports_bounded_memory(self) -> bool:
        """Can this backend honor memory_limit (spill-to-disk)?"""
        ...

    @property
    def supports_anti_join(self) -> bool:
        """Can this backend perform LEFT ANTI JOIN natively?"""
        ...
```

| Backend | `supports_bounded_memory` | `supports_anti_join` |
|---------|:---:|:---:|
| DataFusion | ✅ | ✅ |
| DuckDB | ✅ | ✅ |
| Polars | ❌ | ✅ (in-memory only) |
| PyArrow | ❌ | ❌ (workaround via `pc.is_in`) |
| cuDF | ❌ (VRAM only) | ✅ (GPU) |

---

## 6. Identified Nuances and Edge Cases

### 6.1 Delete File Handling: The Critical Difference

Currently, `ArrowScan` reads ALL delete files upfront into memory (`_read_all_delete_files()`).
This is where OOM happens for large delete sets.

In the pluggable model:
- **Positional deletes:** Backend receives the delete file paths in `FileScanTask.delete_files`.
  It reads them and applies the position filter. This is a streaming operation (per-batch).
- **Equality deletes:** Backend receives data file + delete files. It performs an ANTI JOIN.
  This is the operation that REQUIRES spill-capable compute.

The interface must accommodate both:
```python
def execute_scan(
    tasks: Iterable[FileScanTask],  # Each task has .delete_files
    ...
) -> Iterator[pa.RecordBatch]:
    # Backend internally:
    # 1. For each task, read data file
    # 2. If task has positional deletes: apply position filter
    # 3. If task has equality deletes: anti-join against delete file(s)
    # 4. Apply residual filter
    # 5. Project to requested schema
```

### 6.2 Schema Reconciliation

Iceberg tables evolve schemas. A file written with schema v1 may be read with schema v5.
The `_task_to_record_batches()` function handles:
- Column projection by field ID (not name)
- Missing columns → fill with null
- Type promotion (int32 → int64)
- Column reordering

This logic is ABOVE the backend — it's Iceberg semantics. The backend produces raw
batches from the file; PyIceberg handles reconciliation via `_to_requested_schema()`.

**Decision:** Schema reconciliation stays in PyIceberg (shared logic). Backends just
read the physical file schema. PyIceberg transforms the output to match the projected schema.

### 6.3 Object Store Credentials

Each engine configures object store differently:
- **PyArrow:** `pyarrow.fs.S3FileSystem(access_key=..., secret_key=..., region=...)`
- **DataFusion:** `RuntimeEnvBuilder` or environment variables / `SessionContext` URL config
- **DuckDB:** `SET s3_region`, `SET s3_access_key_id`, etc.
- **Polars:** `storage_options={"aws_access_key_id": "...", ...}`
- **cuDF:** Environment variables or `fsspec` storage options

**Resolution:** The `IOBackend` interface accepts `io_properties: dict[str, str]` (same
dict PyIceberg's `FileIO` already uses). Each backend translates these to its native format.
This is a one-time `_configure_object_store(backend, io_properties)` call.

### 6.4 Streaming vs. Materialized Results

- **DataFusion:** Can stream results via `execute_stream()` → `RecordBatchStream`
- **DuckDB:** Results are materialized by `.to_arrow_table()` (can chunk with `.fetchmany()`)
- **Polars:** Lazy execution → `.collect()` materializes; no streaming iterator
- **PyArrow:** `Scanner.to_batches()` is streaming (iterator of RecordBatch)

**Resolution:** The protocol uses `Iterator[pa.RecordBatch]` as the output type.
Backends that materialize internally (DuckDB, Polars) convert to an iterator post-hoc.
The contract is streaming — backends that can truly stream (DataFusion, PyArrow) are
more memory-efficient, but all can satisfy the contract.

### 6.5 Parallelism Within a Backend

- **PyArrow:** Uses `ExecutorFactory` (thread pool) to read files in parallel
- **DataFusion:** Internal Tokio async runtime, `target_partitions` for parallelism
- **DuckDB:** Automatic parallelism (no config needed)
- **Polars:** Automatic parallelism in lazy mode

**Resolution:** Parallelism is internal to each backend. The protocol doesn't specify
how backends parallelize — only what they produce (correct output, within memory budget).

---

## 7. The Proposed Protocol (Validated Against All Engines)

Based on the discovery above, the protocol that fits ALL engines:

```python
from typing import Protocol, Iterator, Literal
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.expressions import BooleanExpression
from pyiceberg.table import FileScanTask, DataFile, TableMetadata

class IOBackend(Protocol):
    """Reads Parquet → Arrow and writes Arrow → Parquet."""

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

    def list_objects(
        self,
        prefix: str,
        io_properties: dict[str, str],
    ) -> Iterator[str]: ...


class ComputeBackend(Protocol):
    """Executes sort/join/filter/aggregate on Arrow data."""

    @property
    def supports_bounded_memory(self) -> bool: ...

    def sort(
        self,
        data: Iterator[pa.RecordBatch],
        sort_keys: list[tuple[str, Literal["ascending", "descending"]]],
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
        join_type: Literal["inner", "left", "right", "outer", "semi", "anti"],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]: ...

    def filter(
        self,
        data: Iterator[pa.RecordBatch],
        predicate: BooleanExpression,
    ) -> Iterator[pa.RecordBatch]: ...


class ExecutionBackend(Protocol):
    """Composite: executes complete scan tasks (read + delete resolution + filter)."""

    def execute_scan(
        self,
        tasks: Iterable[FileScanTask],
        table_metadata: TableMetadata,
        projected_schema: Schema,
        row_filter: BooleanExpression,
        io_properties: dict[str, str],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]: ...
```

### 7.1 Why This Works for Each Engine

| Engine | `read_parquet` | `write_parquet` | `sort` | `anti_join` | `filter` |
|--------|:---:|:---:|:---:|:---:|:---:|
| **PyArrow** | `Scanner.to_batches()` | `pq.ParquetWriter` | `pa.Table.sort_by()` (no limit) | `pc.is_in()` workaround | `table.filter(expr)` |
| **DataFusion** | `register_parquet` + SQL | Delegate to PyArrow | SQL ORDER BY (spills) | SQL LEFT ANTI JOIN (spills) | SQL WHERE |
| **DuckDB** | `read_parquet()` | `COPY TO` | SQL ORDER BY (spills) | SQL LEFT ANTI JOIN (spills) | SQL WHERE |
| **Polars** | `pl.scan_parquet().collect()` | `write_parquet()` | `.sort()` (no limit) | `.join(how="anti")` | `.filter()` |
| **cuDF** | `cudf.read_parquet()` | delegate | `.sort_values()` (VRAM) | `.merge(how="leftanti")` | mask filter |

Every cell has a concrete API call. The protocol fits all five without special-casing.

---

## 8. Summary of Findings

### 8.1 The Interface Is Correct

The `FileScanTask` boundary between planning and execution is clean, engine-agnostic,
and already exists. The proposed `IOBackend + ComputeBackend + ExecutionBackend` protocol
maps naturally to all 5 studied engines with no special-casing.

### 8.2 The First PR Scope (PyArrow + DataFusion)

- Extract `PyArrowIOBackend` from the monolith (move `ArrowScan` read logic + `write_file`)
- Extract `PyArrowComputeBackend` (sort via `sort_by`, filter via `pc.Expression`)
- Implement `DataFusionComputeBackend` (sort/join/filter via SQL with `FairSpillPool`)
- Implement `DataFusionIOBackend` (read via `register_parquet`)
- Wire `resolve_backend()` into `DataScan.to_arrow()` and `_to_arrow_via_file_scan_tasks()`
- Schema reconciliation (`_to_requested_schema`) stays in shared PyIceberg code

### 8.3 Known Gaps to Address

1. **Expression conversion:** Need `expression_to_sql()` for DataFusion/DuckDB (currently only `expression_to_pyarrow()` exists)
2. **Object store bridge:** Need `configure_object_store(ctx, io_properties)` per backend
3. **Write delegation:** DataFusion backend delegates writes to PyArrow (acceptable — write is I/O-bound)
4. **Streaming DuckDB results:** DuckDB materializes; need to chunk into iterator
5. **Equality deletes:** Not yet supported — the protocol enables it (via `anti_join`), implementation is a separate PR

---

## 9. Expression Conversion: The Visitor Pattern

### 9.1 How It Works Today (PyArrow)

PyIceberg's `BooleanExpression` is a strongly-typed AST. Users don't write SQL — they
write Iceberg expressions:

```python
# User writes:
table.scan(row_filter="age > 18 AND country = 'US'")
# Or programmatically:
table.scan(row_filter=And(GreaterThan("age", 18), EqualTo("country", "US")))
```

PyIceberg parses this into a `BooleanExpression` tree. During scan planning, this tree
is used for partition pruning. The remainder (residual) is passed to the execution backend.

The existing `expression_to_pyarrow()` (200 lines in `pyarrow.py`) uses PyIceberg's
`BoundBooleanExpressionVisitor` to walk the AST and produce `pc.Expression` objects:

```python
# Existing: pyiceberg/io/pyarrow.py (simplified)
class _ConvertToArrowExpression(BoundBooleanExpressionVisitor[pc.Expression]):
    def visit_equal(self, term, literal) -> pc.Expression:
        return pc.field(term.ref().name) == pc.scalar(literal.value)
    def visit_greater_than(self, term, literal) -> pc.Expression:
        return pc.field(term.ref().name) > pc.scalar(literal.value)
    def visit_and(self, left, right) -> pc.Expression:
        return left & right
    # ... ~15 more visit methods
```

### 9.2 What We Need for SQL-Based Backends

For DataFusion and DuckDB, the same visitor produces SQL strings instead:

```python
# New: pyiceberg/execution/expression_converters.py (conceptual)
class _ConvertToSql(BoundBooleanExpressionVisitor[str]):
    def visit_equal(self, term, literal) -> str:
        return f"{self._quote(term.ref().name)} = {self._literal(literal)}"
    def visit_greater_than(self, term, literal) -> str:
        return f"{self._quote(term.ref().name)} > {self._literal(literal)}"
    def visit_and(self, left, right) -> str:
        return f"({left} AND {right})"
    def visit_in(self, term, literals) -> str:
        values = ", ".join(self._literal(lit) for lit in literals)
        return f"{self._quote(term.ref().name)} IN ({values})"
    def visit_starts_with(self, term, literal) -> str:
        return f"{self._quote(term.ref().name)} LIKE '{literal.value}%'"
    # ... ~15 more visit methods

def expression_to_sql(expr: BooleanExpression, schema: Schema) -> str:
    """Convert Iceberg BooleanExpression to SQL WHERE clause."""
    bound = bind(schema, expr)
    return visit(bound, _ConvertToSql())
```

This is ~50-80 lines per backend. DataFusion and DuckDB share the same SQL converter
since both use standard SQL. Polars would need its own (`expression_to_polars()`
returning `pl.Expr` objects).

### 9.3 The Complete Predicate Type Set

Iceberg defines exactly these predicate types (from the spec):

| Predicate | PyArrow (`pc.*`) | SQL (DataFusion/DuckDB) | Polars (`pl.*`) |
|-----------|-----------------|-------------------------|-----------------|
| `AlwaysTrue` | `pc.scalar(True)` | `1=1` | `pl.lit(True)` |
| `AlwaysFalse` | `pc.scalar(False)` | `1=0` | `pl.lit(False)` |
| `Not(x)` | `~x` | `NOT (x)` | `~x` |
| `And(l, r)` | `l & r` | `(l AND r)` | `l & r` |
| `Or(l, r)` | `l \| r` | `(l OR r)` | `l \| r` |
| `IsNull(col)` | `pc.field(col).is_null()` | `col IS NULL` | `pl.col(col).is_null()` |
| `NotNull(col)` | `pc.field(col).is_valid()` | `col IS NOT NULL` | `pl.col(col).is_not_null()` |
| `IsNaN(col)` | `pc.field(col).is_nan()` | `isnan(col)` | `pl.col(col).is_nan()` |
| `Equal(col, val)` | `pc.field(col) == pc.scalar(val)` | `col = val` | `pl.col(col) == val` |
| `NotEqual(col, val)` | `pc.field(col) != pc.scalar(val)` | `col != val` | `pl.col(col) != val` |
| `GreaterThan(col, val)` | `pc.field(col) > pc.scalar(val)` | `col > val` | `pl.col(col) > val` |
| `GreaterThanOrEqual` | `pc.field(col) >= pc.scalar(val)` | `col >= val` | `pl.col(col) >= val` |
| `LessThan(col, val)` | `pc.field(col) < pc.scalar(val)` | `col < val` | `pl.col(col) < val` |
| `LessThanOrEqual` | `pc.field(col) <= pc.scalar(val)` | `col <= val` | `pl.col(col) <= val` |
| `In(col, vals)` | `pc.field(col).isin(vals)` | `col IN (v1, v2, ...)` | `pl.col(col).is_in(vals)` |
| `NotIn(col, vals)` | `~pc.field(col).isin(vals)` | `col NOT IN (v1, v2, ...)` | `~pl.col(col).is_in(vals)` |
| `StartsWith(col, prefix)` | `pc.starts_with(col, prefix)` | `col LIKE 'prefix%'` | `pl.col(col).str.starts_with(prefix)` |
| `NotStartsWith` | `~pc.starts_with(col, prefix)` | `col NOT LIKE 'prefix%'` | `~pl.col(col).str.starts_with(prefix)` |

**Every predicate has a natural translation in all three target formats (PyArrow,
SQL, Polars).** No predicate is impossible to express in any backend.

### 9.4 The Fallback Strategy

If a backend's expression converter encounters an unsupported predicate type:

```python
class _ConvertToSql(BoundBooleanExpressionVisitor[str]):
    def visit_unknown(self, expr) -> str:
        # Can't translate → return TRUE (accept all rows)
        # PyIceberg will post-filter the results
        return "1=1"
```

This is safe because:
1. The backend returns MORE rows than needed (superset)
2. PyIceberg applies the full filter as a post-filter on the Arrow output
3. Result is always correct — just slightly less efficient (more rows read)

---

## 10. Edge Cases and Validation Requirements

### 10.1 Column Identity and Schema Evolution

**The concern:** Iceberg matches columns by field ID, not by name. A renamed column
(`old_name` → `new_name`) has the same field ID in both schemas. The physical Parquet
file still has `old_name` as the column header.

**How it's handled today:** `_task_to_record_batches()` in `pyarrow.py`:
1. Reads the physical file schema
2. Maps physical column names → Iceberg field IDs (via metadata or name mapping)
3. Projects/reorders to match the requested schema
4. Fills missing columns with nulls
5. Casts types if needed (int32 → int64 promotion)

**In the pluggable model:** This logic stays in PyIceberg (shared code), ABOVE the
backend. The backend reads the raw physical file. PyIceberg's `_to_requested_schema()`
transforms the output to match the projected Iceberg schema.

**Backend's responsibility:** Just read the file honestly (physical columns, physical types).
**PyIceberg's responsibility:** Map the result to the requested schema using field IDs.

### 10.2 Partial Pushdown (Residual Expressions)

**The concern:** What if the backend can't evaluate part of the filter?

**How it's handled today:** `ManifestGroupPlanner` computes a `ResidualEvaluator`.
The `FileScanTask.residual` contains ONLY the filter that partition pruning couldn't
resolve. This residual is what gets passed to the backend.

**In the pluggable model:** The backend receives the residual expression. If it can
evaluate the full residual → it pushes it down (fewer rows returned). If it can only
evaluate part → it pushes what it can and returns a superset. PyIceberg applies the
full residual as a post-filter on the output.

**Validation test:**
```python
def test_partial_pushdown_correctness(backend, data_with_complex_filter):
    """Backend that can't push down StartsWith still returns correct results."""
    # Filter includes StartsWith (which some backends might not push down)
    result_with_pushdown = backend.read_parquet(path, schema, complex_filter, ...)
    result_full_scan = backend.read_parquet(path, schema, ALWAYS_TRUE, ...)
    post_filtered = apply_filter(result_full_scan, complex_filter)
    assert_arrow_equal(result_with_pushdown, post_filtered)
```

### 10.3 Expression Function Mismatch

**The concern:** `StartsWith("col", "prefix")` maps to different syntax per engine.

**How it's handled:** The expression converter table in §9.3 shows every predicate
has a natural translation. The set is finite (Iceberg spec defines exactly these 17
predicate types). Each converter handles all 17.

**Validation test:**
```python
@pytest.mark.parametrize("predicate", ALL_ICEBERG_PREDICATE_TYPES)
def test_expression_converter_handles_all_types(backend, predicate):
    """Every Iceberg predicate type converts without error for every backend."""
    sql_or_expr = backend.convert_expression(predicate)
    assert sql_or_expr is not None
```

### 10.4 Positional Delete Application

**The concern:** Files with positional deletes require reading a companion delete file
and excluding specific row indices.

**How it's handled today:** `_read_all_delete_files()` reads ALL delete files upfront
into `dict[file_path → list[ChunkedArray of positions]]`. Then per-batch, rows at
those positions are excluded via `batch.take(keep_indices)`.

**In the pluggable model:** `FileScanTask.delete_files` tells the backend which delete
files apply. The `ExecutionBackend.execute_scan()` contract says: "read the data file,
apply the delete files, return only surviving rows." The backend decides HOW to apply
them (PyArrow: `take(indices)`, DataFusion: anti-join on position, DuckDB: filter).

**Validation test:**
```python
def test_positional_deletes_applied_correctly(backend, data_file_with_deletes):
    """Backend produces correct output with positional deletes applied."""
    task = FileScanTask(data_file, delete_files={pos_delete_file})
    result = backend.execute_scan([task], ...)
    # Verify deleted row indices are NOT in result
    assert row_at_position_5_not_in(result)
```

### 10.5 NULL Handling in Expressions

**The concern:** SQL's three-valued logic (TRUE/FALSE/NULL) differs from Python's
boolean logic. `col = 5` in SQL excludes NULLs; in Python `None == 5` is `False`.

**How it's handled:** Iceberg's spec explicitly defines NULL semantics for each
predicate. `IsNull(col)` and `NotNull(col)` are separate predicates. Comparisons
(`Equal`, `GreaterThan`) follow SQL semantics (NULL comparisons yield NULL, not FALSE).

**Validation test:**
```python
def test_null_handling_in_filter(backend, data_with_nulls):
    """Backend correctly handles NULLs per SQL/Iceberg semantics."""
    # Filter: col > 5 should NOT include rows where col is NULL
    result = backend.read_parquet(path, schema, GreaterThan("col", 5), ...)
    assert no_nulls_in_column(result, "col")
```

### 10.6 Nested Types in Expressions

**The concern:** Iceberg supports struct, list, and map types. Expressions can
reference nested fields (`struct_col.field_name`).

**Current status:** PyIceberg's expression system supports referencing nested fields
via dotted names. The `BoundReference` resolves the field path to a specific field ID.

**In the pluggable model:** The expression converter must handle dotted paths:
- SQL: `struct_col.field_name > 5` (most SQL engines support this)
- PyArrow: `pc.field("struct_col", "field_name") > pc.scalar(5)`
- Polars: `pl.col("struct_col").struct.field("field_name") > 5`

**Validation test:**
```python
def test_nested_field_filter(backend, data_with_structs):
    """Backend correctly filters on nested struct fields."""
    result = backend.read_parquet(path, schema, GreaterThan("address.zip", 90000), ...)
    assert all(row["address"]["zip"] > 90000 for row in result.to_pydict())
```

### 10.7 Comprehensive Validation Suite

The first PR must include a parametrized test suite that validates ALL edge cases
across ALL backends:

```python
# tests/execution/test_backend_validation.py

BACKENDS = ["pyarrow", "datafusion"]  # PR 1 scope

@pytest.fixture(params=BACKENDS)
def backend(request): ...

class TestExpressionConversion:
    """All predicate types convert correctly for all backends."""
    @pytest.mark.parametrize("pred", ALL_17_PREDICATE_TYPES)
    def test_predicate_converts(self, backend, pred): ...
    def test_null_semantics(self, backend): ...
    def test_nested_field_reference(self, backend): ...
    def test_unsupported_predicate_falls_back(self, backend): ...

class TestSchemaEvolution:
    """Column renames, type promotions, and missing columns handled."""
    def test_renamed_column_read_correctly(self, backend): ...
    def test_missing_column_filled_with_null(self, backend): ...
    def test_type_promotion_int32_to_int64(self, backend): ...

class TestDeleteResolution:
    """Positional (and future equality) deletes applied correctly."""
    def test_positional_deletes_exclude_rows(self, backend): ...
    def test_multiple_delete_files_composed(self, backend): ...
    def test_no_deletes_returns_all_rows(self, backend): ...

class TestBoundedMemory:
    """Spill-capable backends complete within memory_limit."""
    def test_sort_large_data_within_budget(self, datafusion_backend): ...
    def test_anti_join_large_data_within_budget(self, datafusion_backend): ...

class TestStreamingContract:
    """All backends produce Iterator[RecordBatch], not materialized tables."""
    def test_output_is_iterator(self, backend): ...
    def test_streaming_does_not_materialize_all(self, backend): ...
```
