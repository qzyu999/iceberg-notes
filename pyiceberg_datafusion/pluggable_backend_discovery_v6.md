# Pluggable Backend v6: Full Decomposition with Independent Read/Write/Compute

Branch: `pluggable-backend-discovery` (commit `ff35d250`)

## Overview

This document defines the complete pluggable architecture where read, write, and
compute are three independently configurable axes. Any combination of libraries
can be used across the three axes because Arrow RecordBatch is the interchange
format at every boundary.

---

## 1. The Three Axes (Fully Independent)

```
Read    : Path → Iterator[RecordBatch]        "Who decodes Parquet into Arrow"
Compute : RecordBatch operations → RecordBatch "Who sorts, joins, filters, aggregates"
Write   : Iterator[RecordBatch] → DataFile     "Who encodes Arrow into Parquet"
```

These are independent because:
- The Read backend produces RecordBatches. It does not care what consumes them.
- The Compute backend accepts and produces RecordBatches. It does not care who read or will write them.
- The Write backend consumes RecordBatches. It does not care who produced them.

Any combination works: read with Polars, compute with DataFusion, write with PyArrow.

### 1.1 Why IOBackend Should Be Split

The current branch has a single `IOBackend` combining read and write. This prevents
configurations like "read with DuckDB (fast predicate pushdown), write with PyArrow
(detailed statistics)." Splitting them:

```python
class ReadBackend(Protocol):
    """Decodes Parquet files into Arrow RecordBatches."""
    def read_parquet(self, location, schema, filter, properties) -> Iterator[RecordBatch]: ...
    def list_objects(self, prefix, properties) -> Iterator[str]: ...

class WriteBackend(Protocol):
    """Encodes Arrow RecordBatches into Parquet files."""
    def write_parquet(self, batches, location, schema, props, properties) -> WriteResult: ...
    def write_partitioned(self, batches, base_location, schema, target_size, props, properties) -> list[WriteResult]: ...

class ComputeBackend(Protocol):
    """Transforms Arrow data: sort, join, filter, aggregate."""
    supports_bounded_memory: bool
    def sort_from_files(self, paths, keys, properties, memory_limit) -> Iterator[RecordBatch]: ...
    def join_from_files(self, left, right, on, join_type, properties, memory_limit) -> Iterator[RecordBatch]: ...
    def anti_join_from_files(self, left, right, on, properties, memory_limit) -> Iterator[RecordBatch]: ...
    def aggregate_from_files(self, paths, group_by, aggs, properties, memory_limit) -> Iterator[RecordBatch]: ...
    def filter(self, batches, predicate) -> Iterator[RecordBatch]: ...
    def apply_positional_deletes(self, data_batches, position_batches) -> Iterator[RecordBatch]: ...
```

### 1.2 Naming: Not "ExecutionContext"

"Execution" implies all three axes are one thing. Since we are separating them,
the coordinator that holds the three resolved backends should reflect this.
Options:

| Name | Pros | Cons |
|------|------|------|
| `BackendConfig` | Clear, simple | Too passive (implies config, not invocation) |
| `EngineContext` | Matches "engine" terminology | "Engine" overloaded (DataFusion calls itself an engine) |
| `TableRuntime` | Mirrors "RuntimeEnv" in DataFusion | Novel term, not established |
| `BackendContext` | Neutral, groups the three backends | Could confuse with Python context managers |
| `Backends` | Simplest possible | Maybe too short |

Recommendation: **`Backends`** as the container, with explicit fields:

```python
@dataclass
class Backends:
    """Resolved backend configuration: independently pluggable read, write, and compute."""
    read: ReadBackend
    write: WriteBackend
    compute: ComputeBackend

    @classmethod
    def resolve(cls, properties: Properties, overrides: BackendOverrides | None = None) -> Backends:
        """Resolve backends from config, auto-detection, and explicit overrides."""
        ...
```

Usage:

```python
backends = Backends.resolve(table.io.properties)
batches = backends.read.read_parquet(path, schema, filter, properties)
sorted_batches = backends.compute.sort_from_files(paths, keys, properties, memory_limit)
results = backends.write.write_partitioned(sorted_batches, location, schema, target_size, props, properties)
```

With explicit overrides:

```python
from pyiceberg.execution.backends.polars_backend import PolarsReadBackend
from pyiceberg.execution.backends.datafusion_backend import DataFusionComputeBackend
from pyiceberg.execution.backends.pyarrow_backend import PyArrowWriteBackend

backends = Backends(
    read=PolarsReadBackend(),
    write=PyArrowWriteBackend(),
    compute=DataFusionComputeBackend(),
)
```

---

## 2. The Complete Universe of Iceberg Operations

Every operation decomposes into a pipeline of Read, Compute, and Write steps.
Below is the exhaustive list with the exact primitive composition.

### 2.1 Read Operations

| Operation | Pipeline | Primitives |
|-----------|----------|------------|
| `table.scan().to_arrow()` (no deletes) | Read files → filter → project | `read.read_parquet` × N files + `compute.filter` |
| `table.scan().to_arrow()` (positional deletes) | Read data + read pos-delete files → exclude positions | `read.read_parquet` + `compute.apply_positional_deletes` |
| `table.scan().to_arrow()` (equality deletes) | Read data + read eq-delete files → anti-join | `compute.anti_join_from_files(data_paths, delete_paths, eq_cols)` |
| `table.scan().to_arrow()` (mixed deletes) | Combination of above | All three |
| `table.scan().count()` (optimized) | Read metadata only (file record_count) | No backend needed (pure metadata) |
| `table.inspect.*` | Read metadata as Arrow tables | No backend needed (metadata formatting) |

### 2.2 Write Operations

| Operation | Pipeline | Primitives |
|-----------|----------|------------|
| `table.append(df)` | Validate schema → write data files → commit | `write.write_parquet` or `write.write_partitioned` |
| `table.overwrite(df)` | Detect partitions → write data files → commit | `write.write_partitioned` |
| Sort-on-write (future) | Sort → write | `compute.sort_from_files` + `write.write_partitioned` |

### 2.3 Read-Modify-Write Operations

| Operation | Pipeline | Primitives |
|-----------|----------|------------|
| `table.delete(filter)` (CoW) | Read affected files → filter complement → rewrite | `read.read_parquet` + `compute.filter` + `write.write_partitioned` |
| `table.upsert(df)` | Join source against target → separate updates/inserts → write | `compute.join_from_files("inner")` + `compute.join_from_files("anti")` + `write.write_partitioned` |
| Position delete compaction | Read data + pos deletes → exclude positions → rewrite clean | `compute.apply_positional_deletes` + `write.write_partitioned` |
| Eq-to-pos conversion | Read data + eq deletes → find matching positions → write pos-delete file | `compute.join_from_files("semi")` + `write.write_parquet` |

### 2.4 Maintenance Operations

| Operation | Pipeline | Primitives |
|-----------|----------|------------|
| `table.compact()` | Read N files → sort → write M files → commit rewrite | `compute.sort_from_files` + `write.write_partitioned` |
| `table.compact()` (z-order) | Read → compute z-order key → sort → write | `compute.sort_from_files` (with z-order key UDF) + `write.write_partitioned` |
| `table.delete_orphan_files()` | List storage → enumerate valid paths → anti-join → delete | `read.list_objects` + `compute.anti_join_from_files` |
| `table.expire_snapshots()` | Enumerate expired paths → anti-join retained → delete | `compute.anti_join_from_files` |
| `table.compute_stats()` | Read all files → aggregate per column | `compute.aggregate_from_files` |

### 2.5 The Missing Primitive: `apply_positional_deletes`

Positional deletes are not a join. They say "exclude row at index 5, 17, 42 in this file."
This requires:

```python
def apply_positional_deletes(
    self,
    data_path: str,
    position_delete_paths: list[str],
    projected_schema: Schema,
    io_properties: Properties,
) -> Iterator[pa.RecordBatch]:
    """Read a data file and exclude rows at positions listed in the delete files.

    The delete files contain (file_path, pos) pairs. This method reads the data
    file, reads the position columns from delete files, and excludes rows at
    those indices.
    """
    ...
```

For PyArrow: read positions into a set, then for each batch track the running row
offset and exclude matching indices via `pa.Table.take()` on the complement.

For DataFusion: register data file with a synthetic `ROW_NUMBER()` column, register
delete positions, and anti-join on the row number.

---

## 3. Protocol Definitions (Revised)

```python
@runtime_checkable
class ReadBackend(Protocol):
    """Decodes Parquet files into Arrow RecordBatches.

    Responsibilities:
    - Open and decode Parquet files (column projection, predicate pushdown)
    - List objects in storage (for orphan detection)
    - Handle storage credentials via io_properties
    """

    def read_parquet(
        self,
        location: str,
        projected_schema: Schema,
        row_filter: BooleanExpression,
        io_properties: Properties,
    ) -> Iterator[pa.RecordBatch]: ...

    def list_objects(
        self,
        prefix: str,
        io_properties: Properties,
    ) -> Iterator[str]: ...


@runtime_checkable
class WriteBackend(Protocol):
    """Encodes Arrow RecordBatches into Parquet files.

    Responsibilities:
    - Write RecordBatches to a single Parquet file with statistics
    - Write RecordBatches to multiple files with size-based splitting
    - Return WriteResult with metadata for DataFile construction
    """

    def write_parquet(
        self,
        batches: Iterator[pa.RecordBatch],
        location: str,
        schema: Schema,
        write_properties: Properties,
        io_properties: Properties,
    ) -> WriteResult: ...

    def write_partitioned(
        self,
        batches: Iterator[pa.RecordBatch],
        base_location: str,
        schema: Schema,
        target_file_size: int,
        write_properties: Properties,
        io_properties: Properties,
    ) -> list[WriteResult]: ...


@runtime_checkable
class ComputeBackend(Protocol):
    """Transforms Arrow data with optional bounded-memory execution.

    Responsibilities:
    - Sort from file paths (external merge sort with spill)
    - Join from file paths (Grace Hash Join with spill)
    - Filter streaming batches (O(1) per batch)
    - Aggregate from file paths (spillable hash aggregate)
    - Apply positional deletes (index-based row exclusion)
    """

    @property
    def supports_bounded_memory(self) -> bool: ...

    def sort_from_files(
        self,
        file_paths: list[str],
        sort_keys: list[tuple[str, Literal["ascending", "descending"]]],
        io_properties: Properties,
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]: ...

    def join_from_files(
        self,
        left_paths: list[str],
        right_paths: list[str],
        on: list[str],
        join_type: Literal["inner", "left", "right", "outer", "semi", "anti"],
        io_properties: Properties,
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]: ...

    def anti_join_from_files(
        self,
        left_paths: list[str],
        right_paths: list[str],
        on: list[str],
        io_properties: Properties,
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]: ...

    def aggregate_from_files(
        self,
        file_paths: list[str],
        group_by: list[str],
        aggregations: list[tuple[str, Literal["count", "count_distinct", "sum", "min", "max", "mean"]]],
        io_properties: Properties,
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]: ...

    def filter(
        self,
        data: Iterator[pa.RecordBatch],
        predicate: BooleanExpression,
    ) -> Iterator[pa.RecordBatch]: ...

    def apply_positional_deletes(
        self,
        data_path: str,
        position_delete_paths: list[str],
        projected_schema: Schema,
        io_properties: Properties,
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]: ...
```

---

## 4. The `Backends` Container

```python
@dataclass
class Backends:
    """Three independently resolved backends for read, write, and compute.

    Each axis can use a different library. Arrow RecordBatch is the interchange
    format at every boundary, enabling arbitrary mix-and-match combinations.
    """

    read: ReadBackend
    write: WriteBackend
    compute: ComputeBackend

    @property
    def supports_bounded_memory(self) -> bool:
        return self.compute.supports_bounded_memory

    @classmethod
    def resolve(
        cls,
        io_properties: Properties,
        *,
        read_override: str | ReadBackend | None = None,
        write_override: str | WriteBackend | None = None,
        compute_override: str | ComputeBackend | None = None,
    ) -> Backends:
        """Resolve all three backends.

        Resolution order per axis:
        1. Explicit override (instance or string name)
        2. Configuration (.pyiceberg.yaml or env var)
        3. Auto-detection (compute only: promote DataFusion if installed)
        4. Default (PyArrow for all three)
        """
        ...
```

### 4.1 Configuration

```yaml
# .pyiceberg.yaml
execution:
  read-backend: pyarrow          # or: polars, datafusion, duckdb
  write-backend: pyarrow         # or: datafusion, duckdb
  compute-backend: datafusion    # or: pyarrow, duckdb, polars
  memory-limit: 512MB
```

```bash
# Environment variables
PYICEBERG_EXECUTION__READ_BACKEND=polars
PYICEBERG_EXECUTION__WRITE_BACKEND=pyarrow
PYICEBERG_EXECUTION__COMPUTE_BACKEND=datafusion
PYICEBERG_EXECUTION__MEMORY_LIMIT=1GB
```

### 4.2 Per-Operation Override

```python
# Global config uses DataFusion for compute, but for this one scan use DuckDB:
table.scan(row_filter="age > 18").to_arrow(compute_backend="duckdb")
```

---

## 5. How All Operations Use the `Backends` Object

Every table operation creates (or receives) a `Backends` instance and composes
the three axes:

```python
def _to_arrow_via_file_scan_tasks(scan, projected_schema, tasks):
    backends = Backends.resolve(scan.io.properties)

    for task in tasks:
        if task.has_equality_deletes:
            batches = backends.compute.anti_join_from_files(
                left_paths=[task.file.file_path],
                right_paths=[d.file_path for d in task.equality_delete_files],
                on=task.equality_field_names,
                io_properties=scan.io.properties,
            )
        elif task.has_positional_deletes:
            batches = backends.compute.apply_positional_deletes(
                data_path=task.file.file_path,
                position_delete_paths=[d.file_path for d in task.positional_delete_files],
                projected_schema=projected_schema,
                io_properties=scan.io.properties,
            )
        else:
            batches = backends.read.read_parquet(
                task.file.file_path, projected_schema, task.residual, scan.io.properties
            )

        filtered = backends.compute.filter(batches, task.residual)
        yield from filtered
```

For compaction:

```python
def compact(self, partition_filter=None, sort_order=None, target_file_size=None):
    backends = Backends.resolve(self.io.properties)

    if not backends.supports_bounded_memory:
        raise ImportError("table.compact() requires pip install 'pyiceberg[datafusion]'")

    files = self._select_files_for_compaction(partition_filter)
    paths = [f.file_path for f in files]

    sorted_batches = backends.compute.sort_from_files(
        paths, sort_order.keys, self.io.properties, memory_limit
    )
    new_files = backends.write.write_partitioned(
        sorted_batches, self._data_location(), self.metadata.schema(),
        target_file_size, {}, self.io.properties
    )
    self._commit_rewrite(old_files=files, new_files=new_files)
```

For CoW delete:

```python
def delete(self, delete_filter):
    backends = Backends.resolve(self._table.io.properties)

    for task in affected_files:
        batches = backends.read.read_parquet(
            task.file.file_path, schema, AlwaysTrue(), self._table.io.properties
        )
        kept = backends.compute.filter(batches, complement_of(delete_filter))
        new_files = backends.write.write_partitioned(
            kept, output_location, schema, target_size, {}, self._table.io.properties
        )
        ...
```

---

## 6. Backend Implementations Per Axis

| Library | ReadBackend | WriteBackend | ComputeBackend |
|---------|:---:|:---:|:---:|
| PyArrow | ✅ `ds.Scanner` | ✅ `pq.ParquetWriter` (full stats) | ✅ (in-memory, no spill) |
| DataFusion | ✅ `register_parquet` + SQL | ⚠️ Delegates to PyArrow | ✅ (FairSpillPool, spill-to-disk) |
| DuckDB | ✅ `read_parquet()` SQL | ✅ `COPY TO` (limited stats) | ✅ (internal spill) |
| Polars | ✅ `pl.scan_parquet()` | ⚠️ Delegates to PyArrow | ✅ (in-memory, no spill) |

"Delegates to PyArrow" means the library does not produce the statistics metadata
required for `WriteResult` (lower_bounds, upper_bounds, column_sizes). Until they do,
the write path delegates to PyArrow's ParquetWriter which extracts full metadata.

### 6.1 Valid Combinations (Examples)

| Read | Compute | Write | Use Case |
|------|---------|-------|----------|
| PyArrow | PyArrow | PyArrow | Default. Works today. OOMs on large data. |
| PyArrow | DataFusion | PyArrow | Production. Bounded-memory compute, full write stats. |
| Polars | DataFusion | PyArrow | Fast Polars reads + DataFusion sort/join + full write stats. |
| DuckDB | DuckDB | DuckDB | Full DuckDB pipeline (BSL caveat for S3). |
| DuckDB | DataFusion | PyArrow | DuckDB reads (good for corrupt files) + DF compute + PA write. |
| PyArrow | PyArrow | PyArrow + Ray | Default per-worker in distributed deployment. |

---

## 7. Difference from Current Branch Code

The current branch has:
- `IOBackend` (read + write combined)
- `ComputeBackend`
- `ExecutionBackend` (composite scan)

The v6 design splits `IOBackend` into `ReadBackend` + `WriteBackend` and removes
`ExecutionBackend` (its logic moves into the operation orchestration code that
composes `backends.read` + `backends.compute` + `backends.write`).

Changes needed on the branch:

| Change | Effort |
|--------|:---:|
| Split `IOBackend` protocol into `ReadBackend` + `WriteBackend` | Small (move methods between protocols) |
| Split each backend's IO class into Read + Write classes | Small (split file or rename) |
| Add `apply_positional_deletes` to `ComputeBackend` | Medium (new method + implementations) |
| Replace `BackendDispatch` with `Backends` dataclass | Small (rename + restructure) |
| Remove `ExecutionBackend` (composite moved to orchestration) | Small (delete protocol, keep orchestration logic) |

---

## 8. Verification of Completeness

**Claim:** Every Iceberg operation is expressible as a composition of the three protocols.

| Primitive | Protocol | Operations that use it |
|-----------|----------|----------------------|
| `read.read_parquet` | ReadBackend | Scan, CoW delete, inspect |
| `read.list_objects` | ReadBackend | Orphan detection |
| `write.write_parquet` | WriteBackend | Append, eq-to-pos conversion |
| `write.write_partitioned` | WriteBackend | Overwrite, CoW delete, compaction, sort-on-write |
| `compute.sort_from_files` | ComputeBackend | Compaction, z-order, sort-on-write |
| `compute.join_from_files` | ComputeBackend | Upsert (inner), eq-to-pos (semi) |
| `compute.anti_join_from_files` | ComputeBackend | Eq delete resolution, orphan detection, expire snapshots |
| `compute.aggregate_from_files` | ComputeBackend | Compute table stats, partition detection |
| `compute.filter` | ComputeBackend | Scan (residual), CoW delete (complement filter) |
| `compute.apply_positional_deletes` | ComputeBackend | Positional delete resolution, pos-delete compaction |

10 primitives. Every operation in sections 2.1 through 2.4 is covered. No operation
requires a primitive not listed here.

---

## 9. Summary

The v6 architecture provides:

1. **Three independent axes:** Read, Write, and Compute are separate protocols.
   Any library can be used for any axis independently.

2. **Full operation coverage:** 10 primitives cover the complete universe of Iceberg
   operations (scan, write, delete, upsert, compact, orphan delete, expire, stats).

3. **True pluggability:** Users can configure each axis independently via config file,
   environment variables, or per-operation overrides. The default is PyArrow for all three.

4. **The `Backends` container:** A simple dataclass holding three resolved backends.
   Created once per operation. Provides `supports_bounded_memory` for capability gating.

5. **No special cases:** PyArrow is a backend like any other. No if-else branching.
   All operations go through the same protocol regardless of which backends are active.
