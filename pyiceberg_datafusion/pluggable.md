# Pluggable Compute Backend: Feasibility Analysis

## Kevin's Latest Comment (2026-06-28)

> Thanks! I think a good next step is to understand how we use pyarrow today and how
> to decouple it from the rest of the code.
>
> Would be great to be able to use different backend for reading and writing --
> datafusion, duckdb, pyarrow, etc 😄

This is an aspirational direction toward a pluggable I/O + compute backend where users
or contributors can choose which engine handles reading, writing, and compute.

This document analyzes what "decoupling from PyArrow" actually means, what is and isn't
pluggable, and how this shapes our implementation plan.

---

## 1. First Principles: Arrow the Format vs. PyArrow the Library

### 1.1 The Critical Distinction

The phrase "decouple from PyArrow" is ambiguous. There are two entirely different things
called "Arrow" in this context:

| Concept | What it is | Role in Iceberg | Decouplable? |
|---------|-----------|-----------------|:---:|
| **Arrow format** | A columnar in-memory data layout specification (RecordBatch, Schema, DataType) | THE canonical in-memory representation for Iceberg data. All engines (Spark, Flink, DataFusion, DuckDB, Polars) speak Arrow as their interchange format. | **No** — this is the universal standard |
| **`pyarrow` library** | A specific Python package that implements Arrow + Parquet codec + compute kernels + filesystem | Currently the only library PyIceberg uses for reading Parquet, writing Parquet, filtering, sorting, and schema conversion | **Yes** — other libraries can also read Parquet → Arrow |

**Axiom (Arrow Format Permanence):** The Arrow columnar format is Iceberg's in-memory
representation. It appears in PyIceberg's public API signatures (`pa.Table`, `pa.RecordBatch`,
`pa.RecordBatchReader`) and is the interchange format between ALL engines. It is never
decoupled. It is the wire format — the common language.

**What "decouple from PyArrow" means:** Replace `pyarrow` the *library* (who reads Parquet,
who does compute) while keeping Arrow the *format* (what the data looks like in memory).

### 1.2 Why Arrow the Format Cannot Be Replaced

Arrow is not merely PyIceberg's internal choice — it IS how Iceberg data exists in memory
across the entire ecosystem:

```
Parquet (on disk) ←→ Arrow (in memory) ←→ Engine (query/compute)
```

- Spark reads Iceberg → Arrow (via Parquet reader)
- Flink reads Iceberg → Arrow (via Parquet reader)
- DataFusion reads Iceberg → Arrow (native)
- DuckDB reads Iceberg → Arrow (native)
- Polars reads Iceberg → Arrow (native, via arrow2/arrow-rs)

Every modern analytics engine uses Arrow as its in-memory format or has zero-copy Arrow
interop. This is by design — the Arrow project exists specifically to be this universal
standard so engines don't waste time converting between formats.

PyIceberg's public API types are Arrow types:

```python
def append(self, df: pa.Table | pa.RecordBatchReader, ...) -> None: ...
def to_arrow(self) -> pa.Table: ...
def to_arrow_batch_reader(self) -> pa.RecordBatchReader: ...
```

These are correct and permanent. They are not coupling to a library — they are adopting
the standard.

### 1.3 What IS Decouplable: The Library Operations

The `pyarrow` package does multiple jobs inside PyIceberg. Each can potentially be done
by a different library that also speaks Arrow:

| Job | Current implementor | Could also do it |
|-----|--------------------|--------------------|
| Read Parquet → Arrow batches | `pyarrow.parquet` | DataFusion, DuckDB, Polars |
| Write Arrow batches → Parquet | `pyarrow.parquet.ParquetWriter` | DataFusion (IcebergWriteExec), DuckDB |
| Filter Arrow data by predicate | `pyarrow.compute` (pc.Expression) | DataFusion (SQL), DuckDB (SQL) |
| Sort Arrow data | `pa.Table.sort_by()` | DataFusion (SortExec), DuckDB |
| Join two Arrow datasets | **Not available in PyArrow** | DataFusion (HashJoinExec), DuckDB |
| Collect column statistics | `pyarrow.parquet` metadata | Any Parquet reader |
| Access object storage (S3/GCS) | `pyarrow.fs` (PyArrowFileIO) | `fsspec`, `object_store` |

**Theorem (Pluggable Surface):** The set of operations that can be provided by different
backends is exactly the set of operations where:
1. The input is expressible as Arrow data + metadata (file paths, schema, predicates)
2. The output is Arrow data + metadata (RecordBatches, DataFile descriptors)
3. The operation semantics are engine-independent (sort, join, filter are mathematical — same output regardless of who computes it)

All six jobs above satisfy these criteria. They are pluggable.

---

## 1.4 The Arrow Interop Stack: What Are the Actual Primitives?

To be confident we can truly swap backends at a single layer, we need to understand
what "speaking Arrow" actually means at the implementation level — how many layers of
abstraction exist, and whether all engines connect at the same layer.

### 1.4.1 The Arrow Interop Layers

The Arrow ecosystem has a precisely defined interoperability stack:

```
Layer 4: High-level APIs (pa.Table, DataFrame, SQL result)
         ↕  language-specific convenience wrappers
Layer 3: RecordBatch (the fundamental unit)
         = Schema (column names + types) + N column Arrays
         ↕  this is what crosses library boundaries
Layer 2: Arrow C Data Interface (the FFI standard)
         = ArrowSchema struct + ArrowArray struct (C pointers)
         ↕  zero-copy pointer handoff between ANY two libraries
Layer 1: Memory layout (the physical format)
         = Contiguous buffers: validity bitmap + offsets + values
         ↕  raw bytes in RAM, defined by Arrow spec
Layer 0: Memory allocation
         = Who owns the buffer (Python heap, Rust allocator, mmap)
```

**The critical interop boundary is Layer 2: the Arrow C Data Interface.**

This is a C-level ABI (Application Binary Interface) defined at
https://arrow.apache.org/docs/format/CDataInterface.html that specifies exactly how
to pass Arrow data between two libraries without copying:

```c
// The entire Arrow interop contract (simplified):
struct ArrowSchema {
    const char* format;     // data type encoding
    int64_t n_children;     // number of child arrays (for nested types)
    struct ArrowSchema** children;
    // ... release callback
};

struct ArrowArray {
    int64_t length;         // number of elements
    int64_t null_count;
    int64_t offset;
    const void** buffers;   // validity + offsets + values
    int64_t n_children;
    struct ArrowArray** children;
    // ... release callback
};
```

**Any library that can produce or consume these two C structs can interoperate
with any other such library — zero copy, zero serialization.**

### 1.4.2 Which Libraries Implement the C Data Interface?

| Library | Produces Arrow C Data | Consumes Arrow C Data | Zero-copy interop? |
|---------|:---:|:---:|:---:|
| **PyArrow** | ✅ | ✅ | ✅ (native) |
| **DataFusion (via datafusion-python)** | ✅ | ✅ | ✅ (via PyO3 + arrow-rs) |
| **DuckDB** | ✅ | ✅ | ✅ (`fetch_arrow_table()`, `from_arrow()`) |
| **Polars** | ✅ | ✅ | ✅ (`to_arrow()`, `from_arrow()`) |
| **cuDF (RAPIDS)** | ✅ | ✅ | ✅ (GPU Arrow) |
| **pandas** | ⚠️ (via PyArrow) | ⚠️ (via PyArrow) | Indirect |
| **NumPy** | ❌ | ❌ | ❌ (row-oriented, different format) |

**All four candidate backends (PyArrow, DataFusion, DuckDB, Polars) implement the
same C Data Interface.** This means they can hand Arrow data to each other at Layer 2
with zero copying. There is no "some work with Arrow and some don't" — they all do.

### 1.4.3 The PyCapsule Protocol (Python-Level Arrow Exchange)

In Python specifically, the Arrow C Data Interface is exposed via the **PyCapsule
protocol** (https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html):

```python
# Any object implementing these dunders can be consumed by any Arrow library:
def __arrow_c_array__(self, requested_schema=None) -> tuple[capsule, capsule]: ...
def __arrow_c_stream__(self, requested_schema=None) -> capsule: ...
```

This means:

```python
# DuckDB produces Arrow that PyArrow consumes — zero copy
result = duckdb.sql("SELECT * FROM data ORDER BY x")
pa_table = result.fetch_arrow_table()  # uses C Data Interface internally

# DataFusion produces Arrow that DuckDB consumes — zero copy
df_result = ctx.sql("SELECT * FROM t").to_arrow_table()
duckdb.from_arrow(df_result)  # no copy

# Polars produces Arrow that DataFusion consumes — zero copy
pl_df = pl.read_parquet("file.parquet")
ctx.register_record_batches("data", [pl_df.to_arrow().to_batches()])
```

**There is exactly ONE interop layer (Layer 2 / C Data Interface), and ALL candidate
libraries implement it fully.** The swap is at a single point.

### 1.4.4 The Swap Layer Diagram

```mermaid
graph TB
    subgraph "PyIceberg (constant — never changes)"
        API["Public API<br/>pa.Table, pa.RecordBatch, pa.RecordBatchReader"]
        SEM["Iceberg Semantics<br/>(scan planning, commit, delete index)"]
        SCHEMA["Schema conversion<br/>(Iceberg Schema ↔ Arrow Schema)<br/>SHARED by all backends"]
    end

    subgraph "THE SWAP LAYER (one boundary, Arrow C Data Interface)"
        IFACE["Backend Protocol<br/>Input: Arrow RecordBatch + metadata<br/>Output: Arrow RecordBatch + metadata"]
    end

    subgraph "Backend implementations (swappable)"
        PA["PyArrowBackend<br/>pq.read_table() → pa.RecordBatch<br/>pc.Expression → filter<br/>pq.ParquetWriter → write"]
        DF["DataFusionBackend<br/>register_parquet() → RecordBatch<br/>SQL → sort/join/filter<br/>(write delegated to PyArrow)"]
        DDB["DuckDBBackend<br/>duckdb.read_parquet() → Arrow<br/>SQL → sort/join/filter<br/>duckdb.write_parquet() → write"]
        POL["PolarsBackend<br/>pl.scan_parquet() → Arrow<br/>expressions → sort/filter<br/>(no spill — limited)"]
    end

    API --> SEM --> SCHEMA --> IFACE
    IFACE --> PA
    IFACE --> DF
    IFACE --> DDB
    IFACE --> POL
```

**There is exactly ONE swap layer.** Above it: PyIceberg's constant API and semantics.
Below it: interchangeable backend implementations. The boundary is Arrow RecordBatch
(exchanged via C Data Interface, zero-copy). It does not go "multiple layers deep" —
it is a single, flat interface.

### 1.4.5 What About Schema Conversion?

Schema conversion (`Iceberg Schema ↔ Arrow Schema`) sits ABOVE the swap layer because:
- All backends use Arrow Schema (it's part of the Arrow format spec)
- The conversion is from Iceberg's logical types → Arrow's type system
- It's the same Arrow Schema regardless of which backend reads the data
- The 800-line visitor code is shared infrastructure, not backend-specific

```python
# This conversion is used by ALL backends — it's above the swap layer
arrow_schema = schema_to_pyarrow(iceberg_schema)  # Iceberg → Arrow Schema

# Then any backend can use the Arrow Schema:
# PyArrow: pq.read_table(file, schema=arrow_schema)
# DataFusion: ctx.register_parquet("t", file)  (infers schema from Parquet, compatible)
# DuckDB: duckdb.read_parquet(file)  (same)
```

### 1.4.6 What About Expression Conversion?

Expression conversion IS backend-specific (sits below the swap layer):

```python
# Each backend has its own expression format:
# PyArrow:     pc.field("x") > pc.scalar(5)
# DataFusion:  "x > 5" (SQL string)
# DuckDB:      "x > 5" (SQL string)
# Polars:      pl.col("x") > 5

# The Iceberg BooleanExpression → backend expression is per-backend code.
# This is part of each backend's implementation, not shared infrastructure.
```

But expressions are simple — a typical predicate is one line. The conversion is
straightforward for any backend that supports SQL (DataFusion, DuckDB) or a reasonable
expression DSL (Polars).

### 1.4.7 Confidence Statement

**Can we truly swap the backend at a single layer and have everything work?**

**Yes, for all four candidates**, because:

1. All four implement the Arrow C Data Interface (zero-copy exchange at Layer 2)
2. All four can read Parquet files into Arrow RecordBatches
3. All four can produce Arrow output that PyIceberg's write path consumes
4. Schema conversion is shared (Arrow Schema is universal)
5. Only expression conversion is per-backend (trivial for SQL-based engines)

The swap is genuinely at a single, flat boundary. There is no "complex network of
relations/connectors" below — it's just:
```
PyIceberg semantics → [Arrow RecordBatch boundary] → Backend does work → [Arrow RecordBatch boundary] → PyIceberg continues
```

One boundary in, one boundary out. Arrow RecordBatch both times. Any library that
speaks Arrow (all four do) works at both boundaries.

---

## 2. How PyArrow Is Coupled in PyIceberg Today

### 2.1 The Monolith

`pyiceberg/io/pyarrow.py` is a **3,046-line file** that performs all six jobs listed above.
It is not structured as a pluggable backend — it directly calls `pyarrow` APIs throughout:

```
pyiceberg/io/pyarrow.py (3,046 lines):
├── FileIO implementation (PyArrowFileIO) — L393-697
│   └── Uses pyarrow.fs for S3/GCS/local access
├── Schema conversion (Iceberg ↔ Arrow) — L699-1575
│   └── 800+ lines of visitor pattern code
├── Expression conversion — L853-1069
│   └── Iceberg BooleanExpression → pc.Expression
├── Reading (ArrowScan) — L1728-1914
│   └── Uses pq.ParquetFile, pyarrow.dataset.Scanner
├── Writing — L2617-2960
│   └── Uses pq.ParquetWriter
├── Statistics collection — L2198-2500
│   └── Reads Parquet row group metadata
└── Delete file handling — L1070-1727
    └── Reads delete files, applies positional deletes
```

### 2.2 Dependency Graph

```mermaid
graph TD
    subgraph "Table operations (pyiceberg/table/__init__.py)"
        SCAN["DataScan.to_arrow()"]
        APPEND["Table.append()"]
        OVERWRITE["Table.overwrite()"]
        DELETE["Transaction.delete()"]
        UPSERT["Transaction.upsert()"]
    end

    subgraph "pyiceberg/io/pyarrow.py — the monolith"
        FILEIO["PyArrowFileIO<br/>(pyarrow.fs)"]
        SCHEMA["schema_to_pyarrow()<br/>pyarrow_to_schema()<br/>(800 lines of visitors)"]
        EXPR["expression_to_pyarrow()<br/>(Iceberg → pc.Expression)"]
        READ["ArrowScan<br/>(pq.ParquetFile + Scanner)"]
        WRITE["write_file()<br/>_dataframe_to_data_files()<br/>(pq.ParquetWriter)"]
        STATS["StatsAggregator<br/>(Parquet metadata)"]
        DELETES["Delete file handling<br/>(read + apply)"]
    end

    subgraph "pyiceberg/io/__init__.py"
        ABS["FileIO (ABC)<br/>InputFile (ABC)<br/>OutputFile (ABC)"]
    end

    SCAN --> READ
    SCAN --> DELETES
    APPEND --> WRITE
    OVERWRITE --> WRITE
    DELETE --> READ
    DELETE --> WRITE
    UPSERT --> READ
    UPSERT --> WRITE

    READ --> FILEIO
    READ --> SCHEMA
    READ --> EXPR
    WRITE --> FILEIO
    WRITE --> STATS
    WRITE --> SCHEMA
    DELETES --> READ

    FILEIO -.->|"implements"| ABS
```

### 2.3 The Three Layers of Coupling

**Layer 1: Byte-level I/O — Already abstract**

`FileIO` is an ABC: `new_input(path) → InputFile`, `new_output(path) → OutputFile`,
`delete(path)`. `PyArrowFileIO` implements it via `pyarrow.fs`. This is already pluggable.

**Layer 2: Parquet Read/Write — Tightly coupled to `pyarrow.parquet`**

`ArrowScan` calls `pq.ParquetFile()` and `Scanner.to_batches()`. `write_file` calls
`pq.ParquetWriter()`. These are direct API calls to the `pyarrow` library, not behind
any interface.

However: all inputs/outputs at this layer are Arrow types (RecordBatch, Schema). A
different Parquet library (DataFusion, DuckDB) would produce the same Arrow types.
The coupling is to the **library**, not to the **format**.

**Layer 3: Compute — Partially coupled**

- `pa.Table.sort_by()` — coupled to PyArrow
- `pc.is_in()` for delete resolution — coupled to PyArrow
- No join capability exists in PyArrow at all (the gap that motivates this entire project)

This layer is where DataFusion provides capabilities PyArrow structurally lacks.

---

## 3. The Pluggable Interface: What It Looks Like

### 3.1 Formal Decomposition

```
Backend = ParquetReader × ParquetWriter × ComputeEngine

Where all three communicate exclusively via Arrow RecordBatch/Table.
```

Since Arrow is the fixed wire format, the interface between PyIceberg and any backend
is purely:

```
Input:  Arrow data (RecordBatch/Table) + metadata (file paths, Schema, predicates)
Output: Arrow data (RecordBatch/Table) + metadata (DataFile descriptors)
```

### 3.2 Interface Definition

```python
class IOBackend(Protocol):
    """Who reads and writes Parquet files."""

    def read_parquet(
        self,
        location: str,
        schema: Schema,
        projection: list[int],           # field IDs to project
        filter: BooleanExpression,
        io_properties: dict[str, str],
    ) -> Iterator[pa.RecordBatch]: ...

    def write_parquet(
        self,
        batches: Iterator[pa.RecordBatch],
        location: str,
        schema: Schema,
        properties: dict[str, str],       # target file size, compression, etc.
        io_properties: dict[str, str],
    ) -> DataFile: ...                    # returns metadata about written file

    def collect_statistics(
        self,
        location: str,
        schema: Schema,
        io_properties: dict[str, str],
    ) -> dict[int, ColumnStatistics]: ... # field_id → stats


class ComputeBackend(Protocol):
    """Who does sort/join/filter on Arrow data."""

    def sort(
        self,
        data: pa.Table | Iterator[pa.RecordBatch],
        sort_keys: list[tuple[str, str]],  # (column, "ascending"/"descending")
        memory_limit: int,
    ) -> Iterator[pa.RecordBatch]: ...

    def anti_join(
        self,
        left: pa.Table | Iterator[pa.RecordBatch],
        right: pa.Table | Iterator[pa.RecordBatch],
        on: list[str],
        memory_limit: int,
    ) -> Iterator[pa.RecordBatch]: ...

    def filter(
        self,
        data: pa.Table | Iterator[pa.RecordBatch],
        predicate: BooleanExpression,
    ) -> Iterator[pa.RecordBatch]: ...
```

**Key property:** Every input and output is Arrow. The interface is engine-agnostic
at the type level. Any engine that can produce/consume Arrow RecordBatches can implement it.

### 3.3 Why Arrow Makes This Tractable

```mermaid
graph LR
    subgraph "All engines speak Arrow"
        PA["PyArrow<br/>pa.RecordBatch"]
        DF["DataFusion<br/>RecordBatch (Rust→Python via C Data Interface)"]
        DDB["DuckDB<br/>.fetch_arrow_table()"]
        POL["Polars<br/>.to_arrow()"]
    end

    subgraph "Universal format"
        ARROW["Arrow RecordBatch<br/>(columnar memory layout)"]
    end

    PA --> ARROW
    DF --> ARROW
    DDB --> ARROW
    POL --> ARROW

    ARROW --> PA
    ARROW --> DF
    ARROW --> DDB
    ARROW --> POL
```

Because Arrow is the universal interchange format, there is **zero serialization cost**
between backends. DataFusion produces an Arrow RecordBatch; PyIceberg's write layer
(even if still using PyArrow's ParquetWriter) consumes it directly. No copy, no
conversion.

This means a hybrid approach works: DataFusion for compute, PyArrow for writing.
Or DuckDB for reading, DataFusion for compute, PyArrow for writing. The Arrow format
makes composition free.

---

## 4. Speed-of-Light Analysis: What Does Pluggability Cost?

### 4.1 The Overhead of Indirection

A pluggable interface adds one virtual dispatch per operation call. Cost:

```
T_dispatch = O(1) ≈ 50ns (Python attribute lookup + function call)
T_operation = O(N/D) ≈ seconds (I/O + compute)
T_dispatch / T_operation ≈ 10⁻⁸ — negligible
```

### 4.2 The Real Cost: Refactoring the Monolith

The actual cost is human effort to decompose `pyiceberg/io/pyarrow.py`:

| Task | Effort | Risk |
|------|--------|------|
| Define `IOBackend` + `ComputeBackend` protocols | Small | Low |
| Extract `PyArrowIOBackend` from monolith (move code, no behavior change) | **Large** | Medium (3K lines, many internal dependencies) |
| Schema conversion stays shared (Arrow format is fixed) | None | None |
| Implement `DataFusionComputeBackend` | Medium | Low (new code) |
| Implement `DataFusionIOBackend` (read via register_parquet) | Medium | Low |
| Wire up backend selection in table operations | Medium | Low |

### 4.3 The Schema Conversion Is NOT Backend-Specific

A crucial insight: `schema_to_pyarrow()` and `pyarrow_to_schema()` (800 lines) are NOT
"coupling to PyArrow." They convert between Iceberg's schema model and the Arrow format's
schema model. Since Arrow is the universal format, this conversion is needed by ALL
backends. DataFusion, DuckDB, and Polars all use Arrow schemas.

These 800 lines are **shared infrastructure**, not backend-specific code. They stay
regardless of which backend reads/writes. This significantly reduces the actual
refactoring surface — the schema code doesn't move; it's already in the right place.

### 4.4 Expression Conversion IS Backend-Specific

`expression_to_pyarrow()` converts Iceberg predicates → `pc.Expression`. A DataFusion
backend would need `expression_to_sql()`. A DuckDB backend would need its own conversion.

This is a genuine per-backend cost. But expressions are comparatively simple — a typical
predicate converts to one line of SQL. The 200-line PyArrow expression visitor is large
because of PyArrow's verbose API, not because the logic is complex.

---

## 5. Feasibility Verdict

### 5.1 Is It Tangible?

**Yes, but with the correct ordering.**

The interface is narrow (~7 methods). Arrow as the wire format means zero-copy
composition. The refactoring is large but mechanical (move existing code behind a
protocol without changing behavior). New backends are moderate effort because they
just need to produce/consume Arrow.

### 5.2 Is It Worth Doing Now?

**No — but it's worth designing toward.**

The cost-benefit today:
```
Cost_now  = R (refactor monolith) + I_interface + M (maintenance)
Benefit_now = B_datafusion (only one new backend exists)
```

The cost-benefit after DataFusion implementation proves the pattern:
```
Cost_later = R (refactor monolith, with benefit of hindsight)
Benefit_later = B_datafusion + B_duckdb + B_polars + community contributions
```

Doing the refactoring NOW gives us one new backend (DataFusion) but risks getting
the interface wrong. Doing it AFTER gives us a proven DataFusion implementation to
generalize from, and the interface emerges from real usage rather than speculation.

---

## 6. The Phased Strategy

### Phase 1 (Now): Build DataFusion Directly, Interface Emerges Implicitly

Build `pyiceberg/execution/compute.py` with DataFusion. The function signatures ARE the
implicit interface:

```python
def anti_join(left: pa.Table, right: pa.Table, on: list[str], ...) -> pa.Table: ...
def sort_batches(data: pa.Table, sort_keys: list[str], ...) -> pa.Table: ...
def filter_parquet(file_path: str, predicate_sql: str, ...) -> pa.Table: ...
```

These signatures are engine-agnostic: accept Arrow, return Arrow. The implementation
uses DataFusion. Swapping to DuckDB later means changing function bodies, not signatures.

**This is the second concrete implementation** (alongside PyArrow). After two exist, the
shared interface becomes obvious.

### Phase 2 (After DataFusion is proven): Formalize the Protocol

Extract `ComputeBackend` and `IOBackend` protocols by observing what PyArrow and
DataFusion have in common. Refactor `pyiceberg/io/pyarrow.py` into a `PyArrowBackend`
that implements the protocol.

This is a **pure refactoring** — no new features, just restructuring. Behavior is
unchanged. Fully testable by running existing test suite against the extracted backend.

### Phase 3 (Community-driven): Additional Backends

With the protocol defined, contributors can add:
- `DuckDBBackend` (read + compute via DuckDB)
- `PolarsBackend` (read + compute via Polars)
- Hybrid configurations (DuckDB for read, DataFusion for compute, PyArrow for write)

Backend selection could be configured via `.pyiceberg.yaml`:
```yaml
execution:
  backend: datafusion  # or: pyarrow, duckdb
```

### Why This Ordering Is Correct

```mermaid
graph LR
    subgraph "Phase 1: Build second implementation"
        P1["DataFusion implementation<br/>(concrete, working)"]
    end

    subgraph "Phase 2: Observe common interface"
        P2["Protocol extracted from<br/>PyArrow + DataFusion"]
    end

    subgraph "Phase 3: Generalize"
        P3["DuckDB, Polars, etc.<br/>implement the protocol"]
    end

    P1 --> P2 --> P3
```

**CS Principle (Interface Emergence — Fowler):** Correct abstractions emerge from
generalizing concrete implementations, not from speculative design.

> "When you have two or three implementations of something, then you can see what
> the interface should be. When you have one implementation, you're just guessing."

We have one implementation today (PyArrow). We're building a second (DataFusion). After
both exist, the shared interface becomes obvious. Building the abstraction before the
second implementation violates this principle.

---

## 7. Does This Change Our Immediate Plan?

**No.** Kevin's comment signals long-term direction, not a prerequisite for our first PR.
Our plan stays:

1. Engine resolution module
2. Bounded-session helpers
3. Object store bridge
4. First operation (upsert or equality deletes)

The function signatures we write will naturally form the protocol that Phase 2 extracts.
We are building toward pluggability by writing clean Arrow-in/Arrow-out functions — not
by building the abstraction layer first.

---

## 8. Summary

| Question | Answer |
|----------|--------|
| Can we decouple from Arrow the format? | **No** — it's the universal standard, in the public API, correct and permanent |
| Can we decouple from `pyarrow` the library? | **Yes** — other libraries can read/write Parquet and compute on Arrow |
| Is the interface narrow? | Yes — ~7 methods, all Arrow-in/Arrow-out |
| Is refactoring the monolith feasible? | Yes, but large (3K lines) and best done after Phase 1 |
| Should we build the pluggable interface now? | No — build DataFusion first, extract protocol after |
| Does this change our immediate plan? | No — we write clean Arrow-based functions that ARE the implicit interface |
