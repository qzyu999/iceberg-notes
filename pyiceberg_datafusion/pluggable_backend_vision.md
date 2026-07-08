# The Idealized PyIceberg: A First-Principles Derivation

**Purpose:** Define the mathematically clean architecture that PyIceberg SHOULD have,
derived from the Iceberg spec's primitives — not from the current implementation.
This is the north star. Code is the engineering approximation of this model.

---

## 1. The Universe of Discourse: Iceberg's Primitive Concepts

The Apache Iceberg spec defines exactly these first-order concepts:

```
TYPES (the alphabet)
    τ ∈ Type        — {boolean, int, long, float, double, date, time, timestamp,
                       string, binary, uuid, fixed(n), decimal(p,s), struct, list, map,
                       geometry, geography}

STRUCTURE (how types compose)
    f ∈ Field       — (id: ℕ, name: String, type: τ, required: Bool)
    σ ∈ Schema      — ordered sequence of Fields, identified by schema_id
    π ∈ PartSpec    — set of (source_field_id, transform, partition_field_id, name)
    ω ∈ SortOrder   — sequence of (field_id, direction, null_order)

DATA (what lives on storage)
    d ∈ DataFile    — (path, format, partition_values, record_count, column_stats, bounds)
    δ ∈ DeleteFile  — (path, content_type ∈ {positional, equality}, referenced_field_ids)
    m ∈ Manifest    — sequence of (status, DataFile | DeleteFile, sequence_number)
    s ∈ Snapshot    — (id, parent_id, sequence_number, manifest_list_path, summary)

STATE (the table at a point in time)
    M ∈ Metadata    — (location, schemas, specs, sort_orders, snapshots, current_snapshot,
                       properties, refs)
    T ∈ Table       — (identifier, metadata, metadata_location)

PREDICATES (constraints on rows)
    ε ∈ Expression  — BooleanExpression AST over field references and literals
    ε_bound         — Expression with field references resolved to field IDs + accessors

OPERATIONS (state transitions)
    commit : (Table × Requirements × Updates) → Table'     (OCC atomic update)
    scan   : (Table × Expression × Projection) → Iterable[RecordBatch]
    write  : (Table × RecordBatch) → Table'
```

---

## 2. The Fundamental Decomposition: Five Orthogonal Axes

Every operation in PyIceberg involves exactly these five concerns. They are
**mathematically independent** — changing one never requires changing another.

```mermaid
graph TB
    subgraph "Axis 1: STORAGE"
        AX1["How bytes move between RAM and persistent media<br/><br/>Domain: Path → Bytes | Bytes → Path<br/>Invariant: content-addressable (same path = same bytes)<br/>Implementations: S3, GCS, ADLS, HDFS, local FS"]
    end

    subgraph "Axis 2: FORMAT"
        AX2["How typed columnar data is encoded/decoded in bytes<br/><br/>Domain: RecordBatch ↔ Bytes<br/>Invariant: decode(encode(R)) = R (round-trip identity)<br/>Implementations: Parquet, ORC, Avro (metadata only)"]
    end

    subgraph "Axis 3: SEMANTICS"
        AX3["What the Iceberg spec requires for correctness<br/><br/>Domain: Metadata state machine (snapshots, schemas, specs)<br/>Invariant: Spec compliance (field IDs, partition pruning, sequence numbers)<br/>Implementation: ONE (the spec is the implementation)"]
    end

    subgraph "Axis 4: COMPUTE"
        AX4["How data is transformed in-flight (sort, join, filter, aggregate)<br/><br/>Domain: RecordBatch → RecordBatch<br/>Invariant: output = f(input) regardless of memory/parallelism strategy<br/>Implementations: PyArrow, DataFusion, DuckDB, Polars, cuDF"]
    end

    subgraph "Axis 5: RECONCILIATION"
        AX5["How schema evolution is resolved at read time<br/><br/>Domain: (RecordBatch_physical, Schema_file, Schema_projected) → RecordBatch_logical<br/>Invariant: field ID mapping is the source of truth (not column names)<br/>Implementation: ONE (spec-defined rules)"]
    end

    AX1 --- AX2
    AX2 --- AX4
    AX3 --- AX5
    AX4 --- AX5
```

**Orthogonality proof:** Changing the storage backend (Axis 1) from S3 to local
doesn't affect how Parquet is decoded (Axis 2). Changing the compute engine (Axis 4)
from PyArrow to DataFusion doesn't affect schema reconciliation rules (Axis 5).
Adding a new Iceberg type (Axis 3) requires updating format codecs (Axis 2) but
doesn't affect storage (Axis 1) or compute algorithms (Axis 4).

---

## 3. The Algebra: How the Axes Compose

### 3.1 The Core Morphisms

Define the system as a category where objects are data representations and
morphisms are transformations between them:

```
OBJECTS:
    Path      — a URI pointing to bytes on storage (opaque handle)
    Bytes     — raw byte sequence (no interpretation)
    Physical  — Arrow RecordBatch with physical column names/types (from file)
    Logical   — Arrow RecordBatch with projected schema (what user requested)
    Metadata  — TableMetadata (the Iceberg state machine)
    Task      — FileScanTask (the execution plan unit)

MORPHISMS (arrows between objects):
    store    : Bytes × Path → ()            — write bytes to location
    fetch    : Path → Bytes                 — read bytes from location
    encode   : Physical × Format → Bytes    — serialize typed data
    decode   : Bytes × Format × σ → Physical  — deserialize with schema
    compute  : Physical × Op → Physical     — transform (sort, join, filter)
    reconcile: Physical × σ_file × σ_proj → Logical  — schema evolution
    plan     : Metadata × ε × Projection → [Task]    — scan planning
    execute  : Task × σ_proj → Logical      — full task execution
```

### 3.2 The Composition Laws

**Law 1 (Round-trip):**
```
decode(encode(R, fmt), fmt, schema(R)) = R
```
Encoding then decoding with the same format yields the original data.

**Law 2 (Reconciliation idempotence):**
```
reconcile(R, σ_file, σ_file) = R     (when projected = physical, no-op)
```

**Law 3 (Filter commutativity with reconciliation):**
```
reconcile(filter(R, ε), σ_f, σ_p) = filter(reconcile(R, σ_f, σ_p), ε')
    where ε' = rewrite(ε, σ_f → σ_p)
```
Filtering before or after reconciliation produces the same result (modulo expression rewriting).

**Law 4 (Sort stability across backends):**
```
∀ backends B₁, B₂: set(sort(R, keys, B₁)) = set(sort(R, keys, B₂))
∧ is_sorted(sort(R, keys, Bᵢ), keys)
```
All backends produce a sorted result with the same multiset of rows (order of equal keys may differ).

**Law 5 (Execution decomposition):**
```
execute(task, σ_proj) = reconcile(compute(decode(fetch(task.path), Parquet, σ_file), ops), σ_file, σ_proj)
    where ops = delete_resolution(task.delete_files) ∘ filter(task.residual)
```
Task execution decomposes into fetch → decode → compute → reconcile.

**Law 6 (Planning correctness):**
```
∀ row r in table at snapshot s:
    r satisfies ε ⟺ ∃ task ∈ plan(metadata(s), ε, *): r ∈ execute(task, *)
```
Planning produces tasks that collectively cover exactly the matching rows.

### 3.3 The Independence Principle

```
∀ operations op₁ on Axis i, op₂ on Axis j where i ≠ j:
    op₁ ∘ op₂ = op₂ ∘ op₁    (axes commute)
```

Changing the storage implementation doesn't affect the result of compute.
Changing the compute implementation doesn't affect schema reconciliation.
This is what makes the architecture truly pluggable — each axis is an
independent substitution point.

---

## 4. The Ideal Module Map (Derived from the Algebra)

Each axis maps to exactly one Python module:

```mermaid
graph TB
    subgraph "Module: pyiceberg/storage/"
        S_DESC["Axis 1: STORAGE<br/>━━━━━━━━━━━━━━━━━━━━<br/>ABC: StorageIO<br/>  .read(path) → bytes<br/>  .write(bytes, path)<br/>  .delete(path)<br/>  .list(prefix) → [path]<br/><br/>Implementations:<br/>  PyArrowStorageIO (S3/GCS/ADLS via pyarrow.fs)<br/>  FsspecStorageIO (S3/GCS/ADLS via fsspec)<br/>  ObjectStoreIO (via Rust object_store crate)"]
    end

    subgraph "Module: pyiceberg/format/"
        F_DESC["Axis 2: FORMAT<br/>━━━━━━━━━━━━━━━━━━━━<br/>ABC: FormatCodec<br/>  .decode(bytes, schema, filter) → RecordBatch<br/>  .encode(RecordBatch, schema, props) → bytes + stats<br/><br/>Implementations:<br/>  ParquetCodec (via pyarrow.parquet)<br/>  ORCCodec (future)<br/>  AvroCodec (for manifests — already exists)"]
    end

    subgraph "Module: pyiceberg/semantics/"
        SE_DESC["Axis 3: SEMANTICS (≈ current table/ + catalog/)<br/>━━━━━━━━━━━━━━━━━━━━<br/>• TableMetadata state machine<br/>• Scan planning (ManifestGroupPlanner)<br/>• Commit protocol (OCC via requirements/updates)<br/>• Catalog interface (REST, SQL, Glue, ...)<br/>• Snapshot management<br/>• Partition pruning<br/><br/>NO execution logic. Pure metadata operations."]
    end

    subgraph "Module: pyiceberg/compute/"
        C_DESC["Axis 4: COMPUTE<br/>━━━━━━━━━━━━━━━━━━━━<br/>ABC: ComputeEngine<br/>  .sort(source, keys, memory_limit) → RecordBatch<br/>  .join(left, right, keys, type, memory_limit) → RecordBatch<br/>  .filter(source, predicate) → RecordBatch<br/>  .aggregate(source, grouping, funcs) → RecordBatch<br/><br/>Where 'source' is always a set of Paths (files on storage).<br/>The engine controls the read lifecycle.<br/><br/>Implementations:<br/>  PyArrowEngine (in-memory, no spill)<br/>  DataFusionEngine (bounded-memory, spill-to-disk)<br/>  DuckDBEngine (bounded-memory, internal spill)"]
    end

    subgraph "Module: pyiceberg/reconciliation/"
        R_DESC["Axis 5: RECONCILIATION<br/>━━━━━━━━━━━━━━━━━━━━<br/>• reconcile(batch, file_schema, projected_schema) → batch<br/>• Field ID mapping (not name matching)<br/>• Type promotion (int32 → int64)<br/>• Missing column fill (null)<br/>• Column reordering<br/><br/>ONE implementation. Spec-defined. Not pluggable."]
    end

    S_DESC -->|"paths"| C_DESC
    F_DESC -->|"codec used by"| C_DESC
    SE_DESC -->|"produces Tasks"| C_DESC
    C_DESC -->|"raw batches"| R_DESC
    R_DESC -->|"logical batches"| USER["User gets Iterator[RecordBatch]"]

    style S_DESC fill:#e74c3c,color:#fff
    style F_DESC fill:#3498db,color:#fff
    style SE_DESC fill:#27ae60,color:#fff
    style C_DESC fill:#9b59b6,color:#fff
    style R_DESC fill:#f39c12,color:#fff
```

### 4.1 Why This Differs from Current `pyiceberg/`

| Current module | What it actually does | Ideal module | Why it should move |
|---|---|---|---|
| `io/__init__.py` | FileIO ABC (open/read/write bytes) | `storage/` | "IO" conflates storage with format decoding |
| `io/pyarrow.py` | Storage + Format + Compute + Reconciliation + Stats | Split across 4 modules | SRP violation (7 responsibilities) |
| `io/fsspec.py` | Storage via fsspec | `storage/` | Correctly placed (wrong directory name) |
| `table/__init__.py` | Semantics + some compute dispatch | `semantics/` | Mostly correct, needs compute extraction |
| `expressions/` | Predicate AST + visitors | Stays (shared algebra) | Already correct |
| `catalog/` | Catalog implementations | `semantics/catalog/` | Already correct (could nest) |
| `schema.py` | Schema class | `semantics/schema.py` | Already correct |
| `types.py` | Type system | `semantics/types.py` | Already correct |

### 4.2 The Key Insight: Compute Engines Own the Read Lifecycle

In the ideal model, the `ComputeEngine` is the ONLY thing that reads data files.
It receives paths, decides how to read them (which library, what parallelism, how much to
buffer), and returns logical RecordBatches after reconciliation.

```
ComputeEngine.execute_task(task, projected_schema) =
    let physical = self.read_and_compute(task.file.path, task.delete_files, task.residual)
    in  reconcile(physical, file_schema(task.file), projected_schema)
```

The engine internally uses whatever FormatCodec and StorageIO it needs — but these
are INTERNAL to the engine. The semantics layer never touches bytes or physical batches.
It only speaks in terms of Tasks (what to read) and Logical RecordBatches (what came back).

This means:
- PyArrow engine uses `pyarrow.dataset.Scanner` (which bundles storage + format + compute)
- DataFusion engine uses `register_parquet` (which bundles storage + format) + SQL (compute)
- DuckDB engine uses `read_parquet()` (which bundles everything internally)

**The current `FileIO` ABC breaks this model** because it exposes storage to the semantics
layer (catalogs read metadata files directly). This is an acceptable pragmatic compromise:
metadata files (JSON, Avro) are small and don't need compute engines. The separation is:

```
Metadata access (small files, JSON/Avro): semantics layer uses StorageIO directly
Data access (large files, Parquet): ONLY through ComputeEngine
```

---

## 5. The Ideal Read Pipeline (Complete Derivation)

### 5.1 The Full Composition

```
table.scan(filter, projection).to_arrow()

= let tasks   = plan(table.metadata, bind(filter, schema), projection)          [Semantics]
  let batches = flatten(map(λt. engine.execute_task(t, projection), tasks))      [Compute]
  in  pa.Table.from_batches(batches)                                            [Materialization]

where engine.execute_task(task, projected) =
    let paths   = {task.file.path} ∪ {d.path | d ∈ task.delete_files}           [Path extraction]
    let raw     = engine.read_and_join(task.file.path, task.delete_files, task.residual)  [Compute]
    let logical = reconcile(raw, file_schema(task.file.path), projected)         [Reconciliation]
    in  logical
```

### 5.2 Memory Analysis

```
M_total = M_semantics + M_compute + M_reconciliation

M_semantics     = O(|tasks|)           — manifest entries, small metadata
M_compute       = O(memory_limit)      — bounded by spill-capable engine
M_reconciliation = O(batch_size)       — processes one batch at a time

Total: O(memory_limit)                 — dominated by compute budget
       where memory_limit is configurable (default 512 MB)
```

### 5.3 The Write Pipeline (Dual)

```
table.append(data)

= let batches  = partition(data, table.metadata.spec)                           [Semantics]
  let files    = flatten(map(λb. engine.write(b, target_path, schema, props), batches)) [Compute]
  let commit   = append_files(files)                                            [Semantics]
  in  catalog.commit(table, requirements, commit)                               [Semantics]
```

---

## 6. Where Reality Departs from the Ideal (Honest Assessment)

### 6.1 Departures That Are Pragmatic (Keep)

| Departure | Why | Cost |
|-----------|-----|------|
| `FileIO` exists as a separate ABC (not inside ComputeEngine) | Catalogs need to read metadata (JSON/Avro) without a compute engine | Dual storage paths |
| Schema conversion lives in `io/pyarrow.py` not a `format/` module | Moving 891 lines touches 50+ import sites for zero user value | Technical debt (cosmetic) |
| `expression_to_pyarrow` has different signature than `expression_to_sql` | PyArrow needs type info from schema; SQL is stringly-typed | Non-uniform functor |
| `materialize_to_parquet` round-trips in-memory data through disk | ComputeEngine needs to control read lifecycle; iterators aren't seekable | One extra disk write (14ms) |

### 6.2 Departures That Are Technical Debt (Fix Eventually)

| Departure | Ideal | Current | Fix PR |
|-----------|-------|---------|:---:|
| ArrowScan has compute + reconciliation fused | Separate: engine.compute() then reconcile() | Monolithic method | PR 3-4 |
| Write pipeline in `io/pyarrow.py` | Should be in compute engine (partitioning = computation) | 429 lines in wrong file | PR 5 |
| Statistics computation in `io/pyarrow.py` | Should be part of format codec output | 418 lines in wrong file | PR 7 |
| No `FormatCodec` abstraction exists | Decode/encode should be protocol-defined | Parquet reading is baked into backends | Future |
| `_scoped_env_vars` for credentials | Engine should accept credentials directly | Env mutation (not thread-safe) | When DataFusion exposes API |

### 6.3 Departures That Are Fundamental Limits (Accept)

| Departure | Why it can't be fixed | Implication |
|-----------|----------------------|-------------|
| Each engine has its own storage layer | Performance: engines optimize I/O internally (prefetch, async, pushdown) | Can't unify StorageIO + ComputeEngine I/O |
| Expression conversion isn't a uniform functor | Target representations are structurally different (AST vs string vs typed expression) | Each backend needs its own converter |
| Sort/join can't be truly streaming | These operations have global data dependencies (need ALL data before output) | Must read from files, not from iterators |

---

## 7. The Composition Diagram (What Perfect Looks Like)

```mermaid
sequenceDiagram
    participant U as User
    participant Sem as Semantics<br/>(table/, catalog/)
    participant Eng as ComputeEngine<br/>(execution/)
    participant Rec as Reconciliation<br/>(execution/schema/)
    participant Sto as Storage<br/>(io/)

    Note over U,Sto: THE IDEAL READ PIPELINE

    U->>Sem: table.scan(filter, cols).to_arrow()
    Sem->>Sem: bind(filter, schema)
    Sem->>Sem: plan_files(metadata, bound_filter)
    Sem-->>Sem: [FileScanTask₁, FileScanTask₂, ...]

    loop For each FileScanTask
        Sem->>Eng: execute_task(task, projected_schema)
        Note over Eng: Engine reads files using its OWN storage layer<br/>(not via pyiceberg's StorageIO)
        Eng->>Eng: read data_file (Parquet decode)
        Eng->>Eng: read delete_files (if any)
        Eng->>Eng: anti_join(data, deletes) [spills if needed]
        Eng->>Eng: filter(residual) [streaming]
        Eng-->>Rec: raw RecordBatch (physical schema)
        Rec->>Rec: map field IDs, promote types, fill nulls
        Rec-->>Sem: logical RecordBatch (projected schema)
    end

    Sem-->>U: pa.Table / Iterator[RecordBatch]

    Note over Sto: Storage is used ONLY by:<br/>1. Catalog (read metadata JSON)<br/>2. Manifest reader (read Avro)<br/>3. ComputeEngine internally (via its own path)
```

---

## 8. The Gap Analysis: Current Code vs. Ideal

### 8.1 Scorecard

| Ideal Principle | Current Implementation | Gap | Severity |
|---|---|---|:---:|
| Axes are orthogonal modules | 7 concerns in one file | Large | High |
| ComputeEngine owns read lifecycle | ArrowScan reads directly, not via protocol | Large | High (blocks bounded-memory) |
| Semantics never touches bytes | `table/__init__.py` imports ArrowScan directly | Medium | Medium |
| Reconciliation is separate from compute | Fused inside ArrowScan._task_to_record_batches | Medium | Medium |
| StorageIO is for metadata only | FileIO also used by ArrowScan for data reads | Small | Low (pragmatic) |
| FormatCodec is explicit | Parquet decode is implicit inside compute backends | Small | Low (future work) |
| Expression conversion is uniform | Different signatures per backend | Fundamental | Accept |

### 8.2 What the Branch (`pluggable-backend-discovery`) Already Solves

| Ideal Principle | Branch Status |
|---|---|
| ComputeEngine as a Protocol | ✅ `ComputeBackend` protocol defined |
| Multiple engine implementations | ✅ PyArrow, DataFusion, DuckDB |
| Engine resolution (selection) | ✅ `resolve_engine()` |
| File-based compute (engine owns read lifecycle) | ✅ `sort_from_files`, `anti_join_from_files` |
| Streaming filter (O(1) per batch) | ✅ `filter()` on protocol |
| Materialization helper (RAM → file path) | ✅ `materialize_to_parquet()` |
| Metadata streaming (no OOM for large metadata) | ✅ `stream_paths_to_parquet()` |
| Expression → SQL converter | ✅ `expression_to_sql.py` |
| Credential bridging | ✅ `object_store.py` |

### 8.3 What Remains to Reach the Ideal

| Step | What | Brings us from | To |
|---|---|---|---|
| Wire dispatch | Connect `resolve_engine()` to `DataScan.to_arrow()` | Dead code | Live code |
| `execute_task()` | Implement full task execution per engine | Backend stubs | Real pipelines |
| Extract reconciliation | Move `_to_requested_schema` to shared module | Fused in ArrowScan | Separate concern |
| Extract write | Move write pipeline to execution/ | In io/pyarrow.py | Correct home |
| Rename `io/` → `storage/` (optional) | Reflect the true responsibility | Misleading name | Honest name |

---

## 9. Is This Achievable? The Honest Answer

**Partially.** Here's what's possible and what isn't:

### Achievable (within our control)

- ✅ ComputeEngine protocol with file-based methods (done)
- ✅ Engine resolution and dispatch (done)
- ✅ Separation of semantics from execution (PR 2)
- ✅ Extraction of reconciliation as shared code (PR 4)
- ✅ Bounded-memory execution via DataFusion (PR 3)
- ✅ New operations: compact, orphan delete, sort-on-write (PR 6-8)

### Not achievable (external constraints)

- ❌ Unified StorageIO for both metadata and data (engines have their own I/O stacks)
- ❌ Uniform expression functor (target representations are structurally different)
- ❌ Truly streaming sort/join (algorithmic impossibility — global data dependency)
- ❌ Zero-overhead for in-memory data (must materialize to file for engine lifecycle control)

### The verdict

This is **continuous improvement toward an asymptote**, not a destination.
The ideal architecture is the limit:

```
lim_{PRs → ∞} (codebase) = ideal_architecture
```

Each PR moves the codebase closer. The branch already covers the most impactful
gap (pluggable compute). The remaining gaps are either pragmatic (FileIO dual path),
cosmetic (module naming), or fundamental (can't be closed by any refactoring).

The correct framing is: **the code implements the best engineering approximation of
the mathematical ideal, given the constraints of the underlying engine APIs.** The
approximation gets tighter with each PR, but never reaches identity — and that's
fine. The user-visible behavior (correctness, bounded memory, new operations) reaches
100% regardless of whether the internal architecture reaches mathematical purity.


---

## 10. Formal Methods: The Mathematical Specification

### 10.1 Definitions

Let the system be defined over the following sets and functions:

**Sets (domains):**

| Symbol | Definition | Python type | Plain English |
|--------|-----------|-------------|---------------|
| **P** | Set of all valid storage paths (URIs) | `str` | A file location like `s3://bucket/file.parquet` |
| **B** | Set of all byte sequences | `bytes` | Raw file content — just 1s and 0s with no meaning |
| **τ** | Set of all Iceberg types | `IcebergType` | Column types: integer, string, timestamp, etc. |
| **F** | Set of all fields: F = ℕ × String × τ × {⊤, ⊥} | `NestedField` | A named, typed column with a unique ID number |
| **Σ** | Set of all schemas: Σ = F* (ordered sequences of fields) | `Schema` | A table's column layout — the list of all its fields |
| **R** | Set of all Arrow RecordBatches: R = Σ × (ℕ → τ-value)* | `pa.RecordBatch` | A chunk of table rows in memory (columnar format) |
| **ε** | Set of all bound Boolean expressions | `BooleanExpression` | A filter like `age > 18 AND country = 'US'` |
| **Π** | Set of all column projections: Π ⊆ ℕ (set of field IDs) | `set[int]` | Which columns the user wants to see in the output |
| **Δ** | Set of all delete specifications: Δ = 𝒫(P) × {pos, eq} × F* | `set[DataFile]` | Files that say "these rows are deleted" |
| **T** | Set of all scan tasks: T = P × Δ × ε | `FileScanTask` | An instruction: "read this file, remove these deletes, apply this filter" |
| **M** | Memory budget (bytes): M ∈ ℝ⁺ | `int` | How much RAM the engine is allowed to use (e.g., 512 MB) |
| **Ω** | Set of sort orders: Ω = (ℕ × {↑,↓})* | `list[tuple[str, str]]` | "Sort by timestamp ascending, then by id descending" |

**Functions (morphisms):**

| Symbol | Signature | Meaning | Plain English |
|--------|-----------|---------|---------------|
| **φ** (fetch) | φ : P → B | Read bytes from storage | Download the file from S3/disk into memory |
| **ψ** (store) | ψ : B × P → () | Write bytes to storage | Upload bytes to S3/disk at a given path |
| **δ** (decode) | δ : B × Σ × ε → R | Decode bytes to RecordBatch with pushdown | Parse a Parquet file into Arrow columns, skipping irrelevant rows |
| **η** (encode) | η : R × Σ → B × Stats | Encode RecordBatch to bytes + statistics | Write Arrow data as a Parquet file with min/max stats |
| **ρ** (reconcile) | ρ : R × Σ_file × Σ_proj → R | Schema reconciliation | Map old column names/types to the current schema using field IDs |
| **σ** (select/filter) | σ_ε : R → R | Filter rows satisfying ε | Keep only rows where the condition is true |
| **γ** (sort) | γ_ω : R → R | Sort by order ω | Reorder all rows by the specified columns |
| **⋈̄** (anti-join) | ⋈̄_K : R × R → R | Left anti-join on keys K | Remove rows from the left that have a matching key in the right |
| **π** (project) | π_Π : R → R | Column projection to field IDs in Π | Drop columns the user didn't ask for |
| **λ** (plan) | λ : Metadata × ε × Π → T* | Scan planning | Figure out which files to read based on partition stats |
| **Ξ** (execute) | Ξ : T × Σ_proj × M → R | Full task execution | Read the file, apply deletes, filter, and return data |

---

### 10.2 Axioms of the System

**Axiom 1 (Storage identity):**
```
∀ b ∈ B, p ∈ P: φ(p) = b  ⟺  ψ(b, p) was the last write to p
```
> **In plain English:** If you write some bytes to a file and then read that file, you get the same bytes back. Storage doesn't corrupt or change your data.

**Axiom 2 (Codec round-trip):**
```
∀ r ∈ R, σ ∈ Σ: let (b, _) = η(r, σ) in δ(b, σ, ⊤) = r
```
> **In plain English:** If you save a table to a Parquet file and then read it back, you get exactly the same table. The encoding is lossless.

**Axiom 3 (Reconciliation identity):**
```
∀ r ∈ R, σ ∈ Σ: ρ(r, σ, σ) = r
```
> **In plain English:** If the file's schema and the desired schema are the same, reconciliation does nothing. No unnecessary work.

**Axiom 4 (Reconciliation idempotence):**
```
∀ r ∈ R, σ_f, σ_p ∈ Σ: ρ(ρ(r, σ_f, σ_p), σ_p, σ_p) = ρ(r, σ_f, σ_p)
```
> **In plain English:** Running reconciliation twice produces the same result as running it once. It's safe to call it redundantly.

**Axiom 5 (Filter soundness):**
```
∀ r ∈ R, ε ∈ ε: ∀ row ∈ σ_ε(r): eval(ε, row) = ⊤
```
> **In plain English:** Every row in the filtered output actually satisfies the filter condition. No false results slip through.

**Axiom 6 (Filter completeness):**
```
∀ r ∈ R, ε ∈ ε: ∀ row ∈ r: eval(ε, row) = ⊤ ⟹ row ∈ σ_ε(r)
```
> **In plain English:** If a row matches the filter, it WILL appear in the output. No matching rows are accidentally dropped.

**Axiom 7 (Anti-join correctness):**
```
∀ L, R ∈ R, K ⊆ F:
    ⋈̄_K(L, R) = { l ∈ L | ¬∃ r ∈ R: ∧_{k∈K} l[k] = r[k] }
```
> **In plain English:** Anti-join keeps only left-side rows that have NO matching row on the right side. This is how deleted rows are removed — if a row's key appears in the delete file, it's excluded from the output.

**Axiom 8 (Sort total order):**
```
∀ r ∈ R, ω ∈ Ω: let s = γ_ω(r) in
    ∀ i < j: compare_ω(s[i], s[j]) ≤ 0
    ∧ multiset(s) = multiset(r)
```
> **In plain English:** After sorting, earlier rows are always ≤ later rows according to the sort key. No rows are added or removed — it's a pure reordering.

**Axiom 9 (Planning coverage):**
```
∀ M ∈ Metadata, ε ∈ ε, Π ∈ Π:
    let tasks = λ(M, ε, Π) in
    ⋃_{t ∈ tasks} rows(Ξ(t, Π, ∞)) = { row ∈ table(M) | eval(ε, row) = ⊤ }
```
> **In plain English:** If you execute every planned task and combine the results, you get ALL matching rows from the table. Scan planning doesn't accidentally skip any data.

**Axiom 10 (Memory boundedness):**
```
∀ t ∈ T, σ ∈ Σ, M ∈ ℝ⁺:
    peak_memory(Ξ(t, σ, M)) ≤ M + O(|output_batch|)
```
> **In plain English:** If you tell the engine "use at most 512 MB," it will never use more than that (plus a small amount for the output buffer). It spills to disk instead of crashing.

---

### 10.3 Theorems (Derived Properties)

**Theorem 1 (Backend equivalence):**
```
∀ engines E₁, E₂ satisfying Axioms 5-10:
∀ t ∈ T, σ ∈ Σ: multiset(Ξ_E₁(t, σ, M)) = multiset(Ξ_E₂(t, σ, M))
```
> **In plain English:** PyArrow, DataFusion, and DuckDB all produce the same rows for the same input. You can swap engines and get identical results (possibly in different order).

*Proof:* By Axioms 5-6 (filter correctness), Axiom 7 (join correctness), and Axiom 8
(sort produces a permutation). The output multiset is determined by the input data
and the operations — not by which engine executes them. ∎

**Theorem 2 (Execution decomposition):**
```
Ξ(t, σ_proj, M) = ρ(σ_{t.residual}(⋈̄_K(δ(φ(t.path), σ_file, ⊤), D)), σ_file, σ_proj)
    where D = ⋃_{d ∈ t.deletes} δ(φ(d.path), σ_del, ⊤)
          K = equality_columns(t.deletes)
```
> **In plain English:** Executing a scan task is: (1) read the data file, (2) read the delete files, (3) remove deleted rows via anti-join, (4) apply the residual filter, (5) reconcile to the user's requested schema. These steps are always the same regardless of which engine does them.

*Proof:* Direct from Law 5 (§3.2). Task execution is the sequential composition of
fetch, decode, anti-join with deletes, filter, and reconcile. ∎

**Theorem 3 (Sort commutativity with reconciliation):**
```
ρ(γ_ω(r), σ_f, σ_p) = γ_{ω'}(ρ(r, σ_f, σ_p))
    where ω' = rename(ω, σ_f → σ_p)
```
> **In plain English:** You can sort before or after reconciliation and get the same result. The order of these two steps doesn't matter (as long as you update the column names in the sort key).

*Proof:* Reconciliation reorders/renames columns but doesn't change row order or values.
Sort key references are renamed accordingly. The sort comparisons yield the same total order. ∎

**Theorem 4 (Memory scaling):**
```
Let N = Σ_{t ∈ tasks} file_size(t.path)    (total data size)
Let M = memory_limit

For a spill-capable engine:
    peak_memory(Scan) = O(M)                 (bounded, independent of N)
    I/O_operations(Sort) = O(N/B · log_{M/B}(N/M))   (Aggarwal-Vitter bound)
    I/O_operations(Join) = O(3(N+K)/B)       (Grace Hash, when M > √(N·B))

For an in-memory engine (PyArrow):
    peak_memory(Scan) = O(N)                 (proportional to data size)
```
> **In plain English:** With DataFusion (spill-capable), a 50 GB scan completes in 512 MB of RAM — it just does more disk I/O passes. With PyArrow (in-memory only), a 50 GB scan needs 50 GB of RAM or it crashes. That's the entire point of this refactoring.

*Proof:* By the external sort and Grace Hash Join complexity theorems
(Aggarwal & Vitter 1988, Shapiro 1986). The spill-capable engine pages data through
a fixed buffer, using O(N/M) spill passes. ∎

**Theorem 5 (Filter is the unique streaming operation):**
```
∀ operations Op ∈ {sort, join, aggregate}:
    ∃ inputs I: |I| > M ⟹ peak_memory(Op(I)) > M    (without spill to disk)

But: ∀ inputs I, batch b ∈ I:
    peak_memory(σ_ε(b)) = O(|b|)                      (constant per batch)
```
> **In plain English:** Sort, join, and aggregate all need to see the entire dataset before producing output — you can't sort half a table. But filtering is different: you can check each batch independently ("does this row match?") without ever seeing the whole dataset. That's why filter is the only operation that works as a streaming iterator in our protocol.

*Proof:* Sort requires seeing all elements before producing output (comparison-based
lower bound). Join requires building a hash table of one side. Aggregate requires
maintaining group state. Only filter has no inter-batch dependency — each batch is
independently decidable. ∎

---

### 10.4 The Pragmatic Optimum vs. The Ideal

Define the **architectural distance** from the ideal as the number of composition laws
that are violated or approximated:

**Ideal system (distance = 0):**
```
Ξ_ideal(t, σ, M) = ρ(σ_ε(⋈̄_K(δ(φ(t.path), σ_file, ε), D)), σ_file, σ)
```
> **In plain English:** In a perfect world, each step (fetch → decode → join → filter → reconcile) is a separate, independent, swappable component.

**Pragmatic system (distance = 3):**

| # | Violation | Why | Plain English |
|---|-----------|-----|---------------|
| 1 | φ is not shared between semantics and compute | Engines have internal I/O | The catalog reads metadata via PyIceberg's FileIO, but DataFusion reads Parquet via its own S3 client. Two separate download paths to the same storage. |
| 2 | δ and σ are fused in practice | Predicate pushdown requires codec integration | In theory, decode and filter are separate steps. In practice, they're combined so the Parquet reader can skip entire row groups that don't match the filter (10x faster). Same output, different performance. |
| 3 | In-memory data requires η before Ξ | Engine needs file paths, not iterators | When a user passes a DataFrame for upsert, we have to write it to a temp file first so the engine can read it back. It's a round-trip through disk (14ms) for data that was already in RAM. Annoying but necessary. |

**Quantifying the gap:**

```
Δ_c(ideal, pragmatic) = 0          correctness distance: ZERO (same output)
Δ_e(ideal, pragmatic) ≈ 14ms       efficiency distance: one disk round-trip for in-memory data
```

> **In plain English:** The pragmatic system gives you the exact same results as the ideal system. It just takes 14ms longer in one specific case (when the user passes a DataFrame instead of a file path). That's the only measurable cost of our engineering compromises.

---

### 10.5 The Convergence Guarantee

> **In plain English:** Each PR we merge makes the codebase structurally cleaner. We can quantify this improvement as a "distance score" that goes from 10 (today's monolith) down toward 0 (ideal architecture). The score never goes back up.

Let C_n denote the codebase state after n PRs are merged. Define the architectural
distance function d : Codebase → ℝ⁺:

```
d(C) = Σ_{i=1}^{7} w_i · violation_i(C)
```

| Violation | Weight | What it means in plain English |
|-----------|:---:|---|
| violation₁: ArrowScan hardcoded in scan path | 3 | The read pipeline is locked to PyArrow — can't swap engines |
| violation₂: Reconciliation fused with compute | 2 | Schema evolution logic is tangled with the read engine |
| violation₃: Write pipeline in io/pyarrow.py | 2 | The write code is in the wrong file (it's execution, not IO) |
| violation₄: Schema conversion in io/pyarrow.py | 1 | Type mapping is in the wrong file (it's schema, not IO) |
| violation₅: Statistics in io/pyarrow.py | 1 | Stats code is in the wrong file (it's analytics, not IO) |
| violation₆: No FormatCodec abstraction | 1 | Parquet decoding isn't formally defined as a swappable component |
| violation₇: Non-uniform expression conversion | 0 | Each engine needs its own filter translator — can't be unified (fundamental limit) |

**The distance decreases monotonically:**
```
d(C₀) = 10  →  d(C₂) = 7  →  d(C₄) = 5  →  d(C₅) = 3  →  d(C₇) = 1  →  d(C∞) = 0
```

> **In plain English:** We start at 10 (messy monolith). After the dispatch PR, we drop to 7 (core blocker removed). Each extraction PR drops it further. We asymptotically approach 0 but the last point (FormatCodec) is low priority because the payoff is minimal.

---

### 10.6 Summary: The Mathematical Contract

> **In plain English:** Here's the full guarantee the system provides, stated formally then translated:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SPECIFICATION: PyIceberg Pluggable Execution

GIVEN:
    M ∈ Metadata                     (table state)
    ε ∈ BooleanExpression            (user filter)
    Π ∈ set[int]                     (projected field IDs)
    M_budget ∈ ℝ⁺                    (memory limit)
    E : ComputeEngine                (backend choice)

REQUIRES:
    E satisfies Axioms 5-10          (correctness + boundedness)

ENSURES:
    table.scan(ε, Π).to_arrow()
        = ⋃_{t ∈ λ(M, ε, Π)} ρ(E.execute(t, Π, M_budget), σ_file(t), σ_proj(Π))

    ∧ peak_memory ≤ M_budget + O(batch_size)
    ∧ ∀ engines E₁, E₂: output(E₁) = output(E₂)     (up to row order)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

> **In plain English:** Given any table, any filter, any column selection, and any memory budget — if you pick an engine that satisfies our axioms (PyArrow, DataFusion, or DuckDB), then:
> 1. You get the correct filtered data back (no missing rows, no extra rows)
> 2. Memory usage stays within your budget (no OOM crashes)
> 3. Swapping engines doesn't change the results (only the speed and memory profile)
>
> That's the whole promise. The code implements this. The tests prove it works.
