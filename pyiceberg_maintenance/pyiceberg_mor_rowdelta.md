<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Rigorous Architectural Analysis: Merge-on-Read, RowDelta, and Deletion Vectors in Apache Iceberg & PyIceberg

This document provides a rigorous, first-principles mathematical and computer science architectural review of **Merge-on-Read (MoR)**, **RowDelta** commits, and **Deletion Vectors (DVs)**. It traces the design paradigms from physical storage limits to the Java implementation (`/iceberg/`), maps out the potential scope for V2 and V3 spec compatibility in PyIceberg (`/iceberg-python/`), and defines the structural mechanics for PyIceberg compaction engines.

Every mathematical formula and theoretical concept in this document is accompanied by concrete, visual data examples (tables, rows, and files) along with structured "Concept #" and "Plain English" translation mappings to make the engineering implications immediately intuitive.

---

## 1. Theoretical Foundations: Mathematical Formalism of Table Mutations

To construct a correct system design, we must define the state space and operations of an Iceberg table from mathematical axioms rather than empirical database constructs.

### 1.1 The Table as a State Machine
An Iceberg table $T$ is an immutable sequence of snapshot states:
$$T = \langle S_0, S_1, S_2, \dots, S_n \rangle$$

Each snapshot $S_k$ represents a logically consistent version of the table at transaction time $t_k$, defined as a 4-tuple:
$$S_k = (D_k, F_{e, k}, F_{p, k}, V_{d, k})$$

Where:
*   $D_k = \{d_1, d_2, \dots\}$ is the set of active **Data Files** (Parquet/ORC).
*   $F_{e, k} = \{f_{e, 1}, f_{e, 2}, \dots\}$ is the set of **Equality Delete Files**.
*   $F_{p, k} = \{f_{p, 1}, f_{p, 2}, \dots\}$ is the set of **Positional Delete Files**.
*   $V_{d, k} = \{v_{d, 1}, v_{d, 2}, \dots\}$ is the set of **Deletion Vectors** (Roaring Bitmaps stored in Puffin).

### 💡 Concept Mapping & Plain English Explanations (Table State)
| Variable / Symbol | Concept Name | Plain English Translation |
| :---: | :--- | :--- |
| **$T$** | **Concept 1**: The Table History | A timeline or movie reel of all snapshots from the birth of the table to the latest committed change. |
| **$S_k$** | **Concept 2**: The Current State Snapshot | A single snapshot frame at version $k$. It acts as a manifest list pointing to all files active at that point in time. |
| **$D_k$** | **Concept 3**: Active Data Files | The physical Parquet files containing the actual table records you want to query. |
| **$F_{e, k}$** | **Concept 4**: Equality Delete Files | Dynamic rules like "delete all rows where `dept = 'HR'`". They wipe out rows on-the-fly when reading the table. |
| **$F_{p, k}$** | **Concept 5**: Positional Delete Files | Physical logs that list exact file paths and row line numbers (offsets) of deleted records. |
| **$V_{d, k}$** | **Concept 6**: Deletion Vectors | High-performance Roaring Bitmaps that track exactly which row indexes inside a data file are deleted. |

#### 📊 Visual Example: Snapshot Evolution Timeline
Let's see how the table state actually changes across commits. Here is a timeline table tracking snapshots as they are created in the catalog:

| Snapshot State | Catalog Version | Active Data Files ($D_k$) | Equality Deletes ($F_{e,k}$) | Positional Deletes ($F_{p,k}$) | Deletion Vectors ($V_{d,k}$) | Logical Row Count |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **$S_0$** | Initial Table | `[data-1.parquet]` | `[]` | `[]` | `[]` | 1,000 |
| **$S_1$** | Row Added & Deleted | `[data-1.parquet, data-2.parquet]` | `[]` | `[delete-1-pos.parquet]` | `[]` | 1,190 (100 added, 10 deleted) |
| **$S_2$** | DV Applied | `[data-1.parquet, data-2.parquet]` | `[]` | `[delete-1-pos.parquet]` | `[delete-2-dv.puffin]` | 1,185 (5 deleted via DV) |

Here is how these snapshot tuples look mathematically:
*   $S_0 = (\{data\_1\}, \emptyset, \emptyset, \emptyset)$
*   $S_1 = (\{data\_1, data\_2\}, \emptyset, \{delete\_1\_pos\}, \emptyset)$
*   $S_2 = (\{data\_1, data\_2\}, \emptyset, \{delete\_1\_pos\}, \{delete\_2\_dv\})$

---

### 1.2 The Logical Row Projection Operator
Let $d \in D_k$ be a data file containing an ordered sequence of physical records. Let $I_d = [0, |d| - 1] \subset \mathbb{N}_0$ be the index space of the row offsets within $d$. We define the physical record mapping as:
$$\text{Record}_d: I_d \to \text{Row}$$

In a pure read-optimized (Copy-on-Write) state, all physical records are logical records. In a write-optimized (Merge-on-Read) state, the active logical row set of data file $d$ under snapshot $S_k$, denoted as $\text{ActiveRows}(d, S_k)$, is the projection of physical records whose indexes do not belong to the set of deleted indexes:
$$\text{ActiveRows}(d, S_k) = \{ \text{Record}_d(i) \mid i \in I_d \land \Psi(d, i, S_k) = 0 \}$$

The deletion indicator function $\Psi(d, i, S_k) \in \{0, 1\}$ is defined as:
$$\Psi(d, i, S_k) = \mathbb{I} \left[ i \in \text{PosDeletes}(d) \lor i \in \text{DVDeletes}(d) \lor \bigvee_{f_e \in F_{e, k}} \text{MatchesEquality}(f_e, \text{Record}_d(i)) \right]$$

Where:
*   $\mathbb{I}[P]$ is the indicator function (returns 1 if predicate $P$ is true, 0 otherwise).
*   $\text{PosDeletes}(d) = \{ pos \mid \exists f_p \in F_{p, k} \text{ s.t. } (f_p.\text{path} = d.\text{path} \land f_p.\text{pos} = pos) \}$.
*   $\text{DVDeletes}(d) = \{ pos \mid pos \in B_d \}$ where $B_d$ is the roaring bitmap serialized in $V_{d, k}$ associated with data file $d$.
*   $\text{MatchesEquality}(f_e, r) = \mathbb{I} [ \Pi_{A}(r) = \Pi_{A}(r_{f_e}) ]$ where $A$ is the set of equality field IDs defined by the delete file schema, and $\Pi$ is the projection operator.

### 💡 Concept Mapping & Plain English Explanations (Row Resolution)
| Variable / Symbol | Concept Name | Plain English Translation |
| :---: | :--- | :--- |
| **$\text{Record}_d(i)$** | **Concept 7**: Row Offset Selector | A function that points to a specific physical line number (index $i$) in a Parquet file to fetch its fields. |
| **$I_d$** | **Concept 8**: Index Range (Zero-based space) | The zero-based array of row offsets in data file $d$, spanning from `0` to the last record index (`length - 1`). |
| **$\text{ActiveRows}(d, S_k)$** | **Concept 9**: Active Logical Rows | The final set of rows that are actually returned to the client after filtering out all deleted records. |
| **$\Psi(d, i, S_k)$** | **Concept 10**: Deletion Indicator Flag | A virtual checkbox (0 or 1) for every physical row. If it is 1, the row is deleted and skipped; if 0, the row is active and kept. |
| **$\text{PosDeletes}(d)$** | **Concept 11**: Target Positional Indexes | A lookup list of exact row positions in data file $d$ marked for deletion by active positional delete files. |
| **$\text{DVDeletes}(d)$** | **Concept 12**: Target Deletion Vector Offsets | A bitset lookup identifying which row offsets are marked as deleted in the roaring bitmap for data file $d$. |
| **$\text{MatchesEquality}(f_e, r)$** | **Concept 13**: Dynamic Predicate Matcher | A check to see if a record's values match an equality delete rule (e.g., matching a specified ID or department). |

#### 📊 Visual Example: Resolving Logical Active Rows
Let's trace exactly how a query engine reads a physical Parquet data file `d1` and filters out deleted records under Snapshot $S_1$.

##### 1. Physical Data File (`d1`) on Disk
| Row Index ($i$) | `id` (INT) | `name` (VARCHAR) | `dept` (VARCHAR) |
|---|---|---|---|
| **0** | 1 | "Alice" | "HR" |
| **1** | 2 | "Bob" | "Sales" |
| **2** | 3 | "Charlie" | "Eng" |
| **3** | 4 | "Diana" | "HR" |

##### 2. Active Delete Files in Snapshot $S_1$
*   **Positional Delete ($f_{p, 1}$)**: Contains target row `(path="s3://bucket/d1.parquet", pos=1)`.
*   **Deletion Vector ($v_{d, 1}$)**: Roaring Bitmap contains element `{2}`.
*   **Equality Delete ($f_{e, 1}$)**: Specifies delete rule `dept = "Sales"`.

##### 3. Bitwise Evaluation Table of $\Psi(d_1, i, S_1)$
Let's see the step-by-step indicator evaluation for every single row offset:

| Row Index ($i$) | Physical Data Record | PosDelete? ($i \in \{1\}$) | DVDelete? ($i \in \{2\}$) | Equality Match? (`dept == "Sales"`) | Indicator Value $\Psi$ | Action |
| :---: | :--- | :---: | :---: | :---: | :---: | :--- |
| **0** | `Row(id=1, name="Alice", dept="HR")` | 0 (False) | 0 (False) | 0 (False) | **0** | **Retained (Active)** |
| **1** | `Row(id=2, name="Bob", dept="Sales")` | 1 (True) | 0 (False) | 1 (True) | **1** | **Dropped (Deleted)** |
| **2** | `Row(id=3, name="Charlie", dept="Eng")` | 0 (False) | 1 (True) | 0 (False) | **1** | **Dropped (Deleted)** |
| **3** | `Row(id=4, name="Diana", dept="HR")` | 0 (False) | 0 (False) | 0 (False) | **0** | **Retained (Active)** |

##### 4. Resulting Logical Active Row Set
$$\text{ActiveRows}(d_1, S_1) = \{ \text{Row}(id=1, name="Alice", dept="HR"), \text{Row}(id=4, name="Diana", dept="HR") \}$$

```mermaid
graph TD
    subgraph Physical Parquet File d1
        R0["[Index 0] Alice (HR)"]
        R1["[Index 1] Bob (Sales)"]
        R2["[Index 2] Charlie (Eng)"]
        R3["[Index 3] Diana (HR)"]
    end
    
    subgraph Active Delete Metadata
        PosDel["Positional Delete f_p1<br/>Target: d1, Offset: 1"]
        DV["Deletion Vector v_d1<br/>Target: d1, Bitmask: {2}"]
        EqDel["Equality Delete f_e1<br/>Rule: dept = 'Sales'"]
    end
    
    R0 --> |Psi = 0| Out1["[Returned Row 0] Alice"]
    R1 --> |Psi = 1| Filter1{Filtered Out}
    R2 --> |Psi = 1| Filter2{Filtered Out}
    R3 --> |Psi = 0| Out2["[Returned Row 3] Diana"]
    
    PosDel --> R1
    DV --> R2
    EqDel --> R1
```

---

### 1.3 Write Amplification (WAF) vs. Read Amplification (RAF)
Let $D_{\text{target}} \subseteq D_k$ be the set of data files containing records targeted for mutation (updates/deletes). Let $R_{\text{mut}}$ be the subset of targeted records, and $R_{\text{total}}$ be the total records in $D_{\text{target}}$.

#### Copy-on-Write (CoW)
In Copy-on-Write, a mutation requires rewriting the entire data files containing the targeted records.
*   **Write Amplification Factor (WAF):**
    $$\text{WAF}_{\text{CoW}} = \frac{\sum_{d \in D_{\text{target}}} \text{SizeOf}(d)}{\text{SizeOf}(R_{\text{mut}})} \approx \frac{\text{SizeOf}(R_{\text{total}})}{\text{SizeOf}(R_{\text{mut}})}$$
*   **Read Amplification Factor (RAF):**
    $$\text{RAF}_{\text{CoW}} = 1.0$$

#### Merge-on-Read (MoR)
In Merge-on-Read, mutations are appended as delete metadata, leaving original files unchanged.
*   **Write Amplification Factor (WAF):**
    $$\text{WAF}_{\text{MoR}} = \frac{\text{SizeOf}(F_{\text{del}})}{\text{SizeOf}(R_{\text{mut}})} \approx O(1)$$
*   **Read Amplification Factor (RAF):**
    $$\text{RAF}_{\text{MoR}} = \frac{\sum_{d \in D} \text{I/O}(d) + \sum_{f \in F_{\text{del}}} \text{I/O}(f)}{\sum_{d \in D} \text{I/O}(d)} > 1.0$$

### 💡 Concept Mapping & Plain English Explanations (WAF & RAF)
| Variable / Symbol | Concept Name | Plain English Translation |
| :---: | :--- | :--- |
| **$D_{\text{target}}$** | **Concept 14**: Mutation Target Files | The physical data files that currently contain the rows we are trying to update or delete. |
| **$R_{\text{mut}}$** | **Concept 15**: Mutated Rows | The set of rows we want to delete or replace. |
| **$R_{\text{total}}$** | **Concept 16**: Target File Row Count | The total number of rows stored in the targeted data files. |
| **$\text{WAF}_{\text{CoW}}$ / $\text{WAF}_{\text{MoR}}$** | **Concept 17**: Write Amplification Factor | The ratio of bytes physically written to disk versus logical bytes changed. Higher means more disk wear and slower writes. |
| **$\text{RAF}_{\text{CoW}}$ / $\text{RAF}_{\text{MoR}}$** | **Concept 18**: Read Amplification Factor | The ratio of bytes physically scanned versus logical data returned. Higher means slower queries and more cloud scan costs. |

#### 📊 Visual Example: Operational Data Flows
Let's see what happens step-by-step when you update a single 100-byte row in a 100 MB data file (which contains 1,000,000 rows):

```
=== Scenario: Update 1 Row (100 Bytes) inside 100 MB File ===

[Copy-on-Write Operational Flow]
1. Read 100 MB data file from Object Storage.
2. Search and locate the target record.
3. Modify the record in CPU memory.
4. Write a NEW 100 MB data file to Object Storage.
5. Swap file pointer in Metadata.
   Total Data Transferred: Read 100 MB + Write 100 MB = 200 MB
   WAF = 100,000,000 Bytes / 100 Bytes = 1,000,000x

[Merge-on-Read Operational Flow]
1. Locate target record's position (e.g. row index 500,000).
2. Write a positional delete file containing "row index 500,000" (size ~10 KB).
3. Swap metadata pointers.
   Total Data Transferred: Write 10 KB = 10,240 Bytes
   WAF = 10,240 Bytes / 100 Bytes = 102.4x
```

##### 📊 WAF vs. RAF Numerical Trade-off Matrix
| Metric | Copy-on-Write (CoW) | Merge-on-Read (MoR) | Visual Explanation |
| :--- | :--- | :--- | :--- |
| **Write Work (Bytes Written)** | **100,000,000 B** (100 MB) | **10,240 B** (10 KB) | CoW writes a whole new file; MoR only writes a tiny log. |
| **Write Amplification (WAF)** | **1,000,000** | **102.4** | CoW WAF is direct function of file size. MoR is near constant. |
| **Read Work (Bytes Read)** | **100,000,000 B** (100 MB) | **100,010,240 B** (100 MB + 10 KB) | MoR must load the original file *and* match all delete files. |
| **Read Amplification (RAF)** | **1.0** | **1.0001** | RAF grows for MoR as delete logs pile up over time. |

---

### 1.4 Physical Limits & Shannon Entropy Compaction of Deletion Vectors
To minimize transaction latency and throughput bounds dictated by the speed of light $c$ in fiber and network round-trip time (RTT), we must minimize the bits transferred.

A standard Positional Delete file (Parquet) stores deleted offsets as explicit 64-bit integers (`Int64`). If there are $M$ deleted offsets in a file of $N$ rows, the uncompressed space required to store these deletes is:
$$H_{\text{raw}} = M \times 64 \text{ bits}$$

A Deletion Vector stores these offsets as a **Roaring Bitmap**. Under Shannon's information theory, the minimum average number of bits required to represent an offset in a subset of size $M$ chosen from $N$ elements is bounded by the entropy:
$$H_{\text{opt}} \approx \log_2 \binom{N}{M}$$

Roaring Bitmaps pack these offsets into $2^{16}$ (65,536) row-chunk containers:
*   **Array Container**: If $M_{\text{chunk}} < 4096$, stores sorted 16-bit integers ($16 \text{ bits/offset}$).
*   **Bitmap Container**: If $M_{\text{chunk}} \ge 4096$, stores a bit array of $2^{16}$ bits ($65536 \text{ bits}$ total, yielding $\le 16 \text{ bits/offset}$).
*   **Run-Length Encoding (RLE) Container**: If deletions are contiguous (e.g., dropping a whole batch), stores pairs of `(start_offset, run_length)` as 16-bit integers ($32 \text{ bits/run}$).

### 💡 Concept Mapping & Plain English Explanations (Storage & Entropy)
| Variable / Symbol | Concept Name | Plain English Translation |
| :---: | :--- | :--- |
| **$N$** | **Concept 19**: Total Rows in File | The absolute physical row count or storage capacity of a single data file. |
| **$M$** | **Concept 20**: Total Deleted Rows in File | The exact number of rows in the data file that have been deleted. |
| **$H_{\text{raw}}$** | **Concept 21**: Raw Positional Cost | The total bits needed to write deletions if we store every single offset as a full 64-bit integer. |
| **$H_{\text{opt}}$** | **Concept 22**: Shannon Information Limit | The theoretical minimum bits required to record which rows are deleted, based on information theory. |

#### 📊 Visual Example: Storage Footprint of Delete Formats
Assume a Parquet data file contains **100,000 rows**. We delete **5,000 rows** under two different patterns.

##### Pattern A: Sparse Deletions (Random individual rows deleted throughout the file)
```
Target offsets to delete: [12, 85, 234, 1004, 1005, 54930, ...]
```
*   **Positional Delete File (Parquet Int64)**:
    $$5,000 \times 64 \text{ bits} = \mathbf{320,000 \text{ bits}} \approx 40 \text{ KB}$$
*   **Deletion Vector (Roaring Bitmap - Array Container)**:
    $$5,000 \times 16 \text{ bits} = \mathbf{80,000 \text{ bits}} \approx 10 \text{ KB}$$ *(Save 4x space)*

##### Pattern B: Bulk Deletions (Deletes occur in contiguous blocks, e.g., rows 10,000 to 14,999)
```
Target offsets to delete: [10000, 10001, 10002, ..., 14999] (1 contiguous run of 5,000 rows)
```
*   **Positional Delete File (Parquet Int64)**:
    $$5,000 \times 64 \text{ bits} = \mathbf{320,000 \text{ bits}} \approx 40 \text{ KB}$$
*   **Deletion Vector (Roaring Bitmap - RLE Container)**:
    Stores only the boundary: `(start=10000, run_length=5000)`.
    $$1 \text{ run} \times 32 \text{ bits} = \mathbf{32 \text{ bits}} \approx 4 \text{ Bytes}$$ *(Save 10,000x space!)*

##### 📊 Comparative Disk-Layout Mapping Table
| Delete Pattern | Storage Format | Physical disk layout | Size on Disk (Bits) |
|---|---|---|---|
| **Sparse** | Positional Parquet | `[0x0000000C, 0x00000055, 0x000000EA, ...]` (Array of 64-bit ints) | 320,000 |
| **Sparse** | Roaring Array | `[0x000C, 0x0055, 0x00EA, ...]` (Array of 16-bit ints) | 80,000 |
| **Dense** | Roaring Bitmap | `010000001000000...` (Bit position is offset index) | 65,536 |
| **Bulk** | Roaring RLE | `[0x2710 (Start), 0x1388 (Length)]` | 32 |

---

## 2. The Java Iceberg Engine: Under the Hood of `RowDelta`

To understand how PyIceberg must evolve, we must dissect the reference implementation of `RowDelta` within `/iceberg/` (Java).

### 2.1 The Commit Pipeline & `RowDelta` Core
In Java Iceberg, `RowDelta` extends `SnapshotProducer<RowDelta>` and is implemented by `BaseRowDelta`. It commits a logical delta of appended data files and added delete files in a single transaction.

```mermaid
sequenceDiagram
    participant Client
    participant BaseRowDelta
    participant SnapshotProducer
    participant ManifestGroup
    participant Catalog
    
    Client->>BaseRowDelta: addRows(DataFile)
    Client->>BaseRowDelta: addDeletes(DeleteFile)
    Client->>BaseRowDelta: validateFromSnapshot(snapshotId)
    Client->>BaseRowDelta: commit()
    
    BaseRowDelta->>SnapshotProducer: commit()
    activate SnapshotProducer
    SnapshotProducer->>SnapshotProducer: Tasks.foreach(retries)
    
    loop OCC Commit Attempt
        SnapshotProducer->>BaseRowDelta: apply(baseMetadata, parentSnapshot)
        activate BaseRowDelta
        BaseRowDelta->>BaseRowDelta: runValidations(parentSnapshot)
        Note over BaseRowDelta: 1. validateDataFilesExist()<br/>2. validateDeletedFiles()<br/>3. validateNoConflictingAppends()
        
        BaseRowDelta->>ManifestGroup: buildManifests()
        ManifestGroup-->>BaseRowDelta: ManifestList / ManifestFiles
        BaseRowDelta-->>SnapshotProducer: newSnapshotMetadata
        deactivate BaseRowDelta
        
        SnapshotProducer->>Catalog: compareAndSwap(expected, updated)
        alt CAS Success
            Catalog-->>SnapshotProducer: Success
        else CAS Fail
            Catalog-->>SnapshotProducer: CommitFailedException (Retry)
            SnapshotProducer->>SnapshotProducer: refreshTableMetadata()
        end
    end
    deactivate SnapshotProducer
    SnapshotProducer-->>Client: Committed Snapshot
```

---

### 2.2 Deep Dive: Concurrency Validation Rules
The core safety of the `RowDelta` commit path lies in the validation operations run inside `BaseRowDelta.runValidations()`. These validations prevent write anomalies under various isolation levels.

### 💡 Concept Mapping & Plain English Explanations (Concurrency Validations)
| Variable / Symbol | Concept Name | Plain English Translation |
| :---: | :--- | :--- |
| **$F_{p, \text{added}}$** | **Concept 23**: Intended New Positional Deletes | The new positional delete entries we are trying to write in our current transaction. |
| **$D_{\text{current}}$** | **Concept 24**: Active Catalog Data Files | The set of data files currently registered as active in the catalog metadata. |
| **$D_{\text{deleted}}$ / $F_{\text{deleted}}$** | **Concept 25**: Files We Are Deleting | Files our transaction is trying to remove from metadata (like during a compaction cleanup). |
| **$D_{\text{removed\_concurrently}}$ / $F_{\text{removed\_concurrently}}$** | **Concept 26**: Concurrently Deleted Files | Files deleted by other transactions that committed while our transaction was still running. |
| **$\text{FilesAdded}(S_{\text{start}} \to S_{\text{current}})$** | **Concept 27**: Concurrently Appended Files | Brand-new data files written to the catalog while our transaction was active. |
| **$D_{\text{conflict}}$** | **Concept 28**: Conflicting Append Scan | Files added concurrently that contain rows that should have been deleted by our transaction's filters. |
| **$V_{\text{added}}$** | **Concept 29**: Newly Written Deletion Vectors | The fresh deletion vector bitmaps we are trying to commit in our transaction. |

---

#### 1. `validateDataFilesExist`
*   **Mathematical Assert**:
    $$\forall f_p \in F_{p, \text{added}}, \text{TargetDataFile}(f_p) \in D_{\text{current}}$$

##### 📊 Visual Conflict Timeline
Suppose a user schedules a row delete on a file while another concurrent transaction drops the parent partition:

| Time Step | Transaction A (Your Update Task) | Transaction B (Concurrent Drop Partition) | Catalog State (Metadata) |
|---|---|---|---|
| **$t_1$** | Starts update on `file_A.parquet` at snapshot $S_1$ | | Active Files: `[file_A.parquet]` |
| **$t_2$** | Writes PosDelete targeting `file_A.parquet` | | Active Files: `[file_A.parquet]` |
| **$t_3$** | | Commits DROP partition (deletes `file_A`) | Active Files: `[]` (Snapshot $S_2$) |
| **$t_4$** | Attempts commit of RowDelta | | Current Catalog: $S_2$ |

##### Concurrency Assert Resolution:
*   $\text{TargetDataFile}(f_p) = \text{"file\_A.parquet"}$
*   Is $\text{"file\_A.parquet"} \in D_{\text{current}}$? (Evaluate: $\text{"file\_A.parquet"} \in \{\}$) $\to$ **False!**
*   **Result**: The transaction throws `ValidationException` and safely aborts. This prevents orphans or dangling delete references.

---

#### 2. `validateDeletedFiles`
*   **Mathematical Assert**:
    $$(D_{\text{deleted}} \cup F_{\text{deleted}}) \cap (D_{\text{removed\_concurrently}} \cup F_{\text{removed\_concurrently}}) = \emptyset$$

##### 📊 Visual Conflict Timeline
Two parallel processes attempt to delete the same data file:

| Time Step | Transaction A (Your File Compactor) | Transaction B (Concurrent User Session) | Catalog State (Metadata) |
|---|---|---|---|
| **$t_1$** | Reads `file_A.parquet` at snapshot $S_1$ | Reads `file_A.parquet` at snapshot $S_1$ | Active Files: `[file_A.parquet]` |
| **$t_2$** | Marks `file_A.parquet` as deleted | | Active Files: `[file_A.parquet]` |
| **$t_3$** | | Commits delete of `file_A.parquet` | Active Files: `[]` (Snapshot $S_2$) |
| **$t_4$** | Attempts to commit delete manifest | | Current Catalog: $S_2$ |

##### Concurrency Assert Resolution:
*   $D_{\text{deleted}} = \{\text{"file\_A.parquet"}\}$
*   $D_{\text{removed\_concurrently}} = \{\text{"file\_A.parquet"}\}$
*   Evaluate intersection: $\{\text{"file\_A.parquet"}\} \cap \{\text{"file\_A.parquet"}\} = \{\text{"file\_A.parquet"}\} \neq \emptyset$.
*   **Result**: The transaction throws `ValidationException` and aborts, preventing concurrent logical double-deletes.

---

#### 3. `validateNoConflictingAppends`
*   **Mathematical Assert (SERIALIZABLE)**:
    $$\text{FilesAdded}(S_{\text{start}} \to S_{\text{current}}) \cap D_{\text{conflict}} = \emptyset$$

##### 📊 Visual Conflict Timeline
An equality delete rule is registered while a concurrent process appends new matching rows:

| Time Step | Transaction A (Equality Delete Job) | Transaction B (Concurrent Row Appender) | Catalog State (Metadata) |
|---|---|---|---|
| **$t_1$** | Starts transaction at $S_1$. Scan query: `status = 'inactive'`. | | Active Files: `[file_A.parquet]` |
| **$t_2$** | Writes Equality Delete: `status = 'inactive'` | | Active Files: `[file_A.parquet]` |
| **$t_3$** | | Commits new row file: `file_B.parquet` | Active Files: `[file_A.parquet, file_B.parquet]` ($S_2$) |
| **$t_4$** | Attempts commit of delete rule | | Current Catalog: $S_2$ |

##### Concurrency Assert Resolution:
*   $\text{FilesAdded}(S_1 \to S_2) = \{\text{"file\_B.parquet"}\}$
*   $D_{\text{conflict}}$ (Files matching rule `status = 'inactive'`) = $\{\text{"file\_B.parquet"}\}$
*   Evaluate intersection: $\{\text{"file\_B.parquet"}\} \cap \{\text{"file\_B.parquet"}\} \to \mathbf{\{\text{"file\_B.parquet"}\}} \neq \emptyset$.
*   **Result**: The transaction aborts with `ValidationException`. This prevents transaction serializability failures where newly appended rows escape a concurrent delete sweep.

---

#### 4. `validateAddedDVs` (V3 Specific)
*   **Mathematical Assert**:
    $$\forall v \in V_{\text{added}}, \text{TargetDataFile}(v) \notin \{ d \in D_{\text{removed\_concurrently}} \}$$

##### 📊 Visual Conflict Timeline
A deletion vector is computed on a file that is concurrently rewritten by compaction:

| Time Step | Transaction A (Client Row Update) | Transaction B (Background Data Compactor) | Catalog State (Metadata) |
|---|---|---|---|
| **$t_1$** | Scans `file_A.parquet` at snapshot $S_1$ | Scans `file_A` and `file_B` at $S_1$ | Active: `[file_A, file_B]` |
| **$t_2$** | Writes Deletion Vector targeting `file_A.parquet` | | Active: `[file_A, file_B]` |
| **$t_3$** | | Compacts `file_A` & `file_B` $\to$ `compacted_AB.parquet` | Active: `[compacted_AB]` (Snapshot $S_2$) |
| **$t_4$** | Attempts commit of DV | | Current Catalog: $S_2$ |

##### Concurrency Assert Resolution:
*   $\text{TargetDataFile}(v) = \text{"file\_A.parquet"}$
*   $D_{\text{removed\_concurrently}} = \{\text{"file\_A.parquet"}, \text{"file\_B.parquet"}\}$
*   Evaluate assert: Is $\text{"file\_A.parquet"} \in \{\text{"file\_A.parquet"}, \text{"file\_B.parquet"}\}$? $\to$ **True!**
*   **Result**: The transaction throws `ValidationException` and aborts. This prevents applying bit offsets to a file that no longer exists in the active catalog schema.

---

## 3. Evolving PyIceberg Architecture: V2/V3 MoR & RowDelta Roadmap

PyIceberg is currently moving from a read-heavy library to an active write-path client. The implementation of Merge-on-Read writes is one of the most critical phases of this evolution.

### 3.1 Current Capabilities vs. Planned State
The following matrix shows the current state of PyIceberg regarding MoR:

| Spec Version | Operation Type | PyIceberg Status | Mechanism |
| :--- | :--- | :--- | :--- |
| **V2** | **Read Positional Deletes** | **Supported** | Parses Parquet/ORC delete schemas and filters PyArrow Tables. |
| **V2** | **Read Equality Deletes** | **Partial** | Initial support exists, but complex nested structures have active issues. |
| **V3** | **Read Deletion Vectors** | **Supported** | Decodes Puffin files and uses `pyroaring` bitmaps to mask Arrow Tables. |
| **V2** | **Write Positional Deletes** | **Unsupported** | Lacks `RowDelta` writer and positional delete writer. |
| **V3** | **Write Deletion Vectors** | **Unsupported** | `PuffinWriter` was prototyped in PR #2822 but closed as stale. |

---

### 3.2 Design Strategy for V2 Positional Delete Writes
To add support for writing V2 positional deletes, PyIceberg must introduce a `RowDelta` class extending `_SnapshotProducer` (mirroring the transaction architecture established in PR #3320).

#### Writing "One Positional Delete File per Data File"
In Java, a single positional delete file can span multiple data files. However, as noted by Fokko in Issue #1808:
> "With Java a lot of learnings have been learned, such as producing a positional delete file per data file. This greatly simplifies the implementation for PyIceberg."

##### 📊 Visual Example: File Alignment Strategy
Suppose we need to delete row index `5` in `file_A.parquet` and row index `12` in `file_B.parquet`.

```
[Option 1: Multi-File Target (Complex)]
A single delete file: delete-file-1.parquet
Inside the delete file:
┌──────────────────────┬──────┐
│      file_path       │ pos  │
├──────────────────────┼──────┤
│ s3://.../file_A.pq   │ 5    │
│ s3://.../file_B.pq   │ 12   │
└──────────────────────┴──────┘
* Crucial Requirement: The rows MUST be sorted globally by file_path, then by pos.
```

```
[Option 2: One Delete File per Data File (Recommended & Simple)]
Write two separate, small delete files:
1. delete-A.parquet:
   ┌──────────────────────┬──────┐
   │      file_path       │ pos  │
   ├──────────────────────┼──────┤
   │ s3://.../file_A.pq   │ 5    │
   └──────────────────────┴──────┘
2. delete-B.parquet:
   ┌──────────────────────┬──────┐
   │      file_path       │ pos  │
   ├──────────────────────┼──────┤
   │ s3://.../file_B.pq   │ 12   │
   └──────────────────────┴──────┘
```

Option 2 completely removes the sorting complexity across different files, making the Python execution pipeline extremely simple and less prone to memory overhead during parallel writes.

---

### 3.3 Design Strategy for V3 Deletion Vector Writes
To support V3 writes, PyIceberg must be able to serialize roaring bitmaps into Puffin files. This requires bringing back and completing the `PuffinWriter` from PR #2822.

```mermaid
graph LR
    Bitmaps["pyroaring.BitMap<br/>(Set of deleted offsets)"]
    --> Deser["_serialize_bitmap()"]
    --> Payload["Puffin Blob Payload"]
    --> Footer["Footer Metadata JSON<br/>(compression, offset, length)"]
    --> File["Puffin File Structure<br/>(PFA1 Magic + Blobs + Footer + Magic)"]
```

#### The Puffin File Specification (PFA1)
A Puffin file consists of the following byte sequence:
1.  **Magic Bytes (4 bytes)**: `PFA1` (short for Puffin Fratercula arctica, version 1).
2.  **Blob Payloads**: Serialized roaring bitmaps with the following inner format:
    *   `number_of_bitmaps` (8 bytes, little-endian).
    *   For each bitmap:
        *   `key` (4 bytes, little-endian unsigned integer). Represents the chunk ID.
        *   `serialized_roaring_bitmap` (variable length, using official Roaring format).
3.  **Footer Payload**: A JSON string describing the metadata of the blobs.
4.  **Footer Payload Size (4 bytes)**: Signed little-endian integer.
5.  **Flags (4 bytes)**: Bit 0 indicates if the footer is compressed.
6.  **Magic Bytes (4 bytes)**: `PFA1`.

##### 📊 Visual Example: Puffin File Byte Layout
Below is the physical byte layout of a Puffin file containing a single deletion vector blob targeting `file_A.parquet`:

```
┌────────────────────────────────────────────────────────────────────────┐
│ MAGIC: "PFA1" (4 Bytes)                                                │
├────────────────────────────────────────────────────────────────────────┤
│ BLOB PAYLOAD:                                                          │
│ - Num Bitmaps: 0x0000000000000001 (8 Bytes)                            │
│ - Chunk Key:   0x00000000 (4 Bytes)                                    │
│ - Bitmap:      [Raw Roaring Bitmap binary payload] (Variable Size)     │
├────────────────────────────────────────────────────────────────────────┤
│ FOOTER PAYLOAD:                                                        │
│ {                                                                      │
│   "blobs": [                                                           │
│     {                                                                  │
│       "type": "deletion-vector-v1",                                    │
│       "fields": [1],                                                   │
│       "snapshot-id": 839201948302,                                     │
│       "sequence-number": 5,                                            │
│       "offset": 4,                                                     │
│       "length": 512,                                                   │
│       "properties": {                                                  │
│         "referenced-data-file": "s3://bucket/table/file_A.parquet"    │
│       }                                                                │
│     }                                                                  │
│   ]                                                                    │
│ }                                                                      │
├────────────────────────────────────────────────────────────────────────┤
│ FOOTER SIZE: 0x000001F0 (4 Bytes little-endian, e.g. 496 Bytes)       │
├────────────────────────────────────────────────────────────────────────┤
│ FLAGS: 0x00000000 (4 Bytes, Uncompressed)                              │
├────────────────────────────────────────────────────────────────────────┤
│ MAGIC: "PFA1" (4 Bytes)                                                │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 4. PyIceberg Maintenance & Compaction Engine Design

Compaction is not merely a utility; it is the mathematical process of converting a table snapshot $S_{\text{input}}$ with high entropy and high RAF into a clean snapshot $S_{\text{output}}$ with low RAF, while preserving logical row identity.

$$\text{ActiveRows}(d, S_{\text{input}}) \equiv \text{ActiveRows}(d', S_{\text{output}})$$
$$\text{RAF}(S_{\text{output}}) \ll \text{RAF}(S_{\text{input}})$$

### 4.1 Compaction Types and Mathematical Modeling

```mermaid
graph TD
    subgraph Metadata Compaction
        M1[Manifest A] --> M_Merged[Merged Manifest]
        M2[Manifest B] --> M_Merged
    end
    
    subgraph Data Compaction
        D1[Small Data File A] --> D_Packed[Optimized Data File]
        D2[Small Data File B] --> D_Packed
    end
    
    subgraph MoR Compaction
        D3[Data File d] --> D_Clean[Clean Data File]
        Del_File[Delete File / DV] --> |Merge-on-Read application| D_Clean
    end
```

#### 1. Data Compaction (Bin-Packing)
Groups small, fragmented data files into optimal-sized files ($128\text{ MB} - 512\text{ MB}$).

##### 📊 Visual Example: Bin-Packing Allocations
Here is how 5 fragmented data files in a partition are packed into 3 optimized physical containers:

| Original Data File | File Size (MB) | Bin Assignment | Resulting Consolidated File | Final Size (MB) |
|---|---|---|---|---|
| `file_1.parquet` | 30 MB | Bin 2 | `compacted_B.parquet` | **120 MB** (Exact match) |
| `file_2.parquet` | 80 MB | Bin 3 | `compacted_C.parquet` | **100 MB** (Under limit) |
| `file_3.parquet` | 110 MB | Bin 1 | `compacted_A.parquet` | **110 MB** (Under limit) |
| `file_4.parquet` | 20 MB | Bin 3 | `compacted_C.parquet` | **100 MB** (Under limit) |
| `file_5.parquet` | 90 MB | Bin 2 | `compacted_B.parquet` | **120 MB** (Exact match) |

---

#### 2. Merge-on-Read Compaction (MoR to CoW)
Consolidates delete files (positional/equality/DVs) and applies them directly to their target data files, producing clean data files.
*   **Axiom**:
    $$d_{\text{new}} = \{ \text{Record}_d(i) \mid i \in [0, |d|-1] \land \Psi(d, i, S_{\text{input}}) = 0 \}$$

##### 📊 Visual Data Transformation Mapping Table
Let's see what happens to the rows physically during a MoR to CoW compaction step:

| Input File (`d1.parquet`) | Connected Delete File (`f_p1`) | Arrow Filter Inversion Mask | Output Compacted File (`d1_clean.parquet`) |
|---|---|---|---|
| `Offset 0: Alice` | | `True` | `Offset 0: Alice` |
| `Offset 1: Bob` | `pos = 1` | `False` | *[REMOVED]* |
| `Offset 2: Charlie` | | `True` | `Offset 1: Charlie` *(Index shifted!)* |
| `Offset 3: Diana` | | `True` | `Offset 2: Diana` *(Index shifted!)* |

---

### 4.2 Speed of Light Compaction in Pure Python (No GIL / JVM Overhead)
PyIceberg can execute these compactions far more efficiently than Java-based engines in local environments because it completely bypasses JVM memory management and GC sweeps. It leverages two architectural pillars:

#### 1. Zero-Copy I/O with PyArrow Dataset
Instead of materializing rows in Python object memory (which would cause massive CPU caching misses and GC pressure), PyIceberg streams data from S3/GCS directly into PyArrow’s C++ memory space.

```python
# Speed-of-Light MoR Compaction Pipeline using PyArrow
import pyarrow.dataset as ds
import pyarrow.compute as pc

def compact_mor_file(io: FileIO, data_file: DataFile, delete_files: list[DataFile]) -> pa.Table:
    # 1. Read deletion offsets using zero-copy Puffin/Parquet parser
    deleted_offsets = pa.chunked_array([])
    for df in delete_files:
        deletes = _read_deletes(io, df) # Returns dict of file_path -> ChunkedArray
        if data_file.file_path in deletes:
            deleted_offsets = deletes[data_file.file_path]
            
    # 2. Build Arrow Dataset scanner for the data file
    with io.new_input(data_file.file_path).open() as f:
        fragment = ds.ParquetFileFormat().make_fragment(f)
        table = ds.Scanner.from_fragment(fragment).to_table()
        
    # 3. Apply Deletion Vector via bitwise inversion in C++
    if len(deleted_offsets) > 0:
        total_rows = len(table)
        full_range = pa.array(range(total_rows))
        
        # Binary search / hash filter matching offsets
        active_mask = pc.invert(pc.is_in(full_range, value_set=deleted_offsets))
        
        # Zero-copy filter operation
        table = table.filter(active_mask)
        
    return table
```

---

## 5. Exhaustive PR Reviewer's Guide for MoR, RowDelta, and Compaction

When reviewing PRs touching these high-sensitivity paths, use this checklist to enforce rigorous software engineering standards and prevent data corruption.

### 5.1 Architectural and Spec Checkpoints
1.  **Spec Version Gating**:
    *   *Rule*: V3 features (Deletion Vectors, Puffin formats, `UnknownType` promotion) **must** actively check `table_metadata.format_version >= 3` and throw clean, actionable `ValidationError`s for V1/V2 tables.
    *   *Check*: Verify the PR does not write DVs to a V2 table or write positional deletes as V1 tables.
2.  **No Jackson Annotations**:
    *   *Rule*: Standard Apache Iceberg rule: *Never use Jackson annotations*. Custom `XxxParser.toJson`/`fromJson` only. JSON keys must use `kebab-case`. Optional fields must only be written when non-null.
    *   *Check*: Ensure no third-party JSON mappings are added to serialization models.
3.  **Strict Package-Private Defaults**:
    *   *Rule*: Keep classes and methods package-private (or internal with `_` prefix in Python) unless there is a clear public integration requirement.
    *   *Check*: Do not expose internal compaction heuristics or custom Puffin writer structures in public APIs.

### 5.2 Threading & Telemetry Safety
1.  **Container Reuse Safety**:
    *   *Rule*: When scanning or planning with parallel iterators, check if the code retains references to manifest entries.
    *   *Check*: Ensure `copyWithoutStats()` or structural copy/cloning is called before appending entries to lists in concurrent blocks.
2.  **Thread-Safe Metrics and Timers**:
    *   *Rule*: Telemetry, scan planning, and retry metrics must be thread-safe.
    *   *Check*: Verify that stats counters or progress bars updated during compaction use thread locks or atomic variables.

### 5.3 Concurrency & Retry Integrity
1.  **Validation Completeness**:
    *   *Rule*: A `RowDelta` commit **must** implement all four core validations (`validateDataFilesExist`, `validateDeletedFiles`, `validateNoConflictingAppends`, `validateAddedDVs` if V3).
    *   *Check*: Verify the PR does not omit `_validate_concurrency()` checks under the assumption that "conflicts are rare".
2.  **Data File Persistence Across Retries**:
    *   *Rule*: If a transaction CAS fails, the data files written during the transaction **must not** be deleted or rewritten on retry. Only the manifests must be regenerated.
    *   *Check*: Check `_refresh_for_retry()` implementation. Verify `_added_data_files` is preserved and not cleared.
3.  **Correct Isolation Level Enforcement**:
    *   *Rule*: Operations must respect the configured isolation levels: `write.delete.isolation-level` and `write.update.isolation-level`.
    *   *Check*: Ensure validations fail immediately with `ValidationException` under `SERIALIZABLE` when any concurrent append overlaps with deleted criteria.

---

## 6. Comprehensive Flow Diagrams

To provide an intuitive mental model, this section visualizes the key runtime flows for MoR, RowDelta, and Compaction.

### 6.1 Life of a `RowDelta` Commit (Retry Loop & Validation)
This diagram traces the execution of a `RowDelta` operation under OCC conflict pressure.

```mermaid
flowchart TD
    Start([1. Start RowDelta Transaction]) --> WritePayload[2. Write Data/Delete Files to Object Storage]
    WritePayload --> AttemptCAS[3. Attempt CAS Commit against Catalog]
    AttemptCAS -->|CAS Success| CommitSuccess([4. Transaction Committed!])
    
    AttemptCAS -->|CAS Fail: CommitFailedException| RefreshMeta[5. Refresh TableMetadata]
    RefreshMeta --> RunValidations{6. Run Concurrency Validations}
    
    RunValidations -->|Conflict Detected: e.g. Concurrent Delete| Abort([7. Abort Transaction with ValidationException])
    RunValidations -->|No Conflict| RegenManifests[8. Regenerate Manifests with Fresh Base]
    
    RegenManifests --> Backoff[9. Exponential Backoff with Jitter]
    Backoff --> AttemptCAS
```

### 6.2 The Read Path: Applying V2 Deletes vs. V3 Deletion Vectors
This diagram maps out how the scan engine resolves logical active records at runtime.

```mermaid
flowchart TD
    Scan([Start Table Scan]) --> ReadManifests[Read Manifests]
    ReadManifests --> FileType{File Type?}
    
    FileType -->|V2 Positional Delete| JoinPos[Join Data File & Parquet Delete File]
    JoinPos --> FilterPos[Filter records where pos matches file_path]
    FilterPos --> ReturnActive[Return Logical Active Table]
    
    FileType -->|V2 Equality Delete| BroadcastEq[Broadcast Equality Conditions]
    BroadcastEq --> FilterEq[Filter rows matching Equality Schema predicate]
    FilterEq --> ReturnActive
    
    FileType -->|V3 Deletion Vector| LoadPuffin[Load compressed roaring bitmap from Puffin]
    LoadPuffin --> ApplyMask[Apply Bitmask directly to PyArrow Arrow Table]
    ApplyMask --> ReturnActive
```

### 6.3 The Compaction Loop (MoR to CoW)
This diagram illustrates the lifecycle of a background compaction task converting Merge-on-Read files into optimized Copy-on-Write style files.

```mermaid
flowchart TD
    ScanMetadata[1. Scan Metadata: Identify high RAF partitions] --> SelectBins[2. Select data and delete files using Bin-Packing]
    SelectBins --> ReadArrow[3. Stream data and deletes into PyArrow Dataset]
    ReadArrow --> ApplyDeletes[4. Apply delete filters in Arrow C++]
    ApplyDeletes --> WriteParquet[5. Write new consolidated Data Files to Storage]
    
    WriteParquet --> StartRowDelta[6. Begin RewriteFiles Transaction]
    StartRowDelta --> CheckCAS[7. Attempt Catalog Swap]
    
    CheckCAS -->|Success| CleanOld[8. Expire Snapshots & Clean up old files via orphan retention]
    CheckCAS -->|Failure: Conflict| ValidateRewrite{9. Did concurrent transactions touch compacted files?}
    
    ValidateRewrite -->|Yes: Conflict| AbortCompaction([10. Abort Compaction safely])
    ValidateRewrite -->|No: Safe| Rebase[11. Rebase onto fresh metadata]
    Rebase --> CheckCAS
```

---

## 7. Mathematical and CS Review Checklist for Pull Requests

As a PR reviewer, you must hold code to the following proof-like criteria:

*   **Theorem of Non-Intersection**: Does this code guarantee that a concurrent write cannot write records that skip our deletes under `SERIALIZABLE` isolation?
    *   *Proof in code*: Look for the call to `_validate_no_new_deletes_for_data_files` and `_validate_no_new_delete_files`.
*   **Conservation of Files**: Does a failed commit attempt clean up its orphan files, or does it reuse them on retry?
    *   *Proof in code*: Verify that `_added_data_files` are preserved for reuse, and if the transaction is completely aborted (exhausted retries), a cleanup job is registered to prevent storage leaks.
*   **Spec-Version Invariant**: Does a write operation on a V1/V2 table throw a validation exception if it attempts to produce a Puffin file or Deletion Vector?
    *   *Proof in code*: Verify that the format version check is executed *before* initializing any `PuffinWriter` or V3 metadata updates.
*   **Memory Bound Invariant**: Does this compaction code read the entire Parquet file into Python list memory, or does it stream it using Arrow C++ batches?
    *   *Proof in code*: Reject any PR that uses `.to_pylist()` or loops over individual rows in Python. All filters and projections must happen inside PyArrow C++ or native libraries to guarantee $O(\text{BatchSize})$ memory consumption.

---

## 8. Coordinated Roadmap: PyIceberg Table Maintenance & MoR Proposal Alignment

This section bridges the theoretical bounds defined in this guide with your community strategic proposal draft at [Support_for_PyIceberg_Maintenance.md](file:///Users/jaredyu/Desktop/open_source/iceberg-notes/pyiceberg_maintenance/Support_for_PyIceberg_Maintenance.md). It maps out the PR dependency trees, spec compatibility logic, and the exact release phases required to land these changes without building architectural dead ends.

### 8.1 Visual Dependency Graph: The PyIceberg Maintenance & Write Roadmap
The high complexity of PyIceberg features stems from tight runtime dependencies. For example, **Data Compaction** (#3124) cannot safely commit without the metadata-only **REPLACE API** (#3131), which itself is vulnerable to data corruption unless **Commit Retry with Concurrency Validation** (#3320) is merged first.

The following graph visualizes the precise path of pull requests and issues:

```mermaid
graph TD
    %% Define Styles
    classDef merged fill:#d4edda,stroke:#28a745,stroke-width:2px,color:#155724;
    classDef open fill:#fff3cd,stroke:#ffc107,stroke-width:2px,color:#856404;
    classDef closed fill:#f8d7da,stroke:#dc3545,stroke-width:2px,color:#721c24;

    %% Phase 1: bedrock validation blocks
    subgraph P1 ["Phase 1: Transaction Bedrock"]
        PR_1935["PR #1935: validation_history<br/>(Merged)"]:::merged
        PR_1938["PR #1938: validateDeletedDataFiles<br/>(Merged)"]:::merged
        PR_2050["PR #2050: validateAddedDataFiles<br/>(Merged)"]:::merged
        PR_3049["PR #3049: validateNoNewDeleteFiles<br/>(Merged)"]:::merged
        PR_3320["PR #3320: Commit Retry / Conflict Val<br/>(Open / In-Progress)"]:::open
        
        PR_1935 --> PR_3320
        PR_1938 --> PR_3320
        PR_2050 --> PR_3320
        PR_3049 --> PR_3320
    end

    %% Phase 2: compaction
    subgraph P2 ["Phase 2: Compaction Bedrock"]
        PR_3131["PR #3131: REPLACE Snapshot API<br/>(Open / In-Progress)"]:::open
        PR_3124["PR #3124: Data File Compaction (CoW)<br/>(Open / In-Progress)"]:::open
        PR_3361["PR #3361: Delete Orphan Files<br/>(Open)"]:::open
        PR_1958["PR #1958: Delete Orphan Files<br/>(Closed / Reopen Candidate)"]:::closed
        
        PR_3320 --> PR_3131
        PR_3131 --> PR_3124
        PR_3124 --> PR_3361
        PR_3124 --> PR_1958
    end

    %% Phase 3: MoR Write path
    subgraph P3 ["Phase 3: Interoperable MoR Write Path"]
        PR_1516["PR #1516: DV Read Support (V3)<br/>(Merged)"]:::merged
        PR_3285["PR #3285: Equality Delete Conversion<br/>(Open)"]:::open
        PR_2822["PR #2822: DV Write Support (V3)<br/>(Closed / Resurrect Candidate)"]:::closed
        
        PR_3124 --> PR_3285
        PR_3285 --> PR_2822
    end

    %% Phase 4: Metadata Optimization
    subgraph P4 ["Phase 4: Manifest & Snapshot Cleanup"]
        PR_1880["PR #1880: Expire Snapshots<br/>(Merged)"]:::merged
        PR_2143["PR #2143: MaintenanceTable Refactor<br/>(Merged)"]:::merged
        PR_1661["PR #1661: RewriteManifests API<br/>(Closed / Resurrect Candidate)"]:::closed
        
        PR_1880 --> PR_2143
        PR_3124 --> PR_1661
    end
```

---

### 8.2 Execution Phases & Community Resource Allocation

To prevent community burnout and ensure each PR is worth the extensive review cycles required, we break down the roadmap into four incremental phases.

#### Phase 1: Transaction Bedrock (Bedrock Validation & Concurrency Safety)
*   **Target Objective**: Make PyIceberg a first-class citizen for transactional safety, enabling concurrent workers to read and write without causing silent data loss or dirty writes.
*   **Active PRs**:
    *   **PR #3320** (lawofcycles): *Commit retry with data conflict validation*.
*   **Strategic Value**: **Extremely High**. This is the single most critical gating PR for all transactional writes. It must land before PR #3131 or PR #3124 can be safely deployed in production. It implements the OCC retry loops modeled in Section 2.1.
*   **Reviewer Priority**: Block all other writing features and dedicate top maintainer reviews to land PR #3320.

#### Phase 2: CoW Compaction & Orphan Cleanup (Solving the Small File Problem)
*   **Target Objective**: Resolve the runaway storage bloat and the degradation of read queries (small file problem) in single-node Python workspaces.
*   **Active PRs**:
    *   **PR #3131** (qzyu999): *Feature: Add metadata-only replace API to Table for REPLACE snapshot operations*.
    *   **PR #3124** (qzyu999): *Support data files compaction*.
    *   **PR #3361 / PR #1958**: *Delete orphan files*.
*   **Strategic Value**: **High**. This introduces the CoW-style bin-packing compaction (modeled in Section 4.1). Landing this solves the single-node performance bottlenecks caused by heavy query fragmentation.
*   **Developer Alignment**: Reopen PR #1958 or actively review PR #3361 concurrently. Once PR #3124 merges, it will aggressively generate unreferenced "orphan" Parquet files; PyIceberg *must* have orphan file pruners ready to prevent catastrophic storage cost spikes.

#### Phase 3: Interoperable MoR Write Path & Deletion Vectors
*   **Target Objective**: Introduce high-performance Merge-on-Read capabilities without duplicate development overhead.
*   **Active PRs**:
    *   **PR #3285** (rambleraptor): *Equality Delete support and dynamic conversion*.
    *   **PR #2822** (rambleraptor): *Iceberg Deletion Vector Write Support* (Needs resurrection).
*   **The V2 vs. V3 Strategic Decision**:
    *   *Decision*: **Skip V2 Positional Delete Writes (#1808), but merge V2 Equality Delete Conversion (#3285) & V3 DV Writes (#2822)**.
    *   *Rationale*: Storing positional deletes in Parquet files ($M \times 64$ bits, Section 1.4) introduces redundant complexity. Skip V2 positional writes entirely to save developer time, and go straight to V3 Deletion Vectors (Roaring Bitmaps, Section 3.3).
    *   *Interop Exception*: We **must** support V2 Equality Delete reading and conversion (#3285) because upstream streaming frameworks (e.g. Apache Flink, Spark structured streaming) write V2 equality deletes. PyIceberg must be able to read these files and compact them into clean V3 Deletion Vectors to act as an effective compaction engine for mixed-language data lakes.

#### Phase 4: Manifest and Metadata Optimization
*   **Target Objective**: Compress manifest metadata layers to prevent catalog query degradation.
*   **Active PRs**:
    *   **PR #1661** (amitgilad3): *Support metadata compaction (RewriteManifests)* (Needs resurrection).
*   **Strategic Value**: **Medium**. Rewriting manifests scales down table metadata loading times when tables accumulate thousands of commits. It should be completed once Phase 2 (Data Compaction) is fully deployed.

---

### 8.3 Feature Interaction Matrix
Use this matrix to trace how individual PR goals interact with core table properties:

| Feature Area | PR | Direct Interaction / Pre-requisite | Core Constraint (from `AGENTS.md`) |
| :--- | :--- | :--- | :--- |
| **Commit Validation** | **PR #3320** | Bedrock for all updates. Pre-requisite for REPLACE. | Do not widening public method signatures; use package-private scopes by default. |
| **Data Compaction** | **PR #3124** | Requires **PR #3131** (REPLACE API) and **PR #3320** (Retry). | Ensure the zero-copy Arrow memory boundary is respected. Avoid `.to_pylist()`. |
| **Orphan Deletion** | **PR #3361** | Triggered immediately after compaction runs to clean unreferenced files. | Must not delete files that are currently active in the concurrent catalog snapshots. |
| **Equality Conversion**| **PR #3285** | Must read heavy dynamic predicates and serialize them to compact DVs. | Reject Jackson annotations; ensure kebab-case serialization keys in Open-API/REST. |
| **Deletion Vectors** | **PR #2822** | Requires stable `PuffinWriter` logic. | Check format version gating `table_metadata.format_version >= 3` before writing DVs. |
| **Manifest Rewrite** | **PR #1661** | Resolves metadata bloating after compaction changes table states. | Ensure parallel scans call `copyWithoutStats()` when building manifest lists. |

---

### 8.4 Reviewer's Cost-Benefit & ROI Checklist
Before approving or prioritizing a PR in the PyIceberg maintenance stream, evaluate its ROI:

```
[PR Review ROI Calculator]

1. Does the PR reduce Write Amplification (WAF)?
   - If yes (e.g. PR #2822 - DV Writes, WAF ~ O(1) instead of CoW WAF ~ 1,000,000x) -> High Priority.
   
2. Does the PR reduce Read Amplification (RAF)?
   - If yes (e.g. PR #3124 - Compaction, resets RAF to 1.0) -> High Priority.
   
3. Does it break cross-language catalog interoperability?
   - If yes (e.g. writing custom V3 features without format version gating) -> REJECT IMMEDIATELY.
   
4. Can single-node Python workers run it without OOM?
   - If no (e.g. loads full table in Python object memory instead of Arrow C++) -> REJECT. Require batch-streaming dataset APIs.
```

---

## 9. PyIceberg vs. Java Iceberg: Critical Maintenance Parity Gaps & Specifications

While basic operations (like bin-pack compaction and metadata-only snapshot expiration) have active Python proposals, several vital safety gates, planning heuristics, and optimization algorithms from Java Iceberg are omitted in current PyIceberg designs. This section identifies the five critical parity gaps that must be resolved to build a production-grade maintenance suite.

### 9.1 Gap 1: The GC-Enabled Safety Gate (`gc.enabled`)

In Java, `gc.enabled` (default `true`) is a table-level boolean property. If a catalog administrator sets `gc.enabled = false`, operations like `ExpireSnapshots` and `DeleteOrphanFiles` **must never** physically delete data or metadata files from physical object storage. 

If this safety gate is missing, PyIceberg could physically delete storage objects in shared environments where an external system manages history, leading to catastrophic, irreversible data loss.

#### 💡 Concept Mapping & Plain English Explanations
| Variable / Symbol | Concept Name | Plain English Translation |
| :---: | :--- | :--- |
| **`gc.enabled`** | **Concept 30**: Garbage Collection Enabled Gate | A master boolean switch in the table properties. If false, it blocks any physical file deletion from disk or S3, keeping storage completely untouched even if metadata is pruned. |

#### 📊 Visual Logic Flow: GC-Gate Verification
Below is the evaluation pipeline that the PyIceberg client must execute before issuing delete commands to `FileIO`:

```mermaid
flowchart TD
    StartCleanup([Start Physical File Cleanup]) --> FetchProps[Read Table Properties]
    FetchProps --> CheckGC{Is gc.enabled == true?}
    
    CheckGC -->|No: False| LogWarning[Log warning: Physical deletion bypassed]
    LogWarning --> CompleteBypass([Metadata-only action complete, physical files preserved])
    
    CheckGC -->|Yes: True / Default| RunDeletes[Scan unreferenced paths and issue FileIO.delete]
    RunDeletes --> CompletePhysical([Files physically removed from object storage])
```

---

### 9.2 Gap 2: Orphan File Grace Period (`olderThan`)

A naive orphan file pruner lists all physical files in the table's directory, cross-references them against active manifests, and deletes the unreferenced ones. 

However, because write transactions are distributed and asynchronous, active writers write Parquet files to S3 **before** committing their metadata changes to the catalog. If `delete_orphan_files` is executed concurrently with a write transaction, it will aggressively delete these uncommitted active files, corrupting the concurrent write path.

#### 💡 Concept Mapping & Plain English Explanations
| Variable / Symbol | Concept Name | Plain English Translation |
| :---: | :--- | :--- |
| **`olderThan`** | **Concept 31**: Orphan Grace Period | A time threshold (e.g. 24 hours). Files written more recently than this grace period are kept safe, protecting active writers that haven't finished committing their changes. |

#### 📊 Visual Timeline: The Writing-Compaction Race Condition
The timeline below illustrates how the absence of a grace period causes write failures:

```
[Timeline of a Corrupting Race Condition: No Grace Period]
t1: Client A starts transaction. Writes s3://bucket/part-0.parquet (Active, but uncommitted).
t2: Client B starts Orphan File Pruner. Scans S3 directory, finds part-0.parquet.
t3: Client B reads Table Metadata. part-0.parquet is NOT referenced yet.
t4: Client B deletes s3://bucket/part-0.parquet from S3.
t5: Client A attempts to commit catalog transaction referencing part-0.parquet.
t6: TRANSACTION FAILED / CORRUPT: Physical file is missing from storage!

[Timeline of a Safe Transaction: Enforced Grace Period (e.g., 24h)]
t1: Client A starts transaction. Writes s3://bucket/part-0.parquet (Mtime = 12:00:00).
t2: Client B starts Orphan File Pruner at 12:05:00.
t3: Pruner checks: Is part-0.parquet older than (12:05:00 - 24 hours)? -> No (only 5 mins old).
t4: Pruner skips deletion of part-0.parquet, keeping the active write safe.
t5: Client A successfully commits catalog transaction.
```

---

### 9.3 Gap 3: Advanced Compaction Strategies (Sort & Z-Order)

PyIceberg PR #3124 only provides a basic **BinPack Strategy**. Bin-packing consolidates files but does not alter the physical sorting layout of rows. 

In Java, the compaction actions expose **Sort** and **Z-Order** strategies. These sort data on disk, which drastically optimizes analytical query filters by grouping related data into tight, contiguous ranges. This allows query engines (like PyArrow, DuckDB, or Trino) to skip reading entire row groups via min/max dictionary index pruning.

#### 💡 Concept Mapping & Plain English Explanations
| Variable / Symbol | Concept Name | Plain English Translation |
| :---: | :--- | :--- |
| **$Z(\mathbf{x})$** | **Concept 32**: Z-Order Multidimensional Sort | A technique that maps multi-dimensional query keys (like `lat` and `lon`) into a single dimension by interleaving their bits. This keeps related data clustered together on disk, accelerating spatial and composite queries. |

#### 📊 Visual Clustering: BinPack vs. Z-Order Layouts
Assume a table contains two columns: `x` (integer range 0-3) and `y` (integer range 0-3), yielding a 2D data space. Let's see how they are laid out across 4 files:

##### 1. BinPack Layout (No sorting, arbitrary distribution)
```
File 1: [(0,3), (3,0), (1,2)]  --> Min/Max X: [0, 3], Min/Max Y: [0, 3] (Reads everything)
File 2: [(1,0), (2,3), (0,1)]  --> Min/Max X: [0, 2], Min/Max Y: [0, 3] (Reads everything)
File 3: [(2,1), (3,2), (0,0)]  --> Min/Max X: [0, 3], Min/Max Y: [0, 2] (Reads everything)
File 4: [(1,3), (2,0), (3,3)]  --> Min/Max X: [1, 3], Min/Max Y: [0, 3] (Reads everything)
```
*   **Result**: Every file's column boundaries cover the entire coordinate space. The engine must scan **100%** of files for any query filters (e.g. `x = 3`).

##### 2. Z-Order Layout (Spatially clustered via bit-interleaving)
```
File 1 (Quadrant 0): [(0,0), (0,1), (1,0), (1,1)] --> Min/Max X: [0, 1], Min/Max Y: [0, 1]
File 2 (Quadrant 1): [(2,0), (2,1), (3,0), (3,1)] --> Min/Max X: [2, 3], Min/Max Y: [0, 1]
File 3 (Quadrant 2): [(0,2), (0,3), (1,2), (1,3)] --> Min/Max X: [0, 1], Min/Max Y: [2, 3]
File 4 (Quadrant 3): [(2,2), (2,3), (3,2), (3,3)] --> Min/Max X: [2, 3], Min/Max Y: [2, 3]
```
*   **Result**: If a query filters for `x = 3 AND y = 0`, the engine checks the min/max ranges and scans **ONLY File 2** (Quadrant 1), completely bypassing Files 1, 3, and 4. This yields an instant **75% reduction** in storage I/O read volume!

---

### 9.4 Gap 4: RewriteManifests Sizing and Automatic Partition Clustering

Java's manifest compaction (`RewriteManifests`) aggregates tiny manifest files into a clean metadata layer. 

A critical gap in PyIceberg's current manifest-writing logic is the absence of size-based clustering and boundary validation. Manifest files must be sized to fit within `manifest.target-size-bytes` (default 8MB) and clustered by partition boundaries to speed up scan planning.

#### 💡 Concept Mapping & Plain English Explanations
| Variable / Symbol | Concept Name | Plain English Translation |
| :---: | :--- | :--- |
| **`manifest.target-size-bytes`** | **Concept 33**: Manifest Target Size | The ideal maximum byte volume for a manifest file. Keeping manifests around this size ensures rapid scan planning without causing high overhead during reads. |

---

### 9.5 Gap 5: Actions / `MaintenanceTable` API Signature Parity

To ensure cross-language developers can easily navigate PyIceberg, the Python client must align its entry point signatures with Java's fluent builder APIs. 

The existing `MaintenanceTable` in `pyiceberg/table/maintenance.py` is extremely bare, exposing only `expire_snapshots()`. Below is the complete, production-grade API signature specification that must be implemented under `MaintenanceTable` to achieve functional parity:

```python
class MaintenanceTable:
    """Production-grade PyIceberg Maintenance Table API.
    
    Exposes the complete table maintenance suite with strict safety gates.
    """
    _table: Table

    def __init__(self, table: Table) -> None:
        self._table = table

    def expire_snapshots(self) -> ExpireSnapshots:
        """Prunes historical snapshots.
        
        Note: The underlying implementation must respect table property `gc.enabled`.
        If gc.enabled is False, physical deletions are bypassed.
        """
        from pyiceberg.table.update.snapshot import ExpireSnapshots
        from pyiceberg.table import Transaction
        return ExpireSnapshots(transaction=Transaction(self._table, autocommit=True))

    def delete_orphan_files(self) -> DeleteOrphanFilesBuilder:
        """Return a builder to discover and remove physical files left behind.
        
        Signatures:
            .older_than(days=3) -> Configure grace period to prevent deleting active writes.
            .location(path)    -> Restrict cleaning to a specific directory.
            .execute()         -> Run safety checks (gc.enabled check) and clean files.
        """
        return DeleteOrphanFilesBuilder(self._table)

    def rewrite_data_files(self) -> RewriteDataFilesBuilder:
        """Return a builder to compact small fragmented data files into larger files.
        
        Signatures:
            .strategy("binpack" | "sort" | "z-order") -> Configure row layout.
            .sort_by(columns: list[str])              -> Specify keys for sorting.
            .target_file_size_bytes(bytes: int)       -> Control written block sizes.
            .execute()                                -> Run PyArrow C++ Dataset compaction.
        """
        return RewriteDataFilesBuilder(self._table)

    def rewrite_manifests(self) -> RewriteManifestsBuilder:
        """Return a builder to consolidate tiny manifest files in the metadata layer.
        
        Signatures:
            .target_manifest_size_bytes(bytes: int)   -> Target size for manifests.
            .execute()                                -> Cluster and rewrite metadata.
        """
        return RewriteManifestsBuilder(self._table)
```

