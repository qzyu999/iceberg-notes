/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

document.addEventListener("DOMContentLoaded", () => {
    lucide.createIcons();
    initCookbook();
    setupSearch();
});

const cookbookSections = {
    "math-model": `
        <div style="margin-bottom: 24px;">
            <h3 style="font-family: var(--serif); font-size: 20px; font-weight: 500; color: var(--slate); margin-bottom: 6px;">1. The Mathematical Table State Machine Model</h3>
            <p style="font-size: 13px; color: var(--g500);">Rigorous formal definitions mapping schemas, specifications, snapshots, and chronological state timelines.</p>
        </div>
        
        <h4 style="font-family: var(--serif); font-size: 14.5px; font-weight: 600; margin-bottom: 10px; color: var(--slate);">1.1 Structural Definitions</h4>
        <p style="font-size: 13px; color: var(--g700); margin-bottom: 16px;">Let $\\mathcal{U}_{\\text{URI}}$ be the universe of valid storage paths, and $\\mathcal{U}_{\\text{ID}} \\subset \\mathbb{N}$ be the universe of unique Field Identifiers.</p>
        
        <div class="theorem-box">
            <div class="theorem-title">Definition 1: Table Schema</div>
            <div class="theorem-statement">
                A table schema $\\Sigma$ is a static mapping linking stable field identifiers to structural data types:
                $$\\Sigma \\subset \\{ (id, name, type, required) \\mid id \\in \\mathcal{U}_{\\text{ID}}, name \\in \\text{String}, type \\in \\text{Type}, required \\in \\mathbb{B} \\}$$
            </div>
            <div class="proof-section">
                <div class="proof-title">Axiomatic Rule</div>
                <div class="proof-step">Columns are resolved exclusively by stable <code>Field ID</code> rather than names or offsets. This ensures structural evolution operations (renames, drops, promotions) commute instantly without physical writes.</div>
            </div>
        </div>

        <div class="theorem-box">
            <div class="theorem-title">Definition 2: Partition Specification</div>
            <div class="theorem-statement">
                A partition specification $P$ of schema $\\Sigma$ is a set of deterministic transform fields:
                $$P = \\{ (source\\_id, field\\_id, \\tau, name) \\}$$
            </div>
            <div class="proof-section">
                <div class="proof-title">Axiomatic Rule</div>
                <div class="proof-step">where $\\tau: \\text{Type}(source\\_id) \\to \\text{PartitionType}$ is a pure transform mapping, guaranteeing that physical layouts are abstract functions of the logical dataset.</div>
            </div>
        </div>

        <div class="theorem-box">
            <div class="theorem-title">Definition 3: Physical File Reference</div>
            <div class="theorem-statement">
                A physical file $f$ committed to active storage is represented as a structured 5-tuple:
                $$f = (path, type, spec\\_id, partition, stats)$$
            </div>
        </div>

        <div class="theorem-box">
            <div class="theorem-title">Definition 4: Active Commit Snapshot</div>
            <div class="theorem-statement">
                A snapshot $S_k$ representing commit checkpoint version $k$ tracks the complete state of physical files:
                $$S_k = (D_k, F_{e, k}, F_{p, k}, V_{d, k})$$
            </div>
            <div class="proof-section">
                <div class="proof-title">Tuple Components</div>
                <div class="proof-step">$D_k$ = Active Data Files, $F_{e, k}$ = Equality Deletes, $F_{p, k}$ = Positional Deletes, $V_{d, k}$ = Deletion Vector Blobs.</div>
            </div>
        </div>

        <h4 style="font-family: var(--serif); font-size: 14.5px; font-weight: 600; margin-top: 24px; margin-bottom: 10px; color: var(--slate);">1.2 Core Execution Axioms</h4>
        <div class="theorem-box" style="border-left-color: var(--olive);">
            <div class="theorem-title">Axiom 1: Physical File Immutability</div>
            <div class="theorem-statement">
                Once a file $f$ is written to physical storage and committed, its data blocks are completely immutable:
                $$\\forall f \\in S_i, \\quad \\text{Content}(f.\\text{path}, t) = \\text{Content}(f.\\text{path}, t + \\delta)$$
            </div>
        </div>

        <div class="theorem-box" style="border-left-color: var(--olive);">
            <div class="theorem-title">Axiom 2: Monotonic Sequence Progress</div>
            <div class="theorem-statement">
                The global logical clock (sequence number $s$) increases strictly monotonically across consecutive commit snapshots:
                $$\\forall S_i, S_j \\in T, \\quad i < j \\implies \\text{Seq}(S_i) < \\text{Seq}(S_j)$$
            </div>
        </div>
    `,
    "read-path": `
        <div style="margin-bottom: 24px;">
            <h3 style="font-family: var(--serif); font-size: 20px; font-weight: 500; color: var(--slate); margin-bottom: 6px;">2. Merge-on-Read (MoR) Logical Projections</h3>
            <p style="font-size: 13px; color: var(--g500);">Mathematical proofs verifying that read-path merge operations yield correct logical states without physical write steps.</p>
        </div>

        <h4 style="font-family: var(--serif); font-size: 14.5px; font-weight: 600; margin-bottom: 10px; color: var(--slate);">2.1 Row-Level Logical Exclusions</h4>
        <p style="font-size: 13px; color: var(--g700); margin-bottom: 16px;">Let $d \\in D_k$ be a physical data file with a physical index space $I_d = [0, |d| - 1] \\subset \\mathbb{N}_0$. The active logical rows under snapshot $S_k$ are evaluated dynamically:</p>
        
        <div class="theorem-box">
            <div class="theorem-title">Definition: Active Logical Rows</div>
            <div class="theorem-statement">
                $$\\text{ActiveRows}(d, S_k) = \\{ \\text{Record}_d(i) \\mid i \\in I_d \\land \\Psi(d, i, S_k) = 0 \\}$$
            </div>
        </div>

        <div class="theorem-box">
            <div class="theorem-title">Definition: Composite Delete Mask</div>
            <div class="theorem-statement">
                The indicator function $\\Psi(d, i, S_k) \\in \\{0, 1\\}$ asserts whether a row offset $i$ is logically deleted:
                $$\\Psi(d, i, S_k) = \\mathbb{I} \\left[ \\Psi_{\\text{pos}}(d, i, S_k) = 1 \\lor \\Psi_{\\text{dv}}(d, i, S_k) = 1 \\lor \\Psi_{\\text{eq}}(d, i, S_k) = 1 \\right]$$
            </div>
            <div class="proof-section">
                <div class="proof-title">Sequence-Gated Positional Exclusions</div>
                <div class="proof-step">
                    $$\\Psi_{\\text{pos}}(d, i, S_k) = \\mathbb{I} \\left[ \\exists f_p \\in F_{p, k} \\text{ s.t. } f_p.\\text{path} = d.\\text{path} \\land f_p.\\text{pos} = i \\land S_{\\text{data}}(d) < S_{\\text{delete}}(f_p) \\right]$$
                </div>
            </div>
        </div>

        <div class="theorem-box" style="border-left-color: var(--olive);">
            <div class="theorem-title">Theorem 1: Merge-on-Read Correctness</div>
            <div class="theorem-statement">
                The lazy read-path projection $\\text{ActiveRows}(d, S_k)$ evaluates to the identical logical state as an eager physical Copy-on-Write write operation committed at snapshot $S_k$:
                $$\\text{ActiveRows}(d, S_k) \\equiv \\text{CoW}(d, S_k)$$
            </div>
            <div class="proof-section">
                <div class="proof-title">Mathematical Proof</div>
                <div class="proof-step">
                    <strong>1. Induction Base:</strong> Under Axiom 1 (Immutability), target data records inside $d$ cannot change their physical offsets.
                </div>
                <div class="proof-step">
                    <strong>2. Equivalence of Filter:</strong> Copy-on-Write executes a physical rewrite, excluding records by executing $\\Psi(d, i, S_k)$ during compilation.
                </div>
                <div class="proof-step">
                    <strong>3. Conclusion:</strong> Since both operators apply the identical mathematical indicator rules $\\Psi$, the resulting records active on read match written rows exactly: $\\text{ActiveRows}(d, S_k) \\equiv \\text{CoW}(d, S_k)$. $\\blacksquare$
                </div>
            </div>
        </div>
    `,
    "concurrency": `
        <div style="margin-bottom: 24px;">
            <h3 style="font-family: var(--serif); font-size: 20px; font-weight: 500; color: var(--slate); margin-bottom: 6px;">3. Concurrency Control & Strict Serializability</h3>
            <p style="font-size: 13px; color: var(--g500);">Verifying isolation, transaction validation checks, and serializable catalog commit operations.</p>
        </div>

        <h4 style="font-family: var(--serif); font-size: 14.5px; font-weight: 600; margin-bottom: 10px; color: var(--slate);">3.1 Commit Safety Invariants</h4>
        <p style="font-size: 13px; color: var(--g700); margin-bottom: 16px;">Let transaction $T_{\\text{commit}}$ propose updates based on base snapshot $S_{\\text{base}}$. If a concurrent transaction commits first (forcing $S_{\\text{current}} \\neq S_{\\text{base}}$), validation gates must evaluate:</p>

        <div class="theorem-box">
            <div class="theorem-title font-code">Invariant 1: Existence Integrity</div>
            <div class="theorem-statement">
                Asserts that delete files reference active paths in the current catalog timeline, avoiding orphaned deletes:
                $$\\forall f \\in F_{\\text{add}\\_del}, \\quad f.\\text{target}\\_path \\in \\{ d.\\text{path} \\mid d \\in D_{\\text{current}} \\}$$
            </div>
        </div>

        <div class="theorem-box">
            <div class="theorem-title font-code">Invariant 2: No Double Delete Conflict</div>
            <div class="theorem-statement">
                Ensures concurrent transactions do not delete the identical subset of physical data files:
                $$D_{\\text{del}} \\cap ( D_{\\text{base}} \\setminus D_{\\text{current}} ) = \\emptyset$$
            </div>
        </div>

        <div class="theorem-box">
            <div class="theorem-title font-code">Invariant 3: Predicate Isolation Guard</div>
            <div class="theorem-statement">
                Guarantees concurrent data appends do not match the deletion predicate rules $\\Phi$:
                $$\\forall d \\in (D_{\\text{current}} \\setminus D_{\\text{base}}), \\quad \\text{MatchesPredicate}(d.\\text{stats}, \\Phi) = \\emptyset$$
            </div>
        </div>

        <div class="theorem-box" style="border-left-color: var(--olive);">
            <div class="theorem-title">Theorem 2: Strict OCC Serializability</div>
            <div class="theorem-statement">
                If commit safety invariants evaluate to true at the instant of atomic catalog pointer swap, the execution of transaction $\\Delta$ is strictly serializable with respect to $S_{\\text{current}}$.
            </div>
            <div class="proof-section">
                <div class="proof-title">Mathematical Proof</div>
                <div class="proof-step">
                    <strong>1. Operation Commutativity:</strong> By Invariant 1, target data paths remain active. By Invariant 2, the sets of deleted files are disjoint.
                </div>
                <div class="proof-step">
                    <strong>2. Integrity of Predicate:</strong> By Invariant 3, no newly committed data row falls within the scope of the concurrent delete filter.
                </div>
                <div class="proof-step">
                    <strong>3. Conclusion:</strong> The concurrent executions resolve as logically independent, proving linearizable transaction ordering. $\\blacksquare$
                </div>
            </div>
        </div>
    `,
    "compaction-theory": `
        <div style="margin-bottom: 24px;">
            <h3 style="font-family: var(--serif); font-size: 20px; font-weight: 500; color: var(--slate); margin-bottom: 6px;">4. Compaction Safety & Storage GC Gates</h3>
            <p style="font-size: 13px; color: var(--g500);">Rigorous safety parameters gating physical storage reclamation and table compaction sweep operations.</p>
        </div>

        <h4 style="font-family: var(--serif); font-size: 14.5px; font-weight: 600; margin-bottom: 10px; color: var(--slate);">4.1 Logical Compaction Equivalence</h4>
        <p style="font-size: 13px; color: var(--g700); margin-bottom: 16px;">Compaction consolidates small fragmented files $D_{\\text{frag}}$ into large layout blocks $D_{\\text{new}}$: $\\Gamma_{\\text{compact}}(D_{\\text{frag}}) \\to D_{\\text{new}}$. We assert logical equivalence:</p>

        <div class="theorem-box">
            <div class="theorem-title">Compaction Preservation Identity</div>
            <div class="theorem-statement">
                $$\\bigcup_{d \\in D_{\\text{frag}}} \\text{ActiveRows}(d, S_k) \\equiv \\bigcup_{d' \\in D_{\\text{new}}} \\text{ActiveRows}(d', S_k)$$
            </div>
        </div>

        <div class="theorem-box" style="border-left-color: var(--olive);">
            <div class="theorem-title">Theorem 3: Compaction Safety</div>
            <div class="theorem-statement">
                Applying layout compaction $\\Gamma_{\\text{compact}}$ does not change the logical row projection of the table for any future snapshot $S_j \\ge S_{k+1}$.
            </div>
            <div class="proof-section">
                <div class="proof-title">Mathematical Proof</div>
                <div class="proof-step">
                    <strong>1. Pre-Commit Exclusions:</strong> Since active row-level deletes are eagerly resolved and merged into $D_{\\text{new}}$, all historical deletes are accounted for.
                </div>
                <div class="proof-step">
                    <strong>2. Post-Commit Gating:</strong> Under Axiom 2, any concurrent deletes staging at sequence number $s_d \\ge s_{k+1}$ target the new file set $D_{\\text{new}}$, preserving logical identity. $\\blacksquare$
                </div>
            </div>
        </div>

        <h4 style="font-family: var(--serif); font-size: 14.5px; font-weight: 600; margin-top: 24px; margin-bottom: 10px; color: var(--slate);">4.2 Safe Garbage Collection Constraints</h4>
        <div class="theorem-box" style="border-left-color: var(--olive);">
            <div class="theorem-title">Theorem 4: Garbage Collection Isolation Guard</div>
            <div class="theorem-statement">
                Physical object store deletions are structurally isolated from active concurrent transactions if gated by metadata locks and an age check exceeding the grace period:
                $$\\forall f \\in F_{\\text{delete}}, \\quad \\text{Now}() - \\text{MTime}(f) > \\Delta t_{\\text{grace}}$$
            </div>
            <div class="proof-section">
                <div class="proof-title">Mathematical Proof</div>
                <div class="proof-step">
                    <strong>1. Unreachable Assert:</strong> Any file older than $\\Delta t_{\\text{grace}}$ that is not referenced in active table metadata is unreachable by the catalog pointer.
                </div>
                <div class="proof-step">
                    <strong>2. Grace Period Guard:</strong> Files currently being staged by active concurrent writers fall inside the $\\Delta t_{\\text{grace}}$ window and are strictly skipped, preventing active data corruption. $\\blacksquare$
                </div>
            </div>
        </div>
    `
};

function initCookbook() {
    renderCookbookSection("math-model");

    // Hook up sub-navigation buttons
    document.querySelectorAll(".cookbook-nav-item").forEach(btn => {
        btn.addEventListener("click", () => {
            document.querySelectorAll(".cookbook-nav-item").forEach(b => b.classList.remove("active"));
            btn.classList.add("active");
            renderCookbookSection(btn.getAttribute("data-section"));
        });
    });
}

function renderCookbookSection(sectionId) {
    const contentDisplay = document.getElementById("cookbook-content-display");
    contentDisplay.innerHTML = cookbookSections[sectionId];
    
    // Trigger KaTeX rendering for the section content
    renderMathInElement(contentDisplay, {
        delimiters: [
            {left: "$$", right: "$$", display: true},
            {left: "$", right: "$", display: false}
        ]
    });
    
    // Re-trigger Lucide icon rendering
    lucide.createIcons();
}

function setupSearch() {
    const searchInput = document.getElementById("global-search");
    if (!searchInput) return;
    
    searchInput.addEventListener("input", (e) => {
        const query = e.target.value.toLowerCase().trim();
        if (!query) {
            document.querySelectorAll(".cookbook-nav-item").forEach(btn => btn.style.opacity = "");
            return;
        }

        document.querySelectorAll(".cookbook-nav-item").forEach(btn => {
            const sectionId = btn.getAttribute("data-section");
            const textContent = cookbookSections[sectionId].toLowerCase();
            const titleText = btn.innerText.toLowerCase();
            
            if (textContent.includes(query) || titleText.includes(query)) {
                btn.style.opacity = "1";
            } else {
                btn.style.opacity = "0.4";
            }
        });
    });
}
