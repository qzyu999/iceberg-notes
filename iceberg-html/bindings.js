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
    initRoadmap();
    initDependenciesTable();
    setupSearch();
});

const roadmapData = [
    {
        phase: "Phase 1: Submitting First PR",
        desc: "Contribute REST catalog HTTP mapping fixes (delivering 409 Conflict handling for atomic collisions, PR #3320).",
        id: "phase_first_pr"
    },
    {
        phase: "Phase 2: Porting Custom Serializers",
        desc: "Extract package-private serialization classes, removing Jackson reflection annotations from critical code paths.",
        id: "phase_serialization"
    },
    {
        phase: "Phase 3: Formal Proofs Integration",
        desc: "Author rigorous database serializability proofs and safety validation logic in mathematical cookbooks.",
        id: "phase_proofs"
    },
    {
        phase: "Phase 4: Concurrency Auditor Integration",
        desc: "Implement optimistic concurrency control retry logic and collision validation tests in PyIceberg core.",
        id: "phase_auditor"
    },
    {
        phase: "Phase 5: Performance Amplification Advisor",
        desc: "Deliver capacity-planning sliders and heap allocation heuristics for Arrow memory buffers.",
        id: "phase_advisor"
    },
    {
        phase: "Phase 6: Table Specification V3 Proposal",
        desc: "Draft and implement roaring deletion vector buffers in Puffin files under standard specifications.",
        id: "phase_spec_v3"
    },
    {
        phase: "Phase 7: Elected Committer & PMC member!",
        desc: "Recognized for specifications compliance, deep architectural reviews, and absolute technical rigor!",
        id: "phase_pmc_member"
    }
];

const dependencyData = [
    {
        upstream: "Phase 1: First PR REST",
        primitive: "<code>Catalog.commitTable()</code> HTTP status handlers",
        downstream: "Phase 4: Concurrency Auditor",
        requirement: "Prerequisite for transaction retries. Catalog locks depend on accurate 409 Conflict states to spin-wait safely."
    },
    {
        upstream: "Phase 2: Custom Serializers",
        primitive: "<code>TableMetadataParser.toJson/fromJson</code> custom builders",
        downstream: "Phase 6: Spec V3 Projections",
        requirement: "Jackson dependencies must be fully removed from metadata parsing APIs to guarantee cross-language REST serialization Parity."
    },
    {
        upstream: "Phase 3: Formal Proofs",
        primitive: "Logical sequence clock correctness proof ($S_{data} < S_{delete}$)",
        downstream: "Phase 4: Auditor validations",
        requirement: "Provides the algebraic foundations for concurrency checking. Informs implementation of <code>validateNoConflictingAppends()</code>."
    },
    {
        upstream: "Phase 4: Concurrency Auditor",
        primitive: "<code>validateDeletedFiles()</code> & <code>validateDataFilesExist()</code> check gates",
        downstream: "Phase 5: Performance Advisor",
        requirement: "OCC checks must run on the critical path. Advisor uses OCC metadata to project compaction write-amplification rates."
    },
    {
        upstream: "Phase 6: V3 Spec Proposal",
        primitive: "Puffin file structure & Roaring Bitmaps delete vectors",
        downstream: "Phase 7: PMC Member Status",
        requirement: "Design contribution defining standard file buffers in upstream specs, accepted and merged by Apache PMC."
    }
];

function initRoadmap() {
    const container = document.getElementById("roadmap-track-container");
    container.innerHTML = "";

    // Load completed states from localStorage
    const completedPhases = JSON.parse(localStorage.getItem("iceberg_completed_phases") || "{}");

    roadmapData.forEach((step, index) => {
        const isCompleted = !!completedPhases[step.id];

        const node = document.createElement("div");
        node.className = `roadmap-node ${isCompleted ? 'completed' : ''}`;
        node.id = `node-${step.id}`;

        node.innerHTML = `
            <div class="roadmap-dot"></div>
            <div class="roadmap-content">
                <div style="display: flex; justify-content: space-between; align-items: flex-start; gap: 12px;">
                    <div>
                        <h4 style="margin: 0; font-size: 13px; font-weight: 600; color: var(--slate);">${step.phase}</h4>
                        <p style="margin: 4px 0 0; font-size: 11px; color: var(--g500); line-height: 1.4;">${step.desc}</p>
                    </div>
                    <input type="checkbox" class="phase-checkbox" data-id="${step.id}" ${isCompleted ? 'checked' : ''} 
                           style="accent-color: var(--olive); cursor: pointer; width: 14px; height: 14px; margin-top: 2px;">
                </div>
            </div>
        `;
        container.appendChild(node);
    });

    // Add event listeners to checkboxes
    document.querySelectorAll(".phase-checkbox").forEach(chk => {
        chk.addEventListener("change", (e) => {
            const phaseId = e.target.getAttribute("data-id");
            const checked = e.target.checked;
            
            // Toggle completed class on node parent
            const nodeParent = document.getElementById(`node-${phaseId}`);
            if (checked) {
                nodeParent.classList.add("completed");
            } else {
                nodeParent.classList.remove("completed");
            }

            // Save to localStorage
            const completed = JSON.parse(localStorage.getItem("iceberg_completed_phases") || "{}");
            if (checked) {
                completed[phaseId] = true;
            } else {
                delete completed[phaseId];
            }
            localStorage.setItem("iceberg_completed_phases", JSON.stringify(completed));

            updateProgress();
        });
    });

    updateProgress();
}

function updateProgress() {
    const completedPhases = JSON.parse(localStorage.getItem("iceberg_completed_phases") || "{}");
    const count = Object.keys(completedPhases).length;
    const total = roadmapData.length;
    const percent = Math.round((count / total) * 100);

    // Update progress bar width and label
    const progressBar = document.getElementById("roadmap-progress-bar");
    const progressLabel = document.getElementById("roadmap-progress-label");
    const rankLabel = document.getElementById("roadmap-rank-label");

    if (progressBar) progressBar.style.width = `${percent}%`;
    if (progressLabel) progressLabel.innerText = `Progress: ${percent}% (${count} of ${total} phases)`;

    // Calculate Rank
    let rank = "Contributor";
    if (count === 0) {
        rank = "Contributor";
    } else if (count <= 2) {
        rank = "Active Contributor";
    } else if (count <= 4) {
        rank = "Spec Reviewer";
    } else if (count <= 6) {
        rank = "Release Manager";
    } else {
        rank = "Committer & PMC Member! 🌟";
    }

    if (rankLabel) {
        rankLabel.innerText = `Rank: ${rank}`;
        if (count === total) {
            rankLabel.style.color = "var(--clay)";
        } else {
            rankLabel.style.color = "var(--olive)";
        }
    }
}

function initDependenciesTable() {
    const tableBody = document.getElementById("roadmap-dependencies-body");
    if (!tableBody) return;
    tableBody.innerHTML = "";

    dependencyData.forEach(item => {
        const row = document.createElement("tr");
        row.innerHTML = `
            <td class="font-outfit font-semibold" style="color: var(--clay); font-size: 11px; padding: 8px 10px;">${item.upstream}</td>
            <td class="font-code text-xs" style="padding: 8px 10px;">${item.primitive}</td>
            <td class="font-outfit text-xs text-purple" style="font-weight: 500; padding: 8px 10px;">${item.downstream}</td>
            <td class="text-secondary text-xs" style="line-height: 1.4; padding: 8px 10px;">${item.requirement}</td>
        `;
        tableBody.appendChild(row);
    });
}

function setupSearch() {
    const searchInput = document.getElementById("global-search");
    if (!searchInput) return;
    
    searchInput.addEventListener("input", (e) => {
        const query = e.target.value.toLowerCase().trim();
        
        // Filter roadmap nodes
        document.querySelectorAll(".roadmap-node").forEach(node => {
            const text = node.innerText.toLowerCase();
            node.style.display = (!query || text.includes(query)) ? "" : "none";
        });

        // Filter dependency table rows
        document.querySelectorAll("#roadmap-dependencies-body tr").forEach(row => {
            const text = row.innerText.toLowerCase();
            row.style.display = (!query || text.includes(query)) ? "" : "none";
        });
    });
}
