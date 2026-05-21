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
    initSimulator();
    setupSearch();
});

let simState = {
    step: 0, // 0: init, 1: initial append, 2: concurrent branched, 3: commit B, 4: commit A, 5: compact, 6: expired, 7: pruned
    snapshotId: "N/A",
    sequenceNumber: 0,
    activeDataFiles: [],
    activeDeleteFiles: [],
    physicalStorageFiles: [],
    logs: []
};

const initialSimState = {
    step: 0,
    snapshotId: "N/A",
    sequenceNumber: 0,
    activeDataFiles: [],
    activeDeleteFiles: [],
    physicalStorageFiles: [],
    logs: [
        "[SYSTEM] OCC Simulator initialized. Catalog active on table 'db.iceberg_table'.",
        "[SYSTEM] Ready. Click 'Append Data (Seq 1)' to initialize table structure."
    ]
};

function initSimulator() {
    resetSimulator();
}

function resetSimulator() {
    simState = JSON.parse(JSON.stringify(initialSimState));
    updateSimulatorUI();
}

function updateSimulatorUI() {
    // Enable/disable buttons based on current simulation step
    document.getElementById("btn-append-v1").disabled = (simState.step !== 0);
    document.getElementById("btn-append-v2").disabled = (simState.step !== 1);
    document.getElementById("btn-stage-concurrent").disabled = (simState.step !== 1);
    document.getElementById("btn-compact").disabled = (simState.step !== 4);
    document.getElementById("btn-expire").disabled = (simState.step !== 5);
    document.getElementById("btn-orphan").disabled = (simState.step !== 6);

    if (simState.step === 2) {
        document.getElementById("split-concurrent-buttons").style.display = "grid";
    } else {
        document.getElementById("split-concurrent-buttons").style.display = "none";
    }

    // Update top status bar
    document.getElementById("navbar-snapshot-id").innerText = simState.snapshotId;
    document.getElementById("navbar-sequence-number").innerText = simState.sequenceNumber;

    // Update live log box
    const logBox = document.getElementById("sim-log-box");
    logBox.innerHTML = "";
    simState.logs.forEach(log => {
        const entry = document.createElement("div");
        entry.className = "log-line";
        if (log.includes("[SUCCESS]") || log.includes("[COMMIT]")) {
            entry.classList.add("success");
        } else if (log.includes("[ERROR]")) {
            entry.classList.add("error");
        } else if (log.includes("[SYSTEM]") || log.includes("[OCC]") || log.includes("[GC]")) {
            entry.classList.add("warn");
        }
        entry.innerText = log;
        logBox.appendChild(entry);
    });
    logBox.scrollTop = logBox.scrollHeight;

    // Render tree visualizer
    renderVisualTree();
}

function addSimLog(msg) {
    simState.logs.push(msg);
}

function simTrigger(action) {
    if (action === "append-v1") {
        simState.step = 1;
        simState.sequenceNumber = 1;
        simState.snapshotId = 10782922;
        simState.activeDataFiles.push({ path: "data_1.parquet", seq: 1 });
        simState.physicalStorageFiles.push("data_1.parquet");
        
        addSimLog("[APPEND] Wrote physical data file 'data_1.parquet' in bucket/table/data/.");
        addSimLog("[COMMIT] Proposed snapshot metadata write successful.");
        addSimLog("[SUCCESS] Atomic swap completed. Catalog pointer successfully swapped to snapshot 10782922 (Sequence 1).");
        
    } else if (action === "append-v2") {
        simState.step = 1; // stay at single commit level but add another file
        simState.sequenceNumber = 2;
        simState.snapshotId = 31319088;
        simState.activeDataFiles.push({ path: "data_2.parquet", seq: 2 });
        simState.physicalStorageFiles.push("data_2.parquet");
        
        addSimLog("[APPEND] Wrote physical data file 'data_2.parquet' in bucket/table/data/.");
        addSimLog("[COMMIT] Swap initiated. Catalog pointer successfully swapped to snapshot 31319088 (Sequence 2).");
        
    } else if (action === "stage-concurrent") {
        simState.step = 2;
        addSimLog("[SYSTEM] Writer A and Writer B branch staged writes concurrently from Snapshot 10782922.");
        addSimLog("[OCC] Writer A: Stages row-level delete on 'data_1.parquet', writing delete file 'delete_1.parquet' (Staged Seq 3).");
        addSimLog("[OCC] Writer B: Stages a new data append, writing 'data_2.parquet' (Staged Seq 2).");
        
    } else if (action === "commit-append") {
        simState.step = 3;
        simState.sequenceNumber = 2;
        simState.snapshotId = 31319088;
        simState.activeDataFiles.push({ path: "data_2.parquet", seq: 2 });
        simState.physicalStorageFiles.push("data_2.parquet");
        
        addSimLog("[COMMIT] Writer B (Append) locked catalog. No conflicting updates detected.");
        addSimLog("[SUCCESS] Snapshot 31319088 committed successfully at Sequence 2!");
        
    } else if (action === "commit-delete") {
        if (simState.step !== 3) {
            addSimLog("[ERROR] Serializability breach! Writer B must commit first to simulate concurrency validation checking.");
            updateSimulatorUI();
            return;
        }
        
        addSimLog("[OCC] Writer A initiating commit. Catalog lock acquired.");
        addSimLog("[OCC] Running optimistic validation checks...");
        addSimLog("[OCC] Validation 1: validateDataFilesExist(). Targeted file 'data_1.parquet' still exists. Passed!");
        addSimLog("[OCC] Validation 2: validateNoConflictingAppends(). Concurrent appends since snapshot 10782922 do not overlap. Passed!");
        
        simState.step = 4;
        simState.sequenceNumber = 3;
        simState.snapshotId = 18082261;
        simState.activeDeleteFiles.push({ path: "delete_1.parquet", target: "data_1.parquet", seq: 3 });
        simState.physicalStorageFiles.push("delete_1.parquet");
        
        addSimLog("[SUCCESS] Writer A committed delete file successfully! Snapshot advanced to 18082261 (Seq 3).");
        addSimLog("[SYSTEM] Invariant holds: S_data(data_1.parquet) = 1 < S_delete(delete_1.parquet) = 3.");
        
    } else if (action === "compact") {
        simState.step = 5;
        simState.sequenceNumber = 4;
        simState.snapshotId = 95818804;
        
        // Compaction consolidates data_1 and data_2 into data_3, applying delete_1
        simState.activeDataFiles = [{ path: "data_3.parquet", seq: 4 }];
        simState.activeDeleteFiles = [];
        simState.physicalStorageFiles.push("data_3.parquet");
        
        addSimLog("[SYSTEM] Compaction triggered. Reading active files: data_1.parquet, data_2.parquet, delete_1.parquet.");
        addSimLog("[SYSTEM] Eagerly filtered deleted rows and output clean consolidated file 'data_3.parquet' (Seq 4).");
        addSimLog("[COMMIT] Committed Snapshot 95818804 via REPLACE transaction API.");
        addSimLog("[SUCCESS] Replaced files removed from active manifest list indices. Table compacted successfully!");
        
    } else if (action === "expire") {
        simState.step = 6;
        addSimLog("[SYSTEM] Expiring historical snapshot logs...");
        addSimLog("[SYSTEM] Pruning snapshots 10782922, 31319088, and 18082261 from table metadata snapshot log.");
        addSimLog("[SUCCESS] Snapshots expired. Replaced physical files data_1.parquet, data_2.parquet, and delete_1.parquet are now completely unreferenced in metadata history.");
        
    } else if (action === "orphan") {
        simState.step = 7;
        
        // Destructive physical storage pruning removes unreferenced files
        simState.physicalStorageFiles = ["data_3.parquet"];
        
        addSimLog("[SYSTEM] Starting Out-of-Band Storage Garbage Collection...");
        addSimLog("[GC] Gating safety check: 'gc.enabled' = true. Verified!");
        addSimLog("[GC] Grace period validation: referenced files older than 24 hours. Verified!");
        addSimLog("[SUCCESS] Physically purged orphaned Parquet storage files: data_1.parquet, data_2.parquet, delete_1.parquet.");
        addSimLog("[SUCCESS] Garbage Collection complete. Storage is perfectly lean!");
    }

    updateSimulatorUI();
}

function renderVisualTree() {
    const container = document.getElementById("viz-tree-container");
    container.innerHTML = "";

    const treeDiv = document.createElement("div");
    treeDiv.className = "tree-node-visual";

    if (simState.step === 0) {
        // Table is empty
        treeDiv.innerHTML = `
            <div class="tree-box active" style="border-left: 3px solid var(--clay);">
                <strong style="color: var(--slate);">REST Catalog Pointer</strong>
                <div style="font-size: 10px; color: var(--g500); margin-top: 3px; font-family: var(--mono);">Empty Catalog</div>
            </div>
            <div style="font-size: 12px; color: var(--g500); margin-top: 24px;">Initialize the table metadata structure by appending data.</div>
        `;
        container.appendChild(treeDiv);
        lucide.createIcons();
        return;
    }

    // Node 1: Catalog
    let innerHTML = `
        <div class="tree-box active" style="border-left: 3px solid var(--clay); width: 220px; text-align: left;">
            <div style="display: flex; align-items: center; gap: 6px; font-size: 11px; font-weight: 600; color: var(--slate);">
                <i data-lucide="database" style="width: 13px; height: 13px; color: var(--clay);"></i>
                REST Catalog Pointer
            </div>
            <div style="font-size: 10px; color: var(--g500); margin-top: 4px; font-family: var(--mono); word-break: break-all;">table-metadata.json</div>
        </div>
        
        <div class="diagram-arrow-vertical"></div>
        
        <!-- Node 2: Table Metadata -->
        <div class="tree-box" style="border-left: 3px solid var(--purple); width: 220px; text-align: left;">
            <div style="display: flex; align-items: center; gap: 6px; font-size: 11px; font-weight: 600; color: var(--slate);">
                <i data-lucide="file-json" style="width: 13px; height: 13px; color: var(--olive);"></i>
                Table Metadata Root
            </div>
            <div style="font-size: 10px; color: var(--g500); margin-top: 4px; font-family: var(--mono);">Snapshot ID: ${simState.snapshotId}</div>
            <div style="font-size: 9px; color: var(--clay); margin-top: 2px;">Format version: V2</div>
        </div>
        
        <div class="diagram-arrow-vertical"></div>
        
        <!-- Node 3: Manifest List -->
        <div class="tree-box" style="border-left: 3px solid var(--clay-d); width: 220px; text-align: left;">
            <div style="display: flex; align-items: center; gap: 6px; font-size: 11px; font-weight: 600; color: var(--slate);">
                <i data-lucide="list-ordered" style="width: 13px; height: 13px; color: var(--clay-d);"></i>
                Manifest List Index
            </div>
            <div style="font-size: 10px; color: var(--g500); margin-top: 4px; font-family: var(--mono);">Sequence Number: ${simState.sequenceNumber}</div>
            <div style="font-size: 9px; color: var(--g500); margin-top: 2px;">Active Snapshots: 1</div>
        </div>
        
        <div class="diagram-arrow-vertical"></div>
    `;

    // Row 4: File columns
    innerHTML += `
        <div class="diagram-split-branches" style="width: 100%; justify-content: center; gap: 16px; align-items: flex-start;">
            <!-- Active Data Files -->
            <div style="flex: 1; display: flex; flex-direction: column; gap: 8px;">
                <div style="font-size: 9px; text-transform: uppercase; color: var(--g500); font-weight: 600; border-bottom: 1.5px solid var(--g300); padding-bottom: 4px;">
                    Active Data Files
                </div>
    `;

    if (simState.activeDataFiles.length === 0) {
        innerHTML += `<div style="font-size: 11px; color: var(--g500); font-style: italic;">Empty</div>`;
    } else {
        simState.activeDataFiles.forEach(file => {
            innerHTML += `
                <div class="tree-box" style="border-left: 3px solid var(--olive); text-align: left; background: var(--paper); padding: 8px;">
                    <div style="font-size: 10.5px; font-weight: 600; font-family: var(--mono); color: var(--slate);">${file.path}</div>
                    <div style="font-size: 9px; color: var(--g500); margin-top: 2px; font-family: var(--mono);">Sequence: ${file.seq}</div>
                </div>
            `;
        });
    }

    innerHTML += `
            </div>
            
            <!-- Active Delete Files -->
            <div style="flex: 1; display: flex; flex-direction: column; gap: 8px;">
                <div style="font-size: 9px; text-transform: uppercase; color: var(--g500); font-weight: 600; border-bottom: 1.5px solid var(--g300); padding-bottom: 4px;">
                    Active Delete Files
                </div>
    `;

    if (simState.activeDeleteFiles.length === 0) {
        innerHTML += `<div style="font-size: 11px; color: var(--g500); font-style: italic;">Empty</div>`;
    } else {
        simState.activeDeleteFiles.forEach(file => {
            innerHTML += `
                <div class="tree-box" style="border-left: 3px solid var(--rust); text-align: left; background: var(--paper); padding: 8px;">
                    <div style="font-size: 10.5px; font-weight: 600; font-family: var(--mono); color: var(--slate);">${file.path}</div>
                    <div style="font-size: 9px; color: var(--g500); margin-top: 2px; font-family: var(--mono);">Seq: ${file.seq} | Targets: ${file.target}</div>
                </div>
            `;
        });
    }

    innerHTML += `
            </div>
        </div>
    `;

    // Staged files block
    if (simState.step === 2 || simState.step === 3) {
        innerHTML += `
            <div style="width: 100%; margin-top: 16px;">
                <div style="font-size: 9px; text-transform: uppercase; color: var(--purple); font-weight: 600; border-bottom: 1.5px solid var(--g300); padding-bottom: 4px; margin-bottom: 8px;">
                    Staged Uncommitted Writes
                </div>
                <div style="display: flex; gap: 12px;">
                    <div class="tree-box" style="flex: 1; border: 1.5px dashed var(--purple); border-left: 3px dashed var(--purple); text-align: left; background: var(--paper); padding: 8px; opacity: 0.85;">
                        <div style="font-size: 10.5px; font-weight: 600; font-family: var(--mono); color: var(--slate);">delete_1.parquet (Staged)</div>
                        <div style="font-size: 9px; color: var(--g500); margin-top: 2px; font-family: var(--mono);">Staged Seq: 3 | Target: data_1.parquet</div>
                        <span style="font-size: 8.5px; background: rgba(180, 80, 200, 0.1); color: var(--purple); padding: 1px 4px; border-radius: 3px; display: inline-block; margin-top: 4px;">Writer A (Delete)</span>
                    </div>
        `;

        if (simState.step === 2) {
            innerHTML += `
                    <div class="tree-box" style="flex: 1; border: 1.5px dashed var(--clay); border-left: 3px dashed var(--clay); text-align: left; background: var(--paper); padding: 8px; opacity: 0.85;">
                        <div style="font-size: 10.5px; font-weight: 600; font-family: var(--mono); color: var(--slate);">data_2.parquet (Staged)</div>
                        <div style="font-size: 9px; color: var(--g500); margin-top: 2px; font-family: var(--mono);">Staged Seq: 2</div>
                        <span style="font-size: 8.5px; background: rgba(217, 119, 87, 0.1); color: var(--clay); padding: 1px 4px; border-radius: 3px; display: inline-block; margin-top: 4px;">Writer B (Append)</span>
                    </div>
            `;
        }

        innerHTML += `
                </div>
            </div>
        `;
    }

    // Physical storage block (Step 5, 6, 7)
    if (simState.step >= 5) {
        innerHTML += `
            <div style="width: 100%; margin-top: 20px;">
                <div style="font-size: 9px; text-transform: uppercase; color: var(--slate); font-weight: 600; border-bottom: 1.5px solid var(--g300); padding-bottom: 4px; margin-bottom: 8px;">
                    Physical Storage Objects (bucket/table/data/)
                </div>
                <div style="display: flex; flex-direction: column; gap: 6px;">
        `;

        const referencedFiles = ["data_3.parquet"];
        const filesToShow = ["data_1.parquet", "data_2.parquet", "delete_1.parquet", "data_3.parquet"];
        if (simState.step === 7) {
            // Deleted orphans
            filesToShow.length = 0;
            filesToShow.push("data_3.parquet");
        }

        filesToShow.forEach(file => {
            const isOrphan = (simState.step === 5 || simState.step === 6) && file !== "data_3.parquet";
            innerHTML += `
                <div class="tree-box" style="border-left: 3px solid ${isOrphan ? 'var(--rust)' : 'var(--olive)'}; text-align: left; background: ${isOrphan ? 'rgba(176, 74, 63, 0.04)' : 'rgba(120, 140, 93, 0.04)'}; display: flex; justify-content: space-between; align-items: center; padding: 6px 10px;">
                    <div style="font-family: var(--mono); font-size: 10.5px; color: var(--slate);">${file}</div>
                    <div style="font-size: 9px; font-weight: 600; text-transform: uppercase; color: ${isOrphan ? 'var(--rust)' : 'var(--olive)'};">
                        ${isOrphan ? 'Orphaned (Unreferenced)' : 'Active (Referenced)'}
                    </div>
                </div>
            `;
        });

        innerHTML += `
                </div>
            </div>
        `;
    }

    treeDiv.innerHTML = innerHTML;
    container.appendChild(treeDiv);
    lucide.createIcons();
}

function setupSearch() {
    const searchInput = document.getElementById("global-search");
    if (!searchInput) return;
    searchInput.addEventListener("input", (e) => {
        const query = e.target.value.toLowerCase().trim();
        if (!query) {
            document.querySelectorAll("#sim-log-box .log-line").forEach(line => line.style.backgroundColor = "");
            return;
        }

        document.querySelectorAll("#sim-log-box .log-line").forEach(line => {
            const text = line.innerText.toLowerCase();
            if (text.includes(query)) {
                line.style.backgroundColor = "rgba(217, 119, 87, 0.15)";
            } else {
                line.style.backgroundColor = "";
            }
        });
    });
}
