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

// Premium Default Mock PR Data matching standard ICOD console
const defaultMockPR = {
    number: 10247,
    title: "PR #10247: Support V3 Spec Roaring Bitmaps for Positional Delete Vectors",
    description: "Proposes integrating Roaring Bitmaps for positional delete vectors in the upcoming V3 format specification. Bypasses list materializations to optimize high-density delete writes.",
    filePath: "core/src/main/java/org/apache/iceberg/deletes/RoaringPositionDeleteWriter.java",
    riskTag: "High Risk",
    diffLines: [
        { type: "hunk", content: "@@ -45,14 +45,35 @@ public class RoaringPositionDeleteWriter implements PositionDeleteWriter {", leftLn: "@@", rightLn: "" },
        { type: "ctx", content: "  private final Path path;", leftLn: "45", rightLn: "45" },
        { type: "del", content: "  @com.fasterxml.jackson.annotation.JsonProperty(\"positions\")", leftLn: "46", rightLn: "" },
        { type: "del", content: "  private final List<Long> positions;", leftLn: "47", rightLn: "" },
        { type: "ctx", content: "  private final Schema schema;", leftLn: "48", rightLn: "48" },
        { type: "add", content: "  private final RoaringBitmap bitmap;", leftLn: "", rightLn: "49" },
        { type: "add", content: "  private final boolean enableVectorOptimization;", leftLn: "", rightLn: "50" },
        { type: "add", content: "", leftLn: "", rightLn: "51" },
        { type: "add", content: "  public RoaringPositionDeleteWriter(Path path, Schema schema) {", leftLn: "", rightLn: "52" },
        { type: "add", content: "    Preconditions.checkNotNull(path, \"Path cannot be null\");", leftLn: "", rightLn: "53" },
        { type: "add", content: "    this.path = path;", leftLn: "", rightLn: "54" },
        { type: "add", content: "    this.schema = schema;", leftLn: "", rightLn: "55" },
        { type: "add", content: "    this.bitmap = new RoaringBitmap();", leftLn: "", rightLn: "56" },
        { type: "add", content: "    this.enableVectorOptimization = true;", leftLn: "", rightLn: "57" },
        { type: "add", content: "  }", leftLn: "", rightLn: "58" }
    ],
    comments: [
        {
            type: "blocking",
            commenter: "jared-yu-pmc",
            tag: "Blocking",
            time: "2 hours ago",
            body: "<strong>Rule Violation [AGENTS.md Style/Serialization]:</strong> Banned use of standard Jackson annotations (<code>@JsonProperty</code>) in core files. Core serialization must utilize standard <code>XxxParser.toJson()</code> and <code>fromJson()</code> to maintain strict parser bounds, backward compatibility, and protect engine integrations. Please remove the annotation and implement <code>DeleteWriterParser</code>."
        },
        {
            type: "blocking",
            commenter: "iceberg-committer-bot",
            tag: "Blocking",
            time: "1 hour ago",
            body: "<strong>Precondition Check Failure:</strong> The constructor violates package-private default restrictions. Unless proven necessary to promote class visibility to public, core writers should remain <strong>package-private</strong> to avoid polluting the public <code>api/</code> module."
        },
        {
            type: "nit",
            commenter: "principal-architect",
            tag: "Nit",
            time: "10 mins ago",
            body: "Consider utilizing <code>RoaringBitmap.runOptimize()</code> before serialization to minimize the memory footprint of physical positional deletions prior to writing."
        }
    ]
};

let activePRData = defaultMockPR;

document.addEventListener("DOMContentLoaded", () => {
    lucide.createIcons();
    initDiagramInspector();
    setupSearch();
    initPRReviewHub();
});

// Initialize PR review hub from localStorage or Workspace file
function initPRReviewHub() {
    // Initialize Workspace Directory Picker UI
    WorkspaceSync.initUI("ws-sync-container", 
        async (dirHandle) => {
            // Callback when directory is connected
            try {
                const data = await WorkspaceSync.readWorkspaceFile(dirHandle, "pr-reviews.json");
                if (data && data.number) {
                    activePRData = data;
                    renderPRDetails(data);
                    localStorage.setItem("icod-active-pr", JSON.stringify(data));
                    console.log("[WorkspaceSync] PR reviews loaded directly from pr-reviews.json");
                } else {
                    loadPRFromLocalStorage();
                }
            } catch (e) {
                console.warn("[WorkspaceSync] pr-reviews.json not found or failed to parse. Mirroring active review state to directory.", e);
                try {
                    await WorkspaceSync.writeWorkspaceFile(dirHandle, "pr-reviews.json", activePRData);
                } catch (writeErr) {
                    console.error("[WorkspaceSync] Failed to initialize pr-reviews.json in directory", writeErr);
                }
                loadPRFromLocalStorage();
            }
        },
        () => {
            // Callback when disconnected
            loadPRFromLocalStorage();
        }
    );

    // Setup Export Action
    setupExportButton();

    // Setup GitHub REST Sync Engine
    setupSyncEngine();

    // Initial hydration
    loadPRFromLocalStorage();
}

function loadPRFromLocalStorage() {
    const saved = localStorage.getItem("icod-active-pr");
    if (saved) {
        try {
            activePRData = JSON.parse(saved);
        } catch (e) {
            activePRData = defaultMockPR;
        }
    } else {
        activePRData = defaultMockPR;
    }
    renderPRDetails(activePRData);
}

// Function to save PR state to both localStorage and the local workspace directory
async function savePRState(pr) {
    activePRData = pr;
    localStorage.setItem("icod-active-pr", JSON.stringify(pr));
    renderPRDetails(pr);

    if (WorkspaceSync.activeHandle) {
        try {
            await WorkspaceSync.writeWorkspaceFile(WorkspaceSync.activeHandle, "pr-reviews.json", pr);
        } catch (e) {
            console.error("[WorkspaceSync] Error writing updated reviews to pr-reviews.json in workspace", e);
        }
    }
}

function setupExportButton() {
    const exportBtn = document.getElementById("pr-export-btn");
    if (!exportBtn) return;
    exportBtn.addEventListener("click", () => {
        const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(activePRData, null, 2));
        const downloadAnchor = document.createElement('a');
        downloadAnchor.setAttribute("href", dataStr);
        downloadAnchor.setAttribute("download", "pr-reviews.json");
        document.body.appendChild(downloadAnchor);
        downloadAnchor.click();
        downloadAnchor.remove();
    });
}

function renderPRDetails(pr) {
    // Title & Metadata
    document.getElementById("pr-title-display").innerText = `PR #${pr.number}`;
    document.getElementById("pr-desc-display").innerText = pr.title.includes(":") ? pr.title.split(":").slice(1).join(":").trim() : pr.title;
    document.getElementById("file-path-display").innerText = pr.filePath;
    
    const riskTag = document.getElementById("risk-tag-display");
    riskTag.innerText = pr.riskTag || "High Risk";
    riskTag.className = `risk-tag ${pr.riskTag === "High Risk" ? "attention" : pr.riskTag === "Medium Risk" ? "warning" : "ok"}`;

    // Render Diff Console
    const diffContainer = document.getElementById("diff-container");
    diffContainer.innerHTML = "";
    
    pr.diffLines.forEach(line => {
        const row = document.createElement("div");
        row.className = `diff-row ${line.type}`;
        
        const lnLeft = document.createElement("div");
        lnLeft.className = "ln";
        lnLeft.innerText = line.leftLn;
        
        const mark = document.createElement("div");
        mark.className = "mark";
        mark.innerText = line.type === "add" ? "+" : line.type === "del" ? "-" : "";
        
        const code = document.createElement("div");
        code.className = "code";
        // Escape HTML tags to prevent rendering issues in monospaces
        const escapedCode = line.content
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;");
        code.innerHTML = escapedCode;

        row.appendChild(lnLeft);
        row.appendChild(mark);
        row.appendChild(code);
        diffContainer.appendChild(row);
    });

    // Render Comments
    const commentsContainer = document.getElementById("comments-container");
    commentsContainer.innerHTML = "";

    if (pr.comments && pr.comments.length > 0) {
        pr.comments.forEach(comment => {
            const bubble = document.createElement("div");
            bubble.className = `bubble ${comment.type}`;
            bubble.innerHTML = `
                <div class="bubble-meta">
                    <span class="commenter">${comment.commenter}</span>
                    <span class="bubble-tag ${comment.type}">${comment.tag}</span>
                    <span class="dot">•</span>
                    <span>${comment.time}</span>
                </div>
                <div class="bubble-content">
                    ${comment.body}
                </div>
            `;
            commentsContainer.appendChild(bubble);
        });
    } else {
        commentsContainer.innerHTML = `
            <div class="bubble success" style="border-left: 4px solid var(--olive); background: rgba(120, 140, 93, 0.04);">
                <div class="bubble-content" style="color: var(--olive); font-weight: 600;">
                    ✓ Zero blocking architectural issues found. The PR conforms to Apache Iceberg comitter standards!
                </div>
            </div>
        `;
    }
}

// GitHub REST API Connector Logic
function setupSyncEngine() {
    const fetchBtn = document.getElementById("pr-fetch-btn");
    const urlInput = document.getElementById("pr-url-input");
    const statusMsg = document.getElementById("pr-sync-status");
    const statusText = document.getElementById("pr-sync-text");
    const spinner = document.getElementById("pr-sync-spinner");

    if (!fetchBtn || !urlInput) return;

    fetchBtn.addEventListener("click", async () => {
        const urlValue = urlInput.value.trim();
        if (!urlValue) {
            showStatus("Please enter a valid GitHub PR URL.", "error");
            return;
        }

        // Regex parsing: matches https://github.com/{owner}/{repo}/pull/{number} or simple owner/repo/pull/number
        const prRegex = /(?:github\.com\/)?([a-zA-Z0-9._-]+)\/([a-zA-Z0-9._-]+)\/pull\/(\d+)/;
        const match = urlValue.match(prRegex);

        if (!match) {
            showStatus("Invalid URL format. Please paste a standard GitHub PR link.", "error");
            return;
        }

        const owner = match[1];
        const repo = match[2];
        const prNumber = parseInt(match[3], 10);

        try {
            showStatus("Connecting to GitHub REST API...", "loading");

            // 1. Fetch Pull Request Metadata
            const metadataRes = await fetch(`https://api.github.com/repos/${owner}/${repo}/pulls/${prNumber}`);
            if (!metadataRes.ok) {
                if (metadataRes.status === 403) {
                    throw new Error("GitHub API Rate limit exceeded (60 req/hr unauthenticated). Try again in an hour.");
                } else if (metadataRes.status === 404) {
                    throw new Error("PR not found. Please confirm the repository is public.");
                } else {
                    throw new Error(`GitHub API returned error: ${metadataRes.statusText}`);
                }
            }
            const prData = await metadataRes.json();

            showStatus("Fetching Pull Request unified diff...", "loading");

            // 2. Fetch raw PR diff
            const diffRes = await fetch(`https://api.github.com/repos/${owner}/${repo}/pulls/${prNumber}`, {
                headers: { 'Accept': 'application/vnd.github.v3.diff' }
            });
            if (!diffRes.ok) {
                throw new Error("Failed to retrieve the repository PR code diff.");
            }
            const diffText = await diffRes.text();

            showStatus("Parsing diff & scanning for committer rules...", "loading");

            // 3. Parse unified diff
            const files = parseUnifiedDiff(diffText);
            if (files.length === 0) {
                throw new Error("No modified files detected in the pull request diff.");
            }

            // Pick the first modified file in core/src/main (or just the first file in the PR if no core files are found)
            let selectedFile = files.find(f => f.path.includes("core/src/main/java")) || files[0];

            // 4. Run automated committer rules lint check
            const committerComments = generateCommitterLintComments(selectedFile);

            const fetchedPR = {
                number: prNumber,
                title: prData.title,
                description: prData.body ? (prData.body.substring(0, 180) + "...") : "No description provided.",
                filePath: selectedFile.path,
                riskTag: selectedFile.path.startsWith("core/") ? "High Risk" : "Medium Risk",
                diffLines: selectedFile.lines,
                comments: committerComments
            };

            // Save to browser databases (localStorage and connected workspace directory)
            await savePRState(fetchedPR);
            
            showStatus(`Successfully synced PR #${prNumber} from ${owner}/${repo}!`, "success");
            urlInput.value = ""; // Clear input

        } catch (err) {
            showStatus(err.message, "error");
        }
    });

    function showStatus(text, type) {
        statusMsg.className = `sync-status-msg active ${type}`;
        statusText.innerText = text;
        
        if (type === "loading") {
            spinner.style.display = "inline-block";
        } else {
            spinner.style.display = "none";
        }
    }
}

// Unified Diff Stream Parser
function parseUnifiedDiff(diffText) {
    const lines = diffText.split('\n');
    const files = [];
    let currentFile = null;
    let leftLine = 0;
    let rightLine = 0;

    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        if (line.startsWith('diff --git ')) {
            const parts = line.split(' ');
            const filePath = parts[3] ? parts[3].substring(2) : 'Unknown File';
            currentFile = {
                path: filePath,
                lines: [],
                additions: 0,
                deletions: 0
            };
            files.push(currentFile);
        } else if (currentFile) {
            if (line.startsWith('--- a/') || line.startsWith('+++ b/')) {
                continue;
            } else if (line.startsWith('@@ ')) {
                const match = line.match(/^@@ -(\d+),?\d* \+(\d+),?\d* @@/);
                if (match) {
                    leftLine = parseInt(match[1], 10);
                    rightLine = parseInt(match[2], 10);
                }
                currentFile.lines.push({
                    type: "hunk",
                    content: line,
                    leftLn: "@@",
                    rightLn: ""
                });
            } else if (line.startsWith('+')) {
                currentFile.additions++;
                currentFile.lines.push({
                    type: "add",
                    content: line.substring(1),
                    leftLn: "",
                    rightLn: rightLine++
                });
            } else if (line.startsWith('-')) {
                currentFile.deletions++;
                currentFile.lines.push({
                    type: "del",
                    content: line.substring(1),
                    leftLn: leftLine++,
                    rightLn: ""
                });
            } else if (line.startsWith(' ')) {
                currentFile.lines.push({
                    type: "ctx",
                    content: line.substring(1),
                    leftLn: leftLine++,
                    rightLn: rightLine++
                });
            }
        }
    }
    return files;
}

// Automated Committer Code Auditor Checks
function generateCommitterLintComments(file) {
    const comments = [];
    let hasJackson = false;
    let hasPublicWriter = false;
    let hasHadoopLeak = false;

    file.lines.forEach(line => {
        const text = line.content.toLowerCase();

        // 1. Serialization check (Jackson Annotations are banned)
        if ((text.includes("jsonproperty") || text.includes("jackson")) && line.type === "add") {
            hasJackson = true;
        }

        // 2. Encapsulation / Boundary gate (Core elements must be package-private by default)
        if (text.includes("public ") && text.includes("class ") && text.includes("writer") && line.type === "add") {
            hasPublicWriter = true;
        }

        // 3. FileIO abstraction check (Hadoop references are banned in core)
        if ((text.includes("hadoop") || text.includes("fs.path")) && line.type === "add") {
            hasHadoopLeak = true;
        }
    });

    if (hasJackson) {
        comments.push({
            type: "blocking",
            commenter: "jared-yu-pmc",
            tag: "Blocking",
            time: "Just now",
            body: "<strong>Rule Violation [AGENTS.md Style/Serialization]:</strong> Banned use of Jackson annotations (<code>@JsonProperty</code>) in core files. Core serialization must utilize standard <code>XxxParser.toJson()</code> and <code>fromJson()</code> to maintain strict parser bounds and protect engine integrations."
        });
    }

    if (hasPublicWriter) {
        comments.push({
            type: "blocking",
            commenter: "iceberg-committer-bot",
            tag: "Blocking",
            time: "Just now",
            body: "<strong>Precondition Check Failure:</strong> The new writer violates package-private default restrictions. Core classes should remain <strong>package-private</strong> to avoid polluting the public <code>api/</code> module unless explicitly justified."
        });
    }

    if (hasHadoopLeak) {
        comments.push({
            type: "blocking",
            commenter: "iceberg-committer-bot",
            tag: "Blocking",
            time: "Just now",
            body: "<strong>Dependency Boundary Leak:</strong> Detected raw Hadoop package leaks inside core module. Iceberg utilizes high-performance <code>FileIO</code> wrappers to stay cloud-neutral. Please remove direct Hadoop imports."
        });
    }

    // Standard optimization nit for RoaringBitmaps
    if (file.path.toLowerCase().includes("roaring") || file.path.toLowerCase().includes("bitmap")) {
        comments.push({
            type: "nit",
            commenter: "principal-architect",
            tag: "Nit",
            time: "Just now",
            body: "Consider utilizing <code>RoaringBitmap.runOptimize()</code> before serialization to minimize the memory footprint of physical positional deletions prior to writing."
        });
    }

    return comments;
}

// ── Academic Wiki Diagram Inspector Functions (Preserved) ──
const nodeDetails = {
    catalog: {
        title: "1. Catalog Layer (Atomic Pointer Swap)",
        description: "The primary transaction linearizer. Iceberg tables enforce isolation by executing optimistic concurrency control (OCC). Writers stage a proposed table metadata update and attempt to atomically swap the catalog's pointer to refer to the new metadata JSON. Concurrent overrides trigger validation retries or throw catalog conflict exceptions.",
        extra: "<h6>Catalog Invariants</h6><code>atomicPointerSwap()</code> <code>validateStagedCommit()</code> <code>REST 409 Conflict Handling</code>"
    },
    metadata: {
        title: "2. Table Metadata JSON (Immutable Timeline Root)",
        description: "The immutable root document logging schemas, partition specifications, dynamic sort orders, and historical snapshots. Changes trigger spec-version gating checks (e.g. validating that V3 features are restricted to V3 table configurations). The timeline metadata is fully serializable to support REST interfaces.",
        extra: "<h6>Verifications Gate</h6><code>current-schema-id</code> <code>partition-specs[]</code> <code>snapshot-log[]</code> <code>format-version</code>"
    },
    "manifest-list": {
        title: "3. Manifest List File (Active Snapshot Index)",
        description: "A binary Avro file corresponding to a unique snapshot identifier. It indexes active manifest files and records partition-level min/max bounds and stats. Pruning algorithms analyze these statistics at query compilation time, entirely skipping manifest reads if target keys fall outside bounds.",
        extra: "<h6>Pruning Invariants</h6><code>manifest_path</code> <code>added_rows_count</code> <code>partitions[] (stats bounds)</code> <code>sequence_number</code>"
    },
    manifest: {
        title: "4. Manifest File (Data & Delete Index)",
        description: "Indexes physical files (both data and deletions). It logs column-level metrics (min/max bounds, null/NaN counts) for every column. To ensure thread safety during parallel scans, callers must call copyWithoutStats() or avoid holding direct pointers due to resource sharing.",
        extra: "<h6>Metric Bounds</h6><code>file_path</code> <code>file_size_in_bytes</code> <code>column_sizes (stats)</code> <code>value_counts</code>"
    },
    "data-file": {
        title: "5a. Physical Data Layer (Parquet Storage)",
        description: "Columnar data files stored in object storage. Writers must structure data in clean, unpartitioned or partition-bounded folders, keeping physical schemas aligned with active schema IDs. Data files are fully immutable and referenced globally across multiple historical snapshots.",
        extra: "<h6>Supported Types</h6><code>Parquet (Primary)</code> <code>ORC</code> <code>Avro</code> <code>Snappy / Gzip Compression</code>"
    },
    "delete-file": {
        title: "5b. Physical Deletes (Deletion Vectors)",
        description: "In Merge-on-Read configurations, row-level updates stage out-of-band updates. Under the V3 spec, deletion vectors are written directly inside Puffin file buffers (using Roaring Bitmaps) to optimize logical exclusion checks, reducing disk seeking and JVM memory loads.",
        extra: "<h6>Deletes Invariant</h6><code>Positional deletes (.parquet)</code> <code>Equality deletes (.parquet)</code> <code>Deletion Vectors (.puffin)</code>"
    }
};

function initDiagramInspector() {
    document.querySelectorAll(".diagram-node").forEach(node => {
        node.addEventListener("click", () => {
            document.querySelectorAll(".diagram-node").forEach(n => n.classList.remove("active"));
            node.classList.add("active");
            
            const type = node.classList.contains("node-catalog") ? "catalog" :
                         node.classList.contains("node-metadata") ? "metadata" :
                         node.classList.contains("node-manifest-list") ? "manifest-list" :
                         node.classList.contains("node-manifest") ? "manifest" :
                         node.classList.contains("node-data-file") ? "data-file" : "delete-file";
                           
            showNodeDetails(type);
        });
    });
}

function showNodeDetails(type) {
    const details = nodeDetails[type];
    if (details) {
        document.getElementById("diag-detail-title").innerText = details.title;
        document.getElementById("diag-detail-description").innerText = details.description;
        document.getElementById("diag-detail-extra").innerHTML = details.extra;
    }
}

function setupSearch() {
    const searchInput = document.getElementById("global-search");
    if (!searchInput) return;
    
    searchInput.addEventListener("input", (e) => {
        const query = e.target.value.toLowerCase().trim();
        document.querySelectorAll(".diagram-node").forEach(node => {
            const text = node.innerText.toLowerCase();
            if (!query || text.includes(query)) {
                node.style.borderColor = query ? "var(--clay)" : "";
                node.style.boxShadow = query ? "0 0 0 3px rgba(217, 119, 87, 0.12)" : "";
            } else {
                node.style.borderColor = "";
                node.style.boxShadow = "";
            }
        });
    });
}
