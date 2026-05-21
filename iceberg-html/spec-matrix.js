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
    initSpecMatrix();
    initKanbanBoard();
    setupFiltersAndSearch();
    setupTriageSyncEngine();
});

// Spec version matrix metadata
const specData = [
    {
        feature: "Schema Evolution via Field IDs",
        v1: "Supported", v2: "Supported", v3: "Supported", v4: "Supported",
        category: "structural",
        description: "Assigns stable, unique integer IDs to every column. Resolution is by Field ID instead of name or position, guaranteeing zero-copy schema renames, additions, and reorderings without data corruption."
    },
    {
        feature: "Hidden Partitioning",
        v1: "Supported", v2: "Supported", v3: "Supported", v4: "Supported",
        category: "structural",
        description: "Applies transform functions (e.g., `days(event_time)`) to locate physical partitions automatically, freeing query writers from needing to manually filter raw directory paths."
    },
    {
        feature: "Partition Spec Evolution",
        v1: "Supported", v2: "Supported", v3: "Supported", v4: "Supported",
        category: "structural",
        description: "Allows table partition schemes to change on-the-fly (e.g., monthly to daily) by tracking unique Spec IDs. Old files remain under their historical spec; new files write to the updated spec."
    },
    {
        feature: "Copy-on-Write (CoW)",
        v1: "Supported", v2: "Supported", v3: "Supported", v4: "Supported",
        category: "row-level",
        description: "Applies updates and deletes by eagerly rewriting the targeted physical data files. Optimized for read-heavy workloads at the expense of high write amplification."
    },
    {
        feature: "Positional Deletes (MoR)",
        v1: "N/A", v2: "Supported", v3: "Supported", v4: "Supported",
        category: "row-level",
        description: "Introduces out-of-band delete files that reference rows by physical file path and absolute offset index. Decreases write amplification but requires large client memory buffers."
    },
    {
        feature: "Equality Deletes (MoR)",
        v1: "N/A", v2: "Supported", v3: "Supported", v4: "Supported",
        category: "row-level",
        description: "Logs deleted rows using field rules (e.g., `user_id = 5`). Highly optimized for streaming engines (Flink) but creates high read amplification at query time."
    },
    {
        feature: "Deletion Vectors (DVs)",
        v1: "N/A", v2: "N/A", v3: "Supported", v4: "Supported",
        category: "performance",
        description: "Directly serializes row delete masks as highly compressed Roaring Bitmaps stored inside Puffin blobs. Delivers near-zero read amplification and avoids expensive positional index sorts."
    },
    {
        feature: "Sequence Numbers Logical Clock",
        v1: "N/A", v2: "Supported", v3: "Supported", v4: "Supported",
        category: "performance",
        description: "Stamps data and delete files with transaction sequence numbers. A delete file applies to a data file if and only if S_data < S_delete, avoiding concurrent write corruptions."
    },
    {
        feature: "REPLACE Snapshot API",
        v1: "Supported", v2: "Supported", v3: "Supported", v4: "Supported",
        category: "metadata",
        description: "The core metadata transaction primitive that allows compaction engines to atomic-swap small fragmented files for large consolidated ones without changing the logical row set."
    },
    {
        feature: "Snapshot Expiration",
        v1: "Supported", v2: "Supported", v3: "Supported", v4: "Supported",
        category: "metadata",
        description: "Prunes historical snapshots from the metadata log and executes destructive physical storage prunes of unreferenced files after a configured grace period."
    }
];

const defaultTickets = [
    {
        id: "ICD-102",
        title: "ROARING Positional Delete Vector serialization in Puffin blobs (Spec V3 compliance)",
        tag: "feat",
        est: 5,
        owner: "AY",
        column: "now",
        category: "performance"
    },
    {
        id: "ICD-103",
        title: "Support REST Catalog atomic 409 Conflict Retry Handling inside transaction blocks",
        tag: "bug",
        est: 2,
        owner: "JY",
        column: "now",
        category: "metadata"
    },
    {
        id: "ICD-104",
        title: "Spec evolution spec-id tracking failure during multithreaded partition schema adjustments",
        tag: "bug",
        est: 3,
        owner: "JY",
        column: "next",
        category: "structural"
    },
    {
        id: "ICD-105",
        title: "Clean up legacy Hadoop FileIO dependency leaks inside core package-private modules",
        tag: "debt",
        est: 8,
        owner: "AY",
        column: "later",
        category: "structural"
    },
    {
        id: "ICD-106",
        title: "Draft Spec V4 JSON Schema updates for metadata timestamp nanosecond resolution",
        tag: "chore",
        est: 4,
        owner: "PM",
        column: "cut",
        category: "metadata"
    }
];

let tickets = [];
let currentCategoryFilter = "all";
let currentSearchQuery = "";

// Initialize spec matrix table
function initSpecMatrix() {
    renderSpecTable();
}

function renderSpecTable() {
    const tableBody = document.getElementById("spec-matrix-body");
    tableBody.innerHTML = "";
    
    const filtered = specData.filter(item => {
        const matchesCat = (currentCategoryFilter === "all" || item.category === currentCategoryFilter);
        const matchesSearch = (!currentSearchQuery || item.feature.toLowerCase().includes(currentSearchQuery) || item.description.toLowerCase().includes(currentSearchQuery));
        return matchesCat && matchesSearch;
    });
    
    filtered.forEach(item => {
        const row = document.createElement("tr");
        row.innerHTML = `
            <td style="font-weight: 600; font-size: 13px; color: var(--slate);">${item.feature}</td>
            <td style="text-align: center;">${getBadgeMarkup(item.v1)}</td>
            <td style="text-align: center;">${getBadgeMarkup(item.v2)}</td>
            <td style="text-align: center;">${getBadgeMarkup(item.v3)}</td>
            <td style="text-align: center;">${getBadgeMarkup(item.v4)}</td>
            <td style="font-size: 12px; color: var(--g700); line-height: 1.45;">${item.description}</td>
        `;
        tableBody.appendChild(row);
    });
}

function getBadgeMarkup(status) {
    if (status === "Supported") return `<span class="badge badge-cyan" style="background: rgba(120, 140, 93, 0.1); color: var(--olive); border-color: rgba(120,140,93,0.3);">Supported</span>`;
    return `<span class="badge" style="background: rgba(217, 119, 87, 0.08); color: var(--clay); border-color: rgba(217, 119, 87, 0.3);">N/A</span>`;
}

// ── Kanban Drag and Drop System & State Persistence ──
function initKanbanBoard() {
    // Initialize Workspace Directory Picker UI
    WorkspaceSync.initUI("ws-sync-container", 
        async (dirHandle) => {
            // Callback when directory connected
            try {
                const data = await WorkspaceSync.readWorkspaceFile(dirHandle, "triage-board.json");
                if (data && Array.isArray(data)) {
                    tickets = data;
                    renderKanbanTickets();
                    localStorage.setItem("icod-kanban-tickets", JSON.stringify(data));
                    console.log("[WorkspaceSync] Kanban tickets loaded directly from triage-board.json");
                } else {
                    loadKanbanFromLocalStorage();
                }
            } catch (e) {
                console.warn("[WorkspaceSync] triage-board.json not found or failed to parse. Mirroring active board state to directory.", e);
                try {
                    await WorkspaceSync.writeWorkspaceFile(dirHandle, "triage-board.json", tickets);
                } catch (writeErr) {
                    console.error("[WorkspaceSync] Failed to initialize triage-board.json in directory", writeErr);
                }
                loadKanbanFromLocalStorage();
            }
        },
        () => {
            // Callback when disconnected
            loadKanbanFromLocalStorage();
        }
    );

    // Setup Export Action Button
    setupExportButton();

    // Initial hydration
    loadKanbanFromLocalStorage();

    setupDragAndDropEvents();
}

function loadKanbanFromLocalStorage() {
    const saved = localStorage.getItem("icod-kanban-tickets");
    if (saved) {
        try {
            tickets = JSON.parse(saved);
        } catch (e) {
            tickets = [...defaultTickets];
        }
    } else {
        tickets = [...defaultTickets];
    }
    renderKanbanTickets();
}

function setupExportButton() {
    const exportBtn = document.getElementById("board-export-btn");
    if (!exportBtn) return;
    exportBtn.addEventListener("click", () => {
        const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(tickets, null, 2));
        const downloadAnchor = document.createElement('a');
        downloadAnchor.setAttribute("href", dataStr);
        downloadAnchor.setAttribute("download", "triage-board.json");
        document.body.appendChild(downloadAnchor);
        downloadAnchor.click();
        downloadAnchor.remove();
    });
}

function renderKanbanTickets() {
    document.querySelectorAll(".col-body").forEach(el => el.innerHTML = "");
    
    tickets.forEach(ticket => {
        const matchesCat = (currentCategoryFilter === "all" || ticket.category === currentCategoryFilter);
        const matchesSearch = (!currentSearchQuery || ticket.title.toLowerCase().includes(currentSearchQuery) || ticket.id.toLowerCase().includes(currentSearchQuery));
        
        if (!matchesCat || !matchesSearch) return;

        const ticketEl = document.createElement("div");
        ticketEl.className = "ticket";
        ticketEl.draggable = true;
        ticketEl.id = `ticket-${ticket.id}`;
        ticketEl.setAttribute("data-id", ticket.id);
        
        const ownerInitials = ticket.owner ? ticket.owner.substring(0, 3).toUpperCase() : "GH";
        
        ticketEl.innerHTML = `
            <div class="ticket-top">
                <span class="tid">${ticket.id}</span>
                <span class="tag tag-${ticket.tag}">${ticket.tag}</span>
                <span class="est">${ticket.est}d</span>
            </div>
            <h3>${ticket.title}</h3>
            <div class="ticket-meta">
                <div class="owner">
                    <div class="avatar-micro">${ownerInitials}</div>
                    <span>${ticket.owner || "Unassigned"}</span>
                </div>
                <span style="font-size: 9px; text-transform: uppercase; color: var(--g500);">${ticket.category}</span>
            </div>
        `;
        
        const body = document.getElementById(`body-${ticket.column}`);
        if (body) body.appendChild(ticketEl);
    });
    
    updateKanbanStats();
    attachTicketDragEvents();
}

function updateKanbanStats() {
    const columns = ["now", "next", "later", "cut"];
    let totalCount = 0;
    
    columns.forEach(col => {
        const colTickets = tickets.filter(t => t.column === col);
        const filteredColTickets = colTickets.filter(ticket => {
            const matchesCat = (currentCategoryFilter === "all" || ticket.category === currentCategoryFilter);
            const matchesSearch = (!currentSearchQuery || ticket.title.toLowerCase().includes(currentSearchQuery) || ticket.id.toLowerCase().includes(currentSearchQuery));
            return matchesCat && matchesSearch;
        });

        document.getElementById(`count-${col}`).innerText = filteredColTickets.length;
        
        const effort = filteredColTickets.reduce((sum, t) => sum + parseInt(t.est || 0, 10), 0);
        document.getElementById(`effort-${col}`).innerText = `${effort}d`;
        
        totalCount += filteredColTickets.length;
    });

    document.getElementById("total-tickets-display").innerText = totalCount;
    const issuesPill = document.getElementById("triage-issues-count");
    if (issuesPill) issuesPill.innerText = `${totalCount} Active`;
}

function attachTicketDragEvents() {
    document.querySelectorAll(".ticket").forEach(ticket => {
        ticket.addEventListener("dragstart", (e) => {
            ticket.classList.add("dragging");
            e.dataTransfer.setData("text/plain", ticket.getAttribute("data-id"));
            e.dataTransfer.effectAllowed = "move";
        });
        
        ticket.addEventListener("dragend", () => {
            ticket.classList.remove("dragging");
            document.querySelectorAll(".col").forEach(c => c.classList.remove("dragover"));
        });
    });
}

function setupDragAndDropEvents() {
    document.querySelectorAll(".col").forEach(col => {
        col.addEventListener("dragover", (e) => {
            e.preventDefault();
            col.classList.add("dragover");
        });
        
        col.addEventListener("dragleave", () => {
            col.classList.remove("dragover");
        });
        
        col.addEventListener("drop", (e) => {
            e.preventDefault();
            col.classList.remove("dragover");
            
            const ticketId = e.dataTransfer.getData("text/plain");
            const targetCol = col.getAttribute("data-col");
            
            const ticket = tickets.find(t => t.id === ticketId);
            if (ticket && ticket.column !== targetCol) {
                ticket.column = targetCol;
                saveKanbanState();
                renderKanbanTickets();
            }
        });
    });
}

async function saveKanbanState() {
    localStorage.setItem("icod-kanban-tickets", JSON.stringify(tickets));

    if (WorkspaceSync.activeHandle) {
        try {
            await WorkspaceSync.writeWorkspaceFile(WorkspaceSync.activeHandle, "triage-board.json", tickets);
        } catch (e) {
            console.error("[WorkspaceSync] Error saving board back to triage-board.json in workspace", e);
        }
    }
}

async function resetKanbanState() {
    localStorage.removeItem("icod-kanban-tickets");
    tickets = [...defaultTickets];
    renderKanbanTickets();

    if (WorkspaceSync.activeHandle) {
        try {
            await WorkspaceSync.writeWorkspaceFile(WorkspaceSync.activeHandle, "triage-board.json", tickets);
        } catch (e) {
            console.error("[WorkspaceSync] Error writing reset board state back to directory", e);
        }
    }
}

// ── Manual Backlog Ticket Creation Drawer ──
function toggleManualDrawer() {
    const drawer = document.getElementById("manual-ticket-drawer");
    if (!drawer) return;
    drawer.classList.toggle("open");
}

function saveManualTicket() {
    const titleInput = document.getElementById("new-ticket-title");
    const idInput = document.getElementById("new-ticket-id");
    const categorySelect = document.getElementById("new-ticket-category");
    const tagSelect = document.getElementById("new-ticket-tag");
    const ownerInput = document.getElementById("new-ticket-owner");
    const estInput = document.getElementById("new-ticket-est");

    const title = titleInput.value.trim();
    const tid = idInput.value.trim().toUpperCase();
    const category = categorySelect.value;
    const tag = tagSelect.value;
    const owner = ownerInput.value.trim() || "JY";
    const est = parseInt(estInput.value, 10) || 3;

    if (!title || !tid) {
        alert("Please complete the Title and Ticket ID fields.");
        return;
    }

    // Check for duplicate ID
    if (tickets.some(t => t.id === tid)) {
        alert("A ticket with this ID already exists. Please choose a unique ID.");
        return;
    }

    const newTicket = {
        id: tid,
        title: title,
        tag: tag,
        est: est,
        owner: owner,
        column: "now",
        category: category
    };

    tickets.push(newTicket);
    saveKanbanState();
    renderKanbanTickets();

    // Reset drawer inputs
    titleInput.value = "";
    idInput.value = "";
    ownerInput.value = "";
    estInput.value = "";
    toggleManualDrawer();
}

// ── GitHub REST API issue sync engine ──
function setupTriageSyncEngine() {
    const repoSyncBtn = document.getElementById("repo-sync-btn");
    const repoInput = document.getElementById("repo-sync-input");
    const importBtn = document.getElementById("issue-import-btn");
    const urlInput = document.getElementById("issue-url-input");
    const statusMsg = document.getElementById("issue-sync-status");
    const statusText = document.getElementById("issue-sync-text");
    const spinner = document.getElementById("issue-sync-spinner");

    if (repoSyncBtn && repoInput) {
        repoSyncBtn.addEventListener("click", async () => {
            const repoValue = repoInput.value.trim();
            if (!repoValue) {
                showStatus("Please enter a repository (owner/repo).", "error");
                return;
            }
            try {
                showStatus("Connecting to GitHub REST API...", "loading");
                const res = await fetch(`https://api.github.com/repos/${repoValue}/issues?state=open&per_page=12`);
                if (!res.ok) {
                    if (res.status === 403) {
                        throw new Error("GitHub REST API Rate Limit exceeded. Try again in an hour.");
                    } else {
                        throw new Error(`Repository not found (${res.statusText})`);
                    }
                }
                const issues = await res.json();
                
                if (issues.length === 0) {
                    showStatus("No open issues or PRs found in this repository.", "error");
                    return;
                }

                showStatus("Parsing and linearizing issues...", "loading");
                
                issues.forEach(issue => {
                    const tid = `GH-${issue.number}`;
                    
                    // Exclude duplicates
                    if (tickets.some(t => t.id === tid)) return;

                    // Tag resolution
                    let tag = "feat";
                    if (issue.labels && issue.labels.length > 0) {
                        const labelsText = issue.labels.map(l => l.name.toLowerCase()).join(" ");
                        if (labelsText.includes("bug")) tag = "bug";
                        else if (labelsText.includes("enhancement") || labelsText.includes("feature")) tag = "feat";
                        else if (labelsText.includes("refactor") || labelsText.includes("technical-debt")) tag = "debt";
                        else if (labelsText.includes("chore") || labelsText.includes("doc")) tag = "chore";
                    }

                    // Category resolution based on title keywords
                    let category = "metadata";
                    const titleText = issue.title.toLowerCase();
                    if (titleText.includes("perf") || titleText.includes("vector") || titleText.includes("roaring")) category = "performance";
                    else if (titleText.includes("spec") || titleText.includes("v2") || titleText.includes("v3") || titleText.includes("json")) category = "metadata";
                    else if (titleText.includes("schema") || titleText.includes("partition")) category = "structural";
                    else if (titleText.includes("delete") || titleText.includes("mor") || titleText.includes("writer")) category = "row-level";

                    // Owner initials mapping
                    const creator = issue.user ? issue.user.login : "GH";
                    const ownerInit = creator.substring(0, 2).toUpperCase();

                    // Dynamic estimate
                    const est = (issue.title.length % 6) + 2;

                    tickets.push({
                        id: tid,
                        title: issue.title,
                        tag: tag,
                        est: est,
                        owner: creator,
                        column: issue.pull_request ? "now" : "next",
                        category: category
                    });
                });

                saveKanbanState();
                renderKanbanTickets();
                showStatus(`Synced issues from ${repoValue}!`, "success");
            } catch (err) {
                showStatus(err.message, "error");
            }
        });
    }

    if (importBtn && urlInput) {
        importBtn.addEventListener("click", async () => {
            const urlValue = urlInput.value.trim();
            if (!urlValue) {
                showStatus("Please paste a GitHub issue or PR URL.", "error");
                return;
            }

            // Regex parsing matches issue or pull request URLs
            const issueRegex = /(?:github\.com\/)?([a-zA-Z0-9._-]+)\/([a-zA-Z0-9._-]+)\/(issues|pull)\/(\d+)/;
            const match = urlValue.match(issueRegex);

            if (!match) {
                showStatus("Invalid URL format. Paste a valid GitHub issue/PR URL.", "error");
                return;
            }

            const owner = match[1];
            const repo = match[2];
            const type = match[3];
            const number = parseInt(match[4], 10);

            try {
                showStatus("Fetching ticket metadata from API...", "loading");
                const res = await fetch(`https://api.github.com/repos/${owner}/${repo}/issues/${number}`);
                if (!res.ok) {
                    throw new Error("Unable to retrieve ticket details. Check repository visibility.");
                }
                const issue = await res.json();
                const tid = `GH-${number}`;

                // Idempotent merge
                const idx = tickets.findIndex(t => t.id === tid);

                let tag = "feat";
                if (issue.labels && issue.labels.length > 0) {
                    const labelsText = issue.labels.map(l => l.name.toLowerCase()).join(" ");
                    if (labelsText.includes("bug")) tag = "bug";
                    else if (labelsText.includes("enhancement") || labelsText.includes("feature")) tag = "feat";
                    else if (labelsText.includes("refactor") || labelsText.includes("debt")) tag = "debt";
                    else if (labelsText.includes("chore") || labelsText.includes("doc")) tag = "chore";
                }

                let category = "metadata";
                const titleText = issue.title.toLowerCase();
                if (titleText.includes("perf") || titleText.includes("vector") || titleText.includes("roaring")) category = "performance";
                else if (titleText.includes("spec") || titleText.includes("v2") || titleText.includes("v3") || titleText.includes("json")) category = "metadata";
                else if (titleText.includes("schema") || titleText.includes("partition")) category = "structural";
                else if (titleText.includes("delete") || titleText.includes("mor") || titleText.includes("writer")) category = "row-level";

                const creator = issue.user ? issue.user.login : "GH";
                const est = (issue.title.length % 5) + 3;

                const ticketData = {
                    id: tid,
                    title: issue.title,
                    tag: tag,
                    est: est,
                    owner: creator,
                    column: type === "pull" ? "now" : "next",
                    category: category
                };

                if (idx > -1) {
                    tickets[idx] = ticketData; // Update existing
                } else {
                    tickets.push(ticketData); // Insert new
                }

                saveKanbanState();
                renderKanbanTickets();
                showStatus(`Successfully imported ticket ${tid}!`, "success");
                urlInput.value = "";
            } catch (err) {
                showStatus(err.message, "error");
            }
        });
    }

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

// Synced search and pill filtering
function setupFiltersAndSearch() {
    document.querySelectorAll("#spec-matrix-filters button").forEach(btn => {
        btn.addEventListener("click", () => {
            document.querySelectorAll("#spec-matrix-filters button").forEach(b => b.classList.remove("active"));
            btn.classList.add("active");
            
            currentCategoryFilter = btn.getAttribute("data-filter");
            
            const badge = document.getElementById("active-filter-badge");
            const badgeName = document.getElementById("active-filter-name");
            
            if (currentCategoryFilter === "all") {
                badge.classList.remove("on");
            } else {
                badge.classList.add("on");
                badgeName.innerText = currentCategoryFilter.toUpperCase();
            }
            
            renderSpecTable();
            renderKanbanTickets();
        });
    });

    const searchInput = document.getElementById("global-search");
    if (searchInput) {
        searchInput.addEventListener("input", (e) => {
            currentSearchQuery = e.target.value.toLowerCase().trim();
            renderSpecTable();
            renderKanbanTickets();
        });
    }
}

function clearActiveFilter() {
    const allBtn = document.querySelector('#spec-matrix-filters button[data-filter="all"]');
    if (allBtn) allBtn.click();
}
