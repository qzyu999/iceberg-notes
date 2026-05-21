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

"use strict";

// Premium preloaded fallback database for the PyIceberg Committer Roadmap & Maintenance operations
const PRELOADED_DATABASE = [
  {
    "id": 1,
    "category": "Expire Snapshots",
    "topic": "Expire Snapshots",
    "issue": "Support Snapshot Expiration Operation · Issue #516 · apache/iceberg-python",
    "issueNum": 516,
    "pr": "https://github.com/apache/iceberg-python/pull/1880",
    "owner": "ForeverAngry",
    "reviewed": "N/A",
    "status": "Merged",
    "version": "V1/V2",
    "morCow": "CoW/MoR",
    "maintenanceRelationship": "Consolidates snapshot history by expiring older commits and removing obsolete manifests.",
    "comment": "Successfully implemented and merged snapshot expiration framework."
  },
  {
    "id": 2,
    "category": "Remove old metadata files",
    "topic": "Remove old metadata files",
    "issue": "Remove old metadata files #1199 - apache/iceberg-python",
    "issueNum": 1199,
    "pr": "https://github.com/apache/iceberg-python/pull/1607",
    "owner": "kaushiksrini",
    "reviewed": "N/A",
    "status": "Merged",
    "version": "V1/V2",
    "morCow": "CoW/MoR",
    "maintenanceRelationship": "Cleans up old metadata json files to prevent storage bloat beyond catalog sync limits.",
    "comment": "Fully merged."
  },
  {
    "id": 3,
    "category": "Delete orphan files",
    "topic": "Delete orphan files",
    "issue": "Delete orphan files #1200 - apache/iceberg-python",
    "issueNum": 1200,
    "pr": "https://github.com/apache/iceberg-python/pull/1958",
    "owner": "jayceslesar",
    "reviewed": "No",
    "status": "Closed",
    "version": "V1/V2",
    "morCow": "CoW/MoR",
    "maintenanceRelationship": "Identifies and deletes untracked physical files not referenced in any metadata trees.",
    "comment": "Would like to reopen PR after progress on REPLACE, compaction, etc."
  },
  {
    "id": 4,
    "category": "Delete orphan files",
    "topic": "Delete orphan files",
    "issue": "Delete orphan files #1200 - apache/iceberg-python",
    "issueNum": 1200,
    "pr": "https://github.com/apache/iceberg-python/pull/3361",
    "owner": "rambleraptor",
    "reviewed": "No",
    "status": "Open",
    "version": "V1/V2",
    "morCow": "CoW/MoR",
    "maintenanceRelationship": "Replaces closed PR #1958 to implement the file-system walker and deletion logic.",
    "comment": "Active review needed."
  },
  {
    "id": 5,
    "category": "Compact data files",
    "topic": "Compact data files",
    "issue": "Support data files compaction · Issue #1092 · apache/iceberg-python",
    "issueNum": 1092,
    "pr": "https://github.com/apache/iceberg-python/pull/3124",
    "owner": "qzyu999",
    "reviewed": "In-progress",
    "status": "Open",
    "version": "V2",
    "morCow": "CoW",
    "maintenanceRelationship": "Compacts small files to resolve read-amplification issues on Copy-on-Write tables.",
    "comment": "Waiting for review on REPLACE before returning to this PR."
  },
  {
    "id": 6,
    "category": "Compact data files",
    "topic": "REPLACE operation",
    "issue": "Feature: Add metadata-only replace API to Table for REPLACE snapshot operations · Issue #3130 · apache/iceberg-python",
    "issueNum": 3130,
    "pr": "https://github.com/apache/iceberg-python/pull/3131",
    "owner": "qzyu999",
    "reviewed": "In-progress",
    "status": "Open",
    "version": "V1/V2",
    "morCow": "CoW",
    "maintenanceRelationship": "Prerequisite metadata-only REPLACE API necessary to commit compaction results atomic-pointer swaps.",
    "comment": "Waiting for response from kevinjqliu, geruh."
  },
  {
    "id": 7,
    "category": "Compact data files, Retry commit",
    "topic": "Support IsolationLevels and Concurrency Safety Validation Checks",
    "issue": "Support IsolationLevels and Concurrency Safety Validation Checks · Issue #819 · apache/iceberg-python",
    "issueNum": 819,
    "pr": "",
    "owner": "",
    "reviewed": "",
    "status": "Open",
    "version": "V1/V2",
    "morCow": "CoW/MoR",
    "maintenanceRelationship": "High-level epic validating transaction safety levels to implement concurrent commit retries.",
    "comment": "Epic tracking all validation requirements for concurrent retries."
  },
  {
    "id": 8,
    "category": "Compact data files, Retry commit",
    "topic": "validation_history, ancestors_between",
    "issue": "Support IsolationLevels and Concurrency Safety Validation Checks · Issue #819 · apache/iceberg-python",
    "issueNum": 819,
    "pr": "https://github.com/apache/iceberg-python/pull/1935",
    "owner": "jayceslesar",
    "reviewed": "N/A",
    "status": "Merged",
    "version": "V1/V2",
    "morCow": "CoW/MoR",
    "maintenanceRelationship": "Provides parent-child snapshot lineage queries so transactions can track historical ancestors during retry validations.",
    "comment": "Fully merged."
  },
  {
    "id": 9,
    "category": "Compact data files, Retry commit",
    "topic": "validateDeletedDataFiles",
    "issue": "Support Concurrency Safety Validation: Implement `validateDeletedDataFiles` · Issue #1928 · apache/iceberg-python",
    "issueNum": 1928,
    "pr": "https://github.com/apache/iceberg-python/pull/1938",
    "owner": "jayceslesar",
    "reviewed": "N/A",
    "status": "Merged",
    "version": "V1/V2",
    "morCow": "CoW/MoR",
    "maintenanceRelationship": "Verifies that files scheduled for deletion during retry weren't concurrently deleted by a parallel commit.",
    "comment": "Fully merged."
  },
  {
    "id": 10,
    "category": "Compact data files, Retry commit",
    "topic": "validateAddedDataFiles",
    "issue": "Support Concurrency Safety Validation: Implement `validateAddedDataFiles` · Issue #1929 · apache/iceberg-python",
    "issueNum": 1929,
    "pr": "https://github.com/apache/iceberg-python/pull/2050",
    "owner": "kaushiksrini",
    "reviewed": "N/A",
    "status": "Merged",
    "version": "V1/V2",
    "morCow": "CoW/MoR",
    "maintenanceRelationship": "Ensures parallel appends do not conflict with the active transactional workspace state.",
    "comment": "Fully merged."
  },
  {
    "id": 11,
    "category": "Compact data files, Retry commit",
    "topic": "validateNoNewDeleteFiles",
    "issue": "Support Concurrency Safety Validation: Implement `validateNoNewDeleteFiles` · Issue #1930 · apache/iceberg-python",
    "issueNum": 1930,
    "pr": "https://github.com/apache/iceberg-python/pull/3049",
    "owner": "gabeiglio",
    "reviewed": "N/A",
    "status": "Merged",
    "version": "V2",
    "morCow": "MoR",
    "maintenanceRelationship": "Fails the retry transaction if new delete files are committed to target partitions in parallel.",
    "comment": "Fully merged."
  },
  {
    "id": 12,
    "category": "Compact data files, Retry commit",
    "topic": "validateNoNewDeletesForDataFiles",
    "issue": "Support Concurrency Safety Validation: Implement `validateNoNewDeletesForDataFiles` · Issue #1931 · apache/iceberg-python",
    "issueNum": 1931,
    "pr": "https://github.com/apache/iceberg-python/pull/3049",
    "owner": "gabeiglio",
    "reviewed": "N/A",
    "status": "Merged",
    "version": "V2",
    "morCow": "MoR",
    "maintenanceRelationship": "Protects against concurrent delete files applied directly to modified data files.",
    "comment": "Fully merged."
  },
  {
    "id": 13,
    "category": "Compact data files, Retry commit",
    "topic": "Add commit retry with data conflict validation #3319",
    "issue": "Add commit retry with data conflict validation · Issue #3319 · apache/iceberg-python",
    "issueNum": 3319,
    "pr": "https://github.com/apache/iceberg-python/pull/3320",
    "owner": "lawofcycles",
    "reviewed": "In-progress",
    "status": "Open",
    "version": "V1/V2",
    "morCow": "CoW/MoR",
    "maintenanceRelationship": "Integrates retry loops and all validation gates. Prerequisite for all compaction commits.",
    "comment": "Needs to merge before #3131, also needs PR follow-up after RowDelta."
  },
  {
    "id": 14,
    "category": "Compact data files, MoR",
    "topic": "Position deletes (compaction and cleanup)",
    "issue": "Support producing positional deletes · Issue #1808 · apache/iceberg-python",
    "issueNum": 1808,
    "pr": "Can open PR here (RowDelta API still needed)",
    "owner": "",
    "reviewed": "Closed as not planned (bot)",
    "status": "Closed",
    "version": "V2",
    "morCow": "MoR",
    "maintenanceRelationship": "Support producing position deletes on updates. Needs RowDelta API.",
    "comment": "Important for MOR/delete-heavy workloads, but skipping in favor of V3 DVs. Similar to #1078."
  },
  {
    "id": 15,
    "category": "MoR",
    "topic": "MoR for Deletes, high-level feature for #1808",
    "issue": "[Feat] Support Merge-on-Read mode for Deletes · Issue #1078 · apache/iceberg-python",
    "issueNum": 1078,
    "pr": "Can open PR here",
    "owner": "",
    "reviewed": "No",
    "status": "Open",
    "version": "V2/V3",
    "morCow": "MoR",
    "maintenanceRelationship": "High level strategy to support Merge-on-Read writes.",
    "comment": "Seems to be V2-focused, may be skipped potentially for V3 DVs. Similar to #1808."
  },
  {
    "id": 16,
    "category": "Compact data files, Delete compaction, MoR",
    "topic": "Delete file compaction (like Java's RewritePositionDeleteFiles)",
    "issue": "Can open issue here",
    "issueNum": 0,
    "pr": "",
    "owner": "",
    "reviewed": "",
    "status": "Planned",
    "version": "V2",
    "morCow": "MoR",
    "maintenanceRelationship": "Optimizes delete file reads by compacting small positional delete files.",
    "comment": "Future milestone following standard Java class RewritePositionDeleteFiles."
  },
  {
    "id": 17,
    "category": "Compact data files, MoR",
    "topic": "Equality delete conversion",
    "issue": "Equality Delete support · Issue #3270 · apache/iceberg-python",
    "issueNum": 3270,
    "pr": "https://github.com/apache/iceberg-python/pull/3285",
    "owner": "rambleraptor",
    "reviewed": "No",
    "status": "Open",
    "version": "V2",
    "morCow": "MoR",
    "maintenanceRelationship": "Needed for interop. Compaction converts Flink-written heavy V2 Equality Deletes to Position Deletes or DVs.",
    "comment": "This is needed while #1808 isn't because other streaming engines (like Flink) write V2 equality deletes, which PyIceberg must process."
  },
  {
    "id": 18,
    "category": "Deletion vectors",
    "topic": "Deletion vector read support",
    "issue": "Support Deletion Vectors #1549 - apache/iceberg-python",
    "issueNum": 1549,
    "pr": "https://github.com/apache/iceberg-python/pull/1516",
    "owner": "Fokko",
    "reviewed": "N/A",
    "status": "Merged",
    "version": "V3",
    "morCow": "MoR",
    "maintenanceRelationship": "Permits reading Iceberg V3 tables containing highly efficient Puffin-based deletion bitmaps.",
    "comment": "Fully merged."
  },
  {
    "id": 19,
    "category": "Deletion vectors",
    "topic": "Deletion vector write support",
    "issue": "Iceberg Deletion Vector Write Support · Issue #2261 · apache/iceberg-python",
    "issueNum": 2261,
    "pr": "https://github.com/apache/iceberg-python/pull/2822",
    "owner": "rambleraptor",
    "reviewed": "No",
    "status": "Closed",
    "version": "V3",
    "morCow": "MoR",
    "maintenanceRelationship": "Generates optimized deletion vectors on updates rather than heavy positional files.",
    "comment": "Closed as not planned by bot. Needs a fresh community revival."
  },
  {
    "id": 20,
    "category": "Rewrite manifests",
    "topic": "Support metadata compaction (RewriteManifests, manifest rewrite/merge)",
    "issue": "Support metadata compaction · Issue #270 · apache/iceberg-python",
    "issueNum": 270,
    "pr": "https://github.com/apache/iceberg-python/pull/1661",
    "owner": "amitgilad3",
    "reviewed": "No",
    "status": "Closed",
    "version": "V1/V2",
    "morCow": "CoW/MoR",
    "maintenanceRelationship": "Rewrites partition manifest lists to merge fragmented structures, optimizing plan scan speeds.",
    "comment": "Looks abandoned, needs to be reopened."
  },
  {
    "id": 21,
    "category": "All maintenance",
    "topic": "Table maintenance tasks",
    "issue": "[feat] Table maintenance tasks · Issue #1065 · apache/iceberg-python",
    "issueNum": 1065,
    "pr": "",
    "owner": "",
    "reviewed": "",
    "status": "Closed",
    "version": "V1/V2",
    "morCow": "CoW/MoR",
    "maintenanceRelationship": "High level feature tracking all table performance optimizations.",
    "comment": "Closed as not planned by bot."
  },
  {
    "id": 22,
    "category": "All maintenance",
    "topic": "Table maintenance tasks",
    "issue": "Support to optimize, analyze tables and expire snapshots, remove orphan files · Issue #31 · apache/iceberg-python",
    "issueNum": 31,
    "pr": "#516, #1199, #1200, #1092, #270",
    "owner": "",
    "reviewed": "",
    "status": "Closed",
    "version": "V1/V2",
    "morCow": "CoW/MoR",
    "maintenanceRelationship": "Central coordination tracking all consolidation tasks.",
    "comment": "Closed as not planned by bot."
  },
  {
    "id": 23,
    "category": "Roadmap",
    "topic": "PyIceberg Near-Term Roadmap",
    "issue": "PyIceberg Near-Term Roadmap · Issue #736 · apache/iceberg-python",
    "issueNum": 736,
    "pr": "",
    "owner": "",
    "reviewed": "",
    "status": "Open",
    "version": "V1/V2/V3",
    "morCow": "CoW/MoR",
    "maintenanceRelationship": "Overall development coordination roadmap.",
    "comment": "Primary near-term roadmap."
  },
  {
    "id": 24,
    "category": "V3",
    "topic": "V3 Tracking Issue",
    "issue": "V3 Tracking issue · Issue #1818 · apache/iceberg-python",
    "issueNum": 1818,
    "pr": "",
    "owner": "",
    "reviewed": "",
    "status": "Open",
    "version": "V3",
    "morCow": "CoW/MoR",
    "maintenanceRelationship": "Coordinating epic tracking full spec parity for the upcoming V3 specification.",
    "comment": "V3 Tracking issue."
  },
  {
    "id": 25,
    "category": "MaintenanceTable",
    "topic": "Consolidate snapshot expiration into MaintenanceTable",
    "issue": "refactor: consolidate snapshot expiration into MaintenanceTable · Issue #2142 · apache/iceberg-python",
    "issueNum": 2142,
    "pr": "https://github.com/apache/iceberg-python/pull/2143",
    "owner": "ForeverAngry",
    "reviewed": "N/A",
    "status": "Merged",
    "version": "V1/V2",
    "morCow": "CoW/MoR",
    "maintenanceRelationship": "Consolidates maintenance operations into a concrete MaintenanceTable class structure.",
    "comment": "Fully merged."
  },
  {
    "id": 26,
    "category": "Upsert",
    "topic": "Upsert is slow",
    "issue": "Upsert with 1M rows extremely slow due to `create_match_filter` and `txn.delete()` performance · Issue #3129 · apache/iceberg-python",
    "issueNum": 3129,
    "pr": "https://github.com/apache/iceberg-python/pull/2943",
    "owner": "EnyMan",
    "reviewed": "No",
    "status": "Closed",
    "version": "V1/V2",
    "morCow": "CoW",
    "maintenanceRelationship": "Identifies slow filter creation during massive transactional delete operations.",
    "comment": "Closed as not planned by bot."
  },
  {
    "id": 27,
    "category": "Upsert",
    "topic": "Discussion on Upsert",
    "issue": "Upsert in PyIceberg: Use Cases, Trade Offs, and Strategy · apache iceberg-python · Discussion #3118 · GitHub",
    "issueNum": 3118,
    "pr": "",
    "owner": "",
    "reviewed": "",
    "status": "Open",
    "version": "V1/V2",
    "morCow": "CoW/MoR",
    "maintenanceRelationship": "Community analysis defining PyIceberg upsert trade-offs and future development directions.",
    "comment": "Ongoing strategy analysis."
  },
  {
    "id": 28,
    "category": "Upsert",
    "topic": "Upsert is slow",
    "issue": "Upserting large table extremely slow · Issue #2159 · apache/iceberg-python",
    "issueNum": 2159,
    "pr": "",
    "owner": "",
    "reviewed": "",
    "status": "Open",
    "version": "V1/V2",
    "morCow": "CoW",
    "maintenanceRelationship": "Tracks general engine slowness during wide upsert writes.",
    "comment": "Active open issue."
  }
];

// Architectural boundaries guidance mapping
const BOUNDARY_MAPPING = {
  "API": "API Boundary: Banned Jackson annotations inside core. New public interface methods must include default implementations. Changes affect Flink, Spark, and all engines. Highly sensitive area.",
  "Core": "Core Engine Boundary: Table spec implementation. Must be engine-agnostic. No Spark/Flink references. All changes must apply consistently across catalogs.",
  "Data": "Data Layer Boundary: DeleteFilter, readers, and writers. Maintain high performance and clean memory structures inside Arrow buffers.",
  "REST": "REST catalog spec: Precision in open-api interop. All mutations must compile to serializable MetadataUpdate structures.",
  "Scan": "Scan planning: Metrics must not leak across TableScan refinements. Ensure absolute thread-safety during parallel manifest scanning."
};

// Rigorous mathematical safety rules dependency maps
const DEPENDENCY_MAP = {
  5: [6],      // Support data files compaction -> REPLACE operation
  6: [13],     // REPLACE operation -> Add commit retry validation
  13: [8, 9, 10, 11, 12], // Add commit retry validation -> validation_history, validateDeletedDataFiles, validateAddedDataFiles, etc.
  16: [17],    // Delete file compaction -> Equality delete conversion
  17: [6],     // Equality delete conversion -> REPLACE operation
  19: [18, 17] // Deletion vector write support -> Deletion vector read support, Equality delete conversion
};

// Chronological sequence swimlane mappings
const SEQUENCE_LANES = [
  {
    title: "1. Concurrency & OCC Validation",
    ids: [7, 8, 9, 10, 11, 12, 13]
  },
  {
    title: "2. Data Compaction & REPLACE",
    ids: [6, 5, 20, 25]
  },
  {
    title: "3. Delete Compaction & MoR",
    ids: [17, 14, 15, 16]
  },
  {
    title: "4. Deletion Vectors & Spec V3",
    ids: [18, 19, 23, 24]
  },
  {
    title: "5. Housekeeping & Performance Ops",
    ids: [1, 2, 3, 4, 21, 22, 26, 27, 28]
  }
];

// Application state variables
let currentDatabase = [];
let searchQuery = "";
let selectedCategory = "all";
let selectedStatus = "all";
let selectedSpec = "all";
let selectedNodeId = null;
let editingItemId = null;

// Initial bootstrap entry point
document.addEventListener("DOMContentLoaded", () => {
  if (window.lucide) window.lucide.createIcons();
  
  // Connect with Workspace Directory engine
  WorkspaceSync.initUI("ws-sync-container", 
    async (dirHandle) => {
      // Connect Workspace File Callback
      try {
        const fileContent = await WorkspaceSync.readWorkspaceFile(dirHandle, "maintenance-roadmap.json");
        if (fileContent && Array.isArray(fileContent)) {
          currentDatabase = fileContent;
          console.log("[WorkspaceSync] roadmap data hydrated directly from maintenance-roadmap.json");
        } else {
          loadFallback();
        }
      } catch (err) {
        console.warn("[WorkspaceSync] maintenance-roadmap.json not found, initializing with defaults.");
        currentDatabase = JSON.parse(JSON.stringify(PRELOADED_DATABASE));
        try {
          await WorkspaceSync.writeWorkspaceFile(dirHandle, "maintenance-roadmap.json", currentDatabase);
        } catch (writeErr) {
          console.error("[WorkspaceSync] Failed to write initial database to workspace", writeErr);
        }
      }
      localStorage.setItem("icod-maintenance-roadmap", JSON.stringify(currentDatabase));
      initializeUI();
    },
    () => {
      // Disconnect Callback
      loadFallback();
      initializeUI();
    }
  );

  // Initialize general search in page header
  const globalSearch = document.getElementById("global-search");
  if (globalSearch) {
    globalSearch.addEventListener("input", (e) => {
      searchQuery = e.target.value.toLowerCase().trim();
      const localSearch = document.getElementById("roadmap-search");
      if (localSearch) localSearch.value = e.target.value;
      renderUI();
    });
  }

  // Set up event listeners for filters
  const localSearchInput = document.getElementById("roadmap-search");
  if (localSearchInput) {
    localSearchInput.addEventListener("input", (e) => {
      searchQuery = e.target.value.toLowerCase().trim();
      const globSearch = document.getElementById("global-search");
      if (globSearch) globSearch.value = e.target.value;
      renderUI();
    });
  }

  const filterCategorySelect = document.getElementById("filter-category");
  if (filterCategorySelect) {
    filterCategorySelect.addEventListener("change", (e) => {
      selectedCategory = e.target.value;
      renderUI();
    });
  }

  const filterStatusSelect = document.getElementById("filter-status");
  if (filterStatusSelect) {
    filterStatusSelect.addEventListener("change", (e) => {
      selectedStatus = e.target.value;
      renderUI();
    });
  }

  const filterSpecSelect = document.getElementById("filter-spec");
  if (filterSpecSelect) {
    filterSpecSelect.addEventListener("change", (e) => {
      selectedSpec = e.target.value;
      renderUI();
    });
  }

  // Set up sync button click
  const syncBtn = document.getElementById("btn-sync-github");
  if (syncBtn) {
    syncBtn.addEventListener("click", () => {
      syncWithGitHub();
    });
  }

  // Set up drawer closes
  const drawerCloseBtn = document.getElementById("drawer-close-btn");
  if (drawerCloseBtn) {
    drawerCloseBtn.addEventListener("click", closeDrawer);
  }
  const overlay = document.getElementById("drawer-overlay");
  if (overlay) {
    overlay.addEventListener("click", closeDrawer);
  }

  // Set up save button
  const saveBtn = document.getElementById("drawer-save-btn");
  if (saveBtn) {
    saveBtn.addEventListener("click", saveDrawerEdits);
  }
});

// Load standard browser fallback databases
function loadFallback() {
  const saved = localStorage.getItem("icod-maintenance-roadmap");
  if (saved) {
    try {
      currentDatabase = JSON.parse(saved);
      console.log("[WorkspaceSync] Hydrated from localStorage cache");
    } catch (e) {
      currentDatabase = JSON.parse(JSON.stringify(PRELOADED_DATABASE));
    }
  } else {
    currentDatabase = JSON.parse(JSON.stringify(PRELOADED_DATABASE));
  }
}

// Hydrate filter dropdown values dynamically and draw UI elements
function initializeUI() {
  populateCategoriesDropdown();
  renderUI();
}

function populateCategoriesDropdown() {
  const filterCat = document.getElementById("filter-category");
  if (!filterCat) return;

  // Extract unique sorted categories
  const categories = Array.from(new Set(currentDatabase.map(item => item.category))).sort();
  
  // Clear previous options and restore default
  filterCat.innerHTML = `<option value="all">All Categories</option>`;
  
  categories.forEach(cat => {
    const option = document.createElement("option");
    option.value = cat;
    option.innerText = cat;
    filterCat.appendChild(option);
  });
}

// Render both the matrix table and the sequence swimlanes
function renderUI() {
  renderAuditTable();
  renderSequenceSwimlanes();
  updateSpecStatsBar();
  if (window.lucide) window.lucide.createIcons();
}

// Render the main operations audit matrix table
function renderAuditTable() {
  const tableBody = document.getElementById("roadmap-audit-body");
  if (!tableBody) return;
  tableBody.innerHTML = "";

  const filtered = currentDatabase.filter(item => {
    // General text search matching category, topic, issue, owner, comment
    const text = `${item.category} ${item.topic} ${item.issue} ${item.owner} ${item.comment} ${item.pr}`.toLowerCase();
    if (searchQuery && !text.includes(searchQuery)) return false;

    // Filter categories
    if (selectedCategory !== "all" && item.category !== selectedCategory) return false;

    // Filter statuses
    if (selectedStatus !== "all" && item.status !== selectedStatus) return false;

    // Filter specs
    if (selectedSpec !== "all" && item.version !== selectedSpec) return false;

    return true;
  });

  if (filtered.length === 0) {
    tableBody.innerHTML = `
      <tr>
        <td colspan="6" style="padding: 32px; text-align: center; color: var(--g500); font-family: var(--mono); font-size: 11px;">
          No operational assets match active filter criteria.
        </td>
      </tr>
    `;
    return;
  }

  filtered.forEach(item => {
    const row = document.createElement("tr");
    row.className = "audit-row";
    row.setAttribute("data-id", item.id);
    if (selectedNodeId === item.id) {
      row.classList.add("selected-row");
    }

    const statusBadgeClass = item.status.toLowerCase();

    row.innerHTML = `
      <td style="padding: 12px 14px; font-weight: 600; color: var(--slate);">${item.category}</td>
      <td style="padding: 12px 14px; font-family: var(--sans); color: var(--clay-d); font-weight: 500;">${item.topic}</td>
      <td style="padding: 12px 14px; line-height: 1.4;">
        <div style="font-weight: 600; color: var(--slate);">${item.issueNum ? '#' + item.issueNum : ''} ${item.topic}</div>
        <div style="font-size: 10.5px; color: var(--g500); max-width: 320px; white-space: nowrap; text-overflow: ellipsis; overflow: hidden;" title="${item.issue}">
          ${item.issue}
        </div>
      </td>
      <td style="padding: 12px 14px; font-family: var(--mono); color: var(--g700); font-weight: 500;">${item.owner || "Unassigned"}</td>
      <td style="padding: 12px 14px; text-align: center;">
        <span class="chip" style="font-size: 10px; padding: 2px 6px; font-family: var(--mono); font-weight: 600; background: var(--g50);">${item.version}</span>
      </td>
      <td style="padding: 12px 14px; text-align: center;">
        <span class="node-status-badge ${statusBadgeClass}">${item.status}</span>
      </td>
    `;

    row.addEventListener("click", () => {
      selectNode(item.id);
    });

    tableBody.appendChild(row);
  });
}

// Check if a sequence node is blocked based on upstream prerequisites
function isNodeBlocked(itemId) {
  const prereqs = DEPENDENCY_MAP[itemId] || [];
  for (let pId of prereqs) {
    const pItem = currentDatabase.find(i => i.id === pId);
    if (pItem && pItem.status !== "Merged") {
      return true;
    }
  }
  return false;
}

// Recursive helpers for graph visualizer tracing
function getRecursivePrerequisites(id, visited = new Set()) {
  if (visited.has(id)) return [];
  visited.add(id);
  const prereqs = DEPENDENCY_MAP[id] || [];
  const recursive = [...prereqs];
  for (const pId of prereqs) {
    recursive.push(...getRecursivePrerequisites(pId, visited));
  }
  return Array.from(new Set(recursive));
}

function getRecursiveDependees(targetId) {
  const dependees = [];
  for (const id in DEPENDENCY_MAP) {
    const prereqs = getRecursivePrerequisites(parseInt(id));
    if (prereqs.includes(targetId)) {
      dependees.push(parseInt(id));
    }
  }
  return Array.from(new Set(dependees));
}

// Render the interactive timeline lane chart
function renderSequenceSwimlanes() {
  const container = document.getElementById("sequence-graph");
  if (!container) return;
  container.innerHTML = "";

  SEQUENCE_LANES.forEach((laneInfo) => {
    // Determine lane visual state based on node items
    let laneClass = "sequence-lane";
    const laneItems = currentDatabase.filter(i => laneInfo.ids.includes(i.id));
    const allMerged = laneItems.length > 0 && laneItems.every(i => i.status === "Merged");
    const hasActive = laneItems.some(i => i.status === "Open" || i.status === "In-progress");
    
    if (allMerged) {
      laneClass += " merged";
    } else if (hasActive) {
      laneClass += " active";
    }

    const lane = document.createElement("div");
    lane.className = laneClass;

    lane.innerHTML = `
      <div class="sequence-lane-title">
        <span class="sequence-lane-title-dot"></span>
        <span>${laneInfo.title}</span>
      </div>
      <div class="sequence-nodes-grid" id="lane-grid-${laneInfo.title.replace(/\s+/g, '-')}"></div>
    `;

    container.appendChild(lane);

    const grid = lane.querySelector(".sequence-nodes-grid");
    
    laneItems.forEach(item => {
      const isBlocked = isNodeBlocked(item.id);
      
      let nodeClass = "sequence-node";
      if (item.status === "Merged") nodeClass += " merged";
      else if (isBlocked) nodeClass += " blocked";
      else if (item.status === "Closed") nodeClass += " closed";
      
      if (selectedNodeId === item.id) {
        nodeClass += " active-focus";
      }

      // Add temporary highlight modifiers during active tracing
      if (selectedNodeId !== null && selectedNodeId !== item.id) {
        const prereqs = getRecursivePrerequisites(selectedNodeId);
        const dependees = getRecursiveDependees(selectedNodeId);
        if (prereqs.includes(item.id)) {
          nodeClass += " prereq-highlight";
        } else if (dependees.includes(item.id)) {
          nodeClass += " dependee-highlight";
        } else {
          nodeClass += " faded-node";
        }
      }

      const statusClass = item.status.toLowerCase();

      const node = document.createElement("div");
      node.className = nodeClass;
      node.setAttribute("data-id", item.id);

      node.innerHTML = `
        <div class="node-info">
          <div class="node-title" title="${item.topic}">${item.topic}</div>
          <div class="node-subtitle">${item.issueNum ? '#' + item.issueNum : 'No Issue'}</div>
        </div>
        <div class="node-status-badge ${statusClass}">${item.status}</div>
      `;

      node.addEventListener("click", () => {
        selectNode(item.id);
      });

      grid.appendChild(node);
    });
  });
}

// Sync the top stat status counters
function updateSpecStatsBar() {
  const specVersion = document.getElementById("navbar-spec-version");
  const snapshotId = document.getElementById("navbar-snapshot-id");
  const seqNumber = document.getElementById("navbar-sequence-number");

  if (specVersion) {
    const total = currentDatabase.length;
    const merged = currentDatabase.filter(i => i.status === "Merged").length;
    specVersion.innerText = `Parity: ${Math.round((merged / total) * 100)}% (${merged}/${total} Merged)`;
  }

  if (snapshotId) {
    const openCount = currentDatabase.filter(i => i.status === "Open" || i.status === "In-progress").length;
    snapshotId.innerText = `${openCount} Active Roadblocks`;
  }

  if (seqNumber) {
    const v3Count = currentDatabase.filter(i => i.version === "V3").length;
    seqNumber.innerText = `${v3Count} Spec-V3 Items`;
  }
}

// Select a node, opening its drawer and applying dependency highlighting
function selectNode(id) {
  selectedNodeId = id;
  renderUI(); // updates active highlights in table and sequence lanes
  openDrawerFor(id);
}

// ── Committer Reviewer Drawer Controller ──

function openDrawerFor(id) {
  const item = currentDatabase.find(i => i.id === id);
  if (!item) return;

  editingItemId = id;

  // Hydrate text nodes
  document.getElementById("drawer-asset-title").innerText = item.issueNum ? `GH-${item.issueNum}: ${item.topic}` : item.topic;
  document.getElementById("drawer-category").innerText = item.category;
  document.getElementById("drawer-topic").innerText = item.topic;
  document.getElementById("drawer-spec").innerText = item.version;
  document.getElementById("drawer-morcow").innerText = item.morCow;
  document.getElementById("drawer-owner").innerText = item.owner || "No Owner Assigned";
  document.getElementById("drawer-maintenance-relationship").innerText = item.maintenanceRelationship || "No direct maintenance relationship mapped.";

  // Set boundary notes dynamically based on topics/keywords
  const boundaryNote = document.getElementById("drawer-boundary-note");
  let guide = BOUNDARY_MAPPING["REST"];
  const catLower = item.category.toLowerCase();
  const topicLower = item.topic.toLowerCase();
  
  if (catLower.includes("api") || topicLower.includes("api") || topicLower.includes("spec") || topicLower.includes("v3")) {
    guide = BOUNDARY_MAPPING["API"];
  } else if (catLower.includes("concurrency") || topicLower.includes("retry") || topicLower.includes("validation")) {
    guide = BOUNDARY_MAPPING["Scan"];
  } else if (catLower.includes("compaction") || topicLower.includes("replace") || catLower.includes("manifest")) {
    guide = BOUNDARY_MAPPING["Core"];
  } else if (catLower.includes("vector") || catLower.includes("delete")) {
    guide = BOUNDARY_MAPPING["Data"];
  }
  boundaryNote.innerText = guide;

  // Math equations safety formulas mapper
  const mathSection = document.getElementById("drawer-math-section");
  const formulaElement = document.getElementById("drawer-math-formula");
  const descElement = document.getElementById("drawer-math-desc");

  const math = getMathFormula(item);
  if (math) {
    mathSection.style.display = "block";
    formulaElement.innerText = math.formula;
    descElement.innerText = math.desc;
  } else {
    mathSection.style.display = "none";
  }

  // GitHub Link parsing
  const gitLink = document.getElementById("drawer-github-link");
  if (item.pr && item.pr.startsWith("http")) {
    gitLink.href = item.pr;
    gitLink.style.display = "inline-flex";
  } else if (item.issueNum) {
    gitLink.href = `https://github.com/apache/iceberg-python/issues/${item.issueNum}`;
    gitLink.style.display = "inline-flex";
  } else {
    gitLink.href = "https://github.com/apache/iceberg-python";
    gitLink.style.display = "inline-flex";
  }

  // Inputs binding
  document.getElementById("drawer-input-reviewed").value = item.reviewed || "No";
  document.getElementById("drawer-input-status").value = item.status || "Open";
  document.getElementById("drawer-input-comment").value = item.comment || "";

  // Show drawer overlay and drawer
  document.getElementById("drawer-overlay").classList.add("open");
  document.getElementById("reviewer-drawer").classList.add("open");
}

function closeDrawer() {
  document.getElementById("drawer-overlay").classList.remove("open");
  document.getElementById("reviewer-drawer").classList.remove("open");
  selectedNodeId = null;
  editingItemId = null;
  renderUI(); // Clears all dependency highlights
}

function getMathFormula(item) {
  const category = item.category.toLowerCase();
  const topic = item.topic.toLowerCase();
  
  if (topic.includes("expire") || category.includes("expire")) {
    return {
      formula: "H_snapshots <= K_retention_limit",
      desc: "Metadata cleanup ceiling. Snapshots list is constrained beneath threshold K to keep scan planing complexity strictly O(1)."
    };
  }
  if (topic.includes("orphan") || category.includes("orphan")) {
    return {
      formula: "S_physical \\ S_logical = EmptySet",
      desc: "Storage invariant verification. Verifies that the set difference of raw storage files against referenced logical snapshots yields nothing."
    };
  }
  if (category.includes("retry") || topic.includes("concurrency") || topic.includes("validation")) {
    return {
      formula: "S_transaction ^ S_concurrent = EmptySet",
      desc: "Optimistic Concurrency Control retry proof. Guarantees that parallel committed datasets do not overlap with active transaction states."
    };
  }
  if (topic.includes("compaction") || category.includes("compact")) {
    return {
      formula: "N_files_post << N_files_pre  (s.t. Sum Size_post = Sum Size_pre)",
      desc: "Write-amplification compaction formula. Consolidates buffers into larger sequential blocks, reducing scan overhead while conserving bytes."
    };
  }
  if (topic.includes("mor") || category.includes("mor") || topic.includes("delete")) {
    return {
      formula: "S_data < S_delete",
      desc: "Merge-on-Read write optimization equation. Fast writes at the cost of slight query read amplification, requiring eventual background compaction."
    };
  }
  if (category.includes("vector") || topic.includes("vector")) {
    return {
      formula: "DV(x) in {0, 1}^R_bitmap",
      desc: "V3 Point delete indexing equation. Maps rows to bitwise Roaring Bitmaps stored inside Puffin file buffers, allowing point-delete tests in O(1)."
    };
  }
  return null;
}

// Save edits back to workspace (if connected) or fall back to localStorage
async function saveDrawerEdits() {
  if (editingItemId === null) return;

  const itemIndex = currentDatabase.findIndex(i => i.id === editingItemId);
  if (itemIndex === -1) return;

  const item = currentDatabase[itemIndex];
  
  // Read inputs from DOM elements
  item.reviewed = document.getElementById("drawer-input-reviewed").value;
  item.status = document.getElementById("drawer-input-status").value;
  item.comment = document.getElementById("drawer-input-comment").value;

  // Persist directly to workspace directory file if picker is active
  if (WorkspaceSync.activeHandle) {
    try {
      await WorkspaceSync.writeWorkspaceFile(WorkspaceSync.activeHandle, "maintenance-roadmap.json", currentDatabase);
      console.log("[WorkspaceSync] Correctly saved matrix state back to workspace directory file!");
    } catch (e) {
      console.error("[WorkspaceSync] Error saving data back to disk", e);
      alert("Failed to write to local directory. Falling back to browser cache.");
    }
  }

  // Backup to localStorage
  localStorage.setItem("icod-maintenance-roadmap", JSON.stringify(currentDatabase));

  closeDrawer();
  renderUI();
}

// ── GitHub REST API Async Poller ──

async function syncWithGitHub() {
  const syncBtn = document.getElementById("btn-sync-github");
  if (!syncBtn) return;

  const originalHTML = syncBtn.innerHTML;
  syncBtn.disabled = true;
  syncBtn.innerHTML = `<i data-lucide="refresh-cw" class="spinner" style="width: 14px; height: 14px;"></i> <span>Polling GitHub...</span>`;
  if (window.lucide) window.lucide.createIcons();

  // Find all items with active issue numbers that are open or planned
  const activeItems = currentDatabase.filter(item => item.issueNum > 0 && (item.status === "Open" || item.status === "Planned" || item.status === "In-progress"));

  if (activeItems.length === 0) {
    alert("Committer Operations Console: No open or planned GitHub assets require polling.");
    syncBtn.disabled = false;
    syncBtn.innerHTML = originalHTML;
    if (window.lucide) window.lucide.createIcons();
    return;
  }

  let updatedCount = 0;

  // Query GitHub REST API concurrently for all active items
  const syncPromises = activeItems.map(async (item) => {
    try {
      // Use issues endpoint as it covers both PRs and standard Issues
      const response = await fetch(`https://api.github.com/repos/apache/iceberg-python/issues/${item.issueNum}`);
      if (!response.ok) return;

      const ghData = await response.json();
      
      let newStatus = item.status;
      if (ghData.state === "closed") {
        // If it is a pull request, check if it was merged rather than just closed
        if (ghData.pull_request) {
          const prRes = await fetch(`https://api.github.com/repos/apache/iceberg-python/pulls/${item.issueNum}`);
          if (prRes.ok) {
            const prData = await prRes.json();
            newStatus = prData.merged ? "Merged" : "Closed";
          } else {
            newStatus = "Closed";
          }
        } else {
          newStatus = "Closed";
        }
      } else if (ghData.state === "open") {
        // Maintain active focus
        newStatus = item.status === "Planned" ? "Open" : item.status;
      }

      if (newStatus !== item.status) {
        item.status = newStatus;
        updatedCount++;
      }
    } catch (e) {
      console.warn(`[GitHubSync] Failed to poll status for asset GH-${item.issueNum}`, e);
    }
  });

  await Promise.all(syncPromises);

  if (updatedCount > 0) {
    // Persist changes back to workspace and cache
    if (WorkspaceSync.activeHandle) {
      try {
        await WorkspaceSync.writeWorkspaceFile(WorkspaceSync.activeHandle, "maintenance-roadmap.json", currentDatabase);
      } catch (e) {
        console.error("[WorkspaceSync] Failed to write GitHub sync updates to folder", e);
      }
    }
    localStorage.setItem("icod-maintenance-roadmap", JSON.stringify(currentDatabase));
    
    renderUI();
    alert(`Committer Operations Deck: Synchronized ${updatedCount} assets with live GitHub state!`);
  } else {
    alert("Committer Operations Deck: GitHub Sync complete. All active open assets are in parity.");
  }

  syncBtn.disabled = false;
  syncBtn.innerHTML = originalHTML;
  if (window.lucide) window.lucide.createIcons();
}
