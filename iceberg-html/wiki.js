// Shared JavaScript Component for The Iceberg Cookbook Wiki
// Enabling dynamic directory-based synchronization with local filesystem

// ── IndexedDB Directory Handle Persistence ──
const DB_NAME = "WikiWorkspaceDB";
const STORE_NAME = "handles";
const KEY_NAME = "dirHandle";

function getDB() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, 1);
    request.onupgradeneeded = (e) => {
      e.target.result.createObjectStore(STORE_NAME);
    };
    request.onsuccess = (e) => resolve(e.target.result);
    request.onerror = (e) => reject(e.target.error);
  });
}

async function saveHandle(handle) {
  const db = await getDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, "readwrite");
    const store = tx.objectStore(STORE_NAME);
    const request = store.put(handle, KEY_NAME);
    request.onsuccess = () => resolve();
    request.onerror = () => reject(request.error);
  });
}

async function loadHandle() {
  const db = await getDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, "readonly");
    const store = tx.objectStore(STORE_NAME);
    const request = store.get(KEY_NAME);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

async function clearHandle() {
  const db = await getDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, "readwrite");
    const store = tx.objectStore(STORE_NAME);
    const request = store.delete(KEY_NAME);
    request.onsuccess = () => resolve();
    request.onerror = () => reject(request.error);
  });
}

// ── File Reading & Writing Global Helpers ──
window.wikiDirHandle = null;
window.wikiPendingHandle = null;

const DATA_FOLDER = "wiki-data";

async function getDataDirectoryHandle(create = false) {
  if (!window.wikiDirHandle) return null;
  try {
    return await window.wikiDirHandle.getDirectoryHandle(DATA_FOLDER, { create });
  } catch (e) {
    if (create) {
      console.error(`Failed to get or create directory handle for ${DATA_FOLDER}:`, e);
    }
    return null;
  }
}

async function migrateRootFilesToSubfolder() {
  if (!window.wikiDirHandle) return;
  try {
    const filesToMigrate = ["wiki-todo-state.json", "wiki-notes.json"];
    let hasFilesToMigrate = false;
    for (const filename of filesToMigrate) {
      try {
        await window.wikiDirHandle.getFileHandle(filename);
        hasFilesToMigrate = true;
        break;
      } catch (e) {
        // File does not exist at root, no need to migrate it
      }
    }

    if (!hasFilesToMigrate) return;

    // Create subfolder
    const subFolder = await window.wikiDirHandle.getDirectoryHandle(DATA_FOLDER, { create: true });

    for (const filename of filesToMigrate) {
      try {
        const rootFileHandle = await window.wikiDirHandle.getFileHandle(filename);
        const file = await rootFileHandle.getFile();
        const text = await file.text();
        const data = JSON.parse(text);

        // Write to subfolder
        const subFileHandle = await subFolder.getFileHandle(filename, { create: true });
        const writable = await subFileHandle.createWritable();
        await writable.write(JSON.stringify(data, null, 2));
        await writable.close();
        console.log(`Successfully migrated ${filename} to ${DATA_FOLDER}/`);

        // Remove from root
        try {
          await window.wikiDirHandle.removeEntry(filename);
          console.log(`Removed old root file: ${filename}`);
        } catch (deleteError) {
          console.error(`Failed to delete old root file ${filename}:`, deleteError);
        }
      } catch (e) {
        // File did not exist at root or was already migrated
      }
    }
  } catch (e) {
    console.error("Migration to subfolder failed:", e);
  }
}

window.readWikiFile = async function (filename) {
  if (!window.wikiDirHandle) return null;
  try {
    const dirHandle = await getDataDirectoryHandle(false);
    if (!dirHandle) return null;
    const fileHandle = await dirHandle.getFileHandle(filename);
    const file = await fileHandle.getFile();
    const text = await file.text();
    return JSON.parse(text);
  } catch (e) {
    return null;
  }
};

window.writeWikiFile = async function (filename, data) {
  if (!window.wikiDirHandle) return false;
  try {
    const dirHandle = await getDataDirectoryHandle(true);
    if (!dirHandle) return false;
    const fileHandle = await dirHandle.getFileHandle(filename, { create: true });
    const writable = await fileHandle.createWritable();
    await writable.write(JSON.stringify(data, null, 2));
    await writable.close();
    return true;
  } catch (e) {
    console.error("Failed to write wiki file:", e);
    return false;
  }
};

async function syncTasksOnConnection() {
  if (!window.wikiDirHandle) return;
  try {
    const diskTasks = await window.readWikiFile("wiki-todo-state.json");
    if (diskTasks) {
      localStorage.setItem("wiki-todo-state", JSON.stringify(diskTasks));
    } else {
      const currentTasks = JSON.parse(localStorage.getItem("wiki-todo-state")) || [];
      if (currentTasks.length > 0) {
        await window.writeWikiFile("wiki-todo-state.json", currentTasks);
      }
    }
  } catch (e) {
    console.error("Failed to sync tasks:", e);
  }
}

async function syncNotesOnConnection() {
  if (!window.wikiDirHandle) return;
  try {
    const diskNotes = await window.readWikiFile("wiki-notes.json");
    if (diskNotes) {
      localStorage.setItem("wiki-notes-state", JSON.stringify(diskNotes));
    } else {
      const currentNotes = JSON.parse(localStorage.getItem("wiki-notes-state")) || {};
      await window.writeWikiFile("wiki-notes.json", currentNotes);
    }
  } catch (e) {
    console.error("Failed to sync notes:", e);
  }
}

// ── Floating Sync Pill State Updater ──
function updateFloatingSyncPill() {
  const pill = document.getElementById("floating-sync-indicator");
  if (!pill) return;

  let dotClass = "disconnected";
  let statusText = "Sync Off";
  let tooltipText = "Workspace sync disabled. Click to setup local directory.";
  
  if (window.wikiDirHandle) {
    dotClass = "synced";
    statusText = window.wikiDirHandle.name;
    tooltipText = `🟢 Synced with: ${window.wikiDirHandle.name}. Click to view workspace options.`;
  } else if (window.wikiPendingHandle) {
    dotClass = "pending";
    statusText = "Re-auth";
    tooltipText = `🟡 Action required: Click to grant permission to ${window.wikiPendingHandle.name}.`;
  }

  // Remove existing state classes to prevent class accumulation
  pill.classList.remove("synced", "pending", "disconnected");
  pill.classList.add(dotClass);

  pill.innerHTML = `
    <div class="floating-sync-icon-wrapper">
      <svg class="floating-sync-icon" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"></path>
      </svg>
      <span class="floating-sync-badge ${dotClass}"></span>
    </div>
    <span class="floating-sync-text">${statusText}</span>
  `;
  pill.title = tooltipText;
}

// ── DOM Main Content Initialization ──
document.addEventListener("DOMContentLoaded", async () => {
  const sidebar = document.querySelector("aside.wiki-sidebar");
  if (!sidebar) return;

  // Apply persisted layout & sidebar modes immediately
  const layoutContainer = document.querySelector(".wiki-layout");
  const savedLayout = localStorage.getItem("wiki-layout") || "standard";
  if (savedLayout === "wide") {
    document.body.classList.add("layout-wide");
  }
  const isSidebarCollapsed = localStorage.getItem("wiki-sidebar-collapsed") === "true";
  if (isSidebarCollapsed && layoutContainer) {
    layoutContainer.classList.add("sidebar-collapsed");
  }

  // Inject floating buttons dynamically to avoid cluttering static HTML files
  if (!document.getElementById("btn-sidebar-expand")) {
    const expandBtn = document.createElement("button");
    expandBtn.id = "btn-sidebar-expand";
    expandBtn.className = `sidebar-expand-btn ${isSidebarCollapsed ? 'visible' : ''}`;
    expandBtn.title = "Expand Sidebar";
    expandBtn.innerHTML = `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"></rect><line x1="9" y1="3" x2="9" y2="21"></line><path d="M13 9l3 3-3 3"></path></svg>`;
    document.body.appendChild(expandBtn);
  }

  if (!document.getElementById("btn-back-to-top")) {
    const topBtn = document.createElement("button");
    topBtn.id = "btn-back-to-top";
    topBtn.className = "back-to-top-btn";
    topBtn.title = "Back to Top";
    topBtn.innerHTML = `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="19" x2="12" y2="5"></line><polyline points="5 12 12 5 19 12"></polyline></svg>`;
    document.body.appendChild(topBtn);
  }

  if (!document.getElementById("floating-sync-indicator")) {
    const syncPill = document.createElement("div");
    syncPill.id = "floating-sync-indicator";
    syncPill.className = `floating-sync-pill ${isSidebarCollapsed ? 'visible' : ''}`;
    document.body.appendChild(syncPill);
    
    // Add click listener for quick actions on the floating pill
    syncPill.addEventListener("click", async () => {
      if (window.wikiPendingHandle) {
        // QUICK RE-AUTHORIZE
        try {
          const mode = 'readwrite';
          const permission = await window.wikiPendingHandle.requestPermission({ mode });
          if (permission === 'granted') {
            window.wikiDirHandle = window.wikiPendingHandle;
            window.wikiPendingHandle = null;
            await saveHandle(window.wikiDirHandle);
            await migrateRootFilesToSubfolder();
            await syncTasksOnConnection();
            await syncNotesOnConnection();
            renderSidebar();
            updateFloatingSyncPill();
            window.dispatchEvent(new CustomEvent("wiki-sync-status-changed"));
          }
        } catch (e) {
          console.error("Re-authorization failed:", e);
        }
      } else if (!window.wikiDirHandle) {
        // QUICK CONNECT
        try {
          const handle = await window.showDirectoryPicker({ mode: 'readwrite' });
          window.wikiDirHandle = handle;
          window.wikiPendingHandle = null;
          await saveHandle(handle);
          await migrateRootFilesToSubfolder();
          await syncTasksOnConnection();
          await syncNotesOnConnection();
          renderSidebar();
          updateFloatingSyncPill();
          window.dispatchEvent(new CustomEvent("wiki-sync-status-changed"));
        } catch (e) {
          console.error("Folder picker cancelled or failed:", e);
        }
      } else {
        // ALREADY SYNCED - EXPAND SIDEBAR FOR FULL METRICS/DISCONNECT
        const layoutContainer = document.querySelector(".wiki-layout");
        if (layoutContainer) {
          layoutContainer.classList.remove("sidebar-collapsed");
          const exp = document.getElementById("btn-sidebar-expand");
          if (exp) exp.classList.remove("visible");
          syncPill.classList.remove("visible");
          localStorage.setItem("wiki-sidebar-collapsed", "false");
        }
      }
    });
  }

  // Register listener to update the floating indicator reactively
  window.addEventListener("wiki-sync-status-changed", () => {
    updateFloatingSyncPill();
  });

  // Load and query directory handle from IndexedDB
  try {
    const saved = await loadHandle();
    if (saved) {
      const mode = 'readwrite';
      const permission = await saved.queryPermission({ mode });
      if (permission === 'granted') {
        window.wikiDirHandle = saved;
        await migrateRootFilesToSubfolder();
      } else {
        window.wikiPendingHandle = saved;
      }
    }
  } catch (e) {
    console.error("IndexedDB error loading handle:", e);
  }
  updateFloatingSyncPill();

  // 1. Define Hierarchical Navigation Schema (SWE Wiki Categories)
  const navSchema = [
    {
      category: "Invariants & State",
      items: [
        { name: "State Transitions", path: "index.html" }
      ]
    },
    {
      category: "Metadata Evolution",
      items: [
        { name: "Partition Evolution", path: "spec-evolution.html" }
      ]
    },
    {
      category: "Row-Level Operations",
      items: [
        { name: "Row Deletes & Actions", path: "row-deletes.html" }
      ]
    },
    {
      category: "Feature Cookbook",
      items: [
        { name: "Foundational State Model", path: "state-model.html" },
        { name: "Schema Evolution Mechanics", path: "evolution-mechanics.html" },
        { name: "Logical Row Projection", path: "row-projection.html" },
        { name: "COW vs MOR Profiles", path: "performance-profiles.html" },
        { name: "Transaction Semantics", path: "transaction-semantics.html" },
        { name: "Maintenance & Compaction", path: "maintenance-compaction.html" }
      ]
    },
    {
      category: "Audits & Parity",
      items: [
        { name: "Binding Parity Matrix", path: "audit-matrix.html" }
      ]
    },
    {
      category: "Wiki Roadmap",
      items: [
        { name: "Site Improvements", path: "todo.html" }
      ]
    }
  ];

  const currentPath = window.location.pathname.split("/").pop() || "index.html";

  // 2. Sidebar Redraw Function
  function renderSidebar() {
    let sidebarHTML = `
      <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 8px;">
        <div>
          <a href="index.html" class="sidebar-logo">The Iceberg <em>Cookbook</em></a>
          <div class="sidebar-tagline">DOCUMENTATION EXPLORER</div>
        </div>
        <button id="btn-sidebar-collapse" class="sidebar-toggle-btn" title="Collapse Sidebar">
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"></rect><line x1="9" y1="3" x2="9" y2="21"></line><path d="M16 15l-3-3 3-3"></path></svg>
        </button>
      </div>
      <div class="file-tree">
    `;

    const arrowSvg = `<svg class="category-arrow" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 18 15 12 9 6"></polyline></svg>`;
    const categorySvg = `<svg class="category-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"></path><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"></path></svg>`;
    const pageSvg = `<svg class="page-icon" width="8" height="8" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="4.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="8"></circle></svg>`;

    navSchema.forEach((group, index) => {
      const isCategoryActive = group.items.some(item => {
        return item.path === currentPath || 
               (currentPath === "" && item.path === "index.html") ||
               (currentPath === "iceberg-html" && item.path === "index.html");
      });

      sidebarHTML += `
        <div class="category-container">
          <div class="category-node ${isCategoryActive ? 'open' : ''}" data-category-index="${index}">
            ${arrowSvg}
            ${categorySvg}
            <span>${group.category}</span>
          </div>
          <ul class="category-pages">
      `;

      group.items.forEach(item => {
        const isActive = item.path === currentPath || 
                         (currentPath === "" && item.path === "index.html") ||
                         (currentPath === "iceberg-html" && item.path === "index.html");
                         
        sidebarHTML += `
          <li class="page-node ${isActive ? 'active' : ''}">
            <a href="${item.path}">
              ${pageSvg}
              <span>${item.name}</span>
            </a>
          </li>
        `;
      });

      sidebarHTML += `
          </ul>
        </div>
      `;
    });

    // Sync Panel Layout Builder
    let syncWidgetHTML = "";
    if (window.wikiDirHandle) {
      syncWidgetHTML = `
        <div style="display: flex; flex-direction: column; gap: 6px;">
          <div class="badge supported" style="font-size: 11px; padding: 6px 10px; width: 100%; justify-content: center; border-radius: 6px; border-color: var(--olive); text-transform: none; display: flex; align-items: center; gap: 6px;">
            <span>🟢 Synced: ${window.wikiDirHandle.name}</span>
          </div>
          <button id="btn-sync-action" class="btn" style="width: 100%; font-size: 11px; padding: 5px 8px; border-color: var(--clay); color: var(--clay); background: transparent; justify-content: center;">
            Disconnect Sync
          </button>
        </div>
      `;
    } else if (window.wikiPendingHandle) {
      syncWidgetHTML = `
        <div style="display: flex; flex-direction: column; gap: 6px;">
          <div class="badge partial" style="font-size: 11px; padding: 6px 10px; width: 100%; justify-content: center; border-radius: 6px; text-transform: none; display: flex; align-items: center; gap: 6px;">
            <span>🟡 Re-authorize: ${window.wikiPendingHandle.name}</span>
          </div>
          <button id="btn-sync-action" class="btn btn-clay" style="width: 100%; font-size: 11px; padding: 5px 8px; justify-content: center; gap: 6px;">
            Grant Folder Access
          </button>
        </div>
      `;
    } else {
      syncWidgetHTML = `
        <button id="btn-sync-action" class="btn" style="width: 100%; font-size: 11px; padding: 7px 10px; border-color: var(--g300); gap: 6px; justify-content: center; background: var(--paper);">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"></path></svg>
          Sync Local Folder
        </button>
      `;
    }

    sidebarHTML += `
      </div>
      <div style="margin-top: auto; padding-top: 20px; border-top: 1px solid var(--g300); display: flex; flex-direction: column; gap: 16px;">
        <div>
          <div class="nav-category" style="border-bottom: none; padding-bottom: 2px; margin-bottom: 6px;">Local Workspace</div>
          <div id="sync-container">${syncWidgetHTML}</div>
        </div>
        <div>
          <div class="nav-category" style="border-bottom: none; padding-bottom: 2px; margin-bottom: 6px;">View Mode</div>
          <div class="layout-switcher">
            <button id="btn-layout-standard" class="layout-btn ${savedLayout === 'standard' ? 'active' : ''}">
              Standard
            </button>
            <button id="btn-layout-wide" class="layout-btn ${savedLayout === 'wide' ? 'active' : ''}">
              Wide View
            </button>
          </div>
        </div>
      </div>
    `;

    sidebar.innerHTML = sidebarHTML;

    // Attach collapsible directories handlers
    const categoryNodes = sidebar.querySelectorAll(".category-node");
    categoryNodes.forEach(node => {
      node.addEventListener("click", () => {
        node.classList.toggle("open");
      });
    });

    // Rebind layout switcher buttons
    const btnStandard = document.getElementById("btn-layout-standard");
    const btnWide = document.getElementById("btn-layout-wide");
    if (btnStandard && btnWide) {
      btnStandard.addEventListener("click", () => {
        document.body.classList.remove("layout-wide");
        btnStandard.classList.add("active");
        btnWide.classList.remove("active");
        localStorage.setItem("wiki-layout", "standard");
      });
      btnWide.addEventListener("click", () => {
        document.body.classList.add("layout-wide");
        btnWide.classList.add("active");
        btnStandard.classList.remove("active");
        localStorage.setItem("wiki-layout", "wide");
      });
    }

    // Rebind workspace sync actions
    const syncBtn = document.getElementById("btn-sync-action");
    if (syncBtn) {
      syncBtn.addEventListener("click", async () => {
        if (window.wikiDirHandle) {
          // DISCONNECT
          if (confirm("Are you sure you want to disconnect local workspace sync? Data will remain cached in the browser but won't sync to disk.")) {
            window.wikiDirHandle = null;
            window.wikiPendingHandle = null;
            await clearHandle();
            renderSidebar();
            window.dispatchEvent(new CustomEvent("wiki-sync-status-changed"));
          }
        } else if (window.wikiPendingHandle) {
          // GRANT ACCESS (Re-authorize)
          try {
            const mode = 'readwrite';
            const permission = await window.wikiPendingHandle.requestPermission({ mode });
            if (permission === 'granted') {
              window.wikiDirHandle = window.wikiPendingHandle;
              window.wikiPendingHandle = null;
              await saveHandle(window.wikiDirHandle);
              await migrateRootFilesToSubfolder();
              await syncTasksOnConnection();
              await syncNotesOnConnection();
              renderSidebar();
              window.dispatchEvent(new CustomEvent("wiki-sync-status-changed"));
            }
          } catch (e) {
            console.error("Re-authorization failed:", e);
          }
        } else {
          // CONNECT NEW FOLDER
          try {
            const handle = await window.showDirectoryPicker({ mode: 'readwrite' });
            window.wikiDirHandle = handle;
            window.wikiPendingHandle = null;
            await saveHandle(handle);
            await migrateRootFilesToSubfolder();
            await syncTasksOnConnection();
            await syncNotesOnConnection();
            renderSidebar();
            window.dispatchEvent(new CustomEvent("wiki-sync-status-changed"));
          } catch (e) {
            console.error("Folder picker cancelled or failed:", e);
          }
        }
      });
    }

    // Rebind collapse sidebar toggle
    const btnCollapse = document.getElementById("btn-sidebar-collapse");
    if (btnCollapse && layoutContainer) {
      btnCollapse.addEventListener("click", () => {
        layoutContainer.classList.add("sidebar-collapsed");
        const exp = document.getElementById("btn-sidebar-expand");
        if (exp) exp.classList.add("visible");
        const pill = document.getElementById("floating-sync-indicator");
        if (pill) pill.classList.add("visible");
        localStorage.setItem("wiki-sidebar-collapsed", "true");
      });
    }
  }

  // 3. Render sidebar initially
  renderSidebar();

  // Dispatch initial sync resolution event (tells other pages the sync handle loaded)
  window.dispatchEvent(new CustomEvent("wiki-sync-status-changed"));

  // 4. Bind Floating Expand Button Click
  const btnExpand = document.getElementById("btn-sidebar-expand");
  if (btnExpand && layoutContainer) {
    btnExpand.addEventListener("click", () => {
      layoutContainer.classList.remove("sidebar-collapsed");
      btnExpand.classList.remove("visible");
      const pill = document.getElementById("floating-sync-indicator");
      if (pill) pill.classList.remove("visible");
      localStorage.setItem("wiki-sidebar-collapsed", "false");
    });
  }

  // 5. Bind Scroll-to-Top Button Scroll & Click
  const backToTop = document.getElementById("btn-back-to-top");
  if (backToTop) {
    const handleScroll = () => {
      if (window.scrollY > 300) {
        backToTop.classList.add("visible");
      } else {
        backToTop.classList.remove("visible");
      }
    };
    window.addEventListener("scroll", handleScroll);
    handleScroll();

    backToTop.addEventListener("click", () => {
      window.scrollTo({
        top: 0,
        behavior: "smooth"
      });
    });
  }

  // 6. Inject Page-Specific Notes Section dynamically
  const contentArea = document.querySelector("main.wiki-content");
  const isNavigablePage = currentPath !== "todo.html" && currentPath !== "cookbook-recipes.html";
  if (contentArea && isNavigablePage) {
    const notesSection = document.createElement("section");
    notesSection.id = "wiki-page-notes";
    notesSection.style.marginTop = "64px";
    notesSection.style.borderTop = "1px solid var(--g300)";
    notesSection.style.paddingTop = "32px";
    
    notesSection.innerHTML = `
      <div class="eyebrow">Engineering Notebook</div>
      <h2>Annotations &amp; Audit Logs</h2>
      <p class="sec-desc">Capture custom audit logs, review comments, and team notes specifically for this page.</p>
      <div class="parity-table-container" style="padding: 20px; background: var(--paper); border: 1.5px solid var(--g300); border-radius: 8px;">
        <textarea id="txt-page-notes" placeholder="Write page-specific notes or audit observations here... (autosaved locally)" style="width: 100%; min-height: 120px; padding: 12px; border: 1.5px solid var(--g300); border-radius: 8px; font-family: var(--sans); font-size: 13.5px; resize: vertical; margin-bottom: 12px; outline: none; transition: border-color 0.2s;" onfocus="this.style.borderColor='var(--clay)'" onblur="this.style.borderColor='var(--g300)'"></textarea>
        <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px;">
          <span id="notes-save-status" style="font-size: 11.5px; font-family: var(--mono); color: var(--g500);">All notes synced locally</span>
          <button id="btn-clear-notes" class="btn" style="font-size: 11px; padding: 5px 10px; border-color: var(--clay); color: var(--clay); background: transparent;">Clear Notes</button>
        </div>
      </div>
    `;
    contentArea.appendChild(notesSection);
    
    const txtNotes = document.getElementById("txt-page-notes");
    const notesStatus = document.getElementById("notes-save-status");
    const btnClear = document.getElementById("btn-clear-notes");
    
    let notes = {};
    
    const loadNotesFromDiskOrCache = async () => {
      // 1. Load local cache fallback
      notes = JSON.parse(localStorage.getItem("wiki-notes-state")) || {};
      
      // 2. Load from disk if synced
      if (window.wikiDirHandle) {
        const diskNotes = await window.readWikiFile("wiki-notes.json");
        if (diskNotes) {
          notes = diskNotes;
          localStorage.setItem("wiki-notes-state", JSON.stringify(notes));
        }
      }
      
      txtNotes.value = notes[currentPath] || "";
      updateStatusLabel();
    };

    function updateStatusLabel() {
      if (window.wikiDirHandle) {
        notesStatus.textContent = `Synced with local folder: ${window.wikiDirHandle.name}`;
      } else {
        notesStatus.textContent = "Saved to local browser cache (sync disabled)";
      }
    }
    
    window.addEventListener("wiki-sync-status-changed", async () => {
      await loadNotesFromDiskOrCache();
    });
    
    await loadNotesFromDiskOrCache();
    
    let saveTimeout;
    txtNotes.addEventListener("input", () => {
      notesStatus.textContent = "Saving changes...";
      clearTimeout(saveTimeout);
      saveTimeout = setTimeout(async () => {
        notes[currentPath] = txtNotes.value;
        localStorage.setItem("wiki-notes-state", JSON.stringify(notes));
        
        if (window.wikiDirHandle) {
          const success = await window.writeWikiFile("wiki-notes.json", notes);
          if (success) {
            notesStatus.textContent = `Synced with local folder: ${window.wikiDirHandle.name}`;
          } else {
            notesStatus.textContent = "Sync write failed. Saved to local cache.";
          }
        } else {
          notesStatus.textContent = "Saved to local browser cache (sync disabled)";
        }
      }, 500);
    });
    
    btnClear.addEventListener("click", () => {
      if (txtNotes.value === "") return;
      if (confirm("Are you sure you want to clear your notes for this page?")) {
        txtNotes.value = "";
        notes[currentPath] = "";
        localStorage.setItem("wiki-notes-state", JSON.stringify(notes));
        if (window.wikiDirHandle) {
          window.writeWikiFile("wiki-notes.json", notes);
        }
        notesStatus.textContent = "Notes cleared";
      }
    });
  }
});
