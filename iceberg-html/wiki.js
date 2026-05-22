// Shared JavaScript Component for The Iceberg Cookbook Wiki
document.addEventListener("DOMContentLoaded", () => {
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
    }
  ];

  // 2. Resolve Active Link
  const currentPath = window.location.pathname.split("/").pop() || "index.html";

  // 3. Construct HTML Markup
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

  // SVGs for modern collapsible Categories and Pages
  const arrowSvg = `<svg class="category-arrow" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 18 15 12 9 6"></polyline></svg>`;
  const categorySvg = `<svg class="category-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"></path><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"></path></svg>`;
  const pageSvg = `<svg class="page-icon" width="8" height="8" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="4.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="8"></circle></svg>`;

  navSchema.forEach((group, index) => {
    // Check if category contains the active page to auto-expand it on load
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

  sidebarHTML += `
    </div>
    <div style="margin-top: auto; padding-top: 20px; border-top: 1px solid var(--g300);">
      <div class="nav-category" style="border-bottom: none; padding-bottom: 2px;">View Mode</div>
      <div class="layout-switcher">
        <button id="btn-layout-standard" class="layout-btn ${savedLayout === 'standard' ? 'active' : ''}">
          Standard
        </button>
        <button id="btn-layout-wide" class="layout-btn ${savedLayout === 'wide' ? 'active' : ''}">
          Wide View
        </button>
      </div>
    </div>
  `;

  // 4. Inject into Placeholder
  sidebar.innerHTML = sidebarHTML;

  // 4b. Category Expand/Collapse Click Listeners
  const categoryNodes = sidebar.querySelectorAll(".category-node");
  categoryNodes.forEach(node => {
    node.addEventListener("click", () => {
      node.classList.toggle("open");
    });
  });

  // 5. Layout Switching Event Listeners
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

  // 6. Sidebar Collapsible Event Listeners
  const btnCollapse = document.getElementById("btn-sidebar-collapse");
  const btnExpand = document.getElementById("btn-sidebar-expand");

  if (btnCollapse && btnExpand && layoutContainer) {
    btnCollapse.addEventListener("click", () => {
      layoutContainer.classList.add("sidebar-collapsed");
      btnExpand.classList.add("visible");
      localStorage.setItem("wiki-sidebar-collapsed", "true");
    });

    btnExpand.addEventListener("click", () => {
      layoutContainer.classList.remove("sidebar-collapsed");
      btnExpand.classList.remove("visible");
      localStorage.setItem("wiki-sidebar-collapsed", "false");
    });
  }

  // 7. Back to Top Event Listeners
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
});
