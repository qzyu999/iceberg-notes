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
    runPlanningCalculation();
    setupSearch();
});

function runPlanningCalculation() {
    const mutationType = document.getElementById("plan-mutation-type").value;
    const fileSizeMB = parseInt(document.getElementById("plan-file-size").value);
    const mutatedRows = parseInt(document.getElementById("plan-mutated-rows").value);
    const commitsBeforeCompaction = parseInt(document.getElementById("plan-concurrency-factor").value);

    // Update Slider Labels
    document.getElementById("label-file-size").innerText = `${fileSizeMB} MB`;
    document.getElementById("label-mutated-rows").innerText = mutatedRows.toLocaleString();
    document.getElementById("label-concurrency-factor").innerText = commitsBeforeCompaction;

    // Mathematical Constants
    const rowSizeBytes = 200; // Average row data size
    const fileSizeBytes = fileSizeMB * 1024 * 1024;
    const netMutationBytes = mutatedRows * rowSizeBytes;

    let waf = 1.0;
    let raf = 1.0;
    let heapLoadMB = 0;

    if (mutationType === "cow") {
        // Copy-on-Write: every commit rewrites the entire file size
        // WAF = File Size / Net Mutations
        waf = fileSizeBytes / netMutationBytes;
        raf = 1.0;
        heapLoadMB = 2.0; // Minimal JVM heap overhead for delete indices since there are none
    } else if (mutationType === "mor-pos") {
        // Merge-on-Read (Positional)
        // Writes tiny delete files (say 50 bytes per row) during commits.
        // Compaction rewrites the data file.
        const deleteFileSizeBytes = mutatedRows * 50;
        const totalWrittenBytes = (commitsBeforeCompaction * deleteFileSizeBytes) + fileSizeBytes;
        const totalMutationBytes = commitsBeforeCompaction * netMutationBytes;
        
        waf = totalWrittenBytes / totalMutationBytes;
        
        // RAF increases with delete files
        raf = 1.0 + (0.15 * commitsBeforeCompaction);
        
        // JVM Heap loads positional lookup list into memory
        heapLoadMB = Math.round((mutatedRows * commitsBeforeCompaction * 128) / (1024 * 1024)); // 128 bytes per pointer in Java heap
    } else if (mutationType === "mor-dv") {
        // Merge-on-Read (Deletion Vectors)
        // Highly compressed roaring bitmaps: only ~4 bytes per row!
        const dvFileSizeBytes = mutatedRows * 4;
        const totalWrittenBytes = (commitsBeforeCompaction * dvFileSizeBytes) + fileSizeBytes;
        const totalMutationBytes = commitsBeforeCompaction * netMutationBytes;
        
        waf = totalWrittenBytes / totalMutationBytes;
        
        // RAF increases very slightly (roaring bitmaps are cached and highly fast to scan)
        raf = 1.0 + (0.015 * commitsBeforeCompaction);
        
        // Dynamic Arrow/JVM heap footprint is extremely low
        heapLoadMB = Math.round((mutatedRows * commitsBeforeCompaction * 8) / (1024 * 1024)); // 8 bytes per bitmask
    }

    // Cap JVM heap minimum
    if (heapLoadMB < 1) heapLoadMB = 1;

    // Display Results
    document.getElementById("plan-waf-val").innerText = `${waf.toFixed(1)}x`;
    document.getElementById("plan-raf-val").innerText = `${raf.toFixed(2)}x`;
    document.getElementById("plan-ram-val").innerText = `${heapLoadMB} MB`;

    // Render Advisory Card
    renderAdvisoryCard(mutationType, waf, raf, heapLoadMB);
}

function renderAdvisoryCard(mutationType, waf, raf, heapLoadMB) {
    const container = document.getElementById("plan-recommendation-container");
    container.innerHTML = "";

    let isWarning = false;
    let title = "Optimal Strategy Configured";
    let desc = "Your configuration complies with Committer-level production specifications. The table layout maintains a highly effective balance between writes, reads, and client memory limits.";

    if (mutationType === "cow" && waf > 40) {
        isWarning = true;
        title = "Severe Write Amplification Risk";
        desc = `Copy-on-Write is rewriting the entire file for tiny updates, resulting in an extreme ${waf.toFixed(1)}x write amplification! This leads to massive storage bills and throttles catalog commit pathways. Reconfigure table settings to use Merge-on-Read (write.delete.mode=merge-on-read).`;
    } else if (mutationType === "mor-pos" && raf > 2.5) {
        isWarning = true;
        title = "Read Scan Seeking Penalty";
        desc = `With ${raf.toFixed(2)}x Read Amplification, query execution times will be severely impacted! Accumulating too many positional delete file handles requires the query planner to run hundreds of random object seeks. Increase compaction frequencies or configure automatic rewrite actions.`;
    } else if (heapLoadMB > 150) {
        isWarning = true;
        title = "JVM Heap Exhaustion Threat";
        desc = `Loading delete index vectors requires ${heapLoadMB} MB of heap memory per active scan context! Under high client concurrency, this will trigger severe JVM garbage collection pauses or OutOfMemory (OOM) errors. Transition to highly compressed V3 Deletion Vectors (Roaring Bitmaps) to reduce heap load by up to 90%.`;
    }

    const card = document.createElement("div");
    card.className = `advisory-card ${isWarning ? 'warning' : ''}`;
    
    card.innerHTML = `
        <div class="advisory-title" style="display: flex; align-items: center; gap: 6px;">
            <i data-lucide="${isWarning ? 'alert-triangle' : 'check-circle-2'}" style="width: 15px; height: 15px; color: ${isWarning ? 'var(--rust)' : 'var(--olive)'};"></i>
            ${title}
        </div>
        <div class="advisory-desc">${desc}</div>
    `;

    container.appendChild(card);
    lucide.createIcons();
}

function setupSearch() {
    const searchInput = document.getElementById("global-search");
    if (!searchInput) return;

    searchInput.addEventListener("input", (e) => {
        const query = e.target.value.toLowerCase().trim();
        const sliders = document.querySelectorAll(".slider-group");
        
        sliders.forEach(slider => {
            const labelText = slider.querySelector("label").innerText.toLowerCase();
            slider.style.opacity = (!query || labelText.includes(query)) ? "1" : "0.3";
        });
    });
}
