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

const WorkspaceSync = {
    dbName: "icod-workspace-db",
    storeName: "handles",
    keyName: "dirHandle",
    activeHandle: null,

    // IndexedDB wrappers
    openDB() {
        return new Promise((resolve, reject) => {
            const request = indexedDB.open(this.dbName, 1);
            request.onupgradeneeded = (e) => {
                const db = e.target.result;
                if (!db.objectStoreNames.contains(this.storeName)) {
                    db.createObjectStore(this.storeName);
                }
            };
            request.onsuccess = (e) => resolve(e.target.result);
            request.onerror = (e) => reject(e.target.error);
        });
    },

    async getStoredDirectoryHandle() {
        try {
            const db = await this.openDB();
            return new Promise((resolve, reject) => {
                const tx = db.transaction(this.storeName, "readonly");
                const store = tx.objectStore(this.storeName);
                const req = store.get(this.keyName);
                req.onsuccess = () => resolve(req.result || null);
                req.onerror = () => reject(req.error);
            });
        } catch (e) {
            console.warn("[WorkspaceSync] Failed to read from IndexedDB", e);
            return null;
        }
    },

    async storeDirectoryHandle(handle) {
        try {
            const db = await this.openDB();
            return new Promise((resolve, reject) => {
                const tx = db.transaction(this.storeName, "readwrite");
                const store = tx.objectStore(this.storeName);
                const req = store.put(handle, this.keyName);
                req.onsuccess = () => resolve();
                req.onerror = () => reject(req.error);
            });
        } catch (e) {
            console.warn("[WorkspaceSync] Failed to write to IndexedDB", e);
        }
    },

    async clearDirectoryHandle() {
        try {
            const db = await this.openDB();
            return new Promise((resolve, reject) => {
                const tx = db.transaction(this.storeName, "readwrite");
                const store = tx.objectStore(this.storeName);
                const req = store.delete(this.keyName);
                req.onsuccess = () => resolve();
                req.onerror = () => reject(req.error);
            });
        } catch (e) {
            console.warn("[WorkspaceSync] Failed to delete from IndexedDB", e);
        }
    },

    // Verify read/write permission of a handle
    async verifyPermission(fileHandle, readWrite = true) {
        const options = {};
        if (readWrite) {
            options.mode = "readwrite";
        }
        // Check if we already have permission
        if ((await fileHandle.queryPermission(options)) === "granted") {
            return true;
        }
        // Request permission
        if ((await fileHandle.requestPermission(options)) === "granted") {
            return true;
        }
        return false;
    },

    // Read a file from directory handle
    async readWorkspaceFile(dirHandle, fileName) {
        try {
            const fileHandle = await dirHandle.getFileHandle(fileName, { create: false });
            const file = await fileHandle.getFile();
            const text = await file.text();
            return JSON.parse(text);
        } catch (e) {
            console.error(`[WorkspaceSync] Error reading file ${fileName}`, e);
            throw e;
        }
    },

    // Write a file back to directory handle
    async writeWorkspaceFile(dirHandle, fileName, dataObj) {
        try {
            const fileHandle = await dirHandle.getFileHandle(fileName, { create: true });
            const writable = await fileHandle.createWritable();
            const formattedJSON = JSON.stringify(dataObj, null, 2);
            await writable.write(formattedJSON);
            await writable.close();
            console.log(`[WorkspaceSync] Successfully wrote file ${fileName} directly to workspace!`);
        } catch (e) {
            console.error(`[WorkspaceSync] Error writing file ${fileName}`, e);
            throw e;
        }
    },

    // Initialize UI connector for directory synchronization
    initUI(containerId, onConnectedCallback, onDisconnectedCallback) {
        const container = document.getElementById(containerId);
        if (!container) return;

        // Render standard Ivory Directory Banner
        container.innerHTML = `
            <div class="workspace-banner" id="ws-banner" style="
                display: flex;
                align-items: center;
                justify-content: space-between;
                background: var(--white);
                border: 1.5px solid var(--gray-200);
                border-radius: 12px;
                padding: 12px 18px;
                margin-bottom: 24px;
                font-size: 13px;
                transition: all 0.2s ease;
            ">
                <div style="display: flex; align-items: center; gap: 10px;">
                    <div id="ws-indicator" style="
                        width: 8px;
                        height: 8px;
                        border-radius: 50%;
                        background: var(--clay);
                    "></div>
                    <div>
                        <strong id="ws-status-text" style="color: var(--slate);">Workspace Sandbox</strong>
                        <div id="ws-sub-text" style="color: var(--gray-500); font-size: 11px; font-family: var(--mono); margin-top: 1px;">
                            Running in local browser memory (localStorage)
                        </div>
                    </div>
                </div>
                <div style="display: flex; gap: 8px;">
                    <button class="btn ghost" id="btn-ws-connect" style="
                        padding: 6px 12px;
                        font-size: 11px;
                        font-family: var(--mono);
                        border-radius: 6px;
                        background: transparent;
                        border: 1.5px solid var(--gray-200);
                        color: var(--gray-800);
                        cursor: pointer;
                        display: flex;
                        align-items: center;
                        gap: 6px;
                    ">
                        <i data-lucide="folder"></i>
                        <span>Connect Directory</span>
                    </button>
                    <button class="btn ghost" id="btn-ws-disconnect" style="
                        padding: 6px 12px;
                        font-size: 11px;
                        font-family: var(--mono);
                        border-radius: 6px;
                        background: transparent;
                        border: 1.5px solid rgba(217, 119, 87, 0.3);
                        color: var(--clay);
                        cursor: pointer;
                        display: none;
                    ">Disconnect</button>
                </div>
            </div>
        `;

        const indicator = document.getElementById("ws-indicator");
        const statusText = document.getElementById("ws-status-text");
        const subText = document.getElementById("ws-sub-text");
        const connectBtn = document.getElementById("btn-ws-connect");
        const disconnectBtn = document.getElementById("btn-ws-disconnect");

        const updateUIState = (connected, dirName = "") => {
            if (connected) {
                indicator.style.background = "var(--olive)";
                statusText.innerText = "Directory Synchronized";
                subText.innerText = `Connected: /${dirName} (Active reads & writes)`;
                connectBtn.style.display = "none";
                disconnectBtn.style.display = "block";
                document.getElementById("ws-banner").style.borderColor = "rgba(120, 140, 93, 0.4)";
                document.getElementById("ws-banner").style.background = "rgba(120, 140, 93, 0.03)";
            } else {
                indicator.style.background = "var(--clay)";
                statusText.innerText = "Workspace Sandbox";
                subText.innerText = "Running in local browser memory (localStorage)";
                connectBtn.style.display = "flex";
                disconnectBtn.style.display = "none";
                document.getElementById("ws-banner").style.borderColor = "var(--gray-200)";
                document.getElementById("ws-banner").style.background = "var(--white)";
            }
            if (window.lucide) window.lucide.createIcons();
        };

        // Try to auto-connect on load
        this.getStoredDirectoryHandle().then(async (handle) => {
            if (handle) {
                try {
                    // Try to verify permission
                    const hasPerm = await this.verifyPermission(handle, true);
                    if (hasPerm) {
                        this.activeHandle = handle;
                        updateUIState(true, handle.name);
                        if (onConnectedCallback) onConnectedCallback(handle);
                    } else {
                        // Permission denied by user
                        this.activeHandle = null;
                        this.clearDirectoryHandle();
                        updateUIState(false);
                    }
                } catch (e) {
                    console.warn("[WorkspaceSync] Auto-connect verification failed", e);
                    updateUIState(false);
                }
            }
        });

        // Click handler to select directory
        connectBtn.addEventListener("click", async () => {
            if (!window.showDirectoryPicker) {
                alert("File System Access API is not supported in this browser. Please use Chrome, Edge, or Opera for live workspace directory syncing. Falling back to local browser storage!");
                return;
            }

            try {
                const handle = await window.showDirectoryPicker({
                    mode: "readwrite"
                });
                const hasPerm = await this.verifyPermission(handle, true);
                if (hasPerm) {
                    this.activeHandle = handle;
                    await this.storeDirectoryHandle(handle);
                    updateUIState(true, handle.name);
                    if (onConnectedCallback) onConnectedCallback(handle);
                }
            } catch (e) {
                console.error("[WorkspaceSync] Error selecting directory", e);
            }
        });

        // Disconnect handler
        disconnectBtn.addEventListener("click", async () => {
            this.activeHandle = null;
            await this.clearDirectoryHandle();
            updateUIState(false);
            if (onDisconnectedCallback) onDisconnectedCallback();
        });
    }
};
