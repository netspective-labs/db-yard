// bin/web-ui/asset/app.js
const procStatusText = document.getElementById("procStatusText");
const procTbody = document.getElementById("procTbody");
const filterInput = document.getElementById("filterInput");
const refreshBtn = document.getElementById("refreshBtn");

const reconcileStatusText = document.getElementById("reconcileStatusText");
const reconcileTbody = document.getElementById("reconcileTbody");
const reconcileBtn = document.getElementById("reconcileBtn");

const proxyTableBtn = document.getElementById("proxyTableBtn");
const proxyTbody = document.getElementById("proxyTbody");
const proxyConflictsText = document.getElementById("proxyConflictsText");

const resolveInput = document.getElementById("resolveInput");
const resolveBtn = document.getElementById("resolveBtn");

const healthBtn = document.getElementById("healthBtn");
const healthTbody = document.getElementById("healthTbody");

let lastPayload = null;

function esc(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function matchesFilter(p, q) {
  if (!q) return true;
  const hay = [
    p.pid,
    p.upstreamUrl,
    p.serviceId,
    p.sessionId,
    p.contextPath,
    p.proxyEndpointPrefix,
    p._sourceFile,
  ].map((x) => String(x ?? "")).join(" ").toLowerCase();
  return hay.includes(q.toLowerCase());
}

function toPosix(p) {
  return String(p ?? "").replaceAll("\\", "/");
}

function stripLedgerPrefix(ledgerDirAbs, p) {
  const ledger = toPosix(ledgerDirAbs || "").replace(/\/+$/, "");
  const path = toPosix(p || "");
  if (!path) return "";
  if (!ledger) return path.replace(/^\/+/, "");
  if (path.startsWith(ledger + "/")) return path.slice((ledger + "/").length);
  if (path === ledger) return "";
  return path.replace(/^\/+/, "");
}

function ledgerHref(rel) {
  const r = toPosix(rel || "").replace(/^\/+/, "");
  return r ? `/.db-yard/ledger.d/${r}` : "";
}

function deriveLogRel(relContextJson, which /* "stdout" | "stderr" */) {
  const rel = toPosix(relContextJson || "");
  if (!rel) return "";
  const suffix = which === "stderr" ? ".stderr.log" : ".stdout.log";
  if (rel.endsWith(".context.json")) {
    return rel.slice(0, -".context.json".length) + suffix;
  }
  return rel + suffix;
}

function renderProcesses(payload) {
  lastPayload = payload;
  const { taggedProcesses, ledgerDir, now, count } = payload;
  const q = filterInput.value.trim();

  procStatusText.textContent =
    `Processes: ${count} | Updated ${now} | ledgerDir=${ledgerDir}`;

  procTbody.innerHTML = "";

  for (const p of taggedProcesses.filter((x) => matchesFilter(x, q))) {
    const pid = p.pid ?? "";
    const upstreamUrl = p.upstreamUrl ?? "";
    const serviceId = p.serviceId ?? "";
    const sessionId = p.sessionId ?? "";
    const contextPath = p.contextPath ?? "";

    const relContext = stripLedgerPrefix(ledgerDir, contextPath);
    const ctxHref = ledgerHref(relContext);
    const ctxLabel = relContext || String(contextPath || "");
    const ledgerCell = ctxHref
      ? `<a class="mono" href="${
        esc(ctxHref)
      }" target="_blank" rel="noreferrer">${esc(ctxLabel)}</a>`
      : `<span class="mono">${esc(ctxLabel)}</span>`;

    const stdoutRel = deriveLogRel(relContext, "stdout");
    const stderrRel = deriveLogRel(relContext, "stderr");
    const stdoutHref = ledgerHref(stdoutRel);
    const stderrHref = ledgerHref(stderrRel);

    const proxiedPath = p.proxyEndpointPrefix ?? "";
    const proxiedCell = proxiedPath
      ? `<a class="mono" href="${
        esc(proxiedPath)
      }" target="_blank" rel="noreferrer">${esc(proxiedPath)}</a>`
      : `<span class="mono"></span>`;

    const svcLabel = serviceId || "(unknown)";
    const svcTitle = sessionId ? `sessionId=${sessionId}` : "";
    const serviceCell = `<span class="mono" title="${esc(svcTitle)}">${
      esc(svcLabel)
    }</span>`;

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="mono">${esc(pid)}</td>
      <td class="mono"><a href="${
      esc(upstreamUrl)
    }" target="_blank" rel="noreferrer">${esc(upstreamUrl)}</a></td>
      <td class="mono">${proxiedCell}</td>
      <td>${serviceCell}</td>
      <td>${ledgerCell}</td>
      <td class="actions">
        <a class="btnlink" href="${
      esc(stdoutHref || "#")
    }" target="_blank" rel="noreferrer">STDOUT</a>
        <a class="btnlink" href="${
      esc(stderrHref || "#")
    }" target="_blank" rel="noreferrer">STDERR</a>
      </td>
    `;
    procTbody.appendChild(tr);
  }
}

function renderReconcile(payload) {
  const { now, ledgerDir, summary, items, proxyConflicts } = payload;
  const ok = summary?.processWithoutLedger === 0 &&
    summary?.ledgerWithoutProcess === 0;

  reconcileStatusText.textContent = ok
    ? `Reconcile: OK | Updated ${now} | ledgerDir=${ledgerDir}`
    : `Reconcile: discrepancies | Updated ${now} | ledgerDir=${ledgerDir} | processWithoutLedger=${
      summary?.processWithoutLedger ?? "?"
    } ledgerWithoutProcess=${summary?.ledgerWithoutProcess ?? "?"}`;

  reconcileTbody.innerHTML = "";

  for (const it of (items || [])) {
    const kind = it.kind || "";
    const pid = it.pid ?? "";
    const serviceId = it.serviceId ?? "";
    const sessionId = it.sessionId ?? "";

    let path = "";
    let cmdline = "";

    if (kind === "process_without_ledger") {
      path = it.contextPath ?? "";
      cmdline = it.cmdline ?? "";
    } else if (kind === "ledger_without_process") {
      path = it.ledgerContextPath ?? "";
      cmdline = "";
    } else {
      path = it.contextPath ?? it.ledgerContextPath ?? "";
      cmdline = it.cmdline ?? "";
    }

    const rel = stripLedgerPrefix(ledgerDir, path);
    const href = ledgerHref(rel);

    const pathCell = href
      ? `<a class="mono" href="${esc(href)}" target="_blank" rel="noreferrer">${
        esc(rel || path)
      }</a>`
      : `<span class="mono">${esc(rel || path)}</span>`;

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="mono">${esc(kind)}</td>
      <td class="mono">${esc(pid)}</td>
      <td class="mono">${esc(serviceId)}</td>
      <td class="mono">${esc(sessionId)}</td>
      <td>${pathCell}</td>
      <td class="mono">${esc(cmdline)}</td>
    `;
    reconcileTbody.appendChild(tr);
  }

  if (Array.isArray(proxyConflicts) && proxyConflicts.length) {
    proxyConflictsText.textContent =
      `Proxy conflicts: ${proxyConflicts.length} (also shown in proxy table)`;
  }
}

function renderProxyTable(payload) {
  const { proxyTable, proxyConflicts } = payload;
  proxyTbody.innerHTML = "";

  for (const r of (proxyTable || [])) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="mono">${esc(r.basePath)}</td>
      <td class="mono"><a href="${
      esc(r.upstreamUrl)
    }" target="_blank" rel="noreferrer">${esc(r.upstreamUrl)}</a></td>
    `;
    proxyTbody.appendChild(tr);
  }

  if (Array.isArray(proxyConflicts) && proxyConflicts.length) {
    const lines = proxyConflicts.map((c) =>
      `${c.basePath} -> ${c.upstreamUrls.join(" | ")}`
    );
    proxyConflictsText.textContent =
      `Proxy conflicts (${proxyConflicts.length}): ${lines.join("; ")}`;
  } else {
    proxyConflictsText.textContent = "Proxy conflicts: none";
  }
}

function renderHealth(payload) {
  const { results } = payload;
  healthTbody.innerHTML = "";

  for (const r of (results || [])) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="mono">${esc(r.basePath)}</td>
      <td class="mono"><a href="${
      esc(r.upstreamUrl)
    }" target="_blank" rel="noreferrer">${esc(r.upstreamUrl)}</a></td>
      <td class="mono">${esc(r.ok)}</td>
      <td class="mono">${esc(r.status ?? "")}</td>
      <td class="mono">${esc(r.ms ?? "")}</td>
      <td class="mono">${esc(r.error ?? "")}</td>
    `;
    healthTbody.appendChild(tr);
  }
}

async function refreshProcesses() {
  try {
    procStatusText.textContent = "Processes: loading…";
    const res = await fetch("/.db-yard/api/tagged-processes.json", {
      cache: "no-store",
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const payload = await res.json();
    renderProcesses(payload);
  } catch (e) {
    procStatusText.textContent = `Processes: failed to load: ${
      e?.message ?? e
    }`;
    if (lastPayload) renderProcesses(lastPayload);
  }
}

async function runReconcile() {
  try {
    reconcileStatusText.textContent = "Reconcile: running…";
    const res = await fetch("/.db-yard/api/reconcile.json", {
      cache: "no-store",
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const payload = await res.json();
    renderReconcile(payload);
  } catch (e) {
    reconcileStatusText.textContent = `Reconcile: failed: ${e?.message ?? e}`;
  }
}

async function loadProxyTable() {
  try {
    proxyConflictsText.textContent = "Proxy conflicts: loading…";
    const res = await fetch("/.db-yard/api/proxy-table.json", {
      cache: "no-store",
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const payload = await res.json();
    renderProxyTable(payload);
  } catch (e) {
    proxyConflictsText.textContent = `Proxy conflicts: failed: ${
      e?.message ?? e
    }`;
  }
}

async function resolveProxy() {
  const p = (resolveInput.value || "").trim();
  if (!p) return;

  try {
    const res = await fetch(
      `/.db-yard/api/proxy-resolve.json?path=${encodeURIComponent(p)}`,
      {
        cache: "no-store",
      },
    );
    const payload = await res.json();

    if (!payload.ok) {
      alert(`No match for ${p}`);
      return;
    }

    alert(
      `matchBasePath=${payload.matchBasePath}\nupstreamUrl=${payload.upstreamUrl}\nproxiedUrl=${payload.proxiedUrl}`,
    );
  } catch (e) {
    alert(`Resolve failed: ${e?.message ?? e}`);
  }
}

async function runHealth() {
  try {
    const res = await fetch("/.db-yard/api/health.json?timeoutMs=1500&max=50", {
      cache: "no-store",
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const payload = await res.json();
    renderHealth(payload);
  } catch (e) {
    alert(`Health checks failed: ${e?.message ?? e}`);
  }
}

refreshBtn.addEventListener("click", refreshProcesses);
filterInput.addEventListener("input", () => {
  if (lastPayload) renderProcesses(lastPayload);
});
reconcileBtn.addEventListener("click", runReconcile);
proxyTableBtn.addEventListener("click", loadProxyTable);
resolveBtn.addEventListener("click", resolveProxy);
healthBtn.addEventListener("click", runHealth);

// run reconcile on start (and processes)
refreshProcesses();
runReconcile();
