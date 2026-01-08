// lib/serve/web-ui.js
async function load() {
  const runtimeEl = document.getElementById("runtime");
  const servicesEl = document.getElementById("services");

  const res = await fetch("/.admin", {
    headers: { "accept": "application/json" },
  });
  const data = await res.json();

  runtimeEl.textContent = JSON.stringify(
    {
      ok: data.ok,
      nowMs: data.nowMs,
      sessionHome: data.sessionHome,
      count: data.count,
    },
    null,
    2,
  );

  servicesEl.innerHTML = `
    <div class="row head">
      <span class="col col-id">id</span>
      <span class="col col-kind">kind</span>
      <span class="col col-host">host</span>
      <span class="col col-prefix">proxy</span>
      <span class="col col-alive">alive</span>
      <span class="col col-db">location</span>
    </div>
  `;

  for (const it of (data.items || [])) {
    const alive = it.pidAlive ? "yes" : "no";
    const row = document.createElement("div");
    row.className = "row";
    row.innerHTML = `
      <span class="col col-id">${escapeHtml(it.id)}</span>
      <span class="col col-kind">${escapeHtml(it.kind)}</span>
      <span class="col col-host">${
      escapeHtml(it.listen.host + ":" + it.listen.port)
    }</span>
      <span class="col col-prefix"><a href="${
      escapeAttr(it.proxyEndpointPrefix)
    }">${escapeHtml(it.proxyEndpointPrefix)}</a></span>
      <span class="col col-alive">${escapeHtml(alive)}</span>
      <span class="col col-db">${escapeHtml(it.supplier.location)}</span>
    `;
    servicesEl.appendChild(row);
  }
}

function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeAttr(s) {
  return escapeHtml(s).replaceAll("`", "&#96;");
}

load().catch((e) => {
  const runtimeEl = document.getElementById("runtime");
  if (runtimeEl) runtimeEl.textContent = String(e);
});
