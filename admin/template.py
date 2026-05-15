"""Admin dashboard HTML — single self-contained page, no external dependencies."""

from __future__ import annotations

ADMIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Owner Research — Admin</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    font-size: 14px;
    background: #f5f5f5;
    color: #222;
  }
  header {
    background: #1a1a2e;
    color: #fff;
    padding: 12px 24px;
    display: flex;
    align-items: center;
    gap: 24px;
  }
  header h1 { font-size: 18px; font-weight: 600; }
  .badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 12px;
    font-size: 12px;
    font-weight: 600;
  }
  .badge-green  { background: #22c55e; color: #fff; }
  .badge-red    { background: #ef4444; color: #fff; }
  .badge-gray   { background: #6b7280; color: #fff; }
  .banner {
    background: #fff;
    border-bottom: 1px solid #e5e7eb;
    padding: 10px 24px;
    display: flex;
    gap: 24px;
    align-items: center;
    font-size: 13px;
    flex-wrap: wrap;
  }
  .banner-item { color: #555; }
  .banner-item strong { color: #111; }
  main {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0;
    min-height: calc(100vh - 90px);
  }
  section {
    padding: 20px 24px;
    overflow: auto;
  }
  section + section {
    border-left: 1px solid #e5e7eb;
  }
  h2 { font-size: 15px; font-weight: 600; margin-bottom: 12px; }
  .btn {
    display: inline-block;
    padding: 5px 12px;
    border-radius: 5px;
    border: 1px solid #d1d5db;
    background: #fff;
    cursor: pointer;
    font-size: 13px;
    color: #374151;
  }
  .btn:hover { background: #f3f4f6; }
  .btn-primary { background: #1a1a2e; color: #fff; border-color: #1a1a2e; }
  .btn-primary:hover { background: #2d2d4a; }
  .zip-controls { display: flex; gap: 8px; margin-bottom: 12px; }
  table { width: 100%; border-collapse: collapse; }
  th, td {
    text-align: left;
    padding: 7px 10px;
    border-bottom: 1px solid #e5e7eb;
    vertical-align: middle;
  }
  th { font-weight: 600; color: #555; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; background: #fafafa; }
  tr:hover td { background: #f9fafb; }
  .toggle {
    position: relative;
    display: inline-block;
    width: 36px;
    height: 20px;
  }
  .toggle input { opacity: 0; width: 0; height: 0; }
  .slider {
    position: absolute;
    cursor: pointer;
    inset: 0;
    background: #d1d5db;
    border-radius: 20px;
    transition: .2s;
  }
  .slider::before {
    content: "";
    position: absolute;
    height: 14px;
    width: 14px;
    left: 3px;
    bottom: 3px;
    background: #fff;
    border-radius: 50%;
    transition: .2s;
  }
  input:checked + .slider { background: #22c55e; }
  input:checked + .slider::before { transform: translateX(16px); }
  .status-success { color: #16a34a; font-weight: 600; }
  .status-failed  { color: #dc2626; font-weight: 600; }
  .status-running, .status-in_progress { color: #6b7280; font-weight: 600; }
  .error-cell { max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: #dc2626; font-size: 12px; }
  .loading { color: #6b7280; font-style: italic; padding: 12px 0; }
  #zip-search {
    width: 100%;
    padding: 6px 10px;
    border: 1px solid #d1d5db;
    border-radius: 5px;
    margin-bottom: 10px;
    font-size: 13px;
  }
</style>
</head>
<body>

<header>
  <h1>Owner Research &mdash; Admin</h1>
  <span id="auto-badge" class="badge badge-gray">loading&hellip;</span>
</header>

<div class="banner">
  <div class="banner-item">Auto-search: <strong id="auto-label">—</strong></div>
  <div class="banner-item">Queue — llc_pierce: <strong id="q-llc">—</strong></div>
  <div class="banner-item">zoominfo: <strong id="q-zoo">—</strong></div>
  <div class="banner-item">multi_source: <strong id="q-ms">—</strong></div>
  <button class="btn" onclick="loadStatus()" style="margin-left:auto">Refresh status</button>
</div>

<main>
  <!-- ===== LEFT: Zipcode toggles ===== -->
  <section>
    <h2>Zipcode Allowlist</h2>
    <div class="zip-controls">
      <button class="btn btn-primary" onclick="bulkToggle(true)">Enable all</button>
      <button class="btn" onclick="bulkToggle(false)">Disable all</button>
    </div>
    <input id="zip-search" type="text" placeholder="Filter zipcodes&hellip;" oninput="filterZips()" />
    <div id="zip-loading" class="loading">Loading&hellip;</div>
    <table id="zip-table" style="display:none">
      <thead>
        <tr>
          <th>Zip</th>
          <th>Properties</th>
          <th>Enabled</th>
        </tr>
      </thead>
      <tbody id="zip-body"></tbody>
    </table>
  </section>

  <!-- ===== RIGHT: Recent runs ===== -->
  <section>
    <h2>Recent Pipeline Runs</h2>
    <div id="runs-loading" class="loading">Loading&hellip;</div>
    <table id="runs-table" style="display:none">
      <thead>
        <tr>
          <th>Source</th>
          <th>Started</th>
          <th>Duration</th>
          <th>Status</th>
          <th>Fetched</th>
          <th>Created</th>
          <th>Updated</th>
          <th>Skipped</th>
          <th>Error</th>
        </tr>
      </thead>
      <tbody id="runs-body"></tbody>
    </table>
  </section>
</main>

<script>
// ── helpers ──────────────────────────────────────────────────────────────────

let _allZips = [];

function fmtDate(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  return d.toLocaleString('en-US', {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'});
}

function fmtDuration(startIso, endIso) {
  if (!startIso || !endIso) return '—';
  const secs = Math.round((new Date(endIso) - new Date(startIso)) / 1000);
  if (secs < 60) return secs + 's';
  const m = Math.floor(secs / 60), s = secs % 60;
  return m + 'm ' + s + 's';
}

function statusClass(s) {
  if (!s) return '';
  return 'status-' + s.toLowerCase().replace(/\\s+/g, '_');
}

// ── status banner ─────────────────────────────────────────────────────────────

async function loadStatus() {
  try {
    const r = await fetch('/admin/api/status');
    if (!r.ok) throw new Error(r.status);
    const d = await r.json();
    const enabled = d.auto_search_enabled;
    const badge = document.getElementById('auto-badge');
    badge.textContent = enabled ? 'AUTO ON' : 'AUTO OFF';
    badge.className = 'badge ' + (enabled ? 'badge-green' : 'badge-red');
    document.getElementById('auto-label').textContent = enabled ? 'enabled' : 'disabled';
    const q = d.queue || {};
    document.getElementById('q-llc').textContent = q.llc_pierce ?? '—';
    document.getElementById('q-zoo').textContent  = q.zoominfo   ?? '—';
    document.getElementById('q-ms').textContent   = q.multi_source ?? '—';
  } catch(e) {
    console.error('status error', e);
  }
}

// ── zipcodes ──────────────────────────────────────────────────────────────────

async function loadZips() {
  try {
    const r = await fetch('/admin/api/zipcodes');
    if (!r.ok) throw new Error(r.status);
    _allZips = await r.json();
    renderZips(_allZips);
    document.getElementById('zip-loading').style.display = 'none';
    document.getElementById('zip-table').style.display = '';
  } catch(e) {
    document.getElementById('zip-loading').textContent = 'Error loading zipcodes: ' + e;
  }
}

function renderZips(zips) {
  const tbody = document.getElementById('zip-body');
  tbody.innerHTML = '';
  zips.forEach(z => {
    const tr = document.createElement('tr');
    const chkId = 'chk-' + z.zip_code;
    tr.innerHTML = `
      <td><code>${z.zip_code}</code></td>
      <td>${z.property_count ?? 0}</td>
      <td>
        <label class="toggle">
          <input type="checkbox" id="${chkId}" ${z.enabled ? 'checked' : ''}
            onchange="toggleZip('${z.zip_code}', this.checked)">
          <span class="slider"></span>
        </label>
      </td>`;
    tbody.appendChild(tr);
  });
}

function filterZips() {
  const q = document.getElementById('zip-search').value.trim();
  if (!q) { renderZips(_allZips); return; }
  renderZips(_allZips.filter(z => z.zip_code.includes(q)));
}

async function toggleZip(zip, enabled) {
  try {
    const r = await fetch('/admin/api/zipcodes/toggle', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({zip_code: zip, enabled}),
    });
    if (!r.ok) throw new Error(r.status);
    // Update local state
    const item = _allZips.find(z => z.zip_code === zip);
    if (item) item.enabled = enabled;
  } catch(e) {
    alert('Failed to toggle ' + zip + ': ' + e);
    loadZips(); // re-sync
  }
}

async function bulkToggle(enabled) {
  try {
    const r = await fetch('/admin/api/zipcodes/bulk?enabled=' + enabled, {method: 'POST'});
    if (!r.ok) throw new Error(r.status);
    _allZips.forEach(z => z.enabled = enabled);
    renderZips(_allZips.filter(z => {
      const q = document.getElementById('zip-search').value.trim();
      return !q || z.zip_code.includes(q);
    }));
  } catch(e) {
    alert('Bulk toggle failed: ' + e);
    loadZips();
  }
}

// ── runs ──────────────────────────────────────────────────────────────────────

async function loadRuns() {
  try {
    const r = await fetch('/admin/api/runs');
    if (!r.ok) throw new Error(r.status);
    const rows = await r.json();
    const tbody = document.getElementById('runs-body');
    tbody.innerHTML = '';
    rows.forEach(row => {
      const tr = document.createElement('tr');
      const err = (row.error_message || '').substring(0, 200);
      const errFull = row.error_message || '';
      tr.innerHTML = `
        <td>${row.source || '—'}</td>
        <td>${fmtDate(row.run_started_at)}</td>
        <td>${fmtDuration(row.run_started_at, row.run_finished_at)}</td>
        <td class="${statusClass(row.status)}">${row.status || '—'}</td>
        <td>${row.records_fetched ?? '—'}</td>
        <td>${row.records_created ?? '—'}</td>
        <td>${row.records_updated ?? '—'}</td>
        <td>${row.records_skipped ?? '—'}</td>
        <td class="error-cell" title="${errFull.replace(/"/g, '&quot;')}">${err}</td>`;
      tbody.appendChild(tr);
    });
    document.getElementById('runs-loading').style.display = 'none';
    document.getElementById('runs-table').style.display = '';
  } catch(e) {
    document.getElementById('runs-loading').textContent = 'Error loading runs: ' + e;
  }
}

// ── init ──────────────────────────────────────────────────────────────────────

loadStatus();
loadZips();
loadRuns();
</script>
</body>
</html>
"""
