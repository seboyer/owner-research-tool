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
  .btn:disabled { opacity: .5; cursor: not-allowed; }
  .btn:disabled:hover { background: #1a1a2e; }
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
  .error-cell a { color: inherit; text-decoration: underline dotted; cursor: pointer; }
  .error-cell a:hover { color: #991b1b; text-decoration: underline; }
  .loading { color: #6b7280; font-style: italic; padding: 12px 0; }
  #zip-search {
    width: 100%;
    padding: 6px 10px;
    border: 1px solid #d1d5db;
    border-radius: 5px;
    margin-bottom: 10px;
    font-size: 13px;
  }
  .nav-tabs {
    background: #fff;
    border-bottom: 1px solid #e5e7eb;
    padding: 0 24px;
    display: flex;
    gap: 0;
  }
  .nav-tab {
    padding: 10px 18px;
    font-size: 13px;
    font-weight: 500;
    color: #6b7280;
    cursor: pointer;
    border-bottom: 2px solid transparent;
    background: none;
    border-top: none;
    border-left: none;
    border-right: none;
  }
  .nav-tab:hover { color: #111; }
  .nav-tab.active { color: #1a1a2e; border-bottom-color: #1a1a2e; }
  .tab-panel { display: none; }
  .tab-panel.active { display: block; }
  .filter-row {
    display: flex;
    gap: 8px;
    align-items: center;
    margin-bottom: 12px;
    flex-wrap: wrap;
  }
  .filter-row label { font-size: 12px; color: #555; }
  .filter-row input, .filter-row select {
    padding: 5px 8px;
    border: 1px solid #d1d5db;
    border-radius: 5px;
    font-size: 13px;
  }
  .skipped-summary {
    display: flex;
    gap: 16px;
    flex-wrap: wrap;
    margin-bottom: 16px;
    padding: 10px 14px;
    background: #fafafa;
    border: 1px solid #e5e7eb;
    border-radius: 6px;
    font-size: 13px;
  }
  .skipped-summary-item { color: #374151; }
  .skipped-summary-item strong { color: #1a1a2e; }
  .pagination-row {
    display: flex;
    gap: 8px;
    align-items: center;
    margin-top: 12px;
    font-size: 13px;
    color: #555;
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
  <div style="margin-left:auto; display:flex; gap:8px; align-items:center;">
    <button class="btn" onclick="loadStatus()">Refresh status</button>
    <button id="btn-run-daily" class="btn btn-primary" onclick="triggerRun('daily')">Run Daily</button>
    <button id="btn-run-weekly" class="btn btn-primary" onclick="triggerRun('weekly')">Run Weekly</button>
  </div>
</div>

<nav class="nav-tabs">
  <button class="nav-tab active" onclick="showTab('allowlists')">Allowlists &amp; Runs</button>
  <button class="nav-tab" onclick="showTab('skipped')">Skipped Entities</button>
</nav>

<div id="tab-allowlists" class="tab-panel active">
<main>
  <!-- ===== LEFT: Allowlists ===== -->
  <section>
    <h2>Borough Fallback <span style="font-weight:400;color:#6b7280;font-size:12px;">(only gates properties with no zip)</span></h2>
    <div id="boro-loading" class="loading">Loading&hellip;</div>
    <table id="boro-table" style="display:none; margin-bottom:24px;">
      <thead>
        <tr>
          <th>Borough</th>
          <th>Null-zip properties</th>
          <th>Enabled</th>
        </tr>
      </thead>
      <tbody id="boro-body"></tbody>
    </table>

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
          <th title="Processed end-to-end but no contact/owner found">No match</th>
          <th>Est. cost</th>
          <th>Error</th>
        </tr>
      </thead>
      <tbody id="runs-body"></tbody>
    </table>
  </section>
</main>
</div><!-- /tab-allowlists -->

<div id="tab-skipped" class="tab-panel">
<section style="max-width:1400px; padding:20px 24px;">
  <h2>Skipped Entities</h2>
  <div id="skipped-summary" class="skipped-summary">Loading summary&hellip;</div>
  <div class="filter-row">
    <label>Reason:
      <select id="skipped-reason-filter" onchange="loadSkipped()">
        <option value="">All reasons</option>
      </select>
    </label>
    <label>Min score:
      <input type="number" id="skipped-min-score" min="0" max="1" step="0.01" style="width:70px"
        placeholder="0.0" onchange="loadSkipped()">
    </label>
    <label>Max score:
      <input type="number" id="skipped-max-score" min="0" max="1" step="0.01" style="width:70px"
        placeholder="1.0" onchange="loadSkipped()">
    </label>
    <label>Sort:
      <select id="skipped-sort" onchange="loadSkipped()">
        <option value="score_desc">Score (lowest first)</option>
        <option value="skipped_at_desc">Skipped at (newest first)</option>
      </select>
    </label>
    <button class="btn" onclick="loadSkipped()">Refresh</button>
    <button class="btn btn-primary" id="btn-requeue-reason"
      onclick="requeueSkippedByReason()"
      style="margin-left:auto" disabled>
      Re-queue all matching reason
    </button>
  </div>
  <div id="skipped-loading" class="loading">Loading&hellip;</div>
  <table id="skipped-table" style="display:none">
    <thead>
      <tr>
        <th>Name</th>
        <th>Type</th>
        <th>Reason</th>
        <th>Score</th>
        <th>Evidence</th>
        <th>Skipped At</th>
        <th>Action</th>
      </tr>
    </thead>
    <tbody id="skipped-body"></tbody>
  </table>
  <div class="pagination-row" id="skipped-pagination" style="display:none">
    <button class="btn" id="skipped-prev" onclick="skippedPage(-1)">&larr; Prev</button>
    <span id="skipped-page-info"></span>
    <button class="btn" id="skipped-next" onclick="skippedPage(1)">Next &rarr;</button>
  </div>
</section>
</div><!-- /tab-skipped -->

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
    const stale = d.worker_stale;
    const badge = document.getElementById('auto-badge');
    const label = document.getElementById('auto-label');
    if (stale) {
      badge.textContent = 'WORKER OFFLINE';
      badge.className = 'badge badge-gray';
      label.textContent = 'worker offline';
      label.title = d.worker_last_seen_at
        ? 'last heartbeat: ' + new Date(d.worker_last_seen_at).toLocaleString()
        : 'no heartbeat ever recorded';
    } else if (enabled) {
      badge.textContent = 'AUTO ON';
      badge.className = 'badge badge-green';
      label.textContent = 'enabled';
      label.title = '';
    } else {
      badge.textContent = 'AUTO OFF';
      badge.className = 'badge badge-red';
      label.textContent = 'disabled';
      label.title = '';
    }
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
      const cost = row.cost_estimated_usd != null
        ? '$' + Number(row.cost_estimated_usd).toFixed(2)
        : '—';
      const capBadge = row.stopped_by_cost_cap
        ? ' <span title="Stopped by per-run cost cap; unprocessed entities roll over to next run" style="color:#b45309;font-weight:600;">🛑 cap hit</span>'
        : '';
      tr.innerHTML = `
        <td>${row.source || '—'}</td>
        <td>${fmtDate(row.run_started_at)}</td>
        <td>${fmtDuration(row.run_started_at, row.run_finished_at)}</td>
        <td class="${statusClass(row.status)}">${row.status || '—'}${capBadge}</td>
        <td>${row.records_fetched ?? '—'}</td>
        <td>${row.records_created ?? '—'}</td>
        <td>${row.records_updated ?? '—'}</td>
        <td>${row.records_skipped ?? '—'}</td>
        <td>${row.records_no_match ?? '—'}</td>
        <td>${cost}</td>
        <td class="error-cell" title="${errFull.replace(/"/g, '&quot;')}">${err ? `<a href="/admin/api/runs" target="_blank" rel="noopener">${err}</a>` : ''}</td>`;
      tbody.appendChild(tr);
    });
    document.getElementById('runs-loading').style.display = 'none';
    document.getElementById('runs-table').style.display = '';
  } catch(e) {
    document.getElementById('runs-loading').textContent = 'Error loading runs: ' + e;
  }
}

// ── manual triggers ───────────────────────────────────────────────────────────

let _pollTimer = null;

function _setRunBtn(name, state) {
  // state is 'idle' | 'pending' | 'running'
  const btn = document.getElementById('btn-run-' + name);
  if (!btn) return;
  const label = 'Run ' + name.charAt(0).toUpperCase() + name.slice(1);
  if (state === 'running') {
    btn.disabled = true;
    btn.textContent = 'Running…';
  } else if (state === 'pending') {
    btn.disabled = true;
    btn.textContent = 'Queued…';
  } else {
    btn.disabled = false;
    btn.textContent = label;
  }
}

function _anyActive(d) {
  return d.daily !== 'idle' || d.weekly !== 'idle';
}

async function _pollRunStatus() {
  try {
    const r = await fetch('/admin/api/run-status');
    const d = await r.json();
    _setRunBtn('daily', d.daily);
    _setRunBtn('weekly', d.weekly);
    if (!_anyActive(d)) {
      clearInterval(_pollTimer);
      _pollTimer = null;
      loadRuns(); // refresh table after pipeline finishes
    }
  } catch(e) { /* ignore transient errors during polling */ }
}

async function loadRunStatus() {
  try {
    const r = await fetch('/admin/api/run-status');
    const d = await r.json();
    _setRunBtn('daily', d.daily);
    _setRunBtn('weekly', d.weekly);
    if (_anyActive(d) && !_pollTimer) {
      _pollTimer = setInterval(_pollRunStatus, 5000);
    }
  } catch(e) { console.error('run-status error', e); }
}

async function triggerRun(name) {
  _setRunBtn(name, 'pending');
  try {
    const r = await fetch('/admin/run/' + name, {method: 'POST'});
    if (r.status === 409) {
      // already queued/running — polling will track it
    } else if (!r.ok) {
      _setRunBtn(name, 'idle');
      const err = await r.json().catch(() => ({}));
      alert('Failed to queue ' + name + ': ' + (err.detail || r.status));
      return;
    }
    if (!_pollTimer) _pollTimer = setInterval(_pollRunStatus, 5000);
  } catch(e) {
    _setRunBtn(name, 'idle');
    alert('Error: ' + e);
  }
}

// ── borough fallback ──────────────────────────────────────────────────────────

async function loadBoroughs() {
  try {
    const r = await fetch('/admin/api/boroughs');
    if (!r.ok) throw new Error(r.status);
    const rows = await r.json();
    const tbody = document.getElementById('boro-body');
    tbody.innerHTML = '';
    rows.forEach(b => {
      const tr = document.createElement('tr');
      const chkId = 'chk-boro-' + b.borough_code;
      tr.innerHTML = `
        <td>${b.borough_name}</td>
        <td>${b.null_zip_property_count ?? 0}</td>
        <td>
          <label class="toggle">
            <input type="checkbox" id="${chkId}" ${b.enabled ? 'checked' : ''}
              onchange="toggleBorough('${b.borough_code}', this.checked)">
            <span class="slider"></span>
          </label>
        </td>`;
      tbody.appendChild(tr);
    });
    document.getElementById('boro-loading').style.display = 'none';
    document.getElementById('boro-table').style.display = '';
  } catch(e) {
    document.getElementById('boro-loading').textContent = 'Error loading boroughs: ' + e;
  }
}

async function toggleBorough(code, enabled) {
  try {
    const r = await fetch('/admin/api/boroughs/toggle', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({borough_code: code, enabled}),
    });
    if (!r.ok) throw new Error(r.status);
  } catch(e) {
    alert('Failed to toggle borough ' + code + ': ' + e);
    loadBoroughs();
  }
}

// ── tab switching ─────────────────────────────────────────────────────────────

let _skippedLoaded = false;

function showTab(name) {
  document.querySelectorAll('.nav-tab').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.tab-panel').forEach(el => el.classList.remove('active'));
  const activeBtn = document.querySelector('.nav-tab[onclick="showTab(\\''+name+'\\')"]');
  if (activeBtn) activeBtn.classList.add('active');
  const panel = document.getElementById('tab-' + name);
  if (panel) panel.classList.add('active');
  if (name === 'skipped' && !_skippedLoaded) {
    _skippedLoaded = true;
    loadSkippedSummary();
    loadSkipped();
  }
}

// ── skipped entities ──────────────────────────────────────────────────────────

let _skippedOffset = 0;
const _skippedLimit = 50;
let _skippedTotal = 0;

function _skippedFilterParams() {
  const params = new URLSearchParams();
  params.set('limit', _skippedLimit);
  params.set('offset', _skippedOffset);
  const reason = document.getElementById('skipped-reason-filter').value;
  if (reason) params.set('reason', reason);
  const minScore = document.getElementById('skipped-min-score').value;
  if (minScore !== '') params.set('min_score', minScore);
  const maxScore = document.getElementById('skipped-max-score').value;
  if (maxScore !== '') params.set('max_score', maxScore);
  const sort = document.getElementById('skipped-sort').value;
  params.set('sort', sort);
  return params;
}

async function loadSkipped() {
  _skippedOffset = 0;
  await _fetchSkipped();
}

async function _fetchSkipped() {
  document.getElementById('skipped-loading').style.display = '';
  document.getElementById('skipped-table').style.display = 'none';
  document.getElementById('skipped-pagination').style.display = 'none';
  try {
    const params = _skippedFilterParams();
    const r = await fetch('/admin/api/skipped?' + params.toString());
    if (!r.ok) throw new Error(r.status);
    const d = await r.json();
    _skippedTotal = d.total || 0;
    const rows = d.rows || [];
    const tbody = document.getElementById('skipped-body');
    tbody.innerHTML = '';
    rows.forEach(row => {
      const tr = document.createElement('tr');
      const evidence = (row.evidence || '').substring(0, 120);
      tr.setAttribute('data-entity-id', row.entity_id);
      tr.innerHTML = `
        <td>${row.name || '<em>unknown</em>'}</td>
        <td>${row.entity_type || '—'}</td>
        <td><code>${row.reason || '—'}</code></td>
        <td>${row.score != null ? Number(row.score).toFixed(3) : '—'}</td>
        <td style="max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${(row.evidence||'').replace(/"/g,'&quot;')}">${evidence || '—'}</td>
        <td>${fmtDate(row.skipped_at)}</td>
        <td><button class="btn" onclick="requeueSkipped('${row.entity_id}', this)">Re-queue</button></td>`;
      tbody.appendChild(tr);
    });
    document.getElementById('skipped-loading').style.display = 'none';
    document.getElementById('skipped-table').style.display = '';

    // pagination
    const pageInfo = document.getElementById('skipped-page-info');
    const start = _skippedOffset + 1;
    const end = Math.min(_skippedOffset + _skippedLimit, _skippedTotal);
    pageInfo.textContent = _skippedTotal > 0 ? (start + '–' + end + ' of ' + _skippedTotal) : '0 results';
    document.getElementById('skipped-prev').disabled = _skippedOffset <= 0;
    document.getElementById('skipped-next').disabled = (_skippedOffset + _skippedLimit) >= _skippedTotal;
    if (_skippedTotal > 0) document.getElementById('skipped-pagination').style.display = '';
  } catch(e) {
    document.getElementById('skipped-loading').textContent = 'Error loading skipped entities: ' + e;
  }
}

function skippedPage(direction) {
  _skippedOffset = Math.max(0, _skippedOffset + direction * _skippedLimit);
  _fetchSkipped();
}

async function loadSkippedSummary() {
  try {
    const r = await fetch('/admin/api/skipped/summary');
    if (!r.ok) throw new Error(r.status);
    const d = await r.json();
    const summary = document.getElementById('skipped-summary');
    const byReason = d.by_reason || {};
    const total = d.total || 0;
    if (total === 0) {
      summary.textContent = 'No currently-skipped entities.';
    } else {
      let html = '<span class="skipped-summary-item">Total skipped: <strong>' + total + '</strong></span>';
      Object.entries(byReason).sort((a,b) => b[1]-a[1]).forEach(([reason, count]) => {
        html += '<span class="skipped-summary-item"><code>' + reason + '</code>: <strong>' + count + '</strong></span>';
      });
      summary.innerHTML = html;
    }

    // Populate reason dropdown
    const sel = document.getElementById('skipped-reason-filter');
    // preserve current selection
    const current = sel.value;
    while (sel.options.length > 1) sel.remove(1);
    Object.keys(byReason).sort().forEach(reason => {
      const opt = document.createElement('option');
      opt.value = reason;
      opt.textContent = reason + ' (' + byReason[reason] + ')';
      sel.appendChild(opt);
    });
    if (current && sel.querySelector('option[value="'+current+'"]')) sel.value = current;

    // Enable/disable bulk requeue button based on reason filter
    _updateRequeueReasonBtn();
  } catch(e) {
    document.getElementById('skipped-summary').textContent = 'Error loading summary: ' + e;
  }
}

function _updateRequeueReasonBtn() {
  const reason = document.getElementById('skipped-reason-filter').value;
  document.getElementById('btn-requeue-reason').disabled = !reason;
}

document.addEventListener('DOMContentLoaded', () => {
  const sel = document.getElementById('skipped-reason-filter');
  if (sel) sel.addEventListener('change', _updateRequeueReasonBtn);
});

async function requeueSkipped(entity_id, btn) {
  if (btn) btn.disabled = true;
  try {
    const r = await fetch('/admin/api/skipped/requeue', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({entity_id}),
    });
    if (!r.ok) throw new Error(r.status);
    // optimistically remove row
    const row = document.querySelector('tr[data-entity-id="'+entity_id+'"]');
    if (row) row.remove();
    _skippedTotal = Math.max(0, _skippedTotal - 1);
    const pageInfo = document.getElementById('skipped-page-info');
    if (pageInfo) {
      const start = _skippedOffset + 1;
      const end = Math.min(_skippedOffset + _skippedLimit, _skippedTotal);
      pageInfo.textContent = _skippedTotal > 0 ? (start + '–' + end + ' of ' + _skippedTotal) : '0 results';
    }
  } catch(e) {
    alert('Re-queue failed: ' + e);
    if (btn) btn.disabled = false;
    _fetchSkipped();
  }
}

async function requeueSkippedByReason() {
  const reason = document.getElementById('skipped-reason-filter').value;
  if (!reason) { alert('Select a reason filter first.'); return; }
  if (!confirm('Re-queue all currently-skipped entities with reason "' + reason + '"? This may take a few seconds.')) return;
  const btn = document.getElementById('btn-requeue-reason');
  btn.disabled = true;
  btn.textContent = 'Re-queuing…';
  try {
    const r = await fetch('/admin/api/skipped/requeue', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({reason}),
    });
    if (!r.ok) throw new Error(r.status);
    const d = await r.json();
    alert('Re-queued ' + d.count + ' entities.');
    loadSkippedSummary();
    loadSkipped();
  } catch(e) {
    alert('Bulk re-queue failed: ' + e);
  } finally {
    btn.textContent = 'Re-queue all matching reason';
    _updateRequeueReasonBtn();
  }
}

// ── init ──────────────────────────────────────────────────────────────────────

loadStatus();
loadBoroughs();
loadZips();
loadRuns();
loadRunStatus();
</script>
</body>
</html>
"""
