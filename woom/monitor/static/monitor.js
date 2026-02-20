/* ============================================================
   woom monitor — client-side logic
   ============================================================ */

/* ── Global state ───────────────────────────────────────────────────── */
let _info = null;
let _statusRows = [];
let _jobsRefreshTimer = null;
let _runStatusTimer = null;
let _logEventSource = null;
let _logLineCount = 0;
const MAX_LOG_LINES = 1000;

/* ── Tab switching ───────────────────────────────────────────────────── */
document.querySelectorAll('.tab-link').forEach(link => {
  link.addEventListener('click', e => {
    e.preventDefault();
    const tab = link.dataset.tab;
    document.querySelectorAll('.tab-link').forEach(l => l.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    link.classList.add('active');
    document.getElementById(tab).classList.add('active');
    if (tab === 'logs' && !_logEventSource) startSSE();
    if (tab === 'artifacts') loadArtifacts();
    if (tab === 'jobs') loadStatus();
    if (tab === 'schedule') { cronBuilderUpdate(); loadCrontab(); }
  });
});

/* ── Boot ────────────────────────────────────────────────────────────── */
async function init() {
  try {
    const resp = await fetch('/api/info');
    _info = await resp.json();
    applyInfo(_info);
    await loadStatus();
    startJobsRefresh();
    startSSE();
    loadArtifacts();
    loadConfig();
    populateJobTaskSelect(_statusRows);
  } catch(e) {
    console.error('Failed to load info:', e);
  }
}

function applyInfo(info) {
  // Header subtitle
  const parts = [info.app_name, info.app_conf, info.app_exp].filter(Boolean);
  document.getElementById('header-subtitle').textContent =
    (parts.length ? parts.join(' / ') + ' — ' : '') + info.workflow_dir;

  // Scheduler badge
  const badge = document.getElementById('scheduler-badge');
  if (info.scheduler) {
    badge.textContent = 'Scheduler (read-only)';
    badge.classList.add('read-only');
  } else {
    badge.textContent = 'Background';
    badge.classList.add('bg');
  }

  // Disable run/kill buttons for scheduler hosts
  if (info.scheduler) {
    document.getElementById('scheduler-warning').style.display = 'block';
    ['btn-run', 'btn-dry-run', 'btn-force-run', 'btn-kill-all'].forEach(id => {
      document.getElementById(id).disabled = true;
    });
  }

  // Overview cards
  const cards = document.getElementById('overview-cards');
  cards.innerHTML =
    makeCard('Host', escapeHtml(info.host)) +
    makeCard('Scheduler', info.scheduler ? 'Yes' : 'No') +
    makeCard('Cycles', String(info.ncycles)) +
    makeCard('Ensemble members', String(info.nmembers)) +
    makeCard('Workflow dir', '', escapeHtml(info.workflow_dir));

  // Task tree
  document.getElementById('task-tree-content').innerHTML = taskTreeToHtml(info.task_tree);

  // Cycle tags
  const ct = document.getElementById('cycle-tags');
  if (info.cycles && info.cycles.length) {
    ct.innerHTML = info.cycles.map(c => `<span class="cycle-tag">${escapeHtml(c)}</span>`).join('');
  } else {
    ct.textContent = 'No cycles';
  }
}

function makeCard(label, value, detail = '') {
  return `<div class="card">
    <div class="card-label">${escapeHtml(label)}</div>
    <div class="card-value">${escapeHtml(value)}</div>
    ${detail ? `<div class="card-detail">${detail}</div>` : ''}
  </div>`;
}

function taskTreeToHtml(tt) {
  const stageAccent = { init: '#0969da', cycles: '#1a7f37', final: '#9a6700' };
  const entries = Object.entries(tt).filter(([, seqs]) => seqs && Object.keys(seqs).length);
  if (!entries.length) return '<p style="color:var(--color-fg-muted);font-size:13px">Empty workflow</p>';

  return entries.map(([stage, seqs]) => {
    const color = stageAccent[stage] || 'var(--color-fg-muted)';
    const seqHtml = Object.entries(seqs).map(([seq, groups]) => {
      const seqLabel = (seq && seq !== 'default')
        ? `<span class="tt-seq-label">${escapeHtml(seq)}</span>` : '';
      const groupsHtml = groups.map((group, gi) => {
        const sep = gi > 0 ? '<span class="tt-parallel-sep">//</span>' : '';
        const tasksHtml = group.map((task, ti) => {
          const arrow = ti > 0 ? '<span class="tt-arrow">→</span>' : '';
          return `${arrow}<span class="tt-task-node" title="${escapeHtml(task)}"
            onclick="highlightTask(this,'${escapeHtml(task)}')">${escapeHtml(task)}</span>`;
        }).join('');
        return `${sep}<span class="tt-group">${tasksHtml}</span>`;
      }).join('');
      return `<div class="tt-seq">${seqLabel}<div class="tt-groups">${groupsHtml}</div></div>`;
    }).join('');
    return `<div class="tt-stage">
      <div class="tt-stage-label" style="border-left-color:${color}">${escapeHtml(stage)}</div>
      ${seqHtml}
    </div>`;
  }).join('');
}

function highlightTask(el, taskName) {
  document.querySelectorAll('.tt-task-node').forEach(n => n.classList.remove('tt-task-selected'));
  el.classList.add('tt-task-selected');
  // Mirror highlight in the Jobs table
  document.querySelectorAll('#jobs-tbody tr').forEach(row => {
    const taskCell = row.cells[2];
    row.classList.toggle('tt-job-highlight', taskCell && taskCell.textContent === taskName);
  });
}

/* ── Status / Jobs tab ───────────────────────────────────────────────── */
async function loadStatus() {
  try {
    const resp = await fetch('/api/status');
    _statusRows = await resp.json();
    renderJobsTable(_statusRows);
    updateStatusSummary(_statusRows);
    populateJobTaskSelect(_statusRows);
    document.getElementById('jobs-count').textContent = String(_statusRows.length);
    document.getElementById('jobs-refresh-indicator').textContent =
      'Last update: ' + new Date().toLocaleTimeString();
  } catch(e) {
    console.error('Status fetch failed:', e);
  }
}

function startJobsRefresh() {
  if (_jobsRefreshTimer) clearInterval(_jobsRefreshTimer);
  _jobsRefreshTimer = setInterval(loadStatus, 3000);
}

function renderJobsTable(rows) {
  const tbody = document.getElementById('jobs-tbody');
  if (!rows || !rows.length) {
    tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#888">No tasks found</td></tr>';
    return;
  }
  const isScheduler = _info && _info.scheduler;
  tbody.innerHTML = rows.map(row => {
    const statusClass = 'badge-' + row.status.toLowerCase();
    const canKill = !isScheduler && row.status === 'RUNNING';
    const killBtn = canKill
      ? `<button class="btn btn-danger btn-sm" onclick="doKillJob(${escapeHtml(JSON.stringify(row.jobid))},${escapeHtml(JSON.stringify(row.task))},${escapeHtml(JSON.stringify(row.cycle))})">Kill</button>`
      : '';
    const logsBtn = `<button class="btn btn-secondary btn-sm" onclick="viewJobLogs(${escapeHtml(JSON.stringify(row.submdir))})">Logs</button>`;
    return `<tr>
      <td><span class="badge ${statusClass}">${escapeHtml(row.status)}</span></td>
      <td style="font-family:var(--font-mono);font-size:12px">${escapeHtml(row.jobid)}</td>
      <td>${escapeHtml(row.task)}</td>
      <td>${escapeHtml(row.cycle)}</td>
      <td>${escapeHtml(row.member || '')}</td>
      <td style="font-family:var(--font-mono);font-size:11px">${escapeHtml(shortPath(row.submdir))}</td>
      <td style="white-space:nowrap">${killBtn} ${logsBtn}</td>
    </tr>`;
  }).join('');
}

function updateStatusSummary(rows) {
  const counts = {};
  rows.forEach(r => { counts[r.status] = (counts[r.status] || 0) + 1; });
  const container = document.getElementById('status-summary-cards');
  container.innerHTML = Object.entries(counts)
    .map(([s, n]) => {
      const cls = 'badge-' + s.toLowerCase();
      return `<div class="card">
        <div class="card-label">${escapeHtml(s)}</div>
        <div class="card-value"><span class="badge ${cls}">${n}</span></div>
      </div>`;
    }).join('');
}

function shortPath(p) {
  if (!p) return '';
  const parts = p.replace(/\\/g, '/').split('/');
  return parts.slice(-3).join('/');
}

/* ── Config view ─────────────────────────────────────────────────────── */
async function loadConfig() {
  try {
    const resp = await fetch('/api/config');
    const data = await resp.json();
    document.getElementById('wf-cfg-body').innerHTML = renderConfigDict(data.workflow);
    document.getElementById('tasks-cfg-body').innerHTML =
      Object.entries(data.tasks).map(([name, cfg]) =>
        `<details class="cfg-task-details">
          <summary class="cfg-task-summary">${escapeHtml(name)}</summary>
          <div class="cfg-body">${renderConfigDict(cfg)}</div>
        </details>`
      ).join('');
  } catch(e) {
    console.error('Config fetch failed:', e);
  }
}

function renderConfigDict(obj) {
  if (!obj || !Object.keys(obj).length)
    return '<p class="cfg-empty">—</p>';
  return Object.entries(obj).map(([key, val]) => {
    if (val && typeof val === 'object' && !Array.isArray(val)) {
      return `<details class="cfg-sub-details">
        <summary class="cfg-sub-summary">${escapeHtml(key)}</summary>
        <div class="cfg-sub-body">${renderConfigDict(val)}</div>
      </details>`;
    }
    const display = Array.isArray(val)
      ? val.map(escapeHtml).join(', ')
      : escapeHtml(String(val));
    const isMultiline = typeof val === 'string' && val.includes('\n');
    const valHtml = isMultiline
      ? `<pre class="cfg-code">${display}</pre>`
      : `<span class="cfg-val">${display}</span>`;
    return `<div class="cfg-row">
      <span class="cfg-key">${escapeHtml(key)}</span>${valHtml}
    </div>`;
  }).join('');
}

/* ── Run / Kill actions ──────────────────────────────────────────────── */
async function doRun(dry, force) {
  showRunStatus(true, 'Starting...');
  try {
    const resp = await fetch('/api/run', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({dry, force})
    });
    const data = await resp.json();
    if (resp.ok) {
      startRunStatusPolling();
    } else {
      showRunStatus(false, 'Error: ' + (data.error || 'Unknown error'), true);
    }
  } catch(e) {
    showRunStatus(false, 'Request failed', true);
  }
}

async function doKillAll() {
  if (!confirm('Kill all running jobs?')) return;
  await fetch('/api/kill', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: '{}'
  });
  await loadStatus();
}

async function doKillJob(jobid, task, cycle) {
  if (!confirm(`Kill job ${jobid} (${task})?`)) return;
  await fetch('/api/kill', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({jobid, task_name: task, cycle})
  });
  await loadStatus();
}

function showRunStatus(spinning, text, isError) {
  const bar = document.getElementById('run-status-bar');
  const spinner = document.getElementById('run-spinner');
  const statusText = document.getElementById('run-status-text');
  bar.style.display = 'flex';
  spinner.style.display = spinning ? 'block' : 'none';
  statusText.textContent = text;
  statusText.style.color = isError ? 'var(--color-danger-fg)' : '';
}

function startRunStatusPolling() {
  if (_runStatusTimer) clearInterval(_runStatusTimer);
  _runStatusTimer = setInterval(async () => {
    try {
      const resp = await fetch('/api/run_status');
      const data = await resp.json();
      if (data.error) {
        showRunStatus(false, 'Failed: ' + data.error, true);
        clearInterval(_runStatusTimer);
      } else if (!data.running) {
        showRunStatus(false, 'Completed');
        clearInterval(_runStatusTimer);
        await loadStatus();
      } else {
        showRunStatus(true, 'Running...');
      }
    } catch(e) {}
  }, 2000);
}

/* ── SSE Logs ────────────────────────────────────────────────────────── */
function startSSE() {
  if (_logEventSource) { _logEventSource.close(); }
  _logEventSource = new EventSource('/api/logs/stream');
  _logEventSource.onmessage = evt => {
    try {
      const d = JSON.parse(evt.data);
      if (d.ping) return;
      appendLogLine(d.level, d.message);
    } catch(e) {}
  };
  _logEventSource.onerror = () => {
    _logEventSource = null;
    setTimeout(startSSE, 5000);
  };
}

function appendLogLine(level, message) {
  const container = document.getElementById('log-lines');
  const div = document.createElement('div');
  div.className = 'log-line ' + (level || 'INFO');
  div.textContent = message;
  container.appendChild(div);
  _logLineCount++;
  while (_logLineCount > MAX_LOG_LINES) {
    container.removeChild(container.firstChild);
    _logLineCount--;
  }
  if (document.getElementById('autoscroll-cb').checked) {
    const viewer = container.closest('.log-viewer');
    if (viewer) viewer.scrollTop = viewer.scrollHeight;
  }
}

/* ── Job log viewer ──────────────────────────────────────────────────── */
function populateJobTaskSelect(rows) {
  const sel = document.getElementById('job-task-select');
  const current = sel.value;
  sel.innerHTML = '<option value="">— select a task —</option>';
  (rows || []).forEach(row => {
    const opt = document.createElement('option');
    const key = JSON.stringify({submdir: row.submdir, task: row.task, cycle: row.cycle});
    opt.value = key;
    opt.textContent = [row.task, row.cycle, row.member].filter(Boolean).join(' / ');
    sel.appendChild(opt);
  });
  if (current) sel.value = current;
}

async function loadJobLog() {
  const sel = document.getElementById('job-task-select');
  const content = document.getElementById('job-log-content');
  if (!sel.value) { content.textContent = 'Select a task above to view its job log.'; return; }
  const {submdir} = JSON.parse(sel.value);
  const fileType = document.querySelector('input[name="job-file-type"]:checked').value;
  try {
    const resp = await fetch(`/api/job/file?submdir=${encodeURIComponent(submdir)}&type=${fileType}`);
    const data = await resp.json();
    content.textContent = data.exists ? (data.content || '(empty file)') : '(file not found)';
  } catch(e) {
    content.textContent = 'Error loading file.';
  }
}

function viewJobLogs(submdir) {
  document.querySelectorAll('.tab-link').forEach(l => l.classList.remove('active'));
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelector('.tab-link[data-tab="logs"]').classList.add('active');
  document.getElementById('logs').classList.add('active');
  if (!_logEventSource) startSSE();
  const sel = document.getElementById('job-task-select');
  for (const opt of sel.options) {
    if (opt.value) {
      try { if (JSON.parse(opt.value).submdir === submdir) { sel.value = opt.value; break; } }
      catch(e) {}
    }
  }
  loadJobLog();
}

/* ── Artifacts tab ───────────────────────────────────────────────────── */
async function loadArtifacts() {
  try {
    const resp = await fetch('/api/artifacts');
    const rows = await resp.json();
    document.getElementById('artifacts-count').textContent = String(rows.length);
    const tbody = document.getElementById('artifacts-tbody');
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;color:#888">No artifacts</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map(r => {
      const existsHtml = r['EXISTS?']
        ? '<span class="check-yes">✓ exists</span>'
        : '<span class="check-no">✗ missing</span>';
      const pathHtml = r['EXISTS?']
        ? `<a href="/api/files?path=${encodeURIComponent(r.PATH)}" target="_blank">${escapeHtml(r.PATH)}</a>`
        : escapeHtml(r.PATH || '');
      return `<tr>
        <td>${escapeHtml(r.TASK)}</td>
        <td>${escapeHtml(r.ARTIFACT)}</td>
        <td class="artifact-path">${pathHtml}</td>
        <td>${existsHtml}</td>
      </tr>`;
    }).join('');
  } catch(e) {
    console.error('Artifacts fetch failed:', e);
  }
}

/* ── Schedule / crontab tab ──────────────────────────────────────────── */
let _cronInfo = null;

async function loadCrontab() {
  try {
    const resp = await fetch('/api/crontab');
    _cronInfo = await resp.json();
    if (_cronInfo.error) {
      document.getElementById('cron-current-wrap').innerHTML =
        `<p class="cron-no-entries" style="color:var(--color-danger-fg)">Error: ${escapeHtml(_cronInfo.error)}</p>`;
      return;
    }
    renderCronEntries(_cronInfo.relevant);
    updateCronPreview();
  } catch(e) {
    console.error('Crontab fetch failed:', e);
  }
}

function renderCronEntries(lines) {
  const wrap = document.getElementById('cron-current-wrap');
  if (!lines || !lines.length) {
    wrap.innerHTML = '<p class="cron-no-entries">No schedule set for this workflow yet.</p>';
    return;
  }
  const rows = lines.map(ln => `<tr class="cron-entry-row">
    <td>${escapeHtml(ln)}</td>
  </tr>`).join('');
  wrap.innerHTML = `<div class="table-wrap"><table class="data-table"><tbody>${rows}</tbody></table></div>`;
}

function cronBuilderUpdate() {
  const period = document.getElementById('cron-period-sel').value;
  const minVal = Math.min(59, Math.max(0, parseInt(document.getElementById('cron-minute-offset').value) || 0));
  const hourVal = Math.min(23, Math.max(0, parseInt(document.getElementById('cron-hour-val').value) || 0));

  const minuteWrap = document.getElementById('cron-minute-wrap');
  const hourWrap   = document.getElementById('cron-hour-wrap');

  let expr = '';
  if (period === '5m')  { expr = '*/5 * * * *';              minuteWrap.style.display = 'none'; hourWrap.style.display = 'none'; }
  else if (period === '15m') { expr = '*/15 * * * *';        minuteWrap.style.display = 'none'; hourWrap.style.display = 'none'; }
  else if (period === '30m') { expr = '*/30 * * * *';        minuteWrap.style.display = 'none'; hourWrap.style.display = 'none'; }
  else if (period === '1h')  { expr = `${minVal} * * * *`;   minuteWrap.style.display = ''; hourWrap.style.display = 'none'; }
  else if (period === '2h')  { expr = `${minVal} */2 * * *`; minuteWrap.style.display = ''; hourWrap.style.display = 'none'; }
  else if (period === '3h')  { expr = `${minVal} */3 * * *`; minuteWrap.style.display = ''; hourWrap.style.display = 'none'; }
  else if (period === '4h')  { expr = `${minVal} */4 * * *`; minuteWrap.style.display = ''; hourWrap.style.display = 'none'; }
  else if (period === '6h')  { expr = `${minVal} */6 * * *`; minuteWrap.style.display = ''; hourWrap.style.display = 'none'; }
  else if (period === '8h')  { expr = `${minVal} */8 * * *`; minuteWrap.style.display = ''; hourWrap.style.display = 'none'; }
  else if (period === '12h') { expr = `${minVal} */12 * * *`; minuteWrap.style.display = ''; hourWrap.style.display = 'none'; }
  else if (period === '24h') { expr = `${minVal} ${hourVal} * * *`; minuteWrap.style.display = ''; hourWrap.style.display = ''; }

  document.getElementById('cron-expr-input').value = expr;
  updateCronPreview();
}

function updateCronPreview() {
  const expr = (document.getElementById('cron-expr-input').value || '').trim();
  const preview = document.getElementById('cron-preview');
  if (!expr || !_cronInfo) { preview.textContent = '—'; return; }
  preview.textContent =
    `${expr}  cd ${_cronInfo.workflow_dir} && ${_cronInfo.woom_cmd} run >> ${_cronInfo.log_file} 2>&1  # woom:${_cronInfo.workflow_dir}`;
}

async function applyCron() {
  const expr = (document.getElementById('cron-expr-input').value || '').trim();
  const status = document.getElementById('cron-status');
  if (!expr) {
    status.textContent = 'Enter a crontab expression first.';
    status.style.color = 'var(--color-danger-fg)'; return;
  }
  status.textContent = 'Applying...'; status.style.color = '';
  try {
    const resp = await fetch('/api/crontab', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({action: 'add', cron_expr: expr})
    });
    const data = await resp.json();
    if (resp.ok) {
      status.textContent = '✓ Schedule applied';
      status.style.color = 'var(--color-success-fg)';
      await loadCrontab();
    } else {
      status.textContent = 'Error: ' + (data.error || 'unknown');
      status.style.color = 'var(--color-danger-fg)';
    }
  } catch(e) {
    status.textContent = 'Request failed';
    status.style.color = 'var(--color-danger-fg)';
  }
}

async function removeCron() {
  if (!confirm('Remove the woom schedule for this workflow?')) return;
  const status = document.getElementById('cron-status');
  status.textContent = 'Removing...'; status.style.color = '';
  try {
    const resp = await fetch('/api/crontab', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({action: 'remove'})
    });
    const data = await resp.json();
    if (resp.ok) {
      status.textContent = '✓ Schedule removed';
      status.style.color = 'var(--color-success-fg)';
      await loadCrontab();
    } else {
      status.textContent = 'Error: ' + (data.error || 'unknown');
      status.style.color = 'var(--color-danger-fg)';
    }
  } catch(e) {
    status.textContent = 'Request failed';
    status.style.color = 'var(--color-danger-fg)';
  }
}

/* ── Shutdown server ─────────────────────────────────────────────────── */
async function stopServer() {
  if (!confirm('Shut down the woom monitor server?')) return;
  // Stop auto-refresh timers and SSE connection before server goes down
  if (_jobsRefreshTimer) clearInterval(_jobsRefreshTimer);
  if (_runStatusTimer)   clearInterval(_runStatusTimer);
  if (_logEventSource)   { _logEventSource.close(); _logEventSource = null; }
  try {
    await fetch('/api/stop', {method: 'POST'});
  } catch(e) { /* server may close before response arrives — that's fine */ }
  document.body.innerHTML =
    '<div style="display:flex;align-items:center;justify-content:center;height:100vh;' +
    'font-family:sans-serif;color:#8b949e;flex-direction:column;gap:12px">' +
    '<div style="font-size:24px">■</div>' +
    '<div>woom monitor shut down — you can close this tab.</div>' +
    '</div>';
}

/* ── Dark / light theme toggle ───────────────────────────────────────── */
function _systemIsDark() {
  return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
}

function _effectiveTheme() {
  const stored = document.documentElement.getAttribute('data-theme');
  return stored || (_systemIsDark() ? 'dark' : 'light');
}

function _updateThemeButton() {
  const btn = document.getElementById('theme-toggle');
  if (!btn) return;
  const dark = _effectiveTheme() === 'dark';
  btn.textContent = dark ? '☀' : '☾';
  btn.title = dark ? 'Switch to light mode' : 'Switch to dark mode';
}

function toggleTheme() {
  const next = _effectiveTheme() === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('woom-theme', next);
  _updateThemeButton();
}

// Initialise button label + listen for system-level changes
_updateThemeButton();
if (window.matchMedia) {
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
    if (!localStorage.getItem('woom-theme')) _updateThemeButton();
  });
}

/* ── Utilities ───────────────────────────────────────────────────────── */
function escapeHtml(s) {
  if (!s) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/* ── Boot ────────────────────────────────────────────────────────────── */
init();
