/* ============================================================
   woom monitor — client-side logic
   ============================================================ */

/* ── Global state ───────────────────────────────────────────────────── */
let _info = null;
let _statusRows = [];
let _configData = null;
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
    updateTaskTreeStatus();
    renderPipeline();
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
  // Header — app title + workflow path
  const parts = [info.app_name, info.app_conf, info.app_exp].filter(Boolean);
  const appTitle = parts.length ? parts.join(' / ') : 'woom monitor';
  document.getElementById('header-app-name').textContent = appTitle;
  document.getElementById('header-wf-path').textContent = info.workflow_dir;
  document.title = appTitle + ' — woom monitor';
  if (info.woom_version)
    document.getElementById('about-version').textContent = 'v' + info.woom_version;

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
    document.getElementById('scheduler-warning-inline').style.display = 'flex';
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
  if (!tt) return '';
  const entries = Object.entries(tt).filter(([, seqs]) => seqs && Object.keys(seqs).length);
  if (!entries.length) return '<p class="tt-empty">Empty workflow</p>';

  // Which tasks are in the cycles stage?
  const cyclesTasks = new Set();
  if (tt.cycles)
    for (const groups of Object.values(tt.cycles))
      for (const group of groups)
        for (const t of group) cyclesTasks.add(t);

  const STAGE_META = {
    prolog: { icon: '▶', color: '#0969da', label: 'Prolog'  },
    cycles: { icon: '↻', color: '#1a7f37', label: 'Cycles'  },
    epilog: { icon: '◀', color: '#9a6700', label: 'Epilog'  },
  };

  const parts = entries.map(([stage, seqs]) => {
    const meta = STAGE_META[stage] || { icon: '▸', color: 'var(--color-fg-muted)', label: stage };
    const isLoop = stage === 'cycles';
    const ncyc = _info && _info.ncycles ? _info.ncycles : 0;

    // Count tasks in this stage
    let taskCount = 0;
    for (const groups of Object.values(seqs)) for (const g of groups) taskCount += g.length;

    const loopBadge = isLoop
      ? `<span class="tt-loop-badge">↻ ${ncyc ? ncyc + ' cycles' : 'loop'}</span>` : '';

    const seqEntries = Object.entries(seqs);
    const seqHtml = seqEntries.map(([seq, groups], si) => {
      const connHtml = si > 0 ? '<div class="tt-seq-conn"><span>↓</span></div>' : '';
      const labelHtml = (seq && seq !== 'default')
        ? `<span class="tt-seq-label">${escapeHtml(seq)}</span>` : '';

      const groupsHtml = groups.map((group, gi) => {
        const sepHtml = gi > 0 ? '<span class="tt-para-sep">⫽</span>' : '';
        const tasksHtml = group.map((task, ti) => {
          const arrowHtml = ti > 0 ? '<span class="tt-arrow">→</span>' : '';
          const inner = isLoop
            ? `<span class="tt-mini-bar">
                 <span class="tt-ms tt-ms-s" data-bar-s="${escapeHtml(task)}" style="flex:0"></span>
                 <span class="tt-ms tt-ms-r" data-bar-r="${escapeHtml(task)}" style="flex:0"></span>
                 <span class="tt-ms tt-ms-f" data-bar-f="${escapeHtml(task)}" style="flex:0"></span>
                 <span class="tt-ms tt-ms-n" data-bar-n="${escapeHtml(task)}" style="flex:1"></span>
               </span>
               <span class="tt-task-name">${escapeHtml(task)}</span>
               <span class="tt-mini-count" data-cnt="${escapeHtml(task)}">—</span>`
            : `<span class="tt-dot" data-dot="${escapeHtml(task)}"></span>
               <span class="tt-task-name">${escapeHtml(task)}</span>`;
          return `${arrowHtml}<span class="tt-task-node${isLoop ? ' tt-task-loop' : ''}"
              data-task="${escapeHtml(task)}" title="${escapeHtml(task)}"
              onclick="highlightTask(this,'${escapeHtml(task)}')">${inner}</span>`;
        }).join('');
        return `${sepHtml}<span class="tt-group">${tasksHtml}</span>`;
      }).join('');

      return `${connHtml}<div class="tt-seq-row">${labelHtml}<div class="tt-groups">${groupsHtml}</div></div>`;
    }).join('');

    return `<details class="tt-stage" open data-stage="${escapeHtml(stage)}">
      <summary class="tt-stage-hdr" style="--sc:${meta.color}">
        <span class="tt-chevron">▸</span>
        <span class="tt-stage-icon">${meta.icon}</span>
        <span class="tt-stage-name">${escapeHtml(meta.label)}</span>
        ${loopBadge}
        <span class="tt-stage-count">${taskCount} task${taskCount !== 1 ? 's' : ''}</span>
      </summary>
      <div class="tt-stage-body">${seqHtml}</div>
    </details>`;
  });

  return parts.join('<div class="tt-stage-conn">↓</div>');
}

function updateTaskTreeStatus() {
  if (!_statusRows || !_info) return;
  const ncyc = _info.ncycles || 0;

  // --- Status dots (non-cycles tasks) ---
  document.querySelectorAll('[data-dot]').forEach(dot => {
    const task = dot.dataset.dot;
    const rows = _statusRows.filter(r => r.task === task);
    let cls = 'tt-dot-unknown';
    if (rows.length) {
      const ss = rows.map(r => r.status);
      if (ss.some(s => ['FAILED','ERROR','KILLED','TERMINATED'].includes(s)))  cls = 'tt-dot-failed';
      else if (ss.some(s => ['RUNNING','COMPLETING','EXITING','INQUEUE','PENDING'].includes(s))) cls = 'tt-dot-running';
      else if (ss.every(s => s === 'SUCCESS'))   cls = 'tt-dot-success';
      else if (ss.some(s => s === 'SUCCESS'))    cls = 'tt-dot-partial';
      else cls = 'tt-dot-pending';
    }
    dot.className = 'tt-dot ' + cls;
  });

  // --- Mini bars (cycles tasks) ---
  // collect bar elements per task (all four segments share same task name attribute)
  const tasks = new Set([...document.querySelectorAll('[data-bar-s]')].map(e => e.dataset.barS));
  tasks.forEach(task => {
    const rows = _statusRows.filter(r => r.task === task);
    const c = { s: 0, r: 0, f: 0 };
    for (const r of rows) {
      if (r.status === 'SUCCESS') c.s++;
      else if (['RUNNING','COMPLETING','EXITING'].includes(r.status)) c.r++;
      else if (['FAILED','ERROR','KILLED','TERMINATED'].includes(r.status)) c.f++;
    }
    const n = Math.max(0, ncyc - c.s - c.r - c.f);

    document.querySelectorAll(`[data-bar-s="${CSS.escape(task)}"]`).forEach(e => { e.style.flex = c.s; });
    document.querySelectorAll(`[data-bar-r="${CSS.escape(task)}"]`).forEach(e => { e.style.flex = c.r; });
    document.querySelectorAll(`[data-bar-f="${CSS.escape(task)}"]`).forEach(e => { e.style.flex = c.f; });
    document.querySelectorAll(`[data-bar-n="${CSS.escape(task)}"]`).forEach(e => { e.style.flex = n;   });

    document.querySelectorAll(`[data-cnt="${CSS.escape(task)}"]`).forEach(el => {
      el.textContent = ncyc ? `${c.s}/${ncyc}` : '—';
      el.style.color = (c.s === ncyc && ncyc > 0) ? 'var(--color-success-fg)' : '';
    });
  });
}

function highlightTask(el, taskName) {
  document.querySelectorAll('.tt-task-node').forEach(n => n.classList.remove('tt-task-selected'));
  el.classList.add('tt-task-selected');
  showTaskDetail(taskName);
}

function showTaskDetail(taskName) {
  const panel = document.getElementById('task-detail-panel');
  document.getElementById('task-detail-name').textContent = taskName;

  // Status rows for this task
  const rows = _statusRows.filter(r => r.task === taskName);
  let statusHtml = '';
  if (rows.length) {
    statusHtml = rows.map(r => {
      const cls = 'badge badge-' + r.status.toLowerCase();
      const label = [r.cycle, r.member].filter(Boolean).join('/');
      return `<span class="${cls}" title="${escapeHtml(r.submdir)}">${escapeHtml(r.status)}${label ? ' <small>'+escapeHtml(label)+'</small>' : ''}</span>`;
    }).join(' ');
  }

  // Config for this task
  const cfg = _configData && _configData.tasks && _configData.tasks[taskName];
  let cfgHtml = '';
  if (cfg) {
    const content  = cfg.content  || {};
    const res      = cfg.resources || {};
    const arts     = cfg.artifacts || {};
    const deps     = cfg.deps || {};

    // Commandline
    if (content.commandline) {
      const cmd = content.commandline;
      const isMulti = cmd.includes('\n');
      cfgHtml += `<div class="td-section-label">Commandline</div>`;
      cfgHtml += isMulti
        ? `<pre class="td-code">${escapeHtml(cmd.trim())}</pre>`
        : `<code class="td-inline-code">${escapeHtml(cmd.trim())}</code>`;
    }

    // Resources
    const resEntries = Object.entries(res).filter(([, v]) => v && v !== 'None');
    if (resEntries.length) {
      cfgHtml += `<div class="td-section-label">Resources</div>`;
      cfgHtml += `<div class="td-kv-row">${
        resEntries.map(([k, v]) =>
          `<span class="td-kv"><span class="td-k">${escapeHtml(k)}</span><span class="td-v">${escapeHtml(String(v))}</span></span>`
        ).join('')
      }</div>`;
    }

    // Artifacts
    const artEntries = Object.entries(arts);
    if (artEntries.length) {
      cfgHtml += `<div class="td-section-label">Artifacts</div>`;
      cfgHtml += artEntries.map(([name, spec]) => {
        const path = (spec && typeof spec === 'object') ? (spec.path || '') : String(spec || '');
        return `<div class="td-art-row"><span class="td-k">${escapeHtml(name)}</span><span class="td-v td-mono">${escapeHtml(Array.isArray(path) ? path.join(', ') : path)}</span></div>`;
      }).join('');
    }

    // Dependencies
    const depEntries = Object.keys(deps);
    if (depEntries.length) {
      cfgHtml += `<div class="td-section-label">Dependencies</div>`;
      cfgHtml += `<div class="td-kv-row">${depEntries.map(d => `<span class="tt-task-node td-dep-node" onclick="highlightTask(this,'${escapeHtml(d)}')">${escapeHtml(d)}</span>`).join('')}</div>`;
    }
  }

  document.getElementById('task-detail-body').innerHTML =
    (statusHtml ? `<div class="td-status-row">${statusHtml}</div>` : '') + cfgHtml;
  panel.style.display = 'block';
}

function closeTaskDetail() {
  document.getElementById('task-detail-panel').style.display = 'none';
  document.querySelectorAll('.tt-task-node').forEach(n => n.classList.remove('tt-task-selected'));
}

/* ── Status / Jobs tab ───────────────────────────────────────────────── */
async function loadStatus() {
  try {
    const resp = await fetch('/api/status');
    _statusRows = await resp.json();
    renderJobsTable(_statusRows);
    updateStatusSummary(_statusRows);
    populateJobTaskSelect(_statusRows);
    updateTaskTreeStatus();
    // Refresh detail panel if a task is currently selected
    const selNode = document.querySelector('.tt-task-selected');
    if (selNode) showTaskDetail(selNode.dataset.task || selNode.textContent);
    renderPipeline();
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

/* ── Pipeline view ───────────────────────────────────────────────────── */
function renderPipeline() {
  const el = document.getElementById('pipeline-content');
  if (!el || !_info || !_info.task_tree) return;

  // Ordered task list per stage
  const stages = [];
  for (const [stage, seqs] of Object.entries(_info.task_tree)) {
    if (!seqs || !Object.keys(seqs).length) continue;
    const tasks = [];
    for (const groups of Object.values(seqs))
      for (const group of groups)
        for (const t of group)
          if (!tasks.includes(t)) tasks.push(t);
    stages.push({ stage, tasks, isLoop: stage === 'cycles' });
  }
  if (!stages.length) { el.innerHTML = ''; return; }

  // Per-task status counts from _statusRows
  const counts = {};
  for (const row of _statusRows) {
    const c = counts[row.task] || (counts[row.task] = {});
    c[row.status] = (c[row.status] || 0) + 1;
  }

  const ncycles = _info.ncycles || 0;
  const allCycles = _info.cycles || [];

  // Current active cycle(s): running/queued first, else last touched
  const activeCycleSet = new Set();
  const touchedCycleSet = new Set();
  for (const row of _statusRows) {
    if (!row.cycle) continue;
    if (['RUNNING','COMPLETING','EXITING','INQUEUE','PENDING'].includes(row.status))
      activeCycleSet.add(row.cycle);
    if (row.status !== 'NOTSUBMITTED' && row.status !== 'UNKNOWN')
      touchedCycleSet.add(row.cycle);
  }
  // Fall back to last touched cycle if nothing active
  let currentCycleLabel = null;
  if (activeCycleSet.size > 0) {
    const active = allCycles.filter(c => activeCycleSet.has(c));
    currentCycleLabel = active.length === 1 ? active[0] : active.length + ' active';
  } else {
    // Last cycle in order that was touched
    for (let i = allCycles.length - 1; i >= 0; i--) {
      if (touchedCycleSet.has(allCycles[i])) { currentCycleLabel = allCycles[i]; break; }
    }
  }

  // Status → colour bucket: success | running | failed | waiting | none
  const bucket = s => {
    s = (s || '').toUpperCase();
    if (s === 'SUCCESS')   return 'success';
    if (['RUNNING','COMPLETING','EXITING'].includes(s)) return 'running';
    if (['FAILED','ERROR','KILLED','TERMINATED'].includes(s)) return 'failed';
    if (['INQUEUE','PENDING'].includes(s)) return 'waiting';
    return 'none';
  };

  let html = '<div class="pl-row">';
  stages.forEach(({ stage, tasks, isLoop }, si) => {
    if (si > 0) html += '<div class="pl-connector">→</div>';
    html += `<div class="pl-stage${isLoop ? ' pl-stage-loop' : ''}">
      <div class="pl-stage-hdr">
        ${isLoop ? '<span class="pl-loop-icon">↻</span>' : ''}
        <span>${escapeHtml(stage)}</span>
        ${isLoop && ncycles ? `<span class="pl-ncycles">${ncycles}</span>` : ''}
      </div>`;

    for (const task of tasks) {
      const tc = counts[task] || {};
      const click = `showTaskDetail(${escapeHtml(JSON.stringify(task))})`;
      html += `<div class="pl-task" onclick="${click}"><span class="pl-task-name">${escapeHtml(task)}</span>`;

      if (isLoop && ncycles > 0) {
        // Segmented bar — aggregate by colour bucket
        const buckets = { success: 0, running: 0, failed: 0, waiting: 0, none: 0 };
        let total = 0;
        for (const [st, n] of Object.entries(tc)) { buckets[bucket(st)] += n; total += n; }
        buckets.none += Math.max(0, ncycles - total);

        let bar = '';
        for (const [b, n] of Object.entries(buckets))
          if (n > 0) bar += `<div class="pl-seg pl-seg-${b}" style="flex:${n}" title="${n} ${b}"></div>`;

        const done = buckets.success, run = buckets.running, fail = buckets.failed;
        let sumCls = 'pl-sum-none', sumTxt = `${done}/${ncycles}`;
        if (fail > 0)        { sumCls = 'pl-sum-fail'; sumTxt = `${fail} failed`; }
        else if (run > 0)    { sumCls = 'pl-sum-run';  sumTxt = `${run} running`; }
        else if (done === ncycles) { sumCls = 'pl-sum-ok'; sumTxt = `${ncycles} ✓`; }

        html += `<div class="pl-bar">${bar}</div><span class="pl-sum ${sumCls}">${sumTxt}</span>`;
      } else {
        const topStatus = Object.entries(tc).sort((a, b) => b[1] - a[1])[0];
        const st = topStatus ? topStatus[0].toLowerCase() : 'notsubmitted';
        html += `<span class="pl-dot badge-${st}"></span>`;
      }
      html += '</div>';
    }
    // Current-cycle footer for the loop stage
    if (isLoop && currentCycleLabel) {
      const isActive = activeCycleSet.size > 0;
      html += `<div class="pl-current-cycle${isActive ? ' pl-current-active' : ''}">` +
        `${isActive ? '↻' : '↳'} ${escapeHtml(currentCycleLabel)}</div>`;
    }
    html += '</div>';
  });
  html += '</div>';
  el.innerHTML = html;
}

/* ── Config view ─────────────────────────────────────────────────────── */
async function loadConfig() {
  try {
    const resp = await fetch('/api/config');
    const data = await resp.json();
    _configData = data;
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

async function doClean() {
  const dry          = document.getElementById('clean-dry').checked;
  const submDirs     = document.getElementById('clean-submission-dirs').checked;
  const logFiles     = document.getElementById('clean-log-files').checked;
  const runDirs      = document.getElementById('clean-run-dirs').checked;
  const artifacts    = document.getElementById('clean-artifacts').checked;
  const extraRaw     = (document.getElementById('clean-extra').value || '').trim();
  const extraFiles   = extraRaw ? extraRaw.split(/[\s,]+/).filter(Boolean) : [];

  const statusEl = document.getElementById('clean-status');
  statusEl.style.color = '';

  if (!submDirs && !logFiles && !runDirs && !artifacts && !extraFiles.length) {
    statusEl.textContent = 'Nothing selected.';
    statusEl.style.color = 'var(--color-attention-fg)';
    return;
  }

  if (!dry) {
    const items = [];
    if (submDirs)        items.push('submission directories');
    if (logFiles)        items.push('log files');
    if (runDirs)         items.push('⚠ run directories');
    if (artifacts)       items.push('⚠ artifacts');
    if (extraFiles.length) items.push('extra: ' + extraFiles.join(', '));
    if (!confirm('Permanently delete:\n• ' + items.join('\n• ') + '\n\nThis cannot be undone. Continue?')) return;
  }

  statusEl.textContent = dry ? 'Running dry clean…' : 'Cleaning…';
  try {
    const resp = await fetch('/api/clean', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        submission_dirs: submDirs, log_files: logFiles,
        run_dirs: runDirs, artifacts, extra_files: extraFiles, dry,
      })
    });
    const data = await resp.json();
    if (resp.ok) {
      statusEl.textContent = dry ? '✓ Dry run done — see Logs tab' : '✓ Clean done — see Logs tab';
      statusEl.style.color = 'var(--color-success-fg)';
      await loadStatus();
    } else {
      statusEl.textContent = 'Error: ' + (data.error || 'unknown');
      statusEl.style.color = 'var(--color-danger-fg)';
    }
  } catch(e) {
    statusEl.textContent = 'Request failed: ' + e.message;
    statusEl.style.color = 'var(--color-danger-fg)';
  }
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
      tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:#888">No artifacts</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map(r => {
      const existsHtml = r['EXISTS?']
        ? '<span class="check-yes">✓ exists</span>'
        : '<span class="check-no">✗ missing</span>';
      const url = `/api/files?path=${encodeURIComponent(r.PATH)}`;
      const pathHtml = r['EXISTS?']
        ? `<a href="${url}" target="_blank">${escapeHtml(r.PATH)}</a>`
        : escapeHtml(r.PATH || '');
      const fileName = r.PATH ? r.PATH.split('/').pop() : 'artifact';
      const dlBtn = r['EXISTS?']
        ? `<button class="btn btn-secondary btn-sm" onclick="downloadArtifact(${escapeHtml(JSON.stringify(r.PATH))},${escapeHtml(JSON.stringify(fileName))})" title="Download">&#8659; Download</button>`
        : '';
      return `<tr>
        <td>${escapeHtml(r.TASK)}</td>
        <td>${escapeHtml(r.ARTIFACT)}</td>
        <td class="artifact-path">${pathHtml}</td>
        <td>${existsHtml}</td>
        <td style="white-space:nowrap">${dlBtn}</td>
      </tr>`;
    }).join('');
  } catch(e) {
    console.error('Artifacts fetch failed:', e);
  }
}

async function downloadArtifact(path, filename) {
  try {
    const resp = await fetch(`/api/files?path=${encodeURIComponent(path)}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    setTimeout(() => { URL.revokeObjectURL(url); a.remove(); }, 1000);
  } catch(e) {
    alert('Download failed: ' + e.message);
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
  const period  = document.getElementById('cron-period-sel').value;
  const minVal  = Math.min(59, Math.max(0, parseInt(document.getElementById('cron-minute-offset').value) || 0));
  const hourVal = Math.min(23, Math.max(0, parseInt(document.getElementById('cron-hour-val').value)    || 0));
  const dayVal  = Math.min(28, Math.max(1, parseInt(document.getElementById('cron-day-val').value)     || 1));
  const dowVal  = document.getElementById('cron-dow-sel').value;  // 0=Sun … 6=Sat, 1=Mon default

  // Show/hide fields based on period
  const show = id => { document.getElementById(id).style.display = ''; };
  const hide = id => { document.getElementById(id).style.display = 'none'; };

  // Sub-hourly: no offsets
  const subHourly = ['5m','15m','30m'].includes(period);
  // Hourly: minute offset only
  const hourly = ['1h','2h','3h','4h','6h','8h','12h'].includes(period);
  // Daily: hour + minute
  const daily = period === '24h';
  // Multi-day (every N days, biweekly): day + hour + minute
  const multiDay = ['2d','3d','2w'].includes(period);
  // Weekly: dow + hour + minute
  const weekly = period === '1w';

  subHourly ? hide('cron-minute-wrap') : show('cron-minute-wrap');
  (daily || multiDay || weekly) ? show('cron-hour-wrap') : hide('cron-hour-wrap');
  multiDay  ? show('cron-day-wrap')    : hide('cron-day-wrap');
  weekly    ? show('cron-dow-wrap')    : hide('cron-dow-wrap');

  let expr = '';
  if      (period === '5m')  expr = '*/5 * * * *';
  else if (period === '15m') expr = '*/15 * * * *';
  else if (period === '30m') expr = '*/30 * * * *';
  else if (period === '1h')  expr = `${minVal} * * * *`;
  else if (period === '2h')  expr = `${minVal} */2 * * *`;
  else if (period === '3h')  expr = `${minVal} */3 * * *`;
  else if (period === '4h')  expr = `${minVal} */4 * * *`;
  else if (period === '6h')  expr = `${minVal} */6 * * *`;
  else if (period === '8h')  expr = `${minVal} */8 * * *`;
  else if (period === '12h') expr = `${minVal} */12 * * *`;
  else if (period === '24h') expr = `${minVal} ${hourVal} * * *`;
  else if (period === '2d')  expr = `${minVal} ${hourVal} ${dayVal}/2 * *`;
  else if (period === '3d')  expr = `${minVal} ${hourVal} ${dayVal}/3 * *`;
  else if (period === '1w')  expr = `${minVal} ${hourVal} * * ${dowVal}`;
  else if (period === '2w')  expr = `${minVal} ${hourVal} ${dayVal}/14 * *`;

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

/* ── Restart / Shutdown server ───────────────────────────────────────── */
async function restartServer() {
  if (!confirm('Restart the woom monitor server? The page will reload automatically.')) return;
  if (_jobsRefreshTimer) clearInterval(_jobsRefreshTimer);
  if (_runStatusTimer)   clearInterval(_runStatusTimer);
  try { await fetch('/api/restart', {method: 'POST'}); } catch(e) {}
  // Poll until the server is back up, then reload
  const t0 = Date.now();
  const poll = setInterval(async () => {
    if (Date.now() - t0 > 30000) { clearInterval(poll); return; }
    try {
      const r = await fetch('/api/info');
      if (r.ok) { clearInterval(poll); window.location.reload(); }
    } catch(e) {}
  }, 800);
}


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

/* ── About panel ─────────────────────────────────────────────────────── */
function toggleAbout(e) {
  e.stopPropagation();
  const panel = document.getElementById('about-panel');
  panel.style.display = panel.style.display === 'none' ? '' : 'none';
}

document.addEventListener('click', () => {
  const panel = document.getElementById('about-panel');
  if (panel) panel.style.display = 'none';
});

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
