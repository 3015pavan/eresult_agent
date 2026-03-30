/**
 * AcadExtract — Frontend Application
 *
 * Vanilla JS, no build step. Talks to the FastAPI backend.
 */

const API = '';  // same origin

const App = {
  trendChart: null,
  queryChart: null,

  // ── Init ────────────────────────────────────────────
  init() {
    this.bindNavigation();
    this.bindKeyboard();
    this.bindMobileMenu();
    this.handleAuthCallback();
    this.loadHealth();
    this.loadAccountsSummary();
    this.loadDashboard();
    setInterval(() => this.loadHealth(), 30000);
    setInterval(() => this.loadAccountsSummary(), 60000);
  },

  // ── Navigation ──────────────────────────────────────
  bindNavigation() {
    document.querySelectorAll('.nav-item').forEach(item => {
      item.addEventListener('click', e => {
        e.preventDefault();
        const page = item.dataset.page;
        this.showPage(page);
      });
    });
  },

  showPage(page) {
    // Update nav
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
    document.querySelector(`.nav-item[data-page="${page}"]`)?.classList.add('active');

    // Update pages
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.getElementById(`page-${page}`)?.classList.add('active');

    // Title
    const titles = { dashboard: 'Dashboard', query: 'Query Engine', student: 'Student Lookup', accounts: 'Email Accounts', admin: 'Admin Panel', assistant: 'AI Assistant' };
    document.getElementById('page-title').textContent = titles[page] || page;

    // Close mobile menu
    document.getElementById('sidebar')?.classList.remove('open');

    // Reload data for certain pages
    if (page === 'dashboard') this.loadDashboard();
    if (page === 'accounts') this.loadAccounts();
  },

  bindKeyboard() {
    document.getElementById('query-input')?.addEventListener('keydown', e => {
      if (e.key === 'Enter') this.submitQuery();
    });
    document.getElementById('student-usn-input')?.addEventListener('keydown', e => {
      if (e.key === 'Enter') this.lookupStudent();
    });
  },

  bindMobileMenu() {
    document.getElementById('menu-toggle')?.addEventListener('click', () => {
      document.getElementById('sidebar')?.classList.toggle('open');
    });
  },

  // ── API Helpers ─────────────────────────────────────
  async apiGet(path) {
    const res = await fetch(`${API}${path}`);
    if (!res.ok) {
      const err = await res.json().catch(() => ({ detail: res.statusText }));
      throw new Error(err.detail || `HTTP ${res.status}`);
    }
    return res.json();
  },

  async apiPost(path, body) {
    const res = await fetch(`${API}${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({ detail: res.statusText }));
      throw new Error(err.detail || `HTTP ${res.status}`);
    }
    return res.json();
  },

  // ── Toast ───────────────────────────────────────────
  toast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    const el = document.createElement('div');
    el.className = `toast ${type}`;
    el.textContent = message;
    container.appendChild(el);
    setTimeout(() => el.remove(), 4000);
  },

  // ── Health ──────────────────────────────────────────
  async loadHealth() {
    try {
      const data = await this.apiGet('/healthz');
      const badge = document.getElementById('health-badge');
      badge.className = 'health-badge alive';
      badge.querySelector('.health-text').textContent = 'System Alive';
    } catch {
      const badge = document.getElementById('health-badge');
      badge.className = 'health-badge down';
      badge.querySelector('.health-text').textContent = 'System Down';
    }
  },

  // ── Dashboard ───────────────────────────────────────
  async loadDashboard() {
    this.loadPipelineStatus();
    this.loadReadiness();
    this.loadStats();
    this.loadSyncedEmails();
    this.loadPipelineSummary();
  },

  async loadPipelineSummary() {
    try {
      const [ps, ss] = await Promise.all([
        this.apiGet('/api/v1/pipeline/status'),
        this.apiGet('/api/v1/sync/status').catch(() => null),
      ]);
      const db = ps.database || {};
      const sync = ss || {};
      const _v = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = (v != null && v !== '') ? String(v) : '—'; };

      _v('pes-email-files',    db.email_extractions  ?? '—');
      _v('pes-admin-files',    db.admin_upload_files ?? '—');
      _v('pes-email-students', db.email_students     ?? '—');
      _v('pes-admin-students', db.admin_students     ?? '—');
      _v('pes-total-students', db.total_students     ?? '—');

      // Last sync info row
      const acct = document.getElementById('psi-account');
      const lastSync = document.getElementById('psi-last-sync');
      const newFetch = document.getElementById('psi-new-fetched');
      if (acct) acct.textContent = sync.account ? '✉️ ' + sync.account : '';
      if (lastSync && sync.last_sync) {
        lastSync.textContent = 'Last sync: ' + new Date(sync.last_sync).toLocaleString();
      }
      if (newFetch) {
        const n = sync.fetched_last_run;
        newFetch.textContent = (n != null && n > 0) ? '+' + n + ' fetched last run' : '';
      }
    } catch (e) {
      // silently ignore — summary is informational
    }
  },

  async runPipeline(force = false) {
    const btn = document.getElementById('run-pipeline-btn');
    const forceBtn = document.getElementById('force-pipeline-btn');
    const log = document.getElementById('pipeline-log');
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spinner"></span> Running…'; }
    if (forceBtn) forceBtn.disabled = true;
    if (log) { log.style.display = 'block'; log.textContent = force ? 'Force reprocessing all emails…' : 'Starting pipeline…'; }
    try {
      const data = await this.apiPost('/api/v1/pipeline/run', { force: !!force });
      // Filter out verbose [ALREADY PROCESSED] lines — keep summary lines only
      const rawLines = data.log || [];
      const skipNoise = rawLines.length > 10;
      const lines = skipNoise
        ? rawLines.filter(l => !l.includes('[ALREADY PROCESSED]') || l.includes('completed'))
        : rawLines;
      if (data.message) {
        lines.push('');
        lines.push('» ' + data.message);
      }
      if (log) log.textContent = lines.join('\n') || JSON.stringify(data, null, 2);
      // Auto-scroll to bottom
      if (log) log.scrollTop = log.scrollHeight;
      const newCount = data.records_extracted ?? 0;
      const skipCount = data.skipped_dedup ?? 0;
      if (data.async_mode) {
        this.toast(
          force
            ? 'Force reprocess queued. Worker will process all cached emails shortly.'
            : 'Pipeline queued successfully. Worker processing will start shortly.',
          'success'
        );
      } else {
        this.toast(
          force
            ? `Force reprocess done: ${newCount} record(s) extracted`
            : newCount > 0
              ? `Pipeline done: ${newCount} new record(s) extracted`
              : `Pipeline done — ${skipCount} already stored. Use Force Reprocess to re-extract.`,
          newCount > 0 ? 'success' : 'info'
        );
      }
      await this.loadStats();
      await this.loadPipelineStatus();
      this.loadPipelineSummary();
    } catch (e) {
      if (log) log.textContent = 'Error: ' + e.message;
      this.toast('Pipeline failed: ' + e.message, 'error');
    } finally {
      if (btn) { btn.disabled = false; btn.innerHTML = '▶ Run Pipeline'; }
      if (forceBtn) { forceBtn.disabled = false; }
    }
  },

  async loadPipelineStatus() {
    try {
      const data = await this.apiGet('/api/v1/pipeline/status');
      const p = data.pipeline || {};
      const db = data.database || {};
      const _set = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v ?? '—'; };
      _set('ps-email-files',    this.formatNum(db.email_extractions  ?? 0));
      _set('ps-admin-files',    this.formatNum(db.admin_upload_files ?? 0));
      _set('ps-email-students', this.formatNum(db.email_students     ?? 0));
      _set('ps-admin-students', this.formatNum(db.admin_students     ?? 0));
      _set('ps-students',       this.formatNum(db.total_students     ?? 0));
      _set('ps-records',        this.formatNum(db.total_results      ?? 0));
      _set('ps-avgcgpa',        db.average_cgpa ? parseFloat(db.average_cgpa).toFixed(2) : '—');
      _set('ps-backlogs',       this.formatNum(db.total_backlogs     ?? 0));
      _set('ps-agent',          p.status ?? '—');
      _set('ps-avgtime',        p.last_run ? new Date(p.last_run).toLocaleString() : 'Never');
    } catch (e) {
      // silently ignore
    }
  },

  async loadReadiness() {
    const container = document.getElementById('readiness-checks');
    try {
      const data = await this.apiGet('/readyz');
      container.innerHTML = '';

      // Overall status
      const overallEl = document.createElement('div');
      overallEl.className = `readiness-row ${data.status === 'ready' ? 'ok' : 'error'}`;
      overallEl.innerHTML = `<span class="readiness-dot"></span><strong>Overall: ${data.status.toUpperCase()}</strong>`;
      container.appendChild(overallEl);

      // Individual checks
      if (data.checks) {
        for (const [name, status] of Object.entries(data.checks)) {
          const isOk = status === 'ok';
          const row = document.createElement('div');
          row.className = `readiness-row ${isOk ? 'ok' : 'error'}`;
          row.innerHTML = `<span class="readiness-dot"></span><span>${name}: ${status}</span>`;
          container.appendChild(row);
        }
      }
    } catch (e) {
      container.innerHTML = `<div class="readiness-row error"><span class="readiness-dot"></span><span>Health check failed: ${e.message}</span></div>`;
    }
  },

  async loadStats() {
    try {
      const data = await this.apiGet('/api/v1/pipeline/status');
      const db = data.database || {};
      const _set = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v ?? '—'; };
      _set('stat-email-files', this.formatNum(db.email_extractions || 0));
      _set('stat-admin-files', this.formatNum(db.admin_upload_files || 0));
      _set('stat-email-students', this.formatNum(db.email_students || 0));
      _set('stat-admin-students', this.formatNum(db.admin_students || 0));
      _set('stat-students', this.formatNum(db.total_students || 0));
      _set('stat-avgcgpa', db.average_cgpa ? parseFloat(db.average_cgpa).toFixed(2) : '—');
      _set('stat-extractions', this.formatNum(db.total_results || 0));
    } catch (e) {
      // Stats not available — leave dashes
    }
  },

  formatNum(n) {
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
    if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
    return n.toLocaleString();
  },

  // ── Query Engine ────────────────────────────────────
  setQuery(text) {
    document.getElementById('query-input').value = text;
    document.getElementById('query-input').focus();
  },

  async submitQuery() {
    const input = document.getElementById('query-input');
    const query = input.value.trim();
    if (!query) return;

    const btn = document.getElementById('query-submit');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Thinking...';

    const resultCard = document.getElementById('query-result-card');
    resultCard.style.display = 'none';

    try {
      const data = await this.apiPost('/api/v1/query', { query });

      // Show result card
      resultCard.style.display = 'block';

      // Answer
      document.getElementById('query-answer').textContent = data.text_answer;

      // Summary
      const summaryEl = document.getElementById('query-summary');
      summaryEl.textContent = data.summary || '';
      summaryEl.style.display = data.summary ? 'block' : 'none';

      // Confidence
      const confBadge = document.getElementById('query-confidence');
      const confPct = Math.round(data.confidence * 100);
      confBadge.textContent = `${confPct}% confidence`;
      confBadge.className = 'confidence-badge ' +
        (confPct >= 80 ? 'confidence-high' : confPct >= 50 ? 'confidence-med' : 'confidence-low');

      // Caveats
      const caveatsEl = document.getElementById('query-caveats');
      caveatsEl.innerHTML = (data.caveats || []).map(c => `<div class="caveat-item">${c}</div>`).join('');

      // Data table
      this.renderQueryData(data.data);

      // Chart
      this.renderQueryChart(data.chart_spec);

      this.toast('Query completed', 'success');
    } catch (e) {
      this.toast('Query failed: ' + e.message, 'error');
    } finally {
      btn.disabled = false;
      btn.innerHTML = `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg> Ask`;
    }
  },

  renderQueryData(rows) {
    const wrap = document.getElementById('query-data-wrap');
    const table = document.getElementById('query-data-table');
    if (!rows || rows.length === 0) {
      wrap.style.display = 'none';
      return;
    }
    wrap.style.display = 'block';
    const cols = Object.keys(rows[0]);
    table.innerHTML = `
      <thead><tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr></thead>
      <tbody>${rows.map(r => `<tr>${cols.map(c => `<td>${r[c] ?? ''}</td>`).join('')}</tr>`).join('')}</tbody>
    `;
  },

  renderQueryChart(spec) {
    const container = document.getElementById('query-chart-container');
    if (!spec) { container.style.display = 'none'; return; }
    container.style.display = 'block';
    if (this.queryChart) this.queryChart.destroy();
    const ctx = document.getElementById('query-chart').getContext('2d');
    // Support both {data: ...} (Chart.js format) and {x: [...], y: [...], labels: [...], values: [...]} (pipeline format)
    let chartData;
    if (spec.data) {
      chartData = spec.data;
    } else if (spec.x) {
      chartData = { labels: spec.x, datasets: [{ label: spec.ylabel || 'Value', data: spec.y, borderColor: '#4f8cff', backgroundColor: 'rgba(79,140,255,0.15)', fill: true, tension: 0.3 }] };
    } else if (spec.labels) {
      chartData = { labels: spec.labels, datasets: [{ label: spec.ylabel || 'CGPA', data: spec.values, backgroundColor: '#4f8cff' }] };
    } else { container.style.display = 'none'; return; }
    this.queryChart = new Chart(ctx, {
      type: spec.type || 'bar',
      data: chartData,
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { labels: { color: '#e4e6eb' } } },
        scales: {
          x: { ticks: { color: '#8b8fa3' }, grid: { color: '#2a2d37' } },
          y: { ticks: { color: '#8b8fa3' }, grid: { color: '#2a2d37' } },
        },
      },
    });
  },

  // ── Student Lookup ──────────────────────────────────
  async lookupStudent() {
    const input = document.getElementById('student-usn-input');
    const raw   = input.value.trim();
    if (!raw) return;

    const btn = document.getElementById('student-submit');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Looking up...';
    document.getElementById('student-result').style.display = 'none';

    const usnPattern = /^[1-4][A-Za-z]{2}\d{2}[A-Za-z]{2,4}\d{3}$/;
    const usn = raw.toUpperCase();

    try {
      if (usnPattern.test(usn)) {
        await this._loadStudentProfile(usn);
      } else {
        // Name-based: search first, then decide
        const search = await this.apiGet(`/api/v1/students?q=${encodeURIComponent(raw)}&limit=10`);
        const students = (search.students || []);
        if (students.length === 0) {
          this.toast(`No student found matching "${raw}"`, 'error');
          return;
        }
        if (students.length === 1) {
          input.value = students[0].usn;
          await this._loadStudentProfile(students[0].usn);
          return;
        }
        // Multiple matches — show disambiguation list
        const list = students.map(s => `
          <li style="cursor:pointer;padding:8px 4px;border-bottom:1px solid #353849;"
              onclick="app.pickStudentByUsn('${s.usn}')">
            <strong style="color:#4f8cff;">${s.usn}</strong>
            <span style="margin-left:8px;">${s.full_name || s.name || ''}</span>
            ${s.cgpa ? `<span style="color:#a0a3b1;margin-left:8px;">CGPA ${parseFloat(s.cgpa).toFixed(2)}</span>` : ''}
          </li>`).join('');
        const box = document.getElementById('student-result');
        box.style.display = 'block';
        box.innerHTML = `<div class="card"><div class="card-body">
          <p style="margin:0 0 10px;color:#b0b3c1;">
            <strong>${students.length}</strong> students match <em>"${raw}"</em> — select one:
          </p>
          <ul style="list-style:none;padding:0;margin:0;">${list}</ul>
        </div></div>`;
        this.toast(`${students.length} students found`, 'info');
      }
    } catch (e) {
      this.toast('Student lookup failed: ' + e.message, 'error');
    } finally {
      btn.disabled = false;
      btn.innerHTML = `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35"/></svg> Lookup`;
    }
  },

  async pickStudentByUsn(usn) {
    document.getElementById('student-usn-input').value = usn;
    document.getElementById('student-result').style.display = 'none';
    await this._loadStudentProfile(usn);
  },

  async _loadStudentProfile(usn) {
    const [student, trend] = await Promise.all([
      this.apiGet(`/api/v1/student/${usn}`),
      this.apiGet(`/api/v1/student/${usn}/trend`).catch(() => null),
    ]);

      // Profile fields
      document.getElementById('sp-usn').textContent = student.usn;
      document.getElementById('sp-name').textContent = student.name;
      document.getElementById('sp-email').textContent = student.email || '—';
      document.getElementById('sp-dept').textContent = student.department;
      document.getElementById('sp-batch').textContent = student.batch_year || '—';
      document.getElementById('sp-cgpa').textContent =
        student.current_cgpa != null ? parseFloat(student.current_cgpa).toFixed(2) : '—';
      document.getElementById('sp-backlogs').textContent = student.active_backlogs ?? '—';
      document.getElementById('sp-rank').textContent =
        student.class_rank ? `${student.class_rank} / ${student.total_students}` : '—';
      document.getElementById('sp-total-results').textContent = student.total_results ?? '—';

      // Semester summary table
      const tbody = document.getElementById('semester-tbody');
      tbody.innerHTML = (student.semesters || []).map(s => `
        <tr>
          <td>Sem ${s.semester}</td>
          <td>${s.sgpa != null ? parseFloat(s.sgpa).toFixed(2) : '—'}</td>
          <td>${s.credits_earned ?? '—'}</td>
          <td>${s.credits_attempted ?? '—'}</td>
          <td>${s.subjects_passed ?? '—'}</td>
          <td>${s.subjects_failed ?? '—'}</td>
        </tr>
      `).join('');

      // Detailed subject results per semester
      const container = document.getElementById('subject-results-container');
      const bySem = student.results_by_semester || {};
      const semKeys = Object.keys(bySem).sort((a, b) => Number(a) - Number(b));
      container.innerHTML = semKeys.map(sem => {
        const subjects = bySem[sem];
        const rows = subjects.map(r => {
          const failed = (r.status || '').toLowerCase() === 'fail';
          const rowClass = failed ? 'style="color:#ff6b6b;"' : '';
          return `<tr ${rowClass}>
            <td>${r.subject_code || '—'}</td>
            <td>${r.subject_name || '—'}</td>
            <td>${r.marks_obtained ?? '—'}</td>
            <td>${r.max_marks ?? '—'}</td>
            <td>${r.grade || '—'}</td>
            <td>${r.grade_points ?? '—'}</td>
            <td><span class="badge ${failed ? 'badge-fail' : 'badge-pass'}">${r.status || '—'}</span></td>
          </tr>`;
        }).join('');
        return `
          <div class="card" style="margin-top:16px;">
            <div class="card-header"><h3>Semester ${sem} — Subject Results</h3></div>
            <div class="card-body">
              <div class="data-table-wrap">
                <table class="data-table">
                  <thead><tr>
                    <th>Code</th><th>Subject</th><th>Marks</th><th>Max</th>
                    <th>Grade</th><th>GP</th><th>Status</th>
                  </tr></thead>
                  <tbody>${rows}</tbody>
                </table>
              </div>
            </div>
          </div>`;
      }).join('');

      // SGPA Trend Chart
      if (trend && trend.trend && trend.trend.length > 0) {
        this.renderTrendChart(trend.trend);
      }

      document.getElementById('student-result').style.display = 'block';
      this.toast(`Loaded profile for ${student.name}`, 'success');
  },

  downloadReport(format) {
    const usn = (document.getElementById('sp-usn').textContent || '').trim();
    if (!usn || usn === '—') return this.toast('Look up a student first', 'error');
    const url = `/api/v1/student/${usn}/report?format=${format}`;
    const a = document.createElement('a');
    a.href = url;
    a.download = `${usn}_report.${format}`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    this.toast(`Downloading ${format.toUpperCase()} report for ${usn}…`, 'success');
  },

  // ── Email Report ────────────────────────────────────
  showEmailModal() {
    const usn = (document.getElementById('sp-usn').textContent || '').trim();
    if (!usn || usn === '—') return this.toast('Look up a student first', 'error');
    const overlay = document.getElementById('email-modal-overlay');
    overlay.style.display = 'flex';
    document.getElementById('email-modal-recipient').value = '';
    document.getElementById('email-modal-recipient').focus();
  },

  hideEmailModal() {
    document.getElementById('email-modal-overlay').style.display = 'none';
  },

  async sendReport() {
    const usn = (document.getElementById('sp-usn').textContent || '').trim();
    const recipient = document.getElementById('email-modal-recipient').value.trim();
    const format = document.getElementById('email-modal-format').value;

    if (!recipient) return this.toast('Enter a recipient email', 'error');
    if (!usn || usn === '—') return this.toast('Look up a student first', 'error');

    const btn = document.getElementById('email-modal-send');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Sending…';

    try {
      const data = await this.apiPost(`/api/v1/student/${usn}/email-report`, {
        usn, recipient, format,
      });
      this.toast(data.message || `Report emailed to ${recipient}`, 'success');
      this.hideEmailModal();
    } catch (e) {
      this.toast('Email failed: ' + e.message, 'error');
    } finally {
      btn.disabled = false;
      btn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg> Send`;
    }
  },

  renderTrendChart(trendData) {
    if (this.trendChart) this.trendChart.destroy();
    const ctx = document.getElementById('trend-chart').getContext('2d');
    this.trendChart = new Chart(ctx, {
      type: 'line',
      data: {
        labels: trendData.map(t => `Sem ${t.semester}`),
        datasets: [{
          label: 'SGPA',
          data: trendData.map(t => t.sgpa),
          borderColor: '#4f8cff',
          backgroundColor: 'rgba(79,140,255,0.1)',
          fill: true,
          tension: 0.3,
          pointBackgroundColor: '#4f8cff',
          pointBorderColor: '#fff',
          pointRadius: 5,
          pointHoverRadius: 7,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: '#181a20',
            borderColor: '#2a2d37',
            borderWidth: 1,
            titleColor: '#e4e6eb',
            bodyColor: '#e4e6eb',
          },
        },
        scales: {
          x: {
            ticks: { color: '#8b8fa3' },
            grid: { color: '#2a2d37' },
          },
          y: {
            min: 0,
            max: 10,
            ticks: { color: '#8b8fa3', stepSize: 2 },
            grid: { color: '#2a2d37' },
          },
        },
      },
    });
  },

  // ── Admin: Document Upload ──────────────────────────
  _uploadFile: null,

  handleFileDrop(event) {
    event.preventDefault();
    document.getElementById('upload-drop-zone').classList.remove('drag-over');
    const file = event.dataTransfer.files[0];
    if (file) this._setUploadFile(file);
  },

  handleFileSelect(input) {
    if (input.files[0]) this._setUploadFile(input.files[0]);
  },

  _setUploadFile(file) {
    this._uploadFile = file;
    document.getElementById('upload-file-name').textContent = `Selected: ${file.name} (${(file.size/1024).toFixed(1)} KB)`;
    document.getElementById('upload-submit-btn').disabled = false;
  },

  async uploadDocument() {
    if (!this._uploadFile) return this.toast('Select a file first', 'error');
    const btn = document.getElementById('upload-submit-btn');
    const resultBox = document.getElementById('upload-result');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Processing...';
    resultBox.style.display = 'none';

    try {
      const form = new FormData();
      form.append('file', this._uploadFile);
      const resp = await fetch('/api/v1/admin/upload', { method: 'POST', body: form });
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.detail || JSON.stringify(data));
      resultBox.style.display = 'block';
      if (data.status === 'no_data') {
        resultBox.className = 'result-box error';
        resultBox.innerHTML = `<strong>No student records found</strong><br>File: ${data.filename}<br>${data.message || ''}`;
        this.toast(`No records found in ${data.filename}`, 'error');
      } else {
        resultBox.className = 'result-box success';
        resultBox.innerHTML = `<strong>✓ Upload processed</strong><br>
          File: ${data.filename}<br>
          Students upserted: ${data.students_upserted}<br>
          Results stored: ${data.results_stored}<br>
          Records parsed: ${data.records_parsed}<br>
          ${ data.errors && data.errors.length ? '<span style="color:#ff9f43;">Warnings: ' + data.errors.join(', ') + '</span>' : '' }`;
        this.toast(`Uploaded ${data.filename} — ${data.students_upserted} students stored`, 'success');
      }
      this._uploadFile = null;
      document.getElementById('upload-file-name').textContent = '';
      document.getElementById('upload-submit-btn').disabled = true;
      const inp = document.getElementById('upload-file-input');
      if (inp) inp.value = '';
    } catch (e) {
      resultBox.style.display = 'block';
      resultBox.className = 'result-box error';
      resultBox.textContent = e.message;
      this.toast('Upload failed: ' + e.message, 'error');
    } finally {
      btn.disabled = false;
      btn.innerHTML = `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg> Upload &amp; Process`;
    }
  },

  // ── Admin: Clear Seeded Data ────────────────────────
  async clearSeeds() {
    if (!confirm('Remove all seeded test data? Real extracted student data will be preserved.')) return;
    const resultBox = document.getElementById('clear-seeds-result');
    try {
      const res = await fetch('/api/v1/pipeline/clear-seeds', { method: 'DELETE' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
      if (resultBox) {
        resultBox.style.display = 'block';
        resultBox.className = 'result-box success';
        resultBox.textContent = `✓ Cleared ${data.seed_students_removed} seeded student(s) from the database.`;
      }
      this.toast(`Cleared ${data.seed_students_removed} seeded record(s)`, 'success');
      await this.loadStats();
    } catch (e) {
      if (resultBox) {
        resultBox.style.display = 'block';
        resultBox.className = 'result-box error';
        resultBox.textContent = e.message;
      }
      this.toast('Clear failed: ' + e.message, 'error');
    }
  },

  // ── Admin: Review Queue ─────────────────────────────
  async loadReviewQueue() {
    const status = document.getElementById('review-queue-status').value;
    const listEl = document.getElementById('review-queue-list');
    listEl.innerHTML = '<p style="color:var(--text-muted);text-align:center;padding:20px;">Loading...</p>';
    try {
      const data = await this.apiGet(`/api/v1/admin/review-queue?status=${status}&limit=50`);
      if (!data.items || data.items.length === 0) {
        listEl.innerHTML = `<p style="color:var(--text-muted);text-align:center;padding:20px;">No ${status} items.</p>`;
        return;
      }
      listEl.innerHTML = data.items.map(item => `
        <div class="result-box" style="margin-bottom:12px;padding:12px;">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px;">
            <div>
              <strong style="font-size:13px;">${item.email_subject || '(no subject)'}</strong>
              <span style="font-size:11px;color:var(--text-muted);margin-left:8px;">${item.email_from || ''}</span>
            </div>
            <span style="font-size:11px;color:var(--text-muted);">Confidence: ${(parseFloat(item.confidence || 0) * 100).toFixed(0)}%</span>
          </div>
          ${item.validation_errors && item.validation_errors.length ? `<p style="font-size:11px;color:#e87;margin:0 0 8px;">${item.validation_errors.join(', ')}</p>` : ''}
          <div style="font-size:11px;color:var(--text-muted);margin-bottom:8px;">ID: ${item.id} · ${item.created_at || ''}</div>
          ${item.status === 'pending' ? `
            <div style="display:flex;gap:8px;">
              <button class="btn btn-primary btn-sm" onclick="App.approveReviewItem('${item.id}')">&#10003; Approve &amp; Save</button>
              <button class="btn btn-danger btn-sm" onclick="App.rejectReviewItem('${item.id}')">&#10007; Reject</button>
            </div>
          ` : `<span style="font-size:12px;color:var(--text-muted);">Status: ${item.status}${item.notes ? ' &mdash; ' + item.notes : ''}</span>`}
        </div>
      `).join('');
    } catch (e) {
      listEl.innerHTML = `<p style="color:#e87;">Error loading review queue: ${e.message}</p>`;
    }
  },

  async approveReviewItem(itemId) {
    try {
      const data = await this.apiPost(`/api/v1/admin/review-queue/${itemId}/approve`, { save_to_db: true, notes: 'Approved via UI' });
      this.toast(`Approved \u2014 ${data.records_saved} record(s) saved`, 'success');
      await this.loadReviewQueue();
    } catch (e) {
      this.toast('Approve failed: ' + e.message, 'error');
    }
  },

  async rejectReviewItem(itemId) {
    try {
      await this.apiPost(`/api/v1/admin/review-queue/${itemId}/reject`, { notes: 'Rejected via UI' });
      this.toast('Item rejected', 'success');
      await this.loadReviewQueue();
    } catch (e) {
      this.toast('Reject failed: ' + e.message, 'error');
    }
  },

  // ── Admin: Trace Lookup ─────────────────────────────
  async lookupTrace() {
    const runId = document.getElementById('trace-id').value.trim();
    if (!runId) return this.toast('Enter a run ID', 'error');
    const resultBox = document.getElementById('trace-result');

    try {
      const data = await this.apiGet(`/api/v1/admin/traces/${runId}`);
      resultBox.style.display = 'block';
      resultBox.className = 'result-box success';
      resultBox.textContent = JSON.stringify(data, null, 2);
    } catch (e) {
      resultBox.style.display = 'block';
      resultBox.className = 'result-box error';
      resultBox.textContent = e.message;
      this.toast('Trace lookup failed: ' + e.message, 'error');
    }
  },

  // ── Gmail Auth + auto-sync after OAuth ────────────────────────────
  handleAuthCallback() {
    const params = new URLSearchParams(window.location.search);
    const sync  = params.get('sync');
    const auth  = params.get('auth');
    if (sync === '1') {
      window.history.replaceState({}, '', '/');
      // Show accounts page first so user sees their profile
      this.showPage('accounts');
      this.loadAccountsSummary();
      // Auto-run the full pipeline after a short delay
      setTimeout(() => this.autoRunPipeline(), 600);
    } else if (auth === 'error') {
      const msg = params.get('message') || 'Unknown error';
      this.toast('Gmail auth failed: ' + msg, 'error');
      window.history.replaceState({}, '', '/');
    }
  },

  // ── Auto-pipeline: sync → extract (runs after OAuth connect) ───────
  async autoRunPipeline() {
    // Create (or reuse) a floating progress banner
    let banner = document.getElementById('auto-pipeline-banner');
    if (!banner) {
      banner = document.createElement('div');
      banner.id = 'auto-pipeline-banner';
      banner.className = 'auto-pipeline-banner';
      document.body.appendChild(banner);
    }

    const setBanner = (icon, msg, cls) => {
      banner.className = `auto-pipeline-banner ${cls || ''}`;
      banner.innerHTML = `<span class="apb-icon">${icon}</span><span class="apb-msg">${msg}</span>`;
      banner.style.display = 'flex';
    };

    // ── Step 1: Sync emails ─────────────────────────────────────────
    setBanner('<span class="spinner"></span>', 'Step 1 / 2 &nbsp;—&nbsp; Syncing emails from Gmail…', 'running');
    let synced = 0;
    try {
      const sync = await this.apiPost('/api/v1/sync', { max_results: 100, since_days: 60 });
      synced = sync.fetched ?? 0;
      await this.loadSyncedEmails();
      this.loadAccountsSummary();
    } catch (e) {
      setBanner('✗', `Sync failed: ${e.message}`, 'error');
      this.toast('Email sync failed: ' + e.message, 'error');
      setTimeout(() => { banner.style.display = 'none'; }, 6000);
      return;
    }

    // ── Step 2: Run pipeline ────────────────────────────────────────
    setBanner('<span class="spinner"></span>', `Step 2 / 2 &nbsp;—&nbsp; Processing ${synced} email${synced !== 1 ? 's' : ''}…`, 'running');
    try {
      const result = await this.apiPost('/api/v1/pipeline/run', {});
      const extracted = result.records_extracted ?? 0;
      const students  = result.students_updated  ?? 0;

      if (result.async_mode) {
        setBanner('✓',
          `Connected &amp; ready — synced ${synced} email${synced !== 1 ? 's' : ''}, and queued the pipeline worker successfully.`,
          'done'
        );
        this.toast('Pipeline queued successfully', 'success');
      } else {
        setBanner('✓',
          `Connected &amp; ready — synced ${synced} email${synced !== 1 ? 's' : ''}, extracted ${extracted} record${extracted !== 1 ? 's' : ''}${students ? `, updated ${students} student${students !== 1 ? 's' : ''}` : ''}.`,
          'done'
        );
        this.toast(`Pipeline complete — ${extracted} records extracted`, 'success');
      }
      this.loadStats();
      this.loadPipelineStatus();

      // Auto-dismiss after 8 s
      setTimeout(() => {
        banner.style.opacity = '0';
        setTimeout(() => { banner.style.display = 'none'; banner.style.opacity = ''; }, 500);
      }, 8000);
    } catch (e) {
      setBanner('⚠', `Sync OK (${synced} emails) — Pipeline error: ${e.message}`, 'error');
      this.toast('Pipeline failed: ' + e.message, 'error');
      setTimeout(() => { banner.style.display = 'none'; }, 8000);
    }
  },

  // ── Email Sync ──────────────────────────────────────────────────────
  async syncEmails() {
    const btn = document.getElementById('sync-btn');
    const bar = document.getElementById('sync-status-bar');
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spinner"></span> Syncing…'; }
    if (bar) { bar.style.display = 'block'; bar.className = 'sync-status-bar syncing'; bar.textContent = 'Fetching emails from Gmail…'; }
    try {
      const data = await this.apiPost('/api/v1/sync', { max_results: 50, since_days: 30 });
      if (bar) {
        bar.className = 'sync-status-bar ok';
        bar.textContent = `✓ Synced ${data.fetched} new email${data.fetched !== 1 ? 's' : ''} · ${data.total_cached} total in cache`;
        setTimeout(() => { bar.style.display = 'none'; }, 4000);
      }
      this.toast(`Synced ${data.fetched} emails`, 'success');
      await this.loadSyncedEmails('', this._inboxFilter ?? 'result_email');
      this.loadPipelineSummary();

      // Automatically run the pipeline after a manual sync too
      this.toast('Running pipeline on new emails…', 'info');
      const result = await this.apiPost('/api/v1/pipeline/run', {});
      const extracted = result.records_extracted ?? 0;
      const resultEmails = result.result_emails ?? 0;
      if (result.async_mode) {
        this.toast('Pipeline queued successfully for background processing', 'success');
      } else {
        this.toast(
          extracted > 0
            ? `Pipeline done — ${resultEmails} result email${resultEmails !== 1 ? 's' : ''}, ${extracted} record${extracted !== 1 ? 's' : ''} extracted`
            : `Pipeline done — no new result emails found`,
          extracted > 0 ? 'success' : 'info'
        );
      }
      this.loadStats();
      this.loadPipelineStatus();
      this.loadPipelineSummary();
    } catch (e) {
      if (bar) { bar.className = 'sync-status-bar err'; bar.textContent = '✗ ' + e.message; }
      this.toast('Sync/pipeline failed: ' + e.message, 'error');
    } finally {
      if (btn) { btn.disabled = false; btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 11-2.12-9.36L23 10"/></svg> Sync'; }
    }
  },

  async loadSyncedEmails(q = '', classification = this._inboxFilter ?? 'result_email') {
    const list = document.getElementById('inbox-list');
    const countEl = document.getElementById('inbox-count');
    if (!list) return;
    try {
      let url = `/api/v1/sync/emails?limit=100`;
      if (q) url += `&q=${encodeURIComponent(q)}`;
      if (classification) url += `&classification=${encodeURIComponent(classification)}`;
      const data = await this.apiGet(url);
      countEl.textContent = data.total ? `(${data.total})` : '';
      if (!data.emails || data.emails.length === 0) {
        const msg = classification === 'result_email'
          ? 'No result/student emails found. Click <strong>All</strong> to see all emails, or <strong>Sync</strong> to fetch more.'
          : 'No emails found. Click <strong>Sync</strong> to fetch from Gmail.';
        list.innerHTML = `<p class="muted" style="padding:20px 24px;">${msg}</p>`;
        return;
      }
      const _clf = (e) => {
        if (e.classification === 'result_email') return `<span class="inbox-clf result">Result</span>`;
        if (e.classification === 'unknown' || !e.classification) return '';
        return `<span class="inbox-clf other">${e.classification}</span>`;
      };
      const _status = (e) => {
        if (e.pipeline_status === 'completed') return `<span class="inbox-status ok">&#10003; processed</span>`;
        return '';
      };
      list.innerHTML = data.emails.map(e => `
        <div class="inbox-row" onclick="App.openEmail('${e.id}')">
          <div class="inbox-row-main">
            <span class="inbox-from">${this._escHtml(e.from)}</span>
            <span class="inbox-subject">${this._escHtml(e.subject)}</span>
            <span class="inbox-snippet">${this._escHtml(e.snippet)}</span>
          </div>
          <div class="inbox-row-meta">
            ${_clf(e)}
            ${_status(e)}
            ${e.attachments && e.attachments.length ? `<span class="inbox-att" title="${e.attachments.length} attachment(s)"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21.44 11.05l-9.19 9.19a6 6 0 01-8.49-8.49l9.19-9.19a4 4 0 015.66 5.66l-9.2 9.19a2 2 0 01-2.83-2.83l8.49-8.48"/></svg>${e.attachments.length}</span>` : ''}
            <span class="inbox-date">${this._fmtDate(e.date)}</span>
          </div>
        </div>
      `).join('');
    } catch (e) {
      list.innerHTML = `<p class="muted" style="padding:20px 24px;">Could not load emails: ${e.message}</p>`;
    }
  },

  setInboxFilter(classification) {
    this._inboxFilter = classification;
    // Update toggle button styles
    const rBtn = document.getElementById('inbox-filter-results');
    const aBtn = document.getElementById('inbox-filter-all');
    if (rBtn && aBtn) {
      if (classification === 'result_email') {
        rBtn.style.background = 'var(--accent)'; rBtn.style.color = '#fff';
        aBtn.style.background = 'transparent'; aBtn.style.color = 'var(--text-dim)';
      } else {
        aBtn.style.background = 'var(--accent)'; aBtn.style.color = '#fff';
        rBtn.style.background = 'transparent'; rBtn.style.color = 'var(--text-dim)';
      }
    }
    const q = document.getElementById('inbox-search')?.value || '';
    this.loadSyncedEmails(q, classification);
  },

  openEmail(id) {
    // Find in DOM to get full data — re-fetch from cache by navigating
    // For now open Gmail directly
    window.open(`https://mail.google.com/mail/u/0/#inbox/${id}`, '_blank');
  },

  filterInbox() {
    const q = document.getElementById('inbox-search')?.value || '';
    clearTimeout(this._inboxSearchTimer);
    this._inboxSearchTimer = setTimeout(() => this.loadSyncedEmails(q, this._inboxFilter ?? 'result_email'), 300);
  },

  _escHtml(s) {
    return (s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  },

  _fmtDate(dateStr) {
    if (!dateStr) return '';
    try {
      const d = new Date(dateStr);
      const now = new Date();
      const diff = now - d;
      if (diff < 86400000) return d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'});
      if (diff < 604800000) return d.toLocaleDateString([], {weekday:'short'});
      return d.toLocaleDateString([], {month:'short', day:'numeric'});
    } catch { return dateStr.slice(0, 10); }
  },

  // ── Email Accounts ──────────────────────────────────────────────────

  _selectedProvider: 'imap',

  async loadAccountsSummary() {
    try {
      const accounts = await this.apiGet('/api/v1/accounts');
      const summaryText = document.getElementById('accounts-summary-text');
      const badge = document.getElementById('accounts-badge');
      if (accounts.length === 0) {
        summaryText.textContent = 'No mail accounts';
        badge.style.display = 'none';
      } else {
        const connected = accounts.filter(a => a.status === 'connected').length;
        summaryText.textContent = `${accounts.length} account${accounts.length > 1 ? 's' : ''} (${connected} connected)`;
        badge.textContent = accounts.length;
        badge.style.display = 'inline-block';
      }
    } catch {
      document.getElementById('accounts-summary-text').textContent = 'Accounts unavailable';
    }
  },

  async loadAccounts() {
    const list = document.getElementById('accounts-list');
    const empty = document.getElementById('accounts-empty');
    list.querySelectorAll('.account-card').forEach(el => el.remove());
    try {
      const accounts = await this.apiGet('/api/v1/accounts');
      if (accounts.length === 0) {
        empty.style.display = 'block';
        return;
      }
      empty.style.display = 'none';
      accounts.forEach(a => {
        const card = document.createElement('div');
        card.className = 'account-card';

        // Build the left side — for connected Gmail show profile
        let leftHtml;
        if (a.provider === 'gmail' && a.status === 'connected' && (a.profile_name || a.display_info)) {
          const avatar = a.profile_picture
            ? `<img class="account-avatar" src="${a.profile_picture}" alt="${this._escHtml(a.profile_name)}">`
            : `<div class="account-avatar account-avatar-fallback">${(a.profile_name || a.display_info || 'G').charAt(0).toUpperCase()}</div>`;
          leftHtml = `
            <span class="provider-chip ${a.provider}">${this._providerLabel(a.provider)}</span>
            <div class="account-card-info">
              <div class="account-profile">
                ${avatar}
                <div class="account-profile-text">
                  <span class="account-profile-name">${this._escHtml(a.profile_name || a.display_info)}</span>
                  ${a.profile_name ? `<span class="account-display">${this._escHtml(a.display_info)}</span>` : ''}
                  ${a.detail ? `<span class="account-detail ${a.status}">${a.detail}</span>` : ''}
                </div>
              </div>
            </div>
          `;
        } else {
          leftHtml = `
            <span class="provider-chip ${a.provider}">${this._providerLabel(a.provider)}</span>
            <div class="account-card-info">
              <span class="account-label">${a.label}</span>
              <span class="account-display">${a.display_info || ''}</span>
              ${a.detail ? `<span class="account-detail ${a.status}">${a.detail}</span>` : ''}
            </div>
          `;
        }

        // Right side — for Gmail OAuth show Reconnect + Disconnect; others show Authorize
        const gmailOAuth = a.provider === 'gmail' && a.account_id === 'gmail_oauth';
        const rightHtml = `
          <span class="status-dot status-${a.status}" title="${a.status}"></span>
          ${gmailOAuth
            ? `<button class="btn btn-sm" onclick="App.connectGmail()">Reconnect</button>
               <button class="btn btn-sm btn-danger" onclick="App.disconnectGmail(this)">Disconnect</button>`
            : `${a.provider === 'gmail' ? `<button class="btn btn-sm" onclick="App.connectGmail()">Authorize Gmail</button>` : ''}
               <button class="btn btn-sm" onclick="App.testAccount('${a.account_id}', this)">Test</button>
               <button class="btn btn-sm btn-danger" onclick="App.deleteAccount('${a.account_id}')">Remove</button>`
          }
        `;

        card.innerHTML = `
          <div class="account-card-left">${leftHtml}</div>
          <div class="account-card-right">${rightHtml}</div>
        `;
        list.appendChild(card);
      });
    } catch (e) {
      empty.style.display = 'block';
      empty.textContent = 'Failed to load accounts: ' + e.message;
    }
  },

  _providerLabel(p) {
    return { gmail: 'Gmail', imap: 'IMAP', msgraph: 'Microsoft 365', webhook: 'Webhook' }[p] || p;
  },

  async testAccount(accountId, btn) {
    const orig = btn.textContent;
    btn.disabled = true;
    btn.textContent = 'Testing...';
    try {
      const data = await this.apiPost(`/api/v1/accounts/${accountId}/test`, {});
      this.toast(`${data.label}: ${data.detail || data.status}`, data.status === 'connected' ? 'success' : 'error');
      this.loadAccounts();
      this.loadAccountsSummary();
    } catch (e) {
      this.toast('Test failed: ' + e.message, 'error');
    } finally {
      btn.disabled = false;
      btn.textContent = orig;
    }
  },

  async deleteAccount(accountId) {
    if (!confirm('Remove this email account?')) return;
    try {
      await fetch(`/api/v1/accounts/${accountId}`, { method: 'DELETE' });
      this.toast('Account removed', 'info');
      this.loadAccounts();
      this.loadAccountsSummary();
    } catch (e) {
      this.toast('Delete failed: ' + e.message, 'error');
    }
  },

  // ── AI Assistant Chat ────────────────────────────────────────────────
  _chatHistory: [],

  chatKeyDown(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      this.sendChatMessage();
    }
  },

  chatAutoResize(el) {
    el.style.height = 'auto';
    el.style.height = Math.min(el.scrollHeight, 140) + 'px';
  },

  clearChat() {
    this._chatHistory = [];
    const msgs = document.getElementById('chat-messages');
    msgs.innerHTML = `
      <div class="chat-msg assistant">
        <div class="chat-bubble">Chat cleared. Ask me anything about your students!</div>
      </div>`;
  },

  async sendChatMessage() {
    const input = document.getElementById('chat-input');
    const btn   = document.getElementById('chat-send-btn');
    const message = (input.value || '').trim();
    if (!message) return;

    // Append user bubble
    this._appendChatBubble('user', message);
    input.value = '';
    input.style.height = 'auto';
    btn.disabled = true;

    // Thinking indicator
    const thinkingId = 'thinking-' + Date.now();
    this._appendChatBubble('assistant', '<span class="chat-thinking"><span></span><span></span><span></span></span>', thinkingId);

    try {
      const data = await this.apiPost('/api/v1/chat', {
        message,
        history: this._chatHistory.slice(-10),
      });

      // Remove thinking bubble, add real reply
      document.getElementById(thinkingId)?.remove();
      this._appendChatBubble('assistant', this._mdToHtml(data.reply));

      // Update history
      this._chatHistory.push({ role: 'user', content: message });
      this._chatHistory.push({ role: 'assistant', content: data.reply });

    } catch (e) {
      document.getElementById(thinkingId)?.remove();
      this._appendChatBubble('assistant', `<span style="color:var(--red)">Error: ${this._escHtml(e.message)}</span>`);
    } finally {
      btn.disabled = false;
    }
  },

  _appendChatBubble(role, html, id) {
    const msgs = document.getElementById('chat-messages');
    const wrap = document.createElement('div');
    wrap.className = `chat-msg ${role}`;
    if (id) wrap.id = id;
    wrap.innerHTML = `<div class="chat-bubble">${html}</div>`;
    msgs.appendChild(wrap);
    msgs.scrollTop = msgs.scrollHeight;
  },

  _mdToHtml(text) {
    // Escape HTML first
    let s = this._escHtml(text);

    // Detect pipe-table rows (e.g. "| Code | Name | Marks |")
    const tableLines = s.split('\n');
    let result = '';
    let inTable = false;
    let tableHeaderDone = false;
    for (let i = 0; i < tableLines.length; i++) {
      const line = tableLines[i];
      const isTableRow = /^\s*\|/.test(line);
      const isSepRow   = /^\s*\|[\s\-|]+\|/.test(line);
      if (isTableRow && isSepRow) { tableHeaderDone = true; continue; } // skip separator
      if (isTableRow) {
        if (!inTable) {
          result += '<table class="chat-table"><thead>';
          inTable = true;
          tableHeaderDone = false;
        }
        const cells = line.split('|').filter((c, idx, arr) => idx > 0 && idx < arr.length - 1);
        const tag = (!tableHeaderDone) ? 'th' : 'td';
        if (!tableHeaderDone) { result += '<tr>' + cells.map(c => `<th>${c.trim()}</th>`).join('') + '</tr></thead><tbody>'; tableHeaderDone = true; }
        else { result += '<tr>' + cells.map(c => `<td>${c.trim()}</td>`).join('') + '</tr>'; }
      } else {
        if (inTable) { result += '</tbody></table>'; inTable = false; tableHeaderDone = false; }
        result += line + '\n';
      }
    }
    if (inTable) result += '</tbody></table>';
    s = result;

    // Bold **text**
    s = s.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    // Inline code
    s = s.replace(/`(.+?)`/g, '<code>$1</code>');
    // Bullet points: lines starting with * or -
    s = s.replace(/^[*\-] (.+)$/gm, '<li>$1</li>');
    s = s.replace(/(<li>.*<\/li>\n?)+/g, m => `<ul>${m}</ul>`);
    // Newlines outside of table/list
    s = s.replace(/(?<!>)\n(?!<)/g, '<br>');

    return s;
  },

  connectGmail() {
    // Pass the email as login_hint so Google pre-selects the right account
    const email = (document.getElementById('acc-email')?.value || '').trim();
    const url = email
      ? `/api/v1/auth/login?hint=${encodeURIComponent(email)}`
      : '/api/v1/auth/login';
    window.location.href = url;
  },

  async disconnectGmail(btn) {
    if (!confirm('Disconnect your Gmail account? You will need to re-authorize to sync emails.')) return;
    btn.disabled = true;
    try {
      await fetch('/api/v1/auth/logout', { method: 'POST' });
      this.toast('Gmail disconnected', 'info');
      this.loadAccounts();
      this.loadAccountsSummary();
    } catch (e) {
      this.toast('Disconnect failed: ' + e.message, 'error');
    } finally {
      btn.disabled = false;
    }
  },

  // ═══════════════════════════════════════════════════════════════
  //  Add Account — modern 2-step modal
  // ═══════════════════════════════════════════════════════════════

  _IMAP_MAP: {
    'gmail.com':        { host: 'imap.gmail.com',              port: 993, label: 'Gmail',           hint: 'gmail' },
    'googlemail.com':   { host: 'imap.gmail.com',              port: 993, label: 'Gmail',           hint: 'gmail' },
    'outlook.com':      { host: 'outlook.office365.com',       port: 993, label: 'Outlook.com',     hint: '' },
    'hotmail.com':      { host: 'outlook.office365.com',       port: 993, label: 'Hotmail',         hint: '' },
    'live.com':         { host: 'outlook.office365.com',       port: 993, label: 'Live.com',        hint: '' },
    'yahoo.com':        { host: 'imap.mail.yahoo.com',         port: 993, label: 'Yahoo Mail',      hint: '' },
    'yahoo.co.in':      { host: 'imap.mail.yahoo.com',         port: 993, label: 'Yahoo Mail',      hint: '' },
    'icloud.com':       { host: 'imap.mail.me.com',            port: 993, label: 'iCloud Mail',     hint: '' },
    'me.com':           { host: 'imap.mail.me.com',            port: 993, label: 'iCloud Mail',     hint: '' },
    'zoho.com':         { host: 'imap.zoho.com',               port: 993, label: 'Zoho Mail',       hint: '' },
    'protonmail.com':   { host: '127.0.0.1',                   port: 1143, label: 'ProtonMail Bridge', hint: '' },
    'proton.me':        { host: '127.0.0.1',                   port: 1143, label: 'ProtonMail Bridge', hint: '' },
  },

  // typing in step-1 input — just keep button enabled
  onEmailType(value) {
    document.getElementById('ea-continue-btn').disabled = !value.includes('@');
  },

  // MX-record lookup via Google DNS-over-HTTPS to detect Google Workspace / M365
  async _detectMx(domain) {
    try {
      const r = await fetch(`https://dns.google/resolve?name=${encodeURIComponent(domain)}&type=MX`);
      if (!r.ok) return null;
      const d = await r.json();
      const mx = (d.Answer || []).map(a => (a.data || '').toLowerCase()).join(' ');
      if (mx.includes('google') || mx.includes('aspmx') || mx.includes('googlemail')) return 'google';
      if (mx.includes('outlook') || mx.includes('protection.outlook') || mx.includes('mail.protection')) return 'microsoft';
      return null;
    } catch { return null; }
  },

  // "Continue" clicked on step 1
  async emailContinue() {
    const email = document.getElementById('acc-email').value.trim();
    if (!email || !email.includes('@')) {
      document.getElementById('acc-email').focus();
      return;
    }
    const domain = email.split('@')[1].toLowerCase();
    const contBtn = document.getElementById('ea-continue-btn');

    // Check local map first (instant)
    let known = this._IMAP_MAP[domain];

    // If not in local map, do MX lookup to detect Google Workspace / M365
    if (!known) {
      contBtn.innerHTML = '<span class="spinner"></span> Detecting…';
      contBtn.disabled = true;
      const mx = await this._detectMx(domain);
      contBtn.innerHTML = 'Continue <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>';
      contBtn.disabled = false;
      if (mx === 'google') {
        // Treat as Google Workspace — same IMAP as Gmail
        known = { host: 'imap.gmail.com', port: 993, label: 'Google Workspace', hint: 'gmail' };
      } else if (mx === 'microsoft') {
        // Jump straight to M365 form
        document.getElementById('mg-user-email').value = email;
        this._showMsgraphForm();
        return;
      }
    }

    const host = known ? known.host : `imap.${domain}`;
    const port = known ? known.port : 993;

    // Pre-fill hidden advanced fields
    document.getElementById('imap-host').value    = host;
    document.getElementById('imap-port').value    = String(port);
    document.getElementById('imap-mailbox').value = 'INBOX';
    document.getElementById('imap-ssl').checked   = true;

    // Detected email pill
    const label = known ? known.label : domain;
    document.getElementById('ea-detected-email').textContent = email;
    document.getElementById('ea-detected-badge').textContent = label;
    document.getElementById('ea-server-text').textContent    = `${host}:${port} · SSL`;

    // Provider icon in pill
    const iconEl = document.getElementById('ea-detected-icon');
    if (known && known.hint === 'gmail') {
      iconEl.innerHTML = `<svg width="18" height="18" viewBox="0 0 48 48"><path fill="#EA4335" d="M24 9.5c3.5 0 6.6 1.2 9.1 3.2l6.8-6.8C35.8 2.2 30.2 0 24 0 14.7 0 6.7 5.4 2.9 13.3l7.9 6.1C12.6 13.4 17.9 9.5 24 9.5z"/><path fill="#4285F4" d="M46.5 24.5c0-1.6-.1-3.1-.4-4.5H24v8.5h12.7c-.6 3-2.3 5.5-4.8 7.2l7.5 5.8c4.4-4.1 7.1-10.1 7.1-17z"/><path fill="#FBBC05" d="M10.8 28.6A14.5 14.5 0 0 1 9.5 24c0-1.6.3-3.2.8-4.6L2.4 13.3A24 24 0 0 0 0 24c0 3.9.9 7.5 2.5 10.8l8.3-6.2z"/><path fill="#34A853" d="M24 48c6.5 0 11.9-2.1 15.9-5.8l-7.5-5.8c-2.1 1.4-4.8 2.2-8.4 2.2-6.1 0-11.4-3.9-13.2-9.4l-7.9 6.1C6.7 42.6 14.7 48 24 48z"/></svg>`;
    } else if (['outlook.com','hotmail.com','live.com'].includes(domain)) {
      iconEl.innerHTML = `<svg width="18" height="18" viewBox="0 0 23 23" fill="none"><rect x="1" y="1" width="10" height="10" fill="#f25022"/><rect x="12" y="1" width="10" height="10" fill="#7fba00"/><rect x="1" y="12" width="10" height="10" fill="#00a4ef"/><rect x="12" y="12" width="10" height="10" fill="#ffb900"/></svg>`;
    } else {
      iconEl.innerHTML = `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="var(--accent)" stroke-width="2"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/></svg>`;
    }

    this._goStep2();

    // Route to right auth UI based on provider
    if (known && known.hint === 'gmail') {
      // Google / Google Workspace → show OAuth button as primary action
      document.getElementById('ea-oauth-google').style.display = 'block';
      document.getElementById('ea-imap-form').style.display    = 'none';
    } else {
      // IMAP / other provider → go straight to password
      document.getElementById('ea-oauth-google').style.display = 'none';
      document.getElementById('ea-imap-form').style.display    = 'block';
      document.getElementById('ea-gmail-warn').style.display   = 'none';
      document.getElementById('acc-password').placeholder      = 'Password';
      setTimeout(() => document.getElementById('acc-password').focus(), 60);
    }
  },

  // User clicked "Use App Password instead" under the Google OAuth button
  showImapForm() {
    document.getElementById('ea-oauth-google').style.display = 'none';
    document.getElementById('ea-imap-form').style.display    = 'block';
    document.getElementById('ea-gmail-warn').style.display   = 'flex';
    document.getElementById('acc-password').placeholder      = 'App Password (not your Google password)';
    setTimeout(() => document.getElementById('acc-password').focus(), 60);
  },

  _goStep1() {
    document.getElementById('ea-step1').style.display = 'flex';
    document.getElementById('ea-step2').style.display = 'none';
    document.getElementById('ea-step3').style.display = 'none';
    setTimeout(() => document.getElementById('acc-email').focus(), 60);
  },
  _goStep2() {
    document.getElementById('ea-step1').style.display = 'none';
    document.getElementById('ea-step2').style.display = 'flex';
    document.getElementById('ea-step3').style.display = 'none';
    const r = document.getElementById('ea-result');
    if (r) { r.style.display='none'; r.innerHTML=''; }
  },

  toggleAdvanced() {
    const adv = document.getElementById('acc-advanced');
    const open = adv.style.display === 'none';
    adv.style.display = open ? 'block' : 'none';
    document.querySelector('#ea-server-row .ea-link').textContent = open ? 'Hide' : 'Edit';
  },

  openAddAccountModal() {
    document.getElementById('add-account-modal').style.display = 'flex';
    this._resetAddAccountForm();
    setTimeout(() => document.getElementById('acc-email').focus(), 60);
  },

  closeAddAccountModal(event) {
    const shouldClose = !event || event.target === document.getElementById('add-account-modal');
    if (shouldClose) {
      document.getElementById('add-account-modal').style.display = 'none';
      this._resetAddAccountForm();
    }
  },

  _showMsgraphForm() {
    document.getElementById('ea-step1').style.display = 'none';
    document.getElementById('ea-step2').style.display = 'none';
    document.getElementById('ea-step3').style.display = 'flex';
    setTimeout(() => document.getElementById('mg-user-email').focus(), 60);
  },

  _resetAddAccountForm() {
    ['acc-email','acc-password','imap-host',
     'mg-user-email','mg-tenant-id','mg-client-id','mg-client-secret'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.value = '';
    });
    document.getElementById('imap-port').value    = '993';
    document.getElementById('imap-mailbox').value = 'INBOX';
    document.getElementById('imap-ssl').checked   = true;
    document.getElementById('acc-advanced').style.display     = 'none';
    document.getElementById('ea-gmail-warn').style.display    = 'none';
    document.getElementById('ea-oauth-google').style.display  = 'none';
    document.getElementById('ea-imap-form').style.display     = 'none';
    document.getElementById('ea-continue-btn').disabled = true;
    ['ea-result','ea-result-mg'].forEach(id => {
      const el = document.getElementById(id);
      if (el) { el.style.display='none'; el.innerHTML=''; }
    });
    this._goStep1();
  },

  // ── Quick connect (IMAP / Gmail) ──
  async submitAddAccount() {
    const resultBox = document.getElementById('ea-result');
    const submitBtn = document.getElementById('ea-connect-btn');

    const email    = document.getElementById('acc-email').value.trim();
    const password = document.getElementById('acc-password').value;
    if (!email)    return this._stepError(resultBox, 'Enter your email address');
    if (!password) return this._stepError(resultBox, 'Enter your password or App Password');

    const domain  = (email.split('@')[1] || '').toLowerCase();
    const known   = this._IMAP_MAP[domain];
    const host    = document.getElementById('imap-host').value.trim() || (known ? known.host : `imap.${domain}`);
    const port    = parseInt(document.getElementById('imap-port').value) || 993;
    const ssl     = document.getElementById('imap-ssl').checked;
    const mailbox = document.getElementById('imap-mailbox').value.trim() || 'INBOX';

    // Provider: gmail domain → 'gmail' so it shows correctly in cards; rest → 'imap'
    const provider = (known && known.hint === 'gmail') ? 'gmail' : 'imap';

    const body = {
      provider,
      label: email,
      imap_host: host,
      imap_port: port,
      imap_use_ssl: ssl,
      imap_username: email,
      imap_password: password,
      imap_mailbox: mailbox,
      imap_oauth2_token: '',
    };

    submitBtn.disabled = true;
    resultBox.style.display = 'none';
    submitBtn.innerHTML = '<span class="spinner"></span> Connecting...';

    let saved;
    try {
      saved = await this.apiPost('/api/v1/accounts', body);
    } catch (e) {
      this._stepError(resultBox, 'Failed to save: ' + e.message);
      submitBtn.disabled = false;
      submitBtn.innerHTML = 'Connect <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>';
      return;
    }

    try {
      const testResult = await this.apiPost(`/api/v1/accounts/${saved.account_id}/test`, {});
      resultBox.style.display = 'block';
      if (testResult.status === 'connected') {
        resultBox.className = 'conn-test-result success';
        resultBox.innerHTML = `
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
          <span><strong>Connected!</strong> ${testResult.detail}</span>
          <button class="ea-done-btn" onclick="App._closeAfterSuccess()">Done</button>
        `;
        this.loadAccounts(); this.loadAccountsSummary();
      } else {
        this._stepError(resultBox, testResult.detail, 'Could not connect');
        this.loadAccounts(); this.loadAccountsSummary();
      }
    } catch (e) {
      this._stepError(resultBox, e.message, 'Connection error');
    } finally {
      submitBtn.disabled = false;
      submitBtn.innerHTML = 'Connect <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>';
    }
  },

  // ── Microsoft 365 Graph connect ──
  async submitMsgraph() {
    const userEmail    = document.getElementById('mg-user-email').value.trim();
    const tenantId     = document.getElementById('mg-tenant-id').value.trim();
    const clientId     = document.getElementById('mg-client-id').value.trim();
    const clientSecret = document.getElementById('mg-client-secret').value.trim();
    const submitBtn    = document.getElementById('mg-connect-btn');
    const resultBox    = document.getElementById('ea-result-mg');

    if (!userEmail)    return this._stepError(resultBox, 'Mailbox email is required');
    if (!tenantId)     return this._stepError(resultBox, 'Tenant ID is required');
    if (!clientId)     return this._stepError(resultBox, 'Client ID is required');
    if (!clientSecret) return this._stepError(resultBox, 'Client Secret is required');

    submitBtn.disabled = true;
    resultBox.style.display = 'none';
    submitBtn.innerHTML = '<span class="spinner"></span> Connecting...';

    const body = {
      provider: 'msgraph', label: userEmail,
      msgraph_user_email: userEmail, msgraph_tenant_id: tenantId,
      msgraph_client_id: clientId,   msgraph_client_secret: clientSecret,
    };

    let saved;
    try {
      saved = await this.apiPost('/api/v1/accounts', body);
    } catch (e) {
      this._stepError(resultBox, 'Save failed: ' + e.message);
      submitBtn.disabled = false;
      submitBtn.innerHTML = 'Connect <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>';
      return;
    }

    try {
      const testResult = await this.apiPost(`/api/v1/accounts/${saved.account_id}/test`, {});
      resultBox.style.display = 'block';
      if (testResult.status === 'connected') {
        resultBox.className = 'conn-test-result success';
        resultBox.innerHTML = `
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
          <span><strong>Connected!</strong> ${testResult.detail}</span>
          <button class="ea-done-btn" onclick="App._closeAfterSuccess()">Done</button>
        `;
        this.loadAccounts(); this.loadAccountsSummary();
      } else {
        this._stepError(resultBox, testResult.detail, 'Could not connect');
        this.loadAccounts(); this.loadAccountsSummary();
      }
    } catch (e) {
      this._stepError(resultBox, e.message, 'Connection error');
    } finally {
      submitBtn.disabled = false;
      submitBtn.innerHTML = 'Connect <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>';
    }
  },

  _closeAfterSuccess() {
    document.getElementById('add-account-modal').style.display = 'none';
    this._resetAddAccountForm();
  },

  _stepError(el, detail, title) {
    el.style.display = 'block';
    el.className = 'conn-test-result error';

    // Add actionable tips for common DNS / server-not-found errors
    let tip = '';
    const d = (detail || '').toLowerCase();
    if (d.includes('getaddrinfo') || d.includes('name or service not known') ||
        d.includes('nodename nor servname') || d.includes('socket') && d.includes('connect')) {
      // Server hostname doesn't exist — guide user
      tip = `<p class="ea-err-tip">💡 <strong>Server not found.</strong> Your mail provider may use a different server. Try clicking <em>Edit</em> above and use one of:<br/>
&nbsp;• Google Workspace / college Gmail → <code>imap.gmail.com</code><br/>
&nbsp;• Microsoft 365 → use the <em>Microsoft</em> option instead</p>`;
    } else if (d.includes('authentication') || d.includes('invalid credentials') || d.includes('535') || d.includes('login failed')) {
      tip = `<p class="ea-err-tip">💡 If you have 2-step verification enabled, you need an <strong>App Password</strong> — your regular password won't work.</p>`;
    } else if (d.includes('ssl') || d.includes('tls') || d.includes('certificate')) {
      tip = `<p class="ea-err-tip">💡 SSL/TLS error. Try clicking <em>Edit</em> and toggling the SSL option, or verify the port (993 = SSL, 143 = STARTTLS).</p>`;
    }

    el.innerHTML = `
      <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
      <div>${title ? `<strong>${title}</strong><br/>` : ''}<span>${detail}</span>${tip}</div>
    `;
  },
};

// Boot
document.addEventListener('DOMContentLoaded', () => App.init());
