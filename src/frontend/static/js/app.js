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
    this.loadGmailStatus();
    this.loadDashboard();
    setInterval(() => this.loadHealth(), 30000);
    setInterval(() => this.loadGmailStatus(), 60000);
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
    const titles = { dashboard: 'Dashboard', query: 'Query Engine', student: 'Student Lookup', admin: 'Admin Panel' };
    document.getElementById('page-title').textContent = titles[page] || page;

    // Close mobile menu
    document.getElementById('sidebar')?.classList.remove('open');

    // Reload data for certain pages
    if (page === 'dashboard') this.loadDashboard();
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
  },

  async loadPipelineStatus() {
    try {
      const data = await this.apiGet('/api/v1/admin/status');
      document.getElementById('ps-emails').textContent = data.emails_processed_24h;
      document.getElementById('ps-docs').textContent = data.documents_parsed_24h;
      document.getElementById('ps-records').textContent = data.records_extracted_24h;
      document.getElementById('ps-pending').textContent = data.pending_emails;
      document.getElementById('ps-failed').textContent = data.failed_documents;
      document.getElementById('ps-agent').textContent = data.agent_runs_24h;
      document.getElementById('ps-avgtime').textContent = data.avg_processing_time_ms.toFixed(1) + ' ms';
    } catch (e) {
      this.toast('Failed to load pipeline status: ' + e.message, 'error');
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
      const data = await this.apiGet('/api/v1/admin/stats');
      if (data.error) {
        document.getElementById('stat-emails').textContent = '—';
        document.getElementById('stat-students').textContent = '—';
        document.getElementById('stat-results').textContent = '—';
        document.getElementById('stat-backlogs').textContent = '—';
        document.getElementById('stat-avgcgpa').textContent = '—';
        document.getElementById('stat-extractions').textContent = '—';
        return;
      }
      document.getElementById('stat-emails').textContent = this.formatNum(data.total_emails || 0);
      document.getElementById('stat-students').textContent = this.formatNum(data.total_students || 0);
      document.getElementById('stat-results').textContent = this.formatNum(data.total_results || 0);
      document.getElementById('stat-backlogs').textContent = this.formatNum(data.students_with_backlogs || 0);
      document.getElementById('stat-avgcgpa').textContent = data.avg_cgpa || '—';
      document.getElementById('stat-extractions').textContent = this.formatNum(data.total_extractions || 0);
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
    if (!spec) {
      container.style.display = 'none';
      return;
    }
    container.style.display = 'block';
    if (this.queryChart) this.queryChart.destroy();
    const ctx = document.getElementById('query-chart').getContext('2d');
    this.queryChart = new Chart(ctx, {
      type: spec.type || 'bar',
      data: spec.data,
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { labels: { color: '#e4e6eb' } },
        },
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
    const usn = input.value.trim().toUpperCase();
    if (!usn) return;

    const btn = document.getElementById('student-submit');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Looking up...';

    document.getElementById('student-result').style.display = 'none';

    try {
      const [student, trend] = await Promise.all([
        this.apiGet(`/api/v1/student/${usn}`),
        this.apiGet(`/api/v1/student/${usn}/trend`).catch(() => null),
      ]);

      // Profile
      document.getElementById('sp-usn').textContent = student.usn;
      document.getElementById('sp-name').textContent = student.name;
      document.getElementById('sp-dept').textContent = student.department;
      document.getElementById('sp-batch').textContent = student.batch_year;
      document.getElementById('sp-cgpa').textContent = student.current_cgpa.toFixed(2);
      document.getElementById('sp-backlogs').textContent = student.active_backlogs;

      // Semester table
      const tbody = document.getElementById('semester-tbody');
      tbody.innerHTML = (student.semesters || []).map(s => `
        <tr>
          <td>Sem ${s.semester}</td>
          <td>${s.sgpa?.toFixed(2) ?? '—'}</td>
          <td>${s.credits_earned ?? '—'}</td>
          <td>${s.credits_attempted ?? '—'}</td>
          <td>${s.subjects_passed ?? '—'}</td>
          <td>${s.subjects_failed ?? '—'}</td>
        </tr>
      `).join('');

      // SGPA Trend Chart
      if (trend && trend.trend && trend.trend.length > 0) {
        this.renderTrendChart(trend.trend);
      }

      document.getElementById('student-result').style.display = 'block';
      this.toast(`Loaded profile for ${student.name}`, 'success');
    } catch (e) {
      this.toast('Student lookup failed: ' + e.message, 'error');
    } finally {
      btn.disabled = false;
      btn.innerHTML = `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35"/></svg> Lookup`;
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

  // ── Admin: Ingestion ────────────────────────────────
  async triggerIngestion() {
    const maxEmails = parseInt(document.getElementById('ingest-max').value) || 50;
    const sinceHours = parseInt(document.getElementById('ingest-hours').value) || 24;
    const resultBox = document.getElementById('ingest-result');

    try {
      const data = await this.apiPost('/api/v1/admin/ingest', {
        max_emails: maxEmails,
        since_hours: sinceHours,
      });
      resultBox.style.display = 'block';
      resultBox.className = 'result-box success';
      resultBox.textContent = JSON.stringify(data, null, 2);
      this.toast('Ingestion triggered', 'success');
    } catch (e) {
      resultBox.style.display = 'block';
      resultBox.className = 'result-box error';
      resultBox.textContent = e.message;
      this.toast('Ingestion failed: ' + e.message, 'error');
    }
  },

  // ── Admin: Reprocess ────────────────────────────────
  async reprocessDocument() {
    const attachmentId = document.getElementById('reprocess-id').value.trim();
    if (!attachmentId) return this.toast('Enter an attachment ID', 'error');
    const force = document.getElementById('reprocess-force').checked;
    const resultBox = document.getElementById('reprocess-result');

    try {
      const data = await this.apiPost('/api/v1/admin/reprocess', {
        attachment_id: attachmentId,
        force,
      });
      resultBox.style.display = 'block';
      resultBox.className = 'result-box success';
      resultBox.textContent = JSON.stringify(data, null, 2);
      this.toast('Reprocessing started', 'success');
    } catch (e) {
      resultBox.style.display = 'block';
      resultBox.className = 'result-box error';
      resultBox.textContent = e.message;
      this.toast('Reprocess failed: ' + e.message, 'error');
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

  // ── Gmail Auth ──────────────────────────────────────
  handleAuthCallback() {
    const params = new URLSearchParams(window.location.search);
    const auth = params.get('auth');
    if (auth === 'success') {
      this.toast('Gmail connected successfully!', 'success');
      window.history.replaceState({}, '', '/');
    } else if (auth === 'error') {
      const msg = params.get('message') || 'Unknown error';
      this.toast('Gmail auth failed: ' + msg, 'error');
      window.history.replaceState({}, '', '/');
    }
  },

  async loadGmailStatus() {
    const statusEl = document.getElementById('gmail-status');
    const btnEl = document.getElementById('gmail-btn');
    const userEl = document.getElementById('gmail-user');
    const avatarEl = document.getElementById('gmail-avatar');
    const emailEl = document.getElementById('gmail-email');

    try {
      const data = await this.apiGet('/api/v1/auth/status');
      if (data.connected) {
        statusEl.className = 'gmail-status connected';
        statusEl.querySelector('.gmail-text').textContent = data.email || 'Connected';
        btnEl.style.display = 'none';

        // Show in topbar
        userEl.style.display = 'flex';
        emailEl.textContent = data.email || '';
        if (data.picture) {
          avatarEl.src = data.picture;
          avatarEl.style.display = 'block';
        } else {
          avatarEl.style.display = 'none';
        }
      } else {
        statusEl.className = 'gmail-status disconnected';
        statusEl.querySelector('.gmail-text').textContent = 'Gmail not connected';
        btnEl.style.display = 'block';
        userEl.style.display = 'none';
      }
    } catch {
      statusEl.className = 'gmail-status disconnected';
      statusEl.querySelector('.gmail-text').textContent = 'Gmail not connected';
      btnEl.style.display = 'block';
      userEl.style.display = 'none';
    }
  },

  connectGmail() {
    window.location.href = '/api/v1/auth/login';
  },

  async disconnectGmail() {
    if (!confirm('Disconnect your Gmail account?')) return;
    try {
      await this.apiPost('/api/v1/auth/logout', {});
      this.toast('Gmail disconnected', 'info');
      this.loadGmailStatus();
    } catch (e) {
      this.toast('Disconnect failed: ' + e.message, 'error');
    }
  },
};

// Boot
document.addEventListener('DOMContentLoaded', () => App.init());
