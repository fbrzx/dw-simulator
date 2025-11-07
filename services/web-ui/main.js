const API_BASE = 'http://localhost:8000/api';
const statusEl = document.getElementById('status');
const experimentListEl = document.getElementById('experiment-list');
const schemaInput = document.getElementById('schema-input');
const jsonForm = document.getElementById('json-form');
const sqlForm = document.getElementById('sql-form');
const sqlInput = document.getElementById('sql-input');
const sqlNameInput = document.getElementById('sql-experiment-name');
const dialectSelect = document.getElementById('dialect-select');
const modeTabs = document.querySelectorAll('.mode-tab');

const setStatus = (message, type = 'info') => {
  if (!statusEl) return;
  statusEl.textContent = message;
  statusEl.className = type;
};

let activeMode = 'json';

const switchMode = (mode) => {
  activeMode = mode;
  modeTabs.forEach((tab) => tab.classList.toggle('active', tab.dataset.mode === mode));
  if (mode === 'json') {
    jsonForm.classList.remove('hidden');
    sqlForm.classList.add('hidden');
  } else {
    sqlForm.classList.remove('hidden');
    jsonForm.classList.add('hidden');
  }
};

modeTabs.forEach((tab) => {
  tab.addEventListener('click', () => switchMode(tab.dataset.mode));
});

const fetchExperiments = async () => {
  setStatus('Loading experiments...');
  try {
    const response = await fetch(`${API_BASE}/experiments`);
    if (!response.ok) throw new Error('Failed to load experiments');
    const data = await response.json();
    renderExperiments(data.experiments ?? []);
    setStatus(`Loaded ${data.experiments.length} experiment(s).`);
  } catch (error) {
    console.error(error);
    setStatus(error.message, 'error');
  }
};

const renderExperiments = (experiments) => {
  experimentListEl.innerHTML = '';
  if (!experiments.length) {
    experimentListEl.innerHTML = '<li>No experiments yet. Create one below.</li>';
    return;
  }

  experiments.forEach((experiment) => {
    const item = document.createElement('li');
    item.className = 'experiment-card';
    item.innerHTML = `
      <div>
        <strong>${experiment.name}</strong>
        <small>${experiment.table_count} table(s) · Created ${new Date(experiment.created_at).toLocaleString()}</small>
      </div>
      <div class="button-group">
        <button class="secondary view-runs-btn" data-name="${experiment.name}">View Runs</button>
        <button class="secondary generate-btn" data-name="${experiment.name}">Generate</button>
        <button class="danger" data-name="${experiment.name}">Delete</button>
      </div>
    `;
    item.querySelector('.view-runs-btn').addEventListener('click', () => openRunsModal(experiment.name));
    item.querySelector('.generate-btn').addEventListener('click', () => openGenerateModal(experiment.name));
    item.querySelector('.danger').addEventListener('click', () => deleteExperiment(experiment.name));
    experimentListEl.appendChild(item);
  });
};

const createExperimentFromJson = async (event) => {
  event.preventDefault();
  let payload;
  try {
    payload = JSON.parse(schemaInput.value);
  } catch (error) {
    setStatus('Invalid JSON payload.', 'error');
    return;
  }

  setStatus('Creating experiment...');
  try {
    const response = await fetch(`${API_BASE}/experiments`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const errorBody = await response.json();
      throw new Error((errorBody.detail || []).join(' '));
    }
    schemaInput.value = '';
    setStatus('Experiment created!', 'success');
    await fetchExperiments();
  } catch (error) {
    console.error(error);
    setStatus(error.message || 'Failed to create experiment', 'error');
  }
};

const importExperimentFromSql = async (event) => {
  event.preventDefault();
  const name = sqlNameInput.value.trim();
  if (!name) {
    setStatus('Experiment name is required.', 'error');
    return;
  }
  if (!sqlInput.value.trim()) {
    setStatus('SQL input is required.', 'error');
    return;
  }

  setStatus('Importing SQL experiment...');
  try {
    const response = await fetch(`${API_BASE}/experiments/import-sql`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name,
        sql: sqlInput.value,
        dialect: dialectSelect.value,
      }),
    });
    if (!response.ok) {
      const errorBody = await response.json();
      throw new Error((errorBody.detail || []).join(' '));
    }
    sqlInput.value = '';
    setStatus('Experiment imported!', 'success');
    await fetchExperiments();
  } catch (error) {
    console.error(error);
    setStatus(error.message || 'Failed to import SQL experiment', 'error');
  }
};

const deleteExperiment = async (name) => {
  if (!confirm(`Delete experiment "${name}"? This removes all data.`)) {
    return;
  }
  setStatus(`Deleting ${name}...`);
  try {
    const response = await fetch(`${API_BASE}/experiments/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    });
    if (!response.ok) {
      const errorBody = await response.json();
      throw new Error((errorBody.detail || []).join(' '));
    }
    setStatus(`Experiment ${name} deleted.`, 'success');
    await fetchExperiments();
  } catch (error) {
    console.error(error);
    setStatus(error.message || 'Failed to delete experiment', 'error');
  }
};

jsonForm.addEventListener('submit', createExperimentFromJson);
sqlForm.addEventListener('submit', importExperimentFromSql);
document.getElementById('refresh-btn').addEventListener('click', fetchExperiments);

// Generate modal handling
const modal = document.getElementById('generate-modal');
const modalClose = modal.querySelector('.modal-close');
const cancelBtn = modal.querySelector('.cancel-btn');
const generateForm = document.getElementById('generate-form');
const experimentNameEl = document.getElementById('generate-experiment-name');
const tableOverridesEl = document.getElementById('table-overrides');
const seedInput = document.getElementById('seed-input');

let currentExperiment = null;

const openGenerateModal = async (name) => {
  currentExperiment = name;
  experimentNameEl.textContent = name;

  // Fetch experiment details to get table information
  setStatus('Loading experiment details...');
  try {
    const response = await fetch(`${API_BASE}/experiments`);
    if (!response.ok) throw new Error('Failed to load experiment details');
    const data = await response.json();
    const experiment = data.experiments.find(exp => exp.name === name);

    if (!experiment || !experiment.schema) {
      throw new Error('Experiment schema not found');
    }

    // Parse schema to get table info
    const schema = typeof experiment.schema === 'string'
      ? JSON.parse(experiment.schema)
      : experiment.schema;

    // Render table override inputs
    tableOverridesEl.innerHTML = schema.tables.map(table => `
      <div class="table-override">
        <label for="rows-${table.name}">Table: ${table.name}</label>
        <small>Default target: ${table.target_rows} rows</small>
        <input
          type="number"
          id="rows-${table.name}"
          name="${table.name}"
          placeholder="${table.target_rows}"
          min="1"
        />
      </div>
    `).join('');

    seedInput.value = '';
    modal.classList.remove('hidden');
    setStatus('');
  } catch (error) {
    console.error(error);
    setStatus(error.message || 'Failed to open generate modal', 'error');
  }
};

const closeGenerateModal = () => {
  modal.classList.add('hidden');
  currentExperiment = null;
};

const generateData = async (event) => {
  event.preventDefault();

  if (!currentExperiment) {
    setStatus('No experiment selected', 'error');
    return;
  }

  // Collect row overrides from inputs
  const rowInputs = tableOverridesEl.querySelectorAll('input[type="number"]');
  const rows = {};
  rowInputs.forEach(input => {
    if (input.value && input.value.trim() !== '') {
      rows[input.name] = parseInt(input.value, 10);
    }
  });

  const payload = {};
  if (Object.keys(rows).length > 0) {
    payload.rows = rows;
  }
  if (seedInput.value && seedInput.value.trim() !== '') {
    payload.seed = parseInt(seedInput.value, 10);
  }

  closeGenerateModal();
  setStatus(`Generating data for ${currentExperiment}...`);

  try {
    const response = await fetch(`${API_BASE}/experiments/${encodeURIComponent(currentExperiment)}/generate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const errorBody = await response.json();
      throw new Error((errorBody.detail || []).join(' '));
    }

    const result = await response.json();
    const totalRows = result.tables.reduce((sum, t) => sum + t.row_count, 0);
    setStatus(`Generated ${totalRows} rows across ${result.tables.length} table(s)!`, 'success');
  } catch (error) {
    console.error(error);
    setStatus(error.message || 'Failed to generate data', 'error');
  }
};

modalClose.addEventListener('click', closeGenerateModal);
cancelBtn.addEventListener('click', closeGenerateModal);
generateForm.addEventListener('submit', generateData);

// Close modal on backdrop click
modal.addEventListener('click', (e) => {
  if (e.target === modal) {
    closeGenerateModal();
  }
});

// Generation Runs Modal
const runsModal = document.getElementById('runs-modal');
const runsModalClose = runsModal.querySelector('.runs-modal-close');
const runsExperimentNameEl = document.getElementById('runs-experiment-name');
const runsListContainer = document.getElementById('runs-list-container');

let currentRunsExperiment = null;
let runsPollingInterval = null;

const openRunsModal = async (name) => {
  currentRunsExperiment = name;
  runsExperimentNameEl.textContent = name;
  runsModal.classList.remove('hidden');

  // Load runs immediately
  await loadGenerationRuns(name);

  // Start polling for updates every 3 seconds
  if (runsPollingInterval) {
    clearInterval(runsPollingInterval);
  }
  runsPollingInterval = setInterval(() => loadGenerationRuns(name), 3000);
};

const closeRunsModal = () => {
  runsModal.classList.add('hidden');
  currentRunsExperiment = null;

  // Stop polling
  if (runsPollingInterval) {
    clearInterval(runsPollingInterval);
    runsPollingInterval = null;
  }
};

const loadGenerationRuns = async (name) => {
  try {
    const response = await fetch(`${API_BASE}/experiments/${encodeURIComponent(name)}/runs`);
    if (!response.ok) throw new Error('Failed to load generation runs');

    const data = await response.json();
    renderGenerationRuns(data.runs);
  } catch (error) {
    console.error(error);
    runsListContainer.innerHTML = '<p class="runs-loading">Failed to load runs</p>';
  }
};

const renderGenerationRuns = (runs) => {
  if (!runs || runs.length === 0) {
    runsListContainer.innerHTML = '<p class="no-runs">No generation runs yet. Click "Generate" to create one.</p>';
    return;
  }

  const list = document.createElement('ul');
  list.className = 'runs-list';

  runs.forEach((run) => {
    const item = document.createElement('li');
    item.className = 'run-card';

    // Parse row counts
    let rowCountsDisplay = '';
    try {
      const rowCounts = JSON.parse(run.row_counts || '{}');
      const entries = Object.entries(rowCounts);
      if (entries.length > 0) {
        rowCountsDisplay = `
          <div class="run-tables">
            <strong>Tables:</strong><br>
            ${entries.map(([table, count]) => `${table}: ${count.toLocaleString()} rows`).join('<br>')}
          </div>
        `;
      }
    } catch (e) {
      // Ignore parse errors
    }

    // Format dates
    const startedAt = new Date(run.started_at);
    const completedAt = run.completed_at ? new Date(run.completed_at) : null;
    const duration = completedAt
      ? `${Math.round((completedAt - startedAt) / 1000)}s`
      : 'In progress...';

    item.innerHTML = `
      <div class="run-header">
        <span><strong>Run #${run.id}</strong></span>
        <span class="run-status ${run.status}">${run.status}</span>
      </div>
      <div class="run-details">
        <div><strong>Started:</strong> ${startedAt.toLocaleString()}</div>
        ${completedAt ? `<div><strong>Completed:</strong> ${completedAt.toLocaleString()}</div>` : ''}
        <div><strong>Duration:</strong> ${duration}</div>
        ${run.seed !== null ? `<div><strong>Seed:</strong> ${run.seed}</div>` : ''}
      </div>
      ${rowCountsDisplay}
      ${run.error_message ? `<div class="run-error"><strong>Error:</strong><br>${run.error_message}</div>` : ''}
    `;

    list.appendChild(item);
  });

  runsListContainer.innerHTML = '';
  runsListContainer.appendChild(list);
};

runsModalClose.addEventListener('click', closeRunsModal);

// Close runs modal on backdrop click
runsModal.addEventListener('click', (e) => {
  if (e.target === runsModal) {
    closeRunsModal();
  }
});

switchMode('json');
fetchExperiments();
