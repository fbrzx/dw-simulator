const API_BASE = 'http://localhost:8000/api';
const statusEl = document.getElementById('status');
const experimentListEl = document.getElementById('experiment-list');
const schemaInput = document.getElementById('schema-input');
const jsonForm = document.getElementById('json-form');
const sqlForm = document.getElementById('sql-form');
const sqlInput = document.getElementById('sql-input');
const sqlNameInput = document.getElementById('sql-experiment-name');
const dialectSelect = document.getElementById('dialect-select');
const warehouseSelect = document.getElementById('warehouse-select');
const modeTabs = document.querySelectorAll('.mode-tab');
const warningBanner = document.getElementById('warning-banner');

// Main tab handling
const mainTabs = document.querySelectorAll('.main-tab');
const tabPanels = document.querySelectorAll('.tab-panel');

const switchMainTab = (tabName) => {
  mainTabs.forEach((tab) => {
    tab.classList.toggle('active', tab.dataset.tab === tabName);
  });
  tabPanels.forEach((panel) => {
    panel.classList.toggle('active', panel.id === `tab-${tabName}`);
  });
};

mainTabs.forEach((tab) => {
  tab.addEventListener('click', () => switchMainTab(tab.dataset.tab));
});

const setStatus = (message, type = 'info') => {
  if (!statusEl) return;
  statusEl.textContent = message;
  statusEl.className = type;
};

const renderWarningBanner = (warnings) => {
  if (!warningBanner) return;

  warningBanner.innerHTML = '';

  if (!warnings || warnings.length === 0) {
    warningBanner.classList.add('hidden');
    return;
  }

  warningBanner.classList.remove('hidden');

  const icon = document.createElement('span');
  icon.className = 'warning-icon';
  icon.setAttribute('aria-hidden', 'true');
  icon.textContent = '⚠️';

  const content = document.createElement('div');
  const heading = document.createElement('strong');
  heading.textContent = warnings.length === 1 ? 'Warning' : 'Warnings';
  content.appendChild(heading);

  const list = document.createElement('ul');
  warnings.forEach((warning) => {
    const item = document.createElement('li');
    item.textContent = warning;
    list.appendChild(item);
  });
  content.appendChild(list);

  warningBanner.appendChild(icon);
  warningBanner.appendChild(content);
};

const clearWarningBanner = () => {
  renderWarningBanner([]);
};

let activeMode = 'json';
let warningListCounter = 0;

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
    const experiments = data.experiments ?? [];
    renderExperiments(experiments);
    populateExperimentSelector(experiments);
    setStatus(`Loaded ${experiments.length} experiment(s).`);
  } catch (error) {
    console.error(error);
    setStatus(error.message, 'error');
  }
};

const renderExperiments = (experiments) => {
  warningListCounter = 0;
  experimentListEl.innerHTML = '';
  if (!experiments.length) {
    experimentListEl.innerHTML = '<li>No experiments yet. Create one below.</li>';
    return;
  }

  experiments.forEach((experiment) => {
    const item = document.createElement('li');
    item.className = 'experiment-card';
    const warehouseInfo = experiment.warehouse_type
      ? ` · Warehouse: <strong>${experiment.warehouse_type}</strong>`
      : '';
    item.innerHTML = `
      <div class="experiment-info">
        <strong>${experiment.name}</strong>
        <small>${experiment.table_count} table(s) · Created ${new Date(experiment.created_at).toLocaleString()}${warehouseInfo}</small>
        <div class="experiment-warnings-container"></div>
      </div>
      <div class="button-group">
        <button class="secondary view-runs-btn" data-name="${experiment.name}">View Runs</button>
        <button class="secondary generate-btn" data-name="${experiment.name}">Generate</button>
        <button class="secondary reset-btn" data-name="${experiment.name}">Reset</button>
        <button class="danger" data-name="${experiment.name}">Delete</button>
      </div>
    `;
    item.querySelector('.view-runs-btn').addEventListener('click', () => openRunsModal(experiment.name));
    item.querySelector('.generate-btn').addEventListener('click', () => openGenerateModal(experiment.name));
    item.querySelector('.reset-btn').addEventListener('click', () => resetExperiment(experiment.name));
    item.querySelector('.danger').addEventListener('click', () => deleteExperiment(experiment.name));

    if (Array.isArray(experiment.warnings) && experiment.warnings.length > 0) {
      const warningsContainer = item.querySelector('.experiment-warnings-container');
      if (warningsContainer) {
        const badge = document.createElement('button');
        badge.type = 'button';
        badge.className = 'warning-badge';
        badge.title = experiment.warnings.join('\n');

        const icon = document.createElement('span');
        icon.setAttribute('aria-hidden', 'true');
        icon.textContent = '⚠️';

        const count = document.createElement('span');
        const countLabel = experiment.warnings.length === 1 ? 'warning' : 'warnings';
        count.textContent = `${experiment.warnings.length} ${countLabel}`;

        const list = document.createElement('ul');
        list.classList.add('warning-list', 'hidden');
        const listId = `experiment-warning-list-${warningListCounter++}`;
        list.id = listId;

        experiment.warnings.forEach((warning) => {
          const warningItem = document.createElement('li');
          warningItem.textContent = warning;
          list.appendChild(warningItem);
        });

        badge.setAttribute('aria-controls', listId);
        badge.setAttribute('aria-expanded', 'false');
        badge.appendChild(icon);
        badge.appendChild(count);

        badge.addEventListener('click', () => {
          const isHidden = list.classList.toggle('hidden');
          badge.setAttribute('aria-expanded', String(!isHidden));
        });

        warningsContainer.appendChild(badge);
        warningsContainer.appendChild(list);
      }
    }

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

  clearWarningBanner();
  setStatus('Importing SQL experiment...');
  try {
    const payload = {
      name,
      sql: sqlInput.value,
      dialect: dialectSelect.value,
    };
    // Only include target_warehouse if a value is selected
    if (warehouseSelect.value) {
      payload.target_warehouse = warehouseSelect.value;
    }
    const response = await fetch(`${API_BASE}/experiments/import-sql`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const body = await response.json();
    if (!response.ok) {
      throw new Error((body.detail || []).join(' '));
    }
    sqlInput.value = '';
    renderWarningBanner(body.warnings ?? []);
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

const resetExperiment = async (name) => {
  if (!confirm(`Reset experiment "${name}"? This will truncate all tables but keep the schema.`)) {
    return;
  }
  setStatus(`Resetting ${name}...`);
  try {
    const response = await fetch(`${API_BASE}/experiments/${encodeURIComponent(name)}/reset`, {
      method: 'POST',
    });
    if (!response.ok) {
      const errorBody = await response.json();
      throw new Error((errorBody.detail || []).join(' '));
    }
    const result = await response.json();
    setStatus(`Experiment ${name} reset (${result.reset_tables} table(s) truncated).`, 'success');
    await fetchExperiments();
  } catch (error) {
    console.error(error);
    setStatus(error.message || 'Failed to reset experiment', 'error');
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

  const experimentName = currentExperiment;

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
  setStatus(`Generating data for ${experimentName}...`);

  try {
    const response = await fetch(`${API_BASE}/experiments/${encodeURIComponent(experimentName)}/generate`, {
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

// Query Interface
const queryForm = document.getElementById('query-form');
const queryInput = document.getElementById('query-input');
const experimentSelect = document.getElementById('experiment-select');
const queryStatus = document.getElementById('query-status');
const queryResultsContainer = document.getElementById('query-results-container');
const queryResults = document.getElementById('query-results');
const rowCountDisplay = document.getElementById('row-count-display');
const clearQueryBtn = document.getElementById('clear-query-btn');
const saveQueryBtn = document.getElementById('save-query-btn');
const exportCsvBtn = document.getElementById('export-csv-btn');

let lastQueryResult = null;

const populateExperimentSelector = (experiments) => {
  // Keep the "None" option
  const noneOption = experimentSelect.querySelector('option[value=""]');
  experimentSelect.innerHTML = '';
  if (noneOption) {
    experimentSelect.appendChild(noneOption);
  }

  // Add experiment options
  experiments.forEach((experiment) => {
    const option = document.createElement('option');
    option.value = experiment.name;
    option.textContent = experiment.name;
    experimentSelect.appendChild(option);
  });
};

const setQueryStatus = (message, type = 'info') => {
  if (!queryStatus) return;
  queryStatus.textContent = message;
  queryStatus.className = `query-status ${type}`;
  queryStatus.classList.remove('hidden');
};

const clearQueryStatus = () => {
  if (!queryStatus) return;
  queryStatus.textContent = '';
  queryStatus.className = 'query-status';
  queryStatus.classList.add('hidden');
};

const executeQuery = async (event) => {
  event.preventDefault();

  const sql = queryInput.value.trim();
  if (!sql) {
    setQueryStatus('Please enter a SQL query.', 'error');
    return;
  }

  // Hide previous results
  queryResultsContainer.classList.add('hidden');
  lastQueryResult = null;

  setQueryStatus('Executing query...');

  try {
    const experimentName = experimentSelect.value || null;
    const payload = { sql, format: 'json' };
    if (experimentName) {
      payload.experiment_name = experimentName;
    }

    const response = await fetch(`${API_BASE}/query/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const errorBody = await response.json();
      throw new Error((errorBody.detail || []).join(' '));
    }

    const result = await response.json();
    lastQueryResult = result;
    renderQueryResults(result);
    setQueryStatus(`Query executed successfully. ${result.row_count} row(s) returned.`, 'success');
  } catch (error) {
    console.error(error);
    setQueryStatus(error.message || 'Failed to execute query', 'error');
  }
};

const renderQueryResults = (result) => {
  if (!result || !result.columns || !result.rows) {
    queryResults.innerHTML = '<p>No results to display.</p>';
    return;
  }

  // Update row count display
  rowCountDisplay.textContent = `${result.row_count} row(s)`;

  // Create table
  const table = document.createElement('table');
  table.className = 'results-table';

  // Create header
  const thead = document.createElement('thead');
  const headerRow = document.createElement('tr');
  result.columns.forEach((column) => {
    const th = document.createElement('th');
    th.textContent = column;
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);
  table.appendChild(thead);

  // Create body
  const tbody = document.createElement('tbody');
  result.rows.forEach((row) => {
    const tr = document.createElement('tr');
    row.forEach((cell) => {
      const td = document.createElement('td');
      td.textContent = cell === null ? 'NULL' : cell;
      if (cell === null) {
        td.className = 'null-value';
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);

  queryResults.innerHTML = '';
  queryResults.appendChild(table);
  queryResultsContainer.classList.remove('hidden');
};

const clearQuery = () => {
  queryInput.value = '';
  queryResultsContainer.classList.add('hidden');
  lastQueryResult = null;
  clearQueryStatus();
};

const saveQuery = () => {
  const sql = queryInput.value.trim();
  if (!sql) {
    setQueryStatus('No query to save.', 'error');
    return;
  }

  // Create a Blob with the SQL content
  const blob = new Blob([sql], { type: 'text/plain' });
  const url = URL.createObjectURL(blob);

  // Create a temporary download link
  const a = document.createElement('a');
  a.href = url;
  a.download = `query_${new Date().toISOString().replace(/[:.]/g, '-')}.sql`;
  document.body.appendChild(a);
  a.click();

  // Clean up
  document.body.removeChild(a);
  URL.revokeObjectURL(url);

  setQueryStatus('Query saved successfully.', 'success');
};

const exportCsv = async () => {
  if (!lastQueryResult) {
    setQueryStatus('No query results to export.', 'error');
    return;
  }

  const sql = queryInput.value.trim();
  setQueryStatus('Exporting to CSV...');

  try {
    const experimentName = experimentSelect.value || null;
    const payload = { sql, format: 'csv' };
    if (experimentName) {
      payload.experiment_name = experimentName;
    }

    const response = await fetch(`${API_BASE}/query/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const errorBody = await response.json();
      throw new Error((errorBody.detail || []).join(' '));
    }

    // Get the CSV content as blob
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);

    // Create a temporary download link
    const a = document.createElement('a');
    a.href = url;
    a.download = `query_results_${new Date().toISOString().replace(/[:.]/g, '-')}.csv`;
    document.body.appendChild(a);
    a.click();

    // Clean up
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    setQueryStatus('Results exported to CSV successfully.', 'success');
  } catch (error) {
    console.error(error);
    setQueryStatus(error.message || 'Failed to export CSV', 'error');
  }
};

// Event listeners for query interface
queryForm.addEventListener('submit', executeQuery);
clearQueryBtn.addEventListener('click', clearQuery);
saveQueryBtn.addEventListener('click', saveQuery);
exportCsvBtn.addEventListener('click', exportCsv);

switchMode('json');
fetchExperiments();
