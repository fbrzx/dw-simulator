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
const createWarningBanner = document.getElementById('create-warning-banner');

// Main tab handling
const mainTabs = document.querySelectorAll('.main-tab');
const tabPanels = document.querySelectorAll('.tab-panel');

const switchMainTab = (tabName) => {
  // Check if trying to switch to query tab when it's disabled
  const queryTab = Array.from(mainTabs).find(tab => tab.dataset.tab === 'query');
  if (tabName === 'query' && queryTab && queryTab.disabled) {
    return; // Don't allow switching to disabled query tab
  }

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
  const baseClass = 'status-message';

  if (!message) {
    statusEl.textContent = '';
    statusEl.className = baseClass;
    return;
  }

  statusEl.textContent = message;
  statusEl.className = `${baseClass} ${type} is-visible`;
};

const renderWarningBanner = (warnings, banner = warningBanner) => {
  if (!banner) return;

  banner.innerHTML = '';

  if (!warnings || warnings.length === 0) {
    banner.classList.add('hidden');
    return;
  }

  banner.classList.remove('hidden');

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

  banner.appendChild(icon);
  banner.appendChild(content);
};

const clearWarningBanner = () => {
  renderWarningBanner([], warningBanner);
  renderWarningBanner([], createWarningBanner);
};

let activeMode = 'json';
let warningListCounter = 0;
let experimentsCache = [];
const schemaCache = new Map();

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

const updateQueryTabState = (experiments) => {
  const queryTab = Array.from(mainTabs).find(tab => tab.dataset.tab === 'query');
  if (!queryTab) return;

  const hasExperiments = experiments && experiments.length > 0;

  if (hasExperiments) {
    // Enable the query tab
    queryTab.disabled = false;
    queryTab.title = '';
  } else {
    // Disable the query tab
    queryTab.disabled = true;
    queryTab.title = 'Create an experiment first to use the SQL Query interface';

    // If currently on query tab, switch to experiments tab
    if (queryTab.classList.contains('active')) {
      switchMainTab('experiments');
    }
  }
};

const cacheExperimentSchemas = (experiments) => {
  schemaCache.clear();
  experiments.forEach((experiment) => {
    if (!experiment.schema) {
      return;
    }
    try {
      const parsed = typeof experiment.schema === 'string'
        ? JSON.parse(experiment.schema)
        : experiment.schema;
      schemaCache.set(experiment.name, parsed);
    } catch (error) {
      console.warn(`Failed to parse schema for experiment "${experiment.name}"`, error);
    }
  });
};

const getTablesForExperiment = (experimentName) => {
  const schema = schemaCache.get(experimentName);
  if (!schema || !Array.isArray(schema.tables)) {
    return [];
  }
  return schema.tables.map((table) => ({
    name: table.name,
    columnCount: Array.isArray(table.columns) ? table.columns.length : 0,
    targetRows: table.target_rows,
    columns: (table.columns || []).map((column) => ({
      name: column.name,
      type: column.data_type ?? column.type ?? 'UNKNOWN',
      required: column.required ?? true,
      isUnique: column.is_unique ?? column.isUnique ?? false,
      foreignKey: column.foreign_key,
    })),
  }));
};

const fetchExperiments = async () => {
  setStatus('Loading experiments…');
  clearWarningBanner();  // Clear warnings when refreshing
  try {
    const response = await fetch(`${API_BASE}/experiments`);
    if (!response.ok) throw new Error('Failed to load experiments');
    const data = await response.json();
    const experiments = data.experiments ?? [];
    experimentsCache = experiments;
    cacheExperimentSchemas(experiments);
    renderExperiments(experiments);
    populateExperimentSelector(experiments);
    updateQueryTabState(experiments);
    const label = experiments.length === 1 ? 'experiment' : 'experiments';
    setStatus(`Loaded ${experiments.length} ${label}.`, 'success');
  } catch (error) {
    console.error(error);
    setStatus(error.message || 'Failed to load experiments.', 'error');
  }
};

const renderExperiments = (experiments) => {
  warningListCounter = 0;
  experimentListEl.innerHTML = '';
  if (!experiments.length) {
    experimentListEl.innerHTML = '<li>No experiments yet.</li>';
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

    if (Array.isArray(experiment.distributions) && experiment.distributions.length > 0) {
      const info = item.querySelector('.experiment-info');
      if (info) {
        const summary = document.createElement('div');
        summary.className = 'distribution-summary';

        const heading = document.createElement('strong');
        heading.textContent = experiment.distributions.length === 1
          ? 'Distribution-configured column'
          : 'Distribution-configured columns';
        summary.appendChild(heading);

        const list = document.createElement('ul');
        experiment.distributions.forEach((distribution) => {
          const itemEl = document.createElement('li');
          const params = Object.entries(distribution.parameters || {})
            .map(([key, value]) => `${key}=${value}`)
            .join(', ');
          itemEl.textContent = `${distribution.table}.${distribution.column}: ${distribution.type}${params ? ` (${params})` : ''}`;
          list.appendChild(itemEl);
        });

        summary.appendChild(list);
        info.appendChild(summary);
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
    setStatus('Invalid JSON payload. Please fix the syntax and try again.', 'error');
    return;
  }

  // Add warehouse selection if specified
  const jsonWarehouseSelect = document.getElementById('json-warehouse-select');
  if (jsonWarehouseSelect && jsonWarehouseSelect.value) {
    payload.target_warehouse = jsonWarehouseSelect.value;
  }

  setStatus('Creating experiment…');
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
    setStatus('Experiment created successfully.', 'success');
    await fetchExperiments();
  } catch (error) {
    console.error(error);
    setStatus(error.message || 'Failed to create experiment.', 'error');
  }
};

const importExperimentFromSql = async (event) => {
  event.preventDefault();
  const name = sqlNameInput.value.trim();
  if (!name) {
    setStatus('Provide an experiment name before importing.', 'error');
    return;
  }
  if (!sqlInput.value.trim()) {
    setStatus('Paste SQL DDL before importing.', 'error');
    return;
  }

  clearWarningBanner();
  setStatus('Importing SQL experiment…');
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
    renderWarningBanner(body.warnings ?? [], createWarningBanner);
    setStatus('Experiment imported successfully.', 'success');
    await fetchExperiments();
  } catch (error) {
    console.error(error);
    setStatus(error.message || 'Failed to import SQL experiment.', 'error');
  }
};

const deleteExperiment = async (name) => {
  if (!confirm(`Delete experiment "${name}"? This removes all data.`)) {
    return;
  }
  setStatus(`Deleting "${name}"…`);
  try {
    const response = await fetch(`${API_BASE}/experiments/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    });
    if (!response.ok) {
      const errorBody = await response.json();
      throw new Error((errorBody.detail || []).join(' '));
    }
    setStatus(`Deleted "${name}".`, 'success');
    await fetchExperiments();
  } catch (error) {
    console.error(error);
    setStatus(error.message || 'Failed to delete experiment.', 'error');
  }
};

const resetExperiment = async (name) => {
  if (!confirm(`Reset experiment "${name}"? This will truncate all tables but keep the schema.`)) {
    return;
  }
  setStatus(`Resetting "${name}"…`);
  try {
    const response = await fetch(`${API_BASE}/experiments/${encodeURIComponent(name)}/reset`, {
      method: 'POST',
    });
    if (!response.ok) {
      const errorBody = await response.json();
      throw new Error((errorBody.detail || []).join(' '));
    }
    const result = await response.json();
    const tablesLabel = result.reset_tables === 1 ? 'table' : 'tables';
    setStatus(`Reset complete (${result.reset_tables} ${tablesLabel} truncated).`, 'success');
    await fetchExperiments();
  } catch (error) {
    console.error(error);
    setStatus(error.message || 'Failed to reset experiment.', 'error');
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
  setStatus('Loading experiment details…');
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
        ${(() => {
          const distributionColumns = (table.columns || []).filter(col => col.distribution);
          if (!distributionColumns.length) {
            return '';
          }
          const list = distributionColumns.map((col) => {
            const params = Object.entries(col.distribution.parameters || {})
              .map(([key, value]) => `${key}=${value}`)
              .join(', ');
            return `<li>${col.name}: ${col.distribution.type}${params ? ` (${params})` : ''}</li>`;
          }).join('');
          return `
            <details class="distribution-details">
              <summary>Distribution-configured column${distributionColumns.length > 1 ? 's' : ''}</summary>
              <ul>${list}</ul>
            </details>
          `;
        })()}
        <input
          type="number"
          id="rows-${table.name}"
          name="${table.name}"
          placeholder="${table.target_rows}"
          min="0"
        />
      </div>
    `).join('');

    seedInput.value = '';
    modal.classList.remove('hidden');
    setStatus('');
  } catch (error) {
    console.error(error);
    setStatus(error.message || 'Failed to open generate modal.', 'error');
  }
};

const closeGenerateModal = () => {
  modal.classList.add('hidden');
  currentExperiment = null;
};

const generateData = async (event) => {
  event.preventDefault();

  if (!currentExperiment) {
    setStatus('Select an experiment before generating data.', 'error');
    return;
  }

  const experimentName = currentExperiment;

  // Collect row overrides from inputs
  const rowInputs = tableOverridesEl.querySelectorAll('input[type="number"]');
  const rows = {};
  rowInputs.forEach(input => {
    if (input.value !== null && input.value.trim() !== '') {
      const value = parseInt(input.value, 10);
      // Allow 0 or positive values (0 means skip generation for that table)
      if (!isNaN(value) && value >= 0) {
        rows[input.name] = value;
      }
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
  setStatus(`Generating data for "${experimentName}"…`);

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
    const tableLabel = result.tables.length === 1 ? 'table' : 'tables';
    setStatus(`Generated ${totalRows.toLocaleString()} rows across ${result.tables.length} ${tableLabel}.`, 'success');
  } catch (error) {
    console.error(error);
    setStatus(error.message || 'Failed to generate data.', 'error');
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

  const formatRowValue = (value) => {
    if (typeof value === 'number') {
      return value.toLocaleString();
    }
    const parsed = Number(value);
    if (!Number.isNaN(parsed)) {
      return parsed.toLocaleString();
    }
    return String(value ?? '0');
  };

  const formatRowCounts = (counts) => {
    if (!counts || typeof counts !== 'object') {
      return '';
    }

    const segments = [];
    Object.entries(counts).forEach(([section, value]) => {
      if (value && typeof value === 'object' && !Array.isArray(value)) {
        const tables = Object.entries(value);
        if (tables.length === 0) {
          return;
        }
        const tableLines = tables
          .map(
            ([tableName, rows]) =>
              `&nbsp;&nbsp;${tableName}: ${formatRowValue(rows)} rows`
          )
          .join('<br>');
        segments.push(`<div><strong>${section}:</strong><br>${tableLines}</div>`);
      } else if (value !== undefined && value !== null) {
        segments.push(
          `<div><strong>${section}:</strong> ${formatRowValue(value)} rows</div>`
        );
      }
    });

    return segments.join('<br>');
  };

  runs.forEach((run) => {
    const item = document.createElement('li');
    item.className = 'run-card';

    // Parse row counts
    let rowCountsDisplay = '';
    try {
      const rowCounts = JSON.parse(run.row_counts || '{}');
      const formattedRowCounts = formatRowCounts(rowCounts);
      if (formattedRowCounts) {
        rowCountsDisplay = `
          <div class="run-tables">
            <strong>Tables:</strong><br>
            ${formattedRowCounts}
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
const dialectInfo = document.getElementById('dialect-info');
const selectedWarehouse = document.getElementById('selected-warehouse');
const selectedDialect = document.getElementById('selected-dialect');
const tableSchemaToggle = document.getElementById('table-schema-toggle');
const tableSchemaDetails = document.getElementById('table-schema-details');
const tableHelper = document.getElementById('table-helper');
const tableHelperList = document.getElementById('table-helper-list');
const tableHelperEmpty = document.getElementById('table-helper-empty');
const tableHelperCount = document.getElementById('table-helper-count');
const tableSuggestions = document.getElementById('table-suggestions');

let lastQueryResult = null;
let currentTableNames = [];
let visibleTableSuggestions = [];
let currentSuggestionIndex = -1;
let suggestionHideTimeoutId = null;
let schemaViewVisible = false;
let lastRenderedTableData = [];

// Warehouse to SQL dialect mapping
const warehouseDialectMap = {
  'sqlite': 'SQLite',
  'redshift': 'PostgreSQL (Redshift-compatible)',
  'snowflake': 'Snowflake SQL'
};

const hideTableSuggestions = () => {
  if (!tableSuggestions) return;
  tableSuggestions.classList.add('hidden');
  tableSuggestions.innerHTML = '';
  visibleTableSuggestions = [];
  currentSuggestionIndex = -1;
};

const resetTableHelper = (message = 'Select an experiment to see available tables.') => {
  if (!tableHelper) return;
  tableHelper.classList.add('hidden');
  if (tableHelperList) {
    tableHelperList.innerHTML = '';
  }
  if (tableHelperEmpty) {
    tableHelperEmpty.textContent = message;
  }
  if (tableHelperCount) {
    tableHelperCount.textContent = '';
  }
  currentTableNames = [];
  hideTableSuggestions();
  lastRenderedTableData = [];
  schemaViewVisible = false;
  if (tableSchemaToggle) {
    tableSchemaToggle.textContent = 'View schema';
    tableSchemaToggle.setAttribute('aria-pressed', 'false');
  }
  if (tableSchemaDetails) {
    tableSchemaDetails.classList.add('hidden');
    tableSchemaDetails.innerHTML = '';
  }
};

const renderTableHelper = (tables, experimentName) => {
  if (!tableHelper || !tableHelperList || !tableHelperEmpty || !tableHelperCount) {
    return;
  }
  tableHelper.classList.remove('hidden');

  if (!tables.length) {
    tableHelperList.innerHTML = '';
    tableHelperEmpty.textContent = experimentName
      ? `No tables found for "${experimentName}".`
      : 'No tables available for this experiment.';
    tableHelperEmpty.classList.remove('hidden');
    tableHelperCount.textContent = '';
    lastRenderedTableData = tables;
    renderTableSchemaDetails();
    return;
  }

  tableHelperEmpty.classList.add('hidden');
  tableHelperCount.textContent = `${tables.length} ${tables.length === 1 ? 'table' : 'tables'}`;

  const fragment = document.createDocumentFragment();
  tables.forEach((table) => {
    const li = document.createElement('li');
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'table-pill';
    button.dataset.tableName = table.name;

    const nameEl = document.createElement('span');
    nameEl.className = 'table-pill-name';
    nameEl.textContent = table.name;

    const metaEl = document.createElement('span');
    metaEl.className = 'table-pill-meta';
    const columnLabel = `${table.columnCount} column${table.columnCount === 1 ? '' : 's'}`;
    const rowsLabel = table.targetRows ? ` · ~${table.targetRows} rows` : '';
    metaEl.textContent = `${columnLabel}${rowsLabel}`;

    button.appendChild(nameEl);
    button.appendChild(metaEl);
    li.appendChild(button);
    fragment.appendChild(li);
  });

  tableHelperList.innerHTML = '';
  tableHelperList.appendChild(fragment);

  lastRenderedTableData = tables;
  renderTableSchemaDetails();
};

const replaceCurrentTokenWith = (replacement) => {
  if (!queryInput) return;
  const selectionStart = queryInput.selectionStart ?? queryInput.value.length;
  const selectionEnd = queryInput.selectionEnd ?? selectionStart;
  let tokenStart = selectionStart;
  let tokenEnd = selectionEnd;

  if (selectionStart === selectionEnd) {
    const before = queryInput.value.slice(0, selectionStart);
    const match = before.match(/([a-zA-Z0-9_"]+)$/);
    if (match) {
      tokenStart = selectionStart - match[1].length;
    }
  }

  const beforeText = queryInput.value.slice(0, tokenStart);
  const afterText = queryInput.value.slice(tokenEnd);
  let textToInsert = replacement;
  if (selectionStart === selectionEnd && tokenStart === selectionStart) {
    const needsSpace = beforeText && !/\s$/.test(beforeText);
    if (needsSpace) {
      textToInsert = ` ${textToInsert}`;
    }
  }

  const newValue = `${beforeText}${textToInsert}${afterText}`;
  queryInput.value = newValue;
  const newCaret = beforeText.length + textToInsert.length;
  requestAnimationFrame(() => {
    queryInput.focus();
    queryInput.setSelectionRange(newCaret, newCaret);
  });
};

const insertTableNameIntoQuery = (tableName) => {
  replaceCurrentTokenWith(tableName);
  hideTableSuggestions();
};

const getCurrentToken = () => {
  if (!queryInput) return '';
  const cursor = queryInput.selectionStart ?? 0;
  const before = queryInput.value.slice(0, cursor);
  const match = before.match(/([a-zA-Z0-9_"]+)$/);
  return match ? match[1] : '';
};

const setSuggestionActive = (index) => {
  if (!tableSuggestions) return;
  const buttons = tableSuggestions.querySelectorAll('button');
  buttons.forEach((button, idx) => {
    button.classList.toggle('active', idx === index);
  });
  currentSuggestionIndex = index;
};

const showTableSuggestionsForNames = (names) => {
  if (!tableSuggestions) return;
  if (!names.length) {
    hideTableSuggestions();
    return;
  }
  tableSuggestions.innerHTML = '';
  const fragment = document.createDocumentFragment();
  names.forEach((name, index) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.dataset.tableName = name;
    button.textContent = name;
    if (index === 0) {
      button.classList.add('active');
    }
    fragment.appendChild(button);
  });
  tableSuggestions.appendChild(fragment);
  tableSuggestions.classList.remove('hidden');
  visibleTableSuggestions = names;
  setSuggestionActive(0);
};

const updateTableSuggestions = () => {
  if (!currentTableNames.length) {
    hideTableSuggestions();
    return;
  }
  const token = getCurrentToken();
  if (!token) {
    hideTableSuggestions();
    return;
  }
  const normalized = token.replace(/"/g, '').toLowerCase();
  if (!normalized) {
    hideTableSuggestions();
    return;
  }
  const matches = currentTableNames.filter((name) => name.toLowerCase().startsWith(normalized)).slice(0, 6);
  if (!matches.length) {
    hideTableSuggestions();
    return;
  }
  showTableSuggestionsForNames(matches);
};

const showAllTableSuggestions = () => {
  if (!currentTableNames.length) {
    hideTableSuggestions();
    return;
  }
  showTableSuggestionsForNames(currentTableNames.slice(0, 6));
};

const applySuggestionAtIndex = (index) => {
  if (index < 0 || index >= visibleTableSuggestions.length) {
    return;
  }
  const name = visibleTableSuggestions[index];
  if (name) {
    insertTableNameIntoQuery(name);
  }
};

const formatForeignKey = (foreignKey) => {
  if (!foreignKey) return null;
  if (foreignKey.references_table && foreignKey.references_column) {
    return `${foreignKey.references_table}.${foreignKey.references_column}`;
  }
  return null;
};

const renderTableSchemaDetails = () => {
  if (!tableSchemaDetails) return;
  if (!schemaViewVisible) {
    tableSchemaDetails.classList.add('hidden');
    return;
  }

  if (!lastRenderedTableData.length) {
    tableSchemaDetails.classList.remove('hidden');
    tableSchemaDetails.innerHTML = '<p class="table-schema-empty">No schema available.</p>';
    return;
  }

  const fragment = document.createDocumentFragment();
  lastRenderedTableData.forEach((table) => {
    const group = document.createElement('div');
    group.className = 'table-schema-group';

    const heading = document.createElement('div');
    heading.className = 'table-schema-group-heading';
    const headingName = document.createElement('span');
    headingName.textContent = table.name;
    heading.appendChild(headingName);

    const headingMeta = document.createElement('small');
    headingMeta.textContent = `${table.columnCount} columns`;
    heading.appendChild(headingMeta);

    const columnList = document.createElement('ul');
    columnList.className = 'table-column-list';
    (table.columns || []).forEach((column) => {
      const li = document.createElement('li');

      const columnName = document.createElement('span');
      columnName.className = 'table-column-name';
      columnName.textContent = column.name;

      const columnMeta = document.createElement('span');
      columnMeta.className = 'table-column-meta';
      const metaParts = [column.type];
      if (column.isUnique) {
        metaParts.push('unique');
      }
      if (!column.required) {
        metaParts.push('nullable');
      }
      const fkLabel = formatForeignKey(column.foreignKey);
      if (fkLabel) {
        metaParts.push(`FK→${fkLabel}`);
      }
      columnMeta.textContent = metaParts.join(' · ');

      li.appendChild(columnName);
      li.appendChild(columnMeta);
      columnList.appendChild(li);
    });

    group.appendChild(heading);
    group.appendChild(columnList);
    fragment.appendChild(group);
  });

  tableSchemaDetails.innerHTML = '';
  tableSchemaDetails.appendChild(fragment);
  tableSchemaDetails.classList.remove('hidden');
};

const toggleSchemaView = () => {
  schemaViewVisible = !schemaViewVisible;
  if (tableSchemaToggle) {
    tableSchemaToggle.textContent = schemaViewVisible ? 'Hide schema' : 'View schema';
    tableSchemaToggle.setAttribute('aria-pressed', schemaViewVisible.toString());
  }
  renderTableSchemaDetails();
};

const handleExperimentSelectionChange = () => {
  const selectedOption = experimentSelect.options[experimentSelect.selectedIndex];
  if (!selectedOption || !selectedOption.value) {
    dialectInfo.classList.add('hidden');
    resetTableHelper();
    return;
  }

  const warehouse = selectedOption.dataset.warehouse || 'sqlite';
  const dialect = warehouseDialectMap[warehouse] || warehouse.toUpperCase();
  selectedWarehouse.textContent = warehouse.toUpperCase();
  selectedDialect.textContent = dialect;
  dialectInfo.classList.remove('hidden');

  const experimentName = selectedOption.value;
  const tables = getTablesForExperiment(experimentName);
  currentTableNames = tables.map((table) => table.name);
  renderTableHelper(tables, experimentName);
  hideTableSuggestions();
};

const populateExperimentSelector = (experiments) => {
  // Clear and add placeholder
  experimentSelect.innerHTML = '<option value="" disabled selected>Select an experiment...</option>';

  // Add experiment options with warehouse info
  experiments.forEach((experiment) => {
    const option = document.createElement('option');
    option.value = experiment.name;
    option.dataset.warehouse = experiment.warehouse_type || 'sqlite';
    const warehouseLabel = experiment.warehouse_type ? ` (${experiment.warehouse_type})` : '';
    option.textContent = `${experiment.name}${warehouseLabel}`;
    experimentSelect.appendChild(option);
  });

  experimentSelect.value = '';
  resetTableHelper();
  dialectInfo.classList.add('hidden');
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

  const experimentName = experimentSelect.value;
  if (!experimentName) {
    setQueryStatus('Please select an experiment.', 'error');
    return;
  }

  // Hide previous results
  queryResultsContainer.classList.add('hidden');
  lastQueryResult = null;

  setQueryStatus('Executing query...');

  try {
    const payload = {
      sql,
      format: 'json',
      experiment_name: experimentName
    };

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
  hideTableSuggestions();
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
  const experimentName = experimentSelect.value;

  if (!experimentName) {
    setQueryStatus('Please select an experiment.', 'error');
    return;
  }

  setQueryStatus('Exporting to CSV...');

  try {
    const payload = {
      sql,
      format: 'csv',
      experiment_name: experimentName
    };

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

if (tableHelperList) {
  tableHelperList.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-table-name]');
    if (!button) return;
    insertTableNameIntoQuery(button.dataset.tableName);
  });
}

if (tableSchemaToggle) {
  tableSchemaToggle.addEventListener('click', toggleSchemaView);
}

if (tableSuggestions) {
  tableSuggestions.addEventListener('mousedown', (event) => {
    const button = event.target.closest('button[data-table-name]');
    if (!button) return;
    event.preventDefault();
    insertTableNameIntoQuery(button.dataset.tableName);
  });
}

if (queryInput) {
  queryInput.addEventListener('input', () => {
    updateTableSuggestions();
  });

  queryInput.addEventListener('keydown', (event) => {
    if (event.ctrlKey && event.key === ' ') {
      event.preventDefault();
      showAllTableSuggestions();
      return;
    }

    if (!tableSuggestions || tableSuggestions.classList.contains('hidden')) {
      return;
    }

    if (event.key === 'ArrowDown') {
      event.preventDefault();
      if (!visibleTableSuggestions.length) return;
      const nextIndex = (currentSuggestionIndex + 1) % visibleTableSuggestions.length;
      setSuggestionActive(nextIndex);
    } else if (event.key === 'ArrowUp') {
      event.preventDefault();
      if (!visibleTableSuggestions.length) return;
      const prevIndex = (currentSuggestionIndex - 1 + visibleTableSuggestions.length) % visibleTableSuggestions.length;
      setSuggestionActive(prevIndex);
    } else if (event.key === 'Enter' || event.key === 'Tab') {
      if (visibleTableSuggestions.length) {
        event.preventDefault();
        const targetIndex = currentSuggestionIndex === -1 ? 0 : currentSuggestionIndex;
        applySuggestionAtIndex(targetIndex);
      }
    } else if (event.key === 'Escape') {
      hideTableSuggestions();
    }
  });

  queryInput.addEventListener('blur', () => {
    suggestionHideTimeoutId = window.setTimeout(hideTableSuggestions, 150);
  });

  queryInput.addEventListener('focus', () => {
    if (suggestionHideTimeoutId) {
      clearTimeout(suggestionHideTimeoutId);
      suggestionHideTimeoutId = null;
    }
  });
}

if (tableSuggestions) {
  document.addEventListener('click', (event) => {
    if (tableSuggestions.classList.contains('hidden')) return;
    if (event.target === queryInput) return;
    if (tableSuggestions.contains(event.target)) return;
    hideTableSuggestions();
  });
}

experimentSelect.addEventListener('change', handleExperimentSelectionChange);

// Event listeners for query interface
queryForm.addEventListener('submit', executeQuery);
clearQueryBtn.addEventListener('click', clearQuery);
saveQueryBtn.addEventListener('click', saveQuery);
exportCsvBtn.addEventListener('click', exportCsv);

switchMode('json');
fetchExperiments();
