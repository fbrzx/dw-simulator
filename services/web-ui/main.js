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
      <button class="danger" data-name="${experiment.name}">Delete</button>
    `;
    item.querySelector('button').addEventListener('click', () => deleteExperiment(experiment.name));
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

switchMode('json');
fetchExperiments();
