# DW Simulator Monorepo

This repository hosts all local-first services that power the synthetic data
warehouse simulator described in `docs/`. Every deployable component lives
under `./services` and is orchestrated via `docker-compose.yml`.

## Service layout

- `services/dw-simulator` – Python synthetic data generator + CLI (runs only as
  the `synthetic-data-generator` Docker Compose service).
- `services/web-ui` – Placeholder for the future UI that lets users manage
  experiments and run queries.
- `services/data-loader` – Placeholder for the batch/ELT worker that moves
  staged Parquet data into the local Redshift/Snowflake mocks.

## Developing the Python service

All runtime interactions happen through Docker Compose:

```bash
docker compose build synthetic-data-generator
docker compose up synthetic-data-generator
```

For TDD and fast feedback you can still run unit tests inside the service
directory:

```bash
cd services/dw-simulator
PYTHONPATH=src pytest
```

Refer to the service-specific README files for more details as each component
is implemented.

## Make targets

```bash
make install   # pip install -e services/dw-simulator
make test      # run PYTHONPATH=src pytest
make build     # docker compose build
make up        # docker compose up
make api       # run the FastAPI server locally (dw-sim api …)
make ui        # serve the static web UI via python -m http.server
```

## Managing experiments

Experiment schemas live in JSON files that follow `docs/product-spec.md`. Use
the CLI (inside the Docker container or via `pip install -e`) to materialize
them:

```bash
dw-sim experiment create path/to/schema.json
dw-sim experiment import-sql schema.sql --name my_experiment --dialect redshift
dw-sim experiment generate customers_experiment --rows customers=50000 --seed 42
```

The command validates the schema, persists metadata to SQLite (or the configured
warehouse URL), and creates the physical tables. Errors are surfaced with the
exact validation message(s).

### Composite primary key support

When importing SQL with composite primary keys, the simulator automatically handles
them by adding a surrogate `_row_id` column and preserving the original key columns:

```sql
-- Input: schema.sql
CREATE TABLE sales (
  region_id INT NOT NULL,
  store_id INT NOT NULL,
  sale_date DATE,
  amount DECIMAL(10,2),
  PRIMARY KEY (region_id, store_id)
);
```

```bash
dw-sim experiment import-sql schema.sql --name sales_exp --dialect redshift
# Output: Warning: Table 'sales' has a composite primary key (region_id, store_id).
#         A surrogate key column '_row_id' has been added for generation.
#         Original columns are preserved.

dw-sim experiment generate sales_exp --rows sales=1000
# Generates data with _row_id (1, 2, 3, ...) plus original composite key columns
```

The `_row_id` column ensures efficient unique data generation while maintaining schema
fidelity. Warnings are displayed in the CLI, API responses, and Web UI to keep you
informed of the transformation.

### Generation runs and tracking

Every data generation operation is tracked as a "generation run" with the following metadata:

- **Status**: RUNNING, COMPLETED, FAILED, or ABORTED
- **Timestamps**: Start time and completion time
- **Row counts**: Actual rows generated per table
- **Seed**: Optional RNG seed for reproducible generation
- **Errors**: Full traceback if the run failed

**Tracking via CLI:**
```bash
# Generation runs are automatically tracked when you run:
dw-sim experiment generate my_experiment --rows table1=10000 --seed 42

# The CLI displays the run summary on completion
```

**Tracking via API:**
```bash
# List all generation runs for an experiment
curl http://localhost:8000/api/experiments/my_experiment/runs

# Get details of a specific run
curl http://localhost:8000/api/experiments/my_experiment/runs/1
```

**Tracking via Web UI:**
1. Open http://localhost:4173
2. Click "View Runs" next to any experiment
3. The modal displays all runs with real-time status updates (polls every 3 seconds)
4. Running jobs show in-progress status; completed jobs show duration and row counts

Generation runs provide full observability into data creation workflows, making
it easy to debug failures, reproduce datasets with seeds, and audit generation
history.

### Resetting experiments

You can reset an experiment to truncate all its tables while keeping the schema intact.
This is useful when you want to clear the data and regenerate it without recreating the
experiment from scratch.

**Reset via CLI:**
```bash
dw-sim experiment reset my_experiment
# Output: Experiment 'my_experiment' reset (truncated 3 tables).
```

**Reset via API:**
```bash
curl -X POST http://localhost:8000/api/experiments/my_experiment/reset
# Response: {"name": "my_experiment", "reset_tables": 3}
```

**Reset via Web UI:**
1. Open http://localhost:4173
2. Click "Reset" next to any experiment
3. Confirm the action in the dialog
4. The experiment's tables are truncated, and row counts return to 0

**Important notes:**
- Reset is **blocked** if a generation run is currently active for the experiment
- The experiment schema and metadata are preserved; only table data is removed
- After reset, you can regenerate data normally

## Local API + Web UI

- The Python service now ships with a FastAPI control plane (default port `8000`)
  exposing `GET/POST/DELETE /api/experiments`, `POST /api/experiments/{name}/generate`,
  `POST /api/experiments/{name}/reset`, and `POST /api/experiments/import-sql`.
- Run it locally via `dw-sim api --host 0.0.0.0 --port 8000` (or `docker compose up
  synthetic-data-generator`).
- `services/web-ui` contains a static UI (served via Docker Compose or
  `python -m http.server 4173`) that lets you list/create/delete/reset experiments,
  paste JSON schemas, or import SQL DDL for Redshift/Snowflake.
