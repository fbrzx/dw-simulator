# DW Simulator

Local-first synthetic data warehouse simulator. Refer to `docs/` for specs.

## Testing & linting

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
PYTHONPATH=src pytest
```

## Runtime (Docker-only)

```bash
docker compose build synthetic-data-generator
docker compose up synthetic-data-generator
```

`docker-compose.yml` provisions the full local stack described in `docs/tech-spec.md`
with containers for:

- `synthetic-data-generator` (this package)
- `local-redshift-mock` (containerized PostgreSQL)
- `local-snowflake-emulator` (LocalStack Snowflake/S3 features)
- `local-s3-staging` (LocalStack S3 bucket for staged Parquet files)

### Experiment management CLI

From the repo root (with `.venv` activated or inside the container):

#### Create experiments

```bash
# From JSON schema
dw-sim experiment create path/to/schema.json

# From SQL DDL (Redshift/Snowflake)
dw-sim experiment import-sql schema.sql --name my_experiment --dialect redshift
```

The command reads the schema, validates it via Pydantic models, and persists it
using the SQLite-backed persistence layer.

#### Generate synthetic data

```bash
# Generate using default target_rows from schema
dw-sim experiment generate my_experiment

# Override row counts for specific tables
dw-sim experiment generate my_experiment --rows customers=50000 --rows orders=200000

# Use a seed for deterministic generation
dw-sim experiment generate my_experiment --seed 123

# Specify custom output directory for Parquet files
dw-sim experiment generate my_experiment --output-dir /tmp/my-data
```

**What it does:**
- Generates synthetic data according to column constraints (data types, uniqueness, ranges, etc.)
- Outputs compressed Parquet files to `<output-dir>/<table_name>/*.parquet`
- Validates uniqueness constraints across batches
- Enforces date ranges, numeric ranges, and VARCHAR length limits
- Supports Faker rules for realistic data generation (e.g., `first_name`, `email`)

**Generated data guarantees:**
- Exact row counts match target volumes (US 2.1 AC 1)
- Unique columns contain zero duplicates (US 2.1 AC 2)
- Date values fall within specified ranges (US 2.1 AC 3)

#### Delete experiments

```bash
dw-sim experiment delete my_experiment
```

Removes metadata and drops all physical tables for the experiment.

### FastAPI control plane

The service exposes HTTP endpoints (default port `8000`) for the Web UI / REST flows:

#### Endpoints

**List experiments**
```
GET /api/experiments
```
Returns all experiments with metadata (name, table count, creation timestamp).

**Create experiment**
```
POST /api/experiments
Content-Type: application/json

{
  "name": "my_experiment",
  "tables": [
    {
      "name": "customers",
      "target_rows": 1000,
      "columns": [
        {"name": "id", "data_type": "INT", "is_unique": true},
        {"name": "email", "data_type": "VARCHAR", "varchar_length": 100}
      ]
    }
  ]
}
```

**Generate synthetic data**
```
POST /api/experiments/{name}/generate
Content-Type: application/json

{
  "rows": {"customers": 5000, "orders": 10000},
  "seed": 42,
  "output_dir": "/tmp/output"
}
```
All fields are optional. Returns generation summary with row counts and file paths.

**Delete experiment**
```
DELETE /api/experiments/{name}
```
Removes metadata and drops physical tables.

**Import SQL**
```
POST /api/experiments/import-sql
Content-Type: application/json

{
  "name": "sql_experiment",
  "sql": "CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR(50));",
  "dialect": "redshift",
  "target_rows": 1000
}
```
Parses Redshift/Snowflake DDL and creates an experiment.

**List generation runs**
```
GET /api/experiments/{name}/runs
```
Returns all generation runs for an experiment, most recent first.

**Get specific generation run**
```
GET /api/experiments/{name}/runs/{run_id}
```
Returns detailed metadata for a specific run including status, timestamps, row counts, and errors.

### Generation run tracking

All data generation operations are automatically tracked in the `generation_runs` table:

- Each run records its status (RUNNING, COMPLETED, FAILED, ABORTED)
- Timestamps for start and completion
- Row counts per table (stored as JSON)
- Optional seed for reproducibility
- Full error messages and tracebacks for failures
- Concurrent run guards prevent simultaneous generation for the same experiment

**Access run data via Python API:**
```python
from dw_simulator.service import ExperimentService

service = ExperimentService()

# List all runs for an experiment
runs = service.persistence.list_generation_runs("my_experiment")

# Get a specific run
run = service.persistence.get_generation_run(run_id=1)
print(f"Status: {run.status}, Rows: {run.row_counts}")
```

**Access via REST API:**
- `GET /api/experiments/{name}/runs` - List all runs
- `GET /api/experiments/{name}/runs/{run_id}` - Get specific run

The web UI (services/web-ui) displays runs with real-time polling for live status updates.

#### Running the API

```bash
dw-sim api --host 0.0.0.0 --port 8000
# or
uvicorn dw_simulator.api:app --reload --port 8000
```

Visit `http://localhost:8000/docs` for interactive API documentation (Swagger UI).
