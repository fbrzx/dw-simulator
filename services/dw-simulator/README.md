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

```bash
dw-sim experiment create path/to/schema.json
dw-sim experiment delete my_experiment
dw-sim experiment generate my_experiment --rows customers=50000 --seed 123
dw-sim experiment import-sql schema.sql --name my_experiment --dialect redshift
```

The command reads the JSON schema, validates it via the Pydantic models, and
persists it using the SQLite-backed persistence layer introduced in Step 4 of
`docs/status.md`.

### FastAPI control plane

The service exposes HTTP endpoints (default port `8000`) for the Web UI / REST flows:

- `GET /api/experiments`
- `POST /api/experiments`
- `DELETE /api/experiments/{name}`
- `POST /api/experiments/{name}/generate`
- `POST /api/experiments/import-sql`

Run locally with either command:

```bash
dw-sim api --host 0.0.0.0 --port 8000
# or
uvicorn dw_simulator.api:app --reload --port 8000
```
