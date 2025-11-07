# Web UI

A lightweight, dependency-free control panel to manage DW Simulator experiments.

## Features

- List experiments exposed by the Python API (`GET /api/experiments`).
- Create experiments by pasting JSON schemas and sending them to the API.
- Import Redshift/Snowflake SQL DDL (via `/api/experiments/import-sql`) by
  pasting scripts and choosing a dialect.
- Delete experiments and drop their corresponding tables.

## Running locally

1. Start the Python API so the UI has something to call:
   ```bash
   cd services/dw-simulator
   uvicorn dw_simulator.api:app --reload --port 8000
   ```
2. Serve the static UI (any local server works):
   ```bash
   cd services/web-ui
   python -m http.server 4173
   ```
3. Open [http://localhost:4173](http://localhost:4173) and interact with the UI.

Docker Compose already includes an `nginx` powered `web-ui` service that serves
this folder and proxies to the Python API.
