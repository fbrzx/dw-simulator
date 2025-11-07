# Implementation Status

## Active User Story
**US 2.1** – Enable experiment generation workflows (row targeting, uniqueness/date enforcement, CLI/API/UI triggers). **Status:** In progress.

### Progress
1. **Generation design (✅ complete):** Faker/Parquet batching approach documented in `docs/tech-spec.md`.
2. **Generation engine implementation (✅ complete):** `dw_simulator.generator` plus CLI/API entry points (`dw-sim experiment generate`, `POST /api/experiments/{name}/generate`) with coverage in `tests/test_generator.py`, `tests/test_cli.py`, `tests/test_api.py`, and `tests/test_service.py`.
3. **Orchestration & persistence wiring (🚧 next):** Add run-tracking metadata (generation_runs table), concurrent job guards, and richer error reporting.
4. **Integration & coverage (pending):** End-to-end tests exercising generation via CLI/API, asserting persisted metadata, and covering failure/abort flows (target ≥90% after new code lands).
5. **Docs & UI updates (pending):** Surface generation run status in the web UI, document the workflow in README/service docs, and update this status once complete.

## Recent Work
- **SQL import & dialect support:** sqlglot-backed parser, CLI command `dw-sim experiment import-sql`, REST endpoint `POST /api/experiments/import-sql`, and UI toggle for JSON vs SQL creation.
- **UI enhancements:** The control panel now lists experiments, supports JSON schemas, SQL imports (Redshift/Snowflake), and data generation.
- **Testing:** `cd services/dw-simulator && PYTHONPATH=src pytest` (53 tests, ~90% coverage). Key suites include `tests/test_sql_importer.py`, `tests/test_cli.py`, `tests/test_api.py`, and `tests/test_generator.py`.

## Backlog
**US 1.4** – Composite primary key support/guidance. When importing SQL like `public.ENT_RLEU_CWW_Sent`, the simulator should either represent multi-column uniqueness or generate a surrogate key and clearly explain it in the UI.
