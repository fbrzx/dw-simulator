# Implementation Status

## Active User Story
**US 2.1** – Enable experiment generation workflows (row targeting, uniqueness/date enforcement, CLI/API/UI triggers). **Status:** In progress.

### Progress
1. **Generation design (✅ complete):** Faker/Parquet batching approach documented in `docs/tech-spec.md`.
2. **Generation engine implementation (✅ complete):** `dw_simulator.generator` plus CLI/API entry points (`dw-sim experiment generate`, `POST /api/experiments/{name}/generate`) with coverage in `tests/test_generator.py`, `tests/test_cli.py`, `tests/test_api.py`, and `tests/test_service.py`.
3. **Orchestration & persistence wiring (✅ complete):** Added `generation_runs` table for run-tracking metadata, concurrent job guards preventing simultaneous runs, and richer error reporting with full tracebacks. New methods in `ExperimentPersistence`: `start_generation_run()`, `complete_generation_run()`, `fail_generation_run()`, `get_generation_run()`, and `list_generation_runs()`. Service layer updated to track all generation attempts with detailed status and error messages.
4. **Integration & coverage (✅ complete):** Comprehensive test suite with 80 tests passing and 90.49% coverage (exceeding ≥90% target). Tests cover persistence layer (generation run lifecycle, concurrent guards, error capture), service layer integration (run tracking, error reporting, guard enforcement), and end-to-end generation flows with failure scenarios.
5. **Docs & UI updates (🚧 next):** Surface generation run status in the web UI, document the workflow in README/service docs, and update this status once complete.

## Recent Work
- **SQL import & dialect support:** sqlglot-backed parser, CLI command `dw-sim experiment import-sql`, REST endpoint `POST /api/experiments/import-sql`, and UI toggle for JSON vs SQL creation.
- **UI enhancements:** The control panel now lists experiments, supports JSON schemas, SQL imports (Redshift/Snowflake), and data generation.
- **Testing:** `cd services/dw-simulator && PYTHONPATH=src pytest` (53 tests, ~90% coverage). Key suites include `tests/test_sql_importer.py`, `tests/test_cli.py`, `tests/test_api.py`, and `tests/test_generator.py`.

## Backlog
**US 1.4** – Composite primary key support/guidance. When importing SQL like `public.ENT_RLEU_CWW_Sent`, the simulator should either represent multi-column uniqueness or generate a surrogate key and clearly explain it in the UI.
