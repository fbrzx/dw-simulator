# Implementation Status

## Active User Story
**US 2.1** – As a user, I want to fill a selected experiment with synthetic data (target row count per table) so I have realistic volumes for testing.

## Implementation Plan (5 steps)
*SQL import & dialect support (✅ complete): Added sqlglot-based DDL ingestion, CLI/API endpoints, and UI toggle so experiments can originate from Redshift/Snowflake scripts.*
1. **Generation design (✅ complete):** Defined the Faker/Parquet pipeline and orchestration touchpoints in `docs/tech-spec.md`.
2. **Generation engine implementation (✅ complete):** Added `dw_simulator.generator`, service wiring, CLI/API commands, and comprehensive tests (generator, service, CLI, API). Verified via `cd services/dw-simulator && PYTHONPATH=src pytest` (41 tests, 90.86% coverage).
3. **Orchestration & persistence wiring (✅ complete):** Extended `ExperimentService` with `generate_data` method (service.py:145-172), added CLI command `experiment generate` (cli.py:122-149), and API endpoint `POST /api/experiments/{name}/generate` (api.py:104-124). All US 2.1 acceptance criteria verified: exact target volumes, uniqueness enforcement, and date range constraints. Test suite expanded to 53 tests with 90.22% coverage. Linting passes with zero issues.
4. **Integration & coverage:** Add end-to-end tests (service/CLI/API) verifying row counts, uniqueness, and failure handling. Maintain ≥90% coverage via `cd services/dw-simulator && PYTHONPATH=src pytest`.
5. **Docs & UI updates:** Document the generation workflow (CLI/API/UI) in README + web UI (add "Generate" action/button), then update `docs/status.md` once tests pass and the UI surfaces the new feature.

## Current Step
Step 3/5 complete. Next up: Step 4/5 – integration testing and ensuring comprehensive end-to-end coverage.
