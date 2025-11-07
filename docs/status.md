# Implementation Status

## Active User Story
**US 2.1** – As a user, I want to fill a selected experiment with synthetic data (target row count per table) so I have realistic volumes for testing.

## Implementation Plan (5 steps)
*SQL import & dialect support (✅ complete): Added sqlglot-based DDL ingestion, CLI/API endpoints, and UI toggle so experiments can originate from Redshift/Snowflake scripts.*
1. **Generation design (✅ complete):** Defined the Faker/Parquet pipeline and orchestration touchpoints in `docs/tech-spec.md`.
2. **Generation engine implementation (✅ complete):** Added `dw_simulator.generator`, service wiring, CLI/API commands, and comprehensive tests (generator, service, CLI, API). Verified via `cd services/dw-simulator && PYTHONPATH=src pytest` (41 tests, 90.86% coverage).
3. **Orchestration & persistence wiring:** Extend `ExperimentService` (and CLI/API) with a `generate` action that triggers the engine, records metadata (e.g., staging paths), and reports progress/errors. Add CLI command + API endpoint.
4. **Integration & coverage:** Add end-to-end tests (service/CLI/API) verifying row counts, uniqueness, and failure handling. Maintain ≥90% coverage via `cd services/dw-simulator && PYTHONPATH=src pytest`.
5. **Docs & UI updates:** Document the generation workflow (CLI/API/UI) in README + web UI (add “Generate” action/button), then update `docs/status.md` once tests pass and the UI surfaces the new feature.

## Current Step
Step 2/5 complete. Next up: Step 3/5 – expose run tracking + metadata persistence for generation jobs.
