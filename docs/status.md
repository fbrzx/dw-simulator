# Implementation Status

## Completed User Stories

### US 2.1 – Enable experiment generation workflows (✅ COMPLETE)
Enable experiment generation workflows (row targeting, uniqueness/date enforcement, CLI/API/UI triggers).

**All steps completed:**
1. **Generation design (✅ complete):** Faker/Parquet batching approach documented in `docs/tech-spec.md`.
2. **Generation engine implementation (✅ complete):** `dw_simulator.generator` plus CLI/API entry points (`dw-sim experiment generate`, `POST /api/experiments/{name}/generate`) with coverage in `tests/test_generator.py`, `tests/test_cli.py`, `tests/test_api.py`, and `tests/test_service.py`.
3. **Orchestration & persistence wiring (✅ complete):** Added `generation_runs` table for run-tracking metadata, concurrent job guards preventing simultaneous runs, and richer error reporting with full tracebacks. New methods in `ExperimentPersistence`: `start_generation_run()`, `complete_generation_run()`, `fail_generation_run()`, `get_generation_run()`, and `list_generation_runs()`. Service layer updated to track all generation attempts with detailed status and error messages.
4. **Integration & coverage (✅ complete):** Comprehensive test suite with 80 tests passing, all green. Tests cover persistence layer (generation run lifecycle, concurrent guards, error capture), service layer integration (run tracking, error reporting, guard enforcement), and end-to-end generation flows with failure scenarios.
5. **Docs & UI updates (✅ complete):**
   - **API endpoints:** Added `GET /api/experiments/{name}/runs` and `GET /api/experiments/{name}/runs/{run_id}` for accessing generation run history
   - **Web UI:** Added "View Runs" button per experiment that opens a modal displaying all generation runs with real-time status polling (every 3 seconds), showing status badges (RUNNING/COMPLETED/FAILED/ABORTED), timestamps, duration, row counts, and full error messages
   - **Documentation:** Updated `README.md`, `services/web-ui/README.md`, and `services/dw-simulator/README.md` with comprehensive generation run tracking workflow documentation
   - All documentation includes CLI, API, and UI usage examples

## Active User Story

### US 1.4 – Composite primary key support/guidance (🔄 IN PROGRESS)
When importing SQL with composite primary keys (e.g., `PRIMARY KEY (id1, id2)`), the simulator should generate a surrogate key and clearly explain the approach to users.

**Implementation Plan:**

1. **Schema extensions for composite key metadata (✅ complete):**
   - Extended `TableSchema` with optional `composite_keys: list[list[str]]` field to track original composite PK column names
   - Added `TableSchema.warnings: list[str]` field to store user-facing guidance messages
   - Added Pydantic validation to ensure composite_keys references valid columns and rejects empty groups
   - Added 8 comprehensive unit tests for schema extensions covering valid cases, invalid cases, and backward compatibility
   - **Deliverable:** Updated `schema.py` with composite key metadata support (93% coverage), all 90 tests passing

2. **SQL importer updates to accept composite keys (✅ complete):**
   - Updated importer to prepend surrogate `_row_id` columns for composite primary keys, record original PKs in `composite_keys`, and emit user warnings.
   - Added `_dedupe_preserve_order` helper to maintain declared key order while avoiding duplicates.
   - Expanded `test_sql_importer.py` with scenarios covering 2-column, 3-column, and multi-table composite keys; all ensure surrogate handling and metadata/warning propagation.
   - Introduced `tests/conftest.py` placeholder to allow pytest execution without optional coverage plugins in constrained environments.
   - **Deliverable:** SQL importer accepts composite keys, generates surrogate columns with warnings, targeted importer suite passing (`PYTHONPATH=src pytest -o addopts="" tests/test_sql_importer.py`).

3. **Generator support for surrogate key columns (✅ complete):**
   - Updated `generator.py` to detect columns named `_row_id` with `is_unique=True`
   - Implemented sequential unique integer generation (1, 2, 3, ...) for surrogate key columns starting at 1
   - Added comprehensive tests: `test_generator_surrogate_key_starts_at_one` verifies sequential values 1-100, `test_generator_surrogate_key_multiple_tables` verifies independent sequences per table
   - Removed obsolete `tests/conftest.py` (pytest-cov properly installed)
   - **Deliverable:** Generator handles surrogate keys correctly, all 85 tests passing (88% coverage)

4. **API enhancements for warning communication (✅ complete):**
   - Extended `ExperimentCreateResult` dataclass with `warnings: Sequence[str]` field
   - Added `_extract_warnings_from_schema()` helper method to extract warnings from all tables in a schema
   - Added `get_experiment_warnings()` method to extract warnings from stored experiment metadata
   - Updated `create_experiment_from_sql()` to collect and return warnings in the result
   - Extended `POST /api/experiments/import-sql` response to include `warnings` field
   - Updated `GET /api/experiments` to include `warnings` array for each experiment summary
   - Added 4 comprehensive API integration tests: composite key warnings, single PK (no warnings), list with warnings, list without warnings
   - **Deliverable:** API endpoints communicate composite key warnings, all 61 tests passing (service.py:274-292, api.py:70,187, test_api.py:94-160)

5. **UI updates for warning display (✅ complete):**
   - Added a dismissible-style warning banner above the experiment status element to surface import warnings immediately after SQL ingestion (index.html, main.js, styles.css)
   - Enhanced experiment list cards with a warning badge showing counts, tooltip text, and toggleable details list for composite-key guidance
   - Styled new warning affordances to match the control panel theme and remain accessible (keyboard focusable, aria-expanded states)
   - **Deliverable:** UI clearly communicates composite key handling to users with inline alerts and experiment-level warning disclosure

6. **Documentation & examples (pending):**
   - Update `docs/tech-spec.md` to document surrogate key approach for composite PKs
   - Add example SQL with composite key to `README.md` and `services/dw-simulator/README.md`
   - Document `_row_id` column behavior in user-facing docs
   - **Deliverable:** Comprehensive documentation of composite key feature

**Current Step:** Steps 1-5 complete. Ready to begin Step 6 (Documentation & examples)

## Recent Work
- **SQL import & dialect support:** sqlglot-backed parser, CLI command `dw-sim experiment import-sql`, REST endpoint `POST /api/experiments/import-sql`, and UI toggle for JSON vs SQL creation.
- **UI enhancements:** The control panel now lists experiments, supports JSON schemas, SQL imports (Redshift/Snowflake), and data generation.
- **Testing:** `cd services/dw-simulator && PYTHONPATH=src pytest` (53 tests, ~90% coverage). Key suites include `tests/test_sql_importer.py`, `tests/test_cli.py`, `tests/test_api.py`, and `tests/test_generator.py`.

## Backlog
**US 2.2** – Reset experiments. Truncate all tables without deleting schema, with guards against resetting during active generation.
**US 3.1-3.3** – SQL Query Interface & Export. Execute SQL queries, export results to CSV, save query scripts.
