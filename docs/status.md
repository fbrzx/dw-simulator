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

1. **Schema extensions for composite key metadata (pending):**
   - Extend `TableSchema` with optional `composite_keys: list[list[str]]` field to track original composite PK column names
   - Add `TableSchema.warnings: list[str]` field to store user-facing guidance messages
   - Update Pydantic validation and serialization
   - Add unit tests for schema extensions
   - **Deliverable:** Updated `schema.py` with composite key metadata support, tests passing

2. **SQL importer updates to accept composite keys (pending):**
   - Remove `SqlImportError` when `len(pk_columns) > 1` in `sql_importer.py:52-56`
   - When composite PK detected:
     - Generate a surrogate column named `_row_id` (INT, `is_unique=True`, prepend to column list)
     - Store original composite key column names in `composite_keys` metadata
     - Add warning: `"Table '{table_name}' has composite primary key ({cols}). A surrogate '_row_id' column was added for uniqueness."`
   - Update `test_sql_importer.py`: change `test_import_sql_rejects_composite_primary_key` to verify surrogate key generation
   - Add tests for multiple composite key scenarios (2-column, 3-column, multiple tables)
   - **Deliverable:** SQL importer accepts composite keys, generates surrogate columns with warnings, 90%+ test coverage

3. **Generator support for surrogate key columns (pending):**
   - Update `generator.py` to detect columns named `_row_id` with `is_unique=True`
   - Generate sequential unique integers (1, 2, 3, ...) for surrogate key columns
   - Add test: generate data for table with composite PK → verify `_row_id` values are unique and sequential
   - **Deliverable:** Generator handles surrogate keys correctly, tests passing

4. **API enhancements for warning communication (pending):**
   - Extend `POST /api/experiments/import-sql` response to include `warnings: list[str]` field
   - Include warnings in `GET /api/experiments` response for each experiment (add to experiment summary)
   - Update `service.py` to extract and propagate warnings from schema metadata
   - Add API integration tests verifying warning inclusion in responses
   - **Deliverable:** API endpoints communicate composite key warnings, tests passing

5. **UI updates for warning display (pending):**
   - After successful SQL import, display warnings in an alert/info box above the status message
   - In experiment list, add a warning icon/badge for experiments with warnings
   - On hover/click, show warning details in a tooltip or expanded section
   - **Deliverable:** UI clearly communicates composite key handling to users

6. **Documentation & examples (pending):**
   - Update `docs/tech-spec.md` to document surrogate key approach for composite PKs
   - Add example SQL with composite key to `README.md` and `services/dw-simulator/README.md`
   - Document `_row_id` column behavior in user-facing docs
   - **Deliverable:** Comprehensive documentation of composite key feature

**Current Step:** Ready to begin Step 1 (Schema extensions)

## Recent Work
- **SQL import & dialect support:** sqlglot-backed parser, CLI command `dw-sim experiment import-sql`, REST endpoint `POST /api/experiments/import-sql`, and UI toggle for JSON vs SQL creation.
- **UI enhancements:** The control panel now lists experiments, supports JSON schemas, SQL imports (Redshift/Snowflake), and data generation.
- **Testing:** `cd services/dw-simulator && PYTHONPATH=src pytest` (53 tests, ~90% coverage). Key suites include `tests/test_sql_importer.py`, `tests/test_cli.py`, `tests/test_api.py`, and `tests/test_generator.py`.

## Backlog
**US 2.2** – Reset experiments. Truncate all tables without deleting schema, with guards against resetting during active generation.
**US 3.1-3.3** – SQL Query Interface & Export. Execute SQL queries, export results to CSV, save query scripts.
