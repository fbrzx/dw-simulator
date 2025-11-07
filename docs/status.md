# Implementation Status

## Recently Completed
**US 2.1** – As a user, I want to fill a selected experiment with synthetic data (target row count per table) so I have realistic volumes for testing. ✅ **COMPLETE**

### Implementation Summary (5 steps - all complete)
*SQL import & dialect support (✅ complete): Added sqlglot-based DDL ingestion, CLI/API endpoints, and UI toggle so experiments can originate from Redshift/Snowflake scripts.*
1. **Generation design (✅ complete):** Defined the Faker/Parquet pipeline and orchestration touchpoints in `docs/tech-spec.md`.
2. **Generation engine implementation (✅ complete):** Added `dw_simulator.generator`, service wiring, CLI/API commands, and comprehensive tests (generator, service, CLI, API). Verified via `cd services/dw-simulator && PYTHONPATH=src pytest` (41 tests, 90.86% coverage).
3. **Orchestration & persistence wiring (✅ complete):** Extended `ExperimentService` with `generate_data` method (service.py:145-172), added CLI command `experiment generate` (cli.py:122-149), and API endpoint `POST /api/experiments/{name}/generate` (api.py:104-124). All US 2.1 acceptance criteria verified: exact target volumes, uniqueness enforcement, and date range constraints. Test suite expanded to 53 tests with 90.22% coverage. Linting passes with zero issues.
4. **Integration & coverage (✅ complete):** Added comprehensive end-to-end integration tests in `tests/test_integration.py` (9 new tests) verifying:
   - US 2.1 AC 1: Exact row counts match target volumes
   - US 2.1 AC 2: Unique columns contain no duplicates
   - US 2.1 AC 3: Date ranges are respected
   - Additional validations: VARCHAR length constraints, numeric ranges, optional columns, row overrides, multi-table generation, and failure handling
   Test suite expanded to 62 tests with 92% coverage (exceeds 90% requirement). All linting checks pass via `ruff check src/ tests/`.
5. **Docs & UI updates (✅ complete):**
   - Enhanced `services/dw-simulator/README.md` with comprehensive CLI and API documentation covering all generation options (row overrides, seed, output directory)
   - Updated `services/web-ui/` with Generate functionality:
     - Added "Generate" button to each experiment in the UI
     - Implemented modal dialog for generation options (row overrides per table, optional seed)
     - Integrated with `POST /api/experiments/{name}/generate` endpoint
     - Shows generation results (total rows, table count)
   - Updated `services/web-ui/README.md` to document Generate feature
   - All 62 tests pass with 92% coverage, linting clean

### Acceptance Criteria Verification
✅ **US 2.1 AC 1**: Generated data matches exact target volumes (verified in tests/test_integration.py::test_integration_exact_row_counts)
✅ **US 2.1 AC 2**: Unique columns contain zero duplicates (verified in tests/test_integration.py::test_integration_uniqueness_enforcement)
✅ **US 2.1 AC 3**: Date values fall within specified ranges (verified in tests/test_integration.py::test_integration_date_range_constraints)

## Next User Story
Ready to begin next story from `docs/product-spec.md` (e.g., US 2.2, US 3.1, or higher-priority features).
