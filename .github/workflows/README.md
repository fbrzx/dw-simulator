# GitHub Actions CI Pipeline

This directory contains the GitHub Actions workflows for the DW Simulator project.

## Workflows

### `ci.yml` - Main CI Pipeline

The main CI pipeline runs on every push to `main` and `claude/**` branches, as well as on pull requests.

#### Jobs

1. **Unit Tests** (`unit-tests`)
   - Runs fast unit tests without Docker infrastructure
   - Executes linting with ruff
   - Runs all non-integration tests
   - Duration: ~2-3 minutes
   - Uses: Python 3.11, pip cache

2. **End-to-End Tests** (`e2e-tests`)
   - Builds and starts full Docker Compose stack
   - **IMPORTANT**: All infrastructure services (PostgreSQL, LocalStack S3, LocalStack Snowflake) are started and verified BEFORE any tests run
   - Infrastructure readiness checks:
     - PostgreSQL health check (up to 90 seconds)
     - PostgreSQL connection verification
     - LocalStack service initialization
     - Full infrastructure status verification
   - Runs comprehensive end-to-end tests **sequentially** for all warehouse dialects:
     - **SQLite**: Simple e-commerce workflow (100 customers, 250 orders)
     - **Redshift (PostgreSQL)**: Analytics workflow (50 users, 200 events)
     - **Snowflake (LocalStack)**: Sales workflow (75 products, 150 sales)
   - Runs existing integration test suite
   - Duration: ~10-15 minutes
   - Uses: Docker Compose, PostgreSQL, LocalStack
   - **Note**: Tests run sequentially to avoid database conflicts; parallel execution is planned for future optimization

3. **Build Validation** (`build-validation`)
   - Validates that all Docker images build successfully
   - Tests basic container startup
   - Duration: ~5-7 minutes
   - Uses: Docker Buildx

4. **CI Status Check** (`ci-status`)
   - Aggregates results from all jobs
   - Required for branch protection rules
   - Provides clear pass/fail status

#### Test Execution Flow

The E2E job follows a strict execution order to ensure reliability:

```
1. Build Docker images
   └─> synthetic-data-generator

2. Start ALL infrastructure services in parallel
   ├─> local-redshift-mock (PostgreSQL)
   ├─> local-s3-staging (LocalStack S3)
   └─> local-snowflake-emulator (LocalStack Snowflake)

3. Wait for infrastructure to be fully ready
   ├─> PostgreSQL health check (up to 90s)
   ├─> PostgreSQL connection test
   ├─> LocalStack initialization (up to 30s)
   └─> Final verification of all services

4. Run tests SEQUENTIALLY (infrastructure is ready)
   ├─> SQLite E2E test
   ├─> Redshift E2E test
   ├─> Snowflake E2E test
   └─> Integration test suite

5. Cleanup
   └─> docker compose down -v
```

**Why infrastructure must be ready before tests:**
- All warehouse emulators must accept connections before test execution
- Database schemas need to be created reliably
- S3 buckets must exist for Parquet staging
- Health checks prevent race conditions and flaky tests

**Why tests run sequentially:**
- All tests use the same PostgreSQL instance
- Redshift and Snowflake tests share LocalStack resources
- Sequential execution prevents table name conflicts
- Future: Use pytest-xdist for parallel execution within isolated test databases

#### Infrastructure Services

The E2E tests use the following services from `docker-compose.yml`:

- **local-redshift-mock**: PostgreSQL 15 (Redshift emulator)
  - Port: 5439 → 5432
  - Database: `dw_simulator`
  - User: `dw_user`
  - Health check enabled

- **local-s3-staging**: LocalStack S3
  - Port: 4572 → 4566
  - Used for Parquet file staging

- **local-snowflake-emulator**: LocalStack Snowflake
  - Port: 4566
  - Account: `test`
  - Database: `test`
  - Warehouse: `test`

- **synthetic-data-generator**: Main application
  - Built from `services/dw-simulator/Dockerfile`
  - Runs pytest inside container for E2E tests

#### Environment Variables

- `LOCALSTACK_AUTH_TOKEN`: Optional, for LocalStack paid features (set as GitHub secret)
- `PYTHONPATH`: Set to `src` for proper module imports
- `DW_SIMULATOR_TARGET_DB_URL`: Metadata database (SQLite)
- `DW_SIMULATOR_REDSHIFT_URL`: Redshift/PostgreSQL warehouse URL
- `DW_SIMULATOR_SNOWFLAKE_URL`: Snowflake warehouse URL

#### Test Markers

Tests use pytest markers to categorize execution:

- `@pytest.mark.integration`: Requires Docker infrastructure (Redshift/Snowflake)
- No marker: Unit tests (can run without Docker)

To run only unit tests locally:
```bash
pytest -m "not integration"
```

To run only integration tests locally:
```bash
pytest -m integration
```

#### Running CI Locally

**Prerequisites:**
- Docker and Docker Compose installed
- Python 3.11
- Make (optional, for convenience)

**Run unit tests:**
```bash
cd services/dw-simulator
pip install -e .[dev]
PYTHONPATH=src pytest -m "not integration" -v
```

**Run E2E tests (requires Docker):**
```bash
# Start infrastructure
docker compose up -d local-redshift-mock local-s3-staging local-snowflake-emulator

# Run SQLite E2E test
docker compose run --rm synthetic-data-generator \
  pytest -v tests/test_e2e_warehouses.py::test_e2e_sqlite_ecommerce_workflow

# Run Redshift E2E test
docker compose run --rm synthetic-data-generator \
  pytest -v tests/test_e2e_warehouses.py::test_e2e_redshift_analytics_workflow

# Run Snowflake E2E test
docker compose run --rm synthetic-data-generator \
  pytest -v tests/test_e2e_warehouses.py::test_e2e_snowflake_sales_workflow

# Cleanup
docker compose down -v
```

#### Debugging Failed CI Runs

If a CI job fails:

1. **Check the job logs** in GitHub Actions
2. **Review service logs** (automatically printed on E2E test failure):
   - PostgreSQL logs
   - LocalStack S3 logs
   - LocalStack Snowflake logs
3. **Run tests locally** using the commands above
4. **Check Docker service health**:
   ```bash
   docker compose ps
   docker compose logs <service-name>
   ```

#### Performance Targets

| Job | Target Duration | Actual Duration |
|-----|----------------|-----------------|
| Unit Tests | < 5 minutes | ~2-3 minutes |
| E2E Tests | < 20 minutes | ~10-15 minutes |
| Build Validation | < 15 minutes | ~5-7 minutes |
| **Total Pipeline** | **< 25 minutes** | **~15-20 minutes** |

#### Coverage Requirements

- Minimum code coverage: 90% (enforced in unit tests)
- All warehouse dialects must have E2E test coverage
- Critical paths (create → generate → query) must be tested end-to-end

#### Future Enhancements

**Performance Optimizations:**
- [ ] Parallel test execution using pytest-xdist (requires isolated test databases)
- [ ] Use GitHub Actions matrix strategy for E2E tests (requires infrastructure duplication)
- [ ] Add caching for Docker layers (reduce build time from ~5 min to ~1 min)
- [ ] Pre-built Docker images on Docker Hub (skip build step in CI)

**Quality & Security:**
- [ ] Add mutation testing (Stryker/Cosmic Ray)
- [ ] Add performance benchmarking (data generation speed, query performance)
- [ ] Add security scanning (Bandit, Safety, Snyk)
- [ ] Add Docker image scanning (Trivy, Grype)
- [ ] Add SAST/DAST scanning for API endpoints

**Reporting & Observability:**
- [ ] Add test result reporting (coverage badges, trend graphs)
- [ ] Add test timing analytics (track CI performance over time)
- [ ] Add failure rate tracking and flaky test detection
- [ ] Add Slack/Discord notifications for CI failures

**Infrastructure:**
- [ ] Add database isolation for parallel E2E tests
- [ ] Add infrastructure health monitoring
- [ ] Add resource usage metrics (CPU, memory, disk)
