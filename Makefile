PY_SERVICE_DIR := services/dw-simulator
PY_TEST_CMD := PYTHONPATH=src pytest

.PHONY: install test build up up-detached down api ui clean \
	test-runner-build test-runner-up test-runner-down test-runner-unit \
	test-runner-e2e test-runner-integration test-runner-all test-runner-shell

install:
	cd $(PY_SERVICE_DIR) && python -m pip install -e .[dev]

test:
	cd $(PY_SERVICE_DIR) && $(PY_TEST_CMD)

build:
	docker compose build

up:
	docker compose up

up-detached:
	docker compose up -d

down:
	docker compose down

api:
	cd $(PY_SERVICE_DIR) && dw-sim api --host 0.0.0.0 --port 8000 --reload

ui:
	cd services/web-ui && python -m http.server 4173

clean:
	@echo "Cleaning up warehouse databases..."
	@docker compose exec -T local-redshift-mock psql -U dw_user -d dw_simulator -c "\
		DO \$$\$$ DECLARE r RECORD; \
		BEGIN \
			FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname='public') LOOP \
				EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE'; \
			END LOOP; \
		END \$$\$$;" 2>/dev/null || true
	@echo "Stopping containers and removing volumes..."
	docker compose down -v || true
	@echo "Removing data directory..."
	rm -rf data
	@echo "Removing Python cache and coverage files..."
	rm -rf $(PY_SERVICE_DIR)/.pytest_cache $(PY_SERVICE_DIR)/.coverage
	@echo "✅ Clean complete!"

# =============================================================================
# Test Runner Targets - Run tests in Docker with full infrastructure
# =============================================================================

test-runner-build:
	@echo "Building test-runner Docker image..."
	docker compose build test-runner

test-runner-up:
	@echo "Starting infrastructure services for testing..."
	docker compose up -d local-redshift-mock local-s3-staging local-snowflake-emulator
	@echo "Waiting for services to be ready..."
	@sleep 5
	@docker compose exec -T local-redshift-mock pg_isready -U dw_user -d dw_simulator || \
		(echo "Waiting for PostgreSQL..." && sleep 5 && docker compose exec -T local-redshift-mock pg_isready -U dw_user -d dw_simulator)
	@echo "✅ Infrastructure is ready"

test-runner-down:
	@echo "Stopping infrastructure services..."
	docker compose down -v

test-runner-unit: test-runner-build test-runner-up
	@echo "Running unit tests (non-integration)..."
	docker compose --profile test run --rm \
		-e DW_SIMULATOR_TARGET_DB_URL="sqlite:///:memory:" \
		test-runner \
		bash -c "unset DW_SIMULATOR_REDSHIFT_URL DW_SIMULATOR_SNOWFLAKE_URL && pytest -v --tb=short -o addopts='' -p no:cov -m 'not integration' tests/"

test-runner-e2e: test-runner-build test-runner-up
	@echo "Running E2E tests for all warehouses..."
	@echo "Cleaning up test databases and tables..."
	@rm -f data/e2e_*.db
	@docker compose exec -T local-redshift-mock psql -U dw_user -d dw_simulator -c "\
		DO \$$\$$ DECLARE r RECORD; \
		BEGIN \
			FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'e2e_%') LOOP \
				EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE'; \
			END LOOP; \
		END \$$\$$;" 2>/dev/null || true
	@echo "\n=== SQLite E2E Test ==="
	docker compose --profile test run --rm \
		-e DW_SIMULATOR_TARGET_DB_URL="sqlite:////data/e2e_sqlite.db" \
		test-runner \
		pytest -v --tb=short -o addopts="" -p no:cov \
		tests/test_e2e_warehouses.py::test_e2e_sqlite_ecommerce_workflow
	@echo "\n=== Redshift (PostgreSQL) E2E Test ==="
	docker compose --profile test run --rm \
		-e DW_SIMULATOR_TARGET_DB_URL="sqlite:///:memory:" \
		-e DW_SIMULATOR_REDSHIFT_URL="postgresql://dw_user:dw_pass@local-redshift-mock:5432/dw_simulator" \
		test-runner \
		pytest -v --tb=short -o addopts="" -p no:cov \
		-m integration \
		tests/test_e2e_warehouses.py::test_e2e_redshift_analytics_workflow
	@echo "\n=== Snowflake E2E Test ==="
	@echo "⚠️  Note: Snowflake emulator requires LOCALSTACK_AUTH_TOKEN (LocalStack Pro license)"
	@if [ -z "$$LOCALSTACK_AUTH_TOKEN" ]; then \
		echo "⚠️  Skipping Snowflake test - set LOCALSTACK_AUTH_TOKEN to enable"; \
	else \
		docker compose --profile test run --rm \
			-e DW_SIMULATOR_TARGET_DB_URL="sqlite:///:memory:" \
			-e DW_SIMULATOR_SNOWFLAKE_URL="snowflake://test:test@local-snowflake-emulator:4566/test?account=test&warehouse=test" \
			test-runner \
			pytest -v --tb=short -o addopts="" -p no:cov \
			-m integration \
			tests/test_e2e_warehouses.py::test_e2e_snowflake_sales_workflow || echo "⚠️  Snowflake test failed (license may be invalid)"; \
	fi

test-runner-integration: test-runner-build test-runner-up
	@echo "Running integration tests..."
	@echo "Cleaning up test databases..."
	@rm -f data/*.db
	docker compose --profile test run --rm \
		-e DW_SIMULATOR_TARGET_DB_URL="sqlite:////data/integration_all.db" \
		test-runner \
		bash -c "unset DW_SIMULATOR_REDSHIFT_URL DW_SIMULATOR_SNOWFLAKE_URL && pytest -v --tb=short -o addopts='' -p no:cov tests/test_integration.py"

test-runner-all: test-runner-build test-runner-up
	@echo "Running all tests (unit + integration + E2E)..."
	@$(MAKE) test-runner-unit
	@$(MAKE) test-runner-integration
	@$(MAKE) test-runner-e2e
	@echo "\n✅ All tests completed!"

test-runner-shell: test-runner-build test-runner-up
	@echo "Opening shell in test-runner container..."
	docker compose --profile test run --rm test-runner /bin/bash
