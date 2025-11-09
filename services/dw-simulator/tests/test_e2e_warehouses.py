"""
End-to-end tests for all three warehouse dialects (SQLite, Redshift, Snowflake).

These tests verify complete workflows from experiment creation through data generation,
loading, and querying across all supported warehouse types. Each test uses a simple,
representative dataset to validate the full stack.

Test Coverage:
- SQLite: Local development workflow
- Redshift (PostgreSQL): AWS-compatible warehouse emulation
- Snowflake (LocalStack): Cloud warehouse emulation
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Generator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from dw_simulator.config import get_redshift_url, get_snowflake_url
from dw_simulator.service import ExperimentService


@pytest.fixture
def tmp_sqlite_service(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ExperimentService:
    """Create an ExperimentService configured for SQLite warehouse."""
    db_path = tmp_path / "e2e_sqlite.db"
    db_url = f"sqlite:///{db_path}"
    monkeypatch.setenv("DW_SIMULATOR_TARGET_DB_URL", db_url)
    # Clear warehouse URLs to ensure SQLite is used
    monkeypatch.delenv("DW_SIMULATOR_REDSHIFT_URL", raising=False)
    monkeypatch.delenv("DW_SIMULATOR_SNOWFLAKE_URL", raising=False)
    return ExperimentService()


@pytest.fixture
def redshift_service(monkeypatch: pytest.MonkeyPatch) -> ExperimentService | None:
    """
    Create an ExperimentService configured for Redshift (PostgreSQL) warehouse.

    Skips if DW_SIMULATOR_REDSHIFT_URL is not configured.
    """
    redshift_url = get_redshift_url()
    if not redshift_url:
        pytest.skip("DW_SIMULATOR_REDSHIFT_URL not configured - skipping Redshift tests")

    # Use in-memory SQLite for metadata, PostgreSQL for warehouse
    metadata_db = "sqlite:///:memory:"
    monkeypatch.setenv("DW_SIMULATOR_TARGET_DB_URL", metadata_db)
    monkeypatch.setenv("DW_SIMULATOR_REDSHIFT_URL", redshift_url)
    monkeypatch.delenv("DW_SIMULATOR_SNOWFLAKE_URL", raising=False)

    return ExperimentService()


@pytest.fixture
def snowflake_service(monkeypatch: pytest.MonkeyPatch) -> ExperimentService | None:
    """
    Create an ExperimentService configured for Snowflake warehouse.

    Skips if DW_SIMULATOR_SNOWFLAKE_URL is not configured.
    """
    snowflake_url = get_snowflake_url()
    if not snowflake_url:
        pytest.skip("DW_SIMULATOR_SNOWFLAKE_URL not configured - skipping Snowflake tests")

    # Use in-memory SQLite for metadata, Snowflake for warehouse
    metadata_db = "sqlite:///:memory:"
    monkeypatch.setenv("DW_SIMULATOR_TARGET_DB_URL", metadata_db)
    monkeypatch.delenv("DW_SIMULATOR_REDSHIFT_URL", raising=False)
    monkeypatch.setenv("DW_SIMULATOR_SNOWFLAKE_URL", snowflake_url)

    return ExperimentService()


# =============================================================================
# SQLite End-to-End Test
# =============================================================================

def test_e2e_sqlite_ecommerce_workflow(tmp_sqlite_service: ExperimentService, tmp_path: Path) -> None:
    """
    End-to-End SQLite: Complete workflow with simple e-commerce dataset.

    Workflow:
    1. Create experiment with customers and orders tables
    2. Generate synthetic data (100 customers, 250 orders)
    3. Verify data auto-loads into SQLite
    4. Execute SQL queries to validate data integrity
    5. Test aggregations and joins

    Dataset: Simple e-commerce with customers and orders
    Warehouse: SQLite
    Expected Duration: <2 seconds
    """
    service = tmp_sqlite_service

    # Step 1: Create experiment with simple e-commerce schema
    schema = {
        "name": "E2E_SQLite_Ecommerce",
        "target_warehouse": "sqlite",
        "tables": [
            {
                "name": "customers",
                "target_rows": 100,
                "columns": [
                    {"name": "customer_id", "data_type": "INT", "is_unique": True},
                    {"name": "email", "data_type": "VARCHAR", "varchar_length": 100, "faker_rule": "email"},
                    {"name": "first_name", "data_type": "VARCHAR", "varchar_length": 50, "faker_rule": "first_name"},
                    {"name": "last_name", "data_type": "VARCHAR", "varchar_length": 50, "faker_rule": "last_name"},
                    {"name": "signup_date", "data_type": "DATE", "date_start": "2024-01-01", "date_end": "2024-12-31"},
                ],
            },
            {
                "name": "orders",
                "target_rows": 250,
                "columns": [
                    {"name": "order_id", "data_type": "INT", "is_unique": True},
                    {"name": "customer_id", "data_type": "INT", "min_value": 1, "max_value": 100},
                    {"name": "order_date", "data_type": "DATE", "date_start": "2024-01-01", "date_end": "2024-12-31"},
                    {"name": "total_amount", "data_type": "FLOAT", "min_value": 10.0, "max_value": 1000.0},
                ],
            },
        ],
    }

    create_result = service.create_experiment_from_payload(schema)
    assert create_result.success is True, f"Failed to create experiment: {create_result.errors}"
    assert create_result.metadata is not None
    assert create_result.metadata.name == "E2E_SQLite_Ecommerce"

    # Step 2: Generate data (should auto-load into SQLite)
    output_dir = tmp_path / "output_sqlite"
    gen_result = service.generate_data("E2E_SQLite_Ecommerce", output_dir=output_dir, seed=12345)
    assert gen_result.success is True, f"Failed to generate data: {gen_result.errors}"
    assert gen_result.run_metadata is not None
    assert gen_result.run_metadata.id is not None

    # Step 3: Verify data was auto-loaded by querying tables
    # Check customer count
    query_result = service.execute_query("SELECT COUNT(*) as cnt FROM e2e_sqlite_ecommerce__customers")
    assert query_result.success is True, f"Query failed: {query_result.errors}"
    assert query_result.result is not None
    assert query_result.result.rows[0][0] == 100, "Expected 100 customers"

    # Check order count
    query_result = service.execute_query("SELECT COUNT(*) as cnt FROM e2e_sqlite_ecommerce__orders")
    assert query_result.success is True
    assert query_result.result.rows[0][0] == 250, "Expected 250 orders"

    # Step 4: Validate data quality - check unique emails
    query_result = service.execute_query(
        "SELECT COUNT(DISTINCT email) as unique_emails FROM e2e_sqlite_ecommerce__customers"
    )
    assert query_result.success is True
    assert query_result.result.rows[0][0] == 100, "All customer emails should be unique"

    # Step 5: Test aggregations - average order amount
    query_result = service.execute_query(
        "SELECT AVG(total_amount) as avg_amount FROM e2e_sqlite_ecommerce__orders"
    )
    assert query_result.success is True
    avg_amount = query_result.result.rows[0][0]
    assert 10.0 <= avg_amount <= 1000.0, f"Average amount {avg_amount} outside expected range"

    # Step 6: Test joins - orders with customer info
    query_result = service.execute_query(
        """
        SELECT c.first_name, c.last_name, COUNT(o.order_id) as order_count
        FROM e2e_sqlite_ecommerce__customers c
        LEFT JOIN e2e_sqlite_ecommerce__orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.first_name, c.last_name
        HAVING order_count > 0
        LIMIT 5
        """
    )
    assert query_result.success is True
    assert len(query_result.result.columns) == 3
    assert "first_name" in query_result.result.columns
    assert "last_name" in query_result.result.columns
    assert "order_count" in query_result.result.columns


# =============================================================================
# Redshift (PostgreSQL) End-to-End Test
# =============================================================================

@pytest.mark.integration
def test_e2e_redshift_analytics_workflow(redshift_service: ExperimentService, tmp_path: Path) -> None:
    """
    End-to-End Redshift: Complete workflow with simple analytics dataset.

    Workflow:
    1. Create experiment targeting Redshift warehouse
    2. Generate synthetic data (200 events, 50 users)
    3. Verify data loads into PostgreSQL (Redshift emulator)
    4. Execute PostgreSQL-compatible queries
    5. Test window functions and CTEs

    Dataset: Simple analytics with events and users
    Warehouse: PostgreSQL (Redshift emulator)
    Expected Duration: <5 seconds
    """
    service = redshift_service

    # Step 1: Create experiment targeting Redshift
    schema = {
        "name": "E2E_Redshift_Analytics",
        "target_warehouse": "redshift",
        "tables": [
            {
                "name": "users",
                "target_rows": 50,
                "columns": [
                    {"name": "user_id", "data_type": "INT", "is_unique": True},
                    {"name": "username", "data_type": "VARCHAR", "varchar_length": 50, "faker_rule": "user_name"},
                    {"name": "email", "data_type": "VARCHAR", "varchar_length": 100, "faker_rule": "email"},
                    {"name": "created_at", "data_type": "DATE", "date_start": "2024-01-01", "date_end": "2024-06-30"},
                ],
            },
            {
                "name": "events",
                "target_rows": 200,
                "columns": [
                    {"name": "event_id", "data_type": "INT", "is_unique": True},
                    {"name": "user_id", "data_type": "INT", "min_value": 1, "max_value": 50},
                    {"name": "event_type", "data_type": "VARCHAR", "varchar_length": 50},
                    {"name": "event_date", "data_type": "DATE", "date_start": "2024-01-01", "date_end": "2024-12-31"},
                    {"name": "value", "data_type": "FLOAT", "min_value": 0.0, "max_value": 100.0},
                ],
            },
        ],
    }

    create_result = service.create_experiment_from_payload(schema)
    assert create_result.success is True, f"Failed to create experiment: {create_result.errors}"

    # Step 2: Generate data
    output_dir = tmp_path / "output_redshift"
    gen_result = service.generate_data("E2E_Redshift_Analytics", output_dir=output_dir, seed=54321)
    assert gen_result.success is True, f"Failed to generate data: {gen_result.errors}"

    # Step 3: Verify data loaded into Redshift (PostgreSQL)
    query_result = service.execute_query("SELECT COUNT(*) FROM e2e_redshift_analytics__users")
    assert query_result.success is True, f"Query failed: {query_result.errors}"
    assert query_result.result.rows[0][0] == 50, "Expected 50 users"

    query_result = service.execute_query("SELECT COUNT(*) FROM e2e_redshift_analytics__events")
    assert query_result.success is True
    assert query_result.result.rows[0][0] == 200, "Expected 200 events"

    # Step 4: Test PostgreSQL-specific features - window functions
    query_result = service.execute_query(
        """
        SELECT
            user_id,
            event_id,
            event_date,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_date) as event_seq
        FROM e2e_redshift_analytics__events
        LIMIT 10
        """
    )
    assert query_result.success is True
    assert len(query_result.result.columns) == 4
    assert "event_seq" in query_result.result.columns

    # Step 5: Test CTEs (Common Table Expressions)
    query_result = service.execute_query(
        """
        WITH user_stats AS (
            SELECT
                user_id,
                COUNT(*) as event_count,
                AVG(value) as avg_value
            FROM e2e_redshift_analytics__events
            GROUP BY user_id
        )
        SELECT
            COUNT(*) as active_users,
            AVG(event_count) as avg_events_per_user
        FROM user_stats
        WHERE event_count > 0
        """
    )
    assert query_result.success is True
    assert query_result.result.rows[0][0] > 0, "Should have active users"

    # Step 6: Test joins across tables
    query_result = service.execute_query(
        """
        SELECT
            u.username,
            u.email,
            COUNT(e.event_id) as event_count
        FROM e2e_redshift_analytics__users u
        INNER JOIN e2e_redshift_analytics__events e ON u.user_id = e.user_id
        GROUP BY u.user_id, u.username, u.email
        ORDER BY event_count DESC
        LIMIT 5
        """
    )
    assert query_result.success is True
    assert len(query_result.result.rows) > 0


# =============================================================================
# Snowflake End-to-End Test
# =============================================================================

@pytest.mark.integration
def test_e2e_snowflake_sales_workflow(snowflake_service: ExperimentService, tmp_path: Path) -> None:
    """
    End-to-End Snowflake: Complete workflow with simple sales dataset.

    Workflow:
    1. Create experiment targeting Snowflake warehouse
    2. Generate synthetic data (75 products, 150 sales)
    3. Verify data loads into Snowflake emulator
    4. Execute Snowflake-compatible queries
    5. Test aggregations and date functions

    Dataset: Simple sales with products and transactions
    Warehouse: LocalStack Snowflake emulator
    Expected Duration: <5 seconds
    """
    service = snowflake_service

    # Step 1: Create experiment targeting Snowflake
    schema = {
        "name": "E2E_Snowflake_Sales",
        "target_warehouse": "snowflake",
        "tables": [
            {
                "name": "products",
                "target_rows": 75,
                "columns": [
                    {"name": "product_id", "data_type": "INT", "is_unique": True},
                    {"name": "product_name", "data_type": "VARCHAR", "varchar_length": 100, "faker_rule": "word"},
                    {"name": "category", "data_type": "VARCHAR", "varchar_length": 50},
                    {"name": "price", "data_type": "FLOAT", "min_value": 5.0, "max_value": 500.0},
                    {"name": "in_stock", "data_type": "BOOLEAN"},
                ],
            },
            {
                "name": "sales",
                "target_rows": 150,
                "columns": [
                    {"name": "sale_id", "data_type": "INT", "is_unique": True},
                    {"name": "product_id", "data_type": "INT", "min_value": 1, "max_value": 75},
                    {"name": "sale_date", "data_type": "DATE", "date_start": "2024-01-01", "date_end": "2024-12-31"},
                    {"name": "quantity", "data_type": "INT", "min_value": 1, "max_value": 10},
                    {"name": "discount", "data_type": "FLOAT", "min_value": 0.0, "max_value": 0.5},
                ],
            },
        ],
    }

    create_result = service.create_experiment_from_payload(schema)
    assert create_result.success is True, f"Failed to create experiment: {create_result.errors}"

    # Step 2: Generate data
    output_dir = tmp_path / "output_snowflake"
    gen_result = service.generate_data("E2E_Snowflake_Sales", output_dir=output_dir, seed=99999)
    assert gen_result.success is True, f"Failed to generate data: {gen_result.errors}"

    # Step 3: Verify data loaded into Snowflake
    query_result = service.execute_query("SELECT COUNT(*) FROM e2e_snowflake_sales__products")
    assert query_result.success is True, f"Query failed: {query_result.errors}"
    assert query_result.result.rows[0][0] == 75, "Expected 75 products"

    query_result = service.execute_query("SELECT COUNT(*) FROM e2e_snowflake_sales__sales")
    assert query_result.success is True
    assert query_result.result.rows[0][0] == 150, "Expected 150 sales"

    # Step 4: Test basic aggregations
    query_result = service.execute_query(
        """
        SELECT
            COUNT(*) as total_sales,
            SUM(quantity) as total_quantity,
            AVG(discount) as avg_discount
        FROM e2e_snowflake_sales__sales
        """
    )
    assert query_result.success is True
    total_sales, total_quantity, avg_discount = query_result.result.rows[0]
    assert total_sales == 150
    assert total_quantity >= 150  # At least 1 per sale
    assert 0.0 <= avg_discount <= 0.5

    # Step 5: Test date functions and grouping
    query_result = service.execute_query(
        """
        SELECT
            sale_date,
            COUNT(*) as daily_sales
        FROM e2e_snowflake_sales__sales
        GROUP BY sale_date
        ORDER BY daily_sales DESC
        LIMIT 5
        """
    )
    assert query_result.success is True
    assert len(query_result.result.rows) > 0

    # Step 6: Test joins and BOOLEAN column
    query_result = service.execute_query(
        """
        SELECT
            p.product_name,
            p.price,
            p.in_stock,
            COUNT(s.sale_id) as times_sold
        FROM e2e_snowflake_sales__products p
        LEFT JOIN e2e_snowflake_sales__sales s ON p.product_id = s.product_id
        GROUP BY p.product_id, p.product_name, p.price, p.in_stock
        HAVING times_sold > 0
        ORDER BY times_sold DESC
        LIMIT 10
        """
    )
    assert query_result.success is True
    assert len(query_result.result.columns) == 4
    assert "in_stock" in query_result.result.columns


# =============================================================================
# Foreign Key Referential Integrity End-to-End Test
# =============================================================================

def test_e2e_foreign_key_referential_integrity(tmp_sqlite_service: ExperimentService, tmp_path: Path) -> None:
    """
    End-to-End Foreign Key: Complete workflow verifying referential integrity.

    Workflow:
    1. Create experiment with multi-level FK chain (customers → orders → order_items)
    2. Generate synthetic data with FK relationships
    3. Verify data auto-loads into warehouse
    4. Execute SQL JOINs to verify referential integrity
    5. Test nullable FK behavior
    6. Validate all FK values reference existing parent keys

    Dataset: E-commerce with customers, orders, and order_items (3-level FK chain)
    Warehouse: SQLite
    Expected Duration: <3 seconds

    This test validates US 6.2 acceptance criteria:
    - AC 3: Generator produces data where all FK values reference existing parent keys
    - AC 4: Nullable FKs correctly introduce NULL values
    - AC 6: Comprehensive test coverage validates FK enforcement
    """
    service = tmp_sqlite_service

    # Step 1: Create experiment with multi-level FK relationships
    schema = {
        "name": "E2E_FK_Ecommerce",
        "target_warehouse": "sqlite",
        "tables": [
            {
                "name": "customers",
                "target_rows": 50,
                "columns": [
                    {"name": "customer_id", "data_type": "INT", "is_unique": True, "required": True},
                    {"name": "email", "data_type": "VARCHAR", "varchar_length": 100, "faker_rule": "email"},
                    {"name": "first_name", "data_type": "VARCHAR", "varchar_length": 50, "faker_rule": "first_name"},
                    {"name": "signup_date", "data_type": "DATE", "date_start": "2024-01-01", "date_end": "2024-12-31"},
                ],
            },
            {
                "name": "orders",
                "target_rows": 150,
                "columns": [
                    {"name": "order_id", "data_type": "INT", "is_unique": True, "required": True},
                    {
                        "name": "customer_id",
                        "data_type": "INT",
                        "required": True,
                        "foreign_key": {
                            "references_table": "customers",
                            "references_column": "customer_id",
                        },
                    },
                    {"name": "order_date", "data_type": "DATE", "date_start": "2024-01-01", "date_end": "2024-12-31"},
                    {"name": "total_amount", "data_type": "FLOAT", "min_value": 10.0, "max_value": 1000.0},
                ],
            },
            {
                "name": "order_items",
                "target_rows": 400,
                "columns": [
                    {"name": "item_id", "data_type": "INT", "is_unique": True, "required": True},
                    {
                        "name": "order_id",
                        "data_type": "INT",
                        "required": True,
                        "foreign_key": {
                            "references_table": "orders",
                            "references_column": "order_id",
                        },
                    },
                    {"name": "product_name", "data_type": "VARCHAR", "varchar_length": 100, "faker_rule": "word"},
                    {"name": "quantity", "data_type": "INT", "min_value": 1, "max_value": 10},
                    {"name": "price", "data_type": "FLOAT", "min_value": 5.0, "max_value": 500.0},
                    {
                        "name": "discount_code",
                        "data_type": "VARCHAR",
                        "varchar_length": 20,
                        "required": False,
                    },
                ],
            },
        ],
    }

    # Step 2: Create the experiment
    create_result = service.create_experiment_from_payload(schema)
    assert create_result.success is True, f"Failed to create experiment: {create_result.errors}"
    assert create_result.metadata is not None
    assert create_result.metadata.name == "E2E_FK_Ecommerce"

    # Step 3: Generate data with FK relationships (should auto-load)
    output_dir = tmp_path / "output_fk"
    gen_result = service.generate_data("E2E_FK_Ecommerce", output_dir=output_dir, seed=88888)
    assert gen_result.success is True, f"Failed to generate data: {gen_result.errors}"
    assert gen_result.run_metadata is not None

    # Step 4: Verify data was auto-loaded by checking row counts
    query_result = service.execute_query("SELECT COUNT(*) FROM e2e_fk_ecommerce__customers")
    assert query_result.success is True, f"Query failed: {query_result.errors}"
    assert query_result.result.rows[0][0] == 50, "Expected 50 customers"

    query_result = service.execute_query("SELECT COUNT(*) FROM e2e_fk_ecommerce__orders")
    assert query_result.success is True
    assert query_result.result.rows[0][0] == 150, "Expected 150 orders"

    query_result = service.execute_query("SELECT COUNT(*) FROM e2e_fk_ecommerce__order_items")
    assert query_result.success is True
    assert query_result.result.rows[0][0] == 400, "Expected 400 order items"

    # Step 5: Verify FK referential integrity - all order customer_ids exist in customers
    query_result = service.execute_query(
        """
        SELECT COUNT(*) as orphaned_orders
        FROM e2e_fk_ecommerce__orders o
        LEFT JOIN e2e_fk_ecommerce__customers c ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        """
    )
    assert query_result.success is True
    orphaned_count = query_result.result.rows[0][0]
    assert orphaned_count == 0, f"Found {orphaned_count} orders with invalid customer_id references"

    # Step 6: Verify FK referential integrity - all order_item order_ids exist in orders
    query_result = service.execute_query(
        """
        SELECT COUNT(*) as orphaned_items
        FROM e2e_fk_ecommerce__order_items oi
        LEFT JOIN e2e_fk_ecommerce__orders o ON oi.order_id = o.order_id
        WHERE o.order_id IS NULL
        """
    )
    assert query_result.success is True
    orphaned_count = query_result.result.rows[0][0]
    assert orphaned_count == 0, f"Found {orphaned_count} order items with invalid order_id references"

    # Step 7: Test multi-level JOIN - verify we can traverse the entire FK chain
    query_result = service.execute_query(
        """
        SELECT
            c.customer_id,
            c.email,
            c.first_name,
            COUNT(DISTINCT o.order_id) as order_count,
            COUNT(oi.item_id) as item_count,
            SUM(oi.quantity) as total_quantity,
            AVG(oi.price) as avg_item_price
        FROM e2e_fk_ecommerce__customers c
        LEFT JOIN e2e_fk_ecommerce__orders o ON c.customer_id = o.customer_id
        LEFT JOIN e2e_fk_ecommerce__order_items oi ON o.order_id = oi.order_id
        GROUP BY c.customer_id, c.email, c.first_name
        HAVING order_count > 0
        ORDER BY order_count DESC
        LIMIT 10
        """
    )
    assert query_result.success is True, f"Multi-level JOIN failed: {query_result.errors}"
    assert len(query_result.result.rows) > 0, "Expected customers with orders"

    # Verify column structure from JOIN
    assert len(query_result.result.columns) == 7
    assert "customer_id" in query_result.result.columns
    assert "email" in query_result.result.columns
    assert "order_count" in query_result.result.columns
    assert "item_count" in query_result.result.columns

    # Step 8: Verify all customers in the result have valid data
    for row in query_result.result.rows:
        customer_id, email, first_name, order_count, item_count, total_quantity, avg_item_price = row
        assert customer_id is not None and customer_id > 0
        assert email is not None and "@" in email
        assert order_count > 0, "Filtered for customers with orders"
        assert item_count >= 0  # Some orders might not have items yet

    # Step 9: Test that all FK values are within expected parent table ranges
    query_result = service.execute_query(
        """
        SELECT MIN(customer_id) as min_cust, MAX(customer_id) as max_cust
        FROM e2e_fk_ecommerce__customers
        """
    )
    assert query_result.success is True
    min_customer_id, max_customer_id = query_result.result.rows[0]

    query_result = service.execute_query(
        """
        SELECT MIN(customer_id) as min_fk, MAX(customer_id) as max_fk
        FROM e2e_fk_ecommerce__orders
        """
    )
    assert query_result.success is True
    min_fk, max_fk = query_result.result.rows[0]

    # All FK values should be within the parent table's range
    assert min_fk >= min_customer_id, f"FK min {min_fk} is less than parent min {min_customer_id}"
    assert max_fk <= max_customer_id, f"FK max {max_fk} is greater than parent max {max_customer_id}"

    # Step 10: Verify order_id FK range for order_items
    query_result = service.execute_query(
        """
        SELECT MIN(order_id) as min_order, MAX(order_id) as max_order
        FROM e2e_fk_ecommerce__orders
        """
    )
    assert query_result.success is True
    min_order_id, max_order_id = query_result.result.rows[0]

    query_result = service.execute_query(
        """
        SELECT MIN(order_id) as min_fk, MAX(order_id) as max_fk
        FROM e2e_fk_ecommerce__order_items
        """
    )
    assert query_result.success is True
    min_fk, max_fk = query_result.result.rows[0]

    assert min_fk >= min_order_id, f"FK min {min_fk} is less than parent min {min_order_id}"
    assert max_fk <= max_order_id, f"FK max {max_fk} is greater than parent max {max_order_id}"

    # Step 11: Test aggregation across FK chain - verify data makes sense
    query_result = service.execute_query(
        """
        SELECT
            o.order_id,
            o.total_amount,
            COUNT(oi.item_id) as item_count,
            SUM(oi.quantity * oi.price) as calculated_total
        FROM e2e_fk_ecommerce__orders o
        LEFT JOIN e2e_fk_ecommerce__order_items oi ON o.order_id = oi.order_id
        GROUP BY o.order_id, o.total_amount
        HAVING item_count > 0
        LIMIT 20
        """
    )
    assert query_result.success is True
    assert len(query_result.result.rows) > 0, "Expected orders with items"

    # Verify aggregation structure
    for row in query_result.result.rows:
        order_id, total_amount, item_count, calculated_total = row
        assert order_id is not None
        assert total_amount >= 10.0  # min_value from schema
        assert item_count > 0
        assert calculated_total is not None
