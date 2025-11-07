"""
Integration tests verifying end-to-end data generation with actual Parquet file validation.

These tests validate US 2.1 acceptance criteria:
- AC 1: Generated row counts match exact target volumes
- AC 2: Unique columns contain unique values (no duplicates)
- AC 3: Date columns respect specified ranges

Note: These tests verify the generated Parquet files directly, as data loading
into the warehouse is a future enhancement (see tech-spec.md).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Generator

import pandas as pd
import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine

from dw_simulator.service import ExperimentService


@pytest.fixture
def db_engine(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[Engine, None, None]:
    """Create a temporary SQLite database for integration tests."""
    db_path = tmp_path / "integration.db"
    db_url = f"sqlite:///{db_path}"
    monkeypatch.setenv("DW_SIMULATOR_TARGET_DB_URL", db_url)
    engine = create_engine(db_url)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def service(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ExperimentService:
    """Create an ExperimentService with a temporary database."""
    db_path = tmp_path / "integration.db"
    monkeypatch.setenv("DW_SIMULATOR_TARGET_DB_URL", f"sqlite:///{db_path}")
    return ExperimentService()


def read_parquet_table(output_dir: Path, table_name: str) -> pd.DataFrame:
    """Read all Parquet files for a table and concatenate into a single DataFrame."""
    table_dir = output_dir / table_name
    if not table_dir.exists():
        raise FileNotFoundError(f"Table directory not found: {table_dir}")

    parquet_files = list(table_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {table_dir}")

    # Read all parquet files and concatenate
    dfs = [pd.read_parquet(f) for f in parquet_files]
    return pd.concat(dfs, ignore_index=True)


def test_integration_exact_row_counts(service: ExperimentService, tmp_path: Path) -> None:
    """
    US 2.1 AC 1: Verify generated data matches exact target volume.

    Given an experiment with target_rows=100,
    When data generation completes,
    Then the parquet files contain exactly 100 rows.
    """
    schema = {
        "name": "RowCountTest",
        "tables": [
            {
                "name": "customers",
                "target_rows": 100,
                "columns": [
                    {"name": "customer_id", "data_type": "INT", "is_unique": True},
                    {"name": "name", "data_type": "VARCHAR", "varchar_length": 50},
                ],
            },
            {
                "name": "orders",
                "target_rows": 250,
                "columns": [
                    {"name": "order_id", "data_type": "INT", "is_unique": True},
                    {"name": "amount", "data_type": "FLOAT"},
                ],
            },
        ],
    }

    # Create experiment
    create_result = service.create_experiment_from_payload(schema)
    assert create_result.success is True, f"Failed to create experiment: {create_result.errors}"

    # Generate data
    output_dir = tmp_path / "output"
    gen_result = service.generate_data("RowCountTest", output_dir=output_dir, seed=42)
    assert gen_result.success is True, f"Failed to generate data: {gen_result.errors}"

    # Verify exact row counts from Parquet files
    customers_df = read_parquet_table(output_dir, "customers")
    orders_df = read_parquet_table(output_dir, "orders")

    assert len(customers_df) == 100, f"Expected 100 customers, got {len(customers_df)}"
    assert len(orders_df) == 250, f"Expected 250 orders, got {len(orders_df)}"


def test_integration_uniqueness_enforcement(service: ExperimentService, tmp_path: Path) -> None:
    """
    US 2.1 AC 2: Verify unique columns contain no duplicates.

    Given a column marked as is_unique=True,
    When data is generated,
    Then all values in that column are unique (no duplicates).
    """
    schema = {
        "name": "UniquenessTest",
        "tables": [
            {
                "name": "users",
                "target_rows": 500,
                "columns": [
                    {"name": "user_id", "data_type": "INT", "is_unique": True},
                    {"name": "email", "data_type": "VARCHAR", "varchar_length": 100, "is_unique": True},
                    {"name": "username", "data_type": "VARCHAR", "varchar_length": 50, "is_unique": True},
                ],
            }
        ],
    }

    # Create and generate
    create_result = service.create_experiment_from_payload(schema)
    assert create_result.success is True

    output_dir = tmp_path / "output"
    gen_result = service.generate_data("UniquenessTest", output_dir=output_dir, seed=123)
    assert gen_result.success is True

    # Verify uniqueness for all unique columns from Parquet
    users_df = read_parquet_table(output_dir, "users")
    total_rows = len(users_df)
    distinct_ids = users_df["user_id"].nunique()
    distinct_emails = users_df["email"].nunique()
    distinct_usernames = users_df["username"].nunique()

    assert total_rows == 500
    assert distinct_ids == 500, f"user_id not unique: {distinct_ids} distinct out of {total_rows}"
    assert distinct_emails == 500, f"email not unique: {distinct_emails} distinct out of {total_rows}"
    assert distinct_usernames == 500, f"username not unique: {distinct_usernames} distinct out of {total_rows}"


def test_integration_date_range_constraints(service: ExperimentService, tmp_path: Path) -> None:
    """
    US 2.1 AC 3: Verify date columns respect specified ranges.

    Given a date column with date_start='2020-01-01' and date_end='2023-12-31',
    When data is generated,
    Then all date values fall within [2020-01-01, 2023-12-31].
    """
    schema = {
        "name": "DateRangeTest",
        "tables": [
            {
                "name": "events",
                "target_rows": 200,
                "columns": [
                    {"name": "event_id", "data_type": "INT", "is_unique": True},
                    {
                        "name": "event_date",
                        "data_type": "DATE",
                        "date_start": "2020-01-01",
                        "date_end": "2023-12-31",
                    },
                ],
            }
        ],
    }

    # Create and generate
    create_result = service.create_experiment_from_payload(schema)
    assert create_result.success is True

    output_dir = tmp_path / "output"
    gen_result = service.generate_data("DateRangeTest", output_dir=output_dir, seed=456)
    assert gen_result.success is True

    # Verify all dates are within range from Parquet
    events_df = read_parquet_table(output_dir, "events")
    total_rows = len(events_df)

    # Convert date column to datetime
    events_df["event_date"] = pd.to_datetime(events_df["event_date"])
    min_date = events_df["event_date"].min().date()
    max_date = events_df["event_date"].max().date()

    assert total_rows == 200

    expected_min = date(2020, 1, 1)
    expected_max = date(2023, 12, 31)

    assert min_date >= expected_min, f"Min date {min_date} is before {expected_min}"
    assert max_date <= expected_max, f"Max date {max_date} is after {expected_max}"


def test_integration_varchar_length_constraints(service: ExperimentService, tmp_path: Path) -> None:
    """
    Verify VARCHAR columns respect length constraints.

    Given a varchar column with varchar_length=20,
    When data is generated,
    Then all values have length <= 20.
    """
    schema = {
        "name": "VarcharTest",
        "tables": [
            {
                "name": "products",
                "target_rows": 100,
                "columns": [
                    {"name": "product_id", "data_type": "INT", "is_unique": True},
                    {"name": "sku", "data_type": "VARCHAR", "varchar_length": 15},
                    {"name": "name", "data_type": "VARCHAR", "varchar_length": 50},
                ],
            }
        ],
    }

    # Create and generate
    create_result = service.create_experiment_from_payload(schema)
    assert create_result.success is True

    output_dir = tmp_path / "output"
    gen_result = service.generate_data("VarcharTest", output_dir=output_dir, seed=789)
    assert gen_result.success is True

    # Verify lengths from Parquet
    products_df = read_parquet_table(output_dir, "products")
    max_sku_len = products_df["sku"].str.len().max()
    max_name_len = products_df["name"].str.len().max()
    total_rows = len(products_df)

    assert total_rows == 100
    assert max_sku_len <= 15, f"SKU length {max_sku_len} exceeds limit of 15"
    assert max_name_len <= 50, f"Name length {max_name_len} exceeds limit of 50"


def test_integration_numeric_range_constraints(service: ExperimentService, tmp_path: Path) -> None:
    """
    Verify numeric columns respect min/max value constraints.

    Given an INT column with min_value=1 and max_value=100,
    When data is generated,
    Then all values fall within [1, 100].
    """
    schema = {
        "name": "NumericRangeTest",
        "tables": [
            {
                "name": "scores",
                "target_rows": 150,
                "columns": [
                    {"name": "score_id", "data_type": "INT", "is_unique": True},
                    {"name": "score", "data_type": "INT", "min_value": 1, "max_value": 100},
                    {"name": "rating", "data_type": "FLOAT", "min_value": 0.0, "max_value": 5.0},
                ],
            }
        ],
    }

    # Create and generate
    create_result = service.create_experiment_from_payload(schema)
    assert create_result.success is True

    output_dir = tmp_path / "output"
    gen_result = service.generate_data("NumericRangeTest", output_dir=output_dir, seed=999)
    assert gen_result.success is True

    # Verify ranges from Parquet
    scores_df = read_parquet_table(output_dir, "scores")
    min_score = scores_df["score"].min()
    max_score = scores_df["score"].max()
    min_rating = scores_df["rating"].min()
    max_rating = scores_df["rating"].max()
    total_rows = len(scores_df)

    assert total_rows == 150
    assert min_score >= 1, f"Min score {min_score} is below minimum of 1"
    assert max_score <= 100, f"Max score {max_score} exceeds maximum of 100"
    assert min_rating >= 0.0, f"Min rating {min_rating} is below minimum of 0.0"
    assert max_rating <= 5.0, f"Max rating {max_rating} exceeds maximum of 5.0"


def test_integration_optional_columns_allow_nulls(service: ExperimentService, tmp_path: Path) -> None:
    """
    Verify optional columns (required=False) can contain NULL values.

    Given a column with required=False,
    When data is generated,
    Then the column is present (schema verification).
    """
    schema = {
        "name": "OptionalTest",
        "tables": [
            {
                "name": "contacts",
                "target_rows": 100,
                "columns": [
                    {"name": "contact_id", "data_type": "INT", "is_unique": True},
                    {"name": "email", "data_type": "VARCHAR", "varchar_length": 100, "required": True},
                    {"name": "phone", "data_type": "VARCHAR", "varchar_length": 20, "required": False},
                    {"name": "notes", "data_type": "VARCHAR", "varchar_length": 200, "required": False},
                ],
            }
        ],
    }

    # Create and generate
    create_result = service.create_experiment_from_payload(schema)
    assert create_result.success is True

    output_dir = tmp_path / "output"
    gen_result = service.generate_data("OptionalTest", output_dir=output_dir, seed=111)
    assert gen_result.success is True

    # Verify schema includes all columns from Parquet
    contacts_df = read_parquet_table(output_dir, "contacts")
    total_rows = len(contacts_df)

    assert total_rows == 100
    # Verify all columns are present
    assert "contact_id" in contacts_df.columns
    assert "email" in contacts_df.columns
    assert "phone" in contacts_df.columns
    assert "notes" in contacts_df.columns

    # Required columns should have no NULLs
    assert contacts_df["email"].isna().sum() == 0, "Required email column should not contain NULLs"


def test_integration_row_override_parameter(service: ExperimentService, tmp_path: Path) -> None:
    """
    Verify that row count overrides work correctly.

    Given an experiment with target_rows=1000,
    When generate_data is called with rows={'items': 50},
    Then the parquet files contain exactly 50 rows (not 1000).
    """
    schema = {
        "name": "OverrideTest",
        "tables": [
            {
                "name": "items",
                "target_rows": 1000,
                "columns": [
                    {"name": "item_id", "data_type": "INT", "is_unique": True},
                ],
            }
        ],
    }

    # Create and generate with override
    create_result = service.create_experiment_from_payload(schema)
    assert create_result.success is True

    output_dir = tmp_path / "output"
    gen_result = service.generate_data("OverrideTest", rows={"items": 75}, output_dir=output_dir, seed=222)
    assert gen_result.success is True

    # Verify overridden row count from Parquet
    items_df = read_parquet_table(output_dir, "items")
    actual_count = len(items_df)

    assert actual_count == 75, f"Expected 75 rows (override), got {actual_count}"


def test_integration_generation_failure_handling(service: ExperimentService) -> None:
    """
    Verify that generation errors are properly reported.

    Given an invalid faker rule,
    When generate_data is called,
    Then the result indicates failure with a descriptive error.
    """
    schema = {
        "name": "FailureTest",
        "tables": [
            {
                "name": "bad_data",
                "target_rows": 10,
                "columns": [
                    {"name": "id", "data_type": "INT", "is_unique": True},
                    {
                        "name": "bad_col",
                        "data_type": "VARCHAR",
                        "varchar_length": 50,
                        "faker_rule": "nonexistent_faker_method",
                    },
                ],
            }
        ],
    }

    # Create experiment
    create_result = service.create_experiment_from_payload(schema)
    assert create_result.success is True

    # Attempt generation (should fail)
    gen_result = service.generate_data("FailureTest")
    assert gen_result.success is False
    assert len(gen_result.errors) > 0
    assert "faker" in gen_result.errors[0].lower() or "nonexistent" in gen_result.errors[0].lower()


def test_integration_multi_table_generation(service: ExperimentService, db_engine: Engine, tmp_path: Path) -> None:
    """
    Verify that multi-table experiments generate data for all tables.

    Given an experiment with 3 tables,
    When generate_data is called,
    Then all 3 tables are populated with correct row counts.
    """
    schema = {
        "name": "MultiTableTest",
        "tables": [
            {
                "name": "customers",
                "target_rows": 50,
                "columns": [{"name": "customer_id", "data_type": "INT", "is_unique": True}],
            },
            {
                "name": "orders",
                "target_rows": 200,
                "columns": [{"name": "order_id", "data_type": "INT", "is_unique": True}],
            },
            {
                "name": "products",
                "target_rows": 30,
                "columns": [{"name": "product_id", "data_type": "INT", "is_unique": True}],
            },
        ],
    }

    # Create and generate
    create_result = service.create_experiment_from_payload(schema)
    assert create_result.success is True

    output_dir = tmp_path / "output"
    gen_result = service.generate_data("MultiTableTest", output_dir=output_dir, seed=333)
    assert gen_result.success is True

    # Verify all tables are created in DB with correct naming
    inspector = inspect(db_engine)
    tables = inspector.get_table_names()
    assert "multitabletest__customers" in tables
    assert "multitabletest__orders" in tables
    assert "multitabletest__products" in tables

    # Verify all parquet files have correct counts
    customers_df = read_parquet_table(output_dir, "customers")
    orders_df = read_parquet_table(output_dir, "orders")
    products_df = read_parquet_table(output_dir, "products")

    assert len(customers_df) == 50
    assert len(orders_df) == 200
    assert len(products_df) == 30
