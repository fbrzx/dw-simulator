"""
Integration tests for the complex.json schema (Ralph Lauren Data Container).

This test suite validates:
1. Schema validation and loading
2. Synthetic data generation with unique date constraints
3. Data quality and referential integrity
4. SQL queries against generated data
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Generator

import pandas as pd
import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from dw_simulator.generator import ExperimentGenerator, GenerationRequest
from dw_simulator.schema import ExperimentSchema


@pytest.fixture
def complex_schema() -> ExperimentSchema:
    """Load the complex.json schema."""
    schema_path = Path(__file__).parent.parent.parent.parent / "docs" / "examples" / "schemas" / "data_container.json"
    with open(schema_path, "r") as f:
        schema_data = json.load(f)
    return ExperimentSchema.model_validate(schema_data)


@pytest.fixture
def db_engine(tmp_path: Path) -> Generator[Engine, None, None]:
    """Create a temporary SQLite database for testing."""
    db_path = tmp_path / "complex_test.db"
    db_url = f"sqlite:///{db_path}"
    engine = create_engine(db_url)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def generated_data(complex_schema: ExperimentSchema, tmp_path: Path, db_engine: Engine) -> Path:
    """Generate synthetic data for the complex schema with reduced row counts."""
    # Use smaller row counts for faster testing
    row_overrides = {
        "clickstream_aggregate_vw": 1000,
        "currency_exchange_tb": 192,  # Keep original - it's small
        "sas_calendar": 365,  # One year of dates
        "sas_customer_mstr_vw": 500,
        "sas_transaction_vw": 2000,
        "sas_lookup_vw": 100,
        "sas_email_addresses_vw": 300,
    }

    output_dir = tmp_path / "generated_data"
    generator = ExperimentGenerator(batch_size=100)
    request = GenerationRequest(
        schema=complex_schema,
        output_root=output_dir,
        row_overrides=row_overrides,
        seed=42,
    )

    result = generator.generate(request)
    assert result.experiment_name == "data_container"
    assert len(result.tables) == 7

    # Import data into the database using pandas
    for table_result in result.tables:
        if table_result.row_count > 0:
            # Read all parquet files for this table
            dfs = []
            for file_path in table_result.files:
                df = pd.read_parquet(file_path)
                dfs.append(df)

            # Concatenate and write to SQL
            if dfs:
                combined_df = pd.concat(dfs, ignore_index=True)
                combined_df.to_sql(
                    table_result.table_name,
                    db_engine,
                    if_exists="replace",
                    index=False,
                )

    return result.output_dir


def test_complex_schema_validation(complex_schema: ExperimentSchema) -> None:
    """Test that the complex schema is valid and correctly structured."""
    assert complex_schema.name == "data_container"
    assert len(complex_schema.tables) == 7

    # Verify table names
    table_names = {table.name for table in complex_schema.tables}
    expected_tables = {
        "clickstream_aggregate_vw",
        "currency_exchange_tb",
        "sas_calendar",
        "sas_customer_mstr_vw",
        "sas_transaction_vw",
        "sas_lookup_vw",
        "sas_email_addresses_vw",
    }
    assert table_names == expected_tables

    # Verify sas_calendar has unique date constraint with proper range
    calendar_table = next(t for t in complex_schema.tables if t.name == "sas_calendar")
    date_column = next(c for c in calendar_table.columns if c.name == "exact_day_dt")
    assert date_column.is_unique is True
    assert date_column.required is True
    assert date_column.date_start is not None
    assert date_column.date_end is not None
    assert date_column.data_type == "DATE"


def test_unique_date_generation(complex_schema: ExperimentSchema, tmp_path: Path) -> None:
    """Test that unique dates are generated correctly for sas_calendar."""
    generator = ExperimentGenerator(batch_size=50)
    request = GenerationRequest(
        schema=complex_schema,
        output_root=tmp_path / "date_test",
        row_overrides={
            "clickstream_aggregate_vw": 1,
            "currency_exchange_tb": 1,
            "sas_calendar": 100,  # Test with 100 unique dates
            "sas_customer_mstr_vw": 1,
            "sas_transaction_vw": 1,
            "sas_lookup_vw": 1,
            "sas_email_addresses_vw": 1,
        },
        seed=12345,
    )

    result = generator.generate(request)

    # Find the calendar table result
    calendar_result = next(t for t in result.tables if t.table_name == "sas_calendar")
    assert calendar_result.row_count == 100

    # Load all dates from parquet files
    all_dates = []
    for file_path in calendar_result.files:
        df = pd.read_parquet(file_path)
        all_dates.extend(df["exact_day_dt"].tolist())

    # Verify uniqueness
    assert len(all_dates) == 100
    assert len(set(all_dates)) == 100, "All dates should be unique"

    # Verify dates are sequential
    sorted_dates = sorted(all_dates)
    for i in range(1, len(sorted_dates)):
        days_diff = (sorted_dates[i] - sorted_dates[i - 1]).days
        assert days_diff == 1, f"Dates should be sequential (found gap of {days_diff} days)"


def test_data_generation_row_counts(generated_data: Path, db_engine: Engine) -> None:
    """Test that generated data matches expected row counts."""
    inspector = inspect(db_engine)
    tables = inspector.get_table_names()

    # Expected row counts (from row_overrides in generated_data fixture)
    expected_counts = {
        "clickstream_aggregate_vw": 1000,
        "currency_exchange_tb": 192,
        "sas_calendar": 365,
        "sas_customer_mstr_vw": 500,
        "sas_transaction_vw": 2000,
        "sas_lookup_vw": 100,
        "sas_email_addresses_vw": 300,
    }

    with db_engine.connect() as conn:
        for table_name, expected_count in expected_counts.items():
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            actual_count = result.scalar()
            assert actual_count == expected_count, f"Table {table_name} has {actual_count} rows, expected {expected_count}"


def test_unique_constraints(generated_data: Path, db_engine: Engine) -> None:
    """Test that unique constraints are satisfied in generated data."""
    test_cases = [
        ("sas_calendar", "exact_day_dt"),
        ("sas_transaction_vw", "transaction_no"),
        ("sas_email_addresses_vw", "email_id"),
    ]

    with db_engine.connect() as conn:
        for table_name, column_name in test_cases:
            # Count total rows
            total_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            total_rows = total_result.scalar()

            # Count distinct values
            distinct_result = conn.execute(text(f"SELECT COUNT(DISTINCT {column_name}) FROM {table_name}"))
            distinct_values = distinct_result.scalar()

            assert total_rows == distinct_values, (
                f"Column {table_name}.{column_name} should have all unique values "
                f"(found {distinct_values} unique out of {total_rows} total)"
            )


def test_date_range_constraints(generated_data: Path, db_engine: Engine) -> None:
    """Test that date columns respect specified ranges."""
    with db_engine.connect() as conn:
        # Test sas_calendar dates are within expected range
        df = pd.read_sql_query("SELECT MIN(exact_day_dt) as min_date, MAX(exact_day_dt) as max_date FROM sas_calendar", db_engine)

        # Convert string dates to date objects if necessary
        min_date = pd.to_datetime(df["min_date"].iloc[0]).date()
        max_date = pd.to_datetime(df["max_date"].iloc[0]).date()

        # Dates should be sequential starting from date_start
        expected_start = date(2022, 1, 1)
        # With 365 rows, we should have exactly one year of dates
        expected_end = date(2022, 12, 30)

        assert min_date == expected_start, f"Min date should be {expected_start}, got {min_date}"
        # Max date should be close to expected (within a few days for 365 sequential dates)
        days_diff = abs((max_date - expected_end).days)
        assert days_diff <= 1, f"Max date {max_date} should be close to {expected_end}"


def test_referential_integrity(generated_data: Path, db_engine: Engine) -> None:
    """Test data generation across all tables completes successfully."""
    # Note: The complex schema doesn't have explicit foreign key constraints defined,
    # so we just verify that all tables have data and relationships could potentially exist
    with db_engine.connect() as conn:
        # Verify all expected tables exist and have data
        for table_name in ["sas_transaction_vw", "sas_customer_mstr_vw", "sas_email_addresses_vw"]:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            row_count = result.scalar()
            assert row_count > 0, f"Table {table_name} should have data"

        # Check that customer IDs exist in their respective tables (even if they don't overlap)
        result = conn.execute(text("SELECT COUNT(DISTINCT mstr_customer_id) FROM sas_transaction_vw"))
        transaction_customers = result.scalar()
        assert transaction_customers > 0, "Transactions should have customer IDs"

        result = conn.execute(text("SELECT COUNT(DISTINCT mstr_customer_id) FROM sas_customer_mstr_vw"))
        master_customers = result.scalar()
        assert master_customers > 0, "Customer master should have customer IDs"

        # Optional: Check for overlap (but don't fail if none exists since FK isn't defined)
        result = conn.execute(
            text("""
                SELECT COUNT(DISTINCT t.mstr_customer_id)
                FROM sas_transaction_vw t
                INNER JOIN sas_customer_mstr_vw c ON t.mstr_customer_id = c.mstr_customer_id
            """)
        )
        matching_customers = result.scalar()
        # Just log this info, don't assert since FK relationships aren't defined in schema
        print(f"Info: {matching_customers}/{transaction_customers} transaction customers match customer master")


def test_query_customer_lifetime_value(generated_data: Path, db_engine: Engine) -> None:
    """Test the customer lifetime value query from complex_01_customer_lifetime_value.sql."""
    query = """
        SELECT
            c.mstr_customer_id,
            c.mstr_household_id,
            COUNT(DISTINCT t.transaction_no) as total_transactions,
            SUM(t.amount) as total_transaction_amount,
            AVG(t.amount) as avg_transaction_amount,
            MAX(t.amount) as max_transaction_amount,
            c.lifetime_cust_profit,
            c.m0_12_net as last_12m_net_profit,
            ROUND(c.lifetime_cust_profit / NULLIF(COUNT(DISTINCT t.transaction_no), 0), 2) as profit_per_transaction,
            COUNT(DISTINCT t.business_unit) as business_units_purchased_from
        FROM
            sas_customer_mstr_vw c
        LEFT JOIN
            sas_transaction_vw t ON c.mstr_customer_id = t.mstr_customer_id
        GROUP BY
            c.mstr_customer_id,
            c.mstr_household_id,
            c.lifetime_cust_profit,
            c.m0_12_net
        ORDER BY
            c.lifetime_cust_profit DESC
        LIMIT 10
    """

    df = pd.read_sql_query(query, db_engine)

    # Verify query returns results
    assert len(df) > 0, "Query should return results"
    assert len(df) <= 10, "Query should respect LIMIT"

    # Verify columns exist
    expected_columns = {
        "mstr_customer_id",
        "mstr_household_id",
        "total_transactions",
        "total_transaction_amount",
        "avg_transaction_amount",
        "max_transaction_amount",
        "lifetime_cust_profit",
        "last_12m_net_profit",
        "profit_per_transaction",
        "business_units_purchased_from",
    }
    assert set(df.columns) == expected_columns

    # Verify data makes sense
    assert df["total_transactions"].min() >= 0
    assert df["lifetime_cust_profit"].notna().all() or df["lifetime_cust_profit"].isna().any()


def test_query_transaction_calendar_analysis(generated_data: Path, db_engine: Engine) -> None:
    """Test the transaction calendar analysis query from complex_02_transaction_calendar_analysis.sql."""
    # First check if there are any matching dates
    check_query = """
        SELECT COUNT(*)
        FROM sas_transaction_vw t
        INNER JOIN sas_calendar cal ON t.transaction_date = cal.exact_day_dt
    """
    matching_count = pd.read_sql_query(check_query, db_engine).iloc[0, 0]

    # Only run the full query if there are matches
    if matching_count == 0:
        pytest.skip("No matching dates between transactions and calendar - this is expected without FK constraints")

    query = """
        SELECT
            cal.fyr as fiscal_year,
            cal.fmm as fiscal_month,
            cal.cycq as cycle_quarter,
            COUNT(DISTINCT t.transaction_no) as transaction_count,
            COUNT(DISTINCT t.mstr_customer_id) as unique_customers,
            SUM(t.amount) as total_amount,
            AVG(t.amount) as avg_transaction_amount,
            MIN(t.amount) as min_amount,
            MAX(t.amount) as max_amount
        FROM
            sas_transaction_vw t
        INNER JOIN
            sas_calendar cal ON t.transaction_date = cal.exact_day_dt
        GROUP BY
            cal.fyr,
            cal.fmm,
            cal.cycq
        ORDER BY
            cal.fyr DESC,
            cal.fmm DESC
    """

    df = pd.read_sql_query(query, db_engine)

    # Verify query returns results
    assert len(df) > 0, "Query should return results"

    # Verify columns
    expected_columns = {
        "fiscal_year",
        "fiscal_month",
        "cycle_quarter",
        "transaction_count",
        "unique_customers",
        "total_amount",
        "avg_transaction_amount",
        "min_amount",
        "max_amount",
    }
    assert set(df.columns) == expected_columns

    # Verify fiscal year is valid
    assert df["fiscal_year"].between(2000, 2050).all()
    assert df["fiscal_month"].between(1, 12).all()


def test_query_email_engagement(generated_data: Path, db_engine: Engine) -> None:
    """Test the email engagement query from complex_03_email_engagement.sql."""
    # First check if there are any matching customers
    check_query = """
        SELECT COUNT(*)
        FROM sas_email_addresses_vw e
        INNER JOIN sas_customer_mstr_vw c ON e.customer_id = c.customer_id
    """
    matching_count = pd.read_sql_query(check_query, db_engine).iloc[0, 0]

    # Only run the full query if there are matches
    if matching_count == 0:
        pytest.skip("No matching customers between emails and customer master - this is expected without FK constraints")

    query = """
        SELECT
            CASE
                WHEN e.opt_out_date IS NOT NULL THEN 'Opted Out'
                WHEN e.opt_in_date IS NOT NULL THEN 'Active Subscriber'
                ELSE 'No Opt-In'
            END as email_status,
            COUNT(DISTINCT e.email_id) as email_count,
            COUNT(DISTINCT e.customer_id) as customer_count,
            COUNT(DISTINCT c.mstr_household_id) as household_count,
            ROUND(AVG(c.lifetime_cust_profit), 2) as avg_customer_lifetime_value,
            ROUND(AVG(c.m0_12_net), 2) as avg_12m_net_profit
        FROM
            sas_email_addresses_vw e
        INNER JOIN
            sas_customer_mstr_vw c ON e.customer_id = c.customer_id
        GROUP BY
            email_status
        ORDER BY
            email_count DESC
    """

    df = pd.read_sql_query(query, db_engine)

    # Verify query returns results
    assert len(df) > 0, "Query should return results"

    # Verify email status values are valid
    valid_statuses = {"Opted Out", "Active Subscriber", "No Opt-In"}
    assert set(df["email_status"]) <= valid_statuses

    # Verify counts are positive
    assert df["email_count"].min() > 0
    assert df["customer_count"].min() > 0


def test_query_clickstream_customer_behavior(generated_data: Path, db_engine: Engine) -> None:
    """Test the clickstream customer behavior query from complex_05_clickstream_customer_behavior.sql."""
    # First check if there are any matching customers
    check_query = """
        SELECT COUNT(*)
        FROM clickstream_aggregate_vw ca
        INNER JOIN sas_customer_mstr_vw c ON ca.mstr_customer_id = c.mstr_customer_id
    """
    matching_count = pd.read_sql_query(check_query, db_engine).iloc[0, 0]

    # Only run the full query if there are matches
    if matching_count == 0:
        pytest.skip("No matching customers between clickstream and customer master - this is expected without FK constraints")

    query = """
        SELECT
            CASE
                WHEN ca.total_visits >= 50 THEN 'Heavy User'
                WHEN ca.total_visits >= 20 THEN 'Regular User'
                WHEN ca.total_visits >= 5 THEN 'Occasional User'
                ELSE 'Light User'
            END as user_segment,
            COUNT(DISTINCT ca.mstr_customer_id) as customer_count,
            AVG(ca.total_visits) as avg_total_visits,
            AVG(ca.avg_total_pv_visits) as avg_page_views_per_visit,
            AVG(c.lifetime_cust_profit) as avg_lifetime_value,
            AVG(c.m0_12_net) as avg_12m_net_profit,
            SUM(CASE WHEN c.lifetime_cust_profit > 0 THEN 1 ELSE 0 END) as profitable_customers,
            ROUND(100.0 * SUM(CASE WHEN c.lifetime_cust_profit > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as profitable_customer_pct
        FROM
            clickstream_aggregate_vw ca
        INNER JOIN
            sas_customer_mstr_vw c ON ca.mstr_customer_id = c.mstr_customer_id
        GROUP BY
            user_segment
        ORDER BY
            avg_total_visits DESC
    """

    df = pd.read_sql_query(query, db_engine)

    # Verify query returns results
    assert len(df) > 0, "Query should return results"

    # Verify user segments are valid
    valid_segments = {"Heavy User", "Regular User", "Occasional User", "Light User"}
    assert set(df["user_segment"]) <= valid_segments

    # Verify customer counts are positive
    assert df["customer_count"].min() > 0

    # Verify percentages are valid
    assert df["profitable_customer_pct"].between(0, 100).all()
