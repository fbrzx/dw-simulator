from collections import defaultdict
from pathlib import Path
import random

import pyarrow.parquet as pq
import pytest
from faker import Faker

from dw_simulator.generator import (
    ExperimentGenerator,
    GenerationRequest,
    GenerationError,
    TableGenerationResult,
)
from dw_simulator.schema import ColumnSchema, ExperimentSchema, TableSchema


def sample_schema() -> ExperimentSchema:
    return ExperimentSchema(
        name="gen_test",
        description=None,
        tables=[
            TableSchema(
                name="customers",
                target_rows=50,
                columns=[
                    ColumnSchema(name="customer_id", data_type="INT", is_unique=True),
                    ColumnSchema(name="email", data_type="VARCHAR", faker_rule="email", varchar_length=64),
                    ColumnSchema(
                        name="signup_date",
                        data_type="DATE",
                        date_start="2024-01-01",
                        date_end="2024-01-31",
                    ),
                ],
            )
        ],
    )


def test_generator_produces_parquet(tmp_path: Path) -> None:
    generator = ExperimentGenerator(batch_size=20)
    schema = sample_schema()
    result = generator.generate(
        GenerationRequest(schema=schema, output_root=tmp_path / "out", seed=1234),
    )

    assert result.experiment_name == "gen_test"
    assert result.output_dir.exists()
    table_result = result.tables[0]
    assert table_result.row_count == 50
    assert len(table_result.files) == 3  # 20 + 20 + 10 batches
    total_rows = 0
    seen_ids: set[int] = set()
    for file_path in table_result.files:
        table = pq.read_table(file_path)
        total_rows += table.num_rows
        ids = table.column("customer_id").to_pylist()
        assert len(ids) == len(set(ids))
        seen_ids.update(ids)
        dates = table.column("signup_date").to_pylist()
        for value in dates:
            assert value.year == 2024
    assert total_rows == 50
    assert len(seen_ids) == 50


def test_generator_row_overrides(tmp_path: Path) -> None:
    generator = ExperimentGenerator(batch_size=100)
    schema = sample_schema()
    result = generator.generate(
        GenerationRequest(schema=schema, output_root=tmp_path / "out", row_overrides={"customers": 5}),
    )
    assert result.tables[0].row_count == 5


def test_generator_optional_column_can_be_null() -> None:
    generator = ExperimentGenerator()
    column = ColumnSchema(name="optional_value", data_type="INT", required=False)
    rng = random.Random(0)
    rng.random = lambda: 0.0  # force null path
    faker = Faker()
    value = generator._generate_value(
        column_schema=column,
        rng=rng,
        faker=faker,
        unique_values=defaultdict(set),
        next_unique_int=defaultdict(int),
    )
    assert value is None


def test_generator_invalid_faker_rule() -> None:
    generator = ExperimentGenerator()
    faker = Faker()
    with pytest.raises(GenerationError):
        generator._run_faker_rule(faker, "notreal.rule")


def test_generator_normal_distribution_for_int_column() -> None:
    """Normal distribution uses RNG.gauss and respects numeric bounds for INT columns."""
    generator = ExperimentGenerator()
    column = ColumnSchema(
        name="metric",
        data_type="INT",
        min_value=0,
        max_value=200,
        distribution={
            "type": "normal",
            "parameters": {"mean": 100.0, "stddev": 15.0},
        },
    )
    rng = random.Random(1234)
    faker = Faker()

    values = [
        generator._generate_value(
            column_schema=column,
            rng=rng,
            faker=faker,
            unique_values=defaultdict(set),
            next_unique_int=defaultdict(int),
        )
        for _ in range(5)
    ]

    control_rng = random.Random(1234)
    expected = []
    for _ in range(5):
        sample = control_rng.gauss(100.0, 15.0)
        sample = max(0, min(200, sample))
        expected.append(int(round(sample)))

    assert values == expected


def test_generator_exponential_distribution_for_float_column() -> None:
    """Exponential distribution pulls from RNG.expovariate and clamps to configured bounds."""
    generator = ExperimentGenerator()
    column = ColumnSchema(
        name="latency",
        data_type="FLOAT",
        min_value=0.0,
        max_value=10.0,
        distribution={
            "type": "exponential",
            "parameters": {"lambda": 2.0},
        },
    )
    rng = random.Random(321)
    faker = Faker()

    values = [
        generator._generate_value(
            column_schema=column,
            rng=rng,
            faker=faker,
            unique_values=defaultdict(set),
            next_unique_int=defaultdict(int),
        )
        for _ in range(3)
    ]

    control_rng = random.Random(321)
    expected = []
    for _ in range(3):
        sample = control_rng.expovariate(2.0)
        sample = max(0.0, min(10.0, sample))
        expected.append(sample)

    assert values == pytest.approx(expected)


def test_generator_beta_distribution_scales_to_range() -> None:
    """Beta distribution scales into the configured min/max window and remains deterministic."""
    generator = ExperimentGenerator()
    column = ColumnSchema(
        name="ratio",
        data_type="FLOAT",
        min_value=10.0,
        max_value=20.0,
        distribution={
            "type": "beta",
            "parameters": {"alpha": 2.0, "beta": 5.0},
        },
    )
    faker = Faker()

    rng = random.Random(999)
    values = [
        generator._generate_value(
            column_schema=column,
            rng=rng,
            faker=faker,
            unique_values=defaultdict(set),
            next_unique_int=defaultdict(int),
        )
        for _ in range(5)
    ]

    assert all(10.0 <= value <= 20.0 for value in values)

    control_rng = random.Random(999)
    expected = []
    for _ in range(5):
        beta_sample = control_rng.betavariate(2.0, 5.0)
        expected.append(10.0 + (20.0 - 10.0) * beta_sample)

    assert values == pytest.approx(expected)


def test_generator_generate_respects_distribution_bounds(tmp_path: Path) -> None:
    """Full table generation respects configured distributions across batches."""

    schema = ExperimentSchema(
        name="distribution_batch_test",
        description=None,
        tables=[
            TableSchema(
                name="metrics",
                target_rows=75,
                columns=[
                    ColumnSchema(name="metric_id", data_type="INT", is_unique=True),
                    ColumnSchema(
                        name="score",
                        data_type="INT",
                        min_value=0,
                        max_value=100,
                        distribution={
                            "type": "normal",
                            "parameters": {"mean": 50, "stddev": 10},
                        },
                    ),
                    ColumnSchema(
                        name="ratio",
                        data_type="FLOAT",
                        min_value=10.0,
                        max_value=20.0,
                        distribution={
                            "type": "beta",
                            "parameters": {"alpha": 2.5, "beta": 3.5},
                        },
                    ),
                ],
            )
        ],
    )

    generator = ExperimentGenerator(batch_size=20)

    first_run = generator.generate(
        GenerationRequest(schema=schema, output_root=tmp_path / "run1", seed=2024)
    )
    second_run = generator.generate(
        GenerationRequest(schema=schema, output_root=tmp_path / "run2", seed=2024)
    )

    assert len(first_run.tables) == 1
    assert len(second_run.tables) == 1

    def _collect(table_result: TableGenerationResult) -> tuple[list[int], list[float]]:
        int_values: list[int] = []
        float_values: list[float] = []
        for file_path in table_result.files:
            table = pq.read_table(file_path)
            int_values.extend(table.column("score").to_pylist())
            float_values.extend(table.column("ratio").to_pylist())
        return int_values, float_values

    first_scores, first_ratios = _collect(first_run.tables[0])
    second_scores, second_ratios = _collect(second_run.tables[0])

    assert len(first_scores) == 75
    assert len(first_ratios) == 75

    assert all(0 <= value <= 100 for value in first_scores)
    assert all(10.0 <= value <= 20.0 for value in first_ratios)

    # Deterministic seeding should yield identical values across runs.
    assert first_scores == second_scores
    assert first_ratios == pytest.approx(second_ratios)


def test_generator_surrogate_key_starts_at_one(tmp_path: Path) -> None:
    """Test that _row_id surrogate key columns start at 1 and increment sequentially."""
    schema = ExperimentSchema(
        name="composite_pk_test",
        description=None,
        tables=[
            TableSchema(
                name="orders",
                target_rows=100,
                columns=[
                    ColumnSchema(name="_row_id", data_type="INT", is_unique=True),
                    ColumnSchema(name="store_id", data_type="INT"),
                    ColumnSchema(name="order_id", data_type="INT"),
                ],
                composite_keys=[["store_id", "order_id"]],
                warnings=["Table 'orders' has composite primary key (store_id, order_id). A surrogate '_row_id' column was added for uniqueness."],
            )
        ],
    )

    generator = ExperimentGenerator(batch_size=30)
    result = generator.generate(
        GenerationRequest(schema=schema, output_root=tmp_path / "out", seed=42),
    )

    # Collect all _row_id values across all batches
    all_row_ids: list[int] = []
    for file_path in result.tables[0].files:
        table = pq.read_table(file_path)
        row_ids = table.column("_row_id").to_pylist()
        all_row_ids.extend(row_ids)

    # Verify: should have exactly 100 values
    assert len(all_row_ids) == 100

    # Verify: all values are unique
    assert len(set(all_row_ids)) == 100

    # Verify: values are sequential starting from 1
    assert sorted(all_row_ids) == list(range(1, 101))

    # Verify: first batch starts at 1
    first_batch = pq.read_table(result.tables[0].files[0])
    first_batch_ids = first_batch.column("_row_id").to_pylist()
    assert first_batch_ids[0] == 1


def test_generator_surrogate_key_multiple_tables(tmp_path: Path) -> None:
    """Test that each table gets its own independent _row_id sequence starting at 1."""
    schema = ExperimentSchema(
        name="multi_table_test",
        description=None,
        tables=[
            TableSchema(
                name="table_a",
                target_rows=10,
                columns=[
                    ColumnSchema(name="_row_id", data_type="INT", is_unique=True),
                    ColumnSchema(name="value", data_type="INT"),
                ],
                composite_keys=[["value"]],
            ),
            TableSchema(
                name="table_b",
                target_rows=20,
                columns=[
                    ColumnSchema(name="_row_id", data_type="INT", is_unique=True),
                    ColumnSchema(name="data", data_type="VARCHAR", varchar_length=50),
                ],
                composite_keys=[["data"]],
            ),
        ],
    )

    generator = ExperimentGenerator(batch_size=100)
    result = generator.generate(
        GenerationRequest(schema=schema, output_root=tmp_path / "out", seed=99),
    )

    # Check table_a: should have _row_id from 1 to 10
    table_a_data = pq.read_table(result.tables[0].files[0])
    table_a_ids = sorted(table_a_data.column("_row_id").to_pylist())
    assert table_a_ids == list(range(1, 11))

    # Check table_b: should have _row_id from 1 to 20 (independent sequence)
    table_b_data = pq.read_table(result.tables[1].files[0])
    table_b_ids = sorted(table_b_data.column("_row_id").to_pylist())
    assert table_b_ids == list(range(1, 21))


# US 4.1 Acceptance Criteria Tests


def test_us41_ac1_varchar_faker_rules(tmp_path: Path) -> None:
    """
    US 4.1 AC 1: GIVEN a column of type VARCHAR is defined,
    WHEN the user specifies a 'Faker Rule' (e.g., first_name or email),
    THEN the generation engine uses that rule for population.
    """
    schema = ExperimentSchema(
        name="faker_test",
        description=None,
        tables=[
            TableSchema(
                name="users",
                target_rows=50,
                columns=[
                    ColumnSchema(name="id", data_type="INT", is_unique=True),
                    ColumnSchema(name="first_name", data_type="VARCHAR", faker_rule="first_name", varchar_length=50),
                    ColumnSchema(name="last_name", data_type="VARCHAR", faker_rule="last_name", varchar_length=50),
                    ColumnSchema(name="email", data_type="VARCHAR", faker_rule="email", varchar_length=100),
                    ColumnSchema(name="company", data_type="VARCHAR", faker_rule="company", varchar_length=100),
                    ColumnSchema(name="city", data_type="VARCHAR", faker_rule="city", varchar_length=50),
                ],
            )
        ],
    )

    generator = ExperimentGenerator(batch_size=50)
    result = generator.generate(
        GenerationRequest(schema=schema, output_root=tmp_path / "out", seed=42),
    )

    # Read generated data
    table = pq.read_table(result.tables[0].files[0])

    # Verify all columns have data
    assert table.num_rows == 50

    # Verify first_name column contains realistic names (not generic words)
    first_names = table.column("first_name").to_pylist()
    assert all(name and len(name) > 0 for name in first_names)

    # Verify last_name column contains realistic names
    last_names = table.column("last_name").to_pylist()
    assert all(name and len(name) > 0 for name in last_names)

    # Verify email column contains @ symbols (characteristic of email addresses)
    emails = table.column("email").to_pylist()
    assert all("@" in email for email in emails)

    # Verify company column contains data
    companies = table.column("company").to_pylist()
    assert all(company and len(company) > 0 for company in companies)

    # Verify city column contains data
    cities = table.column("city").to_pylist()
    assert all(city and len(city) > 0 for city in cities)


def test_us41_ac2_int_numeric_ranges(tmp_path: Path) -> None:
    """
    US 4.1 AC 2: GIVEN a numeric column is defined,
    WHEN the user specifies a range (min/max),
    THEN the generated data for that column respects the defined boundaries.
    Testing with INT data type.
    """
    schema = ExperimentSchema(
        name="int_range_test",
        description=None,
        tables=[
            TableSchema(
                name="measurements",
                target_rows=100,
                columns=[
                    ColumnSchema(name="id", data_type="INT", is_unique=True),
                    ColumnSchema(name="age", data_type="INT", min_value=18, max_value=65),
                    ColumnSchema(name="score", data_type="INT", min_value=0, max_value=100),
                    ColumnSchema(name="quantity", data_type="INT", min_value=1, max_value=10),
                ],
            )
        ],
    )

    generator = ExperimentGenerator(batch_size=100)
    result = generator.generate(
        GenerationRequest(schema=schema, output_root=tmp_path / "out", seed=123),
    )

    # Read generated data
    table = pq.read_table(result.tables[0].files[0])
    assert table.num_rows == 100

    # Verify age column respects min_value=18, max_value=65
    ages = table.column("age").to_pylist()
    assert all(18 <= age <= 65 for age in ages), f"Found age values outside [18, 65]: {[a for a in ages if a < 18 or a > 65]}"

    # Verify score column respects min_value=0, max_value=100
    scores = table.column("score").to_pylist()
    assert all(0 <= score <= 100 for score in scores), f"Found score values outside [0, 100]: {[s for s in scores if s < 0 or s > 100]}"

    # Verify quantity column respects min_value=1, max_value=10
    quantities = table.column("quantity").to_pylist()
    assert all(1 <= qty <= 10 for qty in quantities), f"Found quantity values outside [1, 10]: {[q for q in quantities if q < 1 or q > 10]}"


def test_us41_ac2_float_numeric_ranges(tmp_path: Path) -> None:
    """
    US 4.1 AC 2: GIVEN a numeric column is defined,
    WHEN the user specifies a range (min/max),
    THEN the generated data for that column respects the defined boundaries.
    Testing with FLOAT data type.
    """
    schema = ExperimentSchema(
        name="float_range_test",
        description=None,
        tables=[
            TableSchema(
                name="metrics",
                target_rows=100,
                columns=[
                    ColumnSchema(name="id", data_type="INT", is_unique=True),
                    ColumnSchema(name="price", data_type="FLOAT", min_value=9.99, max_value=999.99),
                    ColumnSchema(name="rating", data_type="FLOAT", min_value=0.0, max_value=5.0),
                    ColumnSchema(name="percentage", data_type="FLOAT", min_value=0.0, max_value=100.0),
                ],
            )
        ],
    )

    generator = ExperimentGenerator(batch_size=100)
    result = generator.generate(
        GenerationRequest(schema=schema, output_root=tmp_path / "out", seed=456),
    )

    # Read generated data
    table = pq.read_table(result.tables[0].files[0])
    assert table.num_rows == 100

    # Verify price column respects min_value=9.99, max_value=999.99
    prices = table.column("price").to_pylist()
    assert all(9.99 <= price <= 999.99 for price in prices), f"Found price values outside [9.99, 999.99]: {[p for p in prices if p < 9.99 or p > 999.99]}"

    # Verify rating column respects min_value=0.0, max_value=5.0
    ratings = table.column("rating").to_pylist()
    assert all(0.0 <= rating <= 5.0 for rating in ratings), f"Found rating values outside [0.0, 5.0]: {[r for r in ratings if r < 0.0 or r > 5.0]}"

    # Verify percentage column respects min_value=0.0, max_value=100.0
    percentages = table.column("percentage").to_pylist()
    assert all(0.0 <= pct <= 100.0 for pct in percentages), f"Found percentage values outside [0.0, 100.0]: {[p for p in percentages if p < 0.0 or p > 100.0]}"


def test_us41_combined_faker_and_ranges(tmp_path: Path) -> None:
    """
    Test combining Faker rules (AC 1) and numeric ranges (AC 2) in a single table.
    """
    schema = ExperimentSchema(
        name="combined_test",
        description=None,
        tables=[
            TableSchema(
                name="products",
                target_rows=50,
                columns=[
                    ColumnSchema(name="id", data_type="INT", is_unique=True),
                    ColumnSchema(name="name", data_type="VARCHAR", faker_rule="company", varchar_length=100),
                    ColumnSchema(name="price", data_type="FLOAT", min_value=10.0, max_value=1000.0),
                    ColumnSchema(name="stock", data_type="INT", min_value=0, max_value=100),
                ],
            )
        ],
    )

    generator = ExperimentGenerator(batch_size=50)
    result = generator.generate(
        GenerationRequest(schema=schema, output_root=tmp_path / "out", seed=789),
    )

    # Read generated data
    table = pq.read_table(result.tables[0].files[0])
    assert table.num_rows == 50

    # Verify Faker rule for name
    names = table.column("name").to_pylist()
    assert all(name and len(name) > 0 for name in names)

    # Verify numeric ranges
    prices = table.column("price").to_pylist()
    assert all(10.0 <= price <= 1000.0 for price in prices)

    stocks = table.column("stock").to_pylist()
    assert all(0 <= stock <= 100 for stock in stocks)
