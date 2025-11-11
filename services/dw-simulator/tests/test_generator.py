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
    table_schema = TableSchema(name="test_table", target_rows=10, columns=[column])
    value = generator._generate_value(
        column_schema=column,
        table_schema=table_schema,
        rng=rng,
        faker=faker,
        unique_values=defaultdict(set),
        next_unique_int=defaultdict(int),
        generated_values={},
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
    table_schema = TableSchema(name="test_table", target_rows=10, columns=[column])

    values = [
        generator._generate_value(
            column_schema=column,
            table_schema=table_schema,
            rng=rng,
            faker=faker,
            unique_values=defaultdict(set),
            next_unique_int=defaultdict(int),
            generated_values={},
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
    table_schema = TableSchema(name="test_table", target_rows=10, columns=[column])

    values = [
        generator._generate_value(
            column_schema=column,
            table_schema=table_schema,
            rng=rng,
            faker=faker,
            unique_values=defaultdict(set),
            next_unique_int=defaultdict(int),
            generated_values={},
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
    table_schema = TableSchema(name="test_table", target_rows=10, columns=[column])

    rng = random.Random(999)
    values = [
        generator._generate_value(
            column_schema=column,
            table_schema=table_schema,
            rng=rng,
            faker=faker,
            unique_values=defaultdict(set),
            next_unique_int=defaultdict(int),
            generated_values={},
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


# US 6.2 Foreign Key Generation Tests


def test_generator_foreign_key_basic_relationship(tmp_path: Path) -> None:
    """Test that FK values are sampled from parent table's referenced column."""
    from dw_simulator.schema import ForeignKeyConfig

    schema = ExperimentSchema(
        name="fk_basic_test",
        description=None,
        tables=[
            TableSchema(
                name="customers",
                target_rows=10,
                columns=[
                    ColumnSchema(name="customer_id", data_type="INT", is_unique=True),
                    ColumnSchema(name="name", data_type="VARCHAR", faker_rule="name", varchar_length=50),
                ],
            ),
            TableSchema(
                name="orders",
                target_rows=20,
                columns=[
                    ColumnSchema(name="order_id", data_type="INT", is_unique=True),
                    ColumnSchema(
                        name="customer_id",
                        data_type="INT",
                        required=True,
                        foreign_key=ForeignKeyConfig(
                            references_table="customers",
                            references_column="customer_id",
                        ),
                    ),
                    ColumnSchema(name="amount", data_type="FLOAT", min_value=1.0, max_value=1000.0),
                ],
            ),
        ],
    )

    generator = ExperimentGenerator(batch_size=50)
    result = generator.generate(
        GenerationRequest(schema=schema, output_root=tmp_path / "out", seed=42),
    )

    # Read generated data
    customers_table = pq.read_table(result.tables[0].files[0])
    orders_table = pq.read_table(result.tables[1].files[0])

    # Get customer IDs from parent table
    customer_ids = set(customers_table.column("customer_id").to_pylist())
    assert len(customer_ids) == 10

    # Get FK values from child table
    order_customer_ids = orders_table.column("customer_id").to_pylist()
    assert len(order_customer_ids) == 20

    # Verify all FK values reference valid parent table values
    for fk_value in order_customer_ids:
        assert fk_value in customer_ids, f"FK value {fk_value} not found in parent table"


def test_generator_foreign_key_nullable(tmp_path: Path) -> None:
    """Test that nullable FKs can generate NULL values."""
    from dw_simulator.schema import ForeignKeyConfig

    schema = ExperimentSchema(
        name="fk_nullable_test",
        description=None,
        tables=[
            TableSchema(
                name="departments",
                target_rows=5,
                columns=[
                    ColumnSchema(name="dept_id", data_type="INT", is_unique=True),
                    ColumnSchema(name="dept_name", data_type="VARCHAR", varchar_length=50),
                ],
            ),
            TableSchema(
                name="employees",
                target_rows=100,
                columns=[
                    ColumnSchema(name="emp_id", data_type="INT", is_unique=True),
                    ColumnSchema(name="name", data_type="VARCHAR", faker_rule="name", varchar_length=50),
                    ColumnSchema(
                        name="dept_id",
                        data_type="INT",
                        required=False,  # Nullable FK
                        foreign_key=ForeignKeyConfig(
                            references_table="departments",
                            references_column="dept_id",
                            nullable=True,
                        ),
                    ),
                ],
            ),
        ],
    )

    generator = ExperimentGenerator(batch_size=100)
    result = generator.generate(
        GenerationRequest(schema=schema, output_root=tmp_path / "out", seed=123),
    )

    # Read generated data
    departments_table = pq.read_table(result.tables[0].files[0])
    employees_table = pq.read_table(result.tables[1].files[0])

    # Get department IDs from parent table
    dept_ids = set(departments_table.column("dept_id").to_pylist())
    assert len(dept_ids) == 5

    # Get FK values from child table
    emp_dept_ids = employees_table.column("dept_id").to_pylist()
    assert len(emp_dept_ids) == 100

    # Verify some NULL values exist (nullable FK should have ~10% NULL rate)
    null_count = sum(1 for val in emp_dept_ids if val is None)
    assert null_count > 0, "Nullable FK should produce some NULL values"

    # Verify non-NULL FK values reference valid parent table values
    non_null_fks = [val for val in emp_dept_ids if val is not None]
    for fk_value in non_null_fks:
        assert fk_value in dept_ids, f"FK value {fk_value} not found in parent table"


def test_generator_foreign_key_multi_level_chain(tmp_path: Path) -> None:
    """Test FK generation with multi-level dependency chain (A -> B -> C)."""
    from dw_simulator.schema import ForeignKeyConfig

    schema = ExperimentSchema(
        name="fk_chain_test",
        description=None,
        tables=[
            TableSchema(
                name="regions",
                target_rows=3,
                columns=[
                    ColumnSchema(name="region_id", data_type="INT", is_unique=True),
                    ColumnSchema(name="region_name", data_type="VARCHAR", varchar_length=50),
                ],
            ),
            TableSchema(
                name="stores",
                target_rows=10,
                columns=[
                    ColumnSchema(name="store_id", data_type="INT", is_unique=True),
                    ColumnSchema(name="store_name", data_type="VARCHAR", varchar_length=50),
                    ColumnSchema(
                        name="region_id",
                        data_type="INT",
                        required=True,
                        foreign_key=ForeignKeyConfig(
                            references_table="regions",
                            references_column="region_id",
                        ),
                    ),
                ],
            ),
            TableSchema(
                name="sales",
                target_rows=50,
                columns=[
                    ColumnSchema(name="sale_id", data_type="INT", is_unique=True),
                    ColumnSchema(
                        name="store_id",
                        data_type="INT",
                        required=True,
                        foreign_key=ForeignKeyConfig(
                            references_table="stores",
                            references_column="store_id",
                        ),
                    ),
                    ColumnSchema(name="amount", data_type="FLOAT", min_value=1.0, max_value=1000.0),
                ],
            ),
        ],
    )

    generator = ExperimentGenerator(batch_size=100)
    result = generator.generate(
        GenerationRequest(schema=schema, output_root=tmp_path / "out", seed=999),
    )

    # Read generated data
    regions_table = pq.read_table(result.tables[0].files[0])
    stores_table = pq.read_table(result.tables[1].files[0])
    sales_table = pq.read_table(result.tables[2].files[0])

    # Verify FK relationships
    region_ids = set(regions_table.column("region_id").to_pylist())
    store_ids = set(stores_table.column("store_id").to_pylist())
    store_region_ids = stores_table.column("region_id").to_pylist()
    sale_store_ids = sales_table.column("store_id").to_pylist()

    # Verify stores reference valid regions
    for fk_value in store_region_ids:
        assert fk_value in region_ids

    # Verify sales reference valid stores
    for fk_value in sale_store_ids:
        assert fk_value in store_ids


def test_generator_foreign_key_multiple_fks_in_one_table(tmp_path: Path) -> None:
    """Test table with multiple FK columns referencing different parent tables."""
    from dw_simulator.schema import ForeignKeyConfig

    schema = ExperimentSchema(
        name="fk_multiple_test",
        description=None,
        tables=[
            TableSchema(
                name="customers",
                target_rows=10,
                columns=[
                    ColumnSchema(name="customer_id", data_type="INT", is_unique=True),
                    ColumnSchema(name="name", data_type="VARCHAR", varchar_length=50),
                ],
            ),
            TableSchema(
                name="products",
                target_rows=5,
                columns=[
                    ColumnSchema(name="product_id", data_type="INT", is_unique=True),
                    ColumnSchema(name="name", data_type="VARCHAR", varchar_length=50),
                ],
            ),
            TableSchema(
                name="orders",
                target_rows=30,
                columns=[
                    ColumnSchema(name="order_id", data_type="INT", is_unique=True),
                    ColumnSchema(
                        name="customer_id",
                        data_type="INT",
                        required=True,
                        foreign_key=ForeignKeyConfig(
                            references_table="customers",
                            references_column="customer_id",
                        ),
                    ),
                    ColumnSchema(
                        name="product_id",
                        data_type="INT",
                        required=True,
                        foreign_key=ForeignKeyConfig(
                            references_table="products",
                            references_column="product_id",
                        ),
                    ),
                    ColumnSchema(name="quantity", data_type="INT", min_value=1, max_value=10),
                ],
            ),
        ],
    )

    generator = ExperimentGenerator(batch_size=100)
    result = generator.generate(
        GenerationRequest(schema=schema, output_root=tmp_path / "out", seed=555),
    )

    # Read generated data
    customers_table = pq.read_table(result.tables[0].files[0])
    products_table = pq.read_table(result.tables[1].files[0])
    orders_table = pq.read_table(result.tables[2].files[0])

    # Get parent table values
    customer_ids = set(customers_table.column("customer_id").to_pylist())
    product_ids = set(products_table.column("product_id").to_pylist())

    # Get FK values from orders
    order_customer_ids = orders_table.column("customer_id").to_pylist()
    order_product_ids = orders_table.column("product_id").to_pylist()

    # Verify both FKs reference valid parent values
    for fk_value in order_customer_ids:
        assert fk_value in customer_ids

    for fk_value in order_product_ids:
        assert fk_value in product_ids


def test_generator_topological_sort_respects_dependencies(tmp_path: Path) -> None:
    """Test that topological sort generates parent tables before child tables."""
    from dw_simulator.schema import ForeignKeyConfig

    schema = ExperimentSchema(
        name="fk_order_test",
        description=None,
        tables=[
            # Define tables in reverse dependency order to test sorting
            TableSchema(
                name="line_items",
                target_rows=10,
                columns=[
                    ColumnSchema(name="line_id", data_type="INT", is_unique=True),
                    ColumnSchema(
                        name="order_id",
                        data_type="INT",
                        required=True,
                        foreign_key=ForeignKeyConfig(
                            references_table="orders",
                            references_column="order_id",
                        ),
                    ),
                ],
            ),
            TableSchema(
                name="orders",
                target_rows=5,
                columns=[
                    ColumnSchema(name="order_id", data_type="INT", is_unique=True),
                    ColumnSchema(
                        name="customer_id",
                        data_type="INT",
                        required=True,
                        foreign_key=ForeignKeyConfig(
                            references_table="customers",
                            references_column="customer_id",
                        ),
                    ),
                ],
            ),
            TableSchema(
                name="customers",
                target_rows=3,
                columns=[
                    ColumnSchema(name="customer_id", data_type="INT", is_unique=True),
                    ColumnSchema(name="name", data_type="VARCHAR", varchar_length=50),
                ],
            ),
        ],
    )

    generator = ExperimentGenerator(batch_size=100)
    result = generator.generate(
        GenerationRequest(schema=schema, output_root=tmp_path / "out", seed=777),
    )

    # Verify generation order in result (should be customers, orders, line_items)
    assert result.tables[0].table_name == "customers"
    assert result.tables[1].table_name == "orders"
    assert result.tables[2].table_name == "line_items"

    # Verify FK relationships are valid
    customers_table = pq.read_table(result.tables[0].files[0])
    orders_table = pq.read_table(result.tables[1].files[0])
    line_items_table = pq.read_table(result.tables[2].files[0])

    customer_ids = set(customers_table.column("customer_id").to_pylist())
    order_ids = set(orders_table.column("order_id").to_pylist())

    order_customer_ids = orders_table.column("customer_id").to_pylist()
    line_order_ids = line_items_table.column("order_id").to_pylist()

    for fk_value in order_customer_ids:
        assert fk_value in customer_ids

    for fk_value in line_order_ids:
        assert fk_value in order_ids


def test_generator_foreign_key_deterministic_seeding(tmp_path: Path) -> None:
    """Test that FK generation is deterministic with same seed."""
    from dw_simulator.schema import ForeignKeyConfig

    schema = ExperimentSchema(
        name="fk_deterministic_test",
        description=None,
        tables=[
            TableSchema(
                name="users",
                target_rows=5,
                columns=[
                    ColumnSchema(name="user_id", data_type="INT", is_unique=True),
                    ColumnSchema(name="name", data_type="VARCHAR", varchar_length=50),
                ],
            ),
            TableSchema(
                name="posts",
                target_rows=20,
                columns=[
                    ColumnSchema(name="post_id", data_type="INT", is_unique=True),
                    ColumnSchema(
                        name="user_id",
                        data_type="INT",
                        required=True,
                        foreign_key=ForeignKeyConfig(
                            references_table="users",
                            references_column="user_id",
                        ),
                    ),
                    ColumnSchema(name="content", data_type="VARCHAR", varchar_length=100),
                ],
            ),
        ],
    )

    generator = ExperimentGenerator(batch_size=100)

    # Generate twice with same seed
    result1 = generator.generate(
        GenerationRequest(schema=schema, output_root=tmp_path / "run1", seed=12345),
    )
    result2 = generator.generate(
        GenerationRequest(schema=schema, output_root=tmp_path / "run2", seed=12345),
    )

    # Read generated data
    posts1 = pq.read_table(result1.tables[1].files[0])
    posts2 = pq.read_table(result2.tables[1].files[0])

    # FK values should be identical across runs with same seed
    fk_values1 = posts1.column("user_id").to_pylist()
    fk_values2 = posts2.column("user_id").to_pylist()

    assert fk_values1 == fk_values2, "FK generation should be deterministic with same seed"


def test_generator_skip_table_with_target_rows_zero(tmp_path: Path) -> None:
    """
    Test that tables with target_rows=0 are skipped during generation.
    This allows referencing existing data from previous experiments.
    """
    schema = ExperimentSchema(
        name="skip_table_test",
        description=None,
        tables=[
            TableSchema(
                name="existing_table",
                target_rows=0,  # Skip generation for this table
                columns=[
                    ColumnSchema(name="id", data_type="INT", is_unique=True),
                    ColumnSchema(name="name", data_type="VARCHAR", varchar_length=50),
                ],
            ),
            TableSchema(
                name="new_table",
                target_rows=10,  # Generate data for this table
                columns=[
                    ColumnSchema(name="id", data_type="INT", is_unique=True),
                    ColumnSchema(name="value", data_type="INT", min_value=1, max_value=100),
                ],
            ),
        ],
    )

    generator = ExperimentGenerator(batch_size=100)
    result = generator.generate(
        GenerationRequest(schema=schema, output_root=tmp_path / "out", seed=42),
    )

    # Verify only one table was generated (new_table)
    assert len(result.tables) == 1
    assert result.tables[0].table_name == "new_table"
    assert result.tables[0].row_count == 10

    # Verify existing_table directory was not created
    existing_table_dir = result.output_dir / "existing_table"
    assert not existing_table_dir.exists()

    # Verify new_table was generated correctly
    new_table_dir = result.output_dir / "new_table"
    assert new_table_dir.exists()
    assert len(result.tables[0].files) > 0


def test_generator_override_to_zero_rows(tmp_path: Path) -> None:
    """
    Test that row_overrides can be used to set target_rows to 0,
    skipping generation for a table at runtime.
    """
    schema = ExperimentSchema(
        name="override_zero_test",
        description=None,
        tables=[
            TableSchema(
                name="table_a",
                target_rows=50,  # Schema says generate 50 rows
                columns=[
                    ColumnSchema(name="id", data_type="INT", is_unique=True),
                ],
            ),
            TableSchema(
                name="table_b",
                target_rows=20,
                columns=[
                    ColumnSchema(name="id", data_type="INT", is_unique=True),
                ],
            ),
        ],
    )

    generator = ExperimentGenerator(batch_size=100)
    result = generator.generate(
        GenerationRequest(
            schema=schema,
            output_root=tmp_path / "out",
            row_overrides={"table_a": 0},  # Override to skip table_a
            seed=123,
        ),
    )

    # Verify only table_b was generated
    assert len(result.tables) == 1
    assert result.tables[0].table_name == "table_b"
    assert result.tables[0].row_count == 20

    # Verify table_a was skipped
    table_a_dir = result.output_dir / "table_a"
    assert not table_a_dir.exists()
