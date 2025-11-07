from collections import defaultdict
from pathlib import Path
import random

import pyarrow.parquet as pq
import pytest
from faker import Faker

from dw_simulator.generator import ExperimentGenerator, GenerationRequest, GenerationError
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
