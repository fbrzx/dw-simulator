"""
Synthetic data generation engine for DW Simulator experiments.

The generator consumes validated ExperimentSchema objects, produces synthetic
records using Faker/numpy, and writes compressed Parquet batches to the local
filesystem (which can later be uploaded to LocalStack S3 by the data-loader
service). It focuses on deterministic, constraint-aware generation to unblock
US 2.1 without pulling in the heavier SDV stack yet.
"""

from __future__ import annotations

import random
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict

import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

from .config import get_data_root
from .schema import (
    ColumnSchema,
    DataType,
    DistributionType,
    ExperimentSchema,
    TableSchema,
)


class GenerationError(RuntimeError):
    """Raised when generation fails for any table/column."""


@dataclass(frozen=True)
class TableGenerationResult:
    table_name: str
    row_count: int
    files: list[Path]


@dataclass(frozen=True)
class GenerationResult:
    experiment_name: str
    output_dir: Path
    tables: list[TableGenerationResult]


@dataclass(frozen=True)
class GenerationRequest:
    schema: ExperimentSchema
    output_root: Path | None = None
    row_overrides: Dict[str, int] | None = None
    seed: int | None = None


class ExperimentGenerator:
    """Generates synthetic data for experiment schemas."""

    def __init__(self, batch_size: int = 10_000, faker_locale: str = "en_US") -> None:
        self.batch_size = batch_size
        self.faker_locale = faker_locale

    def generate(self, request: GenerationRequest) -> GenerationResult:
        schema = request.schema
        data_root = get_data_root()
        output_dir = request.output_root or data_root / "generated" / schema.name / str(int(time.time()))
        output_dir.mkdir(parents=True, exist_ok=True)

        seed = request.seed if request.seed is not None else random.randrange(0, 10**6)
        rng = random.Random(seed)
        faker = Faker(self.faker_locale)
        faker.seed_instance(seed)

        tables: list[TableGenerationResult] = []
        overrides = {k.lower(): v for k, v in (request.row_overrides or {}).items()}

        for table_schema in schema.tables:
            target_rows = overrides.get(table_schema.name.lower(), table_schema.target_rows)
            if target_rows <= 0:
                raise GenerationError(f"Target rows for table '{table_schema.name}' must be > 0.")

            table_dir = output_dir / table_schema.name
            table_dir.mkdir(parents=True, exist_ok=True)
            files = self._generate_table(table_schema, target_rows, table_dir, rng, faker)
            tables.append(TableGenerationResult(table_name=table_schema.name, row_count=target_rows, files=files))

        return GenerationResult(experiment_name=schema.name, output_dir=output_dir, tables=tables)

    # Internal helpers -----------------------------------------------------

    def _generate_table(
        self,
        table_schema: TableSchema,
        target_rows: int,
        output_dir: Path,
        rng: random.Random,
        faker: Faker,
    ) -> list[Path]:
        files: list[Path] = []
        unique_values: dict[str, set[Any]] = defaultdict(set)
        next_unique_int: dict[str, int] = defaultdict(int)

        # Initialize surrogate key columns (_row_id) to start at 1 instead of 0
        for column_schema in table_schema.columns:
            if column_schema.name == "_row_id" and column_schema.is_unique:
                next_unique_int[column_schema.name] = 1

        rows_remaining = target_rows
        batch_index = 0
        while rows_remaining > 0:
            batch_size = min(self.batch_size, rows_remaining)
            rows_remaining -= batch_size
            records: list[dict[str, Any]] = []
            for _ in range(batch_size):
                row: dict[str, Any] = {}
                for column_schema in table_schema.columns:
                    value = self._generate_value(
                        column_schema=column_schema,
                        rng=rng,
                        faker=faker,
                        unique_values=unique_values,
                        next_unique_int=next_unique_int,
                    )
                    row[column_schema.name] = value
                records.append(row)

            table = pa.Table.from_pylist(records)
            file_path = output_dir / f"batch-{batch_index:05d}.parquet"
            pq.write_table(table, file_path, compression="snappy")
            files.append(file_path)
            batch_index += 1
        return files

    def _generate_value(
        self,
        column_schema: ColumnSchema,
        rng: random.Random,
        faker: Faker,
        unique_values: dict[str, set[Any]],
        next_unique_int: dict[str, int],
    ) -> Any:
        if not column_schema.required and rng.random() < 0.05:
            return None

        data_type = column_schema.data_type
        generator = {
            DataType.INT: self._generate_int,
            DataType.FLOAT: self._generate_float,
            DataType.BOOLEAN: lambda: rng.random() < 0.5,
            DataType.DATE: self._generate_date,
            DataType.VARCHAR: self._generate_string,
        }.get(data_type)

        if generator is None:
            raise GenerationError(f"Unsupported data type '{data_type}' for column '{column_schema.name}'.")

        attempts = 0
        while True:
            value = generator(column_schema, rng, faker, next_unique_int)
            if column_schema.is_unique:
                bucket = unique_values[column_schema.name]
                if value in bucket:
                    attempts += 1
                    if attempts > 1000:
                        raise GenerationError(f"Unable to produce unique values for column '{column_schema.name}'.")
                    continue
                bucket.add(value)
            return value

    def _generate_int(
        self,
        column_schema: ColumnSchema,
        rng: random.Random,
        faker: Faker,
        next_unique_int: dict[str, int],
    ) -> int:
        if column_schema.is_unique:
            next_value = next_unique_int[column_schema.name]
            next_unique_int[column_schema.name] += 1
            return next_value

        if column_schema.distribution is not None:
            return self._generate_numeric_with_distribution(column_schema, rng, as_int=True)

        low = int(column_schema.min_value) if column_schema.min_value is not None else 0
        high = int(column_schema.max_value) if column_schema.max_value is not None else 1_000_000
        return rng.randint(low, high)

    def _generate_float(
        self,
        column_schema: ColumnSchema,
        rng: random.Random,
        faker: Faker,
        next_unique_int: dict[str, int],
    ) -> float:
        if column_schema.is_unique:
            next_value = float(next_unique_int[column_schema.name])
            next_unique_int[column_schema.name] += 1
            return next_value

        if column_schema.distribution is not None:
            return self._generate_numeric_with_distribution(column_schema, rng, as_int=False)

        low = column_schema.min_value if column_schema.min_value is not None else 0.0
        high = column_schema.max_value if column_schema.max_value is not None else 1_000_000.0
        return rng.uniform(low, high)

    def _generate_date(
        self,
        column_schema: ColumnSchema,
        rng: random.Random,
        faker: Faker,
        next_unique_int: dict[str, int],
    ) -> date:
        start = column_schema.date_start or date(2020, 1, 1)
        end = column_schema.date_end or date(2025, 12, 31)
        delta_days = (end - start).days
        if delta_days <= 0:
            return start
        offset = rng.randint(0, delta_days)
        return start + timedelta(days=offset)

    def _generate_string(
        self,
        column_schema: ColumnSchema,
        rng: random.Random,
        faker: Faker,
        next_unique_int: dict[str, int],
    ) -> str:
        max_length = column_schema.varchar_length or 255
        if column_schema.faker_rule:
            value = self._run_faker_rule(faker, column_schema.faker_rule)
        else:
            value = faker.word()
        if len(value) > max_length:
            value = value[:max_length]
        return value

    def _run_faker_rule(self, faker: Faker, rule: str) -> str:
        target = faker
        for part in rule.split("."):
            if not hasattr(target, part):
                raise GenerationError(f"Invalid Faker rule '{rule}'.")
            target = getattr(target, part)
        if callable(target):
            return str(target())
        return str(target)

    def _generate_numeric_with_distribution(
        self,
        column_schema: ColumnSchema,
        rng: random.Random,
        *,
        as_int: bool,
    ) -> int | float:
        config = column_schema.distribution
        if config is None:
            raise GenerationError(
                f"Distribution configuration missing for column '{column_schema.name}'."
            )

        low, high = self._resolve_numeric_bounds(
            column_schema, as_int=as_int, distribution_type=config.type
        )

        if config.type == DistributionType.NORMAL:
            sample = rng.gauss(config.parameters["mean"], config.parameters["stddev"])
        elif config.type == DistributionType.EXPONENTIAL:
            sample = rng.expovariate(config.parameters["lambda"])
        elif config.type == DistributionType.BETA:
            beta_value = rng.betavariate(
                config.parameters["alpha"], config.parameters["beta"]
            )
            sample = low + (high - low) * beta_value
        else:
            raise GenerationError(
                f"Unsupported distribution type '{config.type}' for column '{column_schema.name}'."
            )

        sample = self._clamp_numeric_sample(sample, low=low, high=high)

        if as_int:
            int_value = int(round(sample))
            if int_value < int(low):
                return int(low)
            if int_value > int(high):
                return int(high)
            return int_value

        return sample

    @staticmethod
    def _resolve_numeric_bounds(
        column_schema: ColumnSchema,
        *,
        as_int: bool,
        distribution_type: str,
    ) -> tuple[float, float]:
        if distribution_type == DistributionType.BETA:
            default_low = 0 if as_int else 0.0
            default_high = default_low + 1
        else:
            default_low = 0 if as_int else 0.0
            default_high = 1_000_000 if as_int else 1_000_000.0

        low = column_schema.min_value if column_schema.min_value is not None else default_low
        high = column_schema.max_value if column_schema.max_value is not None else default_high

        if as_int:
            low = float(int(low))
            high = float(int(high))

        if high < low:
            high = low

        return float(low), float(high)

    @staticmethod
    def _clamp_numeric_sample(sample: float, *, low: float, high: float) -> float:
        if sample < low:
            return low
        if sample > high:
            return high
        return sample


__all__ = [
    "ExperimentGenerator",
    "GenerationRequest",
    "GenerationResult",
    "TableGenerationResult",
    "GenerationError",
]
