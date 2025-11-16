"""
Synthetic data generation engine for DW Simulator experiments.

The generator consumes validated ExperimentSchema objects, produces synthetic
records using Faker/numpy, and writes compressed Parquet batches to the local
filesystem (which can later be uploaded to LocalStack S3 by the data-loader
service). It focuses on deterministic, constraint-aware generation to unblock
US 2.1 without pulling in the heavier SDV stack yet.
"""

from __future__ import annotations

import multiprocessing
import os
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


@dataclass(frozen=True)
class BatchGenerationTask:
    """Task for generating a single batch of rows."""
    table_schema: TableSchema
    batch_index: int
    batch_size: int
    output_path: Path
    seed: int
    faker_locale: str
    generated_values: dict[str, dict[str, list[Any]]]
    # For unique columns, pass the starting index for this batch
    unique_int_offsets: dict[str, int]


@dataclass(frozen=True)
class BatchGenerationResult:
    """Result from generating a single batch."""
    batch_index: int
    output_path: Path
    unique_column_values: dict[str, list[Any]]


def _generate_batch_worker(task: BatchGenerationTask) -> BatchGenerationResult:
    """
    Worker function for parallel batch generation.

    This is a module-level function (not a method) so it can be pickled
    for multiprocessing.
    """
    # Create fresh RNG and Faker instances for this batch with deterministic seed
    rng = random.Random(task.seed)
    faker = Faker(task.faker_locale)
    faker.seed_instance(task.seed)

    # Track unique values generated in this batch
    unique_values: dict[str, set[Any]] = defaultdict(set)
    next_unique_int: dict[str, int] = dict(task.unique_int_offsets)
    unique_column_values: dict[str, list[Any]] = {}

    records: list[dict[str, Any]] = []
    for _ in range(task.batch_size):
        row: dict[str, Any] = {}
        for column_schema in task.table_schema.columns:
            value = _generate_value_worker(
                column_schema=column_schema,
                table_schema=task.table_schema,
                rng=rng,
                faker=faker,
                unique_values=unique_values,
                next_unique_int=next_unique_int,
                generated_values=task.generated_values,
            )
            row[column_schema.name] = value

            # Track unique column values for FK referencing
            if column_schema.is_unique and value is not None:
                if column_schema.name not in unique_column_values:
                    unique_column_values[column_schema.name] = []
                unique_column_values[column_schema.name].append(value)

        records.append(row)

    # Write Parquet file
    table = pa.Table.from_pylist(records)
    pq.write_table(table, task.output_path, compression="snappy")

    return BatchGenerationResult(
        batch_index=task.batch_index,
        output_path=task.output_path,
        unique_column_values=unique_column_values,
    )


def _generate_value_worker(
    column_schema: ColumnSchema,
    table_schema: TableSchema,
    rng: random.Random,
    faker: Faker,
    unique_values: dict[str, set[Any]],
    next_unique_int: dict[str, int],
    generated_values: dict[str, dict[str, list[Any]]],
) -> Any:
    """
    Generate a single value for a column (worker version).

    This is a simplified version of _generate_value that can be used in worker processes.
    """
    # Handle FK columns by sampling from parent table
    if column_schema.foreign_key is not None:
        fk_config = column_schema.foreign_key
        is_nullable = (not column_schema.required) or (fk_config.nullable is True)

        # Nullable FKs have 10% chance of being NULL
        if is_nullable and rng.random() < 0.10:
            return None

        # Get parent table's generated values
        ref_table = fk_config.references_table.lower()
        ref_column = fk_config.references_column

        if ref_table not in generated_values:
            raise GenerationError(
                f"Table '{table_schema.name}' column '{column_schema.name}' references table '{fk_config.references_table}', "
                f"but that table has not been generated yet."
            )

        parent_values = generated_values[ref_table].get(ref_column)
        if not parent_values:
            raise GenerationError(
                f"Table '{table_schema.name}' column '{column_schema.name}' references "
                f"'{ref_table}.{ref_column}', but no values were generated for that column."
            )

        return rng.choice(parent_values)

    # Handle nullable columns
    if not column_schema.required and rng.random() < 0.05:
        return None

    # Generate value based on data type
    data_type = column_schema.data_type

    if data_type == DataType.INT:
        if column_schema.is_unique:
            value = next_unique_int[column_schema.name]
            next_unique_int[column_schema.name] += 1
            return value

        if column_schema.distribution is not None:
            config = column_schema.distribution
            low = int(column_schema.min_value) if column_schema.min_value is not None else 0
            high = int(column_schema.max_value) if column_schema.max_value is not None else 1_000_000

            if config.type == DistributionType.NORMAL:
                sample = rng.gauss(config.parameters["mean"], config.parameters["stddev"])
            elif config.type == DistributionType.EXPONENTIAL:
                sample = rng.expovariate(config.parameters["lambda"])
            elif config.type == DistributionType.BETA:
                beta_value = rng.betavariate(config.parameters["alpha"], config.parameters["beta"])
                sample = low + (high - low) * beta_value
            else:
                sample = rng.randint(low, high)

            sample = max(low, min(high, sample))
            return int(round(sample))

        low = int(column_schema.min_value) if column_schema.min_value is not None else 0
        high = int(column_schema.max_value) if column_schema.max_value is not None else 1_000_000
        return rng.randint(low, high)

    elif data_type == DataType.FLOAT:
        if column_schema.is_unique:
            value = float(next_unique_int[column_schema.name])
            next_unique_int[column_schema.name] += 1
            return value

        if column_schema.distribution is not None:
            config = column_schema.distribution
            low = column_schema.min_value if column_schema.min_value is not None else 0.0
            high = column_schema.max_value if column_schema.max_value is not None else 1_000_000.0

            if config.type == DistributionType.NORMAL:
                sample = rng.gauss(config.parameters["mean"], config.parameters["stddev"])
            elif config.type == DistributionType.EXPONENTIAL:
                sample = rng.expovariate(config.parameters["lambda"])
            elif config.type == DistributionType.BETA:
                beta_value = rng.betavariate(config.parameters["alpha"], config.parameters["beta"])
                sample = low + (high - low) * beta_value
            else:
                sample = rng.uniform(low, high)

            return max(low, min(high, sample))

        low = column_schema.min_value if column_schema.min_value is not None else 0.0
        high = column_schema.max_value if column_schema.max_value is not None else 1_000_000.0
        return rng.uniform(low, high)

    elif data_type == DataType.BOOLEAN:
        return rng.random() < 0.5

    elif data_type == DataType.DATE:
        start = column_schema.date_start or date(2020, 1, 1)
        end = column_schema.date_end or date(2025, 12, 31)
        delta_days = (end - start).days

        if delta_days <= 0:
            return start

        if column_schema.is_unique:
            next_value = next_unique_int[column_schema.name]
            next_unique_int[column_schema.name] += 1
            if next_value > delta_days:
                raise GenerationError(
                    f"Unable to generate unique date for column '{column_schema.name}': "
                    f"requested more unique dates than available in date range."
                )
            return start + timedelta(days=next_value)

        offset = rng.randint(0, delta_days)
        return start + timedelta(days=offset)

    elif data_type == DataType.VARCHAR:
        max_length = column_schema.varchar_length or 255
        if column_schema.faker_rule:
            target = faker
            for part in column_schema.faker_rule.split("."):
                if not hasattr(target, part):
                    raise GenerationError(f"Invalid Faker rule '{column_schema.faker_rule}'.")
                target = getattr(target, part)
            if callable(target):
                value = str(target())
            else:
                value = str(target)
        else:
            value = faker.word()

        if len(value) > max_length:
            value = value[:max_length]

        # For unique strings, handle collisions
        if column_schema.is_unique:
            attempts = 0
            while value in unique_values[column_schema.name]:
                attempts += 1
                if attempts > 1000:
                    # Fallback to appending unique integer
                    value = f"{value}_{next_unique_int.get(column_schema.name, 0)}"
                    next_unique_int[column_schema.name] = next_unique_int.get(column_schema.name, 0) + 1
                    break
                value = faker.word() if not column_schema.faker_rule else str(target() if callable(target) else target)
                if len(value) > max_length:
                    value = value[:max_length]

            unique_values[column_schema.name].add(value)

        return value

    else:
        raise GenerationError(f"Unsupported data type '{data_type}' for column '{column_schema.name}'.")


class ExperimentGenerator:
    """Generates synthetic data for experiment schemas."""

    def __init__(
        self,
        batch_size: int = 10_000,
        faker_locale: str = "en_US",
        max_workers: int | None = None,
    ) -> None:
        self.batch_size = batch_size
        self.faker_locale = faker_locale
        # Default to cpu_count - 1, minimum of 1
        if max_workers is None:
            cpu_count = multiprocessing.cpu_count()
            max_workers = max(1, cpu_count - 1)
        self.max_workers = max_workers

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

        # Sort tables by FK dependencies (parent tables first)
        sorted_tables = self._topological_sort_tables(schema.tables)

        # Track generated values for FK sampling
        # Maps: table_name -> column_name -> list of generated unique values
        generated_values: dict[str, dict[str, list[Any]]] = {}

        for table_schema in sorted_tables:
            target_rows = overrides.get(table_schema.name.lower(), table_schema.target_rows)
            if target_rows < 0:
                raise GenerationError(f"Target rows for table '{table_schema.name}' must be >= 0.")

            # Skip generation for tables with target_rows = 0 (allows referencing existing data)
            if target_rows == 0:
                continue

            table_dir = output_dir / table_schema.name
            table_dir.mkdir(parents=True, exist_ok=True)
            files, unique_column_values = self._generate_table(
                table_schema, target_rows, table_dir, rng, faker, generated_values
            )
            tables.append(TableGenerationResult(table_name=table_schema.name, row_count=target_rows, files=files))

            # Store generated unique values for FK referencing (use lowercase for case-insensitive lookups)
            if unique_column_values:
                generated_values[table_schema.name.lower()] = unique_column_values

        return GenerationResult(experiment_name=schema.name, output_dir=output_dir, tables=tables)

    # Internal helpers -----------------------------------------------------

    def _topological_sort_tables(self, tables: list[TableSchema]) -> list[TableSchema]:
        """
        Sort tables by FK dependencies using topological sort.

        Returns tables in generation order (parent tables before children).
        Circular dependencies should have been detected during schema validation.
        """
        # Build dependency graph
        table_map = {t.name.lower(): t for t in tables}
        dependencies: dict[str, set[str]] = {t.name.lower(): set() for t in tables}

        for table in tables:
            table_name = table.name.lower()
            for col_name, fk_config in table.foreign_keys:
                # Find the column to check if FK is nullable
                column = next((c for c in table.columns if c.name == col_name), None)
                if column:
                    # Only add hard dependency if FK is required (not nullable)
                    is_nullable = (not column.required) or (fk_config.nullable is True)
                    if not is_nullable:
                        ref_table = fk_config.references_table.lower()
                        if ref_table in table_map:
                            dependencies[table_name].add(ref_table)

        # Kahn's algorithm for topological sort
        # in_degree[X] = number of dependencies X has (how many tables X depends on)
        in_degree = {name: len(deps) for name, deps in dependencies.items()}

        # Find all nodes with no dependencies (these are root tables)
        queue = [name for name, degree in in_degree.items() if degree == 0]
        sorted_names: list[str] = []

        while queue:
            # Sort queue for deterministic ordering
            queue.sort()
            current = queue.pop(0)
            sorted_names.append(current)

            # For each node that depends on current, decrement in-degree
            for table_name, deps in dependencies.items():
                if current in deps:
                    in_degree[table_name] -= 1
                    if in_degree[table_name] == 0:
                        queue.append(table_name)

        # Return tables in sorted order
        return [table_map[name] for name in sorted_names]

    def _generate_table(
        self,
        table_schema: TableSchema,
        target_rows: int,
        output_dir: Path,
        rng: random.Random,
        faker: Faker,
        generated_values: dict[str, dict[str, list[Any]]],
    ) -> tuple[list[Path], dict[str, list[Any]]]:
        """
        Generate synthetic data for a table using parallel batch generation.

        Args:
            generated_values: Previously generated unique values from parent tables for FK sampling

        Returns:
            Tuple of (parquet files, unique column values for this table)
        """
        # Calculate number of batches
        num_batches = (target_rows + self.batch_size - 1) // self.batch_size

        # Pre-calculate unique column offsets for each batch
        # This ensures deterministic unique value generation across parallel workers
        batch_unique_offsets: list[dict[str, int]] = []
        next_unique_int: dict[str, int] = defaultdict(int)

        # Initialize all unique numeric columns to start at 1 instead of 0
        # This ensures IDs are always positive (1, 2, 3, ...) for better FK compatibility
        for column_schema in table_schema.columns:
            if column_schema.is_unique and column_schema.data_type in (DataType.INT, DataType.FLOAT, DataType.DATE):
                next_unique_int[column_schema.name] = 1

        for batch_idx in range(num_batches):
            batch_offsets = {}
            for column_schema in table_schema.columns:
                if column_schema.is_unique and column_schema.data_type in (DataType.INT, DataType.FLOAT, DataType.DATE):
                    # Each batch gets a unique range of values
                    batch_offsets[column_schema.name] = next_unique_int[column_schema.name]
                    next_unique_int[column_schema.name] += self.batch_size
            batch_unique_offsets.append(batch_offsets)

        # Create tasks for parallel batch generation
        tasks: list[BatchGenerationTask] = []
        base_seed = rng.randint(0, 10**9)

        for batch_idx in range(num_batches):
            rows_in_batch = min(self.batch_size, target_rows - batch_idx * self.batch_size)
            output_path = output_dir / f"batch-{batch_idx:05d}.parquet"

            # Each batch gets a deterministic but different seed
            batch_seed = base_seed + batch_idx

            task = BatchGenerationTask(
                table_schema=table_schema,
                batch_index=batch_idx,
                batch_size=rows_in_batch,
                output_path=output_path,
                seed=batch_seed,
                faker_locale=self.faker_locale,
                generated_values=generated_values,
                unique_int_offsets=batch_unique_offsets[batch_idx],
            )
            tasks.append(task)

        # Use multiprocessing Pool to generate batches in parallel
        # For single-worker mode or small datasets, use sequential processing
        if self.max_workers == 1 or num_batches == 1:
            results = [_generate_batch_worker(task) for task in tasks]
        else:
            with multiprocessing.Pool(processes=self.max_workers) as pool:
                results = pool.map(_generate_batch_worker, tasks)

        # Aggregate results
        files: list[Path] = []
        unique_column_values: dict[str, list[Any]] = {}

        # Sort results by batch_index to maintain order
        results_sorted = sorted(results, key=lambda r: r.batch_index)

        for result in results_sorted:
            files.append(result.output_path)

            # Merge unique column values
            for col_name, values in result.unique_column_values.items():
                if col_name not in unique_column_values:
                    unique_column_values[col_name] = []
                unique_column_values[col_name].extend(values)

        return files, unique_column_values

    def _generate_value(
        self,
        column_schema: ColumnSchema,
        table_schema: TableSchema,
        rng: random.Random,
        faker: Faker,
        unique_values: dict[str, set[Any]],
        next_unique_int: dict[str, int],
        generated_values: dict[str, dict[str, list[Any]]],
    ) -> Any:
        # Handle FK columns by sampling from parent table
        if column_schema.foreign_key is not None:
            return self._generate_foreign_key_value(
                column_schema, table_schema, rng, generated_values
            )

        # Handle nullable columns
        if not column_schema.required and rng.random() < 0.05:
            return None

        data_type = column_schema.data_type
        generator = {
            DataType.INT: self._generate_int,
            DataType.FLOAT: self._generate_float,
            DataType.BOOLEAN: lambda col, r, f, n: r.random() < 0.5,
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

    def _generate_foreign_key_value(
        self,
        column_schema: ColumnSchema,
        table_schema: TableSchema,
        rng: random.Random,
        generated_values: dict[str, dict[str, list[Any]]],
    ) -> Any:
        """
        Generate a value for a FK column by sampling from the parent table's referenced column.

        Returns None if FK is nullable and random dice roll succeeds.
        """
        fk_config = column_schema.foreign_key
        if fk_config is None:
            raise GenerationError(f"Internal error: _generate_foreign_key_value called for non-FK column '{column_schema.name}'.")

        # Determine if FK is nullable
        is_nullable = (not column_schema.required) or (fk_config.nullable is True)

        # Nullable FKs have 10% chance of being NULL
        if is_nullable and rng.random() < 0.10:
            return None

        # Get parent table's generated values (use lowercase for case-insensitive lookups)
        ref_table = fk_config.references_table.lower()
        ref_column = fk_config.references_column

        if ref_table not in generated_values:
            raise GenerationError(
                f"Table '{table_schema.name}' column '{column_schema.name}' references table '{fk_config.references_table}', "
                f"but that table has not been generated yet. Check FK dependency order."
            )

        parent_values = generated_values[ref_table].get(ref_column)
        if not parent_values:
            raise GenerationError(
                f"Table '{table_schema.name}' column '{column_schema.name}' references "
                f"'{ref_table}.{ref_column}', but no values were generated for that column."
            )

        # Sample a random value from the parent table's column
        return rng.choice(parent_values)

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

        # For unique date columns, generate sequentially to avoid collisions
        if column_schema.is_unique:
            next_value = next_unique_int[column_schema.name]
            next_unique_int[column_schema.name] += 1
            # Ensure we don't exceed the date range
            if next_value > delta_days:
                raise GenerationError(
                    f"Unable to generate unique date for column '{column_schema.name}': "
                    f"requested more unique dates than available in date range "
                    f"({delta_days + 1} days from {start} to {end})."
                )
            return start + timedelta(days=next_value)

        # For non-unique dates, use random sampling
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
    "BatchGenerationTask",
    "BatchGenerationResult",
    "GenerationError",
]
