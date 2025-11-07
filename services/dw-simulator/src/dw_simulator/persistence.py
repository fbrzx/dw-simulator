"""
Persistence layer for experiment metadata and physical table creation.

Backed by SQLite (or any SQLAlchemy URL), this module stores experiment
definitions and materializes the corresponding tables in the local warehouse,
enforcing uniqueness/identifier guarantees laid out in docs/product-spec.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    inspect,
    select,
    text,
)
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from .config import get_target_db_url
from .schema import ColumnSchema, DataType, ExperimentSchema

import pyarrow.parquet as pq


class GenerationStatus(str, Enum):
    """Status values for generation runs."""
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ABORTED = "ABORTED"


class ExperimentAlreadyExistsError(RuntimeError):
    """Raised when attempting to create an experiment that already exists."""


class ExperimentMaterializationError(RuntimeError):
    """Raised when DDL generation fails to create backing tables."""


class ExperimentNotFoundError(RuntimeError):
    """Raised when delete/lookups reference a non-existent experiment."""


class GenerationAlreadyRunningError(RuntimeError):
    """Raised when attempting to start generation while one is already running."""


class GenerationRunNotFoundError(RuntimeError):
    """Raised when referencing a non-existent generation run."""


class QueryExecutionError(RuntimeError):
    """Raised when SQL query execution fails."""


class DataLoadError(RuntimeError):
    """Raised when loading Parquet data into warehouse tables fails."""


@dataclass(frozen=True)
class ExperimentMetadata:
    """Metadata view returned by the repository."""

    name: str
    description: str | None
    schema_json: str
    created_at: datetime


@dataclass(frozen=True)
class GenerationRunMetadata:
    """Metadata for a generation run."""

    id: int
    experiment_name: str
    status: GenerationStatus
    started_at: datetime
    completed_at: datetime | None
    row_counts: str  # JSON string
    output_path: str | None
    error_message: str | None
    seed: int | None


@dataclass(frozen=True)
class QueryResult:
    """Result of a SQL query execution."""

    columns: list[str]
    rows: list[tuple]
    row_count: int


class ExperimentPersistence:
    """Coordinates metadata storage plus warehouse table creation."""

    def __init__(self, connection_string: str | None = None) -> None:
        self.connection_string = connection_string or get_target_db_url()
        self.engine: Engine = create_engine(self.connection_string, future=True)
        self._metadata = MetaData()
        self._experiments = Table(
            "experiments",
            self._metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(255), nullable=False, unique=True),
            Column("description", Text),
            Column("schema_json", Text, nullable=False),
            Column("created_at", String(32), nullable=False),
        )
        self._experiment_tables = Table(
            "experiment_tables",
            self._metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("experiment_name", String(255), nullable=False),
            Column("table_name", String(255), nullable=False),
            Column("target_rows", Integer, nullable=False),
            UniqueConstraint("experiment_name", "table_name", name="uq_experiment_table"),
        )
        self._generation_runs = Table(
            "generation_runs",
            self._metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("experiment_name", String(255), nullable=False),
            Column("status", String(32), nullable=False),
            Column("started_at", String(32), nullable=False),
            Column("completed_at", String(32), nullable=True),
            Column("row_counts", Text, nullable=True),  # JSON
            Column("output_path", Text, nullable=True),
            Column("error_message", Text, nullable=True),
            Column("seed", Integer, nullable=True),
        )
        self._metadata.create_all(self.engine)

    # Public API -----------------------------------------------------------------

    def create_experiment(self, schema: ExperimentSchema) -> ExperimentMetadata:
        """Persist metadata and create the physical tables for the experiment."""

        try:
            with self.engine.begin() as conn:
                if self._experiment_exists(conn, schema.name):
                    raise ExperimentAlreadyExistsError(
                        f"Experiment '{schema.name}' already exists. Choose a unique name."
                    )

                created_at = datetime.now(timezone.utc).isoformat()
                schema_json = schema.model_dump_json()
                conn.execute(
                    self._experiments.insert().values(
                        name=schema.name,
                        description=schema.description,
                        schema_json=schema_json,
                        created_at=created_at,
                    )
                )
                for table_schema in schema.tables:
                    conn.execute(
                        self._experiment_tables.insert().values(
                            experiment_name= schema.name,
                            table_name=table_schema.name,
                            target_rows=table_schema.target_rows,
                        )
                    )
                self._create_data_tables(schema, conn)
        except ExperimentAlreadyExistsError:
            raise
        except SQLAlchemyError as exc:
            raise ExperimentMaterializationError(f"Failed to create experiment '{schema.name}': {exc}") from exc

        return ExperimentMetadata(
            name=schema.name,
            description=schema.description,
            schema_json=schema_json,
            created_at=datetime.fromisoformat(created_at),
        )

    def get_experiment_metadata(self, name: str) -> ExperimentMetadata | None:
        """Fetch experiment metadata, if present."""

        with self.engine.connect() as conn:
            row = conn.execute(
                select(
                    self._experiments.c.name,
                    self._experiments.c.description,
                    self._experiments.c.schema_json,
                    self._experiments.c.created_at,
                ).where(self._experiments.c.name == name)
            ).first()

        if not row:
            return None
        return ExperimentMetadata(
            name=row.name,
            description=row.description,
            schema_json=row.schema_json,
            created_at=datetime.fromisoformat(row.created_at),
        )

    def list_tables(self, experiment_name: str) -> list[str]:
        """Return the fully qualified table names materialized for the experiment."""

        inspector = inspect(self.engine)
        all_tables = inspector.get_table_names()
        prefix = normalize_identifier(experiment_name) + "__"
        return [table for table in all_tables if table.startswith(prefix)]

    def list_experiments(self) -> list[ExperimentMetadata]:
        """Return all experiment metadata rows."""

        with self.engine.connect() as conn:
            rows = conn.execute(
                select(
                    self._experiments.c.name,
                    self._experiments.c.description,
                    self._experiments.c.schema_json,
                    self._experiments.c.created_at,
                ).order_by(self._experiments.c.created_at.desc())
            ).all()

        return [
            ExperimentMetadata(
                name=row.name,
                description=row.description,
                schema_json=row.schema_json,
                created_at=datetime.fromisoformat(row.created_at),
            )
            for row in rows
        ]

    def delete_experiment(self, name: str) -> int:
        """Drop experiment tables and metadata. Returns number of dropped tables."""

        try:
            with self.engine.begin() as conn:
                if not self._experiment_exists(conn, name):
                    raise ExperimentNotFoundError(f"Experiment '{name}' does not exist.")

                table_rows = conn.execute(
                    select(self._experiment_tables.c.table_name).where(
                        self._experiment_tables.c.experiment_name == name
                    )
                ).all()
                physical_tables = [
                    self._physical_table_name(name, row.table_name) for row in table_rows
                ]

                inspector = inspect(conn)
                dropped = 0
                for table_name in physical_tables:
                    if inspector.has_table(table_name):
                        conn.exec_driver_sql(f'DROP TABLE IF EXISTS "{table_name}"')
                        dropped += 1

                conn.execute(
                    self._experiment_tables.delete().where(
                        self._experiment_tables.c.experiment_name == name
                    )
                )
                conn.execute(
                    self._generation_runs.delete().where(
                        self._generation_runs.c.experiment_name == name
                    )
                )
                conn.execute(self._experiments.delete().where(self._experiments.c.name == name))
        except ExperimentNotFoundError:
            raise
        except SQLAlchemyError as exc:
            raise ExperimentMaterializationError(f"Failed to delete experiment '{name}': {exc}") from exc

        return dropped

    def start_generation_run(
        self,
        experiment_name: str,
        output_path: str | None = None,
        seed: int | None = None,
    ) -> int:
        """
        Start a new generation run. Returns the run ID.
        Raises GenerationAlreadyRunningError if a run is already in progress.
        """
        try:
            with self.engine.begin() as conn:
                # Check if experiment exists
                if not self._experiment_exists(conn, experiment_name):
                    raise ExperimentNotFoundError(f"Experiment '{experiment_name}' does not exist.")

                # Check for concurrent runs (concurrent job guard)
                active_run = conn.execute(
                    select(self._generation_runs.c.id, self._generation_runs.c.started_at).where(
                        (self._generation_runs.c.experiment_name == experiment_name)
                        & (self._generation_runs.c.status == GenerationStatus.RUNNING.value)
                    )
                ).first()

                if active_run:
                    raise GenerationAlreadyRunningError(
                        f"Generation already running for experiment '{experiment_name}' "
                        f"(started at {active_run.started_at}, run_id={active_run.id}). "
                        f"Please wait for it to complete or abort it first."
                    )

                # Create new run record
                started_at = datetime.now(timezone.utc).isoformat()
                result = conn.execute(
                    self._generation_runs.insert().values(
                        experiment_name=experiment_name,
                        status=GenerationStatus.RUNNING.value,
                        started_at=started_at,
                        output_path=output_path,
                        seed=seed,
                    )
                )
                return result.lastrowid
        except (GenerationAlreadyRunningError, ExperimentNotFoundError):
            raise
        except SQLAlchemyError as exc:
            raise ExperimentMaterializationError(
                f"Failed to start generation run for '{experiment_name}': {exc}"
            ) from exc

    def complete_generation_run(self, run_id: int, row_counts: str) -> None:
        """Mark a generation run as completed with row count summary."""
        try:
            with self.engine.begin() as conn:
                completed_at = datetime.now(timezone.utc).isoformat()
                result = conn.execute(
                    self._generation_runs.update()
                    .where(self._generation_runs.c.id == run_id)
                    .values(
                        status=GenerationStatus.COMPLETED.value,
                        completed_at=completed_at,
                        row_counts=row_counts,
                    )
                )
                if result.rowcount == 0:
                    raise GenerationRunNotFoundError(f"Generation run {run_id} not found.")
        except GenerationRunNotFoundError:
            raise
        except SQLAlchemyError as exc:
            raise ExperimentMaterializationError(
                f"Failed to complete generation run {run_id}: {exc}"
            ) from exc

    def fail_generation_run(self, run_id: int, error_message: str) -> None:
        """Mark a generation run as failed with error details."""
        try:
            with self.engine.begin() as conn:
                completed_at = datetime.now(timezone.utc).isoformat()
                result = conn.execute(
                    self._generation_runs.update()
                    .where(self._generation_runs.c.id == run_id)
                    .values(
                        status=GenerationStatus.FAILED.value,
                        completed_at=completed_at,
                        error_message=error_message,
                    )
                )
                if result.rowcount == 0:
                    raise GenerationRunNotFoundError(f"Generation run {run_id} not found.")
        except GenerationRunNotFoundError:
            raise
        except SQLAlchemyError as exc:
            raise ExperimentMaterializationError(
                f"Failed to mark generation run {run_id} as failed: {exc}"
            ) from exc

    def get_generation_run(self, run_id: int) -> GenerationRunMetadata | None:
        """Fetch metadata for a specific generation run."""
        with self.engine.connect() as conn:
            row = conn.execute(
                select(
                    self._generation_runs.c.id,
                    self._generation_runs.c.experiment_name,
                    self._generation_runs.c.status,
                    self._generation_runs.c.started_at,
                    self._generation_runs.c.completed_at,
                    self._generation_runs.c.row_counts,
                    self._generation_runs.c.output_path,
                    self._generation_runs.c.error_message,
                    self._generation_runs.c.seed,
                ).where(self._generation_runs.c.id == run_id)
            ).first()

        if not row:
            return None

        return GenerationRunMetadata(
            id=row.id,
            experiment_name=row.experiment_name,
            status=GenerationStatus(row.status),
            started_at=datetime.fromisoformat(row.started_at),
            completed_at=datetime.fromisoformat(row.completed_at) if row.completed_at else None,
            row_counts=row.row_counts or "{}",
            output_path=row.output_path,
            error_message=row.error_message,
            seed=row.seed,
        )

    def list_generation_runs(self, experiment_name: str) -> list[GenerationRunMetadata]:
        """List all generation runs for an experiment, most recent first."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(
                    self._generation_runs.c.id,
                    self._generation_runs.c.experiment_name,
                    self._generation_runs.c.status,
                    self._generation_runs.c.started_at,
                    self._generation_runs.c.completed_at,
                    self._generation_runs.c.row_counts,
                    self._generation_runs.c.output_path,
                    self._generation_runs.c.error_message,
                    self._generation_runs.c.seed,
                )
                .where(self._generation_runs.c.experiment_name == experiment_name)
                .order_by(self._generation_runs.c.started_at.desc())
            ).all()

        return [
            GenerationRunMetadata(
                id=row.id,
                experiment_name=row.experiment_name,
                status=GenerationStatus(row.status),
                started_at=datetime.fromisoformat(row.started_at),
                completed_at=datetime.fromisoformat(row.completed_at) if row.completed_at else None,
                row_counts=row.row_counts or "{}",
                output_path=row.output_path,
                error_message=row.error_message,
                seed=row.seed,
            )
            for row in rows
        ]

    def reset_experiment(self, name: str) -> int:
        """
        Truncate all tables in an experiment without deleting the schema.
        Returns the number of tables reset.
        Raises GenerationAlreadyRunningError if a generation is currently active.
        """
        try:
            with self.engine.begin() as conn:
                if not self._experiment_exists(conn, name):
                    raise ExperimentNotFoundError(f"Experiment '{name}' does not exist.")

                # Check for concurrent generation runs (guard against reset during generation)
                active_run = conn.execute(
                    select(self._generation_runs.c.id, self._generation_runs.c.started_at).where(
                        (self._generation_runs.c.experiment_name == name)
                        & (self._generation_runs.c.status == GenerationStatus.RUNNING.value)
                    )
                ).first()

                if active_run:
                    raise GenerationAlreadyRunningError(
                        f"Cannot reset experiment '{name}' while generation is running "
                        f"(started at {active_run.started_at}, run_id={active_run.id}). "
                        f"Please wait for it to complete or abort it first."
                    )

                # Get all tables for this experiment
                table_rows = conn.execute(
                    select(self._experiment_tables.c.table_name).where(
                        self._experiment_tables.c.experiment_name == name
                    )
                ).all()
                physical_tables = [
                    self._physical_table_name(name, row.table_name) for row in table_rows
                ]

                inspector = inspect(conn)
                reset_count = 0
                for table_name in physical_tables:
                    if inspector.has_table(table_name):
                        conn.exec_driver_sql(f'DELETE FROM "{table_name}"')
                        reset_count += 1

        except (ExperimentNotFoundError, GenerationAlreadyRunningError):
            raise
        except SQLAlchemyError as exc:
            raise ExperimentMaterializationError(f"Failed to reset experiment '{name}': {exc}") from exc

        return reset_count

    def execute_query(self, sql: str) -> QueryResult:
        """
        Execute a SQL query and return the results.
        Raises QueryExecutionError if the query fails.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))

                # Get column names from keys()
                columns = list(result.keys())

                # Fetch all rows
                rows = result.fetchall()

                return QueryResult(
                    columns=columns,
                    rows=rows,
                    row_count=len(rows),
                )
        except Exception as exc:
            # Extract the error message for better user feedback
            error_msg = str(exc)
            # For syntax errors, try to provide line/position info if available
            raise QueryExecutionError(f"Query execution failed: {error_msg}") from exc

    def load_parquet_files_to_table(
        self,
        experiment_name: str,
        table_name: str,
        parquet_files: list[str | Path],
    ) -> int:
        """
        Load Parquet batches into the physical table backing an experiment table.

        Returns the number of rows inserted. Raises DataLoadError for any
        validation or insertion failure.
        """

        if not parquet_files:
            raise DataLoadError("No Parquet files provided for loading.")

        paths = [Path(path) for path in parquet_files]
        missing_files = [str(path) for path in paths if not path.exists()]
        if missing_files:
            raise DataLoadError(
                "Missing Parquet files: " + ", ".join(sorted(missing_files))
            )

        try:
            with self.engine.begin() as conn:
                if not self._experiment_exists(conn, experiment_name):
                    raise ExperimentNotFoundError(
                        f"Experiment '{experiment_name}' does not exist."
                    )

                table_record = conn.execute(
                    select(self._experiment_tables.c.table_name).where(
                        (self._experiment_tables.c.experiment_name == experiment_name)
                        & (self._experiment_tables.c.table_name == table_name)
                    )
                ).first()

                if not table_record:
                    raise DataLoadError(
                        f"Table '{table_name}' is not registered under experiment '{experiment_name}'."
                    )

                physical_table = self._physical_table_name(experiment_name, table_name)
                inspector = inspect(conn)
                if not inspector.has_table(physical_table):
                    raise DataLoadError(
                        f"Physical table '{physical_table}' does not exist in the warehouse."
                    )

                target_table = Table(physical_table, MetaData(), autoload_with=conn)
                # Replace existing contents to mirror the latest Parquet export.
                conn.execute(target_table.delete())

                total_rows = 0
                for file_path in paths:
                    try:
                        parquet_table = pq.read_table(file_path)
                    except Exception as exc:  # pragma: no cover - defensive
                        raise DataLoadError(
                            f"Failed to read Parquet file '{file_path}': {exc}"
                        ) from exc

                    if parquet_table.num_rows == 0:
                        continue

                    records = parquet_table.to_pylist()
                    try:
                        conn.execute(target_table.insert(), records)
                    except SQLAlchemyError as exc:
                        raise DataLoadError(
                            f"Failed to load Parquet file '{file_path}' into '{physical_table}': {exc}"
                        ) from exc

                    total_rows += parquet_table.num_rows

                return total_rows
        except (ExperimentNotFoundError, DataLoadError):
            raise
        except SQLAlchemyError as exc:
            raise DataLoadError(
                f"Failed to load data into '{table_name}' for experiment '{experiment_name}': {exc}"
            ) from exc

    # Internal helpers -----------------------------------------------------------

    def _experiment_exists(self, conn: Connection, name: str) -> bool:
        result = conn.execute(
            select(self._experiments.c.id).where(self._experiments.c.name == name)
        ).first()
        return result is not None

    def _create_data_tables(self, schema: ExperimentSchema, conn: Connection) -> None:
        inspector = inspect(conn)
        for table_schema in schema.tables:
            table_name = self._physical_table_name(schema.name, table_schema.name)
            if inspector.has_table(table_name):
                raise ExperimentMaterializationError(
                    f"Physical table '{table_name}' already exists. Choose a different experiment/table name."
                )

            metadata = MetaData()
            columns = [
                Column(
                    column_schema.name,
                    self._map_column_type(column_schema),
                    nullable=not column_schema.required,
                    unique=column_schema.is_unique,
                )
                for column_schema in table_schema.columns
            ]
            Table(table_name, metadata, *columns)
            metadata.create_all(conn)

    @staticmethod
    def _physical_table_name(experiment_name: str, table_name: str) -> str:
        return f"{normalize_identifier(experiment_name)}__{normalize_identifier(table_name)}"

    @staticmethod
    def _map_column_type(column_schema: ColumnSchema):
        if column_schema.data_type == DataType.INT:
            return Integer
        if column_schema.data_type == DataType.FLOAT:
            return Float
        if column_schema.data_type == DataType.BOOLEAN:
            return Boolean
        if column_schema.data_type == DataType.DATE:
            return Date
        if column_schema.data_type == DataType.VARCHAR:
            length = column_schema.varchar_length or 255
            return String(length)
        raise ValueError(f"Unsupported data type '{column_schema.data_type}'")


def normalize_identifier(identifier: str) -> str:
    """Normalize identifiers for use in SQLite table names."""

    return identifier.strip().lower()


__all__ = [
    "ExperimentPersistence",
    "ExperimentAlreadyExistsError",
    "ExperimentMaterializationError",
    "ExperimentNotFoundError",
    "GenerationAlreadyRunningError",
    "GenerationRunNotFoundError",
    "QueryExecutionError",
    "DataLoadError",
    "ExperimentMetadata",
    "GenerationRunMetadata",
    "GenerationStatus",
    "QueryResult",
    "normalize_identifier",
]
