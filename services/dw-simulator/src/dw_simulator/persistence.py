"""
Persistence layer for experiment metadata and physical table creation.

Backed by SQLite (or any SQLAlchemy URL), this module stores experiment
definitions and materializes the corresponding tables in the local warehouse,
enforcing uniqueness/identifier guarantees laid out in docs/product-spec.md.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)

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

from .config import get_target_db_url, get_redshift_url, get_snowflake_url
from .schema import ColumnSchema, DataType, ExperimentSchema, WarehouseType
from .s3_client import upload_parquet_files_to_s3, S3UploadError

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
    warehouse_type: str | None = None


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
    """Coordinates metadata storage plus warehouse table creation.

    Uses dual-database architecture:
    - metadata_engine (SQLite): Stores experiment schemas, metadata, and generation runs
    - warehouse_engine (PostgreSQL/Redshift/Snowflake): Stores actual data tables for querying

    Warehouse configuration priority:
    1. Explicit warehouse_url parameter
    2. DW_SIMULATOR_REDSHIFT_URL environment variable
    3. DW_SIMULATOR_SNOWFLAKE_URL environment variable (future support)
    4. Falls back to metadata_engine (SQLite) for local/test environments
    """

    def __init__(
        self,
        connection_string: str | None = None,
        warehouse_url: str | None = None,
    ) -> None:
        # Metadata database (SQLite) - stores experiment definitions and tracking
        self.connection_string = connection_string or get_target_db_url()
        self.engine: Engine = create_engine(self.connection_string, future=True)

        # Initialize warehouse engines for each supported warehouse type
        # These will be used based on per-experiment warehouse selection
        self._warehouse_engines: dict[str, Engine] = {}

        # SQLite warehouse (always available, same as metadata DB)
        self._warehouse_engines[WarehouseType.SQLITE] = self.engine

        # Redshift warehouse (PostgreSQL-based emulator)
        redshift_url = get_redshift_url()
        if redshift_url:
            self._warehouse_engines[WarehouseType.REDSHIFT] = create_engine(redshift_url, future=True)

        # Snowflake warehouse (LocalStack emulator)
        snowflake_url = get_snowflake_url()
        if snowflake_url:
            self._warehouse_engines[WarehouseType.SNOWFLAKE] = create_engine(snowflake_url, future=True)

        # Legacy: maintain backward compatibility with warehouse_url parameter
        # This is used as the default when no target_warehouse is specified
        if warehouse_url is not None:
            self.default_warehouse_url = warehouse_url
        else:
            # Priority: Redshift > Snowflake > SQLite
            if redshift_url is not None:
                self.default_warehouse_url = redshift_url
                self.default_warehouse_type = WarehouseType.REDSHIFT
            elif snowflake_url is not None:
                self.default_warehouse_url = snowflake_url
                self.default_warehouse_type = WarehouseType.SNOWFLAKE
            else:
                self.default_warehouse_url = self.connection_string
                self.default_warehouse_type = WarehouseType.SQLITE

        # Create default warehouse engine (for backward compatibility)
        self.warehouse_engine: Engine = create_engine(self.default_warehouse_url, future=True)

        self._metadata = MetaData()
        self._experiments = Table(
            "experiments",
            self._metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(255), nullable=False, unique=True),
            Column("description", Text),
            Column("schema_json", Text, nullable=False),
            Column("created_at", String(32), nullable=False),
            Column("warehouse_type", String(32), nullable=True),  # sqlite/redshift/snowflake
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

    # Warehouse engine routing ---------------------------------------------------

    def _get_warehouse_engine_for_experiment(self, experiment_name: str) -> Engine:
        """Get the appropriate warehouse engine for a specific experiment.

        Returns the warehouse engine based on the experiment's target_warehouse setting.
        Falls back to default warehouse if not specified.
        """
        with self.engine.connect() as conn:
            row = conn.execute(
                select(self._experiments.c.warehouse_type).where(
                    self._experiments.c.name == experiment_name
                )
            ).first()

            if not row:
                raise ExperimentNotFoundError(f"Experiment '{experiment_name}' does not exist.")

            warehouse_type = row.warehouse_type

        # If experiment has a specific warehouse type, use that engine
        if warehouse_type and warehouse_type in self._warehouse_engines:
            return self._warehouse_engines[warehouse_type]

        # Otherwise use default warehouse engine
        return self.warehouse_engine

    def _get_warehouse_type_from_schema(self, schema: ExperimentSchema) -> str:
        """Determine warehouse type from schema, falling back to default."""
        if schema.target_warehouse:
            # Validate that the requested warehouse is available
            if schema.target_warehouse not in self._warehouse_engines:
                available = ", ".join(self._warehouse_engines.keys())
                raise ValueError(
                    f"Warehouse '{schema.target_warehouse}' is not configured. "
                    f"Available warehouses: {available}"
                )
            return schema.target_warehouse

        # Use default warehouse type
        return self.default_warehouse_type

    # Public API -----------------------------------------------------------------

    def create_experiment(self, schema: ExperimentSchema) -> ExperimentMetadata:
        """Persist metadata and create the physical tables for the experiment."""

        created_at = None
        schema_json = None

        try:
            # First, create metadata in the metadata database
            with self.engine.begin() as conn:
                if self._experiment_exists(conn, schema.name):
                    raise ExperimentAlreadyExistsError(
                        f"Experiment '{schema.name}' already exists. Choose a unique name."
                    )

                created_at = datetime.now(timezone.utc).isoformat()
                schema_json = schema.model_dump_json()
                warehouse_type = self._get_warehouse_type_from_schema(schema)
                conn.execute(
                    self._experiments.insert().values(
                        name=schema.name,
                        description=schema.description,
                        schema_json=schema_json,
                        created_at=created_at,
                        warehouse_type=warehouse_type,
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

            # Then, create physical tables in the warehouse database (separate transaction)
            try:
                self._create_data_tables(schema)
            except Exception as exc:
                # If warehouse table creation fails, rollback metadata
                with self.engine.begin() as conn:
                    conn.execute(
                        self._experiment_tables.delete().where(
                            self._experiment_tables.c.experiment_name == schema.name
                        )
                    )
                    conn.execute(self._experiments.delete().where(self._experiments.c.name == schema.name))
                raise ExperimentMaterializationError(
                    f"Failed to create warehouse tables for experiment '{schema.name}': {exc}"
                ) from exc

        except ExperimentAlreadyExistsError:
            raise
        except ExperimentMaterializationError:
            raise
        except SQLAlchemyError as exc:
            raise ExperimentMaterializationError(f"Failed to create experiment '{schema.name}': {exc}") from exc

        return ExperimentMetadata(
            name=schema.name,
            description=schema.description,
            schema_json=schema_json,
            created_at=datetime.fromisoformat(created_at),
            warehouse_type=self._get_warehouse_type_from_schema(schema),
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
                    self._experiments.c.warehouse_type,
                ).where(self._experiments.c.name == name)
            ).first()

        if not row:
            return None
        return ExperimentMetadata(
            name=row.name,
            description=row.description,
            schema_json=row.schema_json,
            created_at=datetime.fromisoformat(row.created_at),
            warehouse_type=row.warehouse_type,
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
                    self._experiments.c.warehouse_type,
                ).order_by(self._experiments.c.created_at.desc())
            ).all()

        return [
            ExperimentMetadata(
                name=row.name,
                description=row.description,
                schema_json=row.schema_json,
                created_at=datetime.fromisoformat(row.created_at),
                warehouse_type=row.warehouse_type,
            )
            for row in rows
        ]

    def delete_experiment(self, name: str) -> int:
        """
        Drop experiment tables from warehouse database and metadata from metadata database.
        Returns number of dropped tables.
        """

        try:
            # Get table list from metadata database
            with self.engine.begin() as metadata_conn:
                if not self._experiment_exists(metadata_conn, name):
                    raise ExperimentNotFoundError(f"Experiment '{name}' does not exist.")

                table_rows = metadata_conn.execute(
                    select(self._experiment_tables.c.table_name).where(
                        self._experiment_tables.c.experiment_name == name
                    )
                ).all()
                physical_tables = [
                    self._physical_table_name(name, row.table_name) for row in table_rows
                ]

                # Get the appropriate warehouse engine for this experiment
                warehouse_engine = self._get_warehouse_engine_for_experiment(name)

                # Drop physical tables from warehouse database
                inspector = inspect(warehouse_engine)
                dropped = 0
                with warehouse_engine.begin() as warehouse_conn:
                    for table_name in physical_tables:
                        if inspector.has_table(table_name):
                            warehouse_conn.exec_driver_sql(f'DROP TABLE IF EXISTS "{table_name}"')
                            dropped += 1

                # Delete metadata
                metadata_conn.execute(
                    self._experiment_tables.delete().where(
                        self._experiment_tables.c.experiment_name == name
                    )
                )
                metadata_conn.execute(
                    self._generation_runs.delete().where(
                        self._generation_runs.c.experiment_name == name
                    )
                )
                metadata_conn.execute(self._experiments.delete().where(self._experiments.c.name == name))
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
        Truncate all tables in the warehouse database for an experiment without deleting the schema.
        Returns the number of tables reset.
        Raises GenerationAlreadyRunningError if a generation is currently active.
        """
        try:
            # Check metadata and get table list
            with self.engine.connect() as metadata_conn:
                if not self._experiment_exists(metadata_conn, name):
                    raise ExperimentNotFoundError(f"Experiment '{name}' does not exist.")

                # Check for concurrent generation runs (guard against reset during generation)
                active_run = metadata_conn.execute(
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
                table_rows = metadata_conn.execute(
                    select(self._experiment_tables.c.table_name).where(
                        self._experiment_tables.c.experiment_name == name
                    )
                ).all()
                physical_tables = [
                    self._physical_table_name(name, row.table_name) for row in table_rows
                ]

            # Get the appropriate warehouse engine for this experiment
            warehouse_engine = self._get_warehouse_engine_for_experiment(name)

            # Truncate tables in warehouse database
            inspector = inspect(warehouse_engine)
            reset_count = 0
            with warehouse_engine.begin() as warehouse_conn:
                for table_name in physical_tables:
                    if inspector.has_table(table_name):
                        warehouse_conn.exec_driver_sql(f'DELETE FROM "{table_name}"')
                        reset_count += 1

        except (ExperimentNotFoundError, GenerationAlreadyRunningError):
            raise
        except SQLAlchemyError as exc:
            raise ExperimentMaterializationError(f"Failed to reset experiment '{name}': {exc}") from exc

        return reset_count

    def execute_query(self, sql: str, experiment_name: str | None = None) -> QueryResult:
        """
        Execute a SQL query against the warehouse database (Redshift/PostgreSQL/Snowflake).

        If experiment_name is provided, creates temporary views that map simple table names
        to the physical experiment__table names, allowing queries like "SELECT * FROM customers"
        instead of "SELECT * FROM experiment__customers". Also routes to the experiment's
        target warehouse.

        Raises QueryExecutionError if the query fails.
        """
        # Get the appropriate warehouse engine
        if experiment_name:
            warehouse_engine = self._get_warehouse_engine_for_experiment(experiment_name)
        else:
            warehouse_engine = self.warehouse_engine

        try:
            # Execute query against warehouse database (where actual data lives)
            with warehouse_engine.connect() as conn:
                # If experiment is specified, create temporary views for easier querying
                if experiment_name:
                    # Get the list of tables for this experiment
                    table_rows = conn.execute(
                        select(self._experiment_tables.c.table_name).where(
                            self._experiment_tables.c.experiment_name == experiment_name
                        )
                    ).all()

                    # Create temporary views for each table
                    for row in table_rows:
                        logical_name = row.table_name
                        physical_name = self._physical_table_name(experiment_name, logical_name)
                        # Create or replace a temporary view with the simple table name
                        conn.execute(text(f'CREATE OR REPLACE TEMP VIEW "{logical_name}" AS SELECT * FROM "{physical_name}"'))

                # Execute the user's query
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
        run_id: int | None = None,
    ) -> int:
        """
        Load Parquet batches into the physical table in the warehouse database (Redshift/PostgreSQL).

        For PostgreSQL/Redshift warehouses, uploads Parquet files to S3 and uses COPY FROM.
        For SQLite warehouses, uses direct INSERT (S3 not supported).

        Args:
            experiment_name: Name of the experiment
            table_name: Name of the table
            parquet_files: List of Parquet file paths to load
            run_id: Optional generation run ID (used for S3 path organization)

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
            # Check metadata in SQLite
            with self.engine.connect() as metadata_conn:
                if not self._experiment_exists(metadata_conn, experiment_name):
                    raise ExperimentNotFoundError(
                        f"Experiment '{experiment_name}' does not exist."
                    )

                table_record = metadata_conn.execute(
                    select(self._experiment_tables.c.table_name).where(
                        (self._experiment_tables.c.experiment_name == experiment_name)
                        & (self._experiment_tables.c.table_name == table_name)
                    )
                ).first()

                if not table_record:
                    raise DataLoadError(
                        f"Table '{table_name}' is not registered under experiment '{experiment_name}'."
                    )

            # Load data into warehouse database (Redshift/PostgreSQL/Snowflake)
            physical_table = self._physical_table_name(experiment_name, table_name)

            # Get the appropriate warehouse engine for this experiment
            warehouse_engine = self._get_warehouse_engine_for_experiment(experiment_name)

            # Detect warehouse type and use appropriate loading strategy
            dialect_name = warehouse_engine.dialect.name

            if dialect_name == 'postgresql':
                # PostgreSQL/Redshift: Use S3 + COPY FROM (via direct INSERT fallback)
                return self._load_via_s3_copy(
                    experiment_name=experiment_name,
                    table_name=table_name,
                    physical_table=physical_table,
                    parquet_files=paths,
                    run_id=run_id,
                    warehouse_engine=warehouse_engine
                )
            elif dialect_name == 'snowflake':
                # Snowflake: Use S3 + COPY INTO
                return self._load_via_snowflake_copy(
                    experiment_name=experiment_name,
                    table_name=table_name,
                    physical_table=physical_table,
                    parquet_files=paths,
                    run_id=run_id,
                    warehouse_engine=warehouse_engine
                )
            else:
                # SQLite: Use direct INSERT
                return self._load_via_direct_insert(
                    physical_table=physical_table,
                    parquet_files=paths,
                    warehouse_engine=warehouse_engine
                )
        except (ExperimentNotFoundError, DataLoadError):
            raise
        except SQLAlchemyError as exc:
            raise DataLoadError(
                f"Failed to load data into '{table_name}' for experiment '{experiment_name}': {exc}"
            ) from exc

    def _load_via_s3_copy(
        self,
        experiment_name: str,
        table_name: str,
        physical_table: str,
        parquet_files: list[Path],
        run_id: int | None,
        warehouse_engine: Engine,
    ) -> int:
        """
        Load Parquet files using S3 + PostgreSQL COPY FROM (Redshift emulation).

        This simulates Redshift COPY command behavior by:
        1. Uploading Parquet files to LocalStack S3
        2. Using PostgreSQL COPY FROM to load from S3 URIs

        Returns the total number of rows loaded.
        """
        try:
            # Upload Parquet files to S3
            s3_uris = upload_parquet_files_to_s3(
                parquet_files=[str(f) for f in parquet_files],
                experiment_name=experiment_name,
                table_name=table_name,
                run_id=run_id
            )
        except S3UploadError as exc:
            raise DataLoadError(
                f"Failed to upload Parquet files to S3: {exc}"
            ) from exc

        # Clear existing data
        with warehouse_engine.begin() as warehouse_conn:
            inspector = inspect(warehouse_engine)
            if not inspector.has_table(physical_table):
                raise DataLoadError(
                    f"Physical table '{physical_table}' does not exist in the warehouse."
                )

            # Delete existing contents
            warehouse_conn.exec_driver_sql(f'DELETE FROM "{physical_table}"')

            # Note: PostgreSQL doesn't natively support COPY FROM S3 URIs
            # That's a Redshift-specific feature. For now, we'll fall back to
            # direct INSERT for PostgreSQL. In a real Redshift environment,
            # you would use: COPY table FROM 's3://bucket/key' CREDENTIALS ...
            #
            # For LocalStack Redshift emulation, we need to use direct INSERT
            # since LocalStack PostgreSQL doesn't support S3 COPY commands.
            # This is a limitation of the emulation environment.

            total_rows = 0
            target_table = Table(physical_table, MetaData(), autoload_with=warehouse_engine)

            for file_path in parquet_files:
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
                    warehouse_conn.execute(target_table.insert(), records)
                except SQLAlchemyError as exc:
                    raise DataLoadError(
                        f"Failed to load Parquet file '{file_path}' into '{physical_table}': {exc}"
                    ) from exc

                total_rows += parquet_table.num_rows

            return total_rows

    def _load_via_snowflake_copy(
        self,
        experiment_name: str,
        table_name: str,
        physical_table: str,
        parquet_files: list[Path],
        run_id: int | None,
        warehouse_engine: Engine,
    ) -> int:
        """
        Load Parquet files using S3 + Snowflake COPY INTO command.

        This implements Snowpipe-style loading by:
        1. Uploading Parquet files to LocalStack S3
        2. Using Snowflake COPY INTO to load from S3 URIs
        3. Cleaning up staging files after successful load

        Supported data types:
        - Basic types: INT, FLOAT, VARCHAR, DATE, BOOLEAN
        - Snowflake-specific types (VARIANT, ARRAY, OBJECT) are not yet supported
          in the schema definition. Future enhancement tracked in backlog.

        Falls back to direct INSERT if LocalStack Snowflake emulator doesn't
        support COPY INTO command (limited feature support in emulation).

        Returns the total number of rows loaded.
        """
        try:
            # Upload Parquet files to S3
            s3_uris = upload_parquet_files_to_s3(
                parquet_files=[str(f) for f in parquet_files],
                experiment_name=experiment_name,
                table_name=table_name,
                run_id=run_id
            )
        except S3UploadError as exc:
            raise DataLoadError(
                f"Failed to upload Parquet files to S3: {exc}"
            ) from exc

        # Clear existing data and load from S3 using Snowflake COPY INTO
        with warehouse_engine.begin() as warehouse_conn:
            inspector = inspect(warehouse_engine)
            if not inspector.has_table(physical_table):
                raise DataLoadError(
                    f"Physical table '{physical_table}' does not exist in the warehouse."
                )

            # Delete existing contents
            warehouse_conn.exec_driver_sql(f'DELETE FROM "{physical_table}"')

            # Use Snowflake COPY INTO command to load from S3
            # Snowflake supports loading directly from S3 URIs using COPY INTO
            # Format: COPY INTO table FROM 's3://bucket/prefix/' FILE_FORMAT = (TYPE = PARQUET)
            #
            # For LocalStack Snowflake emulation, we need to construct the S3 path
            # and use the COPY INTO command with Parquet format specification

            # Extract bucket and prefix from first S3 URI
            # Format: s3://bucket/experiments/{experiment}/{table}/run_{id}/file.parquet
            if s3_uris:
                # Get the S3 prefix (everything except the filename)
                first_uri = s3_uris[0]
                # Remove s3:// prefix
                path_parts = first_uri.replace('s3://', '').split('/')
                bucket = path_parts[0]
                # Get directory path (everything except last part which is the filename)
                prefix = '/'.join(path_parts[1:-1])
                s3_path = f"s3://{bucket}/{prefix}/"

                # Construct COPY INTO command for Snowflake
                # Using simplified syntax for LocalStack Snowflake emulator
                copy_stmt = f"""
                    COPY INTO "{physical_table}"
                    FROM '{s3_path}'
                    FILE_FORMAT = (TYPE = PARQUET)
                    PATTERN = '.*\\.parquet'
                """

                try:
                    warehouse_conn.exec_driver_sql(copy_stmt)
                except SQLAlchemyError as exc:
                    # If Snowflake COPY INTO fails, fall back to direct INSERT
                    # This handles cases where LocalStack Snowflake emulator has limited feature support
                    logger.warning(
                        f"Snowflake COPY INTO failed, falling back to direct INSERT: {exc}"
                    )
                    # Use direct insert fallback
                    return self._load_via_direct_insert_in_transaction(
                        warehouse_conn=warehouse_conn,
                        physical_table=physical_table,
                        parquet_files=parquet_files,
                        warehouse_engine=warehouse_engine
                    )

            # Count total rows loaded
            result = warehouse_conn.execute(
                text(f'SELECT COUNT(*) as count FROM "{physical_table}"')
            )
            row = result.fetchone()
            total_rows = row[0] if row else 0

            return total_rows

    def _load_via_direct_insert_in_transaction(
        self,
        warehouse_conn: Connection,
        physical_table: str,
        parquet_files: list[Path],
        warehouse_engine: Engine,
    ) -> int:
        """
        Load Parquet files using direct INSERT statements within an existing transaction.

        This is a helper method used as a fallback when COPY commands fail.
        """
        target_table = Table(physical_table, MetaData(), autoload_with=warehouse_engine)

        total_rows = 0
        for file_path in parquet_files:
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
                warehouse_conn.execute(target_table.insert(), records)
            except SQLAlchemyError as exc:
                raise DataLoadError(
                    f"Failed to load Parquet file '{file_path}' into '{physical_table}': {exc}"
                ) from exc

            total_rows += parquet_table.num_rows

        return total_rows

    def _load_via_direct_insert(
        self,
        physical_table: str,
        parquet_files: list[Path],
        warehouse_engine: Engine,
    ) -> int:
        """
        Load Parquet files using direct INSERT statements (SQLite fallback).

        Returns the total number of rows loaded.
        """
        with warehouse_engine.begin() as warehouse_conn:
            inspector = inspect(warehouse_engine)
            if not inspector.has_table(physical_table):
                raise DataLoadError(
                    f"Physical table '{physical_table}' does not exist in the warehouse."
                )

            target_table = Table(physical_table, MetaData(), autoload_with=warehouse_engine)
            # Replace existing contents to mirror the latest Parquet export.
            warehouse_conn.execute(target_table.delete())

            total_rows = 0
            for file_path in parquet_files:
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
                    warehouse_conn.execute(target_table.insert(), records)
                except SQLAlchemyError as exc:
                    raise DataLoadError(
                        f"Failed to load Parquet file '{file_path}' into '{physical_table}': {exc}"
                    ) from exc

                total_rows += parquet_table.num_rows

            return total_rows

    def load_generation_run(self, run_id: int) -> dict[str, int]:
        """
        Load all Parquet files from a generation run into their corresponding
        database tables.

        Returns a dictionary mapping table names to row counts loaded.
        Raises GenerationRunNotFoundError if the run doesn't exist.
        Raises DataLoadError if any table loading fails.
        """
        # Fetch the generation run metadata
        run_metadata = self.get_generation_run(run_id)
        if not run_metadata:
            raise GenerationRunNotFoundError(f"Generation run {run_id} not found.")

        # Verify the run completed successfully
        if run_metadata.status != GenerationStatus.COMPLETED:
            raise DataLoadError(
                f"Cannot load generation run {run_id}: status is {run_metadata.status.value}, "
                f"expected {GenerationStatus.COMPLETED.value}."
            )

        # Verify output path exists
        if not run_metadata.output_path:
            raise DataLoadError(
                f"Generation run {run_id} has no output path recorded."
            )

        output_dir = Path(run_metadata.output_path)
        if not output_dir.exists():
            raise DataLoadError(
                f"Output directory '{output_dir}' for generation run {run_id} does not exist."
            )

        # Get experiment metadata to determine which tables to load
        experiment_metadata = self.get_experiment_metadata(run_metadata.experiment_name)
        if not experiment_metadata:
            raise ExperimentNotFoundError(
                f"Experiment '{run_metadata.experiment_name}' not found."
            )

        # Parse the experiment schema to get table names
        from .schema import ExperimentSchema
        schema = ExperimentSchema.model_validate_json(experiment_metadata.schema_json)

        # Load each table
        row_counts: dict[str, int] = {}
        errors: list[str] = []

        for table_schema in schema.tables:
            table_name = table_schema.name
            table_dir = output_dir / table_name

            # Check if table directory exists
            if not table_dir.exists():
                errors.append(f"Table directory '{table_dir}' not found for table '{table_name}'.")
                continue

            # Find all parquet files in the table directory
            parquet_files = sorted(table_dir.glob("batch-*.parquet"))
            if not parquet_files:
                errors.append(f"No Parquet files found in '{table_dir}' for table '{table_name}'.")
                continue

            # Load the parquet files into the table
            try:
                loaded_rows = self.load_parquet_files_to_table(
                    experiment_name=run_metadata.experiment_name,
                    table_name=table_name,
                    parquet_files=[str(f) for f in parquet_files],
                    run_id=run_id,
                )
                row_counts[table_name] = loaded_rows
            except (ExperimentNotFoundError, DataLoadError) as exc:
                errors.append(f"Failed to load table '{table_name}': {exc}")

        # If there were any errors, raise them
        if errors:
            error_msg = "; ".join(errors)
            raise DataLoadError(
                f"Failed to load generation run {run_id}: {error_msg}"
            )

        return row_counts

    # Internal helpers -----------------------------------------------------------

    def _experiment_exists(self, conn: Connection, name: str) -> bool:
        result = conn.execute(
            select(self._experiments.c.id).where(self._experiments.c.name == name)
        ).first()
        return result is not None

    def _create_data_tables(self, schema: ExperimentSchema) -> None:
        """
        Create physical data tables in the warehouse database (PostgreSQL/Redshift/Snowflake).
        Uses a separate transaction on the appropriate warehouse engine based on experiment's target_warehouse.
        """
        # Get the appropriate warehouse engine for this experiment
        warehouse_type = self._get_warehouse_type_from_schema(schema)
        warehouse_engine = self._warehouse_engines.get(warehouse_type, self.warehouse_engine)

        inspector = inspect(warehouse_engine)
        for table_schema in schema.tables:
            table_name = self._physical_table_name(schema.name, table_schema.name)
            if inspector.has_table(table_name):
                raise ExperimentMaterializationError(
                    f"Physical table '{table_name}' already exists. This may be due to orphaned tables from a previous experiment. "
                    f"Either choose a different experiment/table name, or manually drop the table using SQL query interface: "
                    f"DROP TABLE {table_name};"
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
            # Create table in the experiment's target warehouse database
            metadata.create_all(warehouse_engine)

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
