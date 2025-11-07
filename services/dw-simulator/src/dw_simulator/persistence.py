"""
Persistence layer for experiment metadata and physical table creation.

Backed by SQLite (or any SQLAlchemy URL), this module stores experiment
definitions and materializes the corresponding tables in the local warehouse,
enforcing uniqueness/identifier guarantees laid out in docs/product-spec.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

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


class ExperimentAlreadyExistsError(RuntimeError):
    """Raised when attempting to create an experiment that already exists."""


class ExperimentMaterializationError(RuntimeError):
    """Raised when DDL generation fails to create backing tables."""


class ExperimentNotFoundError(RuntimeError):
    """Raised when delete/lookups reference a non-existent experiment."""


@dataclass(frozen=True)
class ExperimentMetadata:
    """Metadata view returned by the repository."""

    name: str
    description: str | None
    schema_json: str
    created_at: datetime


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
                conn.execute(self._experiments.delete().where(self._experiments.c.name == name))
        except ExperimentNotFoundError:
            raise
        except SQLAlchemyError as exc:
            raise ExperimentMaterializationError(f"Failed to delete experiment '{name}': {exc}") from exc

        return dropped

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
    "ExperimentMetadata",
    "normalize_identifier",
]
