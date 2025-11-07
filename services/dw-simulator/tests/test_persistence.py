from pathlib import Path

import pytest
from sqlalchemy import inspect

from dw_simulator.persistence import (
    ExperimentAlreadyExistsError,
    ExperimentNotFoundError,
    ExperimentPersistence,
    normalize_identifier,
)
from dw_simulator.schema import ColumnSchema, ExperimentSchema, TableSchema


def build_schema(name: str = "ExperimentAlpha") -> ExperimentSchema:
    return ExperimentSchema(
        name=name,
        description="Test experiment",
        tables=[
            TableSchema(
                name="customers",
                target_rows=100,
                columns=[
                    ColumnSchema(name="customer_id", data_type="INT", is_unique=True),
                    ColumnSchema(name="email", data_type="VARCHAR", varchar_length=255),
                    ColumnSchema(
                        name="signup_date",
                        data_type="DATE",
                        date_start="2024-01-01",
                        date_end="2024-12-31",
                    ),
                ],
            )
        ],
    )


def create_persistence(tmp_path: Path) -> ExperimentPersistence:
    db_path = tmp_path / "warehouse.db"
    return ExperimentPersistence(connection_string=f"sqlite:///{db_path}")


def test_create_experiment_materializes_tables_and_metadata(tmp_path: Path) -> None:
    persistence = create_persistence(tmp_path)
    schema = build_schema()

    metadata = persistence.create_experiment(schema)
    assert metadata.name == schema.name
    fetched = persistence.get_experiment_metadata(schema.name)
    assert fetched is not None
    assert fetched.schema_json

    inspector = inspect(persistence.engine)
    physical_table = f"{normalize_identifier(schema.name)}__{normalize_identifier(schema.tables[0].name)}"
    columns = inspector.get_columns(physical_table)
    column_names = {column["name"] for column in columns}
    assert {"customer_id", "email", "signup_date"} <= column_names
    assert persistence.list_tables(schema.name) == [physical_table]


def test_duplicate_experiment_names_raise(tmp_path: Path) -> None:
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    with pytest.raises(ExperimentAlreadyExistsError):
        persistence.create_experiment(schema)


def test_delete_experiment_drops_tables_and_metadata(tmp_path: Path) -> None:
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    dropped = persistence.delete_experiment(schema.name)
    assert dropped == 1
    assert persistence.get_experiment_metadata(schema.name) is None
    assert persistence.list_tables(schema.name) == []


def test_delete_experiment_not_found(tmp_path: Path) -> None:
    persistence = create_persistence(tmp_path)
    with pytest.raises(ExperimentNotFoundError):
        persistence.delete_experiment("unknown")


def test_list_experiments_returns_all(tmp_path: Path) -> None:
    persistence = create_persistence(tmp_path)
    schema = build_schema("Alpha")
    schema_b = build_schema("Beta")
    persistence.create_experiment(schema)
    persistence.create_experiment(schema_b)

    experiments = persistence.list_experiments()
    names = [exp.name for exp in experiments]
    assert set(names) == {"Alpha", "Beta"}
