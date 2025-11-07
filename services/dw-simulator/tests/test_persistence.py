from pathlib import Path

import pytest
from sqlalchemy import inspect

from dw_simulator.persistence import (
    ExperimentAlreadyExistsError,
    ExperimentNotFoundError,
    ExperimentPersistence,
    GenerationAlreadyRunningError,
    GenerationRunNotFoundError,
    GenerationStatus,
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
    run_id = persistence.start_generation_run(schema.name, output_path="/tmp/out")
    persistence.complete_generation_run(run_id, '{"customers": 10}')

    dropped = persistence.delete_experiment(schema.name)
    assert dropped == 1
    assert persistence.get_experiment_metadata(schema.name) is None
    assert persistence.list_tables(schema.name) == []
    assert persistence.list_generation_runs(schema.name) == []


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


def test_start_generation_run_creates_run_record(tmp_path: Path) -> None:
    """Test that starting a generation run creates a record with RUNNING status."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    run_id = persistence.start_generation_run(
        experiment_name=schema.name,
        output_path="/tmp/test",
        seed=42,
    )

    assert run_id is not None
    run = persistence.get_generation_run(run_id)
    assert run is not None
    assert run.experiment_name == schema.name
    assert run.status == GenerationStatus.RUNNING
    assert run.output_path == "/tmp/test"
    assert run.seed == 42
    assert run.completed_at is None


def test_concurrent_generation_guard_prevents_simultaneous_runs(tmp_path: Path) -> None:
    """Test that concurrent job guard prevents multiple simultaneous runs."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    # Start first run
    run_id_1 = persistence.start_generation_run(experiment_name=schema.name)
    assert run_id_1 is not None

    # Attempt second run should fail
    with pytest.raises(GenerationAlreadyRunningError) as exc_info:
        persistence.start_generation_run(experiment_name=schema.name)

    assert schema.name in str(exc_info.value)
    assert str(run_id_1) in str(exc_info.value)


def test_concurrent_guard_allows_run_after_completion(tmp_path: Path) -> None:
    """Test that concurrent guard allows new run after previous completes."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    # Start and complete first run
    run_id_1 = persistence.start_generation_run(experiment_name=schema.name)
    persistence.complete_generation_run(run_id_1, '{"customers": 100}')

    # Second run should succeed
    run_id_2 = persistence.start_generation_run(experiment_name=schema.name)
    assert run_id_2 != run_id_1


def test_concurrent_guard_allows_run_after_failure(tmp_path: Path) -> None:
    """Test that concurrent guard allows new run after previous fails."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    # Start and fail first run
    run_id_1 = persistence.start_generation_run(experiment_name=schema.name)
    persistence.fail_generation_run(run_id_1, "Test error")

    # Second run should succeed
    run_id_2 = persistence.start_generation_run(experiment_name=schema.name)
    assert run_id_2 != run_id_1


def test_complete_generation_run_updates_status_and_metadata(tmp_path: Path) -> None:
    """Test that completing a run updates status and stores row counts."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    run_id = persistence.start_generation_run(experiment_name=schema.name)
    row_counts_json = '{"customers": 1000, "orders": 5000}'
    persistence.complete_generation_run(run_id, row_counts_json)

    run = persistence.get_generation_run(run_id)
    assert run is not None
    assert run.status == GenerationStatus.COMPLETED
    assert run.row_counts == row_counts_json
    assert run.completed_at is not None


def test_fail_generation_run_captures_error_message(tmp_path: Path) -> None:
    """Test that failing a run captures detailed error message."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    run_id = persistence.start_generation_run(experiment_name=schema.name)
    error_msg = "GenerationError: Unable to generate unique values\n\nTraceback:\n  File foo.py, line 42"
    persistence.fail_generation_run(run_id, error_msg)

    run = persistence.get_generation_run(run_id)
    assert run is not None
    assert run.status == GenerationStatus.FAILED
    assert run.error_message == error_msg
    assert run.completed_at is not None


def test_complete_nonexistent_run_raises(tmp_path: Path) -> None:
    """Test that completing a non-existent run raises error."""
    persistence = create_persistence(tmp_path)
    with pytest.raises(GenerationRunNotFoundError):
        persistence.complete_generation_run(99999, '{"test": 100}')


def test_fail_nonexistent_run_raises(tmp_path: Path) -> None:
    """Test that failing a non-existent run raises error."""
    persistence = create_persistence(tmp_path)
    with pytest.raises(GenerationRunNotFoundError):
        persistence.fail_generation_run(99999, "error")


def test_start_generation_for_nonexistent_experiment_raises(tmp_path: Path) -> None:
    """Test that starting generation for non-existent experiment raises error."""
    persistence = create_persistence(tmp_path)
    with pytest.raises(ExperimentNotFoundError):
        persistence.start_generation_run(experiment_name="NonExistent")


def test_list_generation_runs_returns_all_for_experiment(tmp_path: Path) -> None:
    """Test that listing runs returns all runs for an experiment, most recent first."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    # Create multiple runs
    run_id_1 = persistence.start_generation_run(experiment_name=schema.name)
    persistence.complete_generation_run(run_id_1, '{"customers": 100}')

    run_id_2 = persistence.start_generation_run(experiment_name=schema.name)
    persistence.fail_generation_run(run_id_2, "Test error")

    run_id_3 = persistence.start_generation_run(experiment_name=schema.name)

    runs = persistence.list_generation_runs(schema.name)
    assert len(runs) == 3
    # Most recent first
    assert runs[0].id == run_id_3
    assert runs[1].id == run_id_2
    assert runs[2].id == run_id_1


def test_list_generation_runs_filters_by_experiment(tmp_path: Path) -> None:
    """Test that listing runs only returns runs for the specified experiment."""
    persistence = create_persistence(tmp_path)
    schema_a = build_schema("ExperimentA")
    schema_b = build_schema("ExperimentB")
    persistence.create_experiment(schema_a)
    persistence.create_experiment(schema_b)

    run_id_a = persistence.start_generation_run(experiment_name=schema_a.name)
    run_id_b = persistence.start_generation_run(experiment_name=schema_b.name)

    runs_a = persistence.list_generation_runs(schema_a.name)
    assert len(runs_a) == 1
    assert runs_a[0].id == run_id_a

    runs_b = persistence.list_generation_runs(schema_b.name)
    assert len(runs_b) == 1
    assert runs_b[0].id == run_id_b
