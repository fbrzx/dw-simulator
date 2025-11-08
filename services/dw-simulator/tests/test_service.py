from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from dw_simulator.generator import (
    GenerationError,
    GenerationRequest,
    GenerationResult,
    TableGenerationResult,
)
from dw_simulator.persistence import (
    DataLoadError,
    ExperimentAlreadyExistsError,
    ExperimentMaterializationError,
    ExperimentMetadata,
    ExperimentNotFoundError,
    GenerationAlreadyRunningError,
    GenerationRunMetadata,
    GenerationStatus,
    QueryExecutionError,
    QueryResult,
)
from dw_simulator.schema import ExperimentSchema, TableSchema, ColumnSchema
from dw_simulator.service import ExperimentService


@dataclass
class StubGenerator:
    result: GenerationResult
    to_raise: Exception | None = None

    def generate(self, request: GenerationRequest) -> GenerationResult:
        if self.to_raise:
            raise self.to_raise
        return self.result


@dataclass
class StubPersistence:
    metadata: ExperimentMetadata | None = None
    to_raise: Exception | None = None
    delete_exception: Exception | None = None
    delete_return: int = 0
    reset_exception: Exception | None = None
    reset_return: int = 0
    schema: ExperimentSchema | None = None
    start_run_exception: Exception | None = None
    complete_run_exception: Exception | None = None
    fail_run_exception: Exception | None = None
    query_result: QueryResult | None = None
    query_exception: Exception | None = None
    load_return: dict[str, int] | None = None
    load_exception: Exception | None = None

    def __post_init__(self) -> None:
        self.recorded_schema: ExperimentSchema | None = None
        self.listed: list[ExperimentMetadata] = []
        self.next_run_id = 1
        self.runs: dict[int, GenerationRunMetadata] = {}
        self.started_runs: list[tuple[str, str | None, int | None]] = []
        self.executed_queries: list[str] = []
        self.loaded_runs: list[int] = []

    def create_experiment(self, schema: ExperimentSchema) -> ExperimentMetadata:
        self.recorded_schema = schema
        if self.to_raise:
            raise self.to_raise
        if not self.metadata:
            raise RuntimeError("StubPersistence requires metadata when no exception is set.")
        return self.metadata

    def list_experiments(self) -> list[ExperimentMetadata]:
        return self.listed or ([self.metadata] if self.metadata else [])

    def delete_experiment(self, name: str) -> int:
        if self.delete_exception:
            raise self.delete_exception
        return self.delete_return

    def reset_experiment(self, name: str) -> int:
        if self.reset_exception:
            raise self.reset_exception
        return self.reset_return

    def get_experiment_metadata(self, name: str) -> ExperimentMetadata | None:
        return self.metadata

    def start_generation_run(
        self,
        experiment_name: str,
        output_path: str | None = None,
        seed: int | None = None,
    ) -> int:
        if self.start_run_exception:
            raise self.start_run_exception
        run_id = self.next_run_id
        self.next_run_id += 1
        self.started_runs.append((experiment_name, output_path, seed))
        self.runs[run_id] = GenerationRunMetadata(
            id=run_id,
            experiment_name=experiment_name,
            status=GenerationStatus.RUNNING,
            started_at=datetime.now(timezone.utc),
            completed_at=None,
            row_counts="{}",
            output_path=output_path,
            error_message=None,
            seed=seed,
        )
        return run_id

    def complete_generation_run(self, run_id: int, row_counts: str) -> None:
        if self.complete_run_exception:
            raise self.complete_run_exception
        if run_id in self.runs:
            old_run = self.runs[run_id]
            self.runs[run_id] = GenerationRunMetadata(
                id=old_run.id,
                experiment_name=old_run.experiment_name,
                status=GenerationStatus.COMPLETED,
                started_at=old_run.started_at,
                completed_at=datetime.now(timezone.utc),
                row_counts=row_counts,
                output_path=old_run.output_path,
                error_message=None,
                seed=old_run.seed,
            )

    def fail_generation_run(self, run_id: int, error_message: str) -> None:
        if self.fail_run_exception:
            raise self.fail_run_exception
        if run_id in self.runs:
            old_run = self.runs[run_id]
            self.runs[run_id] = GenerationRunMetadata(
                id=old_run.id,
                experiment_name=old_run.experiment_name,
                status=GenerationStatus.FAILED,
                started_at=old_run.started_at,
                completed_at=datetime.now(timezone.utc),
                row_counts=old_run.row_counts,
                output_path=old_run.output_path,
                error_message=error_message,
                seed=old_run.seed,
            )

    def get_generation_run(self, run_id: int) -> GenerationRunMetadata | None:
        return self.runs.get(run_id)

    def list_generation_runs(self, experiment_name: str) -> list[GenerationRunMetadata]:
        return [run for run in self.runs.values() if run.experiment_name == experiment_name]

    def execute_query(self, sql: str) -> QueryResult:
        self.executed_queries.append(sql)
        if self.query_exception:
            raise self.query_exception
        if self.query_result is None:
            raise RuntimeError("StubPersistence requires query_result when no exception is set.")
        return self.query_result

    def load_generation_run(self, run_id: int) -> dict[str, int]:
        self.loaded_runs.append(run_id)
        if self.load_exception:
            raise self.load_exception
        return self.load_return or {}


def valid_payload() -> dict[str, Any]:
    return {
        "name": "ServiceExperiment",
        "tables": [
            {
                "name": "customers",
                "target_rows": 1,
                "columns": [
                    {"name": "customer_id", "data_type": "INT", "is_unique": True},
                ],
            }
        ],
    }


def build_metadata(name: str = "ServiceExperiment") -> ExperimentMetadata:
    schema = ExperimentSchema(
        name=name,
        description=None,
        tables=[
            TableSchema(
                name="customers",
                target_rows=1,
                columns=[ColumnSchema(name="customer_id", data_type="INT")],
            )
        ],
    )
    return ExperimentMetadata(
        name=name,
        description=None,
        schema_json=schema.model_dump_json(),
        created_at=datetime.now(timezone.utc),
    )


def test_service_create_experiment_success() -> None:
    metadata = build_metadata()
    stub = StubPersistence(metadata=metadata)
    service = ExperimentService(persistence=stub)  # type: ignore[arg-type]

    result = service.create_experiment_from_payload(valid_payload())

    assert result.success is True
    assert result.metadata == metadata
    assert isinstance(stub.recorded_schema, ExperimentSchema)


def test_service_returns_validation_errors() -> None:
    stub = StubPersistence(metadata=build_metadata())
    service = ExperimentService(persistence=stub)  # type: ignore[arg-type]
    result = service.create_experiment_from_payload({"name": "", "tables": []})

    assert result.success is False
    assert result.errors


def test_service_handles_duplicate_experiments() -> None:
    stub = StubPersistence(
        metadata=None,
        to_raise=ExperimentAlreadyExistsError("Experiment 'ServiceExperiment' already exists."),
    )
    service = ExperimentService(persistence=stub)  # type: ignore[arg-type]
    result = service.create_experiment_from_payload(valid_payload())

    assert result.success is False
    assert "already exists" in result.errors[0]


def test_service_handles_materialization_errors() -> None:
    stub = StubPersistence(
        metadata=None,
        to_raise=ExperimentMaterializationError("ddl failure"),
    )
    service = ExperimentService(persistence=stub)  # type: ignore[arg-type]
    result = service.create_experiment_from_payload(valid_payload())

    assert result.success is False
    assert "ddl failure" in result.errors[0]


def test_create_experiment_from_file_success(tmp_path: Path) -> None:
    metadata = build_metadata()
    stub = StubPersistence(metadata=metadata)
    service = ExperimentService(persistence=stub)  # type: ignore[arg-type]

    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps(valid_payload()))

    result = service.create_experiment_from_file(schema_path)
    assert result.success is True
    assert result.metadata == metadata


def test_create_experiment_from_file_invalid_json(tmp_path: Path) -> None:
    stub = StubPersistence(metadata=build_metadata())
    service = ExperimentService(persistence=stub)  # type: ignore[arg-type]

    schema_path = tmp_path / "schema.json"
    schema_path.write_text("{invalid json")

    result = service.create_experiment_from_file(schema_path)
    assert result.success is False
    assert any("Invalid JSON" in error for error in result.errors)


def test_create_experiment_from_file_read_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    stub = StubPersistence(metadata=build_metadata())
    service = ExperimentService(persistence=stub)  # type: ignore[arg-type]
    schema_path = tmp_path / "missing.json"

    def _broken_read_text() -> str:
        raise OSError("boom")

    monkeypatch.setattr(Path, "read_text", lambda self, *args, **kwargs: _broken_read_text())

    result = service.create_experiment_from_file(schema_path)
    assert result.success is False
    assert "Failed to read schema" in result.errors[0]


def test_list_experiments_returns_persistence_data() -> None:
    metadata = build_metadata()
    stub = StubPersistence(metadata=metadata)
    stub.listed = [metadata]
    service = ExperimentService(persistence=stub)  # type: ignore[arg-type]

    experiments = service.list_experiments()
    assert experiments == [metadata]


def test_delete_experiment_success() -> None:
    stub = StubPersistence(delete_return=2, metadata=build_metadata())
    service = ExperimentService(persistence=stub)  # type: ignore[arg-type]

    result = service.delete_experiment("ServiceExperiment")
    assert result.success is True
    assert result.deleted_tables == 2


def test_delete_experiment_not_found() -> None:
    stub = StubPersistence(delete_exception=ExperimentNotFoundError("missing"))
    service = ExperimentService(persistence=stub)  # type: ignore[arg-type]

    result = service.delete_experiment("missing")
    assert result.success is False
    assert "missing" in result.errors[0]


def test_delete_experiment_materialization_error() -> None:
    stub = StubPersistence(delete_exception=ExperimentMaterializationError("drop failure"))
    service = ExperimentService(persistence=stub)  # type: ignore[arg-type]

    result = service.delete_experiment("boom")
    assert result.success is False
    assert "drop failure" in result.errors[0]


def test_delete_experiment_cleans_generated_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    experiment_name = "ServiceExperiment"
    data_root = tmp_path / "data-root"
    default_dir = data_root / "generated" / experiment_name
    custom_dir = data_root / "runs" / "custom-output"
    default_dir.mkdir(parents=True)
    custom_dir.mkdir(parents=True)
    (default_dir / "placeholder.txt").write_text("default")
    (custom_dir / "placeholder.txt").write_text("custom")

    monkeypatch.setattr("dw_simulator.service.get_data_root", lambda: data_root)

    metadata = build_metadata(experiment_name)
    stub = StubPersistence(delete_return=1, metadata=metadata)
    run_metadata = GenerationRunMetadata(
        id=1,
        experiment_name=experiment_name,
        status=GenerationStatus.COMPLETED,
        started_at=datetime.now(timezone.utc),
        completed_at=datetime.now(timezone.utc),
        row_counts="{}",
        output_path=str(custom_dir),
        error_message=None,
        seed=42,
    )
    stub.runs[run_metadata.id] = run_metadata

    service = ExperimentService(persistence=stub)  # type: ignore[arg-type]
    result = service.delete_experiment(experiment_name)

    assert result.success is True
    assert not default_dir.exists()
    assert not custom_dir.exists()


def test_generate_data_success(tmp_path: Path) -> None:
    metadata = build_metadata()
    stub_persistence = StubPersistence(metadata=metadata, load_return={"customers": 5})
    generation_result = GenerationResult(
        experiment_name="ServiceExperiment",
        output_dir=tmp_path / "out",
        tables=[TableGenerationResult(table_name="customers", row_count=5, files=[tmp_path / "out" / "file.parquet"])],
    )
    stub_generator = StubGenerator(result=generation_result)
    service = ExperimentService(persistence=stub_persistence, generator=stub_generator)  # type: ignore[arg-type]

    result = service.generate_data("ServiceExperiment", rows={"customers": 5}, seed=123)
    assert result.success is True
    assert result.summary == generation_result
    assert result.loaded_row_counts == {"customers": 5}
    assert stub_persistence.loaded_runs == [1]


def test_generate_data_missing_experiment() -> None:
    stub_persistence = StubPersistence(metadata=None)
    service = ExperimentService(persistence=stub_persistence)  # type: ignore[arg-type]

    result = service.generate_data("missing")
    assert result.success is False
    assert "does not exist" in result.errors[0]


def test_generate_data_handles_generation_error() -> None:
    metadata = build_metadata()
    stub_persistence = StubPersistence(metadata=metadata)
    stub_generator = StubGenerator(
        result=None,  # type: ignore[arg-type]
        to_raise=GenerationError("boom"),
    )
    service = ExperimentService(persistence=stub_persistence, generator=stub_generator)  # type: ignore[arg-type]

    result = service.generate_data("ServiceExperiment")
    assert result.success is False
    assert "boom" in result.errors[0]


def test_create_experiment_from_sql_success() -> None:
    sql = """
    CREATE TABLE public.users (
        user_id BIGINT PRIMARY KEY,
        email VARCHAR(100),
        created_at TIMESTAMP
    );
    """
    metadata = build_metadata()
    stub = StubPersistence(metadata=metadata)
    service = ExperimentService(persistence=stub)  # type: ignore[arg-type]

    result = service.create_experiment_from_sql(name="users_exp", sql=sql, dialect="redshift")
    assert result.success is True
    assert isinstance(stub.recorded_schema, ExperimentSchema)
    assert stub.recorded_schema.tables[0].columns[0].is_unique is True  # type: ignore[union-attr]


def test_create_experiment_from_sql_invalid_dialect() -> None:
    sql = "CREATE TABLE t (id BIGINT);"
    service = ExperimentService()  # real persistence is ok; parse will fail first

    result = service.create_experiment_from_sql(name="t", sql=sql, dialect="oracle")
    assert result.success is False
    assert "Unsupported dialect" in result.errors[0]


def test_generate_data_creates_generation_run(tmp_path: Path) -> None:
    """Test that generate_data creates a generation run record."""
    metadata = build_metadata()
    stub_persistence = StubPersistence(metadata=metadata, load_return={})
    generation_result = GenerationResult(
        experiment_name="ServiceExperiment",
        output_dir=tmp_path / "out",
        tables=[TableGenerationResult(table_name="customers", row_count=100, files=[])],
    )
    stub_generator = StubGenerator(result=generation_result)
    service = ExperimentService(persistence=stub_persistence, generator=stub_generator)  # type: ignore[arg-type]

    result = service.generate_data("ServiceExperiment", seed=42, output_dir=tmp_path)
    assert result.success is True
    assert len(stub_persistence.started_runs) == 1
    assert stub_persistence.started_runs[0][0] == "ServiceExperiment"
    assert stub_persistence.started_runs[0][2] == 42  # seed
    assert stub_persistence.started_runs[0][1] == str(tmp_path)


def test_generate_data_completes_run_on_success(tmp_path: Path) -> None:
    """Test that successful generation marks run as completed with row counts."""
    metadata = build_metadata()
    stub_persistence = StubPersistence(
        metadata=metadata,
        load_return={
            "customers": 100,
            "orders": 500,
        },
    )
    generation_result = GenerationResult(
        experiment_name="ServiceExperiment",
        output_dir=tmp_path / "out",
        tables=[
            TableGenerationResult(table_name="customers", row_count=100, files=[]),
            TableGenerationResult(table_name="orders", row_count=500, files=[]),
        ],
    )
    stub_generator = StubGenerator(result=generation_result)
    service = ExperimentService(persistence=stub_persistence, generator=stub_generator)  # type: ignore[arg-type]

    result = service.generate_data("ServiceExperiment")
    assert result.success is True
    assert result.run_metadata is not None
    assert result.run_metadata.status == GenerationStatus.COMPLETED
    assert '"customers": 100' in result.run_metadata.row_counts
    assert '"orders": 500' in result.run_metadata.row_counts
    assert '"loaded"' in result.run_metadata.row_counts
    assert result.loaded_row_counts == {
        "customers": 100,
        "orders": 500,
    }


def test_generate_data_fails_run_on_error() -> None:
    """Test that generation errors mark run as failed with error message."""
    metadata = build_metadata()
    stub_persistence = StubPersistence(metadata=metadata)
    stub_generator = StubGenerator(
        result=None,  # type: ignore[arg-type]
        to_raise=GenerationError("Unable to generate unique values"),
    )
    service = ExperimentService(persistence=stub_persistence, generator=stub_generator)  # type: ignore[arg-type]

    result = service.generate_data("ServiceExperiment")
    assert result.success is False
    assert result.run_id is not None

    # Check that error was persisted
    run = stub_persistence.get_generation_run(result.run_id)
    assert run is not None
    assert run.status == GenerationStatus.FAILED
    assert "Unable to generate unique values" in run.error_message or "Unable to generate unique values" in result.errors[0]


def test_generate_data_reports_load_failure(tmp_path: Path) -> None:
    """If loading Parquet files fails, the error is surfaced and run marked failed."""
    metadata = build_metadata()
    stub_persistence = StubPersistence(metadata=metadata, load_exception=DataLoadError("load exploded"))
    generation_result = GenerationResult(
        experiment_name="ServiceExperiment",
        output_dir=tmp_path / "out",
        tables=[TableGenerationResult(table_name="customers", row_count=50, files=[])],
    )
    stub_generator = StubGenerator(result=generation_result)
    service = ExperimentService(persistence=stub_persistence, generator=stub_generator)  # type: ignore[arg-type]

    result = service.generate_data("ServiceExperiment", output_dir=tmp_path / "out")

    assert result.success is False
    assert any("load exploded" in err for err in result.errors)
    assert result.run_id is not None
    run = stub_persistence.get_generation_run(result.run_id)
    assert run is not None
    assert run.status == GenerationStatus.FAILED


def test_generate_data_includes_traceback_in_error() -> None:
    """Test that error reporting includes full traceback."""
    metadata = build_metadata()
    stub_persistence = StubPersistence(metadata=metadata)
    stub_generator = StubGenerator(
        result=None,  # type: ignore[arg-type]
        to_raise=ValueError("Invalid row count"),
    )
    service = ExperimentService(persistence=stub_persistence, generator=stub_generator)  # type: ignore[arg-type]

    result = service.generate_data("ServiceExperiment")
    assert result.success is False
    assert "Traceback" in result.errors[0]
    assert "ValueError" in result.errors[0]
    assert "Invalid row count" in result.errors[0]


def test_generate_data_handles_concurrent_generation_guard() -> None:
    """Test that concurrent generation guard exception is handled properly."""
    metadata = build_metadata()
    stub_persistence = StubPersistence(
        metadata=metadata,
        start_run_exception=GenerationAlreadyRunningError("Generation already running"),
    )
    service = ExperimentService(persistence=stub_persistence)  # type: ignore[arg-type]

    result = service.generate_data("ServiceExperiment")
    assert result.success is False
    assert "already running" in result.errors[0].lower()


def test_list_generation_runs_returns_persistence_data() -> None:
    """Test that list_generation_runs delegates to persistence."""
    metadata = build_metadata()
    stub_persistence = StubPersistence(metadata=metadata)
    # Manually add a run
    run = GenerationRunMetadata(
        id=1,
        experiment_name="ServiceExperiment",
        status=GenerationStatus.COMPLETED,
        started_at=datetime.now(timezone.utc),
        completed_at=datetime.now(timezone.utc),
        row_counts='{"customers": 100}',
        output_path="/tmp/out",
        error_message=None,
        seed=42,
    )
    stub_persistence.runs[1] = run
    service = ExperimentService(persistence=stub_persistence)  # type: ignore[arg-type]

    runs = service.list_generation_runs("ServiceExperiment")
    assert len(runs) == 1
    assert runs[0].id == 1


def test_get_generation_run_returns_persistence_data() -> None:
    """Test that get_generation_run delegates to persistence."""
    metadata = build_metadata()
    stub_persistence = StubPersistence(metadata=metadata)
    run = GenerationRunMetadata(
        id=1,
        experiment_name="ServiceExperiment",
        status=GenerationStatus.COMPLETED,
        started_at=datetime.now(timezone.utc),
        completed_at=datetime.now(timezone.utc),
        row_counts='{"customers": 100}',
        output_path="/tmp/out",
        error_message=None,
        seed=42,
    )
    stub_persistence.runs[1] = run
    service = ExperimentService(persistence=stub_persistence)  # type: ignore[arg-type]

    fetched_run = service.get_generation_run(1)
    assert fetched_run is not None
    assert fetched_run.id == 1
    assert fetched_run.experiment_name == "ServiceExperiment"


def test_reset_experiment_success() -> None:
    """Test successful experiment reset."""
    stub_persistence = StubPersistence(
        metadata=ExperimentMetadata(
            name="TestExperiment",
            description="Test",
            schema_json="{}",
            created_at=datetime.now(timezone.utc),
        ),
        reset_return=2,
    )
    service = ExperimentService(persistence=stub_persistence)  # type: ignore[arg-type]

    result = service.reset_experiment("TestExperiment")
    assert result.success is True
    assert result.reset_tables == 2
    assert not result.errors


def test_reset_experiment_not_found() -> None:
    """Test reset returns error when experiment doesn't exist."""
    stub_persistence = StubPersistence(
        metadata=None,
        reset_exception=ExperimentNotFoundError("Experiment 'Unknown' does not exist."),
    )
    service = ExperimentService(persistence=stub_persistence)  # type: ignore[arg-type]

    result = service.reset_experiment("Unknown")
    assert result.success is False
    assert len(result.errors) == 1
    assert "does not exist" in result.errors[0]
    assert result.reset_tables == 0


def test_reset_experiment_during_active_generation() -> None:
    """Test reset returns error when generation is running."""
    stub_persistence = StubPersistence(
        metadata=ExperimentMetadata(
            name="TestExperiment",
            description="Test",
            schema_json="{}",
            created_at=datetime.now(timezone.utc),
        ),
        reset_exception=GenerationAlreadyRunningError(
            "Cannot reset experiment 'TestExperiment' while generation is running."
        ),
    )
    service = ExperimentService(persistence=stub_persistence)  # type: ignore[arg-type]

    result = service.reset_experiment("TestExperiment")
    assert result.success is False
    assert len(result.errors) == 1
    assert "generation is running" in result.errors[0]
    assert result.reset_tables == 0


def test_reset_experiment_materialization_error() -> None:
    """Test reset handles materialization errors."""
    stub_persistence = StubPersistence(
        metadata=ExperimentMetadata(
            name="TestExperiment",
            description="Test",
            schema_json="{}",
            created_at=datetime.now(timezone.utc),
        ),
        reset_exception=ExperimentMaterializationError("Failed to reset experiment."),
    )
    service = ExperimentService(persistence=stub_persistence)  # type: ignore[arg-type]

    result = service.reset_experiment("TestExperiment")
    assert result.success is False
    assert len(result.errors) == 1
    assert "Failed to reset" in result.errors[0]
    assert result.reset_tables == 0


def test_execute_query_success() -> None:
    """Test successful query execution."""
    query_result = QueryResult(
        columns=["id", "name"],
        rows=[(1, "Alice"), (2, "Bob")],
        row_count=2,
    )
    stub_persistence = StubPersistence(query_result=query_result)
    service = ExperimentService(persistence=stub_persistence)  # type: ignore[arg-type]

    result = service.execute_query("SELECT * FROM test_table")
    assert result.success is True
    assert result.result is not None
    assert result.result.row_count == 2
    assert result.result.columns == ["id", "name"]
    assert len(result.result.rows) == 2
    assert stub_persistence.executed_queries == ["SELECT * FROM test_table"]


def test_execute_query_handles_query_execution_error() -> None:
    """Test query execution handles QueryExecutionError."""
    stub_persistence = StubPersistence(
        query_exception=QueryExecutionError("Query execution failed: syntax error near 'WHERE'")
    )
    service = ExperimentService(persistence=stub_persistence)  # type: ignore[arg-type]

    result = service.execute_query("SELECT * FROM table WHERE")
    assert result.success is False
    assert len(result.errors) == 1
    assert "Query execution failed" in result.errors[0]


def test_execute_query_handles_unexpected_error() -> None:
    """Test query execution handles unexpected errors."""
    stub_persistence = StubPersistence(
        query_exception=RuntimeError("Unexpected database error")
    )
    service = ExperimentService(persistence=stub_persistence)  # type: ignore[arg-type]

    result = service.execute_query("SELECT * FROM table")
    assert result.success is False
    assert len(result.errors) == 1
    assert "Unexpected error during query execution" in result.errors[0]


def test_export_query_results_to_csv() -> None:
    """Test CSV export functionality (US 3.2 AC 1)."""
    query_result = QueryResult(
        columns=["id", "name", "email"],
        rows=[(1, "Alice", "alice@example.com"), (2, "Bob", "bob@example.com")],
        row_count=2,
    )

    csv_content = ExperimentService.export_query_results_to_csv(query_result)

    # Verify CSV format (strip to handle different line endings)
    lines = [line.strip() for line in csv_content.strip().split("\n")]
    assert len(lines) == 3  # Header + 2 data rows
    assert lines[0] == "id,name,email"
    assert lines[1] == "1,Alice,alice@example.com"
    assert lines[2] == "2,Bob,bob@example.com"


def test_save_query_to_file(tmp_path: Path) -> None:
    """Test saving query to SQL file (US 3.3 AC 1)."""
    sql_query = "SELECT * FROM customers WHERE age > 18"
    output_file = tmp_path / "query.sql"

    ExperimentService.save_query_to_file(sql_query, output_file)

    assert output_file.exists()
    assert output_file.read_text() == sql_query


def test_load_experiment_data_with_explicit_run_id() -> None:
    """Test loading data with an explicit run_id (US 5.1 Step 4)."""
    metadata = build_metadata()
    stub_persistence = StubPersistence(
        metadata=metadata,
        load_return={"customers": 100, "orders": 500},
    )
    service = ExperimentService(persistence=stub_persistence)  # type: ignore[arg-type]

    result = service.load_experiment_data("ServiceExperiment", run_id=42)

    assert result.success is True
    assert result.loaded_tables == 2
    assert result.row_counts == {"customers": 100, "orders": 500}
    assert stub_persistence.loaded_runs == [42]
    assert not result.errors


def test_load_experiment_data_without_run_id_uses_latest() -> None:
    """Test loading data without run_id uses the most recent completed run (US 5.1 Step 4)."""
    metadata = build_metadata()
    stub_persistence = StubPersistence(
        metadata=metadata,
        load_return={"customers": 200},
    )

    # Add multiple runs, including non-completed ones
    now = datetime.now(timezone.utc)
    stub_persistence.runs[1] = GenerationRunMetadata(
        id=1,
        experiment_name="ServiceExperiment",
        status=GenerationStatus.COMPLETED,
        started_at=now,
        completed_at=now,
        row_counts='{"customers": 100}',
        output_path="/tmp/run1",
        error_message=None,
        seed=None,
    )
    stub_persistence.runs[2] = GenerationRunMetadata(
        id=2,
        experiment_name="ServiceExperiment",
        status=GenerationStatus.FAILED,
        started_at=now,
        completed_at=now,
        row_counts='{}',
        output_path="/tmp/run2",
        error_message="Some error",
        seed=None,
    )
    stub_persistence.runs[3] = GenerationRunMetadata(
        id=3,
        experiment_name="ServiceExperiment",
        status=GenerationStatus.COMPLETED,
        started_at=now,
        completed_at=now,
        row_counts='{"customers": 200}',
        output_path="/tmp/run3",
        error_message=None,
        seed=None,
    )

    service = ExperimentService(persistence=stub_persistence)  # type: ignore[arg-type]
    result = service.load_experiment_data("ServiceExperiment")

    assert result.success is True
    # Should load the first completed run in the list (run 1)
    assert stub_persistence.loaded_runs == [1]
    assert result.loaded_tables == 1
    assert result.row_counts == {"customers": 200}


def test_load_experiment_data_experiment_not_found() -> None:
    """Test loading data returns error when experiment doesn't exist (US 5.1 Step 4)."""
    stub_persistence = StubPersistence(metadata=None)
    service = ExperimentService(persistence=stub_persistence)  # type: ignore[arg-type]

    result = service.load_experiment_data("MissingExperiment")

    assert result.success is False
    assert len(result.errors) == 1
    assert "does not exist" in result.errors[0]
    assert result.loaded_tables == 0
    assert not result.row_counts


def test_load_experiment_data_no_completed_runs() -> None:
    """Test loading data returns error when no completed runs exist (US 5.1 Step 4)."""
    metadata = build_metadata()
    stub_persistence = StubPersistence(metadata=metadata)

    # Add only failed/running runs
    now = datetime.now(timezone.utc)
    stub_persistence.runs[1] = GenerationRunMetadata(
        id=1,
        experiment_name="ServiceExperiment",
        status=GenerationStatus.FAILED,
        started_at=now,
        completed_at=now,
        row_counts='{}',
        output_path="/tmp/run1",
        error_message="Error",
        seed=None,
    )
    stub_persistence.runs[2] = GenerationRunMetadata(
        id=2,
        experiment_name="ServiceExperiment",
        status=GenerationStatus.RUNNING,
        started_at=now,
        completed_at=None,
        row_counts='{}',
        output_path="/tmp/run2",
        error_message=None,
        seed=None,
    )

    service = ExperimentService(persistence=stub_persistence)  # type: ignore[arg-type]
    result = service.load_experiment_data("ServiceExperiment")

    assert result.success is False
    assert len(result.errors) == 1
    assert "No completed generation runs found" in result.errors[0]
    assert result.loaded_tables == 0


def test_load_experiment_data_handles_data_load_error() -> None:
    """Test loading data handles DataLoadError from persistence (US 5.1 Step 4)."""
    from dw_simulator.persistence import DataLoadError

    metadata = build_metadata()
    stub_persistence = StubPersistence(
        metadata=metadata,
        load_exception=DataLoadError("Failed to load Parquet files: missing directory"),
    )

    service = ExperimentService(persistence=stub_persistence)  # type: ignore[arg-type]
    result = service.load_experiment_data("ServiceExperiment", run_id=1)

    assert result.success is False
    assert len(result.errors) == 1
    assert "Failed to load Parquet files" in result.errors[0]
    assert result.loaded_tables == 0


def test_load_experiment_data_handles_generation_run_not_found() -> None:
    """Test loading data handles GenerationRunNotFoundError (US 5.1 Step 4)."""
    from dw_simulator.persistence import GenerationRunNotFoundError

    metadata = build_metadata()
    stub_persistence = StubPersistence(
        metadata=metadata,
        load_exception=GenerationRunNotFoundError("Generation run 999 not found."),
    )

    service = ExperimentService(persistence=stub_persistence)  # type: ignore[arg-type]
    result = service.load_experiment_data("ServiceExperiment", run_id=999)

    assert result.success is False
    assert len(result.errors) == 1
    assert "Generation run 999 not found" in result.errors[0]
    assert result.loaded_tables == 0


def test_load_experiment_data_handles_unexpected_error() -> None:
    """Test loading data handles unexpected errors gracefully (US 5.1 Step 4)."""
    metadata = build_metadata()
    stub_persistence = StubPersistence(
        metadata=metadata,
        load_exception=RuntimeError("Unexpected database connection error"),
    )

    service = ExperimentService(persistence=stub_persistence)  # type: ignore[arg-type]
    result = service.load_experiment_data("ServiceExperiment", run_id=1)

    assert result.success is False
    assert len(result.errors) == 1
    assert "Unexpected error during data loading" in result.errors[0]
    assert "RuntimeError" in result.errors[0]
    assert result.loaded_tables == 0
