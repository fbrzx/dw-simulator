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
    ExperimentAlreadyExistsError,
    ExperimentMaterializationError,
    ExperimentMetadata,
    ExperimentNotFoundError,
)
from dw_simulator.schema import ExperimentSchema, TableSchema, ColumnSchema
from dw_simulator.service import ExperimentGenerateResult, ExperimentService


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
    schema: ExperimentSchema | None = None

    def __post_init__(self) -> None:
        self.recorded_schema: ExperimentSchema | None = None
        self.listed: list[ExperimentMetadata] = []

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

    def get_experiment_metadata(self, name: str) -> ExperimentMetadata | None:
        return self.metadata


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


def test_generate_data_success(tmp_path: Path) -> None:
    metadata = build_metadata()
    stub_persistence = StubPersistence(metadata=metadata)
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
