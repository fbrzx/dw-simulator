import json
import os
from pathlib import Path
from typing import Generator

import pytest
from typer.testing import CliRunner

from dw_simulator import __version__
from dw_simulator.cli import app

runner = CliRunner()


@pytest.fixture(autouse=True)
def clear_env() -> Generator[None, None, None]:
    original_target = os.environ.pop("DW_SIMULATOR_TARGET_DB_URL", None)
    original_stage = os.environ.pop("DW_SIMULATOR_STAGE_BUCKET", None)
    try:
        yield
    finally:
        if original_target is not None:
            os.environ["DW_SIMULATOR_TARGET_DB_URL"] = original_target
        if original_stage is not None:
            os.environ["DW_SIMULATOR_STAGE_BUCKET"] = original_stage


def test_doctor_outputs_defaults() -> None:
    result = runner.invoke(app, ["doctor"])
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["version"] == __version__
    assert payload["target_db_url"].startswith("sqlite:")
    assert payload["stage_bucket"].startswith("s3://")


def test_doctor_respects_environment_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DW_SIMULATOR_TARGET_DB_URL", "postgresql://postgres:secret@localhost:5432/dw")
    monkeypatch.setenv("DW_SIMULATOR_STAGE_BUCKET", "s3://tmp/staging")
    result = runner.invoke(app, ["doctor"])
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["target_db_url"] == "postgresql://postgres:secret@localhost:5432/dw"
    assert payload["stage_bucket"] == "s3://tmp/staging"


def test_version_command_matches_package_metadata() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


def test_experiment_create_command_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    schema = {
        "name": "ExperimentCLI",
        "tables": [
            {
                "name": "customers",
                "target_rows": 10,
                "columns": [
                    {"name": "customer_id", "data_type": "INT", "is_unique": True},
                    {"name": "email", "data_type": "VARCHAR", "varchar_length": 64},
                ],
            }
        ],
    }
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps(schema))
    monkeypatch.setenv("DW_SIMULATOR_TARGET_DB_URL", f"sqlite:///{tmp_path/'warehouse.db'}")

    result = runner.invoke(app, ["experiment", "create", str(schema_path)])
    assert result.exit_code == 0
    assert "Experiment 'ExperimentCLI' created" in result.stdout


def test_experiment_create_command_handles_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    schema_path = tmp_path / "schema.json"
    schema_path.write_text("{invalid json")
    monkeypatch.setenv("DW_SIMULATOR_TARGET_DB_URL", f"sqlite:///{tmp_path/'warehouse.db'}")

    result = runner.invoke(app, ["experiment", "create", str(schema_path)])
    assert result.exit_code == 1
    assert "Invalid JSON" in result.stderr


def test_experiment_delete_command(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DW_SIMULATOR_TARGET_DB_URL", f"sqlite:///{tmp_path/'warehouse.db'}")
    schema = {
        "name": "DeleteMe",
        "tables": [
            {
                "name": "customers",
                "target_rows": 10,
                "columns": [{"name": "customer_id", "data_type": "INT", "is_unique": True}],
            }
        ],
    }
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps(schema))
    create_result = runner.invoke(app, ["experiment", "create", str(schema_path)])
    assert create_result.exit_code == 0

    delete_result = runner.invoke(app, ["experiment", "delete", "DeleteMe"])
    assert delete_result.exit_code == 0
    assert "deleted" in delete_result.stdout


def test_experiment_delete_command_handles_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DW_SIMULATOR_TARGET_DB_URL", f"sqlite:///{tmp_path/'warehouse.db'}")
    result = runner.invoke(app, ["experiment", "delete", "unknown"])
    assert result.exit_code == 1
    assert "does not exist" in result.stderr


def test_experiment_generate_command(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DW_SIMULATOR_TARGET_DB_URL", f"sqlite:///{tmp_path/'warehouse.db'}")
    schema = {
        "name": "GenerateMe",
        "tables": [
            {
                "name": "customers",
                "target_rows": 5,
                "columns": [{"name": "customer_id", "data_type": "INT", "is_unique": True}],
            }
        ],
    }
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps(schema))
    create_result = runner.invoke(app, ["experiment", "create", str(schema_path)])
    assert create_result.exit_code == 0

    output_dir = tmp_path / "generated"
    result = runner.invoke(
        app,
        [
            "experiment",
            "generate",
            "GenerateMe",
            "--rows",
            "customers=3",
            "--seed",
            "123",
            "--output-dir",
            str(output_dir),
        ],
    )
    assert result.exit_code == 0
    assert "Generated data for experiment 'GenerateMe'" in result.stdout


def test_import_sql_command(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DW_SIMULATOR_TARGET_DB_URL", f"sqlite:///{tmp_path/'warehouse.db'}")
    sql_path = tmp_path / "schema.sql"
    sql_path.write_text("CREATE TABLE demo (id BIGINT PRIMARY KEY, name VARCHAR(50));")
    result = runner.invoke(
        app,
        [
            "experiment",
            "import-sql",
            str(sql_path),
            "--name",
            "demo_exp",
            "--dialect",
            "redshift",
            "--target-rows",
            "10",
        ],
    )
    assert result.exit_code == 0
    assert "demo_exp" in result.stdout


def test_import_sql_command_invalid_dialect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    sql_path = tmp_path / "schema.sql"
    sql_path.write_text("CREATE TABLE demo (id BIGINT);")
    result = runner.invoke(
        app,
        [
            "experiment",
            "import-sql",
            str(sql_path),
            "--name",
            "demo_exp",
            "--dialect",
            "oracle",
        ],
    )
    assert result.exit_code == 1
    assert "Unsupported dialect" in result.stderr
