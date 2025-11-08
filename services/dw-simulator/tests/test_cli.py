import json
import os
from pathlib import Path
from typing import Generator

import pytest
from typer.testing import CliRunner

from dw_simulator import __version__
from dw_simulator.cli import app, _summarize_distribution_columns

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


def test_experiment_create_command_reports_distributions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CLI surfaces distribution-configured columns after creation."""

    schema = {
        "name": "DistributionCLI",
        "tables": [
            {
                "name": "metrics",
                "target_rows": 10,
                "columns": [
                    {"name": "metric_id", "data_type": "INT", "is_unique": True},
                    {
                        "name": "score",
                        "data_type": "FLOAT",
                        "distribution": {
                            "type": "normal",
                            "parameters": {"mean": 50, "stddev": 5},
                        },
                    },
                ],
            }
        ],
    }
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps(schema))
    monkeypatch.setenv("DW_SIMULATOR_TARGET_DB_URL", f"sqlite:///{tmp_path/'warehouse.db'}")

    result = runner.invoke(app, ["experiment", "create", str(schema_path)])

    assert result.exit_code == 0
    assert "Experiment 'DistributionCLI' created" in result.stdout
    assert "Distribution-configured columns" in result.stdout
    assert "metrics.score" in result.stdout
    assert "normal" in result.stdout
    assert "mean=50" in result.stdout
    assert "stddev=5" in result.stdout


def test_summarize_distribution_columns_helper_handles_invalid_json() -> None:
    """Helper safely handles invalid JSON and formats parameter order."""

    assert _summarize_distribution_columns("not json") == []

    schema = {
        "name": "HelperSchema",
        "tables": [
            {
                "name": "metrics",
                "target_rows": 5,
                "columns": [
                    {
                        "name": "score",
                        "data_type": "INT",
                        "distribution": {
                            "type": "normal",
                            "parameters": {"stddev": 5, "mean": 25},
                        },
                    },
                    {
                        "name": "latency",
                        "data_type": "FLOAT",
                        "distribution": {
                            "type": "exponential",
                            "parameters": {"lambda": 2.0},
                        },
                    },
                ],
            }
        ],
    }

    lines = _summarize_distribution_columns(json.dumps(schema))

    assert lines == [
        " - metrics.score: normal (mean=25, stddev=5)",
        " - metrics.latency: exponential (lambda=2)",
    ]


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


def test_experiment_load_command_with_explicit_run_id(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Test loading experiment data with explicit run_id."""
    monkeypatch.setenv("DW_SIMULATOR_TARGET_DB_URL", f"sqlite:///{tmp_path/'warehouse.db'}")
    schema = {
        "name": "LoadMe",
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

    # Create experiment
    create_result = runner.invoke(app, ["experiment", "create", str(schema_path)])
    assert create_result.exit_code == 0

    # Generate data
    output_dir = tmp_path / "generated"
    generate_result = runner.invoke(
        app,
        ["experiment", "generate", "LoadMe", "--output-dir", str(output_dir)],
    )
    assert generate_result.exit_code == 0

    # Load data with explicit run_id=1
    load_result = runner.invoke(app, ["experiment", "load", "LoadMe", "--run-id", "1"])
    assert load_result.exit_code == 0
    assert "Loaded data for experiment 'LoadMe'" in load_result.stdout
    assert "customers: 5 rows" in load_result.stdout


def test_experiment_load_command_without_run_id(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Test loading experiment data using most recent run (no run_id specified)."""
    monkeypatch.setenv("DW_SIMULATOR_TARGET_DB_URL", f"sqlite:///{tmp_path/'warehouse.db'}")
    schema = {
        "name": "LoadLatest",
        "tables": [
            {
                "name": "orders",
                "target_rows": 3,
                "columns": [{"name": "order_id", "data_type": "INT", "is_unique": True}],
            }
        ],
    }
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps(schema))

    # Create and generate
    runner.invoke(app, ["experiment", "create", str(schema_path)])
    output_dir = tmp_path / "generated"
    runner.invoke(app, ["experiment", "generate", "LoadLatest", "--output-dir", str(output_dir)])

    # Load without specifying run_id (should use most recent)
    load_result = runner.invoke(app, ["experiment", "load", "LoadLatest"])
    assert load_result.exit_code == 0
    assert "Loaded data for experiment 'LoadLatest'" in load_result.stdout


def test_experiment_load_command_experiment_not_found(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Test loading data for non-existent experiment."""
    monkeypatch.setenv("DW_SIMULATOR_TARGET_DB_URL", f"sqlite:///{tmp_path/'warehouse.db'}")
    result = runner.invoke(app, ["experiment", "load", "NonExistent"])
    assert result.exit_code == 1
    assert "does not exist" in result.stderr


def test_experiment_load_command_no_completed_runs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Test loading when no completed generation runs exist."""
    monkeypatch.setenv("DW_SIMULATOR_TARGET_DB_URL", f"sqlite:///{tmp_path/'warehouse.db'}")
    schema = {
        "name": "NoRuns",
        "tables": [
            {
                "name": "items",
                "target_rows": 2,
                "columns": [{"name": "item_id", "data_type": "INT", "is_unique": True}],
            }
        ],
    }
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps(schema))

    # Create experiment but don't generate data
    runner.invoke(app, ["experiment", "create", str(schema_path)])

    # Try to load (should fail - no runs)
    result = runner.invoke(app, ["experiment", "load", "NoRuns"])
    assert result.exit_code == 1
    assert "No completed generation runs" in result.stderr
