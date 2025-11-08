from pathlib import Path
from typing import Generator

import pytest
from fastapi.testclient import TestClient

from dw_simulator.api import create_app
from dw_simulator.service import ExperimentService


def sample_schema(name: str = "ApiExperiment") -> dict:
    return {
        "name": name,
        "tables": [
            {
                "name": "customers",
                "target_rows": 1,
                "columns": [
                    {"name": "customer_id", "data_type": "INT", "is_unique": True},
                    {"name": "email", "data_type": "VARCHAR", "varchar_length": 64},
                ],
            }
        ],
    }


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    db_path = tmp_path / "api.db"
    monkeypatch.setenv("DW_SIMULATOR_TARGET_DB_URL", f"sqlite:///{db_path}")
    service = ExperimentService()
    app = create_app(service)
    with TestClient(app) as test_client:
        yield test_client


def test_list_experiments_initially_empty(client: TestClient) -> None:
    response = client.get("/api/experiments")
    assert response.status_code == 200
    assert response.json() == {"experiments": []}


def test_create_and_delete_experiment(client: TestClient) -> None:
    create_response = client.post("/api/experiments", json=sample_schema())
    assert create_response.status_code == 201
    body = create_response.json()
    assert body["name"] == "ApiExperiment"

    list_response = client.get("/api/experiments")
    assert list_response.status_code == 200
    experiments = list_response.json()["experiments"]
    assert len(experiments) == 1

    delete_response = client.delete("/api/experiments/ApiExperiment")
    assert delete_response.status_code == 200
    assert delete_response.json()["dropped_tables"] == 1

    missing_delete = client.delete("/api/experiments/ApiExperiment")
    assert missing_delete.status_code == 404


def test_generate_endpoint(client: TestClient, tmp_path: Path) -> None:
    client.post("/api/experiments", json=sample_schema("GenerateApi"))
    response = client.post(
        "/api/experiments/GenerateApi/generate",
        json={"rows": {"customers": 4}, "seed": 42, "output_dir": str(tmp_path / "out")},
    )
    assert response.status_code == 202
    payload = response.json()
    assert payload["experiment"] == "GenerateApi"
    assert payload["tables"][0]["row_count"] == 4


def test_import_sql_endpoint(client: TestClient) -> None:
    sql_payload = {
        "name": "sql_exp",
        "sql": "CREATE TABLE demo (id BIGINT PRIMARY KEY, name VARCHAR(50));",
        "dialect": "redshift",
        "target_rows": 5,
    }
    response = client.post("/api/experiments/import-sql", json=sql_payload)
    assert response.status_code == 201
    assert response.json()["name"] == "sql_exp"


def test_import_sql_endpoint_invalid_dialect(client: TestClient) -> None:
    response = client.post(
        "/api/experiments/import-sql",
        json={"name": "sql_exp", "sql": "CREATE TABLE demo (id BIGINT);", "dialect": "oracle"},
    )
    assert response.status_code == 400


def test_import_sql_with_composite_key_returns_warnings(client: TestClient) -> None:
    """Test that importing SQL with composite primary keys returns warnings."""
    sql_payload = {
        "name": "composite_test",
        "sql": "CREATE TABLE orders (order_id INT, line_id INT, PRIMARY KEY (order_id, line_id));",
        "dialect": "redshift",
        "target_rows": 10,
    }
    response = client.post("/api/experiments/import-sql", json=sql_payload)
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "composite_test"
    assert "warnings" in body
    assert len(body["warnings"]) > 0
    # Verify warning mentions composite key and surrogate
    warning_text = " ".join(body["warnings"])
    assert "composite" in warning_text.lower() or "_row_id" in warning_text


def test_import_sql_without_composite_key_has_empty_warnings(client: TestClient) -> None:
    """Test that importing SQL without composite keys returns empty warnings list."""
    sql_payload = {
        "name": "single_pk_test",
        "sql": "CREATE TABLE users (user_id BIGINT PRIMARY KEY, name VARCHAR(50));",
        "dialect": "redshift",
        "target_rows": 5,
    }
    response = client.post("/api/experiments/import-sql", json=sql_payload)
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "single_pk_test"
    assert "warnings" in body
    assert body["warnings"] == []


def test_list_experiments_includes_warnings(client: TestClient) -> None:
    """Test that GET /api/experiments includes warnings for each experiment."""
    # Create experiment with composite key
    sql_payload = {
        "name": "warning_exp",
        "sql": "CREATE TABLE items (item_id INT, store_id INT, PRIMARY KEY (item_id, store_id));",
        "dialect": "redshift",
        "target_rows": 10,
    }
    client.post("/api/experiments/import-sql", json=sql_payload)

    # List experiments and verify warnings are included
    response = client.get("/api/experiments")
    assert response.status_code == 200
    experiments = response.json()["experiments"]
    assert len(experiments) == 1
    assert "warnings" in experiments[0]
    assert len(experiments[0]["warnings"]) > 0


def test_list_experiments_with_no_warnings(client: TestClient) -> None:
    """Test that experiments without warnings return empty warnings list."""
    # Create regular experiment
    client.post("/api/experiments", json=sample_schema("NoWarningExp"))

    # List experiments and verify empty warnings
    response = client.get("/api/experiments")
    assert response.status_code == 200
    experiments = response.json()["experiments"]
    assert len(experiments) == 1
    assert "warnings" in experiments[0]
    assert experiments[0]["warnings"] == []


def test_list_experiments_includes_distribution_summary(client: TestClient) -> None:
    """GET /api/experiments surfaces distribution-configured columns."""

    schema = {
        "name": "DistributionApi",
        "tables": [
            {
                "name": "metrics",
                "target_rows": 5,
                "columns": [
                    {"name": "metric_id", "data_type": "INT", "is_unique": True},
                    {
                        "name": "score",
                        "data_type": "FLOAT",
                        "distribution": {
                            "type": "beta",
                            "parameters": {"alpha": 2, "beta": 5},
                        },
                    },
                ],
            }
        ],
    }

    create_response = client.post("/api/experiments", json=schema)
    assert create_response.status_code == 201

    list_response = client.get("/api/experiments")
    assert list_response.status_code == 200
    experiments = list_response.json()["experiments"]
    assert len(experiments) == 1

    distribution_summary = experiments[0].get("distributions")
    assert distribution_summary == [
        {
            "table": "metrics",
            "column": "score",
            "type": "beta",
            "parameters": {"alpha": 2, "beta": 5},
        }
    ]


def test_list_experiments_distribution_summary_multiple_columns(client: TestClient) -> None:
    """Distribution summaries include multiple columns with sorted parameters."""

    schema = {
        "name": "DistributionApiMulti",
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
                            "parameters": {"stddev": 7, "mean": 40},
                        },
                    },
                    {
                        "name": "latency",
                        "data_type": "FLOAT",
                        "distribution": {
                            "type": "exponential",
                            "parameters": {"lambda": 1.25},
                        },
                    },
                ],
            }
        ],
    }

    create_response = client.post("/api/experiments", json=schema)
    assert create_response.status_code == 201

    list_response = client.get("/api/experiments")
    assert list_response.status_code == 200
    experiments = list_response.json()["experiments"]

    assert experiments[0]["distributions"] == [
        {
            "table": "metrics",
            "column": "score",
            "type": "normal",
            "parameters": {"mean": 40, "stddev": 7},
        },
        {
            "table": "metrics",
            "column": "latency",
            "type": "exponential",
            "parameters": {"lambda": 1.25},
        },
    ]


def test_reset_experiment_success(client: TestClient) -> None:
    """Test successful experiment reset via API."""
    # Create experiment first
    client.post("/api/experiments", json=sample_schema("ResetTest"))

    # Reset it
    response = client.post("/api/experiments/ResetTest/reset")
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "ResetTest"
    assert "reset_tables" in body
    assert body["reset_tables"] >= 0  # Could be 0 or more depending on schema


def test_reset_experiment_not_found(client: TestClient) -> None:
    """Test reset returns 404 when experiment doesn't exist."""
    response = client.post("/api/experiments/NonExistent/reset")
    assert response.status_code == 404
    body = response.json()
    assert "detail" in body
    assert any("does not exist" in str(err).lower() for err in body["detail"])


def test_reset_experiment_during_generation(client: TestClient) -> None:
    """Test reset returns 409 when generation is running."""
    from unittest.mock import patch
    from dw_simulator.persistence import GenerationAlreadyRunningError

    # Create experiment
    client.post("/api/experiments", json=sample_schema("GenRunning"))

    # Mock persistence to raise GenerationAlreadyRunningError
    with patch("dw_simulator.service.ExperimentService.reset_experiment") as mock_reset:
        from dw_simulator.service import ExperimentResetResult
        mock_reset.return_value = ExperimentResetResult(
            success=False,
            errors=["Cannot reset experiment 'GenRunning' while generation is running."],
        )

        response = client.post("/api/experiments/GenRunning/reset")
        assert response.status_code == 409  # Conflict
        body = response.json()
        assert "detail" in body
        assert any("generation is running" in str(err).lower() for err in body["detail"])


def test_load_experiment_with_explicit_run_id(client: TestClient, tmp_path: Path) -> None:
    """Test loading experiment data with explicit run_id via API."""
    # Create experiment
    client.post("/api/experiments", json=sample_schema("LoadApi"))

    # Generate data
    output_dir = tmp_path / "generated"
    client.post(
        "/api/experiments/LoadApi/generate",
        json={"output_dir": str(output_dir)},
    )

    # Load data with explicit run_id=1
    response = client.post("/api/experiments/LoadApi/load", json={"run_id": 1})
    assert response.status_code == 200
    body = response.json()
    assert body["experiment"] == "LoadApi"
    assert body["loaded_tables"] == 1
    assert "customers" in body["row_counts"]
    assert body["row_counts"]["customers"] == 1


def test_load_experiment_without_run_id(client: TestClient, tmp_path: Path) -> None:
    """Test loading experiment data using most recent run (no run_id)."""
    # Create experiment
    client.post("/api/experiments", json=sample_schema("LoadLatestApi"))

    # Generate data
    output_dir = tmp_path / "generated"
    client.post(
        "/api/experiments/LoadLatestApi/generate",
        json={"output_dir": str(output_dir)},
    )

    # Load without specifying run_id (should use most recent)
    response = client.post("/api/experiments/LoadLatestApi/load", json={})
    assert response.status_code == 200
    body = response.json()
    assert body["experiment"] == "LoadLatestApi"
    assert body["loaded_tables"] == 1


def test_load_experiment_not_found(client: TestClient) -> None:
    """Test load returns 404 when experiment doesn't exist."""
    response = client.post("/api/experiments/NonExistent/load", json={})
    assert response.status_code == 404
    body = response.json()
    assert "detail" in body
    assert any("does not exist" in str(err).lower() for err in body["detail"])


def test_load_experiment_no_completed_runs(client: TestClient) -> None:
    """Test load returns 409 when no completed generation runs exist."""
    # Create experiment but don't generate data
    client.post("/api/experiments", json=sample_schema("NoRunsApi"))

    # Try to load (should fail - no runs)
    response = client.post("/api/experiments/NoRunsApi/load", json={})
    assert response.status_code == 409
    body = response.json()
    assert "detail" in body
    assert any("no completed generation runs" in str(err).lower() for err in body["detail"])
