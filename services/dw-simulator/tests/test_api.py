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
