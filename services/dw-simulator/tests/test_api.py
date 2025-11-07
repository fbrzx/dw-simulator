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


def test_import_sql_endpoint_with_composite_key_includes_warnings(client: TestClient) -> None:
    """Test that importing SQL with composite primary keys returns warnings in the response."""
    sql_payload = {
        "name": "composite_key_exp",
        "sql": "CREATE TABLE orders (customer_id INT, order_id INT, amount DECIMAL, PRIMARY KEY (customer_id, order_id));",
        "dialect": "redshift",
        "target_rows": 5,
    }
    response = client.post("/api/experiments/import-sql", json=sql_payload)
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "composite_key_exp"
    assert "warnings" in data
    assert isinstance(data["warnings"], list)
    # Should have at least one warning about composite key handling
    assert len(data["warnings"]) > 0
    # Verify warning mentions surrogate key
    warning_text = " ".join(data["warnings"]).lower()
    assert "composite" in warning_text or "surrogate" in warning_text or "_row_id" in warning_text


def test_import_sql_endpoint_without_composite_key_has_empty_warnings(client: TestClient) -> None:
    """Test that importing SQL without composite keys returns empty warnings."""
    sql_payload = {
        "name": "simple_exp",
        "sql": "CREATE TABLE demo (id BIGINT PRIMARY KEY, name VARCHAR(50));",
        "dialect": "redshift",
        "target_rows": 5,
    }
    response = client.post("/api/experiments/import-sql", json=sql_payload)
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "simple_exp"
    assert "warnings" in data
    assert data["warnings"] == []


def test_list_experiments_includes_warnings(client: TestClient) -> None:
    """Test that GET /api/experiments includes warnings for each experiment."""
    # Create experiment with composite key (should have warnings)
    sql_payload_with_warnings = {
        "name": "exp_with_warnings",
        "sql": "CREATE TABLE multi_key (id1 INT, id2 INT, data VARCHAR(50), PRIMARY KEY (id1, id2));",
        "dialect": "redshift",
        "target_rows": 3,
    }
    client.post("/api/experiments/import-sql", json=sql_payload_with_warnings)

    # Create experiment without composite key (should have no warnings)
    sql_payload_no_warnings = {
        "name": "exp_no_warnings",
        "sql": "CREATE TABLE simple (id INT PRIMARY KEY, name VARCHAR(50));",
        "dialect": "redshift",
        "target_rows": 3,
    }
    client.post("/api/experiments/import-sql", json=sql_payload_no_warnings)

    # List experiments and verify warnings are included
    response = client.get("/api/experiments")
    assert response.status_code == 200
    experiments = response.json()["experiments"]
    assert len(experiments) == 2

    # Find our experiments
    exp_with_warnings = next(e for e in experiments if e["name"] == "exp_with_warnings")
    exp_no_warnings = next(e for e in experiments if e["name"] == "exp_no_warnings")

    # Verify warnings field exists and has expected content
    assert "warnings" in exp_with_warnings
    assert isinstance(exp_with_warnings["warnings"], list)
    assert len(exp_with_warnings["warnings"]) > 0

    assert "warnings" in exp_no_warnings
    assert isinstance(exp_no_warnings["warnings"], list)
    assert len(exp_no_warnings["warnings"]) == 0


def test_import_sql_multi_table_composite_keys_aggregates_warnings(client: TestClient) -> None:
    """Test that warnings from multiple tables with composite keys are all included."""
    sql_payload = {
        "name": "multi_table_exp",
        "sql": """
            CREATE TABLE orders (customer_id INT, order_id INT, amount DECIMAL, PRIMARY KEY (customer_id, order_id));
            CREATE TABLE shipments (order_id INT, shipment_id INT, tracking VARCHAR(50), PRIMARY KEY (order_id, shipment_id));
        """,
        "dialect": "redshift",
        "target_rows": 5,
    }
    response = client.post("/api/experiments/import-sql", json=sql_payload)
    assert response.status_code == 201
    data = response.json()
    assert "warnings" in data
    # Should have warnings from both tables
    assert len(data["warnings"]) >= 2
    # Each warning should mention the specific table
    warning_text = " ".join(data["warnings"])
    assert "orders" in warning_text.lower() or "shipments" in warning_text.lower()
