import json
from datetime import date

import pytest
from pydantic import ValidationError

from dw_simulator.schema import (
    ColumnSchema,
    DataType,
    ExperimentSchema,
    SchemaValidationResult,
    TableSchema,
    parse_experiment_schema,
    validate_experiment_payload,
)


def build_sample_experiment() -> dict:
    return {
        "name": "marketing_experiment",
        "description": "Valid schema for testing.",
        "tables": [
            {
                "name": "customers",
                "target_rows": 1000,
                "columns": [
                    {"name": "customer_id", "data_type": "INT", "is_unique": True},
                    {"name": "email", "data_type": "VARCHAR", "faker_rule": "internet.email"},
                    {
                        "name": "signup_date",
                        "data_type": "DATE",
                        "date_start": "2020-01-01",
                        "date_end": "2025-12-31",
                    },
                ],
            }
        ],
    }


def test_parse_experiment_schema_from_dict() -> None:
    payload = build_sample_experiment()
    schema = parse_experiment_schema(payload)
    assert isinstance(schema, ExperimentSchema)
    assert schema.name == "marketing_experiment"
    assert schema.tables[0].target_rows == 1000
    assert schema.tables[0].columns[0].is_unique is True


def test_parse_experiment_schema_from_json_string() -> None:
    payload = json.dumps(build_sample_experiment())
    schema = parse_experiment_schema(payload)
    assert schema.tables[0].columns[1].faker_rule == "internet.email"


def test_table_name_conflicts_with_sql_keyword() -> None:
    payload = build_sample_experiment()
    payload["tables"][0]["name"] = "select"
    with pytest.raises(ValidationError):
        parse_experiment_schema(payload)


def test_duplicate_columns_raise_validation_error() -> None:
    payload = build_sample_experiment()
    payload["tables"][0]["columns"].append({"name": "email", "data_type": "VARCHAR"})
    with pytest.raises(ValidationError):
        parse_experiment_schema(payload)


def test_numeric_range_validation() -> None:
    with pytest.raises(ValidationError):
        ColumnSchema(name="price", data_type="FLOAT", min_value=10, max_value=5)


def test_date_range_validation() -> None:
    with pytest.raises(ValidationError):
        ColumnSchema(
            name="event_date",
            data_type="DATE",
            date_start=date(2024, 1, 2),
            date_end=date(2024, 1, 1),
        )


def test_schema_validation_result_collects_errors() -> None:
    result = validate_experiment_payload({"name": "", "tables": []})
    assert result.is_valid is False
    assert len(result.errors) >= 1


def test_schema_validation_success_result() -> None:
    payload = build_sample_experiment()
    result = validate_experiment_payload(payload)
    assert result == SchemaValidationResult(is_valid=True, errors=[])
