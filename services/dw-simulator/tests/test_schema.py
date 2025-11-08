import json
from datetime import date

import pytest
from pydantic import ValidationError

from dw_simulator.schema import (
    ColumnSchema,
    ExperimentSchema,
    SchemaValidationResult,
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


def test_table_schema_with_composite_keys() -> None:
    """Test that TableSchema accepts valid composite_keys metadata."""
    payload = build_sample_experiment()
    payload["tables"][0]["composite_keys"] = [["customer_id", "email"]]
    schema = parse_experiment_schema(payload)
    assert schema.tables[0].composite_keys == [["customer_id", "email"]]


def test_table_schema_with_multiple_composite_keys() -> None:
    """Test that TableSchema accepts multiple composite key groups."""
    payload = build_sample_experiment()
    payload["tables"][0]["composite_keys"] = [["customer_id", "email"], ["email", "signup_date"]]
    schema = parse_experiment_schema(payload)
    assert len(schema.tables[0].composite_keys) == 2
    assert schema.tables[0].composite_keys[0] == ["customer_id", "email"]
    assert schema.tables[0].composite_keys[1] == ["email", "signup_date"]


def test_table_schema_with_warnings() -> None:
    """Test that TableSchema accepts and stores warnings."""
    payload = build_sample_experiment()
    payload["tables"][0]["warnings"] = [
        "Table 'customers' has composite primary key (customer_id, email). A surrogate '_row_id' column was added for uniqueness."
    ]
    schema = parse_experiment_schema(payload)
    assert len(schema.tables[0].warnings) == 1
    assert "surrogate" in schema.tables[0].warnings[0]


def test_table_schema_with_composite_keys_and_warnings() -> None:
    """Test that both composite_keys and warnings can be used together."""
    payload = build_sample_experiment()
    payload["tables"][0]["composite_keys"] = [["customer_id", "email"]]
    payload["tables"][0]["warnings"] = ["Composite key detected."]
    schema = parse_experiment_schema(payload)
    assert schema.tables[0].composite_keys == [["customer_id", "email"]]
    assert schema.tables[0].warnings == ["Composite key detected."]


def test_table_schema_composite_keys_invalid_column_reference() -> None:
    """Test that composite_keys validation rejects unknown column names."""
    payload = build_sample_experiment()
    payload["tables"][0]["composite_keys"] = [["customer_id", "nonexistent_column"]]
    with pytest.raises(ValidationError, match="unknown column"):
        parse_experiment_schema(payload)


def test_table_schema_composite_keys_empty_group() -> None:
    """Test that empty composite key groups are rejected."""
    payload = build_sample_experiment()
    payload["tables"][0]["composite_keys"] = [[]]
    with pytest.raises(ValidationError, match="empty composite key group"):
        parse_experiment_schema(payload)


def test_table_schema_backward_compatibility() -> None:
    """Test that schemas without composite_keys and warnings still work."""
    payload = build_sample_experiment()
    # Don't include composite_keys or warnings fields
    schema = parse_experiment_schema(payload)
    assert schema.tables[0].composite_keys is None
    assert schema.tables[0].warnings == []


def test_table_schema_warnings_default_empty_list() -> None:
    """Test that warnings defaults to empty list when not provided."""
    payload = build_sample_experiment()
    schema = parse_experiment_schema(payload)
    assert schema.tables[0].warnings == []
    assert isinstance(schema.tables[0].warnings, list)


# US 5.2 Phase 3: Warehouse selection tests


def test_experiment_schema_with_target_warehouse_sqlite() -> None:
    """Test that ExperimentSchema accepts valid sqlite target_warehouse."""
    payload = build_sample_experiment()
    payload["target_warehouse"] = "sqlite"
    schema = parse_experiment_schema(payload)
    assert schema.target_warehouse == "sqlite"


def test_experiment_schema_with_target_warehouse_redshift() -> None:
    """Test that ExperimentSchema accepts valid redshift target_warehouse."""
    payload = build_sample_experiment()
    payload["target_warehouse"] = "redshift"
    schema = parse_experiment_schema(payload)
    assert schema.target_warehouse == "redshift"


def test_experiment_schema_with_target_warehouse_snowflake() -> None:
    """Test that ExperimentSchema accepts valid snowflake target_warehouse."""
    payload = build_sample_experiment()
    payload["target_warehouse"] = "snowflake"
    schema = parse_experiment_schema(payload)
    assert schema.target_warehouse == "snowflake"


def test_experiment_schema_target_warehouse_case_insensitive() -> None:
    """Test that target_warehouse is normalized to lowercase."""
    payload = build_sample_experiment()
    payload["target_warehouse"] = "SQLite"
    schema = parse_experiment_schema(payload)
    assert schema.target_warehouse == "sqlite"

    payload["target_warehouse"] = "REDSHIFT"
    schema = parse_experiment_schema(payload)
    assert schema.target_warehouse == "redshift"

    payload["target_warehouse"] = "SnowFlake"
    schema = parse_experiment_schema(payload)
    assert schema.target_warehouse == "snowflake"


def test_experiment_schema_invalid_target_warehouse() -> None:
    """Test that invalid target_warehouse values are rejected."""
    payload = build_sample_experiment()
    payload["target_warehouse"] = "invalid_warehouse"
    with pytest.raises(ValidationError, match="Unsupported warehouse type"):
        parse_experiment_schema(payload)


def test_experiment_schema_target_warehouse_optional() -> None:
    """Test that target_warehouse is optional and defaults to None."""
    payload = build_sample_experiment()
    # Don't include target_warehouse field
    schema = parse_experiment_schema(payload)
    assert schema.target_warehouse is None


def test_experiment_schema_target_warehouse_null() -> None:
    """Test that target_warehouse can be explicitly set to null."""
    payload = build_sample_experiment()
    payload["target_warehouse"] = None
    schema = parse_experiment_schema(payload)
    assert schema.target_warehouse is None
