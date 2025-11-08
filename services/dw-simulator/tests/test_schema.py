import json
from datetime import date

import pytest
from pydantic import ValidationError

from dw_simulator.schema import (
    ColumnSchema,
    ExperimentSchema,
    ForeignKeyConfig,
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


def test_column_schema_accepts_distribution_config() -> None:
    """ColumnSchema accepts valid distribution configuration for numeric columns."""
    column = ColumnSchema(
        name="amount",
        data_type="FLOAT",
        distribution={
            "type": "normal",
            "parameters": {"mean": 10.0, "stddev": 2.5},
        },
    )
    assert column.distribution is not None
    assert column.distribution.type == "normal"
    assert column.distribution.parameters == {"mean": 10.0, "stddev": 2.5}


def test_column_schema_distribution_requires_numeric_column() -> None:
    """Distribution config is rejected for non-numeric data types."""
    with pytest.raises(ValidationError, match="numeric columns"):
        ColumnSchema(
            name="category",
            data_type="VARCHAR",
            distribution={"type": "normal", "parameters": {"mean": 0, "stddev": 1}},
        )


def test_column_schema_distribution_requires_expected_parameters() -> None:
    """Distribution config validates parameter requirements per distribution type."""
    with pytest.raises(ValidationError, match="stddev"):
        ColumnSchema(
            name="amount",
            data_type="FLOAT",
            distribution={"type": "normal", "parameters": {"mean": 10}},
        )


def test_column_schema_distribution_rejects_unknown_type() -> None:
    """Distribution config rejects unsupported distribution types."""
    with pytest.raises(ValidationError, match="Unsupported distribution type"):
        ColumnSchema(
            name="amount",
            data_type="FLOAT",
            distribution={"type": "lognormal", "parameters": {"mean": 0, "sigma": 1}},
        )


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


# ============================================================================
# Foreign Key Tests
# ============================================================================


def build_experiment_with_foreign_keys() -> dict:
    """Helper to create a valid experiment with FK relationships."""
    return {
        "name": "ecommerce",
        "tables": [
            {
                "name": "customers",
                "target_rows": 100,
                "columns": [
                    {"name": "customer_id", "data_type": "INT", "is_unique": True, "required": True},
                    {"name": "email", "data_type": "VARCHAR", "faker_rule": "internet.email"},
                ],
            },
            {
                "name": "orders",
                "target_rows": 500,
                "columns": [
                    {"name": "order_id", "data_type": "INT", "is_unique": True, "required": True},
                    {
                        "name": "customer_id",
                        "data_type": "INT",
                        "required": True,
                        "foreign_key": {
                            "references_table": "customers",
                            "references_column": "customer_id",
                        },
                    },
                    {"name": "order_total", "data_type": "FLOAT"},
                ],
            },
        ],
    }


def test_valid_foreign_key_relationship() -> None:
    """Test that valid FK relationships are accepted."""
    payload = build_experiment_with_foreign_keys()
    schema = parse_experiment_schema(payload)

    assert len(schema.tables) == 2
    orders_table = schema.tables[1]
    assert len(orders_table.foreign_keys) == 1

    col_name, fk_config = orders_table.foreign_keys[0]
    assert col_name == "customer_id"
    assert fk_config.references_table == "customers"
    assert fk_config.references_column == "customer_id"


def test_foreign_key_references_nonexistent_table() -> None:
    """Test that FK referencing non-existent table raises ValidationError."""
    payload = build_experiment_with_foreign_keys()
    payload["tables"][1]["columns"][1]["foreign_key"]["references_table"] = "nonexistent_table"

    with pytest.raises(ValidationError, match="references unknown table 'nonexistent_table'"):
        parse_experiment_schema(payload)


def test_foreign_key_references_nonexistent_column() -> None:
    """Test that FK referencing non-existent column raises ValidationError."""
    payload = build_experiment_with_foreign_keys()
    payload["tables"][1]["columns"][1]["foreign_key"]["references_column"] = "nonexistent_column"

    with pytest.raises(ValidationError, match="references unknown column 'nonexistent_column'"):
        parse_experiment_schema(payload)


def test_foreign_key_references_non_unique_column() -> None:
    """Test that FK must reference a unique column."""
    payload = build_experiment_with_foreign_keys()
    # Change the referenced column to non-unique
    payload["tables"][0]["columns"][0]["is_unique"] = False

    with pytest.raises(ValidationError, match="is not marked as unique"):
        parse_experiment_schema(payload)


def test_foreign_key_nullable_field() -> None:
    """Test that FK can be configured as nullable."""
    payload = build_experiment_with_foreign_keys()
    payload["tables"][1]["columns"][1]["foreign_key"]["nullable"] = True

    schema = parse_experiment_schema(payload)
    orders_table = schema.tables[1]
    _, fk_config = orders_table.foreign_keys[0]
    assert fk_config.nullable is True


def test_foreign_key_identifier_validation() -> None:
    """Test that FK table/column names are validated as identifiers."""
    with pytest.raises(ValidationError, match="must be alphanumeric"):
        ForeignKeyConfig(
            references_table="invalid table!",
            references_column="customer_id"
        )

    with pytest.raises(ValidationError, match="must be alphanumeric"):
        ForeignKeyConfig(
            references_table="customers",
            references_column="invalid column!"
        )


def test_circular_foreign_key_dependency_detected() -> None:
    """Test that circular FK dependencies are detected and rejected."""
    payload = {
        "name": "circular_test",
        "tables": [
            {
                "name": "table_a",
                "target_rows": 100,
                "columns": [
                    {"name": "a_id", "data_type": "INT", "is_unique": True},
                    {
                        "name": "b_ref",
                        "data_type": "INT",
                        "required": True,
                        "foreign_key": {
                            "references_table": "table_b",
                            "references_column": "b_id",
                        },
                    },
                ],
            },
            {
                "name": "table_b",
                "target_rows": 100,
                "columns": [
                    {"name": "b_id", "data_type": "INT", "is_unique": True},
                    {
                        "name": "a_ref",
                        "data_type": "INT",
                        "required": True,
                        "foreign_key": {
                            "references_table": "table_a",
                            "references_column": "a_id",
                        },
                    },
                ],
            },
        ],
    }

    with pytest.raises(ValidationError, match="Circular foreign key dependency detected"):
        parse_experiment_schema(payload)


def test_circular_dependency_broken_by_nullable_fk() -> None:
    """Test that circular dependencies can be broken by making one FK nullable."""
    payload = {
        "name": "circular_nullable",
        "tables": [
            {
                "name": "table_a",
                "target_rows": 100,
                "columns": [
                    {"name": "a_id", "data_type": "INT", "is_unique": True},
                    {
                        "name": "b_ref",
                        "data_type": "INT",
                        "required": False,  # Nullable FK breaks the cycle
                        "foreign_key": {
                            "references_table": "table_b",
                            "references_column": "b_id",
                        },
                    },
                ],
            },
            {
                "name": "table_b",
                "target_rows": 100,
                "columns": [
                    {"name": "b_id", "data_type": "INT", "is_unique": True},
                    {
                        "name": "a_ref",
                        "data_type": "INT",
                        "required": True,
                        "foreign_key": {
                            "references_table": "table_a",
                            "references_column": "a_id",
                        },
                    },
                ],
            },
        ],
    }

    # Should not raise ValidationError since one FK is nullable
    schema = parse_experiment_schema(payload)
    assert len(schema.tables) == 2


def test_multi_table_foreign_key_chain() -> None:
    """Test that multi-level FK chains (A -> B -> C) are supported."""
    payload = {
        "name": "fk_chain",
        "tables": [
            {
                "name": "customers",
                "target_rows": 50,
                "columns": [
                    {"name": "customer_id", "data_type": "INT", "is_unique": True},
                    {"name": "name", "data_type": "VARCHAR"},
                ],
            },
            {
                "name": "orders",
                "target_rows": 200,
                "columns": [
                    {"name": "order_id", "data_type": "INT", "is_unique": True},
                    {
                        "name": "customer_id",
                        "data_type": "INT",
                        "foreign_key": {
                            "references_table": "customers",
                            "references_column": "customer_id",
                        },
                    },
                ],
            },
            {
                "name": "order_items",
                "target_rows": 1000,
                "columns": [
                    {"name": "item_id", "data_type": "INT", "is_unique": True},
                    {
                        "name": "order_id",
                        "data_type": "INT",
                        "foreign_key": {
                            "references_table": "orders",
                            "references_column": "order_id",
                        },
                    },
                    {"name": "quantity", "data_type": "INT"},
                ],
            },
        ],
    }

    schema = parse_experiment_schema(payload)
    assert len(schema.tables) == 3

    # Verify FK chains
    orders_table = schema.tables[1]
    assert len(orders_table.foreign_keys) == 1
    assert orders_table.foreign_keys[0][1].references_table == "customers"

    items_table = schema.tables[2]
    assert len(items_table.foreign_keys) == 1
    assert items_table.foreign_keys[0][1].references_table == "orders"


def test_multiple_foreign_keys_in_single_table() -> None:
    """Test that a table can have multiple FK columns."""
    payload = {
        "name": "multi_fk",
        "tables": [
            {
                "name": "users",
                "target_rows": 100,
                "columns": [
                    {"name": "user_id", "data_type": "INT", "is_unique": True},
                ],
            },
            {
                "name": "products",
                "target_rows": 50,
                "columns": [
                    {"name": "product_id", "data_type": "INT", "is_unique": True},
                ],
            },
            {
                "name": "reviews",
                "target_rows": 500,
                "columns": [
                    {"name": "review_id", "data_type": "INT", "is_unique": True},
                    {
                        "name": "user_id",
                        "data_type": "INT",
                        "foreign_key": {
                            "references_table": "users",
                            "references_column": "user_id",
                        },
                    },
                    {
                        "name": "product_id",
                        "data_type": "INT",
                        "foreign_key": {
                            "references_table": "products",
                            "references_column": "product_id",
                        },
                    },
                    {"name": "rating", "data_type": "INT"},
                ],
            },
        ],
    }

    schema = parse_experiment_schema(payload)
    reviews_table = schema.tables[2]
    assert len(reviews_table.foreign_keys) == 2

    fk_refs = {fk[1].references_table for fk in reviews_table.foreign_keys}
    assert fk_refs == {"users", "products"}
