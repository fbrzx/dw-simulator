"""
Schema validation utilities for experiment definitions.

The schema is expressed as JSON (or dicts) and mirrors the requirements in
docs/product-spec.md: experiments contain tables, each table contains columns,
and every column defines metadata such as data type, faker rules, and
constraints (unique, ranges, etc.).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Sequence

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator


SQL_RESERVED_KEYWORDS = {
    "select",
    "from",
    "where",
    "group",
    "order",
    "table",
    "create",
    "drop",
    "insert",
    "update",
    "delete",
    "into",
}


def _validate_identifier(identifier: str) -> str:
    """Ensure identifiers are SQL-safe and not reserved keywords."""
    candidate = identifier.strip()
    if not candidate:
        raise ValueError("Identifier must not be empty.")
    if not candidate.replace("_", "").isalnum():
        raise ValueError(f"Identifier '{identifier}' must be alphanumeric/underscore.")
    if candidate.lower() in SQL_RESERVED_KEYWORDS:
        raise ValueError(f"Identifier '{identifier}' conflicts with SQL reserved keywords.")
    return candidate


class DataType(str):
    """Enum-lite data types supported by the schema definition."""

    INT = "INT"
    VARCHAR = "VARCHAR"
    DATE = "DATE"
    BOOLEAN = "BOOLEAN"
    FLOAT = "FLOAT"


class ColumnSchema(BaseModel):
    """Column definition with optional constraints for generation."""

    name: str = Field(..., description="Column identifier.")
    data_type: str = Field(..., description="Supported DW data type.")
    faker_rule: str | None = Field(
        default=None, description="Optional Faker provider path (e.g., 'profile.email')."
    )
    is_unique: bool = Field(default=False, description="Enforce uniqueness when generating data.")
    required: bool = Field(default=True, description="Whether NULLs are allowed.")
    min_value: float | None = Field(default=None, description="Numeric minimum (inclusive).")
    max_value: float | None = Field(default=None, description="Numeric maximum (inclusive).")
    varchar_length: int | None = Field(
        default=None, description="Max length applied to VARCHAR columns."
    )
    date_start: date | None = Field(default=None, description="Inclusive start for DATE columns.")
    date_end: date | None = Field(default=None, description="Inclusive end for DATE columns.")

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _validate_identifier(value)

    @field_validator("data_type")
    @classmethod
    def validate_data_type(cls, value: str) -> str:
        normalized = value.upper()
        if normalized not in {DataType.INT, DataType.VARCHAR, DataType.DATE, DataType.BOOLEAN, DataType.FLOAT}:
            raise ValueError(f"Unsupported data type '{value}'.")
        return normalized

    @model_validator(mode="after")
    def validate_value_constraints(self) -> "ColumnSchema":
        if self.data_type in {DataType.INT, DataType.FLOAT}:
            if self.min_value is not None and self.max_value is not None:
                if self.min_value > self.max_value:
                    raise ValueError(
                        f"Column '{self.name}' min_value ({self.min_value}) cannot exceed max_value ({self.max_value})."
                    )
        else:
            if self.min_value is not None or self.max_value is not None:
                raise ValueError(
                    f"Column '{self.name}' only supports numeric ranges when data_type is INT or FLOAT."
                )

        if self.data_type == DataType.VARCHAR:
            if self.varchar_length is not None and self.varchar_length <= 0:
                raise ValueError(f"Column '{self.name}' must define varchar_length > 0.")
        elif self.varchar_length is not None:
            raise ValueError(
                f"Column '{self.name}' can only set varchar_length when data_type is VARCHAR."
            )

        if self.data_type == DataType.DATE:
            if self.date_start and self.date_end and self.date_start > self.date_end:
                raise ValueError(
                    f"Column '{self.name}' date_start ({self.date_start}) cannot exceed date_end ({self.date_end})."
                )
        else:
            if self.date_start is not None or self.date_end is not None:
                raise ValueError(
                    f"Column '{self.name}' can only define date ranges when data_type is DATE."
                )
        return self


class TableSchema(BaseModel):
    """Table definition covering name, row counts, and column declarations."""

    name: str = Field(..., description="Table name.")
    target_rows: int = Field(..., gt=0, description="Desired rows for the generator to create.")
    columns: Sequence[ColumnSchema] = Field(..., min_length=1)

    @field_validator("name")
    @classmethod
    def validate_table_name(cls, value: str) -> str:
        return _validate_identifier(value)

    @model_validator(mode="after")
    def validate_columns(self) -> "TableSchema":
        seen: set[str] = set()
        for column in self.columns:
            lowered = column.name.lower()
            if lowered in seen:
                raise ValueError(f"Duplicate column name '{column.name}' detected for table '{self.name}'.")
            seen.add(lowered)
        return self


class ExperimentSchema(BaseModel):
    """Top-level experiment definition used to build local warehouses."""

    name: str = Field(..., description="Experiment identifier.")
    description: str | None = Field(default=None)
    tables: Sequence[TableSchema] = Field(..., min_length=1)

    @field_validator("name")
    @classmethod
    def validate_experiment_name(cls, value: str) -> str:
        return _validate_identifier(value)

    def total_rows(self) -> int:
        return sum(table.target_rows for table in self.tables)


def parse_experiment_schema(payload: Mapping[str, Any] | str) -> ExperimentSchema:
    """
    Convert JSON/dict payloads into validated ExperimentSchema instances.

    Args:
        payload: JSON string or dict describing the experiment.
    """

    if isinstance(payload, str):
        parsed = json.loads(payload)
    elif isinstance(payload, Mapping):
        parsed = payload
    else:
        raise TypeError("Schema payload must be a JSON string or mapping.")

    return ExperimentSchema.model_validate(parsed)


@dataclass(frozen=True)
class SchemaValidationResult:
    """Structured response for validation pipelines and user feedback."""

    is_valid: bool
    errors: list[str]


def validate_experiment_payload(payload: Mapping[str, Any] | str) -> SchemaValidationResult:
    """
    Validate payloads and return structured errors instead of raising.

    Useful for CLI/REST contexts where we need to collect multiple validation
    failures before responding to the caller.
    """

    try:
        parse_experiment_schema(payload)
        return SchemaValidationResult(is_valid=True, errors=[])
    except (ValidationError, ValueError, TypeError) as exc:
        if isinstance(exc, ValidationError):
            errors = [err["msg"] for err in exc.errors()]
        else:
            errors = [str(exc)]
        return SchemaValidationResult(is_valid=False, errors=errors)


__all__ = [
    "ColumnSchema",
    "TableSchema",
    "ExperimentSchema",
    "SchemaValidationResult",
    "parse_experiment_schema",
    "validate_experiment_payload",
    "DataType",
]
