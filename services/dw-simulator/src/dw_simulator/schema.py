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


class WarehouseType(str):
    """Supported warehouse types for experiment targeting."""

    SQLITE = "sqlite"
    REDSHIFT = "redshift"
    SNOWFLAKE = "snowflake"


class DistributionType(str):
    """Supported statistical distribution identifiers."""

    NORMAL = "normal"
    EXPONENTIAL = "exponential"
    BETA = "beta"


class DistributionConfig(BaseModel):
    """Configuration describing a statistical distribution and its parameters."""

    type: str = Field(..., description="Distribution identifier (normal/exponential/beta).")
    parameters: dict[str, float] = Field(
        default_factory=dict,
        description="Distribution-specific parameters (e.g., mean/stddev).",
    )

    @field_validator("type")
    @classmethod
    def normalize_type(cls, value: str) -> str:
        normalized = value.lower()
        if normalized not in {
            DistributionType.NORMAL,
            DistributionType.EXPONENTIAL,
            DistributionType.BETA,
        }:
            raise ValueError(f"Unsupported distribution type '{value}'.")
        return normalized

    @model_validator(mode="after")
    def validate_parameters(self) -> "DistributionConfig":
        params = self.parameters
        if self.type == DistributionType.NORMAL:
            self._require_params({"mean", "stddev"}, params)
            if params["stddev"] <= 0:
                raise ValueError("Normal distribution requires stddev > 0.")
        elif self.type == DistributionType.EXPONENTIAL:
            self._require_params({"lambda"}, params)
            if params["lambda"] <= 0:
                raise ValueError("Exponential distribution requires lambda > 0.")
        elif self.type == DistributionType.BETA:
            self._require_params({"alpha", "beta"}, params)
            if params["alpha"] <= 0 or params["beta"] <= 0:
                raise ValueError("Beta distribution requires alpha > 0 and beta > 0.")
        return self

    @staticmethod
    def _require_params(expected: set[str], params: Mapping[str, float]) -> None:
        missing = expected.difference(params.keys())
        if missing:
            raise ValueError(
                "Distribution configuration missing required parameter(s): "
                + ", ".join(sorted(missing))
            )


class ForeignKeyConfig(BaseModel):
    """Configuration for foreign key relationships between tables."""

    references_table: str = Field(
        ..., description="Name of the parent table containing the referenced column."
    )
    references_column: str = Field(
        ..., description="Name of the column in the parent table (typically a primary key)."
    )
    nullable: bool | None = Field(
        default=None,
        description=(
            "Whether NULL values are allowed for this FK. "
            "If not specified, inherits from the column's 'required' field."
        ),
    )

    @field_validator("references_table", "references_column")
    @classmethod
    def validate_identifier(cls, value: str) -> str:
        return _validate_identifier(value)


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
    distribution: DistributionConfig | None = Field(
        default=None,
        description=(
            "Optional statistical distribution configuration applied during synthetic data generation."
        ),
    )
    foreign_key: ForeignKeyConfig | None = Field(
        default=None,
        description=(
            "Optional foreign key configuration referencing another table's column. "
            "During generation, values will be sampled from the referenced column's value pool."
        ),
    )

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

        if self.distribution is not None and self.data_type not in {DataType.INT, DataType.FLOAT}:
            raise ValueError(
                "Distribution configuration is only supported for numeric columns (INT or FLOAT)."
            )
        return self


class TableSchema(BaseModel):
    """Table definition covering name, row counts, and column declarations."""

    name: str = Field(..., description="Table name.")
    target_rows: int = Field(..., gt=0, description="Desired rows for the generator to create.")
    columns: Sequence[ColumnSchema] = Field(..., min_length=1)
    composite_keys: list[list[str]] | None = Field(
        default=None,
        description="Optional list of composite primary key column groups. Each inner list represents a set of columns forming a composite key."
    )
    warnings: list[str] = Field(
        default_factory=list,
        description="User-facing guidance messages (e.g., surrogate key explanations)."
    )
    foreign_keys: list[tuple[str, ForeignKeyConfig]] = Field(
        default_factory=list,
        description=(
            "List of (column_name, foreign_key_config) tuples extracted from column definitions. "
            "Populated automatically during validation."
        ),
    )

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

        # Validate composite_keys references
        if self.composite_keys:
            column_names = {col.name.lower() for col in self.columns}
            for key_group in self.composite_keys:
                if not key_group:
                    raise ValueError(f"Table '{self.name}' has an empty composite key group.")
                for col_name in key_group:
                    if col_name.lower() not in column_names:
                        raise ValueError(
                            f"Table '{self.name}' composite key references unknown column '{col_name}'."
                        )

        # Collect foreign key information from columns
        self.foreign_keys = []
        for column in self.columns:
            if column.foreign_key is not None:
                self.foreign_keys.append((column.name, column.foreign_key))

        return self


class ExperimentSchema(BaseModel):
    """Top-level experiment definition used to build local warehouses."""

    name: str = Field(..., description="Experiment identifier.")
    description: str | None = Field(default=None)
    tables: Sequence[TableSchema] = Field(..., min_length=1)
    target_warehouse: str | None = Field(
        default=None,
        description="Target warehouse type (sqlite/redshift/snowflake). If not specified, uses system default."
    )

    @field_validator("name")
    @classmethod
    def validate_experiment_name(cls, value: str) -> str:
        return _validate_identifier(value)

    @field_validator("target_warehouse")
    @classmethod
    def validate_target_warehouse(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.lower()
        if normalized not in {WarehouseType.SQLITE, WarehouseType.REDSHIFT, WarehouseType.SNOWFLAKE}:
            raise ValueError(
                f"Unsupported warehouse type '{value}'. Must be one of: sqlite, redshift, snowflake."
            )
        return normalized

    @model_validator(mode="after")
    def validate_foreign_keys(self) -> "ExperimentSchema":
        """Validate foreign key references across tables."""
        # Build table and column lookup maps
        table_map: dict[str, TableSchema] = {}
        for table in self.tables:
            table_map[table.name.lower()] = table

        # Validate each foreign key reference
        for table in self.tables:
            for col_name, fk_config in table.foreign_keys:
                # Find the column with this FK
                column = next((c for c in table.columns if c.name == col_name), None)
                if column is None:
                    raise ValueError(f"Internal error: FK column '{col_name}' not found in table '{table.name}'.")

                # Check if referenced table exists
                ref_table_name = fk_config.references_table.lower()
                if ref_table_name not in table_map:
                    raise ValueError(
                        f"Table '{table.name}' column '{col_name}' references unknown table '{fk_config.references_table}'."
                    )

                ref_table = table_map[ref_table_name]

                # Check if referenced column exists
                ref_col_name = fk_config.references_column.lower()
                ref_column = next(
                    (c for c in ref_table.columns if c.name.lower() == ref_col_name), None
                )
                if ref_column is None:
                    raise ValueError(
                        f"Table '{table.name}' column '{col_name}' references unknown column "
                        f"'{fk_config.references_column}' in table '{fk_config.references_table}'."
                    )

                # Verify referenced column is unique (typically a primary key)
                if not ref_column.is_unique:
                    raise ValueError(
                        f"Table '{table.name}' column '{col_name}' references column "
                        f"'{ref_column.name}' in table '{ref_table.name}', but that column is not marked as unique. "
                        f"Foreign keys must reference unique columns (typically primary keys)."
                    )

        # Check for circular dependencies
        self._detect_circular_dependencies()

        return self

    def _detect_circular_dependencies(self) -> None:
        """Detect circular FK dependencies that would prevent generation."""
        # Build dependency graph: table -> set of tables it depends on
        dependencies: dict[str, set[str]] = {}
        for table in self.tables:
            table_name = table.name.lower()
            dependencies[table_name] = set()
            for _, fk_config in table.foreign_keys:
                # Only add dependency if FK is required (not nullable)
                # Nullable FKs can be generated in multiple passes
                column = next((c for c in table.columns if c.foreign_key == fk_config), None)
                if column and column.required and (fk_config.nullable is None or not fk_config.nullable):
                    dependencies[table_name].add(fk_config.references_table.lower())

        # Topological sort to detect cycles
        visited: set[str] = set()
        rec_stack: set[str] = set()

        def has_cycle(node: str, path: list[str]) -> tuple[bool, list[str]]:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            for neighbor in dependencies.get(node, set()):
                if neighbor not in visited:
                    has_cycle_result, cycle_path = has_cycle(neighbor, path[:])
                    if has_cycle_result:
                        return True, cycle_path
                elif neighbor in rec_stack:
                    # Found a cycle
                    cycle_start = path.index(neighbor)
                    return True, path[cycle_start:] + [neighbor]

            rec_stack.remove(node)
            return False, []

        for table in dependencies:
            if table not in visited:
                cycle_found, cycle_path = has_cycle(table, [])
                if cycle_found:
                    cycle_str = " -> ".join(cycle_path)
                    raise ValueError(
                        f"Circular foreign key dependency detected: {cycle_str}. "
                        f"To break the cycle, make at least one FK nullable."
                    )

    def total_rows(self) -> int:
        return sum(table.target_rows for table in self.tables)

    def validate_generation_constraints(self) -> list[str]:
        """
        Validate that the schema can realistically generate the requested data.

        Returns a list of warning messages about potentially problematic constraints.
        """
        warnings: list[str] = []

        # Estimate max unique values for different data types without faker rules
        DEFAULT_UNIQUE_LIMITS = {
            DataType.VARCHAR: 500,  # faker.word() has ~500 unique common words
            DataType.INT: 1_000_000,  # Large range, unlikely to be a problem
            DataType.FLOAT: 10_000_000,  # Very large range
            DataType.DATE: 2000,  # Default range is ~6 years = ~2000 days
            DataType.BOOLEAN: 2,  # Only 2 possible values
        }

        for table in self.tables:
            # Check for unique columns that might not generate enough unique values
            for column in table.columns:
                if not column.is_unique:
                    continue

                # Skip columns with faker rules (they likely have better variety)
                if column.faker_rule:
                    continue

                # Skip INT columns with explicit min/max (user-controlled range)
                if column.data_type == DataType.INT and (column.min_value is not None or column.max_value is not None):
                    continue

                # Get the estimated limit for this data type
                estimated_limit = DEFAULT_UNIQUE_LIMITS.get(column.data_type, 1_000_000)

                # For VARCHAR, consider the length (shorter = fewer combos)
                if column.data_type == DataType.VARCHAR:
                    varchar_len = column.varchar_length or 255
                    # faker.word() typically returns English words truncated to varchar_length
                    # Very short lengths severely limit variety
                    if varchar_len <= 4:
                        estimated_limit = 100  # 2-4 chars: very limited
                    elif varchar_len <= 10:
                        estimated_limit = 300  # 5-10 chars: limited variety

                # Check if target_rows exceeds the estimated limit
                if table.target_rows > estimated_limit:
                    warnings.append(
                        f"Table '{table.name}' column '{column.name}': Requesting {table.target_rows} unique {column.data_type} values "
                        f"may fail. Recommended: reduce target_rows to ≤{estimated_limit} or add a faker_rule for better variety."
                    )

        return warnings


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
    "WarehouseType",
    "DistributionConfig",
    "DistributionType",
    "ForeignKeyConfig",
]
