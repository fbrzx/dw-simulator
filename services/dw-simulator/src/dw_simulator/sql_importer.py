"""Utilities for importing SQL DDL into ExperimentSchema definitions."""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from .schema import ColumnSchema, DataType, ExperimentSchema, TableSchema

SUPPORTED_DIALECTS = {"redshift", "snowflake"}
DEFAULT_TARGET_ROWS = 1000
DEFAULT_VARCHAR_LENGTH = 255


class SqlImportError(RuntimeError):
    """Raised when SQL cannot be converted into the internal schema."""


@dataclass(frozen=True)
class SqlImportOptions:
    experiment_name: str
    dialect: str = "redshift"
    default_target_rows: int = DEFAULT_TARGET_ROWS


def import_sql(sql: str, options: SqlImportOptions) -> ExperimentSchema:
    """Parse SQL CREATE TABLE statements into an ExperimentSchema."""

    dialect = options.dialect.lower()
    if dialect not in SUPPORTED_DIALECTS:
        raise SqlImportError(f"Unsupported dialect '{options.dialect}'. Supported: {', '.join(SUPPORTED_DIALECTS)}")

    try:
        statements = sqlglot.parse(sql, read=dialect)
    except sqlglot.errors.ParseError as exc:
        raise SqlImportError(f"Failed to parse SQL ({options.dialect}): {exc}") from exc

    tables: list[TableSchema] = []
    for statement in statements:
        if not isinstance(statement, exp.Create):
            continue
        schema_node = statement.this
        if not isinstance(schema_node, exp.Schema):
            continue
        table_expr = schema_node.this
        if not isinstance(table_expr, exp.Table):
            continue
        table_name = _normalize_identifier(table_expr)
        cols, constraints = _extract_columns_and_constraints(schema_node)
        primary_key_columns = _dedupe_preserve_order(constraints.get("primary_key", []))
        is_composite_pk = len(primary_key_columns) > 1

        column_schemas: list[ColumnSchema] = []

        if is_composite_pk:
            column_schemas.append(
                ColumnSchema(name="_row_id", data_type=DataType.INT, is_unique=True)
            )

        pk_lookup = {col.lower() for col in primary_key_columns}
        for column in cols:
            col_name = column["name"]
            column_schemas.append(
                ColumnSchema(
                    name=col_name,
                    data_type=column["type"],
                    varchar_length=column.get("varchar_length"),
                    is_unique=(not is_composite_pk and col_name.lower() in pk_lookup),
                )
            )

        warnings: list[str] = []
        composite_keys: list[list[str]] | None = None
        if is_composite_pk:
            composite_keys = [primary_key_columns]
            cols_formatted = ", ".join(primary_key_columns)
            warnings.append(
                f"Table '{table_name}' has composite primary key ({cols_formatted}). "
                "A surrogate '_row_id' column was added for uniqueness."
            )

        tables.append(
            TableSchema(
                name=table_name,
                target_rows=options.default_target_rows,
                columns=column_schemas,
                composite_keys=composite_keys,
                warnings=warnings,
            )
        )

    if not tables:
        raise SqlImportError("No CREATE TABLE statements were found in the supplied SQL.")

    return ExperimentSchema(name=options.experiment_name, tables=tables)


def _extract_columns_and_constraints(schema_node: exp.Schema) -> tuple[list[dict], dict[str, list[str]]]:
    columns: list[dict] = []
    constraints: dict[str, list[str]] = {"primary_key": []}

    for expression in schema_node.expressions or []:
        if isinstance(expression, exp.ColumnDef):
            columns.append(_parse_column_definition(expression))
            inline_pk = False
            for constraint in expression.args.get("constraints", []):
                if isinstance(constraint, exp.ColumnConstraint):
                    kind = constraint.this or constraint.args.get("kind")
                    if isinstance(kind, (exp.PrimaryKey, exp.PrimaryKeyColumnConstraint)):
                        inline_pk = True
                        break
            if inline_pk:
                constraints["primary_key"].append(columns[-1]["name"])
        elif isinstance(expression, exp.Constraint):
            if isinstance(expression.this, exp.PrimaryKey):
                pk_columns = [_normalize_identifier(col) for col in expression.expressions or []]
                constraints["primary_key"].extend(pk_columns)
        elif isinstance(expression, exp.PrimaryKey):
            pk_columns = [
                _normalize_identifier(expr.this if hasattr(expr, "this") else expr)
                for expr in expression.expressions or []
            ]
            constraints["primary_key"].extend(pk_columns)

    return columns, constraints


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        lowered = value.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        ordered.append(value)
    return ordered


def _parse_column_definition(definition: exp.ColumnDef) -> dict:
    column_name = _normalize_identifier(definition.this)
    data_type = definition.args.get("kind")
    if not isinstance(data_type, exp.DataType):
        raise SqlImportError(f"Column '{column_name}' is missing a data type.")
    mapped_type, extra = _map_data_type(data_type)
    return {"name": column_name, "type": mapped_type, **extra}


def _map_data_type(data_type: exp.DataType) -> tuple[str, dict]:
    type_this = data_type.this
    if hasattr(type_this, "value"):
        type_name = str(type_this.value).lower()
    elif isinstance(type_this, exp.Identifier):
        type_name = type_this.name.lower()
    else:
        type_name = str(type_this).lower()
    length = None
    if data_type.expressions:
        first = data_type.expressions[0]
        literal = first.this if isinstance(first, exp.DataTypeParam) else first
        if isinstance(literal, exp.Literal):
            try:
                length = int(literal.this)
            except ValueError:
                length = None

    varchar_types = {"varchar", "character varying", "char", "character", "nvarchar", "nchar", "text"}
    int_types = {"int", "integer", "bigint", "smallint"}
    float_types = {"float", "double", "double precision", "real"}
    numeric_types = {"numeric", "decimal", "number"}
    date_types = {"date"}
    timestamp_types = {"timestamp", "timestamp_ntz", "timestamp_ltz", "timestamp_tz", "timestamptz"}
    bool_types = {"boolean", "bool"}

    if type_name in varchar_types:
        return DataType.VARCHAR, {"varchar_length": length or DEFAULT_VARCHAR_LENGTH}
    if type_name in int_types:
        return DataType.INT, {}
    if type_name in float_types or type_name in numeric_types:
        return DataType.FLOAT, {}
    if type_name in date_types or type_name in timestamp_types:
        return DataType.DATE, {}
    if type_name in bool_types:
        return DataType.BOOLEAN, {}

    raise SqlImportError(f"Unsupported data type '{data_type.sql()}'")


def _normalize_identifier(identifier: exp.Expression) -> str:
    if isinstance(identifier, exp.Identifier):
        return identifier.name
    if isinstance(identifier, exp.Table):
        return identifier.name
    if isinstance(identifier, exp.Column):
        return identifier.this.name
    return identifier.sql().strip('"')


__all__ = ["import_sql", "SqlImportOptions", "SqlImportError", "SUPPORTED_DIALECTS", "DEFAULT_TARGET_ROWS"]
