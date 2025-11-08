"""Utilities for rewriting logical SQL table names to experiment-prefixed names."""

from __future__ import annotations

import sqlglot
from sqlglot import exp


class QueryRewriteError(RuntimeError):
    """Raised when SQL query rewriting fails."""


def rewrite_query_for_experiment(
    sql: str,
    experiment_name: str,
    table_mapping: dict[str, str],
    dialect: str | None = None,
) -> str:
    """
    Rewrite SQL queries by replacing logical table names with physical experiment tables.

    Args:
        sql: The original SQL query submitted by the user.
        experiment_name: The experiment whose physical tables should be used.
        table_mapping: Mapping of logical table names -> physical table names.
        dialect: Optional SQL dialect hint passed to sqlglot.

    Returns:
        str: SQL query with table names rewritten to their physical equivalents.
    """

    if not table_mapping:
        return sql

    normalized_mapping = {
        _normalize_identifier(logical_name): physical_name
        for logical_name, physical_name in table_mapping.items()
    }

    try:
        statements = sqlglot.parse(sql, read=dialect) if dialect else sqlglot.parse(sql)
    except sqlglot.errors.ParseError as exc:
        raise QueryRewriteError(f"Failed to parse SQL for experiment '{experiment_name}': {exc}") from exc

    rewritten_statements: list[str] = []
    for statement in statements:
        for table in statement.find_all(exp.Table):
            logical_name = _normalize_identifier(table)
            physical_name = normalized_mapping.get(logical_name)
            if not physical_name:
                continue

            table.set("this", exp.to_identifier(physical_name))

        rewritten_statements.append(statement.sql(dialect=dialect))

    return ";\n".join(rewritten_statements)


def _normalize_identifier(identifier: str | exp.Table) -> str:
    """
    Normalize a table identifier for case-insensitive lookups.

    Returns:
        str: Lowercase table name without schema qualifiers.
    """
    if isinstance(identifier, exp.Table):
        name = identifier.name
    else:
        name = identifier

    return name.strip().strip('"').split(".")[-1].lower()


__all__ = ["rewrite_query_for_experiment", "QueryRewriteError"]
