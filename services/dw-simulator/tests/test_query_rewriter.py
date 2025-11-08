import pytest

from dw_simulator.query_rewriter import QueryRewriteError, rewrite_query_for_experiment


def test_rewrite_simple_select_replaces_table_name() -> None:
    sql = "SELECT * FROM SAS_HOUSEHOLD_VW"
    rewritten = rewrite_query_for_experiment(
        sql,
        experiment_name="rl_dw",
        table_mapping={"SAS_HOUSEHOLD_VW": "rl_dw__sas_household_vw"},
    )

    assert "rl_dw__sas_household_vw" in rewritten
    assert "SAS_HOUSEHOLD_VW" not in rewritten


def test_rewrite_handles_joins_and_aliases() -> None:
    sql = """
        SELECT c.name, t.amount
        FROM SAS_HOUSEHOLD_VW AS c
        JOIN SAS_TRANSACTION_VW t ON c.household_id = t.household_id
    """
    rewritten = rewrite_query_for_experiment(
        sql,
        experiment_name="rl_dw",
        table_mapping={
            "SAS_HOUSEHOLD_VW": "rl_dw__sas_household_vw",
            "SAS_TRANSACTION_VW": "rl_dw__sas_transaction_vw",
        },
    )

    assert "rl_dw__sas_household_vw AS c" in rewritten
    assert "rl_dw__sas_transaction_vw AS t" in rewritten or "rl_dw__sas_transaction_vw t" in rewritten


def test_rewrite_preserves_schema_qualifiers() -> None:
    sql = "SELECT COUNT(*) FROM analytics.SAS_TRANSACTION_VW"
    rewritten = rewrite_query_for_experiment(
        sql,
        experiment_name="rl_dw",
        table_mapping={"sas_transaction_vw": "rl_dw__sas_transaction_vw"},
    )

    assert "analytics.rl_dw__sas_transaction_vw" in rewritten


def test_rewrite_is_case_insensitive_and_handles_quotes() -> None:
    sql = 'SELECT * FROM "Sas_Household_Vw"'
    rewritten = rewrite_query_for_experiment(
        sql,
        experiment_name="rl_dw",
        table_mapping={"SAS_HOUSEHOLD_VW": "rl_dw__sas_household_vw"},
    )

    assert 'FROM rl_dw__sas_household_vw' in rewritten


def test_rewrite_ignores_unknown_tables() -> None:
    sql = "SELECT * FROM UNKNOWN_TABLE"
    rewritten = rewrite_query_for_experiment(
        sql,
        experiment_name="rl_dw",
        table_mapping={"SAS_HOUSEHOLD_VW": "rl_dw__sas_household_vw"},
    )

    assert rewritten == "SELECT * FROM UNKNOWN_TABLE"


def test_rewrite_raises_on_parse_error() -> None:
    with pytest.raises(QueryRewriteError):
        rewrite_query_for_experiment(
            "SELECT * FROM table WHERE",
            experiment_name="rl_dw",
            table_mapping={"table": "rl_dw__table"},
        )


def test_rewrite_handles_ctes_and_nested_references() -> None:
    sql = """
        WITH recent_customers AS (
            SELECT id FROM CUSTOMERS
            WHERE signup_date > '2024-01-01'
        )
        SELECT rc.id, o.amount
        FROM recent_customers rc
        JOIN ORDERS o ON rc.id = o.customer_id
    """
    rewritten = rewrite_query_for_experiment(
        sql,
        experiment_name="rl_dw",
        table_mapping={
            "CUSTOMERS": "rl_dw__customers",
            "ORDERS": "rl_dw__orders",
        },
    )

    assert "rl_dw__customers" in rewritten
    assert "rl_dw__orders" in rewritten
    assert "recent_customers" in rewritten


def test_rewrite_applies_to_multiple_statements() -> None:
    sql = "SELECT * FROM CUSTOMERS; SELECT COUNT(*) FROM ORDERS;"
    rewritten = rewrite_query_for_experiment(
        sql,
        experiment_name="rl_dw",
        table_mapping={
            "CUSTOMERS": "rl_dw__customers",
            "ORDERS": "rl_dw__orders",
        },
    )

    assert rewritten.count("rl_dw__customers") == 1
    assert "rl_dw__orders" in rewritten
