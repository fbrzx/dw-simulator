import pytest

from dw_simulator.sql_importer import import_sql, SqlImportOptions, SqlImportError


def test_import_sql_parses_basic_table() -> None:
    sql = """
    CREATE TABLE products (
        product_id BIGINT PRIMARY KEY,
        name VARCHAR(200),
        price NUMERIC
    );
    """
    schema = import_sql(sql, SqlImportOptions(experiment_name="products", dialect="redshift"))
    assert schema.name == "products"
    assert schema.tables[0].name == "products"
    assert schema.tables[0].columns[0].is_unique is True
    assert schema.tables[0].columns[1].varchar_length == 200


def test_import_sql_rejects_composite_primary_key() -> None:
    sql = """
    CREATE TABLE sample (
        id1 BIGINT,
        id2 BIGINT,
        PRIMARY KEY (id1, id2)
    );
    """
    with pytest.raises(SqlImportError):
        import_sql(sql, SqlImportOptions(experiment_name="sample"))


def test_import_sql_rejects_unsupported_type() -> None:
    sql = "CREATE TABLE demo (payload VARIANT);"
    with pytest.raises(SqlImportError):
        import_sql(sql, SqlImportOptions(experiment_name="demo", dialect="snowflake"))


def test_import_sql_rejects_unknown_dialect() -> None:
    sql = "CREATE TABLE demo (id BIGINT);"
    with pytest.raises(SqlImportError):
        import_sql(sql, SqlImportOptions(experiment_name="demo", dialect="oracle"))
