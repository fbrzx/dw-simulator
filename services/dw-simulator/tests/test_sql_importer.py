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


def test_import_sql_generates_surrogate_for_composite_primary_key() -> None:
    sql = """
    CREATE TABLE sample (
        id1 BIGINT,
        id2 BIGINT,
        PRIMARY KEY (id1, id2)
    );
    """

    schema = import_sql(sql, SqlImportOptions(experiment_name="sample"))

    table = schema.tables[0]
    assert table.name == "sample"
    assert [column.name for column in table.columns] == ["_row_id", "id1", "id2"]
    assert table.columns[0].is_unique is True
    assert table.columns[1].is_unique is False
    assert table.columns[2].is_unique is False
    assert table.composite_keys == [["id1", "id2"]]
    assert table.warnings == [
        "Table 'sample' has composite primary key (id1, id2). A surrogate '_row_id' column was added for uniqueness."
    ]


def test_import_sql_handles_three_column_composite_primary_key() -> None:
    sql = """
    CREATE TABLE wide_keys (
        store_id INT,
        sku_id INT,
        date_id INT,
        PRIMARY KEY (store_id, sku_id, date_id)
    );
    """

    schema = import_sql(sql, SqlImportOptions(experiment_name="inventory"))

    table = schema.tables[0]
    assert table.name == "wide_keys"
    assert [column.name for column in table.columns][:4] == ["_row_id", "store_id", "sku_id", "date_id"]
    assert table.columns[0].is_unique is True
    assert table.composite_keys == [["store_id", "sku_id", "date_id"]]
    assert "wide_keys" in table.warnings[0]


def test_import_sql_supports_multiple_tables_with_composite_keys() -> None:
    sql = """
    CREATE TABLE parent (
        id BIGINT PRIMARY KEY,
        name VARCHAR(100)
    );

    CREATE TABLE child (
        parent_id BIGINT,
        child_id BIGINT,
        attribute VARCHAR(50),
        PRIMARY KEY (parent_id, child_id)
    );
    """

    schema = import_sql(sql, SqlImportOptions(experiment_name="family"))

    parent, child = schema.tables

    assert parent.columns[0].name == "id"
    assert parent.columns[0].is_unique is True
    assert parent.composite_keys is None
    assert parent.warnings == []

    assert child.columns[0].name == "_row_id"
    assert child.columns[0].is_unique is True
    assert child.composite_keys == [["parent_id", "child_id"]]
    assert child.warnings


def test_import_sql_rejects_unsupported_type() -> None:
    sql = "CREATE TABLE demo (payload VARIANT);"
    with pytest.raises(SqlImportError):
        import_sql(sql, SqlImportOptions(experiment_name="demo", dialect="snowflake"))


def test_import_sql_rejects_unknown_dialect() -> None:
    sql = "CREATE TABLE demo (id BIGINT);"
    with pytest.raises(SqlImportError):
        import_sql(sql, SqlImportOptions(experiment_name="demo", dialect="oracle"))
