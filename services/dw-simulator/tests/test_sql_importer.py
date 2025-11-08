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


# US 5.2 Phase 3: Warehouse selection tests


def test_import_sql_with_target_warehouse_sqlite() -> None:
    """Test that SqlImportOptions accepts and passes through target_warehouse for SQLite."""
    sql = """
    CREATE TABLE products (
        product_id BIGINT PRIMARY KEY,
        name VARCHAR(200)
    );
    """
    schema = import_sql(sql, SqlImportOptions(
        experiment_name="products",
        dialect="redshift",
        target_warehouse="sqlite"
    ))
    assert schema.target_warehouse == "sqlite"


def test_import_sql_with_target_warehouse_redshift() -> None:
    """Test that SqlImportOptions accepts and passes through target_warehouse for Redshift."""
    sql = """
    CREATE TABLE products (
        product_id BIGINT PRIMARY KEY,
        name VARCHAR(200)
    );
    """
    schema = import_sql(sql, SqlImportOptions(
        experiment_name="products",
        dialect="redshift",
        target_warehouse="redshift"
    ))
    assert schema.target_warehouse == "redshift"


def test_import_sql_with_target_warehouse_snowflake() -> None:
    """Test that SqlImportOptions accepts and passes through target_warehouse for Snowflake."""
    sql = """
    CREATE TABLE products (
        product_id BIGINT PRIMARY KEY,
        name VARCHAR(200)
    );
    """
    schema = import_sql(sql, SqlImportOptions(
        experiment_name="products",
        dialect="snowflake",
        target_warehouse="snowflake"
    ))
    assert schema.target_warehouse == "snowflake"


def test_import_sql_without_target_warehouse() -> None:
    """Test that target_warehouse defaults to None when not specified."""
    sql = """
    CREATE TABLE products (
        product_id BIGINT PRIMARY KEY,
        name VARCHAR(200)
    );
    """
    schema = import_sql(sql, SqlImportOptions(
        experiment_name="products",
        dialect="redshift"
    ))
    assert schema.target_warehouse is None


# ============================================================================
# Foreign Key Tests
# ============================================================================


def test_import_sql_with_inline_foreign_key_redshift() -> None:
    """Test that inline REFERENCES syntax is detected in Redshift."""
    sql = """
    CREATE TABLE customers (
        customer_id INT PRIMARY KEY,
        email VARCHAR(255)
    );

    CREATE TABLE orders (
        order_id INT PRIMARY KEY,
        customer_id INT REFERENCES customers(customer_id),
        order_total FLOAT
    );
    """
    schema = import_sql(sql, SqlImportOptions(
        experiment_name="ecommerce",
        dialect="redshift"
    ))

    assert len(schema.tables) == 2
    orders_table = schema.tables[1]
    assert orders_table.name == "orders"

    # Find customer_id column
    customer_id_col = next((c for c in orders_table.columns if c.name == "customer_id"), None)
    assert customer_id_col is not None
    assert customer_id_col.foreign_key is not None
    assert customer_id_col.foreign_key.references_table == "customers"
    assert customer_id_col.foreign_key.references_column == "customer_id"


def test_import_sql_with_inline_foreign_key_snowflake() -> None:
    """Test that inline REFERENCES syntax is detected in Snowflake."""
    sql = """
    CREATE TABLE users (
        user_id BIGINT PRIMARY KEY,
        username VARCHAR(100)
    );

    CREATE TABLE posts (
        post_id BIGINT PRIMARY KEY,
        user_id BIGINT REFERENCES users(user_id),
        content TEXT
    );
    """
    schema = import_sql(sql, SqlImportOptions(
        experiment_name="social",
        dialect="snowflake"
    ))

    assert len(schema.tables) == 2
    posts_table = schema.tables[1]

    user_id_col = next((c for c in posts_table.columns if c.name == "user_id"), None)
    assert user_id_col is not None
    assert user_id_col.foreign_key is not None
    assert user_id_col.foreign_key.references_table == "users"
    assert user_id_col.foreign_key.references_column == "user_id"


def test_import_sql_with_table_level_foreign_key() -> None:
    """Test that table-level FOREIGN KEY constraints are detected."""
    sql = """
    CREATE TABLE products (
        product_id INT PRIMARY KEY,
        name VARCHAR(200)
    );

    CREATE TABLE reviews (
        review_id INT PRIMARY KEY,
        product_id INT,
        rating INT,
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    );
    """
    schema = import_sql(sql, SqlImportOptions(
        experiment_name="reviews_system",
        dialect="redshift"
    ))

    assert len(schema.tables) == 2
    reviews_table = schema.tables[1]

    product_id_col = next((c for c in reviews_table.columns if c.name == "product_id"), None)
    assert product_id_col is not None
    assert product_id_col.foreign_key is not None
    assert product_id_col.foreign_key.references_table == "products"
    assert product_id_col.foreign_key.references_column == "product_id"


def test_import_sql_with_multiple_foreign_keys() -> None:
    """Test that multiple FKs in a single table are detected."""
    sql = """
    CREATE TABLE users (
        user_id INT PRIMARY KEY
    );

    CREATE TABLE products (
        product_id INT PRIMARY KEY
    );

    CREATE TABLE orders (
        order_id INT PRIMARY KEY,
        user_id INT REFERENCES users(user_id),
        product_id INT REFERENCES products(product_id)
    );
    """
    schema = import_sql(sql, SqlImportOptions(
        experiment_name="marketplace",
        dialect="redshift"
    ))

    assert len(schema.tables) == 3
    orders_table = schema.tables[2]

    user_id_col = next((c for c in orders_table.columns if c.name == "user_id"), None)
    assert user_id_col is not None
    assert user_id_col.foreign_key is not None
    assert user_id_col.foreign_key.references_table == "users"

    product_id_col = next((c for c in orders_table.columns if c.name == "product_id"), None)
    assert product_id_col is not None
    assert product_id_col.foreign_key is not None
    assert product_id_col.foreign_key.references_table == "products"


def test_import_sql_with_multi_level_foreign_keys() -> None:
    """Test that FK chains (customers -> orders -> order_items) are detected."""
    sql = """
    CREATE TABLE customers (
        customer_id INT PRIMARY KEY,
        name VARCHAR(100)
    );

    CREATE TABLE orders (
        order_id INT PRIMARY KEY,
        customer_id INT REFERENCES customers(customer_id)
    );

    CREATE TABLE order_items (
        item_id INT PRIMARY KEY,
        order_id INT REFERENCES orders(order_id),
        quantity INT
    );
    """
    schema = import_sql(sql, SqlImportOptions(
        experiment_name="retail",
        dialect="redshift"
    ))

    assert len(schema.tables) == 3

    # Verify orders -> customers FK
    orders_table = schema.tables[1]
    orders_fk = next((c for c in orders_table.columns if c.name == "customer_id"), None)
    assert orders_fk is not None
    assert orders_fk.foreign_key.references_table == "customers"

    # Verify order_items -> orders FK
    items_table = schema.tables[2]
    items_fk = next((c for c in items_table.columns if c.name == "order_id"), None)
    assert items_fk is not None
    assert items_fk.foreign_key.references_table == "orders"


def test_import_sql_without_foreign_keys() -> None:
    """Test that tables without FKs are handled correctly."""
    sql = """
    CREATE TABLE standalone (
        id INT PRIMARY KEY,
        data VARCHAR(100)
    );
    """
    schema = import_sql(sql, SqlImportOptions(
        experiment_name="simple",
        dialect="redshift"
    ))

    assert len(schema.tables) == 1
    table = schema.tables[0]

    # Verify no columns have FK constraints
    for col in table.columns:
        assert col.foreign_key is None
