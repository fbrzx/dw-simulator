from pathlib import Path

import pytest
from sqlalchemy import inspect

from dw_simulator.persistence import (
    DataLoadError,
    ExperimentAlreadyExistsError,
    ExperimentNotFoundError,
    ExperimentPersistence,
    GenerationAlreadyRunningError,
    GenerationRunNotFoundError,
    GenerationStatus,
    QueryExecutionError,
    normalize_identifier,
)
from dw_simulator.schema import ColumnSchema, ExperimentSchema, TableSchema


def build_schema(name: str = "ExperimentAlpha") -> ExperimentSchema:
    return ExperimentSchema(
        name=name,
        description="Test experiment",
        tables=[
            TableSchema(
                name="customers",
                target_rows=100,
                columns=[
                    ColumnSchema(name="customer_id", data_type="INT", is_unique=True),
                    ColumnSchema(name="email", data_type="VARCHAR", varchar_length=255),
                    ColumnSchema(
                        name="signup_date",
                        data_type="DATE",
                        date_start="2024-01-01",
                        date_end="2024-12-31",
                    ),
                ],
            )
        ],
    )


def create_persistence(tmp_path: Path) -> ExperimentPersistence:
    db_path = tmp_path / "warehouse.db"
    return ExperimentPersistence(connection_string=f"sqlite:///{db_path}")


def test_create_experiment_materializes_tables_and_metadata(tmp_path: Path) -> None:
    persistence = create_persistence(tmp_path)
    schema = build_schema()

    metadata = persistence.create_experiment(schema)
    assert metadata.name == schema.name
    fetched = persistence.get_experiment_metadata(schema.name)
    assert fetched is not None
    assert fetched.schema_json

    inspector = inspect(persistence.engine)
    physical_table = f"{normalize_identifier(schema.name)}__{normalize_identifier(schema.tables[0].name)}"
    columns = inspector.get_columns(physical_table)
    column_names = {column["name"] for column in columns}
    assert {"customer_id", "email", "signup_date"} <= column_names
    assert persistence.list_tables(schema.name) == [physical_table]


def test_duplicate_experiment_names_raise(tmp_path: Path) -> None:
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    with pytest.raises(ExperimentAlreadyExistsError):
        persistence.create_experiment(schema)


def test_delete_experiment_drops_tables_and_metadata(tmp_path: Path) -> None:
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)
    run_id = persistence.start_generation_run(schema.name, output_path="/tmp/out")
    persistence.complete_generation_run(run_id, '{"customers": 10}')

    dropped = persistence.delete_experiment(schema.name)
    assert dropped == 1
    assert persistence.get_experiment_metadata(schema.name) is None
    assert persistence.list_tables(schema.name) == []
    assert persistence.list_generation_runs(schema.name) == []


def test_delete_experiment_not_found(tmp_path: Path) -> None:
    persistence = create_persistence(tmp_path)
    with pytest.raises(ExperimentNotFoundError):
        persistence.delete_experiment("unknown")


def test_list_experiments_returns_all(tmp_path: Path) -> None:
    persistence = create_persistence(tmp_path)
    schema = build_schema("Alpha")
    schema_b = build_schema("Beta")
    persistence.create_experiment(schema)
    persistence.create_experiment(schema_b)

    experiments = persistence.list_experiments()
    names = [exp.name for exp in experiments]
    assert set(names) == {"Alpha", "Beta"}


def test_start_generation_run_creates_run_record(tmp_path: Path) -> None:
    """Test that starting a generation run creates a record with RUNNING status."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    run_id = persistence.start_generation_run(
        experiment_name=schema.name,
        output_path="/tmp/test",
        seed=42,
    )

    assert run_id is not None
    run = persistence.get_generation_run(run_id)
    assert run is not None
    assert run.experiment_name == schema.name
    assert run.status == GenerationStatus.RUNNING
    assert run.output_path == "/tmp/test"
    assert run.seed == 42
    assert run.completed_at is None


def test_concurrent_generation_guard_prevents_simultaneous_runs(tmp_path: Path) -> None:
    """Test that concurrent job guard prevents multiple simultaneous runs."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    # Start first run
    run_id_1 = persistence.start_generation_run(experiment_name=schema.name)
    assert run_id_1 is not None

    # Attempt second run should fail
    with pytest.raises(GenerationAlreadyRunningError) as exc_info:
        persistence.start_generation_run(experiment_name=schema.name)

    assert schema.name in str(exc_info.value)
    assert str(run_id_1) in str(exc_info.value)


def test_concurrent_guard_allows_run_after_completion(tmp_path: Path) -> None:
    """Test that concurrent guard allows new run after previous completes."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    # Start and complete first run
    run_id_1 = persistence.start_generation_run(experiment_name=schema.name)
    persistence.complete_generation_run(run_id_1, '{"customers": 100}')

    # Second run should succeed
    run_id_2 = persistence.start_generation_run(experiment_name=schema.name)
    assert run_id_2 != run_id_1


def test_concurrent_guard_allows_run_after_failure(tmp_path: Path) -> None:
    """Test that concurrent guard allows new run after previous fails."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    # Start and fail first run
    run_id_1 = persistence.start_generation_run(experiment_name=schema.name)
    persistence.fail_generation_run(run_id_1, "Test error")

    # Second run should succeed
    run_id_2 = persistence.start_generation_run(experiment_name=schema.name)
    assert run_id_2 != run_id_1


def test_complete_generation_run_updates_status_and_metadata(tmp_path: Path) -> None:
    """Test that completing a run updates status and stores row counts."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    run_id = persistence.start_generation_run(experiment_name=schema.name)
    row_counts_json = '{"customers": 1000, "orders": 5000}'
    persistence.complete_generation_run(run_id, row_counts_json)

    run = persistence.get_generation_run(run_id)
    assert run is not None
    assert run.status == GenerationStatus.COMPLETED
    assert run.row_counts == row_counts_json
    assert run.completed_at is not None


def test_fail_generation_run_captures_error_message(tmp_path: Path) -> None:
    """Test that failing a run captures detailed error message."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    run_id = persistence.start_generation_run(experiment_name=schema.name)
    error_msg = "GenerationError: Unable to generate unique values\n\nTraceback:\n  File foo.py, line 42"
    persistence.fail_generation_run(run_id, error_msg)

    run = persistence.get_generation_run(run_id)
    assert run is not None
    assert run.status == GenerationStatus.FAILED
    assert run.error_message == error_msg
    assert run.completed_at is not None


def test_complete_nonexistent_run_raises(tmp_path: Path) -> None:
    """Test that completing a non-existent run raises error."""
    persistence = create_persistence(tmp_path)
    with pytest.raises(GenerationRunNotFoundError):
        persistence.complete_generation_run(99999, '{"test": 100}')


def test_fail_nonexistent_run_raises(tmp_path: Path) -> None:
    """Test that failing a non-existent run raises error."""
    persistence = create_persistence(tmp_path)
    with pytest.raises(GenerationRunNotFoundError):
        persistence.fail_generation_run(99999, "error")


def test_start_generation_for_nonexistent_experiment_raises(tmp_path: Path) -> None:
    """Test that starting generation for non-existent experiment raises error."""
    persistence = create_persistence(tmp_path)
    with pytest.raises(ExperimentNotFoundError):
        persistence.start_generation_run(experiment_name="NonExistent")


def test_list_generation_runs_returns_all_for_experiment(tmp_path: Path) -> None:
    """Test that listing runs returns all runs for an experiment, most recent first."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    # Create multiple runs
    run_id_1 = persistence.start_generation_run(experiment_name=schema.name)
    persistence.complete_generation_run(run_id_1, '{"customers": 100}')

    run_id_2 = persistence.start_generation_run(experiment_name=schema.name)
    persistence.fail_generation_run(run_id_2, "Test error")

    run_id_3 = persistence.start_generation_run(experiment_name=schema.name)

    runs = persistence.list_generation_runs(schema.name)
    assert len(runs) == 3
    # Most recent first
    assert runs[0].id == run_id_3
    assert runs[1].id == run_id_2
    assert runs[2].id == run_id_1


def test_list_generation_runs_filters_by_experiment(tmp_path: Path) -> None:
    """Test that listing runs only returns runs for the specified experiment."""
    persistence = create_persistence(tmp_path)
    schema_a = build_schema("ExperimentA")
    schema_b = build_schema("ExperimentB")
    persistence.create_experiment(schema_a)
    persistence.create_experiment(schema_b)

    run_id_a = persistence.start_generation_run(experiment_name=schema_a.name)
    run_id_b = persistence.start_generation_run(experiment_name=schema_b.name)

    runs_a = persistence.list_generation_runs(schema_a.name)
    assert len(runs_a) == 1
    assert runs_a[0].id == run_id_a

    runs_b = persistence.list_generation_runs(schema_b.name)
    assert len(runs_b) == 1
    assert runs_b[0].id == run_id_b


def test_reset_experiment_truncates_tables(tmp_path: Path) -> None:
    """Test that reset truncates all tables but keeps schema intact."""
    from sqlalchemy import text

    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    # Insert some test data into the table
    physical_table = f"{normalize_identifier(schema.name)}__{normalize_identifier(schema.tables[0].name)}"
    with persistence.engine.begin() as conn:
        conn.execute(
            text(f'INSERT INTO "{physical_table}" (customer_id, email, signup_date) VALUES (1, \'test@example.com\', \'2024-01-01\')')
        )
        # Verify data exists
        result = conn.execute(text(f'SELECT COUNT(*) FROM "{physical_table}"'))
        count = result.scalar()
        assert count == 1

    # Reset the experiment
    reset_count = persistence.reset_experiment(schema.name)
    assert reset_count == 1

    # Verify table still exists but is empty
    inspector = inspect(persistence.engine)
    assert inspector.has_table(physical_table)
    with persistence.engine.connect() as conn:
        result = conn.execute(text(f'SELECT COUNT(*) FROM "{physical_table}"'))
        count = result.scalar()
        assert count == 0

    # Verify metadata still exists
    metadata = persistence.get_experiment_metadata(schema.name)
    assert metadata is not None
    assert metadata.name == schema.name


def test_reset_experiment_not_found(tmp_path: Path) -> None:
    """Test that resetting a non-existent experiment raises error."""
    persistence = create_persistence(tmp_path)
    with pytest.raises(ExperimentNotFoundError):
        persistence.reset_experiment("unknown")


def test_reset_experiment_blocks_during_active_generation(tmp_path: Path) -> None:
    """Test that reset is blocked when generation is running (AC 2)."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    # Start a generation run (RUNNING status)
    run_id = persistence.start_generation_run(experiment_name=schema.name)

    # Attempt to reset should fail
    with pytest.raises(GenerationAlreadyRunningError) as exc_info:
        persistence.reset_experiment(schema.name)

    assert "Cannot reset experiment" in str(exc_info.value)
    assert "generation is running" in str(exc_info.value)

    # Complete the run
    persistence.complete_generation_run(run_id, '{"customers": 100}')

    # Now reset should succeed
    reset_count = persistence.reset_experiment(schema.name)
    assert reset_count == 1


def test_reset_experiment_with_multiple_tables(tmp_path: Path) -> None:
    """Test that reset truncates all tables in a multi-table experiment."""
    from sqlalchemy import text

    persistence = create_persistence(tmp_path)
    schema = ExperimentSchema(
        name="MultiTableTest",
        description="Test with multiple tables",
        tables=[
            TableSchema(
                name="users",
                target_rows=50,
                columns=[ColumnSchema(name="user_id", data_type="INT", is_unique=True)],
            ),
            TableSchema(
                name="orders",
                target_rows=100,
                columns=[ColumnSchema(name="order_id", data_type="INT", is_unique=True)],
            ),
        ],
    )
    persistence.create_experiment(schema)

    # Insert data into both tables
    users_table = f"{normalize_identifier(schema.name)}__{normalize_identifier('users')}"
    orders_table = f"{normalize_identifier(schema.name)}__{normalize_identifier('orders')}"
    with persistence.engine.begin() as conn:
        conn.execute(text(f'INSERT INTO "{users_table}" (user_id) VALUES (1), (2)'))
        conn.execute(text(f'INSERT INTO "{orders_table}" (order_id) VALUES (10), (20), (30)'))

    # Reset
    reset_count = persistence.reset_experiment(schema.name)
    assert reset_count == 2

    # Verify both tables are empty
    with persistence.engine.connect() as conn:
        users_count = conn.execute(text(f'SELECT COUNT(*) FROM "{users_table}"')).scalar()
        orders_count = conn.execute(text(f'SELECT COUNT(*) FROM "{orders_table}"')).scalar()
        assert users_count == 0
        assert orders_count == 0


def test_reset_experiment_row_count_zero_after_reset(tmp_path: Path) -> None:
    """Test acceptance criteria 3: row count is 0 after reset."""
    from sqlalchemy import text

    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    # Insert test data
    physical_table = f"{normalize_identifier(schema.name)}__{normalize_identifier(schema.tables[0].name)}"
    with persistence.engine.begin() as conn:
        for i in range(10):
            conn.execute(
                text(f'INSERT INTO "{physical_table}" (customer_id, email, signup_date) VALUES ({i}, \'test{i}@example.com\', \'2024-01-01\')')
            )

    # Reset
    persistence.reset_experiment(schema.name)

    # Verify row count is 0 (AC 3)
    with persistence.engine.connect() as conn:
        result = conn.execute(text(f'SELECT COUNT(*) FROM "{physical_table}"'))
        count = result.scalar()
        assert count == 0


def test_execute_query_returns_results(tmp_path: Path) -> None:
    """Test that execute_query successfully returns query results (US 3.1 AC 1)."""
    from sqlalchemy import text

    persistence = create_persistence(tmp_path)
    schema = ExperimentSchema(
        name="QueryTest",
        description="Test query execution",
        tables=[
            TableSchema(
                name="customers",
                target_rows=10,
                columns=[
                    ColumnSchema(name="id", data_type="INT", is_unique=True),
                    ColumnSchema(name="name", data_type="VARCHAR", varchar_length=100),
                ],
            ),
            TableSchema(
                name="orders",
                target_rows=10,
                columns=[
                    ColumnSchema(name="order_id", data_type="INT", is_unique=True),
                    ColumnSchema(name="customer_id", data_type="INT"),
                    ColumnSchema(name="amount", data_type="FLOAT"),
                ],
            ),
        ],
    )
    persistence.create_experiment(schema)

    # Insert test data
    customers_table = f"{normalize_identifier(schema.name)}__{normalize_identifier('customers')}"
    orders_table = f"{normalize_identifier(schema.name)}__{normalize_identifier('orders')}"
    with persistence.engine.begin() as conn:
        conn.execute(text(f'INSERT INTO "{customers_table}" (id, name) VALUES (1, \'Alice\'), (2, \'Bob\')'))
        conn.execute(text(f'INSERT INTO "{orders_table}" (order_id, customer_id, amount) VALUES (100, 1, 50.0), (101, 2, 75.5)'))

    # Test simple SELECT
    result = persistence.execute_query(f'SELECT * FROM "{customers_table}"')
    assert result.row_count == 2
    assert "id" in result.columns
    assert "name" in result.columns
    assert len(result.rows) == 2

    # Test JOIN query (AC 1)
    join_query = f'SELECT c.name, o.amount FROM "{customers_table}" c JOIN "{orders_table}" o ON c.id = o.customer_id'
    result = persistence.execute_query(join_query)
    assert result.row_count == 2
    assert "name" in result.columns
    assert "amount" in result.columns


def test_execute_query_handles_invalid_sql(tmp_path: Path) -> None:
    """Test that execute_query provides clear error messages for invalid SQL (US 3.1 AC 2)."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    # Test syntax error
    with pytest.raises(QueryExecutionError) as exc_info:
        persistence.execute_query("SELECT * FROM nonexistent_table WHERE")

    assert "Query execution failed" in str(exc_info.value)


def test_execute_query_column_headers_match_schema(tmp_path: Path) -> None:
    """Test that query results have column headers matching schema (US 3.1 AC 3)."""
    from sqlalchemy import text

    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    # Insert test data
    physical_table = f"{normalize_identifier(schema.name)}__{normalize_identifier(schema.tables[0].name)}"
    with persistence.engine.begin() as conn:
        conn.execute(
            text(f'INSERT INTO "{physical_table}" (customer_id, email, signup_date) VALUES (1, \'test@example.com\', \'2024-01-01\')')
        )

    # Execute query
    result = persistence.execute_query(f'SELECT * FROM "{physical_table}"')

    # Verify column headers match schema definition
    expected_columns = {"customer_id", "email", "signup_date"}
    assert expected_columns <= set(result.columns)


def test_execute_query_returns_empty_result_for_empty_table(tmp_path: Path) -> None:
    """Test that execute_query handles empty tables correctly."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    physical_table = f"{normalize_identifier(schema.name)}__{normalize_identifier(schema.tables[0].name)}"
    result = persistence.execute_query(f'SELECT * FROM "{physical_table}"')

    assert result.row_count == 0
    assert len(result.rows) == 0
    assert len(result.columns) > 0  # Columns should still be present


def test_load_parquet_files_to_table_replaces_existing_rows(tmp_path: Path) -> None:
    """Loading Parquet data should replace existing table contents and return row count."""
    from datetime import date
    from sqlalchemy import text
    import pyarrow as pa
    import pyarrow.parquet as pq

    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    physical_table = f"{normalize_identifier(schema.name)}__{normalize_identifier(schema.tables[0].name)}"

    # Seed existing rows that should be replaced by the Parquet load
    with persistence.engine.begin() as conn:
        conn.execute(
            text(
                f'INSERT INTO "{physical_table}" (customer_id, email, signup_date) '
                "VALUES (999, 'old@example.com', '2024-01-01')"
            )
        )

    parquet_path = tmp_path / "customers.parquet"
    pq.write_table(
        pa.table(
            {
                "customer_id": pa.array([1, 2], type=pa.int64()),
                "email": pa.array(["alice@example.com", "bob@example.com"]),
                "signup_date": pa.array([date(2024, 1, 10), date(2024, 2, 5)], type=pa.date32()),
            }
        ),
        parquet_path,
        compression="snappy",
    )

    inserted = persistence.load_parquet_files_to_table(
        experiment_name=schema.name,
        table_name=schema.tables[0].name,
        parquet_files=[parquet_path],
    )

    assert inserted == 2

    with persistence.engine.connect() as conn:
        rows = conn.execute(
            text(
                f'SELECT customer_id, email, signup_date FROM "{physical_table}" ORDER BY customer_id'
            )
        ).fetchall()

    assert rows == [
        (1, "alice@example.com", "2024-01-10"),
        (2, "bob@example.com", "2024-02-05"),
    ]


def test_load_parquet_files_to_table_missing_file(tmp_path: Path) -> None:
    """Missing Parquet files should raise DataLoadError with a clear message."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    missing_path = tmp_path / "missing.parquet"

    with pytest.raises(DataLoadError) as exc_info:
        persistence.load_parquet_files_to_table(
            experiment_name=schema.name,
            table_name=schema.tables[0].name,
            parquet_files=[missing_path],
        )

    assert "Missing Parquet files" in str(exc_info.value)


def test_load_generation_run_success(tmp_path: Path) -> None:
    """Successfully loading a generation run should load all tables and return row counts."""
    from datetime import date
    from sqlalchemy import text
    import pyarrow as pa
    import pyarrow.parquet as pq

    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    # Create output directory structure with parquet files
    output_dir = tmp_path / "output"
    table_dir = output_dir / "customers"
    table_dir.mkdir(parents=True, exist_ok=True)

    # Write two parquet batch files
    batch1_path = table_dir / "batch-00000.parquet"
    pq.write_table(
        pa.table(
            {
                "customer_id": pa.array([1, 2], type=pa.int64()),
                "email": pa.array(["alice@example.com", "bob@example.com"]),
                "signup_date": pa.array([date(2024, 1, 10), date(2024, 2, 5)], type=pa.date32()),
            }
        ),
        batch1_path,
        compression="snappy",
    )

    batch2_path = table_dir / "batch-00001.parquet"
    pq.write_table(
        pa.table(
            {
                "customer_id": pa.array([3], type=pa.int64()),
                "email": pa.array(["charlie@example.com"]),
                "signup_date": pa.array([date(2024, 3, 15)], type=pa.date32()),
            }
        ),
        batch2_path,
        compression="snappy",
    )

    # Start and complete a generation run
    run_id = persistence.start_generation_run(
        experiment_name=schema.name,
        output_path=str(output_dir),
        seed=12345,
    )
    persistence.complete_generation_run(run_id, '{"customers": 3}')

    # Load the generation run
    row_counts = persistence.load_generation_run(run_id)

    # Verify row counts returned
    assert row_counts == {"customers": 3}

    # Verify data was loaded into the physical table
    physical_table = f"{normalize_identifier(schema.name)}__{normalize_identifier(schema.tables[0].name)}"
    with persistence.engine.connect() as conn:
        rows = conn.execute(
            text(f'SELECT customer_id, email FROM "{physical_table}" ORDER BY customer_id')
        ).fetchall()

    assert len(rows) == 3
    assert rows == [
        (1, "alice@example.com"),
        (2, "bob@example.com"),
        (3, "charlie@example.com"),
    ]


def test_load_generation_run_not_found(tmp_path: Path) -> None:
    """Loading a non-existent generation run should raise GenerationRunNotFoundError."""
    persistence = create_persistence(tmp_path)

    with pytest.raises(GenerationRunNotFoundError) as exc_info:
        persistence.load_generation_run(999)

    assert "Generation run 999 not found" in str(exc_info.value)


def test_load_generation_run_not_completed(tmp_path: Path) -> None:
    """Loading a generation run that is not COMPLETED should raise DataLoadError."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    # Start a generation run but don't complete it
    run_id = persistence.start_generation_run(schema.name, output_path="/tmp/out")

    with pytest.raises(DataLoadError) as exc_info:
        persistence.load_generation_run(run_id)

    assert "status is RUNNING" in str(exc_info.value)
    assert "expected COMPLETED" in str(exc_info.value)


def test_load_generation_run_no_output_path(tmp_path: Path) -> None:
    """Loading a generation run with no output path should raise DataLoadError."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    # Start and complete a run without an output path
    run_id = persistence.start_generation_run(schema.name)
    persistence.complete_generation_run(run_id, '{"customers": 0}')

    with pytest.raises(DataLoadError) as exc_info:
        persistence.load_generation_run(run_id)

    assert "no output path recorded" in str(exc_info.value)


def test_load_generation_run_missing_output_directory(tmp_path: Path) -> None:
    """Loading a generation run with missing output directory should raise DataLoadError."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    # Start and complete a run with a non-existent output path
    run_id = persistence.start_generation_run(
        schema.name, output_path="/nonexistent/path"
    )
    persistence.complete_generation_run(run_id, '{"customers": 0}')

    with pytest.raises(DataLoadError) as exc_info:
        persistence.load_generation_run(run_id)

    assert "does not exist" in str(exc_info.value)


def test_load_generation_run_missing_parquet_files(tmp_path: Path) -> None:
    """Loading a generation run with missing parquet files should raise DataLoadError."""
    persistence = create_persistence(tmp_path)
    schema = build_schema()
    persistence.create_experiment(schema)

    # Create output directory without parquet files
    output_dir = tmp_path / "output"
    table_dir = output_dir / "customers"
    table_dir.mkdir(parents=True, exist_ok=True)

    # Start and complete a generation run
    run_id = persistence.start_generation_run(
        schema.name, output_path=str(output_dir)
    )
    persistence.complete_generation_run(run_id, '{"customers": 10}')

    with pytest.raises(DataLoadError) as exc_info:
        persistence.load_generation_run(run_id)

    assert "No Parquet files found" in str(exc_info.value)


def test_load_generation_run_multi_table(tmp_path: Path) -> None:
    """Loading a generation run with multiple tables should load all tables."""
    from datetime import date
    from sqlalchemy import text
    import pyarrow as pa
    import pyarrow.parquet as pq

    persistence = create_persistence(tmp_path)

    # Create a schema with two tables
    schema = ExperimentSchema(
        name="MultiTable",
        description="Multi-table test",
        tables=[
            TableSchema(
                name="customers",
                target_rows=2,
                columns=[
                    ColumnSchema(name="id", data_type="INT", is_unique=True),
                    ColumnSchema(name="name", data_type="VARCHAR"),
                ],
            ),
            TableSchema(
                name="orders",
                target_rows=3,
                columns=[
                    ColumnSchema(name="order_id", data_type="INT", is_unique=True),
                    ColumnSchema(name="customer_id", data_type="INT"),
                ],
            ),
        ],
    )
    persistence.create_experiment(schema)

    # Create output directory with parquet files for both tables
    output_dir = tmp_path / "output"
    customers_dir = output_dir / "customers"
    orders_dir = output_dir / "orders"
    customers_dir.mkdir(parents=True, exist_ok=True)
    orders_dir.mkdir(parents=True, exist_ok=True)

    # Write customers parquet file
    pq.write_table(
        pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "name": pa.array(["Alice", "Bob"]),
            }
        ),
        customers_dir / "batch-00000.parquet",
        compression="snappy",
    )

    # Write orders parquet file
    pq.write_table(
        pa.table(
            {
                "order_id": pa.array([100, 101, 102], type=pa.int64()),
                "customer_id": pa.array([1, 1, 2], type=pa.int64()),
            }
        ),
        orders_dir / "batch-00000.parquet",
        compression="snappy",
    )

    # Start and complete a generation run
    run_id = persistence.start_generation_run(schema.name, output_path=str(output_dir))
    persistence.complete_generation_run(run_id, '{"customers": 2, "orders": 3}')

    # Load the generation run
    row_counts = persistence.load_generation_run(run_id)

    # Verify row counts for both tables
    assert row_counts == {"customers": 2, "orders": 3}

    # Verify customers data
    customers_table = f"{normalize_identifier(schema.name)}__{normalize_identifier('customers')}"
    with persistence.engine.connect() as conn:
        customer_rows = conn.execute(
            text(f'SELECT id, name FROM "{customers_table}" ORDER BY id')
        ).fetchall()

    assert len(customer_rows) == 2
    assert customer_rows == [(1, "Alice"), (2, "Bob")]

    # Verify orders data
    orders_table = f"{normalize_identifier(schema.name)}__{normalize_identifier('orders')}"
    with persistence.engine.connect() as conn:
        order_rows = conn.execute(
            text(f'SELECT order_id, customer_id FROM "{orders_table}" ORDER BY order_id')
        ).fetchall()

    assert len(order_rows) == 3
    assert order_rows == [(100, 1), (101, 1), (102, 2)]
