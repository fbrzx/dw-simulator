# DW Simulator Monorepo

This repository hosts all local-first services that power the synthetic data
warehouse simulator described in `docs/`. Every deployable component lives
under `./services` and is orchestrated via `docker-compose.yml`.

## Service layout

- `services/dw-simulator` – Python synthetic data generator + CLI + FastAPI server (runs as
  the `synthetic-data-generator` Docker Compose service).
- `services/web-ui` – Static web UI for managing experiments, generating data, and executing SQL queries.
- `services/data-loader` – Placeholder for the batch/ELT worker that moves
  staged Parquet data into the local Redshift/Snowflake mocks.

## Developing the Python service

All runtime interactions happen through Docker Compose:

```bash
docker compose build synthetic-data-generator
docker compose up synthetic-data-generator
```

For TDD and fast feedback you can still run unit tests inside the service
directory:

```bash
cd services/dw-simulator
PYTHONPATH=src pytest
```

Refer to the service-specific README files for more details as each component
is implemented.

## Make targets

```bash
make install   # pip install -e services/dw-simulator
make test      # run PYTHONPATH=src pytest
make build     # docker compose build
make up        # docker compose up
make api       # run the FastAPI server locally (dw-sim api …)
make ui        # serve the static web UI via python -m http.server
```

## Managing experiments

Experiment schemas live in JSON files that follow `docs/product-spec.md`. Use
the CLI (inside the Docker container or via `pip install -e`) to materialize
them:

```bash
dw-sim experiment create path/to/schema.json
dw-sim experiment import-sql schema.sql --name my_experiment --dialect redshift
dw-sim experiment generate customers_experiment --rows customers=50000 --seed 42
```

The command validates the schema, persists metadata to SQLite (or the configured
warehouse URL), and creates the physical tables. Errors are surfaced with the
exact validation message(s).

### Composite primary key support

When importing SQL with composite primary keys, the simulator automatically handles
them by adding a surrogate `_row_id` column and preserving the original key columns:

```sql
-- Input: schema.sql
CREATE TABLE sales (
  region_id INT NOT NULL,
  store_id INT NOT NULL,
  sale_date DATE,
  amount DECIMAL(10,2),
  PRIMARY KEY (region_id, store_id)
);
```

```bash
dw-sim experiment import-sql schema.sql --name sales_exp --dialect redshift
# Output: Warning: Table 'sales' has a composite primary key (region_id, store_id).
#         A surrogate key column '_row_id' has been added for generation.
#         Original columns are preserved.

dw-sim experiment generate sales_exp --rows sales=1000
# Generates data with _row_id (1, 2, 3, ...) plus original composite key columns
```

The `_row_id` column ensures efficient unique data generation while maintaining schema
fidelity. Warnings are displayed in the CLI, API responses, and Web UI to keep you
informed of the transformation.

### Generation runs and tracking

Every data generation operation is tracked as a "generation run" with the following metadata:

- **Status**: RUNNING, COMPLETED, FAILED, or ABORTED
- **Timestamps**: Start time and completion time
- **Row counts**: Actual rows generated per table
- **Seed**: Optional RNG seed for reproducible generation
- **Errors**: Full traceback if the run failed

**Tracking via CLI:**
```bash
# Generation runs are automatically tracked when you run:
dw-sim experiment generate my_experiment --rows table1=10000 --seed 42

# The CLI displays the run summary on completion
```

**Tracking via API:**
```bash
# List all generation runs for an experiment
curl http://localhost:8000/api/experiments/my_experiment/runs

# Get details of a specific run
curl http://localhost:8000/api/experiments/my_experiment/runs/1
```

**Tracking via Web UI:**
1. Open http://localhost:4173
2. Click "View Runs" next to any experiment
3. The modal displays all runs with real-time status updates (polls every 3 seconds)
4. Running jobs show in-progress status; completed jobs show duration and row counts

Generation runs provide full observability into data creation workflows, making
it easy to debug failures, reproduce datasets with seeds, and audit generation
history.

### Resetting experiments

You can reset an experiment to truncate all its tables while keeping the schema intact.
This is useful when you want to clear the data and regenerate it without recreating the
experiment from scratch.

**Reset via CLI:**
```bash
dw-sim experiment reset my_experiment
# Output: Experiment 'my_experiment' reset (truncated 3 tables).
```

**Reset via API:**
```bash
curl -X POST http://localhost:8000/api/experiments/my_experiment/reset
# Response: {"name": "my_experiment", "reset_tables": 3}
```

**Reset via Web UI:**
1. Open http://localhost:4173
2. Click "Reset" next to any experiment
3. Confirm the action in the dialog
4. The experiment's tables are truncated, and row counts return to 0

**Important notes:**
- Reset is **blocked** if a generation run is currently active for the experiment
- The experiment schema and metadata are preserved; only table data is removed
- After reset, you can regenerate data normally

## Data Generation Rules

You can customize how synthetic data is generated by specifying generation rules in your schema definition. The simulator supports Faker-based rules for realistic data and numeric/date ranges for constrained values.

### Using Faker rules for VARCHAR columns

Faker rules allow you to generate realistic data like names, emails, addresses, and more. Simply add a `faker_rule` field to any VARCHAR column:

```json
{
  "name": "my_experiment",
  "tables": [
    {
      "name": "customers",
      "target_rows": 1000,
      "columns": [
        {"name": "id", "data_type": "INT", "is_unique": true},
        {"name": "first_name", "data_type": "VARCHAR", "faker_rule": "first_name", "varchar_length": 50},
        {"name": "last_name", "data_type": "VARCHAR", "faker_rule": "last_name", "varchar_length": 50},
        {"name": "email", "data_type": "VARCHAR", "faker_rule": "email", "varchar_length": 100},
        {"name": "company", "data_type": "VARCHAR", "faker_rule": "company", "varchar_length": 100},
        {"name": "phone", "data_type": "VARCHAR", "faker_rule": "phone_number", "varchar_length": 20},
        {"name": "city", "data_type": "VARCHAR", "faker_rule": "city", "varchar_length": 50},
        {"name": "country", "data_type": "VARCHAR", "faker_rule": "country", "varchar_length": 50}
      ]
    }
  ]
}
```

**Available Faker rules** (common examples):
- `first_name`, `last_name`, `name` - Person names
- `email`, `phone_number` - Contact information
- `company`, `job` - Business-related
- `address`, `city`, `country`, `postcode` - Location data
- `date`, `date_time` - Temporal data (note: use DATE columns with date_start/date_end for better control)
- Many more! See [Faker documentation](https://faker.readthedocs.io/) for the full list

### Using numeric ranges for INT and FLOAT columns

Specify `min_value` and `max_value` to constrain numeric data within a specific range:

```json
{
  "name": "retail_experiment",
  "tables": [
    {
      "name": "products",
      "target_rows": 5000,
      "columns": [
        {"name": "id", "data_type": "INT", "is_unique": true},
        {"name": "price", "data_type": "FLOAT", "min_value": 9.99, "max_value": 999.99},
        {"name": "stock_quantity", "data_type": "INT", "min_value": 0, "max_value": 1000},
        {"name": "rating", "data_type": "FLOAT", "min_value": 0.0, "max_value": 5.0},
        {"name": "age_restricted", "data_type": "INT", "min_value": 18, "max_value": 100}
      ]
    }
  ]
}
```

All generated values are guaranteed to fall within the specified range (inclusive).

### Using date ranges for DATE columns

Control the date range for DATE columns using `date_start` and `date_end`:

```json
{
  "name": "events_experiment",
  "tables": [
    {
      "name": "orders",
      "target_rows": 10000,
      "columns": [
        {"name": "id", "data_type": "INT", "is_unique": true},
        {"name": "order_date", "data_type": "DATE", "date_start": "2024-01-01", "date_end": "2024-12-31"},
        {"name": "signup_date", "data_type": "DATE", "date_start": "2020-01-01", "date_end": "2023-12-31"}
      ]
    }
  ]
}
```

Dates are generated uniformly within the specified range (inclusive).

### Complete example combining all features

```json
{
  "name": "ecommerce_experiment",
  "description": "Realistic e-commerce dataset with Faker rules and ranges",
  "tables": [
    {
      "name": "users",
      "target_rows": 10000,
      "columns": [
        {"name": "user_id", "data_type": "INT", "is_unique": true},
        {"name": "first_name", "data_type": "VARCHAR", "faker_rule": "first_name", "varchar_length": 50},
        {"name": "last_name", "data_type": "VARCHAR", "faker_rule": "last_name", "varchar_length": 50},
        {"name": "email", "data_type": "VARCHAR", "faker_rule": "email", "varchar_length": 100},
        {"name": "age", "data_type": "INT", "min_value": 18, "max_value": 80},
        {"name": "signup_date", "data_type": "DATE", "date_start": "2020-01-01", "date_end": "2024-12-31"}
      ]
    },
    {
      "name": "orders",
      "target_rows": 50000,
      "columns": [
        {"name": "order_id", "data_type": "INT", "is_unique": true},
        {"name": "user_id", "data_type": "INT", "min_value": 0, "max_value": 9999},
        {"name": "order_date", "data_type": "DATE", "date_start": "2024-01-01", "date_end": "2024-12-31"},
        {"name": "total_amount", "data_type": "FLOAT", "min_value": 10.00, "max_value": 5000.00},
        {"name": "status", "data_type": "VARCHAR", "varchar_length": 20}
      ]
    }
  ]
}
```

Save this to a file (e.g., `ecommerce_schema.json`) and create the experiment:

```bash
dw-sim experiment create ecommerce_schema.json
dw-sim experiment generate ecommerce_experiment
```

## Querying Data

Once you've generated data for an experiment, you can query it using standard SQL.
The simulator provides multiple interfaces for executing queries and exporting results.

### Data loading behavior

**Automatic loading:**
When you generate data using `dw-sim experiment generate`, the generated Parquet files are **automatically loaded** into the local warehouse tables. This means you can immediately query the data without any additional steps:

```bash
# Generate data (auto-loads into warehouse)
dw-sim experiment generate my_experiment

# Query immediately
dw-sim query execute "SELECT COUNT(*) FROM my_experiment__customers"
```

**Manual loading:**
If you need to reload data (for example, after resetting an experiment), you can use the `load` command:

```bash
# Load data from the most recent generation run
dw-sim experiment load my_experiment

# Load data from a specific generation run
dw-sim experiment load my_experiment --run-id 2
```

You can also load data via the API:

```bash
# Load most recent run
curl -X POST http://localhost:8000/api/experiments/my_experiment/load \
  -H "Content-Type: application/json" \
  -d '{}'

# Load specific run
curl -X POST http://localhost:8000/api/experiments/my_experiment/load \
  -H "Content-Type: application/json" \
  -d '{"run_id": 2}'
```

### Executing SQL queries

**Query via CLI:**
```bash
# Execute a query and view results in the console
dw-sim query execute "SELECT * FROM my_experiment__customers LIMIT 10"

# Execute a query and export to CSV
dw-sim query execute "SELECT * FROM my_experiment__customers" --output results.csv

# Join multiple tables
dw-sim query execute "SELECT c.name, o.amount FROM my_experiment__customers c JOIN my_experiment__orders o ON c.id = o.customer_id"
```

**Query via API:**
```bash
# Execute query and get JSON results
curl -X POST http://localhost:8000/api/query/execute \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM my_experiment__customers LIMIT 10", "format": "json"}'

# Execute query and download CSV
curl -X POST http://localhost:8000/api/query/execute \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM my_experiment__customers", "format": "csv"}' \
  --output results.csv
```

### Saving SQL queries

You can save your SQL queries to `.sql` files for reuse:

**Save via CLI:**
```bash
dw-sim query save "SELECT * FROM my_experiment__customers WHERE age > 18" --output query.sql
```

**Query via Web UI:**
1. Open http://localhost:4173
2. Navigate to the **SQL Query Interface** section
3. Enter your SQL query in the text area
4. Click **Execute Query** to run the query
5. Results are displayed in a formatted table with:
   - Column headers matching your schema
   - Row count displayed at the top
   - NULL values clearly marked
6. Use the action buttons:
   - **Clear**: Reset the query and results
   - **Save SQL**: Download the query as a `.sql` file
   - **Export CSV**: Download the results as a CSV file

**Example queries in the Web UI:**
```sql
-- Basic selection
SELECT * FROM customers_experiment__customers LIMIT 100;

-- With filtering and ordering
SELECT customer_id, email, registration_date
FROM customers_experiment__customers
WHERE registration_date > '2024-01-01'
ORDER BY registration_date DESC;

-- Aggregations
SELECT COUNT(*) as total_orders, AVG(order_total) as avg_order_value
FROM orders_experiment__orders;
```

### Query features

The query interface supports:
- **Standard SQL operations**: `SELECT`, `JOIN`, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`
- **Table naming**: Use the format `<experiment_name>__<table_name>` (e.g., `my_experiment__customers`)
- **Error handling**: Clear error messages for syntax errors or missing tables
- **Export formats**: JSON (default) or CSV via API; console display or CSV file via CLI

## Local API + Web UI

- The Python service now ships with a FastAPI control plane (default port `8000`)
  exposing `GET/POST/DELETE /api/experiments`, `POST /api/experiments/{name}/generate`,
  `POST /api/experiments/{name}/reset`, `POST /api/query/execute`, and `POST /api/experiments/import-sql`.
- Run it locally via `dw-sim api --host 0.0.0.0 --port 8000` (or `docker compose up
  synthetic-data-generator`).
- `services/web-ui` contains a static UI (served via Docker Compose or
  `python -m http.server 4173`) that lets you:
  - List/create/delete/reset experiments
  - Paste JSON schemas or import SQL DDL for Redshift/Snowflake
  - Generate synthetic data with custom row counts and seeds
  - View generation run history with real-time status tracking
  - **Execute SQL queries** against populated experiments
  - Export query results to CSV format
  - Save queries as `.sql` files for reuse
