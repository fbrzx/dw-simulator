# Web UI

A lightweight, dependency-free control panel to manage DW Simulator experiments.

## Features

- **List experiments** exposed by the Python API (`GET /api/experiments`).
- **Create experiments** by pasting JSON schemas and sending them to the API.
- **Import SQL DDL** (Redshift/Snowflake) via `/api/experiments/import-sql` by
  pasting scripts and choosing a dialect.
- **Generate synthetic data** for experiments with options to:
  - Override row counts per table
  - Set a seed for deterministic generation
  - View generation results (row counts)
- **View generation runs** for each experiment with:
  - Real-time status tracking (RUNNING, COMPLETED, FAILED, ABORTED)
  - Automatic polling every 3 seconds for live updates
  - Detailed run metadata (timestamps, duration, row counts, errors)
  - Full error messages and tracebacks for debugging failed runs
- **Execute SQL queries** against populated experiments with:
  - Interactive query editor with syntax highlighting
  - Results displayed in formatted tables
  - Export results to CSV format
  - Save queries as .sql files for reuse
  - Clear error messages for syntax errors
- **Delete experiments** and drop their corresponding tables.

## Running locally

1. Start the Python API so the UI has something to call:
   ```bash
   cd services/dw-simulator
   uvicorn dw_simulator.api:app --reload --port 8000
   ```
2. Serve the static UI (any local server works):
   ```bash
   cd services/web-ui
   python -m http.server 4173
   ```
3. Open [http://localhost:4173](http://localhost:4173) and interact with the UI.

Docker Compose already includes an `nginx` powered `web-ui` service that serves
this folder and proxies to the Python API.

## Using the Query Interface

The web UI includes a SQL query interface that allows you to query your populated experiments directly from the browser.

### Executing Queries

1. Navigate to the **SQL Query Interface** section on the control panel
2. Enter your SQL query in the text area. Example:
   ```sql
   SELECT * FROM customers_experiment__customers LIMIT 10;
   ```
3. Click **Execute Query** to run the query
4. Results will be displayed in a formatted table below the query input

### Query Features

- **Clear**: Clears the current query and results
- **Save SQL**: Downloads the query text as a `.sql` file for reuse
- **Export CSV**: After executing a query, export the results as a CSV file
- **Row Count**: Displays the number of rows returned by the query
- **Error Handling**: Clear error messages are shown for syntax errors or execution failures

### Example Queries

Basic selection:
```sql
SELECT * FROM my_experiment__users LIMIT 100;
```

With filtering and ordering:
```sql
SELECT customer_id, email, registration_date
FROM customers_experiment__customers
WHERE registration_date > '2024-01-01'
ORDER BY registration_date DESC;
```

Aggregations:
```sql
SELECT COUNT(*) as total_orders, AVG(order_total) as avg_order_value
FROM orders_experiment__orders;
```

### Notes

- Table names follow the pattern: `{experiment_name}__{table_name}`
- The query interface uses the same backend as the CLI (`dw-sim query execute`)
- Query execution is synchronous - large result sets may take some time to render
