# Sample SQL Scripts

This directory contains sample CREATE TABLE scripts demonstrating different SQL dialects and use cases.

## Quick Reference

| Dataset   | Tables              | ANSI          | Redshift          | Snowflake          |
|-----------|---------------------|---------------|-------------------|--------------------|
| Customers | customers           | ✓             | ✓                 | ✓                  |
| Orders    | orders, order_items | ✓             | ✓                 | ✓                  |
| Products  | products            | ✓             | ✓                 | ✓                  |

**9 total scripts** - 3 datasets × 3 dialects

## Usage

1. Open the DW Simulator Web UI at http://localhost:4173
2. Go to "Create Experiment" tab
3. Click "SQL Import" mode
4. Copy the contents of one of these scripts into the SQL textarea
5. Select the appropriate dialect and target warehouse
6. Click "Import SQL"

## Files

Each dataset is available in three SQL dialect variants:

### Customers Dataset (Single Table)

- **customers_ansi.sql** - ANSI SQL dialect for SQLite
- **customers_redshift.sql** - Redshift dialect with INT/FLOAT types
- **customers_snowflake.sql** - Snowflake dialect with NUMBER type

### Orders Dataset (Multi-Table: orders + order_items)

- **orders_ansi.sql** - ANSI SQL dialect for SQLite
- **orders_redshift.sql** - Redshift dialect with INT/FLOAT types
- **orders_snowflake.sql** - Snowflake dialect with NUMBER type

### Products Dataset (Single Table)

- **products_ansi.sql** - ANSI SQL dialect for SQLite
- **products_redshift.sql** - Redshift dialect with TIMESTAMP
- **products_snowflake.sql** - Snowflake dialect with NUMBER type

## Dialect vs Warehouse

- **Dialect**: Determines how the SQL syntax is parsed (redshift, snowflake, ansi)
- **Target Warehouse**: Where the data will be stored (sqlite, redshift, snowflake)

You can use any dialect to create tables in any warehouse. For example:
- Use Redshift dialect → Store in SQLite warehouse
- Use Snowflake dialect → Store in Redshift warehouse
