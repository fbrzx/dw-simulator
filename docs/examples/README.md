# DW Simulator Examples

This directory contains comprehensive examples to help you get started with the DW Simulator. All examples are tested and ready to use.

## Directory Structure

```
docs/examples/
├── schemas/          # JSON schema definitions for experiments
├── queries/          # SQL query examples for analyzing generated data
└── sql/              # SQL DDL scripts for various dialects
```

## Quick Start

### 1. E-Commerce Example

Create a complete e-commerce dataset with customers, products, orders, and order items demonstrating foreign key relationships.

**Create the experiment:**
```bash
cd services/dw-simulator
dw-sim experiment create ../../docs/examples/schemas/ecommerce_simple.json
```

**Generate data:**
```bash
dw-sim experiment generate ecommerce_simple --seed 12345
```

**Run example queries:**
```bash
# Customer lifetime value analysis
dw-sim query execute "$(cat ../../docs/examples/queries/ecommerce_01_customer_metrics.sql)"

# Product performance analysis
dw-sim query execute "$(cat ../../docs/examples/queries/ecommerce_02_product_performance.sql)"

# Monthly revenue trends
dw-sim query execute "$(cat ../../docs/examples/queries/ecommerce_03_monthly_revenue.sql)"
```

**Via Web UI:**
1. Open http://localhost:4173
2. Go to "Create Experiment"
3. Paste contents of `schemas/ecommerce_simple.json`
4. Click "Create Experiment"
5. Click "Generate Data" with default settings
6. Navigate to "SQL Query Interface"
7. Copy/paste queries from `queries/ecommerce_*.sql`

### 2. Marketing Campaign Example

Create a marketing analytics dataset tracking campaigns, email sends, opens, and clicks.

**Create the experiment:**
```bash
dw-sim experiment create ../../docs/examples/schemas/marketing_campaigns.json
```

**Generate data:**
```bash
dw-sim experiment generate marketing_campaigns --seed 99999
```

**Run example queries:**
```bash
# Campaign performance overview
dw-sim query execute "$(cat ../../docs/examples/queries/marketing_01_campaign_performance.sql)"

# Subscriber engagement analysis
dw-sim query execute "$(cat ../../docs/examples/queries/marketing_02_subscriber_engagement.sql)"

# Funnel analysis (send → open → click)
dw-sim query execute "$(cat ../../docs/examples/queries/marketing_03_funnel_analysis.sql)"
```

## Schema Examples

### E-Commerce (`schemas/ecommerce_simple.json`)

**Tables:**
- `customers` (200 rows) - Customer information with Faker-generated names, emails, cities
- `products` (100 rows) - Product catalog with prices
- `orders` (500 rows) - Customer orders with foreign key to customers
- `order_items` (1,500 rows) - Line items with foreign keys to orders and products

**Demonstrates:**
- ✅ Foreign key relationships (3-level chain)
- ✅ Faker rules for realistic data (names, emails, cities)
- ✅ Numeric ranges (prices between $9.99 - $999.99)
- ✅ Date ranges (2023-01-01 to 2024-12-31)
- ✅ Multi-table JOINs

### E-Commerce (Complete) (`schemas/ecommerce_complete.json`)

A more comprehensive version with:
- 1,000 customers, 500 products, 3,000 orders, 8,000 order items
- Additional fields: loyalty points, shipping costs, tax amounts, ratings, stock levels
- More complex foreign key relationships

**Note:** This schema includes BOOLEAN columns which may have compatibility issues with some configurations. Use `ecommerce_simple.json` for guaranteed compatibility.

### Marketing Campaigns (`schemas/marketing_campaigns.json`)

**Tables:**
- `campaigns` (100 rows) - Marketing campaign metadata
- `subscribers` (10,000 rows) - Email subscriber list
- `email_sends` (50,000 rows) - Individual email sends
- `email_opens` (25,000 rows) - Email open tracking
- `email_clicks` (15,000 rows) - Link click tracking

**Demonstrates:**
- ✅ Multi-level foreign key chains (campaigns → sends → opens/clicks)
- ✅ Conversion funnel analysis
- ✅ Realistic marketing metrics (open rates, click-through rates)
- ✅ User agent and device tracking
- ✅ Time series analysis

## SQL Query Examples

### E-Commerce Queries

| File | Description | Key Metrics |
|------|-------------|-------------|
| `ecommerce_01_customer_metrics.sql` | Customer lifetime value and purchase behavior | Total orders, lifetime value, average order value per customer |
| `ecommerce_02_product_performance.sql` | Product sales and profitability analysis | Units sold, revenue, profit by product |
| `ecommerce_03_monthly_revenue.sql` | Monthly revenue trends | Revenue, orders, average order value by month |
| `ecommerce_04_customer_cohorts.sql` | Customer cohort analysis by signup month | Cohort size, activation rate, revenue by signup month |
| `ecommerce_05_cart_analysis.sql` | Shopping cart and order composition | Items per order, product combinations |

### Marketing Queries

| File | Description | Key Metrics |
|------|-------------|-------------|
| `marketing_01_campaign_performance.sql` | Campaign-level performance metrics | Sends, opens, clicks, open rate, CTR by campaign |
| `marketing_02_subscriber_engagement.sql` | Individual subscriber engagement levels | Personal open rate, CTR, engagement scoring |
| `marketing_03_funnel_analysis.sql` | Conversion funnel from send to click | Stage-by-stage conversion rates and drop-off |
| `marketing_04_device_analysis.sql` | Device and user agent breakdown | Opens and clicks by device type |
| `marketing_05_time_series.sql` | Daily email performance trends | Daily metrics over time |

## SQL DDL Scripts

The `sql/` directory contains SQL DDL scripts for importing existing schemas:

### Customers Dataset
- `customers_ansi.sql` - ANSI SQL (SQLite compatible)
- `customers_redshift.sql` - Redshift dialect
- `customers_snowflake.sql` - Snowflake dialect

### Orders Dataset
- `orders_ansi.sql` - Multi-table with orders + order_items
- `orders_redshift.sql` - Redshift version
- `orders_snowflake.sql` - Snowflake version

### Products Dataset
- `products_ansi.sql` - Product catalog table
- `products_redshift.sql` - With TIMESTAMP support
- `products_snowflake.sql` - With NUMBER types

**Usage:**
```bash
# Import SQL DDL
dw-sim experiment import-sql ../../docs/examples/sql/customers_redshift.sql \
  --name customers_from_sql \
  --dialect redshift

# Generate data
dw-sim experiment generate customers_from_sql
```

## Tips and Best Practices

### Starting Small
- Begin with small datasets (100-1,000 rows) to validate your schema
- Increase row counts after confirming queries work correctly
- Use `--seed` parameter for reproducible datasets

### Foreign Key Relationships
- Always generate parent tables before child tables (handled automatically)
- Use foreign keys to maintain referential integrity
- Nullable foreign keys allow ~10% NULL values

### Realistic Data Generation
- Use Faker rules for names, emails, addresses, companies
- Define min/max ranges for numeric columns (prices, quantities)
- Set date ranges to match your analysis period

### Query Development
- Start with simple SELECT queries to verify data structure
- Build up to complex JOINs and aggregations
- Use LIMIT clause during development to speed up queries
- Export results to CSV for sharing: `--output results.csv`

### Performance
- Use appropriate row counts for your use case:
  - Quick tests: 100-1,000 rows
  - Development: 10,000-100,000 rows
  - Performance testing: 1M+ rows
- Smaller batches generate faster
- Consider using seeds for reproducibility

## Common Patterns

### Customer Analytics Pattern
```json
{
  "tables": [
    {
      "name": "customers",
      "columns": [
        {"name": "id", "data_type": "INT", "is_unique": true},
        {"name": "email", "faker_rule": "email"},
        {"name": "signup_date", "data_type": "DATE", "date_start": "2023-01-01"}
      ]
    },
    {
      "name": "transactions",
      "columns": [
        {"name": "customer_id", "foreign_key": {"references_table": "customers", "references_column": "id"}},
        {"name": "amount", "data_type": "FLOAT", "min_value": 10.0, "max_value": 1000.0}
      ]
    }
  ]
}
```

### Event Tracking Pattern
```json
{
  "tables": [
    {
      "name": "events",
      "columns": [
        {"name": "event_id", "data_type": "INT", "is_unique": true},
        {"name": "user_id", "data_type": "INT"},
        {"name": "event_type", "data_type": "VARCHAR"},
        {"name": "event_date", "data_type": "DATE", "date_start": "2024-01-01"}
      ]
    }
  ]
}
```

## Troubleshooting

### Schema Validation Errors
- Check that all referenced tables/columns exist for foreign keys
- Ensure column names are valid SQL identifiers
- Verify data types are supported (INT, FLOAT, VARCHAR, DATE)

### Data Generation Errors
- Verify parent tables are defined before child tables with FKs
- Check that min/max ranges are valid (min < max)
- Ensure date ranges are in correct format (YYYY-MM-DD)

### Query Errors
- Use experiment-prefixed table names: `{experiment_name}__{table_name}`
- Verify all columns exist in the schema
- Check for proper JOIN conditions on foreign key columns

## Additional Resources

- [Product Specification](../product-spec.md) - Full product requirements
- [Technical Specification](../tech-spec.md) - Architecture details
- [Main README](../../README.md) - Getting started guide
- [Service README](../../services/dw-simulator/README.md) - CLI reference

## Contributing Examples

Have a useful example? Consider contributing!

1. Create your schema in `schemas/`
2. Add 3-5 representative queries in `queries/`
3. Test end-to-end: create → generate → query
4. Document the use case in this README
5. Submit a pull request

---

**All examples in this directory are tested and maintained as part of the DW Simulator test suite.**
