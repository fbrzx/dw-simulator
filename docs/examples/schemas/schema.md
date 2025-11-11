# Schema Definition Format

This document describes the JSON schema format used to define datasets for the DW Simulator. The schema defines tables, columns, data types, relationships, and data generation rules.

## Schema Structure

```json
{
  "name": "schema_name",
  "description": "A description of the dataset and its purpose",
  "tables": [
    // Array of table definitions
  ]
}
```

### Top-Level Properties

- **name** (string, required): The name of the schema
- **description** (string, recommended): A description of the dataset, its purpose, and key characteristics
- **tables** (array, required): Array of table definitions

## Table Definition

Each table in the `tables` array has the following structure:

```json
{
  "name": "table_name",
  "target_rows": 1000,
  "columns": [
    // Array of column definitions
  ]
}
```

### Table Properties

- **name** (string, required): The name of the table
- **target_rows** (integer, required): The number of rows to generate for this table. Set to `0` to skip generation for this table (useful when referencing existing data from previous experiments)
- **columns** (array, required): Array of column definitions

## Column Definition

Each column has a core set of properties plus type-specific properties:

### Core Column Properties

```json
{
  "name": "column_name",
  "data_type": "VARCHAR",
  "required": true,
  "is_unique": false
}
```

- **name** (string, required): The name of the column
- **data_type** (string, required): The SQL data type (see Data Types section below)
- **required** (boolean, optional, default: false): Whether the column can contain NULL values (if false, ~5% of values will be NULL)
- **is_unique** (boolean, optional, default: false): Whether all values must be unique (primary keys, unique identifiers)

**Unique Value Generation:**
When `is_unique: true` is set, the generator ensures all values are unique:
- **INT/BIGINT/SMALLINT**: Sequential generation starting from `min_value` (or 0 if not specified)
- **FLOAT/DOUBLE**: Sequential generation as float values (0.0, 1.0, 2.0, etc.)
- **DATE**: Sequential dates starting from `date_start` (MUST specify adequate date range)
- **VARCHAR/CHAR**: Generated using faker rules with uniqueness checks (may fail if range is too small)
- **UUID**: Automatically generates unique UUIDs

## Data Types

### Integer Types

#### INT / INTEGER
Standard integer type.

```json
{
  "name": "user_id",
  "data_type": "INT",
  "min_value": 1,
  "max_value": 999999
}
```

**Properties:**
- `min_value` (integer, optional): Minimum value (default: 0 for regular columns, -2147483648 for system range)
- `max_value` (integer, optional): Maximum value (default: 1000000)

**Unique Constraint:** When `is_unique: true`, integers are generated sequentially starting from `min_value`. Ensure the range `max_value - min_value + 1` is at least equal to `target_rows`.

#### SMALLINT
Small integer type for values between -32,768 and 32,767.

```json
{
  "name": "age",
  "data_type": "SMALLINT",
  "min_value": 18,
  "max_value": 100
}
```

**Properties:**
- `min_value` (integer, optional): Minimum value (default: -32768)
- `max_value` (integer, optional): Maximum value (default: 32767)

#### BIGINT
Large integer type for values up to ±9,223,372,036,854,775,807.

```json
{
  "name": "transaction_amount_cents",
  "data_type": "BIGINT",
  "min_value": 0,
  "max_value": 999999999999
}
```

**Properties:**
- `min_value` (integer, optional): Minimum value (default: -9223372036854775808)
- `max_value` (integer, optional): Maximum value (default: 9223372036854775807)

### Floating Point Types

#### FLOAT / REAL
Single-precision floating point number.

```json
{
  "name": "temperature",
  "data_type": "FLOAT",
  "min_value": -50.0,
  "max_value": 50.0
}
```

**Properties:**
- `min_value` (float, optional): Minimum value (default: 0.0)
- `max_value` (float, optional): Maximum value (default: 1000000.0)

**Unique Constraint:** When `is_unique: true`, floats are generated sequentially as whole numbers (0.0, 1.0, 2.0, etc.). This is generally not recommended for unique columns - prefer INT instead.

#### DOUBLE / DOUBLE PRECISION
Double-precision floating point number.

```json
{
  "name": "latitude",
  "data_type": "DOUBLE",
  "min_value": -90.0,
  "max_value": 90.0
}
```

**Properties:**
- `min_value` (float, optional): Minimum value
- `max_value` (float, optional): Maximum value

#### DECIMAL / NUMERIC
Fixed-precision decimal number.

```json
{
  "name": "price",
  "data_type": "DECIMAL",
  "precision": 10,
  "scale": 2,
  "min_value": 0.01,
  "max_value": 9999.99
}
```

**Properties:**
- `precision` (integer, required): Total number of digits
- `scale` (integer, required): Number of digits after the decimal point
- `min_value` (float, optional): Minimum value
- `max_value` (float, optional): Maximum value

### String Types

#### VARCHAR / CHARACTER VARYING
Variable-length character string.

```json
{
  "name": "email",
  "data_type": "VARCHAR",
  "varchar_length": 255,
  "faker_rule": "email"
}
```

**Properties:**
- `varchar_length` (integer, required): Maximum length of the string
- `faker_rule` (string, optional): Faker rule to generate realistic data (see Faker Rules section)

**Unique Constraint:** When `is_unique: true`, the generator attempts to produce unique strings and will retry up to 1000 times if collisions occur. For large `target_rows`, use faker rules that produce highly varied output (like `email`, `uuid`, `ipv4`) or ensure the faker rule can generate enough unique values.

#### CHAR / CHARACTER
Fixed-length character string.

```json
{
  "name": "country_code",
  "data_type": "CHAR",
  "char_length": 2
}
```

**Properties:**
- `char_length` (integer, required): Fixed length of the string
- `faker_rule` (string, optional): Faker rule to generate realistic data

#### TEXT
Variable-length text field for long strings.

```json
{
  "name": "description",
  "data_type": "TEXT",
  "faker_rule": "text"
}
```

**Properties:**
- `faker_rule` (string, optional): Faker rule to generate realistic data

### Date and Time Types

#### DATE
Calendar date (year, month, day).

```json
{
  "name": "birth_date",
  "data_type": "DATE",
  "date_start": "1950-01-01",
  "date_end": "2005-12-31"
}
```

**Properties:**
- `date_start` (string, optional): Start of date range in YYYY-MM-DD format (default: 2020-01-01)
- `date_end` (string, optional): End of date range in YYYY-MM-DD format (default: 2025-12-31)

**Important:** When using `is_unique: true` with DATE columns, ensure the date range contains enough days to accommodate the `target_rows`. For example, a date range from 2008-01-01 to 2025-12-31 contains 6,574 days, which is sufficient for 6,209 unique dates. Unique dates are generated sequentially within the specified range.

#### TIMESTAMP / TIMESTAMP WITHOUT TIME ZONE
Date and time without timezone.

```json
{
  "name": "created_at",
  "data_type": "TIMESTAMP",
  "timestamp_start": "2024-01-01 00:00:00",
  "timestamp_end": "2024-12-31 23:59:59"
}
```

**Properties:**
- `timestamp_start` (string, optional): Start timestamp in YYYY-MM-DD HH:MM:SS format
- `timestamp_end` (string, optional): End timestamp in YYYY-MM-DD HH:MM:SS format

#### TIMESTAMPTZ / TIMESTAMP WITH TIME ZONE
Date and time with timezone.

```json
{
  "name": "event_time",
  "data_type": "TIMESTAMPTZ",
  "timestamp_start": "2024-01-01 00:00:00+00",
  "timestamp_end": "2024-12-31 23:59:59+00"
}
```

**Properties:**
- `timestamp_start` (string, optional): Start timestamp in YYYY-MM-DD HH:MM:SS+TZ format
- `timestamp_end` (string, optional): End timestamp in YYYY-MM-DD HH:MM:SS+TZ format

#### DATETIME
MySQL-style datetime type (equivalent to TIMESTAMP).

```json
{
  "name": "order_datetime",
  "data_type": "DATETIME",
  "timestamp_start": "2024-01-01 00:00:00",
  "timestamp_end": "2024-12-31 23:59:59"
}
```

**Properties:**
- `timestamp_start` (string, optional): Start timestamp in YYYY-MM-DD HH:MM:SS format
- `timestamp_end` (string, optional): End timestamp in YYYY-MM-DD HH:MM:SS format

#### TIME
Time of day (without date).

```json
{
  "name": "opening_time",
  "data_type": "TIME",
  "time_start": "08:00:00",
  "time_end": "18:00:00"
}
```

**Properties:**
- `time_start` (string, optional): Start time in HH:MM:SS format
- `time_end` (string, optional): End time in HH:MM:SS format

### Boolean Type

#### BOOLEAN / BOOL
True or false value.

```json
{
  "name": "is_active",
  "data_type": "BOOLEAN"
}
```

**Properties:** None. Randomly generates true/false values.

### JSON Type

#### JSON / JSONB
JSON data structure.

```json
{
  "name": "metadata",
  "data_type": "JSON"
}
```

**Properties:** None. Generates random JSON structures.

### UUID Type

#### UUID
Universally unique identifier.

```json
{
  "name": "uuid",
  "data_type": "UUID"
}
```

**Properties:** None. Automatically generates valid UUIDs.

## Foreign Key Relationships

Foreign keys establish relationships between tables. The referenced table must be defined before the referencing table in the schema.

```json
{
  "name": "order_id",
  "data_type": "INT",
  "required": true,
  "foreign_key": {
    "references_table": "orders",
    "references_column": "order_id"
  }
}
```

**Foreign Key Properties:**
- `references_table` (string, required): Name of the referenced table
- `references_column` (string, required): Name of the referenced column

**Important Notes:**
- The referenced column must exist and be unique in the referenced table
- The data type of the foreign key column must match the referenced column
- Tables must be ordered so referenced tables appear before referencing tables

## Faker Rules

Faker rules generate realistic-looking data using the Faker library. Available rules include:

### Personal Information

- **first_name**: Random first name
  ```json
  {"faker_rule": "first_name"}
  ```

- **last_name**: Random last name
  ```json
  {"faker_rule": "last_name"}
  ```

- **name**: Random full name
  ```json
  {"faker_rule": "name"}
  ```

- **email**: Random email address
  ```json
  {"faker_rule": "email"}
  ```

- **phone_number**: Random phone number
  ```json
  {"faker_rule": "phone_number"}
  ```

- **ssn**: Random Social Security Number
  ```json
  {"faker_rule": "ssn"}
  ```

### Location

- **address**: Random street address
  ```json
  {"faker_rule": "address"}
  ```

- **street_address**: Random street address (without city/state)
  ```json
  {"faker_rule": "street_address"}
  ```

- **city**: Random city name
  ```json
  {"faker_rule": "city"}
  ```

- **state**: Random state name
  ```json
  {"faker_rule": "state"}
  ```

- **state_abbr**: Random state abbreviation
  ```json
  {"faker_rule": "state_abbr"}
  ```

- **zipcode**: Random ZIP code
  ```json
  {"faker_rule": "zipcode"}
  ```

- **country**: Random country name
  ```json
  {"faker_rule": "country"}
  ```

- **country_code**: Random country code (2 letters)
  ```json
  {"faker_rule": "country_code"}
  ```

- **latitude**: Random latitude coordinate
  ```json
  {"faker_rule": "latitude"}
  ```

- **longitude**: Random longitude coordinate
  ```json
  {"faker_rule": "longitude"}
  ```

### Company

- **company**: Random company name
  ```json
  {"faker_rule": "company"}
  ```

- **company_suffix**: Random company suffix (Inc., LLC, etc.)
  ```json
  {"faker_rule": "company_suffix"}
  ```

- **catch_phrase**: Random business catch phrase
  ```json
  {"faker_rule": "catch_phrase"}
  ```

- **bs**: Random business buzzword phrase
  ```json
  {"faker_rule": "bs"}
  ```

- **job**: Random job title
  ```json
  {"faker_rule": "job"}
  ```

### Internet

- **url**: Random URL
  ```json
  {"faker_rule": "url"}
  ```

- **domain_name**: Random domain name
  ```json
  {"faker_rule": "domain_name"}
  ```

- **ipv4**: Random IPv4 address
  ```json
  {"faker_rule": "ipv4"}
  ```

- **ipv6**: Random IPv6 address
  ```json
  {"faker_rule": "ipv6"}
  ```

- **mac_address**: Random MAC address
  ```json
  {"faker_rule": "mac_address"}
  ```

- **user_agent**: Random browser user agent string
  ```json
  {"faker_rule": "user_agent"}
  ```

- **username**: Random username
  ```json
  {"faker_rule": "username"}
  ```

### Text

- **text**: Random paragraph of text
  ```json
  {"faker_rule": "text"}
  ```

- **sentence**: Random sentence
  ```json
  {"faker_rule": "sentence"}
  ```

- **word**: Random word
  ```json
  {"faker_rule": "word"}
  ```

### Other

- **credit_card_number**: Random credit card number
  ```json
  {"faker_rule": "credit_card_number"}
  ```

- **credit_card_provider**: Random credit card provider name
  ```json
  {"faker_rule": "credit_card_provider"}
  ```

- **currency_code**: Random currency code (USD, EUR, etc.)
  ```json
  {"faker_rule": "currency_code"}
  ```

- **iban**: Random IBAN (International Bank Account Number)
  ```json
  {"faker_rule": "iban"}
  ```

- **color_name**: Random color name
  ```json
  {"faker_rule": "color_name"}
  ```

- **file_name**: Random file name
  ```json
  {"faker_rule": "file_name"}
  ```

- **file_extension**: Random file extension
  ```json
  {"faker_rule": "file_extension"}
  ```

- **mime_type**: Random MIME type
  ```json
  {"faker_rule": "mime_type"}
  ```

## Complete Example

Here's a complete example demonstrating various data types and features:

```json
{
  "name": "ecommerce",
  "description": "E-commerce dataset with customers, products, and orders",
  "tables": [
    {
      "name": "customers",
      "target_rows": 1000,
      "columns": [
        {
          "name": "customer_id",
          "data_type": "BIGINT",
          "is_unique": true,
          "required": true
        },
        {
          "name": "email",
          "data_type": "VARCHAR",
          "varchar_length": 255,
          "faker_rule": "email",
          "required": true,
          "is_unique": true
        },
        {
          "name": "first_name",
          "data_type": "VARCHAR",
          "varchar_length": 50,
          "faker_rule": "first_name"
        },
        {
          "name": "last_name",
          "data_type": "VARCHAR",
          "varchar_length": 50,
          "faker_rule": "last_name"
        },
        {
          "name": "registration_date",
          "data_type": "TIMESTAMP",
          "timestamp_start": "2020-01-01 00:00:00",
          "timestamp_end": "2024-12-31 23:59:59"
        },
        {
          "name": "is_active",
          "data_type": "BOOLEAN"
        },
        {
          "name": "lifetime_value",
          "data_type": "DECIMAL",
          "precision": 10,
          "scale": 2,
          "min_value": 0,
          "max_value": 50000
        }
      ]
    },
    {
      "name": "products",
      "target_rows": 500,
      "columns": [
        {
          "name": "product_id",
          "data_type": "INT",
          "is_unique": true,
          "required": true
        },
        {
          "name": "sku",
          "data_type": "VARCHAR",
          "varchar_length": 50,
          "is_unique": true,
          "required": true
        },
        {
          "name": "product_name",
          "data_type": "VARCHAR",
          "varchar_length": 200
        },
        {
          "name": "price",
          "data_type": "DECIMAL",
          "precision": 8,
          "scale": 2,
          "min_value": 1.00,
          "max_value": 9999.99
        },
        {
          "name": "stock_quantity",
          "data_type": "SMALLINT",
          "min_value": 0,
          "max_value": 1000
        },
        {
          "name": "created_at",
          "data_type": "TIMESTAMPTZ",
          "timestamp_start": "2020-01-01 00:00:00+00",
          "timestamp_end": "2024-12-31 23:59:59+00"
        }
      ]
    },
    {
      "name": "orders",
      "target_rows": 5000,
      "columns": [
        {
          "name": "order_id",
          "data_type": "BIGINT",
          "is_unique": true,
          "required": true
        },
        {
          "name": "customer_id",
          "data_type": "BIGINT",
          "required": true,
          "foreign_key": {
            "references_table": "customers",
            "references_column": "customer_id"
          }
        },
        {
          "name": "product_id",
          "data_type": "INT",
          "required": true,
          "foreign_key": {
            "references_table": "products",
            "references_column": "product_id"
          }
        },
        {
          "name": "order_date",
          "data_type": "DATETIME",
          "timestamp_start": "2024-01-01 00:00:00",
          "timestamp_end": "2024-12-31 23:59:59"
        },
        {
          "name": "quantity",
          "data_type": "SMALLINT",
          "min_value": 1,
          "max_value": 10
        },
        {
          "name": "total_amount",
          "data_type": "DECIMAL",
          "precision": 10,
          "scale": 2,
          "min_value": 1.00,
          "max_value": 99999.99
        },
        {
          "name": "status",
          "data_type": "VARCHAR",
          "varchar_length": 20
        }
      ]
    }
  ]
}
```

## Best Practices

1. **Table Ordering**: Define tables in dependency order - referenced tables must come before tables that reference them

2. **Primary Keys**: Always include a unique identifier column for each table with `is_unique: true` and `required: true`

3. **Data Types**: Choose appropriate precision for numeric types:
   - Use SMALLINT for small ranges (0-32,767)
   - Use INT for most integer needs
   - Use BIGINT for large identifiers or counts
   - Use DECIMAL for monetary values (never FLOAT)

4. **Timestamps**:
   - Use TIMESTAMP for application timestamps
   - Use TIMESTAMPTZ if timezone information is important
   - Use DATETIME for MySQL compatibility

5. **String Lengths**: Set realistic varchar_length values based on expected content:
   - Email: 255
   - Names: 50-100
   - URLs: 200-500
   - Short codes: 10-20

6. **Faker Rules**: Use faker rules for realistic data generation instead of random strings

7. **Required Fields**: Mark essential columns as `required: true` to avoid NULL values where they don't make sense

8. **Date Ranges**: Provide realistic date ranges that make sense for your use case
   - **CRITICAL for unique dates**: When using `is_unique: true` with DATE columns, ensure your date range has at least as many days as `target_rows`
   - Example: For 6,209 unique dates, a range from 2008-01-01 to 2025-12-31 (6,574 days) works perfectly
   - The generator will fail with a clear error if the date range is insufficient

9. **Foreign Keys**: Ensure target_rows ratios make sense (e.g., more orders than customers)

10. **Documentation**: Always include a clear description at the schema level explaining the dataset's purpose

11. **Skipping Table Generation**: Set `target_rows: 0` to skip data generation for a table while keeping its schema definition. This is useful when:
   - Referencing existing data from previous experiment runs
   - Creating incremental experiments that build on existing data
   - Testing schema changes without regenerating all tables
   - Note: Tables with `target_rows: 0` will not generate any data files, but can still be referenced by foreign keys in other tables (assuming the data exists from a previous run)

## Common Issues and Solutions

### Issue: "Unable to produce unique values for column 'exact_day_dt'"

**Cause:** The DATE column is marked as `is_unique: true` but the date range doesn't contain enough days for the requested number of rows.

**Solution:** Add explicit `date_start` and `date_end` properties with a sufficient range:

```json
{
  "name": "exact_day_dt",
  "data_type": "DATE",
  "required": true,
  "is_unique": true,
  "date_start": "2008-01-01",
  "date_end": "2025-12-31"
}
```

Calculate the required range: (end_date - start_date).days + 1 ≥ target_rows

### Issue: "Unable to produce unique values" for VARCHAR columns

**Cause:** The faker rule or string generation cannot produce enough unique values.

**Solution:** Use faker rules that produce highly varied output:
- ✓ Good: `email`, `uuid`, `ipv4`, `url`, `iban`
- ✗ Poor: `word`, `first_name` (limited variety)

Or ensure your VARCHAR length is large enough and use compound faker rules.
