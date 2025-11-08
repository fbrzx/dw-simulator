# Query Rewriting Specification

## Problem Statement

Users need to write SQL queries using their original table names (e.g., `SAS_HOUSEHOLD_VW`) but the system stores tables with experiment prefixes (e.g., `rl_dw__sas_household_vw`).

**Current State:**
- Tables stored as: `{experiment}__{ lowercase(table_name)}`
- Users must manually rewrite queries to use prefixed names
- Temporary views don't work due to connection lifecycle

**Desired State:**
- Users write queries with original table names
- System automatically rewrites queries to use prefixed names
- Works seamlessly across all SQL warehouses (SQLite, Redshift, Snowflake)

## Solution Design

### Option 1: SQL Query Rewriting (RECOMMENDED)

**Approach**: Parse the SQL query, identify table references, and rewrite them with experiment prefixes.

**Pros:**
- Works across all databases
- No connection management issues
- Fast and reliable
- Can handle complex queries (JOINs, subqueries, CTEs)

**Cons:**
- Requires SQL parsing
- Must handle all SQL dialects

**Implementation:**
```python
def rewrite_query_for_experiment(sql: str, experiment_name: str, table_mapping: dict[str, str]) -> str:
    """
    Rewrite SQL query to replace logical table names with physical names.

    Args:
        sql: Original SQL query
        experiment_name: Name of the experiment
        table_mapping: Dict mapping logical names -> physical names
                      e.g., {"SAS_HOUSEHOLD_VW": "rl_dw__sas_household_vw"}

    Returns:
        Rewritten SQL with table references replaced
    """
    # Use sqlglot to parse and rewrite the query
    # Handle case-insensitive matching
    # Replace table names while preserving schema qualifiers
```

### Option 2: Database Views/Synonyms

**Approach**: Create persistent views or synonyms in the warehouse database.

**Pros:**
- Standard SQL feature
- No query modification needed

**Cons:**
- Pollutes database with many views
- Name conflicts between experiments
- Not all warehouses support synonyms
- Cleanup complexity

### Option 3: Schema-Based Isolation

**Approach**: Each experiment gets its own PostgreSQL schema.

**Pros:**
- Clean namespace separation
- Standard SQL schemas

**Cons:**
- Requires schema management
- More complex migrations
- Breaks existing data

## Recommended Implementation

### Phase 1: SQL Query Rewriting with sqlglot

**Files to modify:**
1. `services/dw-simulator/src/dw_simulator/persistence.py`
   - Add `_rewrite_query_for_experiment()` method
   - Modify `execute_query()` to use rewriter

2. `services/dw-simulator/src/dw_simulator/query_rewriter.py` (NEW)
   - Dedicated module for SQL rewriting
   - Uses sqlglot for parsing
   - Handles all SQL dialects

**Algorithm:**
```
1. When experiment_name is provided:
   a. Fetch logical table names from metadata
   b. Build mapping: logical_name -> physical_name
      - Handle case-insensitive matching
      - Strip schema qualifiers from logical names

2. Parse SQL with sqlglot:
   a. Identify all table references
   b. For each table reference:
      - Extract table name (without schema)
      - Look up in mapping (case-insensitive)
      - If found, replace with physical name

3. Return rewritten SQL
4. Execute against warehouse
```

**Example:**
```
Input:  SELECT * FROM SAS_HOUSEHOLD_VW
        JOIN SAS_TRANSACTION_VW ON ...

Mapping: {
  "sas_household_vw": "rl_dw__sas_household_vw",
  "sas_transaction_vw": "rl_dw__sas_transaction_vw"
}

Output: SELECT * FROM rl_dw__sas_household_vw
        JOIN rl_dw__sas_transaction_vw ON ...
```

### Phase 2: UI Improvements

**Add to SQL Query tab:**
- Show available table names for selected experiment
- Auto-complete table names
- Display rewritten query (optional debug mode)

### Phase 3: Testing

**Test cases:**
1. Simple SELECT with single table
2. JOINs across multiple tables
3. Subqueries and CTEs
4. Mixed case table names
5. Schema-qualified names (public.table)
6. Table aliases
7. Error handling for unknown tables

## Implementation Steps

1. **Create query_rewriter.py module**
   - Implement table name extraction
   - Implement query rewriting with sqlglot
   - Add comprehensive tests

2. **Update persistence.py**
   - Add table mapping builder
   - Integrate rewriter into execute_query()
   - Remove temp view logic

3. **Add tests**
   - Unit tests for rewriter
   - Integration tests for query execution

4. **Update documentation**
   - User guide: how queries work
   - Examples of query patterns

## Success Criteria

- ✅ Users can query `SELECT * FROM SAS_HOUSEHOLD_VW`
- ✅ Works for all SQL patterns (JOINs, CTEs, subqueries)
- ✅ Case-insensitive table name matching
- ✅ Clear error messages for unknown tables
- ✅ No performance degradation
- ✅ Works across SQLite, Redshift, Snowflake

## Alternative Approaches Considered

### Persistent Views in Warehouse
**Rejected**: Pollutes namespace, name conflicts, cleanup complexity

### Search Path Manipulation (PostgreSQL)
**Rejected**: PostgreSQL-specific, doesn't work for SQLite/Snowflake

### Table Renaming
**Rejected**: Would break multi-experiment support

## Migration Plan

1. Deploy query rewriter (backward compatible)
2. Update UI to default to using experiment_name
3. Remove temp view code after validation
4. Update examples and documentation

## Future Enhancements

- Query optimization hints
- Query history per experiment
- Saved query templates
- Query performance analytics
