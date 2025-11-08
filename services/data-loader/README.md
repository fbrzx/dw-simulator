# Data Loader Service (US 5.2 - NOT YET IMPLEMENTED)

## Current Status: 🔴 HIGH PRIORITY - NOT STARTED

**Why this is critical:**
The core project goal is to enable SQL queries against local Redshift/Snowflake emulators. Currently, all data is loaded into SQLite, which prevents testing of warehouse-specific SQL features.

## Planned Responsibilities

This service will be responsible for:

1. **S3 Staging Integration:**
   - Read generated Parquet files from LocalStack S3 staging bucket (`s3://local/dw-simulator/staging/`)
   - Manage S3 object lifecycle (upload, versioning, cleanup)

2. **Warehouse Loading:**
   - Execute PostgreSQL `COPY` commands to load data into Redshift emulator
   - Execute Snowflake `COPY INTO` commands for Snowflake emulator
   - Handle warehouse-specific data type mappings and conversions

3. **Observability:**
   - Track load metadata (start time, end time, rows loaded, errors)
   - Integrate with generation_runs table for end-to-end tracking
   - Provide progress updates for long-running loads

4. **Error Handling:**
   - Retry logic for transient S3/network failures
   - Clear error messages for schema mismatches or constraint violations
   - Rollback support for failed loads

## Implementation Plan (US 5.2)

See `docs/status.md` for the full implementation roadmap, including:

**Phase 1: Redshift Emulator Integration (5-7 days)**
- Dual-database architecture (SQLite for metadata, PostgreSQL for data)
- COPY command implementation for Parquet loading
- Query routing to Redshift emulator

**Phase 2: Snowflake Emulator Integration (3-5 days)**
- LocalStack Snowflake connector setup
- Snowpipe-style loading with COPY INTO
- Snowflake-specific data type handling

**Phase 3: Multi-Warehouse Support (3-4 days)**
- Per-experiment warehouse selection
- Warehouse adapter pattern implementation
- CLI/API/UI updates for warehouse switching

## Technical Architecture (Planned)

```
┌─────────────────────────────────────────────────────────────┐
│  dw-simulator service                                        │
│  ┌──────────────────┐                                        │
│  │ ExperimentGenerator │──► Parquet files ──► LocalStack S3 │
│  └──────────────────┘                            │           │
│                                                   ▼           │
│  ┌──────────────────┐      ┌─────────────────────────┐      │
│  │ DataLoaderService │─────►│ Redshift Emulator       │      │
│  │                   │      │ (PostgreSQL:5439)       │      │
│  │ - load_to_redshift│      └─────────────────────────┘      │
│  │ - load_to_snowflake      ┌─────────────────────────┐      │
│  │ - manage_s3_staging│────►│ Snowflake Emulator      │      │
│  └──────────────────┘      │ (LocalStack:4566)       │      │
│                             └─────────────────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

## Dependencies

- `boto3` - S3 interaction with LocalStack
- `psycopg2` or `sqlalchemy` - PostgreSQL/Redshift connectivity
- `snowflake-connector-python` - Snowflake emulator connectivity (if supported)
- `pyarrow` - Parquet file handling

## Environment Variables (Planned)

```bash
# S3 Staging
DW_SIMULATOR_STAGE_BUCKET=s3://local/dw-simulator/staging
AWS_ENDPOINT_URL=http://local-s3-staging:4566

# Redshift Emulator
DW_SIMULATOR_REDSHIFT_URL=postgresql://dw_user:dw_pass@local-redshift-mock:5432/dw_simulator

# Snowflake Emulator (if supported)
DW_SIMULATOR_SNOWFLAKE_URL=snowflake://user:pass@local-snowflake-emulator:4566/db

# Metadata Database (existing)
DW_SIMULATOR_TARGET_DB_URL=sqlite:////data/sqlite/dw_simulator.db
```

## Testing Strategy

1. **Unit Tests:**
   - S3 upload/download logic
   - COPY command generation
   - Error handling and retry logic

2. **Integration Tests:**
   - End-to-end: Generate → Stage → Load → Query
   - Multi-table loading with foreign keys
   - Large dataset handling (1M+ rows)

3. **Performance Tests:**
   - Loading throughput (rows/second)
   - Memory usage during large loads
   - Query performance on loaded data

## Getting Started (When Implemented)

```bash
# Build the data-loader service
docker compose build data-loader

# Run data loading manually
dw-sim experiment load my_experiment --target-warehouse redshift

# Or via API
curl -X POST http://localhost:8000/api/experiments/my_experiment/load \
  -H "Content-Type: application/json" \
  -d '{"target_warehouse": "redshift"}'
```

## Timeline

- **Estimated effort:** 5-7 days for Phase 1 (Redshift)
- **Dependencies:** None (infrastructure exists in docker-compose.yml)
- **Risk:** LocalStack Snowflake emulator may have limited feature support

## Contributing

If you're interested in implementing this service, please:
1. Review the full plan in `docs/status.md` (US 5.2)
2. Check the technical design in `docs/tech-spec.md`
3. Start with Phase 1 (Redshift emulator integration)
4. Open a PR with comprehensive tests and documentation
