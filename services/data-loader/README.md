# Data Loader Service (Optional Backlog)

## Current Status

The core `synthetic-data-generator` service already performs end-to-end loading:

- Metadata always lives in SQLite.
- Physical tables are created inside the configured warehouse (priority: Redshift/PostgreSQL → Snowflake → SQLite).
- After each generation run the service stages Parquet files under `data/generated/…`, uploads them to LocalStack S3 for parity, and immediately loads the selected warehouse (Snowflake via `COPY INTO`, PostgreSQL/SQLite via fast inserts).

Because loading happens inline, no separate `data-loader` container is required for day-to-day development. This directory remains as a placeholder for future work that may require a dedicated process.

## Why keep this folder?

We may still want a standalone loader when:

1. **Asynchronous or remote loads** – e.g., pushing staged data to a managed Redshift/Snowflake account, or retrying long-running COPY jobs outside of the API/CLI lifecycle.
2. **Advanced warehouse features** – leveraging true Redshift `COPY FROM S3`, Snowpipe auto-ingest, role-based credentials, or warehouse-specific session settings without coupling them to the generator.
3. **Operational separation** – allowing generation to finish quickly while a different service handles loading, monitoring, and alerting.

## Potential Responsibilities (Future)

If we resurrect this service it would likely:

- Watch `data/generated/` (or LocalStack S3) for new run artifacts.
- Execute warehouse-native COPY commands (Redshift `COPY`, Snowflake `COPY INTO`, Snowpipe tasks) using dedicated credentials.
- Manage S3 object lifecycle (expiration, retry markers) independent of the generator.
- Emit rich telemetry (load duration, bytes transferred, failed files) so long-running loads can be monitored separately.

## Implementation Notes (Deferred)

- Follow the multi-phase plan documented in `docs/status.md` whenever new work is scheduled (see backlog items under US 6.x).
- Reuse existing helpers in `dw_simulator.persistence` and `dw_simulator.s3_client` to avoid duplicating staging logic.
- Provide a small CLI (Typer) so developers can trigger targeted reloads without touching the main API.

## Getting Involved Later

If/when we decide to build this service:

1. Review the latest roadmap in `docs/status.md` to confirm scope.
2. Align with the warehouse expectations in `docs/tech-spec.md` (copy semantics, credential handling).
3. Start by scripting a one-off loader that consumes a single experiment/run; once the flow is reliable, graduate it into a long-running service.
