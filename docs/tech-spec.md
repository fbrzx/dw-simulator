The user has requested a local, containerized solution for generating and querying synthetic data that simulates the environment of Amazon Redshift and Snowflake for development and testing purposes.

## Architecture Design Document: Local Data Simulation Environment

### I. Executive Summary

This architecture proposes a comprehensive **Local-First Data Simulation Environment** for developers and data engineers. The solution achieves parity with production data warehouse systems (Redshift and Snowflake) by leveraging Docker-based local emulators. Synthetic data generation is handled by a Python-based utility that adheres to defined schemas, ensuring high-quality, privacy-safe, and realistic data. The entire environment is containerized via Docker Compose, fulfilling the **Local-First with Cloud Scalability** principle and ensuring a consistent developer experience.

| Production Component | Local Simulation Tool | Principle |
| :--- | :--- | :--- |
| Snowflake (Data Warehouse) | LocalStack Snowflake Emulator (Docker) | Local-First, Service Abstraction |
| Amazon Redshift (Data Warehouse) | Containerized PostgreSQL with Redshift Mocking Layer | Local-First, Data Architecture |
| Synthetic Data Generation | Python/SDV Utility (Containerized) | 12-Factor App (Build/Release/Run) |
| Amazon S3 (Data Lake/Staging) | LocalStack S3 Emulator (Docker) | Service Abstraction, Local-First |

---

### II. Architecture Decision Records (ADR)

| ADR ID | Status | Context | Decision | Consequences |
| :--- | :--- | :--- | :--- | :--- |
| **ADR-001** | Decided | A local solution is required to emulate both Redshift and Snowflake data warehouses for local development and testing, without using costly cloud resources. | **Adopt LocalStack (for Snowflake) and a Postgres-based mock (for Redshift).** LocalStack offers a dedicated, high-fidelity Snowflake emulator running in Docker. Redshift, being PostgreSQL-compatible, will use a standard containerized PostgreSQL instance combined with a mocking library to handle Redshift-specific SQL. | This approach provides the highest feature parity while adhering to our **Local-First** and **Data Architecture** mandates (favoring containerized PostgreSQL). A LocalStack license (for the Snowflake/Redshift advanced features) may be required. |
| **ADR-002** | Decided | We need a robust, schema-driven, and open-source solution for generating large volumes of synthetic data that respects statistical properties. | **Adopt Synthetic Data Vault (SDV) Python Library.** SDV is open-source, handles relational integrity (multi-table), and preserves the statistical characteristics of the input schema. The generation utility will be packaged as a stateless Docker container. | SDV requires a schema definition phase (either from sample data or manually defined Pydantic models) before generation. The output will be Parquet/CSV files staged in the local S3 emulator (LocalStack S3). |
| **ADR-003** | Decided | We need a portable and efficient file format for moving data from the generation utility to the data warehouses. | **Standardize on Parquet format for data transfer.** Parquet is a column-oriented format, which is the most efficient and standard way to load data into cloud data warehouses like Redshift and Snowflake. | The synthetic data generation utility must be configured to output compressed Parquet files. LocalStack S3 will be the staging area, simulating a real-world S3 staging bucket. |

---

### III. System Architecture & Design

The entire solution is defined by a single `docker-compose.yml` file for a "single-click" local setup.

#### New: Experiment Deletion & Web UI Control Plane

To satisfy **US 1.2** and accelerate the UI roadmap, the architecture now includes a lightweight HTTP control plane plus a React-based web UI:

1. **Experiment lifecycle service (Python/FastAPI)**  
   * Lives inside the existing `synthetic-data-generator` container.  
   * Exposes REST endpoints that wrap the existing `ExperimentService`:
     * `GET /api/experiments` → list metadata (name, description, table counts).  
     * `POST /api/experiments` → create a new experiment from JSON payloads.  
     * `DELETE /api/experiments/{name}` → invoke the new deletion workflow (metadata rows + physical tables).  
   * The HTTP layer reuses the same SQLAlchemy engine as the CLI and runs under Uvicorn/Gunicorn. Typer continues to serve CLI use cases.
   * All destructive operations execute inside explicit SQL transactions. Dropping the prefixed physical tables and deleting metadata happen atomically; failures roll back and return structured errors (HTTP 409/500).

2. **Deletion workflow (Persistence updates)**  
   * The repository gains `delete_experiment(name: str)` which:
     1. Loads experiment metadata; returns a `NotFound` error if absent.  
     2. Enumerates the physical tables via the `experiment_tables` tracking table (falling back to inspector lookups) and issues `DROP TABLE IF EXISTS` statements.  
     3. Deletes metadata rows from `experiment_tables` + `experiments` inside the same transaction.  
     4. Optionally cleans up staged Parquet prefixes in LocalStack S3 (future enhancement; stub call left in interface for extension).  
   * The method returns the count of tables dropped so the UI/CLI can confirm the outcome.

3. **Web UI (React/Vite, `services/web-ui`)**  
   * Runs as a local-only SPA served via Vite development server (or static build).  
   * Core screens delivered in the first increment:
     * **Experiment List** – fetches `/api/experiments`, shows create/delete controls.
     * **Create Experiment form** – accepts JSON schema text area, posts to `/api/experiments`.
     * **Delete Experiment action** – issues `DELETE /api/experiments/{name}` and refreshes the list.
   * Future iterations layer in query execution + export once the backend exposes the necessary endpoints.
   * UI will reuse a small API client module to keep fetch logic isolated and to facilitate unit testing (Vitest + React Testing Library).

4. **Security & Local-only Guarantees**  
   * The HTTP server binds to `localhost` only, ensuring no external exposure.  
   * CORS is disabled by default; the UI proxies through Vite's dev server to avoid cross-origin issues during development.  
   * Authentication is deferred until multi-user support is required; current scope assumes trusted local operator.

This addition keeps the architecture local-first while enabling richer interaction patterns beyond the CLI, and it ensures the deletion workflow is fully accessible via both automation (CLI) and the forthcoming UI.

#### New: Synthetic Data Generation Pipeline (US 2.1)

With experiment authoring/deletion complete, the next increment introduces an opinionated generation pipeline that converts schemas into synthetic Parquet batches staged in LocalStack S3 and loaded into the local warehouses.

1. **Inputs & Configuration**
   * `ExperimentSchema` remains the source of truth (table definitions, constraints).
   * Users supply a JSON payload (CLI/API/UI) specifying:
     * `experiment_name`
     * Optional per-table overrides for `target_rows` (default: use schema values)
     * Optional seed for deterministic generation (`--seed` flag / request field)
   * Generation jobs run under the same container as the CLI/API, ensuring a single codepath whether invoked via `dw-sim experiment generate`, REST, or future scheduler.

2. **Generation Engine (Python module `dw_simulator.generator`)**
   * Uses Faker for discrete column rules (varchar/email/names) and NumPy/Pandas utilities for numeric/date distributions; SDV integration is deferred until richer statistical requirements arrive.
   * Each table is processed in batches (default 10k rows) to keep memory bounded. The generator yields Pandas DataFrames that are immediately written to compressed Parquet files in `/tmp/dw-sim/<experiment>/<table>/<batch>.parquet`.
   * Constraints/enforcements:
     * `is_unique`: tracked via incremental sets to guarantee no duplicates even across batches.
     * `required=False`: introduces NULLs at a configurable percentage (default 0%).
     * `date_start/date_end`, `min_value/max_value`, and `varchar_length` are enforced before serialization.
   * After each table completes, the parquet files are uploaded to LocalStack S3 (`DW_SIMULATOR_STAGE_BUCKET`) under `s3://.../experiments/<experiment>/<table>/batch-*.parquet`.

3. **Orchestration Flow**
   1. Service receives generate request → validates experiment exists + not currently running (simple status flag in SQLite).
   2. Generator produces staged Parquet files while emitting progress events (table-level counts) that are appended to a `generation_runs` table (metadata store).
   3. Once staging completes, the persistence layer optionally issues `COPY` commands to the local Redshift mock (future step) or simply records the staged object keys so downstream jobs (data-loader service) can ingest them.

4. **API/CLI/UI Surface**
   * CLI: `dw-sim experiment generate <name> [--rows customers=50000] [--seed 123]`.
   * API: `POST /api/experiments/{name}/generate` with JSON overrides; responds with a run-id and summary.
   * UI: Adds a “Generate Data” button per experiment and a modal for specifying row counts/seed; progress is shown via periodic polling of `GET /api/experiments/{name}/runs`.

5. **Failure Handling & Observability**
   * Jobs execute inside `asyncio` tasks with cancellation hooks so CLI/UI can abort long-running generations.
   * Errors (e.g., missing Faker provider, disk issues) are captured and returned to the caller with actionable context; metadata tables store `status=FAILED` plus traceback snippet.
   * A future enhancement will stream stdout to the UI via Server-Sent Events, but initial implementation relies on polling run status.

This design ensures feature parity across CLI/API/UI, keeps generation local-first (no external services), and lays the groundwork for integrating the data-loader service in subsequent stories.

#### Upcoming: SQL-Driven Experiment Import (Redshift & Snowflake)

To reduce friction for analysts who already have DDL files, US 1.3 introduces SQL ingestion and dialect-awareness:

1. **Parser Layer**
   * Adopt `sqlglot` for multi-dialect parsing (Redshift, Snowflake, ANSI fallback).
   * Translate `CREATE TABLE` statements into our `ExperimentSchema`:
     - Map `BIGINT/INTEGER/SMALLINT` → `INT`, `NUMERIC/DECIMAL` → `FLOAT` (with optional `min_value/max_value`), `VARCHAR/NVARCHAR/CHAR` → `VARCHAR` (length inferred), `DATE/TIMESTAMP*` → `DATE` plus optional synthetic time column.
     - Capture PK/unique constraints; composite keys are flattened into a derived surrogate unique column until multi-column uniqueness is supported natively.
   * Reject unsupported constructs (e.g., `IDENTITY`, `EXTERNAL TABLE`, `VARIANT`) with actionable errors that include line/column and dialect-specific guidance.

2. **Dialects & Metadata**
   * Experiments gain a `dialect` field (`redshift`/`snowflake`) stored alongside the schema JSON.
   * Type mapping tables per dialect so, for example, Snowflake `NUMBER(38,0)` becomes `INT` while Redshift `TIMESTAMPTZ` downgrades to `DATE` + warning.

3. **API/CLI Changes**
   * New endpoint: `POST /api/experiments/import-sql` accepting `{ sql: "...", dialect: "redshift" }` (or multipart uploads). The server parses → validates → persists via the existing service.
   * CLI command: `dw-sim experiment import-sql schema.sql --dialect snowflake` producing the normalized JSON (for auditing) before storing it.
   * The existing `create` endpoints remain for raw JSON to preserve backwards compatibility.

4. **UI Enhancements**
   * “Create Experiment” modal offers two tabs: **JSON Schema** and **SQL Import**.
   * SQL tab includes a file picker/text area, dialect dropdown, and inline error reporting sourced from parser diagnostics.
   * After successful import the UI renders the interpreted column list (data type, lengths, unique flags) so users can confirm the translation before running generation jobs.

5. **Testing & Validation**
   * Parser unit tests for representative Redshift/Snowflake constructs + failure cases.
   * Integration tests calling the new API/CLI flows end-to-end, ensuring imported experiments can be created, generated, and deleted like native JSON-defined ones.

By front-loading SQL ingestion and dialect metadata, the simulator stays aligned with real warehouse schemas while keeping the local generator/persistence model simple and portable.

#### Composite Primary Key Handling

Real-world data warehouses frequently employ composite primary keys (e.g., `PRIMARY KEY (region_id, store_id)`). The simulator provides transparent handling and clear user guidance for these schemas:

1. **Surrogate Key Generation**
   * When the SQL parser detects a composite primary key during import, the system automatically prepends a `_row_id` column to the table definition.
   * The `_row_id` column has type `INT` with `is_unique=True`, serving as a single-column surrogate primary key for data generation purposes.
   * Original composite key columns are preserved in the schema and tracked via the `TableSchema.composite_keys` metadata field (e.g., `[["region_id", "store_id"]]`).

2. **Sequential Generation**
   * During synthetic data generation, the `_row_id` column is populated with sequential integers starting from 1 (1, 2, 3, ..., N).
   * Each table maintains an independent sequence, ensuring uniqueness within that table.
   * The original composite key columns are generated normally according to their data types and constraints.

3. **User Communication**
   * The system emits warnings stored in `TableSchema.warnings` explaining the composite key approach:
     > "Table 'sales' has a composite primary key (region_id, store_id). A surrogate key column '_row_id' has been added for generation. Original columns are preserved."
   * Warnings are surfaced through all interfaces:
     - **CLI**: Printed after successful import
     - **API**: Included in `POST /api/experiments/import-sql` response under `warnings` field
     - **UI**: Displayed as dismissible banners immediately after import and as badge/tooltip on experiment cards
   * The `GET /api/experiments` endpoint includes warnings in experiment summaries for persistent visibility.

4. **Schema Metadata Extensions**
   * `TableSchema.composite_keys: list[list[str]]` – Stores the original composite primary key column groups
   * `TableSchema.warnings: list[str]` – Accumulates user-facing guidance messages
   * Pydantic validation ensures composite key references are valid column names and prevents empty groups

5. **Example: Importing Composite Key SQL**

   **Input DDL (Redshift):**
   ```sql
   CREATE TABLE sales (
     region_id INT NOT NULL,
     store_id INT NOT NULL,
     sale_date DATE,
     amount DECIMAL(10,2),
     PRIMARY KEY (region_id, store_id)
   );
   ```

   **Resulting Schema:**
   ```json
   {
     "name": "sales_experiment",
     "tables": [
       {
         "name": "sales",
         "target_rows": 1000,
         "composite_keys": [["region_id", "store_id"]],
         "warnings": [
           "Table 'sales' has a composite primary key (region_id, store_id). A surrogate key column '_row_id' has been added for generation. Original columns are preserved."
         ],
         "columns": [
           {"name": "_row_id", "data_type": "INT", "is_unique": true, "required": true},
           {"name": "region_id", "data_type": "INT", "required": true},
           {"name": "store_id", "data_type": "INT", "required": true},
           {"name": "sale_date", "data_type": "DATE"},
           {"name": "amount", "data_type": "FLOAT"}
         ]
       }
     ]
   }
   ```

   **Generated Data Sample:**
   ```
   _row_id | region_id | store_id | sale_date  | amount
   --------|-----------|----------|------------|---------
   1       | 5         | 101      | 2024-01-15 | 299.99
   2       | 3         | 102      | 2024-01-16 | 450.00
   3       | 5         | 103      | 2024-01-17 | 125.50
   ```

This approach balances practical synthetic data generation (which benefits from single-column unique identifiers) with fidelity to the original schema design, ensuring users understand the transformation and can adapt downstream workflows accordingly.

#### A. Component Breakdown (Microservices/Utilities)

| Component | Type | Core Capability | Dependency (Base Image) | Status |
| :--- | :--- | :--- | :--- | :--- |
| **`synthetic-data-generator`** | Utility/Job | Generates schema-compliant synthetic data (Parquet files). | Python (using SDV/Faker/Pydantic) | ✅ **Implemented** |
| **`local-snowflake-emulator`** | Service (Emulator) | Simulates Snowflake's SQL, connectivity, and data loading features. | LocalStack (with Snowflake feature enabled) | ⚠️ **Running but Unused** |
| **`local-redshift-mock`** | Service (Mock DW) | Provides a PostgreSQL endpoint with Redshift-specific SQL mocking capabilities. | `postgres:15-alpine` (with Redshift mock extensions) | ⚠️ **Running but Unused** |
| **`local-s3-staging`** | Service (Emulator) | Acts as the local data staging area (e.g., for Snowpipe/Redshift `COPY`). | LocalStack (S3 feature) | ⚠️ **Running but Unused** |
| **`data-loader-utility`** | Utility/Job | Containerized Python/dbt job to execute Snowpipe/COPY commands to load data from local S3 to the local DWs. | Python/Data Client SDKs | 🔴 **Not Implemented** |

**Current Implementation Note:**
The current system loads all data into **SQLite** using direct SQLAlchemy inserts from Parquet files. The Redshift/Snowflake emulators and S3 staging are configured in docker-compose.yml but not yet integrated into the data loading pipeline. This is tracked as **US 5.2** in the implementation roadmap.

#### NEW: Data Loader Service Implementation Roadmap (US 5.2)

To achieve the core project goal of querying against actual warehouse emulators, the following implementation is required:

**Phase 1: Dual-Database Architecture**
- **Metadata Database (SQLite):** Continue using SQLite for experiment metadata, schemas, and generation run tracking
- **Data Warehouse (PostgreSQL/Redshift):** Load generated data into the PostgreSQL-based Redshift emulator
- **Connection Management:** Implement `ExperimentPersistence` with two database connections:
  - `metadata_engine` → SQLite (experiments, experiment_tables, generation_runs)
  - `warehouse_engine` → PostgreSQL (actual data tables)

**Phase 2: S3-Based Loading Pipeline**
1. **Upload to S3:** After generation, upload Parquet files to LocalStack S3 (`s3://local/dw-simulator/staging/<experiment>/<table>/`)
2. **Execute COPY Command:** Use PostgreSQL `COPY FROM PROGRAM` or S3-compatible extensions to load from staged files
3. **Metadata Tracking:** Record S3 URIs in generation_runs table for reproducibility

**Phase 3: Query Routing**
- Modify `execute_query()` to run against `warehouse_engine` instead of metadata engine
- Support Redshift-specific SQL syntax (DISTKEY, SORTKEY, window functions)
- Add connection pooling for query performance

**Phase 4: Multi-Warehouse Support (Future)**
- Add `target_warehouse` field to ExperimentSchema (sqlite/redshift/snowflake)
- Implement warehouse-specific adapters (RedshiftAdapter, SnowflakeAdapter)
- Allow per-experiment warehouse selection in CLI/API/UI

**Benefits:**
- ✅ Test Redshift-specific SQL features (DISTKEY, SORTKEY, SUPER data type)
- ✅ Validate query performance characteristics similar to production
- ✅ Enable Snowflake-specific features (VARIANT, semi-structured data)
- ✅ Support multi-warehouse testing workflows

**Technical Considerations:**
- LocalStack Snowflake emulator has limited feature support (may require fallback to PostgreSQL for advanced features)
- PostgreSQL COPY commands require CSV format or custom extensions for Parquet (may need conversion step)
- Connection management complexity increases (need robust error handling for multiple databases)

#### B. Inter-Service Communication

| Communication Type | Source | Target | Protocol/Method | Justification |
| :--- | :--- | :--- | :--- | :--- |
| **Asynchronous** | `synthetic-data-generator` | `local-s3-staging` | **File Staging (Parquet)** | Decouples data generation from data warehouse loading. Standard cloud pattern. |
| **Synchronous** | `data-loader-utility` | `local-snowflake-emulator` / `local-redshift-mock` | **SQL/JDBC/ODBC** | Direct client-to-DB connection for query execution and data loading commands. |
| **Synchronous** | `data-loader-utility` | `local-s3-staging` | **AWS SDK/LocalStack API** | For issuing commands like Snowpipe auto-ingestion or Redshift `COPY` from the S3 endpoint. |

#### C. Technology Stack

*   **Languages & Frameworks:** Python (for the synthetic generation and loading utilities), shell scripting (for setup and orchestration).
*   **Data Generation:** **SDV (Synthetic Data Vault)** and/or **Pydantic** for strict schema definition.
*   **Emulators/Mocks:** **LocalStack** (for Snowflake and S3 emulation), **PostgreSQL (Docker)** with a Redshift-mocking layer (e.g., `docker-pgredshift` or a custom extension) for Redshift.
*   **Orchestration:** **Docker Compose** (to define the Local-First stack).

---

### IV. Data & API Architecture

#### A. Data Storage Tiers (Local vs. Cloud)

| Data Type/Purpose | Cloud (Production) | Local (Development) | Abstraction Layer |
| :--- | :--- | :--- | :--- |
| **Data Warehouse** | Snowflake / Amazon Redshift | LocalStack Snowflake / Containerized PostgreSQL (Mock) | Standard SQL Clients/Connectors |
| **Staging/Data Lake** | Amazon S3 | LocalStack S3 Emulator (Docker) | AWS SDK (Boto3) / Generic S3 API |
| **Metadata/Config** | Kubernetes ConfigMaps / Cloud Secret Manager | Configuration Files (`.env`, `config.yml`) | 12-Factor App Environment Variables |

#### B. Database & Caching Strategy

The core strategy is to ensure application code uses standard, vendor-neutral drivers or, if needed, a thin abstraction layer (Service Abstraction Layer) to connect to the DWs.

*   **Database:** The primary datastore is the simulated Data Warehouse. Code should favor using standard SQL or Data client libraries (e.g., `snowflake-connector-python`, `psycopg2`).
*   **Portability/Vendor Lock-in Mitigation:**
    *   Avoid using highly vendor-specific functions or stored procedures (e.g., Snowflake Streams/Tasks) unless they are specifically being tested and the test case is isolated.
    *   Use a tool like **`dbt`** (Data Build Tool) to define data transformation models. `dbt` is SQL-centric and can be configured to target both Snowflake and Redshift, abstracting much of the difference in SQL dialects and schema management.
    *   The synthetic data schema will be defined using **Pydantic/SDV schema definitions**, which are language-agnostic and ensure a consistent structure regardless of the target DW.

#### C. API Specification (Draft - Synthetic Data Schema Definition)

The "API" for the synthetic data generator is the input schema definition. This demonstrates using a portable, declarative format (like JSON/YAML) to define the data model for generation.

```yaml
schema:
  Customer:
    count: 10000 # Number of rows to generate
    fields:
      customer_id:
        type: serial
        primary_key: true
      first_name:
        type: categorical
        pii: false
        generator: faker.first_name
      last_name:
        type: categorical
        pii: false
        generator: faker.last_name
      email:
        type: unique
        generator: faker.email
      registration_date:
        type: datetime
        distribution: uniform
        range: ["2023-01-01", "2024-12-31"]
  Order:
    count: 50000
    fields:
      order_id:
        type: serial
        primary_key: true
      customer_id:
        type: foreign_key
        references: Customer.customer_id # Enforce referential integrity
        pii: false
      order_total:
        type: numerical
        distribution: gaussian
        mean: 150.00
        stddev: 50.00
```

---

### V. Security Architecture

#### A. Authentication & Authorization

*   **Local Simulation:** Authentication is handled by **static, non-sensitive, mock credentials** (e.g., `USER=mockuser`, `PASSWORD=mockpass`) defined in the `docker-compose.yml` file. These are intentionally non-secret and local-only.
*   **Production:** Authentication uses standard cloud mechanisms:
    *   **Snowflake:** Key-Pair Authentication or OAuth/JWT managed by an Identity Provider.
    *   **Redshift:** IAM-based authentication (Role-Based Access Control - RBAC) via the Redshift Data API.
*   **Mitigation:** The application code must be built with a configuration layer that is capable of switching between the mock credentials (Local) and the secure cloud-native authentication methods (Production) via environment variables.

#### B. Data Protection

*   **Local Development:**
    *   **Synthetic Data Principle:** The data is non-sensitive and intentionally non-PII, mitigating the need for high-level security for the data itself.
    *   **Local Secrets:** Mock credentials are stored in an uncommitted, local-only file (e.g., `.env.local` or directly in `docker-compose.yml`).
*   **Production Secrets Management:**
    *   **Production:** All sensitive credentials (IAM Keys, Snowflake Private Keys, etc.) must be stored in a **Cloud Secret Manager** (e.g., AWS Secrets Manager, GCP Secret Manager, HashiCorp Vault) and injected into the execution environment at runtime as **Environment Variables** (following **12-Factor Principle IV**).
    *   **In-Transit Encryption:** All communication will mandate TLS/SSL (e.g., connections to the local Snowflake/Redshift emulators and the LocalStack S3 endpoint).
    *   **At-Rest Encryption:** The local Docker volumes for the DW mocks will be mounted on the developer's encrypted filesystem, satisfying the at-rest requirement for this local context.

---

### VI. Local-First Implementation Strategy

#### A. Local Stack Mapping

The entire stack is deployed via Docker Compose, facilitating configuration-driven parity between local and cloud.

| Production Component | Cloud Service/Tool | Local Docker Image / Tool | Config Mapping Strategy |
| :--- | :--- | :--- | :--- |
| Snowflake Instance | Snowflake | `localstack/localstack:latest` (with `SERVICES=snowflake,s3`) | Environment variables configure the connection host/port to `snowflake.localhost.localstack.cloud:4566`. |
| Redshift Instance | AWS Redshift | `hearthsim/docker-pgredshift:latest` or `postgres:15-alpine` | Code connects using a PostgreSQL driver. Mocking library (if used) intercepts and translates Redshift-specific SQL. |
| Data Staging | AWS S3 Bucket | `localstack/localstack:latest` (`SERVICES=s3`) | S3 SDK (`boto3`) endpoints are overridden via environment variables to point to `s3.localhost.localstack.cloud:4566`. |

#### B. Configuration Strategy

The configuration hierarchy adheres strictly to the defined priority order: **Defaults -> Config Files -> Env Vars -> Runtime Overrides**. The following simplified example shows how the DW connection URL is managed.

| Configuration File | Variable Name | Value (Example) | Priority/Purpose |
| :--- | :--- | :--- | :--- |
| **`config/default.yml`** | `DATA_WAREHOUSE_HOST` | `127.0.0.1` | **Default:** Common, local value. |
| **`.env.local`** (uncommitted) | `SNOWFLAKE_USER` | `mock_dev_user` | **Local Override:** Non-sensitive local credentials. |
| **`deployment/production.yml`** | `DATA_WAREHOUSE_HOST` | `prod-cluster-01.us-east-1.redshift.aws` | **Environment-Specific:** Production-level DNS/endpoint. |
| **Runtime (Kubernetes)** | `DATA_WAREHOUSE_HOST` | `$(${CLOUD_SECRET_MANAGER_REDSHIFT_HOST})` | **Runtime Override:** Highest priority; retrieved from a Secret Manager on service start. |

---

### VII. Observability Plan

The `synthetic-data-generator` and `data-loader-utility` must be instrumented to provide comprehensive observability.

#### A. Key Metrics & SLOs

| Metric Type | Measurement | SLO/Target | Business Impact |
| :--- | :--- | :--- | :--- |
| **System Reliability** | Local DW Emulator Availability (Uptime) | 99.9% Uptime (for dev/CI pipelines) | Ensures developers are not blocked by local environment failures. |
| **Performance (Generator)** | Data Generation Latency (Time to generate N rows) | P95 < 60 seconds (for a standard test run) | Faster development feedback loop and CI/CD execution. |
| **Data Quality** | Synthetic Data Generation Error Rate (e.g., integrity constraint failures) | < 0.1% of generated records | Confidence in the realism and utility of the synthetic dataset. |
| **DORA Metric** | Deployment Frequency (Local) | Daily/Multiple times per day | Maximizes local iteration and reduces integration risk. |

#### B. Monitoring Components

*   **Logging:** Mandate **Structured Logging (JSON)** for all Python utilities and LocalStack services. Logs should be output to `stdout` (12-Factor App Principle XI) and collected by a local utility (e.g., **Loki/Promtail** in a full dev environment) for analysis.
*   **Metrics:**
    *   **Tools:** **Prometheus** (collector) and **Grafana** (dashboard).
    *   **Application Metrics:** The generation/loading utilities must expose a Prometheus endpoint (`/metrics`) detailing row counts, latency, and error states.
    *   **Infrastructure Metrics:** LocalStack provides metrics for the emulators' usage and health (CPU, Memory).
*   **Tracing:**
    *   **Tools:** **OpenTelemetry SDK** (for instrumenting Python code) feeding to **Jaeger** or **Tempo** (containerized locally).
    *   **Implementation:** Critical operations (e.g., "Generate 10k rows," "Execute Redshift Copy command") must be wrapped in OpenTelemetry Spans to measure and analyze distributed transaction latency across the generator, S3 mock, and DW emulators.
