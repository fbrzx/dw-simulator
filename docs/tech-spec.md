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

#### A. Component Breakdown (Microservices/Utilities)

| Component | Type | Core Capability | Dependency (Base Image) |
| :--- | :--- | :--- | :--- |
| **`synthetic-data-generator`** | Utility/Job | Generates schema-compliant synthetic data (Parquet files). | Python (using SDV/Faker/Pydantic) |
| **`local-snowflake-emulator`** | Service (Emulator) | Simulates Snowflake's SQL, connectivity, and data loading features. | LocalStack (with Snowflake feature enabled) |
| **`local-redshift-mock`** | Service (Mock DW) | Provides a PostgreSQL endpoint with Redshift-specific SQL mocking capabilities. | `postgres:15-alpine` (with Redshift mock extensions) |
| **`local-s3-staging`** | Service (Emulator) | Acts as the local data staging area (e.g., for Snowpipe/Redshift `COPY`). | LocalStack (S3 feature) |
| **`data-loader-utility`** | Utility/Job | Containerized Python/dbt job to execute Snowpipe/COPY commands to load data from local S3 to the local DWs. | Python/Data Client SDKs |

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