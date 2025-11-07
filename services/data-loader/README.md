# Data Loader (Future Service)

This placeholder will contain the batch/ELT utility responsible for:

- Reading generated Parquet files from the LocalStack S3 staging bucket
- Executing COPY/Snowpipe-style loads into the local Redshift/Snowflake mocks
- Managing load metadata/progress for downstream observability

A dedicated Dockerfile plus orchestration hooks will be added when the service
implementation begins.
