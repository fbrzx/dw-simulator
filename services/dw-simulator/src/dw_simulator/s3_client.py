"""S3 client utilities for uploading Parquet files to LocalStack S3."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from .config import get_aws_endpoint_url, get_stage_bucket

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)


class S3UploadError(RuntimeError):
    """Raised when S3 upload operations fail."""


def get_s3_client() -> S3Client:
    """
    Create and configure an S3 client for LocalStack.

    Uses AWS_ENDPOINT_URL from config to connect to LocalStack S3.
    """
    endpoint_url = get_aws_endpoint_url()

    # For LocalStack, we use dummy credentials
    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )


def ensure_bucket_exists(s3_client: S3Client, bucket_name: str) -> None:
    """
    Ensure the S3 bucket exists, creating it if necessary.

    Args:
        s3_client: Boto3 S3 client
        bucket_name: Name of the bucket to create

    Raises:
        S3UploadError: If bucket creation fails
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.debug(f"Bucket '{bucket_name}' already exists")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code == '404':
            # Bucket doesn't exist, create it
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                logger.info(f"Created S3 bucket '{bucket_name}'")
            except ClientError as create_error:
                raise S3UploadError(
                    f"Failed to create bucket '{bucket_name}': {create_error}"
                ) from create_error
        else:
            raise S3UploadError(
                f"Failed to check bucket '{bucket_name}': {e}"
            ) from e


def upload_file_to_s3(
    file_path: str | Path,
    s3_key: str,
    bucket: str | None = None,
    s3_client: S3Client | None = None
) -> str:
    """
    Upload a file to S3 and return the S3 URI.

    Args:
        file_path: Local path to the file to upload
        s3_key: S3 object key (path within the bucket)
        bucket: S3 bucket name (defaults to DW_SIMULATOR_STAGE_BUCKET from config)
        s3_client: Boto3 S3 client (creates one if not provided)

    Returns:
        S3 URI in the format s3://bucket/key

    Raises:
        S3UploadError: If the file doesn't exist or upload fails
    """
    path = Path(file_path)
    if not path.exists():
        raise S3UploadError(f"File not found: {file_path}")

    # Use default bucket if not specified
    if bucket is None:
        stage_bucket = get_stage_bucket()
        # Parse s3://bucket/prefix format
        parsed = urlparse(stage_bucket)
        bucket = parsed.netloc

    # Create S3 client if not provided
    if s3_client is None:
        s3_client = get_s3_client()

    # Ensure bucket exists
    ensure_bucket_exists(s3_client, bucket)

    # Upload the file
    try:
        s3_client.upload_file(
            Filename=str(path),
            Bucket=bucket,
            Key=s3_key
        )
        s3_uri = f"s3://{bucket}/{s3_key}"
        logger.info(f"Uploaded {file_path} to {s3_uri}")
        return s3_uri
    except ClientError as e:
        raise S3UploadError(
            f"Failed to upload {file_path} to s3://{bucket}/{s3_key}: {e}"
        ) from e


def upload_parquet_files_to_s3(
    parquet_files: list[str | Path],
    experiment_name: str,
    table_name: str,
    run_id: int | None = None,
    s3_client: S3Client | None = None
) -> list[str]:
    """
    Upload a batch of Parquet files to S3 for a specific experiment and table.

    Creates a structured S3 path: experiments/{experiment_name}/{table_name}/run_{run_id}/

    Args:
        parquet_files: List of local Parquet file paths
        experiment_name: Name of the experiment
        table_name: Name of the table
        run_id: Optional generation run ID (defaults to 'latest')
        s3_client: Boto3 S3 client (creates one if not provided)

    Returns:
        List of S3 URIs for the uploaded files

    Raises:
        S3UploadError: If any upload fails
    """
    if not parquet_files:
        raise S3UploadError("No Parquet files provided for upload")

    # Create S3 client if not provided
    if s3_client is None:
        s3_client = get_s3_client()

    # Build S3 prefix
    run_suffix = f"run_{run_id}" if run_id is not None else "latest"
    s3_prefix = f"experiments/{experiment_name}/{table_name}/{run_suffix}"

    # Upload each file
    s3_uris = []
    for file_path in parquet_files:
        path = Path(file_path)
        s3_key = f"{s3_prefix}/{path.name}"
        s3_uri = upload_file_to_s3(
            file_path=path,
            s3_key=s3_key,
            s3_client=s3_client
        )
        s3_uris.append(s3_uri)

    logger.info(f"Uploaded {len(s3_uris)} Parquet files to S3 for {experiment_name}.{table_name}")
    return s3_uris


__all__ = [
    "S3UploadError",
    "get_s3_client",
    "ensure_bucket_exists",
    "upload_file_to_s3",
    "upload_parquet_files_to_s3",
]
