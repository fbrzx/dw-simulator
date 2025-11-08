"""Unit tests for S3 client utilities."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from dw_simulator.s3_client import (
    S3UploadError,
    ensure_bucket_exists,
    get_s3_client,
    upload_file_to_s3,
    upload_parquet_files_to_s3,
)


class TestGetS3Client:
    """Tests for get_s3_client()."""

    @patch('dw_simulator.s3_client.boto3.client')
    @patch('dw_simulator.s3_client.get_aws_endpoint_url')
    def test_creates_client_with_localstack_endpoint(
        self, mock_get_endpoint, mock_boto_client
    ):
        """Test that S3 client is created with correct LocalStack configuration."""
        mock_get_endpoint.return_value = 'http://localhost:4566'
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = get_s3_client()

        assert client == mock_s3
        mock_boto_client.assert_called_once_with(
            's3',
            endpoint_url='http://localhost:4566',
            aws_access_key_id='test',
            aws_secret_access_key='test',
            region_name='us-east-1'
        )

    @patch('dw_simulator.s3_client.boto3.client')
    @patch('dw_simulator.s3_client.get_aws_endpoint_url')
    def test_handles_none_endpoint(self, mock_get_endpoint, mock_boto_client):
        """Test that S3 client handles None endpoint (uses real AWS)."""
        mock_get_endpoint.return_value = None
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = get_s3_client()

        assert client == mock_s3
        mock_boto_client.assert_called_once_with(
            's3',
            endpoint_url=None,
            aws_access_key_id='test',
            aws_secret_access_key='test',
            region_name='us-east-1'
        )


class TestEnsureBucketExists:
    """Tests for ensure_bucket_exists()."""

    def test_bucket_already_exists(self):
        """Test that existing bucket is detected and not recreated."""
        mock_s3 = MagicMock()
        mock_s3.head_bucket.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}

        ensure_bucket_exists(mock_s3, 'existing-bucket')

        mock_s3.head_bucket.assert_called_once_with(Bucket='existing-bucket')
        mock_s3.create_bucket.assert_not_called()

    def test_creates_missing_bucket(self):
        """Test that missing bucket is created."""
        mock_s3 = MagicMock()
        # Simulate bucket not found
        error_response = {'Error': {'Code': '404', 'Message': 'Not Found'}}
        mock_s3.head_bucket.side_effect = ClientError(error_response, 'HeadBucket')

        ensure_bucket_exists(mock_s3, 'new-bucket')

        mock_s3.head_bucket.assert_called_once_with(Bucket='new-bucket')
        mock_s3.create_bucket.assert_called_once_with(Bucket='new-bucket')

    def test_raises_on_bucket_creation_failure(self):
        """Test that S3UploadError is raised when bucket creation fails."""
        mock_s3 = MagicMock()
        # Simulate bucket not found
        error_response_404 = {'Error': {'Code': '404', 'Message': 'Not Found'}}
        mock_s3.head_bucket.side_effect = ClientError(error_response_404, 'HeadBucket')
        # Simulate creation failure
        error_response_403 = {'Error': {'Code': '403', 'Message': 'Access Denied'}}
        mock_s3.create_bucket.side_effect = ClientError(error_response_403, 'CreateBucket')

        with pytest.raises(S3UploadError, match="Failed to create bucket 'new-bucket'"):
            ensure_bucket_exists(mock_s3, 'new-bucket')

    def test_raises_on_head_bucket_error(self):
        """Test that S3UploadError is raised when head_bucket fails with non-404 error."""
        mock_s3 = MagicMock()
        # Simulate access denied
        error_response = {'Error': {'Code': '403', 'Message': 'Access Denied'}}
        mock_s3.head_bucket.side_effect = ClientError(error_response, 'HeadBucket')

        with pytest.raises(S3UploadError, match="Failed to check bucket 'test-bucket'"):
            ensure_bucket_exists(mock_s3, 'test-bucket')


class TestUploadFileToS3:
    """Tests for upload_file_to_s3()."""

    def test_raises_when_file_not_found(self, tmp_path):
        """Test that S3UploadError is raised when file doesn't exist."""
        non_existent = tmp_path / "missing.parquet"

        with pytest.raises(S3UploadError, match="File not found"):
            upload_file_to_s3(non_existent, "key/file.parquet")

    @patch('dw_simulator.s3_client.get_s3_client')
    @patch('dw_simulator.s3_client.ensure_bucket_exists')
    @patch('dw_simulator.s3_client.get_stage_bucket')
    def test_uploads_file_successfully(
        self, mock_get_bucket, mock_ensure_bucket, mock_get_client, tmp_path
    ):
        """Test successful file upload."""
        # Create a test file
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"test data")

        mock_s3 = MagicMock()
        mock_get_client.return_value = mock_s3
        mock_get_bucket.return_value = "s3://test-bucket/prefix"

        s3_uri = upload_file_to_s3(
            file_path=test_file,
            s3_key="experiments/test/data.parquet"
        )

        assert s3_uri == "s3://test-bucket/experiments/test/data.parquet"
        mock_ensure_bucket.assert_called_once_with(mock_s3, 'test-bucket')
        mock_s3.upload_file.assert_called_once_with(
            Filename=str(test_file),
            Bucket='test-bucket',
            Key='experiments/test/data.parquet'
        )

    @patch('dw_simulator.s3_client.ensure_bucket_exists')
    def test_uses_provided_s3_client(self, mock_ensure_bucket, tmp_path):
        """Test that provided S3 client is used instead of creating new one."""
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"test data")

        mock_s3 = MagicMock()

        s3_uri = upload_file_to_s3(
            file_path=test_file,
            s3_key="test/key.parquet",
            bucket="custom-bucket",
            s3_client=mock_s3
        )

        assert s3_uri == "s3://custom-bucket/test/key.parquet"
        mock_s3.upload_file.assert_called_once()

    @patch('dw_simulator.s3_client.get_s3_client')
    @patch('dw_simulator.s3_client.ensure_bucket_exists')
    @patch('dw_simulator.s3_client.get_stage_bucket')
    def test_raises_on_upload_failure(
        self, mock_get_bucket, mock_ensure_bucket, mock_get_client, tmp_path
    ):
        """Test that S3UploadError is raised when upload fails."""
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"test data")

        mock_s3 = MagicMock()
        mock_get_client.return_value = mock_s3
        mock_get_bucket.return_value = "s3://test-bucket/prefix"

        # Simulate upload failure
        error_response = {'Error': {'Code': '500', 'Message': 'Internal Server Error'}}
        mock_s3.upload_file.side_effect = ClientError(error_response, 'PutObject')

        with pytest.raises(S3UploadError, match="Failed to upload"):
            upload_file_to_s3(test_file, "test/key.parquet")


class TestUploadParquetFilesToS3:
    """Tests for upload_parquet_files_to_s3()."""

    def test_raises_when_no_files_provided(self):
        """Test that S3UploadError is raised when file list is empty."""
        with pytest.raises(S3UploadError, match="No Parquet files provided"):
            upload_parquet_files_to_s3([], "experiment", "table")

    @patch('dw_simulator.s3_client.upload_file_to_s3')
    @patch('dw_simulator.s3_client.get_s3_client')
    def test_uploads_multiple_files_with_run_id(
        self, mock_get_client, mock_upload_file, tmp_path
    ):
        """Test uploading multiple files with run ID."""
        mock_s3 = MagicMock()
        mock_get_client.return_value = mock_s3

        # Create test files
        file1 = tmp_path / "batch_001.parquet"
        file2 = tmp_path / "batch_002.parquet"
        file1.write_bytes(b"data1")
        file2.write_bytes(b"data2")

        # Mock upload_file_to_s3 to return S3 URIs
        mock_upload_file.side_effect = [
            "s3://bucket/experiments/exp1/customers/run_42/batch_001.parquet",
            "s3://bucket/experiments/exp1/customers/run_42/batch_002.parquet"
        ]

        s3_uris = upload_parquet_files_to_s3(
            parquet_files=[file1, file2],
            experiment_name="exp1",
            table_name="customers",
            run_id=42
        )

        assert len(s3_uris) == 2
        assert s3_uris[0] == "s3://bucket/experiments/exp1/customers/run_42/batch_001.parquet"
        assert s3_uris[1] == "s3://bucket/experiments/exp1/customers/run_42/batch_002.parquet"

        # Verify upload_file_to_s3 was called with correct parameters
        assert mock_upload_file.call_count == 2
        mock_upload_file.assert_any_call(
            file_path=file1,
            s3_key="experiments/exp1/customers/run_42/batch_001.parquet",
            s3_client=mock_s3
        )

    @patch('dw_simulator.s3_client.upload_file_to_s3')
    @patch('dw_simulator.s3_client.get_s3_client')
    def test_uploads_files_without_run_id_uses_latest(
        self, mock_get_client, mock_upload_file, tmp_path
    ):
        """Test uploading files without run ID uses 'latest' suffix."""
        mock_s3 = MagicMock()
        mock_get_client.return_value = mock_s3

        file1 = tmp_path / "data.parquet"
        file1.write_bytes(b"data")

        mock_upload_file.return_value = "s3://bucket/experiments/exp1/table/latest/data.parquet"

        s3_uris = upload_parquet_files_to_s3(
            parquet_files=[file1],
            experiment_name="exp1",
            table_name="table",
            run_id=None
        )

        assert len(s3_uris) == 1
        mock_upload_file.assert_called_once_with(
            file_path=file1,
            s3_key="experiments/exp1/table/latest/data.parquet",
            s3_client=mock_s3
        )

    @patch('dw_simulator.s3_client.upload_file_to_s3')
    def test_uses_provided_s3_client(self, mock_upload_file, tmp_path):
        """Test that provided S3 client is used."""
        mock_s3 = MagicMock()

        file1 = tmp_path / "data.parquet"
        file1.write_bytes(b"data")

        mock_upload_file.return_value = "s3://bucket/key"

        upload_parquet_files_to_s3(
            parquet_files=[file1],
            experiment_name="exp",
            table_name="table",
            s3_client=mock_s3
        )

        # Should use provided client instead of creating new one
        mock_upload_file.assert_called_once()
        assert mock_upload_file.call_args[1]['s3_client'] == mock_s3
