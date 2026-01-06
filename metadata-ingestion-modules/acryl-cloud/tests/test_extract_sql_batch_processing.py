"""
Unit tests for DataHub Reporting Extract SQL Source - Batch Processing Logic

Tests comprehensive coverage of the streaming batch processing functionality including:
- Batch grouping logic
- S3 error handling
- Chunked streaming
- First file download optimization
- Edge cases (empty prefix, large files, duplicate names)
"""

import io
import os
import tempfile
import zipfile
from typing import List
from unittest.mock import MagicMock, Mock, patch

import pytest
from botocore.exceptions import ClientError

from acryl_datahub_cloud.datahub_reporting.extract_sql import (
    DataHubReportingExtractSQLSource,
    DataHubReportingExtractSQLSourceConfig,
    S3ClientConfig,
)


@pytest.fixture
def mock_s3_client() -> MagicMock:
    """Mock boto3 S3 client"""
    return MagicMock()


@pytest.fixture
def mock_s3_resource() -> MagicMock:
    """Mock boto3 S3 resource"""
    return MagicMock()


@pytest.fixture
def config() -> DataHubReportingExtractSQLSourceConfig:
    """Create test configuration"""
    return DataHubReportingExtractSQLSourceConfig(
        enabled=True,
        sql_backup_config=S3ClientConfig(bucket="test-bucket", path="test-path"),
        # Pass as dict - the validator expects dict format with mode="before"
        extract_sql_store={  # type: ignore[arg-type]
            "dataset_name": "test_dataset",
            "bucket_prefix": "test-path",
            "file": "test.zip",
        },
        batch_size_bytes=1024 * 1024,  # 1MB for testing
    )


@pytest.fixture
def source(
    config: DataHubReportingExtractSQLSourceConfig,
) -> DataHubReportingExtractSQLSource:
    """Create test source instance"""
    ctx = Mock()
    ctx.pipeline_name = "test_pipeline"
    return DataHubReportingExtractSQLSource(config, ctx)


def create_mock_s3_object(key: str, size: int) -> Mock:
    """Helper to create a mock S3 ObjectSummary"""
    obj = Mock()
    obj.key = key
    obj.size = size
    return obj


def create_mock_download_file(content: bytes = b"test_content"):
    """Create a mock download_file function that actually creates the file on disk.

    The first file is downloaded to disk for schema computation and later read
    when adding to the ZIP. This mock simulates that behavior.
    """

    def mock_download_file(bucket: str, key: str, local_path: str) -> None:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(content)

    return mock_download_file


class TestBatchGrouping:
    """Test batch grouping logic"""

    def test_batch_grouping_with_exact_size(
        self,
        source: DataHubReportingExtractSQLSource,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test batch grouping when files exactly fill batch size"""
        # Files: [500KB, 500KB, 300KB, 700KB] with 1MB batch size
        # Expected: [[500KB, 500KB], [300KB, 700KB]]
        objects = [
            create_mock_s3_object("file1.parquet", 500 * 1024),
            create_mock_s3_object("file2.parquet", 500 * 1024),
            create_mock_s3_object("file3.parquet", 300 * 1024),
            create_mock_s3_object("file4.parquet", 700 * 1024),
        ]

        mock_bucket = Mock()
        mock_bucket.objects.filter.return_value = objects
        mock_s3_resource.Bucket.return_value = mock_bucket

        source.s3_client = mock_s3_client

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_dir = os.path.join(tmpdir, "download")
            output_zip = os.path.join(tmpdir, "output.zip")

            # Mock download_file to create the file on disk
            mock_s3_client.download_file = create_mock_download_file(
                b"test_content" * 1000
            )

            # Mock get_object to return chunked data
            def mock_get_object(Bucket: str, Key: str) -> dict:
                response = {
                    "Body": io.BytesIO(b"test_content" * 1000)  # Mock file content
                }
                return response

            mock_s3_client.get_object = mock_get_object

            with patch("boto3.resource", return_value=mock_s3_resource):
                result = source._download_and_zip_in_batches(
                    bucket="test-bucket",
                    prefix="test-prefix",
                    batch_dir=batch_dir,
                    output_zip=output_zip,
                    batch_size_bytes=1024 * 1024,  # 1MB
                )

            assert result is True
            assert os.path.exists(output_zip)

            # Verify ZIP contains all files
            with zipfile.ZipFile(output_zip, "r") as zipf:
                assert len(zipf.namelist()) == 4

    def test_batch_grouping_single_file(
        self,
        source: DataHubReportingExtractSQLSource,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test batch processing with single file"""
        objects = [create_mock_s3_object("file1.parquet", 500 * 1024)]

        mock_bucket = Mock()
        mock_bucket.objects.filter.return_value = objects
        mock_s3_resource.Bucket.return_value = mock_bucket

        source.s3_client = mock_s3_client

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_dir = os.path.join(tmpdir, "download")
            output_zip = os.path.join(tmpdir, "output.zip")

            # Mock download_file to create the file on disk
            mock_s3_client.download_file = create_mock_download_file()

            # Mock get_object
            def mock_get_object(Bucket: str, Key: str) -> dict:
                return {"Body": io.BytesIO(b"test_content")}

            mock_s3_client.get_object = mock_get_object

            with patch("boto3.resource", return_value=mock_s3_resource):
                result = source._download_and_zip_in_batches(
                    bucket="test-bucket",
                    prefix="test-prefix",
                    batch_dir=batch_dir,
                    output_zip=output_zip,
                    batch_size_bytes=1024 * 1024,
                )

            assert result is True
            assert os.path.exists(output_zip)

    def test_batch_grouping_file_larger_than_batch_size(
        self,
        source: DataHubReportingExtractSQLSource,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test batch processing when file is larger than batch size"""
        # Files: [200KB, 2MB, 300KB] with 1MB batch size
        # Expected: [[200KB], [2MB], [300KB]]
        objects = [
            create_mock_s3_object("file1.parquet", 200 * 1024),
            create_mock_s3_object(
                "file2.parquet", 2 * 1024 * 1024
            ),  # Larger than batch size
            create_mock_s3_object("file3.parquet", 300 * 1024),
        ]

        mock_bucket = Mock()
        mock_bucket.objects.filter.return_value = objects
        mock_s3_resource.Bucket.return_value = mock_bucket

        source.s3_client = mock_s3_client

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_dir = os.path.join(tmpdir, "download")
            output_zip = os.path.join(tmpdir, "output.zip")

            # Mock download_file to create the file on disk
            mock_s3_client.download_file = create_mock_download_file(
                b"test_content" * 10000
            )

            # Mock get_object
            def mock_get_object(Bucket: str, Key: str) -> dict:
                return {"Body": io.BytesIO(b"test_content" * 10000)}

            mock_s3_client.get_object = mock_get_object

            with patch("boto3.resource", return_value=mock_s3_resource):
                result = source._download_and_zip_in_batches(
                    bucket="test-bucket",
                    prefix="test-prefix",
                    batch_dir=batch_dir,
                    output_zip=output_zip,
                    batch_size_bytes=1024 * 1024,  # 1MB
                )

            assert result is True
            # Verify all files were processed despite one being oversized
            with zipfile.ZipFile(output_zip, "r") as zipf:
                assert len(zipf.namelist()) == 3


class TestErrorHandling:
    """Test S3 error handling"""

    def test_first_file_download_failure(
        self,
        source: DataHubReportingExtractSQLSource,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test error handling when first file download fails"""
        objects = [create_mock_s3_object("file1.parquet", 500 * 1024)]

        mock_bucket = Mock()
        mock_bucket.objects.filter.return_value = objects
        mock_s3_resource.Bucket.return_value = mock_bucket

        source.s3_client = mock_s3_client

        # Mock download_file to raise ClientError
        mock_s3_client.download_file.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Key not found"}},
            "download_file",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_dir = os.path.join(tmpdir, "download")
            output_zip = os.path.join(tmpdir, "output.zip")

            with (
                patch("boto3.resource", return_value=mock_s3_resource),
                pytest.raises(RuntimeError, match="Cannot compute schema"),
            ):
                source._download_and_zip_in_batches(
                    bucket="test-bucket",
                    prefix="test-prefix",
                    batch_dir=batch_dir,
                    output_zip=output_zip,
                    batch_size_bytes=1024 * 1024,
                )

    def test_s3_get_object_failure_during_streaming(
        self,
        source: DataHubReportingExtractSQLSource,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test error handling when S3 get_object fails during streaming"""
        objects = [
            create_mock_s3_object("file1.parquet", 500 * 1024),
            create_mock_s3_object("file2.parquet", 500 * 1024),
        ]

        mock_bucket = Mock()
        mock_bucket.objects.filter.return_value = objects
        mock_s3_resource.Bucket.return_value = mock_bucket

        source.s3_client = mock_s3_client

        # Mock download_file to create the file on disk
        mock_s3_client.download_file = create_mock_download_file()

        # Mock get_object to fail on second file
        def mock_get_object(Bucket: str, Key: str) -> dict:
            if "file2" in Key:
                raise ClientError(
                    {"Error": {"Code": "NoSuchKey", "Message": "Key not found"}},
                    "get_object",
                )
            return {"Body": io.BytesIO(b"test_content")}

        mock_s3_client.get_object = mock_get_object

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_dir = os.path.join(tmpdir, "download")
            output_zip = os.path.join(tmpdir, "output.zip")

            with (
                patch("boto3.resource", return_value=mock_s3_resource),
                pytest.raises(RuntimeError, match="Failed to stream file"),
            ):
                source._download_and_zip_in_batches(
                    bucket="test-bucket",
                    prefix="test-prefix",
                    batch_dir=batch_dir,
                    output_zip=output_zip,
                    batch_size_bytes=1024 * 1024,
                )


class TestEdgeCases:
    """Test edge cases"""

    def test_empty_s3_prefix_returns_false(
        self, source: DataHubReportingExtractSQLSource, mock_s3_resource: MagicMock
    ) -> None:
        """Test that empty S3 prefix returns False"""
        mock_bucket = Mock()
        mock_bucket.objects.filter.return_value = []  # No objects
        mock_s3_resource.Bucket.return_value = mock_bucket

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_dir = os.path.join(tmpdir, "download")
            output_zip = os.path.join(tmpdir, "output.zip")

            with patch("boto3.resource", return_value=mock_s3_resource):
                result = source._download_and_zip_in_batches(
                    bucket="test-bucket",
                    prefix="test-prefix",
                    batch_dir=batch_dir,
                    output_zip=output_zip,
                    batch_size_bytes=1024 * 1024,
                )

            assert result is False
            assert not os.path.exists(output_zip)

    def test_duplicate_filenames_different_paths(
        self,
        source: DataHubReportingExtractSQLSource,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test that files with same name in different S3 paths are preserved"""
        # Files in different subdirectories with same filename
        objects = [
            create_mock_s3_object("test-prefix/region=us/data.parquet", 500 * 1024),
            create_mock_s3_object("test-prefix/region=eu/data.parquet", 500 * 1024),
        ]

        mock_bucket = Mock()
        mock_bucket.objects.filter.return_value = objects
        mock_s3_resource.Bucket.return_value = mock_bucket

        source.s3_client = mock_s3_client

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_dir = os.path.join(tmpdir, "download")
            output_zip = os.path.join(tmpdir, "output.zip")

            # Mock download_file to create the file on disk
            mock_s3_client.download_file = create_mock_download_file()

            # Mock get_object
            def mock_get_object(Bucket: str, Key: str) -> dict:
                return {"Body": io.BytesIO(b"test_content_unique_" + Key.encode())}

            mock_s3_client.get_object = mock_get_object

            with patch("boto3.resource", return_value=mock_s3_resource):
                result = source._download_and_zip_in_batches(
                    bucket="test-bucket",
                    prefix="test-prefix",
                    batch_dir=batch_dir,
                    output_zip=output_zip,
                    batch_size_bytes=1024 * 1024,
                )

            assert result is True

            # Verify both files are in ZIP with different paths
            with zipfile.ZipFile(output_zip, "r") as zipf:
                filenames = zipf.namelist()
                assert len(filenames) == 2
                # Should preserve subdirectory structure
                assert "region=us/data.parquet" in filenames
                assert "region=eu/data.parquet" in filenames


class TestFirstFileOptimization:
    """Test first file download optimization"""

    def test_first_file_not_downloaded_twice(
        self,
        source: DataHubReportingExtractSQLSource,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test that first file is downloaded once and reused from local disk"""
        objects = [
            create_mock_s3_object("file1.parquet", 500 * 1024),
            create_mock_s3_object("file2.parquet", 500 * 1024),
        ]

        mock_bucket = Mock()
        mock_bucket.objects.filter.return_value = objects
        mock_s3_resource.Bucket.return_value = mock_bucket

        source.s3_client = mock_s3_client

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_dir = os.path.join(tmpdir, "download")
            output_zip = os.path.join(tmpdir, "output.zip")

            # Create a real sample file for the first file
            os.makedirs(batch_dir, exist_ok=True)
            sample_file = os.path.join(batch_dir, "file1.parquet")
            with open(sample_file, "wb") as f:
                f.write(b"sample_content")

            # Mock download_file for first file
            mock_s3_client.download_file = MagicMock()

            # Track get_object calls
            get_object_calls: List[str] = []

            def mock_get_object(Bucket: str, Key: str) -> dict:
                get_object_calls.append(Key)
                return {"Body": io.BytesIO(b"test_content")}

            mock_s3_client.get_object = mock_get_object

            with patch("boto3.resource", return_value=mock_s3_resource):
                result = source._download_and_zip_in_batches(
                    bucket="test-bucket",
                    prefix="test-prefix",
                    batch_dir=batch_dir,
                    output_zip=output_zip,
                    batch_size_bytes=1024 * 1024,
                )

            assert result is True

            # Verify download_file was called once for first file
            assert mock_s3_client.download_file.call_count == 1

            # Verify get_object was only called for file2 (not file1)
            assert len(get_object_calls) == 1
            assert "file2" in get_object_calls[0]


class TestZipModeHandling:
    """Test ZIP mode switching"""

    def test_zip_mode_switches_from_create_to_append(
        self,
        source: DataHubReportingExtractSQLSource,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test that ZIP mode switches from 'x' (create) to 'a' (append) after first batch"""
        # Create multiple batches to test mode switching
        objects = [
            create_mock_s3_object("file1.parquet", 800 * 1024),  # Batch 1
            create_mock_s3_object("file2.parquet", 800 * 1024),  # Batch 2
        ]

        mock_bucket = Mock()
        mock_bucket.objects.filter.return_value = objects
        mock_s3_resource.Bucket.return_value = mock_bucket

        source.s3_client = mock_s3_client

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_dir = os.path.join(tmpdir, "download")
            output_zip = os.path.join(tmpdir, "output.zip")

            # Mock download_file to create the file on disk
            mock_s3_client.download_file = create_mock_download_file(
                b"test_content" * 1000
            )

            # Mock get_object
            def mock_get_object(Bucket: str, Key: str) -> dict:
                return {"Body": io.BytesIO(b"test_content" * 1000)}

            mock_s3_client.get_object = mock_get_object

            with patch("boto3.resource", return_value=mock_s3_resource):
                result = source._download_and_zip_in_batches(
                    bucket="test-bucket",
                    prefix="test-prefix",
                    batch_dir=batch_dir,
                    output_zip=output_zip,
                    batch_size_bytes=1024 * 1024,  # 1MB - forces 2 batches
                )

            assert result is True

            # Verify both files are in the final ZIP
            with zipfile.ZipFile(output_zip, "r") as zipf:
                assert len(zipf.namelist()) == 2
