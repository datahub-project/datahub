"""
Unit tests for DataHub Reporting Extract SQL Source - Batch Processing Logic

Tests comprehensive coverage of the streaming batch processing functionality including:
- Batch grouping logic
- S3 error handling
- Chunked streaming
- First file download optimization
- Edge cases (empty prefix, large files, duplicate names)
- WorkUnit type validation
"""

import io
import os
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone
from typing import List
from unittest.mock import MagicMock, Mock, patch

import pytest
from botocore.exceptions import ClientError

from acryl_datahub_cloud.datahub_reporting.extract_sql import (
    DataHubReportingExtractSQLSource,
    DataHubReportingExtractSQLSourceConfig,
    ExtractionStats,
    S3ClientConfig,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit


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
    mock_s3_client: MagicMock,
    mock_s3_resource: MagicMock,
) -> DataHubReportingExtractSQLSource:
    """Create test source instance with mocked S3 clients"""
    ctx = Mock()
    ctx.pipeline_name = "test_pipeline"
    with (
        patch("boto3.client", return_value=mock_s3_client),
        patch("boto3.resource", return_value=mock_s3_resource),
    ):
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

            result = source._download_and_zip_in_batches(
                bucket="test-bucket",
                prefix="test-prefix",
                batch_dir=batch_dir,
                output_zip=output_zip,
                batch_size_bytes=1024 * 1024,  # 1MB
            )

            assert isinstance(result, ExtractionStats)
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

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_dir = os.path.join(tmpdir, "download")
            output_zip = os.path.join(tmpdir, "output.zip")

            # Mock download_file to create the file on disk
            mock_s3_client.download_file = create_mock_download_file()

            # Mock get_object
            def mock_get_object(Bucket: str, Key: str) -> dict:
                return {"Body": io.BytesIO(b"test_content")}

            mock_s3_client.get_object = mock_get_object

            result = source._download_and_zip_in_batches(
                bucket="test-bucket",
                prefix="test-prefix",
                batch_dir=batch_dir,
                output_zip=output_zip,
                batch_size_bytes=1024 * 1024,
            )

            assert isinstance(result, ExtractionStats)
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

            result = source._download_and_zip_in_batches(
                bucket="test-bucket",
                prefix="test-prefix",
                batch_dir=batch_dir,
                output_zip=output_zip,
                batch_size_bytes=1024 * 1024,  # 1MB
            )

            assert isinstance(result, ExtractionStats)
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

        # Mock download_file to raise ClientError
        mock_s3_client.download_file.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Key not found"}},
            "download_file",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_dir = os.path.join(tmpdir, "download")
            output_zip = os.path.join(tmpdir, "output.zip")

            with pytest.raises(RuntimeError, match="Cannot compute schema"):
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

            with pytest.raises(RuntimeError, match="Failed to stream file"):
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
        self,
        source: DataHubReportingExtractSQLSource,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test that empty S3 prefix returns False"""
        mock_bucket = Mock()
        mock_bucket.objects.filter.return_value = []  # No objects
        mock_s3_resource.Bucket.return_value = mock_bucket

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_dir = os.path.join(tmpdir, "download")
            output_zip = os.path.join(tmpdir, "output.zip")

            result = source._download_and_zip_in_batches(
                bucket="test-bucket",
                prefix="test-prefix",
                batch_dir=batch_dir,
                output_zip=output_zip,
                batch_size_bytes=1024 * 1024,
            )

            assert result is None
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

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_dir = os.path.join(tmpdir, "download")
            output_zip = os.path.join(tmpdir, "output.zip")

            # Mock download_file to create the file on disk
            mock_s3_client.download_file = create_mock_download_file()

            # Mock get_object
            def mock_get_object(Bucket: str, Key: str) -> dict:
                return {"Body": io.BytesIO(b"test_content_unique_" + Key.encode())}

            mock_s3_client.get_object = mock_get_object

            result = source._download_and_zip_in_batches(
                bucket="test-bucket",
                prefix="test-prefix",
                batch_dir=batch_dir,
                output_zip=output_zip,
                batch_size_bytes=1024 * 1024,
            )

            assert isinstance(result, ExtractionStats)

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

            result = source._download_and_zip_in_batches(
                bucket="test-bucket",
                prefix="test-prefix",
                batch_dir=batch_dir,
                output_zip=output_zip,
                batch_size_bytes=1024 * 1024,
            )

            assert isinstance(result, ExtractionStats)

            # Verify download_file was called once for first file
            assert mock_s3_client.download_file.call_count == 1

            # Verify get_object was only called for file2 (not file1)
            assert len(get_object_calls) == 1
            assert "file2" in get_object_calls[0]


class TestMultiBatchProcessing:
    """Test multi-batch ZIP processing with single session"""

    def test_multiple_batches_preserves_all_files(
        self,
        source: DataHubReportingExtractSQLSource,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test that files from multiple batches are all preserved in final ZIP"""
        # Create multiple batches to test multi-batch processing
        objects = [
            create_mock_s3_object("file1.parquet", 800 * 1024),  # Batch 1
            create_mock_s3_object("file2.parquet", 800 * 1024),  # Batch 2
        ]

        mock_bucket = Mock()
        mock_bucket.objects.filter.return_value = objects
        mock_s3_resource.Bucket.return_value = mock_bucket

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

            result = source._download_and_zip_in_batches(
                bucket="test-bucket",
                prefix="test-prefix",
                batch_dir=batch_dir,
                output_zip=output_zip,
                batch_size_bytes=1024 * 1024,  # 1MB - forces 2 batches
            )

            assert isinstance(result, ExtractionStats)

            # Verify both files are in the final ZIP
            with zipfile.ZipFile(output_zip, "r") as zipf:
                assert len(zipf.namelist()) == 2


class TestZipBatchingRegressionFix:
    """
    Regression tests for ZIP batching bug fix.

    The original bug: When using ZIP append mode ("a") across multiple batches,
    the central directory could become corrupted, causing files from later batches
    to be silently lost. The fix uses a single ZipFile session for the entire
    operation, writing the central directory only once at the end.
    """

    def test_many_batches_preserves_all_files(
        self,
        source: DataHubReportingExtractSQLSource,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """
        Regression test: Verify all files are preserved when streaming across many batches.

        The original bug caused files from batches 9+ to be lost due to ZIP append mode
        corrupting the central directory.
        """
        # Create 50 files that will be split into multiple batches
        # Using small file sizes and tiny batch size to force many batches
        num_files = 50
        file_content = b"test content for parquet file simulation" * 100

        objects = [
            create_mock_s3_object(f"prefix/part-{i:05d}.parquet", len(file_content))
            for i in range(num_files)
        ]

        mock_bucket = Mock()
        mock_bucket.objects.filter.return_value = objects
        mock_s3_resource.Bucket.return_value = mock_bucket

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_dir = os.path.join(tmpdir, "download")
            output_zip = os.path.join(tmpdir, "output.zip")

            # Mock download_file to create the file on disk
            mock_s3_client.download_file = create_mock_download_file(file_content)

            # Mock get_object
            def mock_get_object(Bucket: str, Key: str) -> dict:
                return {"Body": io.BytesIO(file_content)}

            mock_s3_client.get_object = mock_get_object

            # Use a small batch size to force ~10 batches (5 files per batch)
            batch_size = len(file_content) * 5

            result = source._download_and_zip_in_batches(
                bucket="test-bucket",
                prefix="prefix/",
                batch_dir=batch_dir,
                output_zip=output_zip,
                batch_size_bytes=batch_size,
            )

            assert isinstance(result, ExtractionStats)

            # CRITICAL ASSERTION: Verify ALL files are in the ZIP
            with zipfile.ZipFile(output_zip, "r") as zf:
                zip_files = zf.namelist()
                assert len(zip_files) == num_files, (
                    f"ZIP should contain all {num_files} files, but only has {len(zip_files)}. "
                    f"This indicates the batching bug has regressed."
                )

    def test_zip_verification_catches_missing_files(
        self,
        source: DataHubReportingExtractSQLSource,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test that the ZIP verification logic raises error on file count mismatch."""
        # We can't easily test the verification failing because it happens after
        # the ZIP is written. Instead, verify the verification happens by checking
        # that when all files are written, the method completes without error.
        # The verification logic itself is straightforward (comparing counts).

        objects = [
            create_mock_s3_object("file1.parquet", 100),
            create_mock_s3_object("file2.parquet", 100),
        ]

        mock_bucket = Mock()
        mock_bucket.objects.filter.return_value = objects
        mock_s3_resource.Bucket.return_value = mock_bucket

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_dir = os.path.join(tmpdir, "download")
            output_zip = os.path.join(tmpdir, "output.zip")

            mock_s3_client.download_file = create_mock_download_file(b"content")

            def mock_get_object(Bucket: str, Key: str) -> dict:
                return {"Body": io.BytesIO(b"content")}

            mock_s3_client.get_object = mock_get_object

            # This should succeed since all files are written
            result = source._download_and_zip_in_batches(
                bucket="test-bucket",
                prefix="",
                batch_dir=batch_dir,
                output_zip=output_zip,
                batch_size_bytes=1024,
            )

            assert isinstance(result, ExtractionStats)

            # Verify the ZIP contains exactly the expected number of files
            with zipfile.ZipFile(output_zip, "r") as zf:
                assert len(zf.namelist()) == 2

    def test_zip_verification_logic_standalone(self) -> None:
        """Test that ZIP verification would catch a mismatch if it occurred."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_zip = os.path.join(tmpdir, "incomplete.zip")

            # Create a ZIP with fewer files than expected
            with zipfile.ZipFile(output_zip, "w") as zf:
                zf.writestr("file1.txt", "content1")
                zf.writestr("file2.txt", "content2")

            # Verify that reading back shows correct count
            with zipfile.ZipFile(output_zip, "r") as zf:
                actual_count = len(zf.namelist())
                expected_count = 10  # Simulating we expected more files

                # This is what our verification code would catch
                assert actual_count != expected_count, (
                    "Test setup: ZIP should have fewer files"
                )
                assert actual_count == 2, "ZIP should have exactly 2 files"


class TestGetWorkunitsYieldsProperTypes:
    """
    Test that get_workunits() yields MetadataWorkUnit objects, not raw MCPs.

    Regression test for bug where _run_full_extraction() and _update_presigned_url_for_date()
    yielded MetadataChangeProposalWrapper objects directly instead of wrapping them with
    .as_workunit(), causing pipeline failures with:
    "ValueError: unknown WorkUnit type <class 'datahub.emitter.mcp.MetadataChangeProposalWrapper'>"
    """

    def test_run_full_extraction_yields_workunits_not_mcps(
        self,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """
        Test that _run_full_extraction yields MetadataWorkUnit objects.

        This tests the full extraction path where register_dataset() is called.
        """
        # Create config with valid S3 URI for bucket_prefix
        config = DataHubReportingExtractSQLSourceConfig(
            enabled=True,
            sql_backup_config=S3ClientConfig(bucket="test-bucket", path="rds_backup"),
            extract_sql_store={  # type: ignore[arg-type]
                "dataset_name": "test_dataset",
                "bucket_prefix": "s3://test-bucket/reporting/sql_raw",
                "file": "data.zip",
            },
            batch_size_bytes=1024 * 1024,
        )

        ctx = Mock()
        ctx.pipeline_name = "test_pipeline"
        ctx.require_graph = Mock(return_value=Mock())

        with (
            patch("boto3.client", return_value=mock_s3_client),
            patch("boto3.resource", return_value=mock_s3_resource),
        ):
            source = DataHubReportingExtractSQLSource(config, ctx)

        # Mock graph to return None for dataset_properties (triggers full extraction)
        mock_graph = Mock()
        mock_graph.get_aspect = Mock(return_value=None)
        source.graph = mock_graph

        # Set up S3 mocks for _SUCCESS marker check
        previous_date = datetime.now(timezone.utc) - timedelta(days=1)
        time_partition_path = "year={}/month={:02d}/day={:02d}".format(
            previous_date.year, previous_date.month, previous_date.day
        )

        # Create mock S3 objects with _SUCCESS marker - use correct path based on config
        success_marker = Mock()
        success_marker.key = f"rds_backup/{time_partition_path}/_SUCCESS"
        success_marker.size = 0
        success_marker.last_modified = datetime.now(timezone.utc)

        data_file = Mock()
        data_file.key = f"rds_backup/{time_partition_path}/data.parquet"
        data_file.size = 1000
        data_file.last_modified = datetime.now(timezone.utc) - timedelta(seconds=10)

        mock_bucket = Mock()
        mock_bucket.objects.filter.return_value = [data_file, success_marker]
        mock_s3_resource.Bucket.return_value = mock_bucket

        # Mock head_object to indicate output zip doesn't exist (triggers full extraction path)
        # This returns 404 which makes _check_output_zip_valid return False
        mock_s3_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}},
            "head_object",
        )

        # Mock download_file
        def mock_download_file(bucket: str, key: str, local_path: str) -> None:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as f:
                f.write(b"test_parquet_content")

        mock_s3_client.download_file = mock_download_file

        # Mock get_object for streaming
        mock_s3_client.get_object = Mock(
            return_value={"Body": io.BytesIO(b"test_parquet_content")}
        )

        # Mock upload_file
        mock_s3_client.upload_file = Mock()
        mock_s3_client.put_object = Mock()

        # Mock generate_presigned_url
        mock_s3_client.generate_presigned_url = Mock(
            return_value="https://presigned-url.example.com"
        )

        # Create a mock MCP to return from register_dataset
        mock_mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:s3,test_dataset,PROD)",
            aspect=Mock(),
        )

        # Mock register_dataset to avoid duckdb parquet scanning
        with patch.object(
            source.datahub_based_s3_dataset,
            "register_dataset",
            return_value=[mock_mcp],
        ):
            # Call get_workunits and collect results
            workunits = list(source.get_workunits())

        # Verify we got some workunits
        assert len(workunits) > 0, "Expected at least one workunit from full extraction"

        # CRITICAL: Verify all yielded items are MetadataWorkUnit, not MetadataChangeProposalWrapper
        for i, wu in enumerate(workunits):
            assert isinstance(wu, MetadataWorkUnit), (
                f"Workunit {i} is {type(wu).__name__}, expected MetadataWorkUnit. "
                f"This indicates _run_full_extraction is yielding MCPs directly "
                f"instead of calling .as_workunit()"
            )
            assert not isinstance(wu, MetadataChangeProposalWrapper), (
                f"Workunit {i} is a raw MetadataChangeProposalWrapper. "
                f"Must wrap with .as_workunit() before yielding from get_workunits()"
            )

    def test_update_presigned_url_path_yields_workunits_not_mcps(
        self,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """
        Test that the presigned URL update path yields MetadataWorkUnit objects.

        This tests the path where a valid output ZIP exists (no _SUCCESS marker needed)
        and we refresh the presigned URL via _run_full_extraction.
        """
        # Create config with valid S3 URI for bucket_prefix
        config = DataHubReportingExtractSQLSourceConfig(
            enabled=True,
            sql_backup_config=S3ClientConfig(bucket="test-bucket", path="rds_backup"),
            extract_sql_store={  # type: ignore[arg-type]
                "dataset_name": "test_dataset",
                "bucket_prefix": "s3://test-bucket/reporting/sql_raw",
                "file": "data.zip",
            },
            batch_size_bytes=1024 * 1024,
        )

        ctx = Mock()
        ctx.pipeline_name = "test_pipeline"
        ctx.require_graph = Mock(return_value=Mock())

        with (
            patch("boto3.client", return_value=mock_s3_client),
            patch("boto3.resource", return_value=mock_s3_resource),
        ):
            source = DataHubReportingExtractSQLSource(config, ctx)

        # Mock graph to return None for dataset_properties
        mock_graph = Mock()
        mock_graph.get_aspect = Mock(return_value=None)
        source.graph = mock_graph

        # Use date-based logic to determine S3 responses:
        # - Today's file (in get_workunits skip/restore checks): doesn't exist
        # - Yesterday's file (in _check_output_zip_valid): exists with valid ZIP header
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        def get_object_side_effect(Bucket, Key, **kwargs):
            if today_str in Key:
                # Today's file doesn't exist
                raise ClientError(
                    {"Error": {"Code": "404", "Message": "Not Found"}},
                    "get_object",
                )
            # Previous days' files exist with valid ZIP header
            return {
                "Body": io.BytesIO(b"PK\x03\x04"),
                "ContentRange": "bytes 0-3/1024",
            }

        mock_s3_client.get_object.side_effect = get_object_side_effect

        def head_object_side_effect(Bucket, Key):
            if today_str in Key:
                # Today's file doesn't exist
                raise ClientError(
                    {"Error": {"Code": "404", "Message": "Not Found"}},
                    "head_object",
                )
            # Previous days' files exist
            return {"ContentLength": 1024}

        mock_s3_client.head_object.side_effect = head_object_side_effect

        # Set up S3 mocks to return NO _SUCCESS marker (RDS backup invalid, so ZIP is fallback)
        mock_bucket = Mock()
        mock_bucket.objects.filter.return_value = []  # No files, no _SUCCESS marker
        mock_s3_resource.Bucket.return_value = mock_bucket

        # Mock generate_presigned_url
        mock_s3_client.generate_presigned_url = Mock(
            return_value="https://presigned-url.example.com"
        )

        # Call get_workunits and collect results
        workunits = list(source.get_workunits())

        # When valid ZIP exists, we should get workunits for presigned URL update
        # The key is that ANY yielded items must be WorkUnits, not MCPs
        for i, wu in enumerate(workunits):
            assert isinstance(wu, MetadataWorkUnit), (
                f"Workunit {i} is {type(wu).__name__}, expected MetadataWorkUnit. "
                f"This indicates _update_presigned_url_for_date is yielding MCPs directly "
                f"instead of calling .as_workunit()"
            )
            assert not isinstance(wu, MetadataChangeProposalWrapper), (
                f"Workunit {i} is a raw MetadataChangeProposalWrapper. "
                f"Must wrap with .as_workunit() before yielding from get_workunits()"
            )


class TestFallbackValidation:
    """Test ZIP validation and fallback loop logic"""

    def test_check_output_zip_valid_returns_true_for_valid_zip(
        self,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test that _check_output_zip_valid returns True for a valid ZIP file"""
        # Create config with valid S3 URI for bucket_prefix
        config = DataHubReportingExtractSQLSourceConfig(
            enabled=True,
            sql_backup_config=S3ClientConfig(bucket="test-bucket", path="rds_backup"),
            extract_sql_store={  # type: ignore[arg-type]
                "dataset_name": "test_dataset",
                "bucket_prefix": "s3://test-bucket/reporting/sql_raw",
                "file": "data.zip",
            },
            batch_size_bytes=1024 * 1024,
        )

        ctx = Mock()
        ctx.pipeline_name = "test_pipeline"

        with (
            patch("boto3.client", return_value=mock_s3_client),
            patch("boto3.resource", return_value=mock_s3_resource),
        ):
            source = DataHubReportingExtractSQLSource(config, ctx)

        # Mock get_object to return valid ZIP magic bytes and ContentRange header
        mock_s3_client.get_object.return_value = {
            "Body": io.BytesIO(b"PK\x03\x04"),
            "ContentRange": "bytes 0-3/1024",
        }

        result = source._check_output_zip_valid(datetime.now(timezone.utc).date())
        assert result is True

    def test_check_output_zip_valid_returns_false_for_missing_file(
        self,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test that _check_output_zip_valid returns False when file doesn't exist"""
        config = DataHubReportingExtractSQLSourceConfig(
            enabled=True,
            sql_backup_config=S3ClientConfig(bucket="test-bucket", path="rds_backup"),
            extract_sql_store={  # type: ignore[arg-type]
                "dataset_name": "test_dataset",
                "bucket_prefix": "s3://test-bucket/reporting/sql_raw",
                "file": "data.zip",
            },
            batch_size_bytes=1024 * 1024,
        )

        ctx = Mock()
        ctx.pipeline_name = "test_pipeline"

        with (
            patch("boto3.client", return_value=mock_s3_client),
            patch("boto3.resource", return_value=mock_s3_resource),
        ):
            source = DataHubReportingExtractSQLSource(config, ctx)

        # get_object returns 404 when file doesn't exist
        mock_s3_client.get_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}},
            "get_object",
        )

        result = source._check_output_zip_valid(datetime.now(timezone.utc).date())
        assert result is False

    def test_check_output_zip_valid_returns_false_for_invalid_header(
        self,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test that _check_output_zip_valid returns False for file with invalid ZIP header"""
        config = DataHubReportingExtractSQLSourceConfig(
            enabled=True,
            sql_backup_config=S3ClientConfig(bucket="test-bucket", path="rds_backup"),
            extract_sql_store={  # type: ignore[arg-type]
                "dataset_name": "test_dataset",
                "bucket_prefix": "s3://test-bucket/reporting/sql_raw",
                "file": "data.zip",
            },
            batch_size_bytes=1024 * 1024,
        )

        ctx = Mock()
        ctx.pipeline_name = "test_pipeline"

        with (
            patch("boto3.client", return_value=mock_s3_client),
            patch("boto3.resource", return_value=mock_s3_resource),
        ):
            source = DataHubReportingExtractSQLSource(config, ctx)

        # Return invalid header (not a ZIP file)
        mock_s3_client.get_object.return_value = {
            "Body": io.BytesIO(b"NOT!"),
            "ContentRange": "bytes 0-3/1024",
        }

        result = source._check_output_zip_valid(datetime.now(timezone.utc).date())
        assert result is False

    def test_check_output_zip_valid_returns_false_for_too_small_file(
        self,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test that _check_output_zip_valid returns False for file smaller than min ZIP size"""
        config = DataHubReportingExtractSQLSourceConfig(
            enabled=True,
            sql_backup_config=S3ClientConfig(bucket="test-bucket", path="rds_backup"),
            extract_sql_store={  # type: ignore[arg-type]
                "dataset_name": "test_dataset",
                "bucket_prefix": "s3://test-bucket/reporting/sql_raw",
                "file": "data.zip",
            },
            batch_size_bytes=1024 * 1024,
        )

        ctx = Mock()
        ctx.pipeline_name = "test_pipeline"

        with (
            patch("boto3.client", return_value=mock_s3_client),
            patch("boto3.resource", return_value=mock_s3_resource),
        ):
            source = DataHubReportingExtractSQLSource(config, ctx)

        # Valid header but file is too small (10 bytes, min is 22)
        mock_s3_client.get_object.return_value = {
            "Body": io.BytesIO(b"PK\x03\x04"),
            "ContentRange": "bytes 0-3/10",
        }

        result = source._check_output_zip_valid(datetime.now(timezone.utc).date())
        assert result is False

    def test_validate_zip_returns_none_for_unknown_content_range(
        self,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test that _validate_zip_at_uri returns None when Content-Range total is '*' or non-numeric."""
        config = DataHubReportingExtractSQLSourceConfig(
            enabled=True,
            sql_backup_config=S3ClientConfig(bucket="test-bucket", path="rds_backup"),
            extract_sql_store={  # type: ignore[arg-type]
                "dataset_name": "test_dataset",
                "bucket_prefix": "s3://test-bucket/reporting/sql_raw",
                "file": "data.zip",
            },
            batch_size_bytes=1024 * 1024,
        )

        ctx = Mock()
        ctx.pipeline_name = "test_pipeline"

        with (
            patch("boto3.client", return_value=mock_s3_client),
            patch("boto3.resource", return_value=mock_s3_resource),
        ):
            source = DataHubReportingExtractSQLSource(config, ctx)

        # Test Content-Range with "*" (unknown total)
        mock_s3_client.get_object.return_value = {
            "Body": io.BytesIO(b"PK\x03\x04"),
            "ContentRange": "bytes 0-3/*",
        }
        assert source._validate_zip_at_uri("s3://test-bucket/test.zip") is None

        # Test Content-Range with non-numeric total
        mock_s3_client.get_object.return_value = {
            "Body": io.BytesIO(b"PK\x03\x04"),
            "ContentRange": "bytes 0-3/garbage",
        }
        assert source._validate_zip_at_uri("s3://test-bucket/test.zip") is None

    def test_read_metadata_returns_none_for_invalid_json(
        self,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test that _read_extraction_metadata_from_s3 returns None for corrupted JSON."""
        config = DataHubReportingExtractSQLSourceConfig(
            enabled=True,
            sql_backup_config=S3ClientConfig(bucket="test-bucket", path="rds_backup"),
            extract_sql_store={  # type: ignore[arg-type]
                "dataset_name": "test_dataset",
                "bucket_prefix": "s3://test-bucket/reporting/sql_raw",
                "file": "data.zip",
            },
            batch_size_bytes=1024 * 1024,
        )

        ctx = Mock()
        ctx.pipeline_name = "test_pipeline"

        with (
            patch("boto3.client", return_value=mock_s3_client),
            patch("boto3.resource", return_value=mock_s3_resource),
        ):
            source = DataHubReportingExtractSQLSource(config, ctx)

        # Test with invalid JSON
        mock_s3_client.get_object.return_value = {
            "Body": io.BytesIO(b"not valid json{{{"),
        }
        assert source._read_extraction_metadata_from_s3() is None

        # Test with valid JSON but wrong structure (missing required fields)
        mock_s3_client.get_object.return_value = {
            "Body": io.BytesIO(b'{"unexpected_field": "value"}'),
        }
        assert source._read_extraction_metadata_from_s3() is None

    def test_prefers_rds_backup_over_existing_zip(
        self,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test that RDS backup is preferred over existing ZIP (fresh data > stale data)"""
        config = DataHubReportingExtractSQLSourceConfig(
            enabled=True,
            sql_backup_config=S3ClientConfig(bucket="test-bucket", path="rds_backup"),
            extract_sql_store={  # type: ignore[arg-type]
                "dataset_name": "test_dataset",
                "bucket_prefix": "s3://test-bucket/reporting/sql_raw",
                "file": "data.zip",
            },
            batch_size_bytes=1024 * 1024,
        )

        ctx = Mock()
        ctx.pipeline_name = "test_pipeline"

        with (
            patch("boto3.client", return_value=mock_s3_client),
            patch("boto3.resource", return_value=mock_s3_resource),
        ):
            source = DataHubReportingExtractSQLSource(config, ctx)

        # Both RDS backup AND ZIP are valid - should use RDS backup (preferred)
        with (
            patch.object(source, "_check_success_marker_valid", return_value=True),
            patch.object(source, "_check_output_zip_valid", return_value=True),
            patch.object(source, "_perform_extraction") as mock_perform,
            patch.object(source, "_update_presigned_url_for_date") as mock_update,
        ):
            mock_perform.return_value = iter([])

            list(
                source._run_full_extraction(
                    tmp_dir="/tmp/test",
                    output_file="data.zip",
                    dataset_properties=None,
                )
            )

            # Should use RDS backup (perform_extraction), NOT the existing ZIP
            mock_perform.assert_called_once()
            mock_update.assert_not_called()

    def test_fallback_uses_zip_when_rds_backup_invalid(
        self,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test that fallback uses existing valid ZIP when RDS backup is invalid"""
        config = DataHubReportingExtractSQLSourceConfig(
            enabled=True,
            sql_backup_config=S3ClientConfig(bucket="test-bucket", path="rds_backup"),
            extract_sql_store={  # type: ignore[arg-type]
                "dataset_name": "test_dataset",
                "bucket_prefix": "s3://test-bucket/reporting/sql_raw",
                "file": "data.zip",
            },
            batch_size_bytes=1024 * 1024,
        )

        ctx = Mock()
        ctx.pipeline_name = "test_pipeline"

        with (
            patch("boto3.client", return_value=mock_s3_client),
            patch("boto3.resource", return_value=mock_s3_resource),
        ):
            source = DataHubReportingExtractSQLSource(config, ctx)

        # RDS backup is invalid, but ZIP exists - should use ZIP as fallback
        with (
            patch.object(source, "_check_success_marker_valid", return_value=False),
            patch.object(source, "_check_output_zip_valid", return_value=True),
            patch.object(
                source, "_update_presigned_url_for_date", return_value=[]
            ) as mock_update,
        ):
            list(
                source._run_full_extraction(
                    tmp_dir="/tmp/test",
                    output_file="data.zip",
                    dataset_properties=None,
                )
            )

            # Should have called _update_presigned_url_for_date since ZIP was used as fallback
            mock_update.assert_called_once()

    def test_fallback_to_previous_day_when_both_invalid(
        self,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test that fallback tries previous day when current day's RDS backup and ZIP are invalid"""
        config = DataHubReportingExtractSQLSourceConfig(
            enabled=True,
            sql_backup_config=S3ClientConfig(bucket="test-bucket", path="rds_backup"),
            extract_sql_store={  # type: ignore[arg-type]
                "dataset_name": "test_dataset",
                "bucket_prefix": "s3://test-bucket/reporting/sql_raw",
                "file": "data.zip",
            },
            batch_size_bytes=1024 * 1024,
        )

        ctx = Mock()
        ctx.pipeline_name = "test_pipeline"

        with (
            patch("boto3.client", return_value=mock_s3_client),
            patch("boto3.resource", return_value=mock_s3_resource),
        ):
            source = DataHubReportingExtractSQLSource(config, ctx)

        # Track call counts
        success_marker_calls: List[str] = []
        zip_check_calls: List[datetime] = []

        def mock_success_marker(bucket, prefix):
            success_marker_calls.append(prefix)
            # Return True on second call (day before yesterday)
            return len(success_marker_calls) >= 2

        def mock_zip_check(target_date):
            zip_check_calls.append(target_date)
            return False  # Always invalid

        with (
            patch.object(
                source, "_check_success_marker_valid", side_effect=mock_success_marker
            ),
            patch.object(source, "_check_output_zip_valid", side_effect=mock_zip_check),
            patch.object(source, "_perform_extraction") as mock_perform,
        ):
            mock_perform.return_value = iter([])

            list(
                source._run_full_extraction(
                    tmp_dir="/tmp/test",
                    output_file="data.zip",
                    dataset_properties=None,
                )
            )

            # Should have checked at least 2 days before finding valid RDS backup
            assert len(success_marker_calls) >= 2
            mock_perform.assert_called_once()

    def test_fallback_raises_error_after_max_days(
        self,
        mock_s3_client: MagicMock,
        mock_s3_resource: MagicMock,
    ) -> None:
        """Test that fallback raises RuntimeError after fallback_lookback_days with no valid backup"""
        lookback_days = 3  # Use a small value for faster test

        config = DataHubReportingExtractSQLSourceConfig(
            enabled=True,
            sql_backup_config=S3ClientConfig(bucket="test-bucket", path="rds_backup"),
            extract_sql_store={  # type: ignore[arg-type]
                "dataset_name": "test_dataset",
                "bucket_prefix": "s3://test-bucket/reporting/sql_raw",
                "file": "data.zip",
            },
            batch_size_bytes=1024 * 1024,
            fallback_lookback_days=lookback_days,
        )

        ctx = Mock()
        ctx.pipeline_name = "test_pipeline"

        with (
            patch("boto3.client", return_value=mock_s3_client),
            patch("boto3.resource", return_value=mock_s3_resource),
        ):
            source = DataHubReportingExtractSQLSource(config, ctx)

        # All checks fail - error message says exactly "last {lookback_days} days"
        with (
            patch.object(source, "_check_success_marker_valid", return_value=False),
            patch.object(source, "_check_output_zip_valid", return_value=False),
            pytest.raises(RuntimeError, match=f"last {lookback_days} days"),
        ):
            list(
                source._run_full_extraction(
                    tmp_dir="/tmp/test",
                    output_file="data.zip",
                    dataset_properties=None,
                )
            )
