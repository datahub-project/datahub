from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.fs.fs_base import get_path_schema
from datahub.ingestion.fs.http_fs import HttpFileSystem
from datahub.ingestion.fs.local_fs import LocalFileSystem
from datahub.ingestion.fs.s3_fs import S3FileSystem


def test_local_filesystem_write_and_exists(tmp_path):
    """Test LocalFileSystem write and exists functionality."""
    fs = LocalFileSystem.create()

    test_file = tmp_path / "test_file.txt"
    test_content = "Hello, World!"

    # Test write
    fs.write(str(test_file), test_content)

    # Test exists
    assert fs.exists(str(test_file))
    assert not fs.exists(str(tmp_path / "nonexistent.txt"))

    # Verify content
    with test_file.open() as f:
        assert f.read() == test_content


def test_local_filesystem_write_creates_directories(tmp_path):
    """Test that LocalFileSystem creates parent directories when writing."""
    fs = LocalFileSystem.create()

    nested_file = tmp_path / "nested" / "dir" / "test_file.txt"
    test_content = "Hello, World!"

    # Write to nested path
    fs.write(str(nested_file), test_content)

    # Verify file and directories were created
    assert nested_file.exists()
    assert nested_file.parent.exists()

    # Verify content
    with nested_file.open() as f:
        assert f.read() == test_content


def test_local_filesystem_write_with_kwargs(tmp_path):
    """Test LocalFileSystem write with additional kwargs."""
    fs = LocalFileSystem.create()

    test_file = tmp_path / "test_file.txt"
    test_content = "Hello, World!"

    # Test write with encoding kwarg
    fs.write(str(test_file), test_content, encoding="utf-8")

    # Verify content
    with test_file.open() as f:
        assert f.read() == test_content


@patch("datahub.ingestion.fs.s3_fs.boto3")
def test_s3_filesystem_write(mock_boto3):
    """Test S3FileSystem write functionality."""
    # Mock the S3 client
    mock_s3_client = Mock()
    mock_boto3.client.return_value = mock_s3_client

    # Mock successful put_object response
    mock_s3_client.put_object.return_value = {
        "ResponseMetadata": {"HTTPStatusCode": 200}
    }

    fs = S3FileSystem.create()

    test_path = "s3://test-bucket/path/to/file.txt"
    test_content = "Hello, S3!"

    # Test write
    fs.write(test_path, test_content)

    # Verify S3 client was called correctly
    mock_s3_client.put_object.assert_called_once_with(
        Bucket="test-bucket", Key="path/to/file.txt", Body=test_content.encode("utf-8")
    )


@patch("datahub.ingestion.fs.s3_fs.boto3")
def test_s3_filesystem_write_with_kwargs(mock_boto3):
    """Test S3FileSystem write with additional kwargs."""
    # Mock the S3 client
    mock_s3_client = Mock()
    mock_boto3.client.return_value = mock_s3_client

    # Mock successful put_object response
    mock_s3_client.put_object.return_value = {
        "ResponseMetadata": {"HTTPStatusCode": 200}
    }

    fs = S3FileSystem.create()

    test_path = "s3://test-bucket/path/to/file.txt"
    test_content = "Hello, S3!"

    # Test write with additional kwargs
    fs.write(
        test_path, test_content, ContentType="text/plain", Metadata={"author": "test"}
    )

    # Verify S3 client was called with additional kwargs
    mock_s3_client.put_object.assert_called_once_with(
        Bucket="test-bucket",
        Key="path/to/file.txt",
        Body=test_content.encode("utf-8"),
        ContentType="text/plain",
        Metadata={"author": "test"},
    )


@patch("datahub.ingestion.fs.s3_fs.boto3")
def test_s3_filesystem_exists_true(mock_boto3):
    """Test S3FileSystem exists functionality when file exists."""
    # Mock the S3 client
    mock_s3_client = Mock()
    mock_boto3.client.return_value = mock_s3_client

    # Mock successful head_object response
    mock_s3_client.head_object.return_value = {
        "ResponseMetadata": {"HTTPStatusCode": 200}
    }

    fs = S3FileSystem.create()

    test_path = "s3://test-bucket/path/to/file.txt"

    # Test exists
    assert fs.exists(test_path)

    # Verify S3 client was called correctly
    mock_s3_client.head_object.assert_called_once_with(
        Bucket="test-bucket", Key="path/to/file.txt"
    )


@patch("datahub.ingestion.fs.s3_fs.boto3")
def test_s3_filesystem_exists_false(mock_boto3):
    """Test S3FileSystem exists functionality when file doesn't exist."""
    # Mock the S3 client
    mock_s3_client = Mock()
    mock_boto3.client.return_value = mock_s3_client

    # Mock 404 response with proper structure
    error = Exception("Not found")
    error.response = {"ResponseMetadata": {"HTTPStatusCode": 404}}  # type: ignore
    mock_s3_client.head_object.side_effect = error

    fs = S3FileSystem.create()

    test_path = "s3://test-bucket/path/to/nonexistent.txt"

    # Test exists
    assert not fs.exists(test_path)


@patch("datahub.ingestion.fs.s3_fs.boto3")
def test_s3_filesystem_exists_error(mock_boto3):
    """Test S3FileSystem exists functionality with non-404 errors."""
    # Mock the S3 client
    mock_s3_client = Mock()
    mock_boto3.client.return_value = mock_s3_client

    # Mock access denied response
    error = Exception("Access denied")
    mock_s3_client.head_object.side_effect = error

    fs = S3FileSystem.create()

    test_path = "s3://test-bucket/path/to/file.txt"

    # Test exists - should re-raise non-404 errors
    with pytest.raises(Exception, match="Access denied"):
        fs.exists(test_path)


def test_http_filesystem_write_not_supported():
    """Test that HttpFileSystem write operation raises NotImplementedError."""
    fs = HttpFileSystem.create()

    with pytest.raises(
        NotImplementedError, match="HTTP file system does not support write operations"
    ):
        fs.write("http://example.com/file.txt", "content")


@patch("datahub.ingestion.fs.http_fs.requests")
def test_http_filesystem_exists_true(mock_requests):
    """Test HttpFileSystem exists functionality when resource exists."""
    # Mock successful HEAD response
    mock_response = Mock()
    mock_response.ok = True
    mock_requests.head.return_value = mock_response

    fs = HttpFileSystem.create()

    assert fs.exists("http://example.com/file.txt")
    mock_requests.head.assert_called_once_with("http://example.com/file.txt")


@patch("datahub.ingestion.fs.http_fs.requests")
def test_http_filesystem_exists_false(mock_requests):
    """Test HttpFileSystem exists functionality when resource doesn't exist."""
    # Mock failed HEAD response
    mock_requests.head.side_effect = Exception("Request failed")

    fs = HttpFileSystem.create()

    assert not fs.exists("http://example.com/nonexistent.txt")


def test_get_path_schema():
    """Test path schema detection."""
    assert get_path_schema("s3://bucket/file.txt") == "s3"
    assert get_path_schema("http://example.com/file.txt") == "http"
    assert get_path_schema("https://example.com/file.txt") == "https"
    assert get_path_schema("/local/path/file.txt") == "file"
    assert get_path_schema("relative/path/file.txt") == "file"
