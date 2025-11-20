import pathlib
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.data_lake_common.object_store import (
    ABSObjectStore,
    GCSObjectStore,
    ObjectStoreSourceAdapter,
    S3ObjectStore,
    create_object_store_adapter,
    get_object_key,
    get_object_store_bucket_name,
    get_object_store_for_uri,
)


class TestS3ObjectStore:
    """Tests for the S3ObjectStore implementation."""

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("s3://bucket/path", True),
            ("s3n://bucket/path", True),
            ("s3a://bucket/path", True),
            ("gs://bucket/path", False),
            ("abfss://container@account.dfs.core.windows.net/path", False),
            ("https://account.blob.core.windows.net/container/path", False),
            ("file:///path/to/file", False),
        ],
    )
    def test_is_uri(self, uri, expected):
        """Test the is_uri method with various URIs."""
        assert S3ObjectStore.is_uri(uri) == expected

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("s3://bucket/path", "s3://"),
            ("s3n://bucket/path", "s3n://"),
            ("s3a://bucket/path", "s3a://"),
            ("gs://bucket/path", None),
        ],
    )
    def test_get_prefix(self, uri, expected):
        """Test the get_prefix method."""
        assert S3ObjectStore.get_prefix(uri) == expected

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("s3://bucket/path", "bucket/path"),
            ("s3n://bucket/path", "bucket/path"),
            ("s3a://bucket/path", "bucket/path"),
        ],
    )
    def test_strip_prefix(self, uri, expected):
        """Test the strip_prefix method."""
        assert S3ObjectStore.strip_prefix(uri) == expected

    def test_strip_prefix_invalid_uri(self):
        """Test strip_prefix with invalid URI."""
        with pytest.raises(ValueError):
            S3ObjectStore.strip_prefix("gs://bucket/path")

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("s3://bucket/path", "bucket"),
            ("s3n://my-bucket/path/to/file", "my-bucket"),
            ("s3a://bucket.name/file.txt", "bucket.name"),
        ],
    )
    def test_get_bucket_name(self, uri, expected):
        """Test the get_bucket_name method."""
        assert S3ObjectStore.get_bucket_name(uri) == expected

    def test_get_bucket_name_invalid_uri(self):
        """Test get_bucket_name with invalid URI."""
        with pytest.raises(ValueError):
            S3ObjectStore.get_bucket_name("gs://bucket/path")

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("s3://bucket/path", "path"),
            ("s3://bucket/path/to/file.txt", "path/to/file.txt"),
            ("s3://bucket/", ""),
            ("s3://bucket", ""),
        ],
    )
    def test_get_object_key(self, uri, expected):
        """Test the get_object_key method."""
        assert S3ObjectStore.get_object_key(uri) == expected

    def test_get_object_key_invalid_uri(self):
        """Test get_object_key with invalid URI."""
        with pytest.raises(ValueError):
            S3ObjectStore.get_object_key("gs://bucket/path")


class TestGCSObjectStore:
    """Tests for the GCSObjectStore implementation."""

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("gs://bucket/path", True),
            ("s3://bucket/path", False),
            ("abfss://container@account.dfs.core.windows.net/path", False),
            ("https://account.blob.core.windows.net/container/path", False),
        ],
    )
    def test_is_uri(self, uri, expected):
        """Test the is_uri method with various URIs."""
        assert GCSObjectStore.is_uri(uri) == expected

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("gs://bucket/path", "gs://"),
            ("s3://bucket/path", None),
        ],
    )
    def test_get_prefix(self, uri, expected):
        """Test the get_prefix method."""
        assert GCSObjectStore.get_prefix(uri) == expected

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("gs://bucket/path", "bucket/path"),
        ],
    )
    def test_strip_prefix(self, uri, expected):
        """Test the strip_prefix method."""
        assert GCSObjectStore.strip_prefix(uri) == expected

    def test_strip_prefix_invalid_uri(self):
        """Test strip_prefix with invalid URI."""
        with pytest.raises(ValueError):
            GCSObjectStore.strip_prefix("s3://bucket/path")

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("gs://bucket/path", "bucket"),
            ("gs://my-bucket/path/to/file", "my-bucket"),
        ],
    )
    def test_get_bucket_name(self, uri, expected):
        """Test the get_bucket_name method."""
        assert GCSObjectStore.get_bucket_name(uri) == expected

    def test_get_bucket_name_invalid_uri(self):
        """Test get_bucket_name with invalid URI."""
        with pytest.raises(ValueError):
            GCSObjectStore.get_bucket_name("s3://bucket/path")

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("gs://bucket/path", "path"),
            ("gs://bucket/path/to/file.txt", "path/to/file.txt"),
            ("gs://bucket/", ""),
            ("gs://bucket", ""),
        ],
    )
    def test_get_object_key(self, uri, expected):
        """Test the get_object_key method."""
        assert GCSObjectStore.get_object_key(uri) == expected

    def test_get_object_key_invalid_uri(self):
        """Test get_object_key with invalid URI."""
        with pytest.raises(ValueError):
            GCSObjectStore.get_object_key("s3://bucket/path")


class TestABSObjectStore:
    """Tests for the ABSObjectStore implementation."""

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("abfss://container@account.dfs.core.windows.net/path", True),
            ("https://account.blob.core.windows.net/container/path", True),
            (
                "https://odedmdatacatalog.blob.core.windows.net/settler/import_export_services/message_data_randomized.csv",
                True,
            ),
            ("s3://bucket/path", False),
            ("gs://bucket/path", False),
            ("https://example.com/path", False),
        ],
    )
    def test_is_uri(self, uri, expected):
        """Test the is_uri method with various URIs."""
        assert ABSObjectStore.is_uri(uri) == expected

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("abfss://container@account.dfs.core.windows.net/path", "abfss://"),
            (
                "https://account.blob.core.windows.net/container/path",
                "https://account.blob.core.windows.net/",
            ),
            (
                "https://odedmdatacatalog.blob.core.windows.net/settler/import_export_services/message_data_randomized.csv",
                "https://odedmdatacatalog.blob.core.windows.net/",
            ),
            ("s3://bucket/path", None),
            ("https://example.com/path", None),
        ],
    )
    def test_get_prefix(self, uri, expected):
        """Test the get_prefix method."""
        assert ABSObjectStore.get_prefix(uri) == expected

    @pytest.mark.parametrize(
        "uri,expected",
        [
            (
                "abfss://container@account.dfs.core.windows.net/path",
                "container@account.dfs.core.windows.net/path",
            ),
            ("https://account.blob.core.windows.net/container/path", "container/path"),
            (
                "https://odedmdatacatalog.blob.core.windows.net/settler/import_export_services/message_data_randomized.csv",
                "settler/import_export_services/message_data_randomized.csv",
            ),
        ],
    )
    def test_strip_prefix(self, uri, expected):
        """Test the strip_prefix method."""
        assert ABSObjectStore.strip_prefix(uri) == expected

    def test_strip_prefix_invalid_uri(self):
        """Test strip_prefix with invalid URI."""
        with pytest.raises(ValueError):
            ABSObjectStore.strip_prefix("s3://bucket/path")

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("abfss://container@account.dfs.core.windows.net/path", "container"),
            ("https://account.blob.core.windows.net/container/path", "container"),
            (
                "https://odedmdatacatalog.blob.core.windows.net/settler/import_export_services/message_data_randomized.csv",
                "settler",
            ),
        ],
    )
    def test_get_bucket_name(self, uri, expected):
        """Test the get_bucket_name method."""
        assert ABSObjectStore.get_bucket_name(uri) == expected

    def test_get_bucket_name_invalid_uri(self):
        """Test get_bucket_name with invalid URI."""
        with pytest.raises(ValueError):
            ABSObjectStore.get_bucket_name("s3://bucket/path")

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("abfss://container@account.dfs.core.windows.net/path", "path"),
            (
                "abfss://container@account.dfs.core.windows.net/path/to/file.txt",
                "path/to/file.txt",
            ),
            ("https://account.blob.core.windows.net/container/path", "path"),
            (
                "https://account.blob.core.windows.net/container/path/to/file.txt",
                "path/to/file.txt",
            ),
            (
                "https://odedmdatacatalog.blob.core.windows.net/settler/import_export_services/message_data_randomized.csv",
                "import_export_services/message_data_randomized.csv",
            ),
        ],
    )
    def test_get_object_key(self, uri, expected):
        """Test the get_object_key method."""
        assert ABSObjectStore.get_object_key(uri) == expected

    def test_get_object_key_invalid_uri(self):
        """Test get_object_key with invalid URI."""
        with pytest.raises(ValueError):
            ABSObjectStore.get_object_key("s3://bucket/path")


class TestUtilityFunctions:
    """Tests for the utility functions in object_store module."""

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("s3://bucket/path", S3ObjectStore),
            ("gs://bucket/path", GCSObjectStore),
            ("abfss://container@account.dfs.core.windows.net/path", ABSObjectStore),
            ("https://account.blob.core.windows.net/container/path", ABSObjectStore),
            (
                "https://odedmdatacatalog.blob.core.windows.net/settler/import_export_services/message_data_randomized.csv",
                ABSObjectStore,
            ),
            ("file:///path/to/file", None),
        ],
    )
    def test_get_object_store_for_uri(self, uri, expected):
        """Test the get_object_store_for_uri function."""
        assert get_object_store_for_uri(uri) == expected

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("s3://bucket/path", "bucket"),
            ("gs://bucket/path", "bucket"),
            ("abfss://container@account.dfs.core.windows.net/path", "container"),
            ("https://account.blob.core.windows.net/container/path", "container"),
            (
                "https://odedmdatacatalog.blob.core.windows.net/settler/import_export_services/message_data_randomized.csv",
                "settler",
            ),
        ],
    )
    def test_get_object_store_bucket_name(self, uri, expected):
        """Test the get_object_store_bucket_name function."""
        assert get_object_store_bucket_name(uri) == expected

    def test_get_object_store_bucket_name_invalid_uri(self):
        """Test get_object_store_bucket_name with unsupported URI."""
        with pytest.raises(ValueError):
            get_object_store_bucket_name("file:///path/to/file")

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("s3://bucket/path", "path"),
            ("gs://bucket/path/to/file.txt", "path/to/file.txt"),
            ("abfss://container@account.dfs.core.windows.net/path", "path"),
            ("https://account.blob.core.windows.net/container/path", "path"),
            (
                "https://odedmdatacatalog.blob.core.windows.net/settler/import_export_services/message_data_randomized.csv",
                "import_export_services/message_data_randomized.csv",
            ),
        ],
    )
    def test_get_object_key(self, uri, expected):
        """Test the get_object_key function."""
        assert get_object_key(uri) == expected

    def test_get_object_key_invalid_uri(self):
        """Test get_object_key with unsupported URI."""
        with pytest.raises(ValueError):
            get_object_key("file:///path/to/file")


class TestObjectStoreSourceAdapter:
    """Tests for the ObjectStoreSourceAdapter class."""

    @pytest.mark.parametrize(
        "bucket,key,expected",
        [
            ("bucket", "path/to/file.txt", "s3://bucket/path/to/file.txt"),
            ("my-bucket", "file.json", "s3://my-bucket/file.json"),
        ],
    )
    def test_create_s3_path(self, bucket, key, expected):
        """Test the create_s3_path static method."""
        assert ObjectStoreSourceAdapter.create_s3_path(bucket, key) == expected

    @pytest.mark.parametrize(
        "bucket,key,expected",
        [
            ("bucket", "path/to/file.txt", "gs://bucket/path/to/file.txt"),
            ("my-bucket", "file.json", "gs://my-bucket/file.json"),
        ],
    )
    def test_create_gcs_path(self, bucket, key, expected):
        """Test the create_gcs_path static method."""
        assert ObjectStoreSourceAdapter.create_gcs_path(bucket, key) == expected

    @pytest.mark.parametrize(
        "container,key,account,expected",
        [
            (
                "container",
                "path/to/file.txt",
                "storage",
                "abfss://container@storage.dfs.core.windows.net/path/to/file.txt",
            ),
            (
                "data",
                "file.json",
                "myaccount",
                "abfss://data@myaccount.dfs.core.windows.net/file.json",
            ),
        ],
    )
    def test_create_abs_path(self, container, key, account, expected):
        """Test the create_abs_path static method."""
        assert (
            ObjectStoreSourceAdapter.create_abs_path(container, key, account)
            == expected
        )

    @pytest.mark.parametrize(
        "table_path,region,expected",
        [
            (
                "s3://bucket/path/to/file.txt",
                None,
                "https://us-east-1.console.aws.amazon.com/s3/buckets/bucket?prefix=path/to/file.txt",
            ),
            (
                "s3://bucket/path/to/file.txt",
                "us-west-2",
                "https://us-west-2.console.aws.amazon.com/s3/buckets/bucket?prefix=path/to/file.txt",
            ),
            ("gs://bucket/path", None, None),
        ],
    )
    def test_get_s3_external_url(self, table_path, region, expected):
        """Test the get_s3_external_url static method."""
        mock_table_data = MagicMock()
        mock_table_data.table_path = table_path
        assert (
            ObjectStoreSourceAdapter.get_s3_external_url(mock_table_data, region)
            == expected
        )

    @pytest.mark.parametrize(
        "table_path,expected",
        [
            (
                "gs://bucket/path/to/file.txt",
                "https://console.cloud.google.com/storage/browser/bucket/path/to/file.txt",
            ),
            ("s3://bucket/path", None),
        ],
    )
    def test_get_gcs_external_url(self, table_path, expected):
        """Test the get_gcs_external_url static method."""
        mock_table_data = MagicMock()
        mock_table_data.table_path = table_path
        assert (
            ObjectStoreSourceAdapter.get_gcs_external_url(mock_table_data) == expected
        )

    @pytest.mark.parametrize(
        "table_path,expected",
        [
            (
                "abfss://container@account.dfs.core.windows.net/path/to/file.txt",
                "https://portal.azure.com/#blade/Microsoft_Azure_Storage/ContainerMenuBlade/overview/storageAccountId/account/containerName/container",
            ),
            (
                "https://account.blob.core.windows.net/container/path/to/file.txt",
                "https://portal.azure.com/#blade/Microsoft_Azure_Storage/ContainerMenuBlade/overview/storageAccountId/account/containerName/container",
            ),
            (
                "https://odedmdatacatalog.blob.core.windows.net/settler/import_export_services/message_data_randomized.csv",
                "https://portal.azure.com/#blade/Microsoft_Azure_Storage/ContainerMenuBlade/overview/storageAccountId/odedmdatacatalog/containerName/settler",
            ),
            ("s3://bucket/path", None),
        ],
    )
    def test_get_abs_external_url(self, table_path, expected):
        """Test the get_abs_external_url static method."""
        mock_table_data = MagicMock()
        mock_table_data.table_path = table_path
        assert (
            ObjectStoreSourceAdapter.get_abs_external_url(mock_table_data) == expected
        )

    def test_adapter_initialization(self):
        """Test the initialization of the adapter."""
        # Test S3 adapter
        s3_adapter = ObjectStoreSourceAdapter(
            platform="s3", platform_name="Amazon S3", aws_region="us-west-2"
        )
        assert s3_adapter.platform == "s3"
        assert s3_adapter.platform_name == "Amazon S3"
        assert s3_adapter.aws_region == "us-west-2"

        # Test GCS adapter
        gcs_adapter = ObjectStoreSourceAdapter(
            platform="gcs", platform_name="Google Cloud Storage"
        )
        assert gcs_adapter.platform == "gcs"
        assert gcs_adapter.platform_name == "Google Cloud Storage"

        # Test ABS adapter
        abs_adapter = ObjectStoreSourceAdapter(
            platform="abs",
            platform_name="Azure Blob Storage",
            azure_storage_account="myaccount",
        )
        assert abs_adapter.platform == "abs"
        assert abs_adapter.platform_name == "Azure Blob Storage"
        assert abs_adapter.azure_storage_account == "myaccount"

    def test_register_customization(self):
        """Test registering customizations."""
        adapter = ObjectStoreSourceAdapter(platform="s3", platform_name="Amazon S3")

        # Register a custom function
        def custom_func(x):
            return x * 2

        adapter.register_customization("custom_method", custom_func)

        assert "custom_method" in adapter.customizations
        assert adapter.customizations["custom_method"] == custom_func

    def test_apply_customizations(self):
        """Test applying customizations to a source."""
        adapter = ObjectStoreSourceAdapter(platform="s3", platform_name="Amazon S3")

        # Create a mock source
        mock_source = MagicMock()
        mock_source.source_config = MagicMock()

        # Register a customization
        def custom_func(x):
            return x * 2

        adapter.register_customization("custom_method", custom_func)

        # Apply customizations
        result = adapter.apply_customizations(mock_source)

        # Check that the platform was set
        assert mock_source.source_config.platform == "s3"

        # Check that the custom method was added
        assert hasattr(mock_source, "custom_method")
        assert mock_source.custom_method == custom_func

        # Check that the result is the same object
        assert result == mock_source

    @pytest.mark.parametrize(
        "platform,table_path,expected_url",
        [
            (
                "s3",
                "s3://bucket/path/to/file.txt",
                "https://us-east-1.console.aws.amazon.com/s3/buckets/bucket?prefix=path/to/file.txt",
            ),
            (
                "gcs",
                "gs://bucket/path/to/file.txt",
                "https://console.cloud.google.com/storage/browser/bucket/path/to/file.txt",
            ),
            (
                "abs",
                "abfss://container@account.dfs.core.windows.net/path/to/file.txt",
                "https://portal.azure.com/#blade/Microsoft_Azure_Storage/ContainerMenuBlade/overview/storageAccountId/account/containerName/container",
            ),
            (
                "abs",
                "https://account.blob.core.windows.net/container/path/to/file.txt",
                "https://portal.azure.com/#blade/Microsoft_Azure_Storage/ContainerMenuBlade/overview/storageAccountId/account/containerName/container",
            ),
        ],
    )
    def test_get_external_url(self, platform, table_path, expected_url):
        """Test the get_external_url method."""
        mock_table_data = MagicMock()
        mock_table_data.table_path = table_path

        if platform == "s3":
            adapter = ObjectStoreSourceAdapter(
                platform=platform, platform_name="Amazon S3", aws_region="us-east-1"
            )
        elif platform == "gcs":
            adapter = ObjectStoreSourceAdapter(
                platform=platform, platform_name="Google Cloud Storage"
            )
        elif platform == "abs":
            adapter = ObjectStoreSourceAdapter(
                platform=platform, platform_name="Azure Blob Storage"
            )

        assert adapter.get_external_url(mock_table_data) == expected_url


class TestCreateObjectStoreAdapter:
    """Tests for the create_object_store_adapter function."""

    @pytest.mark.parametrize(
        "platform,aws_region,azure_storage_account,expected_platform,expected_name",
        [
            ("s3", "us-west-2", None, "s3", "Amazon S3"),
            ("gcs", None, None, "gcs", "Google Cloud Storage"),
            ("abs", None, "myaccount", "abs", "Azure Blob Storage"),
            ("unknown", None, None, "unknown", "Unknown (unknown)"),
        ],
    )
    def test_create_adapter(
        self,
        platform,
        aws_region,
        azure_storage_account,
        expected_platform,
        expected_name,
    ):
        """Test creating adapters for different platforms."""
        adapter = create_object_store_adapter(
            platform, aws_region=aws_region, azure_storage_account=azure_storage_account
        )
        assert adapter.platform == expected_platform
        assert adapter.platform_name == expected_name
        if aws_region:
            assert adapter.aws_region == aws_region
        if azure_storage_account:
            assert adapter.azure_storage_account == azure_storage_account


class TestABSHTTPSSupport:
    """Tests specifically for HTTPS Azure Blob Storage support."""

    @pytest.mark.parametrize(
        "uri,expected",
        [
            (
                "https://odedmdatacatalog.blob.core.windows.net/settler/import_export_services/message_data_randomized.csv",
                True,
            ),
            ("https://account.blob.core.windows.net/container/path", True),
            ("https://myaccount123.blob.core.windows.net/data/file.json", True),
            ("https://google.com/path", False),
            ("https://example.com/path", False),
        ],
    )
    def test_https_uri_detection(self, uri, expected):
        """Test that HTTPS Azure Blob Storage URIs are detected correctly."""
        assert ABSObjectStore.is_uri(uri) == expected

    @pytest.mark.parametrize(
        "uri,expected",
        [
            (
                "https://odedmdatacatalog.blob.core.windows.net/settler/import_export_services/message_data_randomized.csv",
                "settler",
            ),
            ("https://account.blob.core.windows.net/container/path", "container"),
            ("https://myaccount123.blob.core.windows.net/data/file.json", "data"),
        ],
    )
    def test_https_container_extraction(self, uri, expected):
        """Test container name extraction from HTTPS URIs."""
        assert ABSObjectStore.get_bucket_name(uri) == expected

    @pytest.mark.parametrize(
        "uri,expected",
        [
            (
                "https://odedmdatacatalog.blob.core.windows.net/settler/import_export_services/message_data_randomized.csv",
                "import_export_services/message_data_randomized.csv",
            ),
            ("https://account.blob.core.windows.net/container/path", "path"),
            ("https://myaccount123.blob.core.windows.net/data/file.json", "file.json"),
        ],
    )
    def test_https_object_key_extraction(self, uri, expected):
        """Test object key extraction from HTTPS URIs."""
        assert ABSObjectStore.get_object_key(uri) == expected

    @pytest.mark.parametrize(
        "uri,expected",
        [
            (
                "https://odedmdatacatalog.blob.core.windows.net/settler/import_export_services/message_data_randomized.csv",
                "https://odedmdatacatalog.blob.core.windows.net/",
            ),
            (
                "https://account.blob.core.windows.net/container/path",
                "https://account.blob.core.windows.net/",
            ),
        ],
    )
    def test_https_prefix_extraction(self, uri, expected):
        """Test prefix extraction from HTTPS URIs."""
        assert ABSObjectStore.get_prefix(uri) == expected

    @pytest.mark.parametrize(
        "uri,expected",
        [
            (
                "https://odedmdatacatalog.blob.core.windows.net/settler/import_export_services/message_data_randomized.csv",
                "settler",
            ),
            ("https://account.blob.core.windows.net/container/path", "container"),
        ],
    )
    def test_fallback_bucket_name_resolution(self, uri, expected):
        """Test the fallback logic in get_object_store_bucket_name."""
        assert get_object_store_bucket_name(uri) == expected

    def test_mixed_format_compatibility(self):
        """Test that both abfss:// and HTTPS formats work for the same container."""
        abfss_uri = "abfss://container@account.dfs.core.windows.net/path/file.txt"
        https_uri = "https://account.blob.core.windows.net/container/path/file.txt"

        # Both should be recognized as ABS URIs
        assert ABSObjectStore.is_uri(abfss_uri)
        assert ABSObjectStore.is_uri(https_uri)

        # Both should extract the same container name
        assert ABSObjectStore.get_bucket_name(abfss_uri) == "container"
        assert ABSObjectStore.get_bucket_name(https_uri) == "container"

        # Both should extract the same object key
        assert ABSObjectStore.get_object_key(abfss_uri) == "path/file.txt"
        assert ABSObjectStore.get_object_key(https_uri) == "path/file.txt"


# Parametrized tests for GCS URI normalization
@pytest.mark.parametrize(
    "input_uri,expected",
    [
        ("gs://bucket/path/to/file.parquet", "s3://bucket/path/to/file.parquet"),
        ("s3://bucket/path/to/file.parquet", "s3://bucket/path/to/file.parquet"),
        ("", ""),
        ("gs://bucket/", "s3://bucket/"),
        ("gs://bucket/nested/path/file.json", "s3://bucket/nested/path/file.json"),
    ],
)
def test_gcs_uri_normalization_for_pattern_matching(input_uri, expected):
    """Test that GCS URIs are normalized to S3 URIs for pattern matching."""
    gcs_adapter = create_object_store_adapter("gcs")
    result = gcs_adapter._normalize_gcs_uri_for_pattern_matching(input_uri)
    assert result == expected


@pytest.mark.parametrize(
    "input_uri,expected",
    [
        ("gs://bucket/path/to/file.parquet", "bucket/path/to/file.parquet"),
        ("s3://bucket/path/to/file.parquet", "s3://bucket/path/to/file.parquet"),
        ("", ""),
        ("gs://bucket/", "bucket/"),
        ("gs://bucket/nested/path/file.json", "bucket/nested/path/file.json"),
    ],
)
def test_gcs_prefix_stripping(input_uri, expected):
    """Test that GCS prefixes are stripped correctly."""
    gcs_adapter = create_object_store_adapter("gcs")
    result = gcs_adapter._strip_gcs_prefix(input_uri)
    assert result == expected


# Parametrized tests for ABS HTTPS URI handling
@pytest.mark.parametrize(
    "input_uri,expected_container,expected_key",
    [
        (
            "https://account.blob.core.windows.net/container/path/file.txt",
            "container",
            "path/file.txt",
        ),
        (
            "https://odedmdatacatalog.blob.core.windows.net/settler/import_export_services/message_data_randomized.csv",
            "settler",
            "import_export_services/message_data_randomized.csv",
        ),
        (
            "https://mystorageaccount.blob.core.windows.net/data/2023/logs/app.log",
            "data",
            "2023/logs/app.log",
        ),
        ("https://account.blob.core.windows.net/container/", "container", ""),
        ("https://account.blob.core.windows.net/container", "container", ""),
    ],
)
def test_abs_https_uri_parsing(input_uri, expected_container, expected_key):
    """Test that HTTPS ABS URIs are parsed correctly."""
    assert ABSObjectStore.is_uri(input_uri)
    assert ABSObjectStore.get_bucket_name(input_uri) == expected_container
    assert ABSObjectStore.get_object_key(input_uri) == expected_key


class TestGCSURINormalization:
    """Tests for the GCS URI normalization fix."""

    def test_gcs_adapter_customizations(self):
        """Test that GCS adapter registers the expected customizations."""
        gcs_adapter = create_object_store_adapter("gcs")

        # Check that the required customizations are registered
        expected_customizations = [
            "is_s3_platform",
            "create_s3_path",
            "get_external_url",
            "_normalize_uri_for_pattern_matching",
            "strip_s3_prefix",
        ]

        for customization in expected_customizations:
            assert customization in gcs_adapter.customizations

    def test_gcs_adapter_applied_to_mock_source(self):
        """Test that GCS adapter customizations are applied to a mock source."""
        gcs_adapter = create_object_store_adapter("gcs")

        # Create a mock S3 source
        mock_source = MagicMock()
        mock_source.source_config = MagicMock()

        # Apply customizations
        gcs_adapter.apply_customizations(mock_source)

        # Check that the customizations were applied
        assert hasattr(mock_source, "_normalize_uri_for_pattern_matching")
        assert hasattr(mock_source, "strip_s3_prefix")
        assert hasattr(mock_source, "create_s3_path")

        # Test that the URI normalization method works on the mock source
        test_uri = "gs://bucket/path/file.parquet"
        normalized = mock_source._normalize_uri_for_pattern_matching(test_uri)
        assert normalized == "s3://bucket/path/file.parquet"

        # Test that the prefix stripping method works on the mock source
        stripped = mock_source.strip_s3_prefix(test_uri)
        assert stripped == "bucket/path/file.parquet"

    def test_gcs_path_creation_via_adapter(self):
        """Test that GCS paths are created correctly via the adapter."""
        gcs_adapter = create_object_store_adapter("gcs")

        # Create a mock source and apply customizations
        mock_source = MagicMock()
        mock_source.source_config = MagicMock()
        gcs_adapter.apply_customizations(mock_source)

        # Test that create_s3_path now creates GCS paths
        gcs_path = mock_source.create_s3_path("bucket", "path/to/file.parquet")
        assert gcs_path == "gs://bucket/path/to/file.parquet"

    def test_pattern_matching_scenario(self):
        """Test the actual pattern matching scenario that was failing."""
        gcs_adapter = create_object_store_adapter("gcs")

        # Simulate the scenario where:
        # 1. Path spec pattern is s3://bucket/path/{table}/*.parquet
        # 2. File URI is gs://bucket/path/food_parquet/file.parquet

        path_spec_pattern = "s3://bucket/path/{table}/*.parquet"
        file_uri = "gs://bucket/path/food_parquet/file.parquet"

        # Normalize the file URI for pattern matching
        normalized_file_uri = gcs_adapter._normalize_gcs_uri_for_pattern_matching(
            file_uri
        )

        # The normalized URI should now be compatible with the pattern
        assert normalized_file_uri == "s3://bucket/path/food_parquet/file.parquet"

        # Test that the normalized URI would match the pattern (simplified test)
        glob_pattern = path_spec_pattern.replace("{table}", "*")
        assert pathlib.PurePath(normalized_file_uri).match(glob_pattern)


if __name__ == "__main__":
    pytest.main([__file__])
