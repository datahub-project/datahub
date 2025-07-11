import pathlib
import unittest
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


class TestS3ObjectStore(unittest.TestCase):
    """Tests for the S3ObjectStore implementation."""

    def test_is_uri(self):
        """Test the is_uri method with various URIs."""
        self.assertTrue(S3ObjectStore.is_uri("s3://bucket/path"))
        self.assertTrue(S3ObjectStore.is_uri("s3n://bucket/path"))
        self.assertTrue(S3ObjectStore.is_uri("s3a://bucket/path"))
        self.assertFalse(S3ObjectStore.is_uri("gs://bucket/path"))
        self.assertFalse(
            S3ObjectStore.is_uri("abfss://container@account.dfs.core.windows.net/path")
        )
        self.assertFalse(S3ObjectStore.is_uri("file:///path/to/file"))

    def test_get_prefix(self):
        """Test the get_prefix method."""
        self.assertEqual(S3ObjectStore.get_prefix("s3://bucket/path"), "s3://")
        self.assertEqual(S3ObjectStore.get_prefix("s3n://bucket/path"), "s3n://")
        self.assertEqual(S3ObjectStore.get_prefix("s3a://bucket/path"), "s3a://")
        self.assertIsNone(S3ObjectStore.get_prefix("gs://bucket/path"))

    def test_strip_prefix(self):
        """Test the strip_prefix method."""
        self.assertEqual(S3ObjectStore.strip_prefix("s3://bucket/path"), "bucket/path")
        self.assertEqual(S3ObjectStore.strip_prefix("s3n://bucket/path"), "bucket/path")
        self.assertEqual(S3ObjectStore.strip_prefix("s3a://bucket/path"), "bucket/path")

        # Should raise ValueError for non-S3 URIs
        with self.assertRaises(ValueError):
            S3ObjectStore.strip_prefix("gs://bucket/path")

    def test_get_bucket_name(self):
        """Test the get_bucket_name method."""
        self.assertEqual(S3ObjectStore.get_bucket_name("s3://bucket/path"), "bucket")
        self.assertEqual(
            S3ObjectStore.get_bucket_name("s3n://my-bucket/path/to/file"), "my-bucket"
        )
        self.assertEqual(
            S3ObjectStore.get_bucket_name("s3a://bucket.name/file.txt"), "bucket.name"
        )

        # Should raise ValueError for non-S3 URIs
        with self.assertRaises(ValueError):
            S3ObjectStore.get_bucket_name("gs://bucket/path")

    def test_get_object_key(self):
        """Test the get_object_key method."""
        self.assertEqual(S3ObjectStore.get_object_key("s3://bucket/path"), "path")
        self.assertEqual(
            S3ObjectStore.get_object_key("s3://bucket/path/to/file.txt"),
            "path/to/file.txt",
        )
        self.assertEqual(S3ObjectStore.get_object_key("s3://bucket/"), "")
        self.assertEqual(S3ObjectStore.get_object_key("s3://bucket"), "")

        # Should raise ValueError for non-S3 URIs
        with self.assertRaises(ValueError):
            S3ObjectStore.get_object_key("gs://bucket/path")


class TestGCSObjectStore(unittest.TestCase):
    """Tests for the GCSObjectStore implementation."""

    def test_is_uri(self):
        """Test the is_uri method with various URIs."""
        self.assertTrue(GCSObjectStore.is_uri("gs://bucket/path"))
        self.assertFalse(GCSObjectStore.is_uri("s3://bucket/path"))
        self.assertFalse(
            GCSObjectStore.is_uri("abfss://container@account.dfs.core.windows.net/path")
        )

    def test_get_prefix(self):
        """Test the get_prefix method."""
        self.assertEqual(GCSObjectStore.get_prefix("gs://bucket/path"), "gs://")
        self.assertIsNone(GCSObjectStore.get_prefix("s3://bucket/path"))

    def test_strip_prefix(self):
        """Test the strip_prefix method."""
        self.assertEqual(GCSObjectStore.strip_prefix("gs://bucket/path"), "bucket/path")

        # Should raise ValueError for non-GCS URIs
        with self.assertRaises(ValueError):
            GCSObjectStore.strip_prefix("s3://bucket/path")

    def test_get_bucket_name(self):
        """Test the get_bucket_name method."""
        self.assertEqual(GCSObjectStore.get_bucket_name("gs://bucket/path"), "bucket")
        self.assertEqual(
            GCSObjectStore.get_bucket_name("gs://my-bucket/path/to/file"), "my-bucket"
        )

        # Should raise ValueError for non-GCS URIs
        with self.assertRaises(ValueError):
            GCSObjectStore.get_bucket_name("s3://bucket/path")

    def test_get_object_key(self):
        """Test the get_object_key method."""
        self.assertEqual(GCSObjectStore.get_object_key("gs://bucket/path"), "path")
        self.assertEqual(
            GCSObjectStore.get_object_key("gs://bucket/path/to/file.txt"),
            "path/to/file.txt",
        )
        self.assertEqual(GCSObjectStore.get_object_key("gs://bucket/"), "")
        self.assertEqual(GCSObjectStore.get_object_key("gs://bucket"), "")

        # Should raise ValueError for non-GCS URIs
        with self.assertRaises(ValueError):
            GCSObjectStore.get_object_key("s3://bucket/path")


class TestABSObjectStore(unittest.TestCase):
    """Tests for the ABSObjectStore implementation."""

    def test_is_uri(self):
        """Test the is_uri method with various URIs."""
        self.assertTrue(
            ABSObjectStore.is_uri("abfss://container@account.dfs.core.windows.net/path")
        )
        self.assertFalse(ABSObjectStore.is_uri("s3://bucket/path"))
        self.assertFalse(ABSObjectStore.is_uri("gs://bucket/path"))

    def test_get_prefix(self):
        """Test the get_prefix method."""
        self.assertEqual(
            ABSObjectStore.get_prefix(
                "abfss://container@account.dfs.core.windows.net/path"
            ),
            "abfss://",
        )
        self.assertIsNone(ABSObjectStore.get_prefix("s3://bucket/path"))

    def test_strip_prefix(self):
        """Test the strip_prefix method."""
        self.assertEqual(
            ABSObjectStore.strip_prefix(
                "abfss://container@account.dfs.core.windows.net/path"
            ),
            "container@account.dfs.core.windows.net/path",
        )

        # Should raise ValueError for non-ABS URIs
        with self.assertRaises(ValueError):
            ABSObjectStore.strip_prefix("s3://bucket/path")

    def test_get_bucket_name(self):
        """Test the get_bucket_name method."""
        self.assertEqual(
            ABSObjectStore.get_bucket_name(
                "abfss://container@account.dfs.core.windows.net/path"
            ),
            "container",
        )

        # Should raise ValueError for non-ABS URIs
        with self.assertRaises(ValueError):
            ABSObjectStore.get_bucket_name("s3://bucket/path")

    def test_get_object_key(self):
        """Test the get_object_key method."""
        self.assertEqual(
            ABSObjectStore.get_object_key(
                "abfss://container@account.dfs.core.windows.net/path"
            ),
            "path",
        )
        self.assertEqual(
            ABSObjectStore.get_object_key(
                "abfss://container@account.dfs.core.windows.net/path/to/file.txt"
            ),
            "path/to/file.txt",
        )

        # Should raise ValueError for non-ABS URIs
        with self.assertRaises(ValueError):
            ABSObjectStore.get_object_key("s3://bucket/path")


class TestUtilityFunctions(unittest.TestCase):
    """Tests for the utility functions in object_store module."""

    def test_get_object_store_for_uri(self):
        """Test the get_object_store_for_uri function."""
        self.assertEqual(get_object_store_for_uri("s3://bucket/path"), S3ObjectStore)
        self.assertEqual(get_object_store_for_uri("gs://bucket/path"), GCSObjectStore)
        self.assertEqual(
            get_object_store_for_uri(
                "abfss://container@account.dfs.core.windows.net/path"
            ),
            ABSObjectStore,
        )
        self.assertIsNone(get_object_store_for_uri("file:///path/to/file"))

    def test_get_object_store_bucket_name(self):
        """Test the get_object_store_bucket_name function."""
        self.assertEqual(get_object_store_bucket_name("s3://bucket/path"), "bucket")
        self.assertEqual(get_object_store_bucket_name("gs://bucket/path"), "bucket")
        self.assertEqual(
            get_object_store_bucket_name(
                "abfss://container@account.dfs.core.windows.net/path"
            ),
            "container",
        )

        # Should raise ValueError for unsupported URIs
        with self.assertRaises(ValueError):
            get_object_store_bucket_name("file:///path/to/file")

    def test_get_object_key(self):
        """Test the get_object_key function."""
        self.assertEqual(get_object_key("s3://bucket/path"), "path")
        self.assertEqual(
            get_object_key("gs://bucket/path/to/file.txt"), "path/to/file.txt"
        )
        self.assertEqual(
            get_object_key("abfss://container@account.dfs.core.windows.net/path"),
            "path",
        )

        # Should raise ValueError for unsupported URIs
        with self.assertRaises(ValueError):
            get_object_key("file:///path/to/file")


class TestObjectStoreSourceAdapter(unittest.TestCase):
    """Tests for the ObjectStoreSourceAdapter class."""

    def test_create_s3_path(self):
        """Test the create_s3_path static method."""
        self.assertEqual(
            ObjectStoreSourceAdapter.create_s3_path("bucket", "path/to/file.txt"),
            "s3://bucket/path/to/file.txt",
        )

    def test_create_gcs_path(self):
        """Test the create_gcs_path static method."""
        self.assertEqual(
            ObjectStoreSourceAdapter.create_gcs_path("bucket", "path/to/file.txt"),
            "gs://bucket/path/to/file.txt",
        )

    def test_create_abs_path(self):
        """Test the create_abs_path static method."""
        self.assertEqual(
            ObjectStoreSourceAdapter.create_abs_path(
                "container", "path/to/file.txt", "storage"
            ),
            "abfss://container@storage.dfs.core.windows.net/path/to/file.txt",
        )

    def test_get_s3_external_url(self):
        """Test the get_s3_external_url static method."""
        mock_table_data = MagicMock()
        mock_table_data.table_path = "s3://bucket/path/to/file.txt"

        # Test with default region
        self.assertEqual(
            ObjectStoreSourceAdapter.get_s3_external_url(mock_table_data),
            "https://us-east-1.console.aws.amazon.com/s3/buckets/bucket?prefix=path/to/file.txt",
        )

        # Test with custom region
        self.assertEqual(
            ObjectStoreSourceAdapter.get_s3_external_url(mock_table_data, "us-west-2"),
            "https://us-west-2.console.aws.amazon.com/s3/buckets/bucket?prefix=path/to/file.txt",
        )

        # Test with non-S3 URI
        mock_table_data.table_path = "gs://bucket/path"
        self.assertIsNone(ObjectStoreSourceAdapter.get_s3_external_url(mock_table_data))

    def test_get_gcs_external_url(self):
        """Test the get_gcs_external_url static method."""
        mock_table_data = MagicMock()
        mock_table_data.table_path = "gs://bucket/path/to/file.txt"

        self.assertEqual(
            ObjectStoreSourceAdapter.get_gcs_external_url(mock_table_data),
            "https://console.cloud.google.com/storage/browser/bucket/path/to/file.txt",
        )

        # Test with non-GCS URI
        mock_table_data.table_path = "s3://bucket/path"
        self.assertIsNone(
            ObjectStoreSourceAdapter.get_gcs_external_url(mock_table_data)
        )

    def test_get_abs_external_url(self):
        """Test the get_abs_external_url static method."""
        mock_table_data = MagicMock()
        mock_table_data.table_path = (
            "abfss://container@account.dfs.core.windows.net/path/to/file.txt"
        )

        self.assertEqual(
            ObjectStoreSourceAdapter.get_abs_external_url(mock_table_data),
            "https://portal.azure.com/#blade/Microsoft_Azure_Storage/ContainerMenuBlade/overview/storageAccountId/account/containerName/container",
        )

        # Test with non-ABS URI
        mock_table_data.table_path = "s3://bucket/path"
        self.assertIsNone(
            ObjectStoreSourceAdapter.get_abs_external_url(mock_table_data)
        )

    def test_adapter_initialization(self):
        """Test the initialization of the adapter."""
        # Test S3 adapter
        s3_adapter = ObjectStoreSourceAdapter(
            platform="s3", platform_name="Amazon S3", aws_region="us-west-2"
        )
        self.assertEqual(s3_adapter.platform, "s3")
        self.assertEqual(s3_adapter.platform_name, "Amazon S3")
        self.assertEqual(s3_adapter.aws_region, "us-west-2")

        # Test GCS adapter
        gcs_adapter = ObjectStoreSourceAdapter(
            platform="gcs", platform_name="Google Cloud Storage"
        )
        self.assertEqual(gcs_adapter.platform, "gcs")
        self.assertEqual(gcs_adapter.platform_name, "Google Cloud Storage")

        # Test ABS adapter
        abs_adapter = ObjectStoreSourceAdapter(
            platform="abs",
            platform_name="Azure Blob Storage",
            azure_storage_account="myaccount",
        )
        self.assertEqual(abs_adapter.platform, "abs")
        self.assertEqual(abs_adapter.platform_name, "Azure Blob Storage")
        self.assertEqual(abs_adapter.azure_storage_account, "myaccount")

    def test_register_customization(self):
        """Test registering customizations."""
        adapter = ObjectStoreSourceAdapter(platform="s3", platform_name="Amazon S3")

        # Register a custom function
        def custom_func(x):
            return x * 2

        adapter.register_customization("custom_method", custom_func)

        self.assertIn("custom_method", adapter.customizations)
        self.assertEqual(adapter.customizations["custom_method"], custom_func)

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
        self.assertEqual(mock_source.source_config.platform, "s3")

        # Check that the custom method was added
        self.assertTrue(hasattr(mock_source, "custom_method"))
        self.assertEqual(mock_source.custom_method, custom_func)

        # Check that the result is the same object
        self.assertEqual(result, mock_source)

    def test_get_external_url(self):
        """Test the get_external_url method."""
        mock_table_data = MagicMock()
        mock_table_data.table_path = "s3://bucket/path/to/file.txt"

        # Test S3 adapter
        s3_adapter = ObjectStoreSourceAdapter(
            platform="s3", platform_name="Amazon S3", aws_region="us-west-2"
        )
        self.assertEqual(
            s3_adapter.get_external_url(mock_table_data),
            "https://us-west-2.console.aws.amazon.com/s3/buckets/bucket?prefix=path/to/file.txt",
        )

        # Test GCS adapter
        mock_table_data.table_path = "gs://bucket/path/to/file.txt"
        gcs_adapter = ObjectStoreSourceAdapter(
            platform="gcs", platform_name="Google Cloud Storage"
        )
        self.assertEqual(
            gcs_adapter.get_external_url(mock_table_data),
            "https://console.cloud.google.com/storage/browser/bucket/path/to/file.txt",
        )

        # Test ABS adapter
        mock_table_data.table_path = (
            "abfss://container@account.dfs.core.windows.net/path/to/file.txt"
        )
        abs_adapter = ObjectStoreSourceAdapter(
            platform="abs", platform_name="Azure Blob Storage"
        )
        self.assertEqual(
            abs_adapter.get_external_url(mock_table_data),
            "https://portal.azure.com/#blade/Microsoft_Azure_Storage/ContainerMenuBlade/overview/storageAccountId/account/containerName/container",
        )


class TestCreateObjectStoreAdapter(unittest.TestCase):
    """Tests for the create_object_store_adapter function."""

    def test_create_s3_adapter(self):
        """Test creating an S3 adapter."""
        adapter = create_object_store_adapter("s3", aws_region="us-west-2")
        self.assertEqual(adapter.platform, "s3")
        self.assertEqual(adapter.platform_name, "Amazon S3")
        self.assertEqual(adapter.aws_region, "us-west-2")

    def test_create_gcs_adapter(self):
        """Test creating a GCS adapter."""
        adapter = create_object_store_adapter("gcs")
        self.assertEqual(adapter.platform, "gcs")
        self.assertEqual(adapter.platform_name, "Google Cloud Storage")

    def test_create_abs_adapter(self):
        """Test creating an ABS adapter."""
        adapter = create_object_store_adapter("abs", azure_storage_account="myaccount")
        self.assertEqual(adapter.platform, "abs")
        self.assertEqual(adapter.platform_name, "Azure Blob Storage")
        self.assertEqual(adapter.azure_storage_account, "myaccount")

    def test_create_unknown_adapter(self):
        """Test creating an adapter for an unknown platform."""
        adapter = create_object_store_adapter("unknown")
        self.assertEqual(adapter.platform, "unknown")
        self.assertEqual(adapter.platform_name, "Unknown (unknown)")


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


class TestGCSURINormalization(unittest.TestCase):
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
            self.assertIn(customization, gcs_adapter.customizations)

    def test_gcs_adapter_applied_to_mock_source(self):
        """Test that GCS adapter customizations are applied to a mock source."""
        gcs_adapter = create_object_store_adapter("gcs")

        # Create a mock S3 source
        mock_source = MagicMock()
        mock_source.source_config = MagicMock()

        # Apply customizations
        gcs_adapter.apply_customizations(mock_source)

        # Check that the customizations were applied
        self.assertTrue(hasattr(mock_source, "_normalize_uri_for_pattern_matching"))
        self.assertTrue(hasattr(mock_source, "strip_s3_prefix"))
        self.assertTrue(hasattr(mock_source, "create_s3_path"))

        # Test that the URI normalization method works on the mock source
        test_uri = "gs://bucket/path/file.parquet"
        normalized = mock_source._normalize_uri_for_pattern_matching(test_uri)
        self.assertEqual(normalized, "s3://bucket/path/file.parquet")

        # Test that the prefix stripping method works on the mock source
        stripped = mock_source.strip_s3_prefix(test_uri)
        self.assertEqual(stripped, "bucket/path/file.parquet")

    def test_gcs_path_creation_via_adapter(self):
        """Test that GCS paths are created correctly via the adapter."""
        gcs_adapter = create_object_store_adapter("gcs")

        # Create a mock source and apply customizations
        mock_source = MagicMock()
        mock_source.source_config = MagicMock()
        gcs_adapter.apply_customizations(mock_source)

        # Test that create_s3_path now creates GCS paths
        gcs_path = mock_source.create_s3_path("bucket", "path/to/file.parquet")
        self.assertEqual(gcs_path, "gs://bucket/path/to/file.parquet")

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
        self.assertEqual(
            normalized_file_uri, "s3://bucket/path/food_parquet/file.parquet"
        )

        # Test that the normalized URI would match the pattern (simplified test)
        glob_pattern = path_spec_pattern.replace("{table}", "*")
        self.assertTrue(pathlib.PurePath(normalized_file_uri).match(glob_pattern))


if __name__ == "__main__":
    unittest.main()
