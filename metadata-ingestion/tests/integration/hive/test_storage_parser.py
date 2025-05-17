from datahub.ingestion.source.sql.hive import StoragePathParser, StoragePlatform


# Mock logger
class MockLogger:
    def warning(self, msg):
        pass


logger = MockLogger()


class TestStoragePathParser:
    def test_local_file_no_scheme(self):
        """Test parsing a local file path with no scheme."""
        result = StoragePathParser.parse_storage_location("/path/to/file")
        assert result == (StoragePlatform.LOCAL, "/path/to/file")

    def test_s3_path(self):
        """Test parsing an S3 path."""
        result = StoragePathParser.parse_storage_location("s3://my-bucket/path/to/file")
        assert result == (StoragePlatform.S3, "my-bucket/path/to/file")

    def test_azure_abfss_path(self):
        """Test parsing an Azure ABFSS path."""
        result = StoragePathParser.parse_storage_location(
            "abfss://container@account.dfs.core.windows.net/path/to/file"
        )
        assert result == (StoragePlatform.AZURE, "container/path/to/file")

    def test_azure_abfs_path(self):
        """Test parsing an Azure ABFS path."""
        result = StoragePathParser.parse_storage_location(
            "abfs://container@account.dfs.core.windows.net/path/to/file"
        )
        assert result == (StoragePlatform.AZURE, "container/path/to/file")

    def test_azure_wasbs_path(self):
        """Test parsing an Azure WASBS path."""
        result = StoragePathParser.parse_storage_location(
            "wasbs://container@account.blob.core.windows.net/path/to/file"
        )
        assert result == (StoragePlatform.AZURE, "container/path/to/file")

    def test_gcs_path(self):
        """Test parsing a Google Cloud Storage path."""
        result = StoragePathParser.parse_storage_location("gs://my-bucket/path/to/file")
        assert result == (StoragePlatform.GCS, "my-bucket/path/to/file")

    def test_dbfs_path(self):
        """Test parsing a Databricks File System path."""
        result = StoragePathParser.parse_storage_location("dbfs:/path/to/file")
        assert result == (StoragePlatform.DBFS, "/path/to/file")

    def test_local_file_with_scheme(self):
        """Test parsing a local file with scheme."""
        result = StoragePathParser.parse_storage_location("file:///path/to/file")
        assert result == (StoragePlatform.LOCAL, "/path/to/file")

    def test_hdfs_path(self):
        """Test parsing an HDFS path."""
        result = StoragePathParser.parse_storage_location(
            "hdfs://namenode:8020/path/to/file"
        )
        assert result == (StoragePlatform.HDFS, "namenode:8020/path/to/file")

    def test_empty_string(self):
        """Test parsing an empty string."""
        result = StoragePathParser.parse_storage_location("")
        assert result is None

    def test_invalid_scheme(self):
        """Test parsing a URI with an invalid scheme."""
        result = StoragePathParser.parse_storage_location("invalid://path/to/file")
        assert result is None

    def test_no_scheme(self):
        """Test parsing a URI with no scheme."""
        result = StoragePathParser.parse_storage_location("path/to/file")
        assert result is None

    def test_normalize_multiple_slashes(self):
        """Test normalization of multiple slashes."""
        result = StoragePathParser.parse_storage_location(
            "s3://bucket//path///to////file"
        )
        assert result == (StoragePlatform.S3, "bucket/path/to/file")

    def test_remove_trailing_slashes(self):
        """Test removal of trailing slashes."""
        result = StoragePathParser.parse_storage_location("s3://bucket/path/")
        assert result == (StoragePlatform.S3, "bucket/path")
