from datahub.emitter.mce_builder import make_storage_urn
from datahub.metadata.schema_classes import FabricTypeClass


class TestMakeStorageUrn:
    """Test the make_storage_urn function for various URI types."""

    def test_s3_uri_with_trailing_slash(self):
        """Test S3 URI with trailing slash is normalized."""
        uri = "s3://my-bucket/data/"
        expected = "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/data,PROD)"
        assert make_storage_urn(uri) == expected

    def test_s3_uri_without_trailing_slash(self):
        """Test S3 URI without trailing slash."""
        uri = "s3://my-bucket/data"
        expected = "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/data,PROD)"
        assert make_storage_urn(uri) == expected

    def test_s3a_protocol_mapping(self):
        """Test S3A protocol is mapped to s3 platform."""
        uri = "s3a://bucket/path/file.txt"
        expected = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/path/file.txt,PROD)"
        assert make_storage_urn(uri) == expected

    def test_dbfs_uri_with_trailing_slash(self):
        """Test DBFS URI with trailing slash is normalized."""
        uri = "dbfs:///mnt/data/"
        expected = "urn:li:dataset:(urn:li:dataPlatform:dbfs,/mnt/data,PROD)"
        assert make_storage_urn(uri) == expected

    def test_dbfs_uri_without_trailing_slash(self):
        """Test DBFS URI without trailing slash."""
        uri = "dbfs:///mnt/data"
        expected = "urn:li:dataset:(urn:li:dataPlatform:dbfs,/mnt/data,PROD)"
        assert make_storage_urn(uri) == expected

    def test_abfs_protocol_mapping(self):
        """Test ABFS protocol is mapped to abs platform."""
        uri = "abfs://container@account.dfs.core.windows.net/path"
        expected = "urn:li:dataset:(urn:li:dataPlatform:abs,container@account.dfs.core.windows.net/path,PROD)"
        assert make_storage_urn(uri) == expected

    def test_abfss_protocol_mapping(self):
        """Test ABFSS protocol is mapped to abs platform."""
        uri = "abfss://container@account.dfs.core.windows.net/path/"
        expected = "urn:li:dataset:(urn:li:dataPlatform:abs,container@account.dfs.core.windows.net/path,PROD)"
        assert make_storage_urn(uri) == expected

    def test_unknown_protocol_uses_protocol_as_platform(self):
        """Test unknown protocol uses the protocol itself as platform."""
        uri = "hdfs://namenode:8020/path/data/"
        expected = (
            "urn:li:dataset:(urn:li:dataPlatform:hdfs,namenode:8020/path/data,PROD)"
        )
        assert make_storage_urn(uri) == expected

    def test_no_protocol_uses_file_platform(self):
        """Test URI without protocol uses file platform."""
        uri = "/local/file/path"
        expected = "urn:li:dataset:(urn:li:dataPlatform:file,/local/file/path,PROD)"
        assert make_storage_urn(uri) == expected

    def test_custom_environment(self):
        """Test custom environment parameter."""
        uri = "s3://bucket/data"
        expected = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/data,DEV)"
        assert make_storage_urn(uri, env=FabricTypeClass.DEV) == expected

    def test_uri_with_nested_path(self):
        """Test URI with nested path structure."""
        uri = "s3://my-bucket/year=2023/month=08/day=13/data.parquet"
        expected = "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/year=2023/month=08/day=13/data.parquet,PROD)"
        assert make_storage_urn(uri) == expected

    def test_gcs_protocol_fallback(self):
        """Test GCS protocol uses itself as platform (fallback behavior)."""
        uri = "gs://bucket/path/file.txt"
        expected = "urn:li:dataset:(urn:li:dataPlatform:gs,bucket/path/file.txt,PROD)"
        assert make_storage_urn(uri) == expected

    def test_multiple_trailing_slashes(self):
        """Test multiple trailing slashes are all removed."""
        uri = "s3://bucket/path///"
        expected = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/path,PROD)"
        assert make_storage_urn(uri) == expected

    def test_root_path_with_slash(self):
        """Test root path with slash."""
        uri = "s3://bucket/"
        expected = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket,PROD)"
        assert make_storage_urn(uri) == expected

    def test_protocol_mapping_coverage(self):
        """Test all defined protocol mappings."""
        test_cases = [
            ("s3://test/path", "s3"),
            ("s3a://test/path", "s3"),
            ("abfs://test/path", "abs"),
            ("abfss://test/path", "abs"),
            ("dbfs://test/path", "dbfs"),
        ]

        for uri, expected_platform in test_cases:
            result = make_storage_urn(uri)
            assert f"urn:li:dataPlatform:{expected_platform}" in result
