"""Unit tests for the Airflow Asset adapter module."""

from typing import Any, Optional

from datahub_airflow_plugin._airflow_asset_adapter import (
    URI_SCHEME_TO_PLATFORM,
    extract_urns_from_iolets,
    is_airflow_asset,
    translate_airflow_asset_to_urn,
)
from datahub_airflow_plugin.entities import Urn


class MockAsset:
    """Mock Airflow Asset/Dataset for testing."""

    def __init__(self, uri: str, name: Optional[str] = None, group: str = ""):
        self.uri = uri
        self.name = name
        self.group = group


class MockDataset:
    """Mock Airflow Dataset (Airflow 2.x naming) for testing."""

    def __init__(self, uri: str, name: Optional[str] = None, group: str = ""):
        self.uri = uri
        self.name = name
        self.group = group


class NotAnAsset:
    """Object that looks like an asset but isn't."""

    def __init__(self, uri: str):
        self.uri = uri


class Asset:
    """Mock that has the class name 'Asset' but no uri attribute."""

    pass


class TestIsAirflowAsset:
    def test_mock_asset_is_recognized(self) -> None:
        asset = MockAsset(uri="s3://bucket/path")
        # MockAsset doesn't have the exact class name, so it should return False
        assert not is_airflow_asset(asset)

    def test_asset_class_name_with_uri(self) -> None:
        # Create a real Asset-named class with uri
        class Asset:
            def __init__(self, uri: str):
                self.uri = uri

        asset = Asset(uri="s3://bucket/path")
        assert is_airflow_asset(asset)

    def test_dataset_class_name_with_uri(self) -> None:
        # Create a real Dataset-named class with uri
        class Dataset:
            def __init__(self, uri: str):
                self.uri = uri

        dataset = Dataset(uri="s3://bucket/path")
        assert is_airflow_asset(dataset)

    def test_asset_class_without_uri_is_rejected(self) -> None:
        asset = Asset()
        assert not is_airflow_asset(asset)

    def test_non_asset_is_rejected(self) -> None:
        obj = NotAnAsset(uri="s3://bucket/path")
        assert not is_airflow_asset(obj)

    def test_string_is_rejected(self) -> None:
        assert not is_airflow_asset("s3://bucket/path")

    def test_none_is_rejected(self) -> None:
        assert not is_airflow_asset(None)


class TestTranslateAirflowAssetToUrn:
    """Tests for translate_airflow_asset_to_urn function."""

    def test_s3_uri(self) -> None:
        class Asset:
            uri = "s3://my-bucket/path/to/data"

        urn = translate_airflow_asset_to_urn(Asset())
        assert (
            urn == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/path/to/data,PROD)"
        )

    def test_s3a_uri(self) -> None:
        class Asset:
            uri = "s3a://my-bucket/path/to/data"

        urn = translate_airflow_asset_to_urn(Asset())
        assert (
            urn == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/path/to/data,PROD)"
        )

    def test_gcs_uri(self) -> None:
        class Asset:
            uri = "gs://my-bucket/path/to/data"

        urn = translate_airflow_asset_to_urn(Asset())
        assert (
            urn
            == "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/path/to/data,PROD)"
        )

    def test_postgres_uri(self) -> None:
        class Asset:
            uri = "postgresql://myhost/mydb/mytable"

        urn = translate_airflow_asset_to_urn(Asset())
        assert (
            urn
            == "urn:li:dataset:(urn:li:dataPlatform:postgres,myhost/mydb/mytable,PROD)"
        )

    def test_mysql_uri(self) -> None:
        class Asset:
            uri = "mysql://myhost/mydb/mytable"

        urn = translate_airflow_asset_to_urn(Asset())
        assert (
            urn == "urn:li:dataset:(urn:li:dataPlatform:mysql,myhost/mydb/mytable,PROD)"
        )

    def test_bigquery_uri(self) -> None:
        class Asset:
            uri = "bigquery://project/dataset/table"

        urn = translate_airflow_asset_to_urn(Asset())
        assert (
            urn
            == "urn:li:dataset:(urn:li:dataPlatform:bigquery,project/dataset/table,PROD)"
        )

    def test_snowflake_uri(self) -> None:
        class Asset:
            uri = "snowflake://account/db/schema/table"

        urn = translate_airflow_asset_to_urn(Asset())
        assert (
            urn
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,account/db/schema/table,PROD)"
        )

    def test_hdfs_uri(self) -> None:
        class Asset:
            uri = "hdfs://namenode/path/to/data"

        urn = translate_airflow_asset_to_urn(Asset())
        assert (
            urn
            == "urn:li:dataset:(urn:li:dataPlatform:hdfs,namenode/path/to/data,PROD)"
        )

    def test_adls_uri_abfs(self) -> None:
        class Asset:
            uri = "abfs://container@storage.dfs.core.windows.net/path"

        urn = translate_airflow_asset_to_urn(Asset())
        assert (
            urn
            == "urn:li:dataset:(urn:li:dataPlatform:adls,container@storage.dfs.core.windows.net/path,PROD)"
        )

    def test_adls_uri_abfss(self) -> None:
        class Asset:
            uri = "abfss://container@storage.dfs.core.windows.net/path"

        urn = translate_airflow_asset_to_urn(Asset())
        assert (
            urn
            == "urn:li:dataset:(urn:li:dataPlatform:adls,container@storage.dfs.core.windows.net/path,PROD)"
        )

    def test_file_uri(self) -> None:
        class Asset:
            uri = "file:///local/path/to/data"

        urn = translate_airflow_asset_to_urn(Asset())
        assert (
            urn == "urn:li:dataset:(urn:li:dataPlatform:file,local/path/to/data,PROD)"
        )

    def test_custom_env(self) -> None:
        class Asset:
            uri = "s3://bucket/path"

        urn = translate_airflow_asset_to_urn(Asset(), env="DEV")
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/path,DEV)"

    def test_unknown_scheme_uses_scheme_as_platform(self) -> None:
        class Asset:
            uri = "customscheme://host/path"

        urn = translate_airflow_asset_to_urn(Asset())
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:customscheme,host/path,PROD)"

    def test_no_uri_attribute_returns_none(self) -> None:
        class NoUri:
            pass

        urn = translate_airflow_asset_to_urn(NoUri())
        assert urn is None

    def test_empty_uri_returns_none(self) -> None:
        class Asset:
            uri = ""

        urn = translate_airflow_asset_to_urn(Asset())
        assert urn is None

    def test_uri_with_only_scheme_returns_none(self) -> None:
        class Asset:
            uri = "s3://"

        urn = translate_airflow_asset_to_urn(Asset())
        assert urn is None


class TestUriSchemeToPlatform:
    """Tests for the URI_SCHEME_TO_PLATFORM mapping."""

    def test_s3_schemes(self) -> None:
        assert URI_SCHEME_TO_PLATFORM["s3"] == "s3"
        assert URI_SCHEME_TO_PLATFORM["s3a"] == "s3"

    def test_gcs_schemes(self) -> None:
        assert URI_SCHEME_TO_PLATFORM["gs"] == "gcs"
        assert URI_SCHEME_TO_PLATFORM["gcs"] == "gcs"

    def test_adls_schemes(self) -> None:
        assert URI_SCHEME_TO_PLATFORM["abfs"] == "adls"
        assert URI_SCHEME_TO_PLATFORM["abfss"] == "adls"

    def test_database_schemes(self) -> None:
        assert URI_SCHEME_TO_PLATFORM["postgresql"] == "postgres"
        assert URI_SCHEME_TO_PLATFORM["mysql"] == "mysql"
        assert URI_SCHEME_TO_PLATFORM["bigquery"] == "bigquery"
        assert URI_SCHEME_TO_PLATFORM["snowflake"] == "snowflake"


class TestExtractUrnsFromIolets:
    """Tests for extract_urns_from_iolets function."""

    def test_extracts_entity_urns(self) -> None:
        entity1 = Urn("urn:li:dataset:(urn:li:dataPlatform:postgres,db.table1,PROD)")
        entity2 = Urn("urn:li:dataset:(urn:li:dataPlatform:postgres,db.table2,PROD)")

        urns = extract_urns_from_iolets(
            [entity1, entity2], capture_airflow_assets=False
        )

        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.table1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.table2,PROD)",
        ]

    def test_extracts_airflow_asset_urns(self) -> None:
        class Asset:
            def __init__(self, uri: str):
                self.uri = uri

        asset1 = Asset("s3://bucket/path1")
        asset2 = Asset("gs://bucket/path2")

        urns = extract_urns_from_iolets([asset1, asset2], capture_airflow_assets=True)

        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/path1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:gcs,bucket/path2,PROD)",
        ]

    def test_mixed_entities_and_assets(self) -> None:
        class Asset:
            def __init__(self, uri: str):
                self.uri = uri

        entity = Urn("urn:li:dataset:(urn:li:dataPlatform:postgres,db.table,PROD)")
        asset = Asset("s3://bucket/path")

        urns = extract_urns_from_iolets([entity, asset], capture_airflow_assets=True)

        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/path,PROD)",
        ]

    def test_ignores_airflow_assets_when_disabled(self) -> None:
        class Asset:
            def __init__(self, uri: str):
                self.uri = uri

        entity = Urn("urn:li:dataset:(urn:li:dataPlatform:postgres,db.table,PROD)")
        asset = Asset("s3://bucket/path")

        urns = extract_urns_from_iolets([entity, asset], capture_airflow_assets=False)

        # Only the entity should be included, asset should be ignored
        assert urns == ["urn:li:dataset:(urn:li:dataPlatform:postgres,db.table,PROD)"]

    def test_skips_invalid_assets(self) -> None:
        class Asset:
            def __init__(self, uri: str):
                self.uri = uri

        valid_asset = Asset("s3://bucket/path")
        invalid_asset = Asset("")  # Empty URI should be skipped

        urns = extract_urns_from_iolets(
            [valid_asset, invalid_asset], capture_airflow_assets=True
        )

        assert urns == ["urn:li:dataset:(urn:li:dataPlatform:s3,bucket/path,PROD)"]

    def test_uses_custom_env(self) -> None:
        class Asset:
            def __init__(self, uri: str):
                self.uri = uri

        asset = Asset("s3://bucket/path")

        urns = extract_urns_from_iolets([asset], capture_airflow_assets=True, env="DEV")

        assert urns == ["urn:li:dataset:(urn:li:dataPlatform:s3,bucket/path,DEV)"]

    def test_empty_list_returns_empty(self) -> None:
        urns = extract_urns_from_iolets([], capture_airflow_assets=True)
        assert urns == []

    def test_ignores_unknown_types(self) -> None:
        unknown = NotAnAsset(uri="s3://bucket/path")

        urns = extract_urns_from_iolets([unknown], capture_airflow_assets=True)

        assert urns == []


def _create_airflow_dataset(uri: str) -> Optional[Any]:
    """Create an Airflow Dataset, or None if creation fails.

    Airflow's Dataset class may fail to initialize in test environments
    where the ProvidersManager cannot fully initialize. This helper
    catches those errors and returns None.
    """
    try:
        from airflow.datasets import Dataset

        return Dataset(uri)
    except Exception:
        return None


class TestRealAirflowDataset:
    """Tests using the actual Airflow Dataset class.

    These tests ensure the adapter works with real Airflow objects,
    not just mocks with the same class name.

    Note: These tests may be skipped if the Airflow environment
    cannot properly initialize the ProvidersManager.
    """

    def test_is_airflow_asset_with_real_dataset(self) -> None:
        """Test that real Airflow Dataset is recognized."""
        dataset = _create_airflow_dataset("s3://my-bucket/data.parquet")
        if dataset is None:
            import pytest

            pytest.skip("Could not create Airflow Dataset - ProvidersManager issue")

        assert is_airflow_asset(dataset)

    def test_translate_real_airflow_dataset_s3(self) -> None:
        """Test translating real Airflow Dataset with S3 URI."""
        dataset = _create_airflow_dataset("s3://my-bucket/path/to/data")
        if dataset is None:
            import pytest

            pytest.skip("Could not create Airflow Dataset - ProvidersManager issue")

        urn = translate_airflow_asset_to_urn(dataset)
        assert (
            urn == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/path/to/data,PROD)"
        )

    def test_translate_real_airflow_dataset_postgres(self) -> None:
        """Test translating real Airflow Dataset with PostgreSQL URI."""
        dataset = _create_airflow_dataset("postgresql://myhost/mydb/schema/table")
        if dataset is None:
            import pytest

            pytest.skip("Could not create Airflow Dataset - ProvidersManager issue")

        urn = translate_airflow_asset_to_urn(dataset)
        assert (
            urn
            == "urn:li:dataset:(urn:li:dataPlatform:postgres,myhost/mydb/schema/table,PROD)"
        )

    def test_translate_real_airflow_dataset_custom_env(self) -> None:
        """Test translating real Airflow Dataset with custom environment."""
        dataset = _create_airflow_dataset("gs://my-bucket/data.csv")
        if dataset is None:
            import pytest

            pytest.skip("Could not create Airflow Dataset - ProvidersManager issue")

        urn = translate_airflow_asset_to_urn(dataset, env="DEV")
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/data.csv,DEV)"

    def test_extract_urns_with_real_airflow_dataset(self) -> None:
        """Test extract_urns_from_iolets with real Airflow Dataset."""
        dataset1 = _create_airflow_dataset("s3://bucket/input.parquet")
        dataset2 = _create_airflow_dataset("bigquery://project/dataset/table")
        if dataset1 is None or dataset2 is None:
            import pytest

            pytest.skip("Could not create Airflow Dataset - ProvidersManager issue")

        urns = extract_urns_from_iolets(
            [dataset1, dataset2], capture_airflow_assets=True
        )

        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/input.parquet,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,project/dataset/table,PROD)",
        ]

    def test_mixed_datahub_entity_and_real_airflow_dataset(self) -> None:
        """Test mixing DataHub entities with real Airflow Datasets."""
        dataset = _create_airflow_dataset("s3://bucket/output.parquet")
        if dataset is None:
            import pytest

            pytest.skip("Could not create Airflow Dataset - ProvidersManager issue")

        entity = Urn(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
        )

        urns = extract_urns_from_iolets([entity, dataset], capture_airflow_assets=True)

        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/output.parquet,PROD)",
        ]

    def test_real_airflow_dataset_ignored_when_disabled(self) -> None:
        """Test that real Airflow Datasets are ignored when capture is disabled."""
        dataset = _create_airflow_dataset("s3://bucket/data.parquet")
        if dataset is None:
            import pytest

            pytest.skip("Could not create Airflow Dataset - ProvidersManager issue")

        urns = extract_urns_from_iolets([dataset], capture_airflow_assets=False)

        assert urns == []
