"""Unit tests for the Airflow Asset adapter module."""

from typing import Any, Optional
from unittest.mock import MagicMock, patch

from datahub_airflow_plugin._airflow_asset_adapter import (
    URI_SCHEME_TO_PLATFORM,
    extract_urns_from_iolets,
    extract_urns_from_resolved_alias_events,
    extract_urns_from_task_instance_outlet_events,
    is_airflow_asset,
    is_airflow_asset_alias,
    translate_airflow_asset_alias_to_urn,
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

    def test_airflow_sanitized_trailing_slash_stripped(self) -> None:
        # Airflow's _sanitize_uri forces a "/" path on netloc-only URIs, e.g.
        # iceberg://catalog → stored as iceberg://catalog/
        # The trailing slash must not appear in the DataHub dataset name,
        # otherwise it won't match URNs produced by the DataHub Iceberg source.
        class Asset:
            uri = "iceberg://my_catalog/"

        urn = translate_airflow_asset_to_urn(Asset())
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:iceberg,my_catalog,PROD)"

    def test_trailing_slash_stripped_from_path(self) -> None:
        # Trailing slashes on S3 prefix URIs are also normalised away.
        class Asset:
            uri = "s3://my-bucket/path/to/table/"

        urn = translate_airflow_asset_to_urn(Asset())
        assert (
            urn
            == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/path/to/table,PROD)"
        )


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


class TestIsAirflowAssetAlias:
    """Tests for is_airflow_asset_alias function."""

    def test_asset_alias_class_with_name(self) -> None:
        class AssetAlias:
            def __init__(self, name: str):
                self.name = name

        alias = AssetAlias("parsed_data")
        assert is_airflow_asset_alias(alias)

    def test_asset_alias_class_without_name(self) -> None:
        class AssetAlias:
            pass

        alias = AssetAlias()
        assert not is_airflow_asset_alias(alias)

    def test_asset_with_uri_is_not_alias(self) -> None:
        # Asset has 'uri', so the guard should exclude it even if class name matched
        class Asset:
            def __init__(self, uri: str):
                self.uri = uri
                self.name = "some_name"

        asset = Asset(uri="s3://bucket/path")
        assert not is_airflow_asset_alias(asset)

    def test_regular_object_is_not_alias(self) -> None:
        class SomeClass:
            def __init__(self, name: str):
                self.name = name

        assert not is_airflow_asset_alias(SomeClass("foo"))

    def test_none_is_rejected(self) -> None:
        assert not is_airflow_asset_alias(None)

    def test_string_is_rejected(self) -> None:
        assert not is_airflow_asset_alias("parsed_data")


class TestTranslateAirflowAssetAliasToUrn:
    """Tests for translate_airflow_asset_alias_to_urn function."""

    def test_alias_name_maps_to_airflow_platform(self) -> None:
        class AssetAlias:
            name = "parsed_data"

        urn = translate_airflow_asset_alias_to_urn(AssetAlias())
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:airflow,parsed_data,PROD)"

    def test_custom_env(self) -> None:
        class AssetAlias:
            name = "my_alias"

        urn = translate_airflow_asset_alias_to_urn(AssetAlias(), env="DEV")
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:airflow,my_alias,DEV)"

    def test_empty_name_returns_none(self) -> None:
        class AssetAlias:
            name = ""

        assert translate_airflow_asset_alias_to_urn(AssetAlias()) is None

    def test_no_name_attribute_returns_none(self) -> None:
        class AssetAlias:
            pass

        assert translate_airflow_asset_alias_to_urn(AssetAlias()) is None


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

    def test_extracts_asset_alias_urns(self) -> None:
        class AssetAlias:
            def __init__(self, name: str):
                self.name = name

        alias = AssetAlias("parsed_data")
        urns = extract_urns_from_iolets([alias], capture_airflow_assets=True)

        assert urns == ["urn:li:dataset:(urn:li:dataPlatform:airflow,parsed_data,PROD)"]

    def test_ignores_asset_alias_when_disabled(self) -> None:
        class AssetAlias:
            def __init__(self, name: str):
                self.name = name

        entity = Urn("urn:li:dataset:(urn:li:dataPlatform:postgres,db.table,PROD)")
        alias = AssetAlias("parsed_data")

        urns = extract_urns_from_iolets([entity, alias], capture_airflow_assets=False)

        assert urns == ["urn:li:dataset:(urn:li:dataPlatform:postgres,db.table,PROD)"]

    def test_mixed_asset_and_alias(self) -> None:
        class Asset:
            def __init__(self, uri: str):
                self.uri = uri

        class AssetAlias:
            def __init__(self, name: str):
                self.name = name

        asset = Asset("s3://bucket/path")
        alias = AssetAlias("my_alias")

        urns = extract_urns_from_iolets([asset, alias], capture_airflow_assets=True)

        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/path,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:airflow,my_alias,PROD)",
        ]


class TestExtractUrnsFromResolvedAliasEvents:
    """Tests for extract_urns_from_resolved_alias_events function.

    The function lazily imports airflow.models.asset, airflow.utils.session,
    and sqlalchemy inside the function body, so we inject mocks via
    patch.dict(sys.modules, ...) rather than patching module-level attributes.
    """

    def _make_event(
        self,
        uri: str,
        alias_names: Optional[list] = None,
    ) -> MagicMock:
        """Build a mock AssetEvent with .asset.uri and .source_aliases."""
        event = MagicMock()
        event.asset = MagicMock()
        event.asset.uri = uri
        if alias_names is not None:
            # MagicMock(name=...) sets the mock's display name, not a .name attr —
            # create plain mocks and set .name explicitly.
            event.source_aliases = [MagicMock() for _ in alias_names]
            for alias_mock, alias_name in zip(
                event.source_aliases, alias_names, strict=False
            ):
                alias_mock.name = alias_name
        else:
            event.source_aliases = []
        return event

    def _make_session_ctx(self, events: list) -> MagicMock:
        """Return a context-manager mock whose scalars().all() yields events."""
        session = MagicMock()
        session.scalars.return_value.all.return_value = events
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=session)
        ctx.__exit__ = MagicMock(return_value=False)
        return ctx

    def _airflow_sys_modules(self, events: list) -> Any:
        """Inject mock airflow modules so the lazy imports inside the function work.

        sqlalchemy is also mocked here because select(MagicMock()) raises a
        SQLAlchemy error when passed a mock instead of a real ORM class. Since
        session.scalars() is fully mocked, the actual query object doesn't matter.
        """
        import sys

        ctx = self._make_session_ctx(events)
        mock_asset_mod = MagicMock()
        mock_session_mod = MagicMock()
        mock_session_mod.create_session.return_value = ctx
        mock_sqlalchemy = MagicMock()
        return patch.dict(
            sys.modules,
            {
                "airflow.models.asset": mock_asset_mod,
                "airflow.utils.session": mock_session_mod,
                "sqlalchemy": mock_sqlalchemy,
            },
        )

    def test_alias_event_converted_to_urn(self) -> None:
        event = self._make_event("s3://bucket/path", alias_names=["parsed_data"])
        with self._airflow_sys_modules([event]):
            urns = extract_urns_from_resolved_alias_events("my_dag", "my_task", "run_1")

        assert urns == ["urn:li:dataset:(urn:li:dataPlatform:s3,bucket/path,PROD)"]

    def test_direct_asset_event_skipped(self) -> None:
        """Events with no source_aliases came from a direct Asset outlet — skip."""
        event = self._make_event("s3://bucket/path", alias_names=[])
        with self._airflow_sys_modules([event]):
            urns = extract_urns_from_resolved_alias_events("my_dag", "my_task", "run_1")

        assert urns == []

    def test_event_with_no_asset_skipped(self) -> None:
        event = self._make_event("s3://bucket/path", alias_names=["alias"])
        event.asset = None
        with self._airflow_sys_modules([event]):
            urns = extract_urns_from_resolved_alias_events("my_dag", "my_task", "run_1")

        assert urns == []

    def test_multiple_alias_events(self) -> None:
        events = [
            self._make_event("s3://bucket/a", alias_names=["alias_a"]),
            self._make_event("gs://bucket/b", alias_names=["alias_b"]),
        ]
        with self._airflow_sys_modules(events):
            urns = extract_urns_from_resolved_alias_events("my_dag", "my_task", "run_1")

        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/a,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:gcs,bucket/b,PROD)",
        ]

    def test_custom_env(self) -> None:
        event = self._make_event("s3://bucket/path", alias_names=["alias"])
        with self._airflow_sys_modules([event]):
            urns = extract_urns_from_resolved_alias_events(
                "my_dag", "my_task", "run_1", env="DEV"
            )

        assert urns == ["urn:li:dataset:(urn:li:dataPlatform:s3,bucket/path,DEV)"]

    def test_db_exception_returns_empty(self) -> None:
        import sys

        ctx = MagicMock()
        ctx.__enter__ = MagicMock(side_effect=Exception("DB error"))
        ctx.__exit__ = MagicMock(return_value=False)
        mock_asset_mod = MagicMock()
        mock_session_mod = MagicMock()
        mock_session_mod.create_session.return_value = ctx

        with patch.dict(
            sys.modules,
            {
                "airflow.models.asset": mock_asset_mod,
                "airflow.utils.session": mock_session_mod,
            },
        ):
            urns = extract_urns_from_resolved_alias_events("my_dag", "my_task", "run_1")

        assert urns == []

    def test_import_error_returns_empty(self) -> None:
        import sys

        # Temporarily hide airflow.models.asset to simulate non-Airflow env
        original = sys.modules.get("airflow.models.asset")
        sys.modules["airflow.models.asset"] = None  # type: ignore[assignment]
        try:
            urns = extract_urns_from_resolved_alias_events("my_dag", "my_task", "run_1")
            assert urns == []
        finally:
            if original is None:
                del sys.modules["airflow.models.asset"]
            else:
                sys.modules["airflow.models.asset"] = original


class TestExtractUrnsFromTaskInstanceOutletEvents:
    """Tests for extract_urns_from_task_instance_outlet_events.

    The function lazily imports airflow.sdk.definitions.asset (AssetAlias) and
    airflow.sdk.execution_time.context (OutletEventAccessors), so we inject real
    Python classes via patch.dict(sys.modules, ...) to make isinstance() checks pass.
    """

    # ------------------------------------------------------------------ helpers

    def _make_classes(self):
        """Return real Python AssetAlias and OutletEventAccessors classes.

        Using real classes (not MagicMock) ensures isinstance() works correctly
        inside the function under test.
        """

        class AssetAlias:
            def __init__(self, name: str):
                self.name = name

        class OutletEventAccessors:
            """Minimal stand-in: iterable mapping of key → accessor."""

            def __init__(self, data: dict):
                self._data = data

            def __iter__(self):
                return iter(self._data)

            def __getitem__(self, key: Any) -> Any:
                return self._data[key]

        return AssetAlias, OutletEventAccessors

    def _make_asset_accessor(self, uri: str, AssetAlias_cls: Any) -> Any:
        """Build a mock OutletEventAccessor with one AssetAliasEvent."""

        class Asset:
            def __init__(self, uri: str):
                self.uri = uri

        event = MagicMock()
        event.dest_asset_key.to_asset.return_value = Asset(uri)
        accessor = MagicMock()
        accessor.asset_alias_events = [event]
        return accessor

    def _sys_modules_patch(
        self, AssetAlias_cls: Any, OutletEventAccessors_cls: Any
    ) -> Any:
        import sys

        mock_sdk_asset = MagicMock()
        mock_sdk_asset.AssetAlias = AssetAlias_cls

        mock_context = MagicMock()
        mock_context.OutletEventAccessors = OutletEventAccessors_cls

        return patch.dict(
            sys.modules,
            {
                "airflow.sdk.definitions.asset": mock_sdk_asset,
                "airflow.sdk.execution_time.context": mock_context,
            },
        )

    # ------------------------------------------------------------------ tests

    def test_resolves_alias_to_s3_urn(self) -> None:
        """Primary path: outlet_events stored on _datahub_outlet_events (Airflow 3.1.x patch)."""
        AssetAlias, OutletEventAccessors = self._make_classes()
        alias = AssetAlias("parsed_data")
        accessor = self._make_asset_accessor("s3://my-bucket/data.parquet", AssetAlias)
        outlet_events = OutletEventAccessors({alias: accessor})

        task_instance = MagicMock(spec=["_datahub_outlet_events"])
        task_instance._datahub_outlet_events = outlet_events

        with self._sys_modules_patch(AssetAlias, OutletEventAccessors):
            urns = extract_urns_from_task_instance_outlet_events(task_instance)

        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/data.parquet,PROD)"
        ]

    def test_resolves_alias_via_cached_context_fallback(self) -> None:
        """Fallback path: outlet_events inside _cached_template_context (Airflow 3.2.x native)."""
        AssetAlias, OutletEventAccessors = self._make_classes()
        alias = AssetAlias("parsed_data")
        accessor = self._make_asset_accessor("s3://my-bucket/data.parquet", AssetAlias)
        outlet_events = OutletEventAccessors({alias: accessor})

        # No _datahub_outlet_events, but _cached_template_context is set (Airflow 3.2.x)
        task_instance = MagicMock(spec=["_cached_template_context"])
        task_instance._cached_template_context = {"outlet_events": outlet_events}

        with self._sys_modules_patch(AssetAlias, OutletEventAccessors):
            urns = extract_urns_from_task_instance_outlet_events(task_instance)

        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/data.parquet,PROD)"
        ]

    def test_non_alias_keys_skipped(self) -> None:
        """Non-AssetAlias keys (e.g. plain Asset) should be ignored."""
        AssetAlias, OutletEventAccessors = self._make_classes()

        class Asset:
            def __init__(self, uri: str):
                self.uri = uri

        # Use an Asset as key instead of AssetAlias
        asset_key = Asset("s3://bucket/direct.parquet")
        accessor = MagicMock()
        accessor.asset_alias_events = []
        outlet_events = OutletEventAccessors({asset_key: accessor})

        task_instance = MagicMock(spec=["_datahub_outlet_events"])
        task_instance._datahub_outlet_events = outlet_events

        with self._sys_modules_patch(AssetAlias, OutletEventAccessors):
            urns = extract_urns_from_task_instance_outlet_events(task_instance)

        assert urns == []

    def test_multiple_aliases_each_resolved(self) -> None:
        AssetAlias, OutletEventAccessors = self._make_classes()
        alias_a = AssetAlias("alias_a")
        alias_b = AssetAlias("alias_b")
        accessor_a = self._make_asset_accessor("s3://bucket/a.parquet", AssetAlias)
        accessor_b = self._make_asset_accessor("gs://bucket/b.json", AssetAlias)
        outlet_events = OutletEventAccessors({alias_a: accessor_a, alias_b: accessor_b})

        task_instance = MagicMock(spec=["_datahub_outlet_events"])
        task_instance._datahub_outlet_events = outlet_events

        with self._sys_modules_patch(AssetAlias, OutletEventAccessors):
            urns = extract_urns_from_task_instance_outlet_events(task_instance)

        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/a.parquet,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:gcs,bucket/b.json,PROD)",
        ]

    def test_no_outlet_events_attribute_returns_empty(self) -> None:
        """Neither _datahub_outlet_events nor _cached_template_context is set."""
        AssetAlias, OutletEventAccessors = self._make_classes()
        # spec=[] means no attributes — both lookups return None
        task_instance = MagicMock(spec=[])

        with self._sys_modules_patch(AssetAlias, OutletEventAccessors):
            urns = extract_urns_from_task_instance_outlet_events(task_instance)

        assert urns == []

    def test_missing_outlet_events_key_returns_empty(self) -> None:
        """_cached_template_context exists but has no 'outlet_events' key."""
        AssetAlias, OutletEventAccessors = self._make_classes()
        task_instance = MagicMock(spec=["_cached_template_context"])
        task_instance._cached_template_context = {}  # no 'outlet_events' key

        with self._sys_modules_patch(AssetAlias, OutletEventAccessors):
            urns = extract_urns_from_task_instance_outlet_events(task_instance)

        assert urns == []

    def test_wrong_type_for_outlet_events_returns_empty(self) -> None:
        """outlet_events must be an OutletEventAccessors, not a plain dict."""
        AssetAlias, OutletEventAccessors = self._make_classes()
        task_instance = MagicMock(spec=["_datahub_outlet_events"])
        task_instance._datahub_outlet_events = {"key": "val"}  # wrong type

        with self._sys_modules_patch(AssetAlias, OutletEventAccessors):
            urns = extract_urns_from_task_instance_outlet_events(task_instance)

        assert urns == []

    def test_custom_env(self) -> None:
        AssetAlias, OutletEventAccessors = self._make_classes()
        alias = AssetAlias("alias")
        accessor = self._make_asset_accessor("s3://bucket/data.parquet", AssetAlias)
        outlet_events = OutletEventAccessors({alias: accessor})

        task_instance = MagicMock(spec=["_datahub_outlet_events"])
        task_instance._datahub_outlet_events = outlet_events

        with self._sys_modules_patch(AssetAlias, OutletEventAccessors):
            urns = extract_urns_from_task_instance_outlet_events(
                task_instance, env="DEV"
            )

        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/data.parquet,DEV)"
        ]

    def test_import_error_returns_empty(self) -> None:
        import sys

        original = sys.modules.get("airflow.sdk.definitions.asset")
        sys.modules["airflow.sdk.definitions.asset"] = None  # type: ignore[assignment]
        try:
            task_instance = MagicMock()
            urns = extract_urns_from_task_instance_outlet_events(task_instance)
            assert urns == []
        finally:
            if original is None:
                sys.modules.pop("airflow.sdk.definitions.asset", None)
            else:
                sys.modules["airflow.sdk.definitions.asset"] = original


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

    def test_is_airflow_asset_alias_with_real_class(self) -> None:
        """Test that real Airflow AssetAlias is recognised."""
        try:
            from airflow.sdk import AssetAlias
        except ImportError:
            import pytest

            pytest.skip("airflow.sdk.AssetAlias not available")

        alias = AssetAlias("my_alias")
        assert is_airflow_asset_alias(alias)

    def test_translate_real_airflow_asset_alias(self) -> None:
        """Test translating a real Airflow AssetAlias to a DataHub URN."""
        try:
            from airflow.sdk import AssetAlias
        except ImportError:
            import pytest

            pytest.skip("airflow.sdk.AssetAlias not available")

        alias = AssetAlias("parsed_data")
        urn = translate_airflow_asset_alias_to_urn(alias)
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:airflow,parsed_data,PROD)"

    def test_extract_urns_with_real_asset_alias(self) -> None:
        """Test extract_urns_from_iolets with a real Airflow AssetAlias."""
        try:
            from airflow.sdk import AssetAlias
        except ImportError:
            import pytest

            pytest.skip("airflow.sdk.AssetAlias not available")

        alias = AssetAlias("output_data")
        urns = extract_urns_from_iolets([alias], capture_airflow_assets=True)
        assert urns == ["urn:li:dataset:(urn:li:dataPlatform:airflow,output_data,PROD)"]
