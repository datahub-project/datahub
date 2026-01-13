from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.starrocks import (
    StarRocksCatalog,
    StarRocksConfig,
    StarRocksSource,
)


def _base_config() -> Dict[str, Any]:
    return {
        "host_port": "localhost:9030",
        "username": "root",
        "password": "",
    }


def _create_source_with_mocked_init(config: StarRocksConfig) -> StarRocksSource:
    """Create a StarRocksSource with mocked parent __init__ for unit testing."""
    with patch(
        "datahub.ingestion.source.sql.sql_common.SQLAlchemySource.__init__",
        return_value=None,
    ):
        source = object.__new__(StarRocksSource)
        source.config = config
        source.platform = "starrocks"
        source._current_catalog = None
        source._catalog_containers_emitted = set()
        return source


class TestStarRocksConfig:
    def test_default_config(self):
        config = StarRocksConfig.model_validate(_base_config())
        assert config.host_port == "localhost:9030"
        assert config.scheme == "starrocks"
        assert config.include_external_catalogs
        assert config.database is None

    def test_config_with_database(self):
        config = StarRocksConfig.model_validate(
            {**_base_config(), "database": "default_catalog.my_db"}
        )
        assert config.database == "default_catalog.my_db"

    def test_config_catalog_pattern(self):
        config = StarRocksConfig.model_validate(
            {**_base_config(), "catalog_pattern": {"allow": ["default_catalog"]}}
        )
        assert config.catalog_pattern.allowed("default_catalog")
        assert not config.catalog_pattern.allowed("other_catalog")

    def test_sql_alchemy_url(self):
        config = StarRocksConfig.model_validate(_base_config())
        url = config.get_sql_alchemy_url()
        assert "starrocks://" in url
        assert "localhost:9030" in url


class TestStarRocksCatalog:
    def test_internal_catalog(self):
        catalog = StarRocksCatalog(name="default_catalog", catalog_type="Internal")
        assert not catalog.is_external

    def test_external_catalog(self):
        catalog = StarRocksCatalog(name="iceberg_catalog", catalog_type="Iceberg")
        assert catalog.is_external

    def test_internal_case_insensitive(self):
        catalog = StarRocksCatalog(name="test", catalog_type="INTERNAL")
        assert not catalog.is_external


class TestStarRocksSourceCatalogDiscovery:
    @patch("datahub.ingestion.source.sql.starrocks.create_engine")
    def test_discover_catalogs_success(self, create_engine_mock):
        execute_mock = create_engine_mock.return_value.connect.return_value.__enter__.return_value.execute
        execute_mock.return_value = [
            ("default_catalog", "Internal", ""),
            ("iceberg_aws", "Iceberg", ""),
        ]

        config = StarRocksConfig.model_validate(_base_config())
        source = StarRocksSource(config, PipelineContext(run_id="test"))

        catalogs = source._discover_catalogs(create_engine_mock.return_value)

        assert len(catalogs) == 2
        assert catalogs[0].name == "default_catalog"
        assert not catalogs[0].is_external
        assert catalogs[1].name == "iceberg_aws"
        assert catalogs[1].is_external

    @patch("datahub.ingestion.source.sql.starrocks.create_engine")
    def test_discover_catalogs_fallback_on_error(self, create_engine_mock):
        execute_mock = create_engine_mock.return_value.connect.return_value.__enter__.return_value.execute
        execute_mock.side_effect = Exception("SHOW CATALOGS failed")

        config = StarRocksConfig.model_validate(_base_config())
        source = StarRocksSource(config, PipelineContext(run_id="test"))

        catalogs = source._discover_catalogs(create_engine_mock.return_value)

        assert len(catalogs) == 1
        assert catalogs[0].name == "default_catalog"


class TestStarRocksSourceDatabaseDiscovery:
    @patch("datahub.ingestion.source.sql.starrocks.create_engine")
    def test_discover_databases_success(self, create_engine_mock):
        execute_mock = create_engine_mock.return_value.connect.return_value.__enter__.return_value.execute
        execute_mock.side_effect = [
            None,  # SET CATALOG
            [("db1",), ("db2",)],  # SHOW DATABASES
        ]

        config = StarRocksConfig.model_validate(_base_config())
        source = StarRocksSource(config, PipelineContext(run_id="test"))

        catalog = StarRocksCatalog(name="default_catalog", catalog_type="Internal")
        databases = source._discover_databases(create_engine_mock.return_value, catalog)

        assert databases == ["db1", "db2"]

    @patch("datahub.ingestion.source.sql.starrocks.create_engine")
    def test_discover_databases_error(self, create_engine_mock):
        execute_mock = create_engine_mock.return_value.connect.return_value.__enter__.return_value.execute
        execute_mock.side_effect = Exception("Connection failed")

        config = StarRocksConfig.model_validate(_base_config())
        source = StarRocksSource(config, PipelineContext(run_id="test"))

        catalog = StarRocksCatalog(name="bad_catalog", catalog_type="Internal")
        databases = source._discover_databases(create_engine_mock.return_value, catalog)

        assert databases == []


class TestStarRocksSourceIdentifiers:
    @pytest.fixture
    def source(self):
        config = StarRocksConfig.model_validate(_base_config())
        source = _create_source_with_mocked_init(config)
        source._current_catalog = StarRocksCatalog(
            name="default_catalog", catalog_type="Internal"
        )
        return source

    def test_get_identifier(self, source):
        identifier = source.get_identifier(
            schema="my_db", entity="my_table", inspector=MagicMock()
        )
        assert identifier == "default_catalog.my_db.my_table"

    def test_get_identifier_no_current_catalog(self, source):
        source._current_catalog = None
        identifier = source.get_identifier(
            schema="my_db", entity="my_table", inspector=MagicMock()
        )
        assert identifier == "default_catalog.my_db.my_table"

    def test_get_db_name(self, source):
        mock_inspector = MagicMock()
        mock_inspector.engine.url.database = "default_catalog.my_db"
        assert source.get_db_name(mock_inspector) == "default_catalog.my_db"

    def test_get_db_schema(self, source):
        catalog, database = source.get_db_schema("default_catalog.my_db.my_table")
        assert catalog == "default_catalog"
        assert database == "my_db"

    def test_get_db_schema_single_part(self, source):
        catalog, database = source.get_db_schema("single_identifier")
        assert catalog is None
        assert database == "single_identifier"


class TestStarRocksSourceContainers:
    @pytest.fixture
    def source(self):
        config = StarRocksConfig.model_validate(
            {**_base_config(), "platform_instance": "my_instance", "env": "PROD"}
        )
        return _create_source_with_mocked_init(config)

    def test_gen_catalog_key(self, source):
        catalog_key = source.gen_catalog_key("default_catalog")
        assert catalog_key.catalog == "default_catalog"
        assert catalog_key.platform == "starrocks"
        assert catalog_key.instance == "my_instance"

    def test_gen_schema_containers_returns_empty(self, source):
        """StarRocks uses two-tier containers (catalog -> database), not three."""
        result = list(source.gen_schema_containers("my_db", "default_catalog.my_db"))
        assert result == []

    def test_get_allowed_schemas(self, source):
        schemas = list(source.get_allowed_schemas(MagicMock(), "default_catalog.my_db"))
        assert schemas == ["my_db"]


class TestStarRocksSourceTableProperties:
    def test_get_table_properties_adds_catalog_info(self):
        config = StarRocksConfig.model_validate(_base_config())
        source = _create_source_with_mocked_init(config)
        source._current_catalog = StarRocksCatalog(
            name="iceberg_aws", catalog_type="Iceberg"
        )

        with patch(
            "datahub.ingestion.source.sql.sql_common.SQLAlchemySource.get_table_properties",
            return_value=(None, {}, None),
        ):
            _, properties, _ = source.get_table_properties(
                MagicMock(), "my_db", "my_table"
            )

        assert properties["catalog"] == "iceberg_aws"
        assert properties["catalog_type"] == "Iceberg"


@pytest.mark.parametrize(
    "catalog_type,expected_external",
    [
        ("Internal", False),
        ("hive", True),
        ("Iceberg", True),
        ("jdbc", True),
    ],
)
def test_catalog_type_detection(catalog_type, expected_external):
    catalog = StarRocksCatalog(name="test", catalog_type=catalog_type)
    assert catalog.is_external == expected_external
