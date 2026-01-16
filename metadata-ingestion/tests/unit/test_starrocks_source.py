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
    def test_config_validation(self):
        config = StarRocksConfig.model_validate(_base_config())
        assert config.host_port == "localhost:9030"
        assert config.scheme == "starrocks"
        assert config.include_external_catalogs is True
        assert config.database is None

        config_with_db = StarRocksConfig.model_validate(
            {**_base_config(), "database": "default_catalog.my_db"}
        )
        assert config_with_db.database == "default_catalog.my_db"

        config_with_pattern = StarRocksConfig.model_validate(
            {**_base_config(), "catalog_pattern": {"allow": ["^default_catalog$"]}}
        )
        assert config_with_pattern.catalog_pattern is not None

    def test_sql_alchemy_url(self):
        config = StarRocksConfig.model_validate(_base_config())
        url = config.get_sql_alchemy_url()
        assert "starrocks://" in url
        assert "localhost:9030" in url


class TestStarRocksSourceCatalogDiscovery:
    @patch("datahub.ingestion.source.sql.starrocks.create_engine")
    def test_discover_catalogs_success(self, create_engine_mock):
        execute_mock = create_engine_mock.return_value.connect.return_value.__enter__.return_value.execute
        execute_mock.return_value = [
            ("default_catalog", "Internal", ""),
            ("iceberg", "Iceberg", ""),
        ]

        config = StarRocksConfig.model_validate(_base_config())
        source = StarRocksSource(config, PipelineContext(run_id="test"))

        catalogs = source._discover_catalogs(create_engine_mock.return_value)

        assert len(catalogs) == 2
        assert catalogs[0].name == "default_catalog"
        assert not catalogs[0].is_external
        assert catalogs[1].name == "iceberg"
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
            name="iceberg", catalog_type="Iceberg"
        )

        with patch(
            "datahub.ingestion.source.sql.sql_common.SQLAlchemySource.get_table_properties",
            return_value=(None, {}, None),
        ):
            _, properties, _ = source.get_table_properties(
                MagicMock(), "my_db", "my_table"
            )

        assert properties["catalog"] == "iceberg"
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


class TestStarRocksCatalogContainerDeduplication:
    def test_catalog_container_deduplication(self):
        config = StarRocksConfig.model_validate(_base_config())
        source = _create_source_with_mocked_init(config)

        catalog = StarRocksCatalog(name="default_catalog", catalog_type="Internal")
        source._current_catalog = catalog

        mock_inspector = MagicMock()

        with patch(
            "datahub.ingestion.source.sql.sql_common.SQLAlchemySource.get_database_level_workunits",
            return_value=iter([]),
        ):
            workunits1 = list(
                source.get_database_level_workunits(
                    mock_inspector, "default_catalog.db1"
                )
            )
        assert "default_catalog" in source._catalog_containers_emitted

        with patch(
            "datahub.ingestion.source.sql.sql_common.SQLAlchemySource.get_database_level_workunits",
            return_value=iter([]),
        ):
            workunits2 = list(
                source.get_database_level_workunits(
                    mock_inspector, "default_catalog.db2"
                )
            )

        assert "default_catalog" in source._catalog_containers_emitted
        assert len(source._catalog_containers_emitted) == 1

        catalog_workunits_1 = [
            wu for wu in workunits1 if "containerProperties" in str(wu)
        ]
        catalog_workunits_2 = [
            wu for wu in workunits2 if "containerProperties" in str(wu)
        ]

        # First call generates catalog container, second doesn't
        assert len(catalog_workunits_1) > 0
        assert len(catalog_workunits_2) == 0


class TestStarRocksContainerHierarchy:
    def test_database_container_has_catalog_parent(self):
        config = StarRocksConfig.model_validate(
            {**_base_config(), "platform_instance": "prod", "env": "PROD"}
        )
        source = _create_source_with_mocked_init(config)
        source._current_catalog = StarRocksCatalog(
            name="iceberg_catalog", catalog_type="Iceberg"
        )

        source.domain_registry = None

        workunits = list(
            source.gen_database_containers(database="iceberg_catalog.warehouse")
        )

        assert len(workunits) > 0

        container_props_wu = None
        for wu in workunits:
            if (
                hasattr(wu, "metadata")
                and wu.metadata.aspectName == "containerProperties"
            ):
                container_props_wu = wu
                break
        assert container_props_wu is not None

        container_props = container_props_wu.metadata.aspect
        # "warehouse", not "iceberg_catalog.warehouse"
        assert container_props.name == "warehouse"

        # Find parent reference
        container_wu = None
        for wu in workunits:
            if hasattr(wu, "metadata") and wu.metadata.aspectName == "container":
                container_wu = wu
                break

        assert container_wu is not None
        parent_container = container_wu.metadata.aspect
        parent_urn = parent_container.container
        assert "container:" in parent_urn
