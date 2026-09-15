from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.flink.config import CatalogPlatformDetail
from datahub.ingestion.source.flink.lineage import (
    CatalogTableReference,
    NodeRole,
    PlatformResolver,
)
from datahub.ingestion.source.flink.sql_gateway_client import FlinkSQLGatewayClient


def _mock_sql_client() -> MagicMock:
    return MagicMock(spec=FlinkSQLGatewayClient)


class TestPlatformResolverJdbc:
    """JDBC catalogs: platform resolved at catalog level from base-url."""

    def test_postgres_from_base_url(self) -> None:
        client = _mock_sql_client()
        client.get_catalog_info.return_value = {
            "type": "jdbc",
            "option:base-url": "jdbc:postgresql://host:5432/",
            "option:default-database": "mydb",
        }

        resolver = PlatformResolver(client)
        ref = CatalogTableReference(catalog="pg_cat", database="mydb", table="users")
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is not None
        assert result.platform == "postgres"
        assert result.dataset_name == "mydb.users"
        client.get_table_connector.assert_not_called()

    def test_mysql_from_base_url(self) -> None:
        client = _mock_sql_client()
        client.get_catalog_info.return_value = {
            "type": "jdbc",
            "option:base-url": "jdbc:mysql://host:3306/",
        }

        resolver = PlatformResolver(client)
        ref = CatalogTableReference(
            catalog="mysql_cat", database="mydb", table="orders"
        )
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is not None
        assert result.platform == "mysql"
        assert result.dataset_name == "mydb.orders"

    def test_postgres_schema_prefix_preserved(self) -> None:
        """Postgres tables may have schema prefix: public.users."""
        client = _mock_sql_client()
        client.get_catalog_info.return_value = {
            "type": "jdbc",
            "option:base-url": "jdbc:postgresql://host:5432/",
        }

        resolver = PlatformResolver(client)
        ref = CatalogTableReference(
            catalog="pg_cat", database="mydb", table="public.users"
        )
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is not None
        assert result.platform == "postgres"
        assert result.dataset_name == "mydb.public.users"

    def test_unknown_jdbc_url_returns_none(self) -> None:
        client = _mock_sql_client()
        client.get_catalog_info.return_value = {
            "type": "jdbc",
            "option:base-url": "jdbc:unknown://host:1234/",
        }

        resolver = PlatformResolver(client)
        ref = CatalogTableReference(catalog="unk_cat", database="db", table="t")
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is None


class TestPlatformResolverIceberg:
    """Iceberg catalogs: always platform='iceberg'."""

    def test_iceberg_resolved_at_catalog_level(self) -> None:
        client = _mock_sql_client()
        client.get_catalog_info.return_value = {
            "type": "iceberg",
            "option:catalog-type": "hive",
            "option:uri": "thrift://localhost:9083",
        }

        resolver = PlatformResolver(client)
        ref = CatalogTableReference(catalog="ice_cat", database="my_ns", table="events")
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is not None
        assert result.platform == "iceberg"
        assert result.dataset_name == "my_ns.events"
        client.get_table_connector.assert_not_called()


class TestPlatformResolverPaimon:
    """Paimon catalogs: always platform='paimon'."""

    def test_paimon_resolved_at_catalog_level(self) -> None:
        client = _mock_sql_client()
        client.get_catalog_info.return_value = {
            "type": "paimon",
            "option:warehouse": "hdfs:///warehouse",
        }

        resolver = PlatformResolver(client)
        ref = CatalogTableReference(catalog="pm_cat", database="mydb", table="logs")
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is not None
        assert result.platform == "paimon"
        assert result.dataset_name == "mydb.logs"
        client.get_table_connector.assert_not_called()


class TestPlatformResolverGenericInMemory:
    """generic_in_memory catalogs: per-table resolution via SHOW CREATE TABLE."""

    def test_kafka_table_resolved_with_topic_name(self) -> None:
        client = _mock_sql_client()
        client.get_catalog_info.return_value = {"type": "generic_in_memory"}
        client.get_table_connector.return_value = {
            "connector": "kafka",
            "topic": "orders",
            "properties.bootstrap.servers": "broker:9092",
        }

        resolver = PlatformResolver(client)
        ref = CatalogTableReference(
            catalog="default_catalog", database="default_database", table="orders_tbl"
        )
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is not None
        assert result.platform == "kafka"
        assert result.dataset_name == "orders"
        client.get_table_connector.assert_called_once_with(
            "default_catalog", "default_database", "orders_tbl"
        )

    def test_jdbc_table_from_generic_catalog(self) -> None:
        client = _mock_sql_client()
        client.get_catalog_info.return_value = {"type": "generic_in_memory"}
        client.get_table_connector.return_value = {
            "connector": "jdbc",
            "url": "jdbc:postgresql://host:5432/mydb",
            "table-name": "public.users",
        }

        resolver = PlatformResolver(client)
        ref = CatalogTableReference(
            catalog="default_catalog", database="default_database", table="pg_users"
        )
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is not None
        assert result.platform == "postgres"

    def test_jdbc_table_uses_table_name_property(self) -> None:
        """JDBC connector uses 'table-name' property for dataset_name instead of catalog ref table."""
        client = _mock_sql_client()
        client.get_catalog_info.return_value = {"type": "generic_in_memory"}
        client.get_table_connector.return_value = {
            "connector": "jdbc",
            "url": "jdbc:postgresql://host:5432/mydb",
            "table-name": "public.users",
        }

        resolver = PlatformResolver(client)
        ref = CatalogTableReference(
            catalog="default_catalog",
            database="default_database",
            table="pg_users",
        )
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is not None
        assert result.dataset_name == "default_database.public.users"

    def test_filesystem_table(self) -> None:
        client = _mock_sql_client()
        client.get_catalog_info.return_value = {"type": "generic_in_memory"}
        client.get_table_connector.return_value = {
            "connector": "filesystem",
            "path": "s3://bucket/data/",
        }

        resolver = PlatformResolver(client)
        ref = CatalogTableReference(
            catalog="default_catalog", database="default_database", table="csv_data"
        )
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is not None
        assert result.platform == "hdfs"

    def test_empty_connector_returns_none(self) -> None:
        """Auto-discovered tables (JdbcCatalog reflected) may have no DDL."""
        client = _mock_sql_client()
        client.get_catalog_info.return_value = {"type": "generic_in_memory"}
        client.get_table_connector.return_value = {}

        resolver = PlatformResolver(client)
        ref = CatalogTableReference(
            catalog="default_catalog", database="default_database", table="mystery"
        )
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is None

    def test_unknown_connector_uses_name_as_platform(self) -> None:
        """Unrecognized connector name is used as-is for the platform."""
        client = _mock_sql_client()
        client.get_catalog_info.return_value = {"type": "generic_in_memory"}
        client.get_table_connector.return_value = {"connector": "custom_source"}

        resolver = PlatformResolver(client)
        ref = CatalogTableReference(
            catalog="default_catalog",
            database="default_database",
            table="custom_tbl",
        )
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is not None
        assert result.platform == "custom_source"


class TestPlatformResolverHive:
    """Hive catalogs: mixed connectors, per-table resolution."""

    def test_kafka_table_in_hive_catalog(self) -> None:
        client = _mock_sql_client()
        client.get_catalog_info.return_value = {
            "type": "hive",
            "option:hive-conf-dir": "/opt/hive-conf",
        }
        client.get_table_connector.return_value = {
            "connector": "kafka",
            "topic": "events",
        }

        resolver = PlatformResolver(client)
        ref = CatalogTableReference(catalog="hive_cat", database="mydb", table="events")
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is not None
        assert result.platform == "kafka"
        assert result.dataset_name == "events"

    def test_hive_native_table_in_hive_catalog(self) -> None:
        client = _mock_sql_client()
        client.get_catalog_info.return_value = {"type": "hive"}
        client.get_table_connector.return_value = {
            "connector": "hive",
        }

        resolver = PlatformResolver(client)
        ref = CatalogTableReference(
            catalog="hive_cat", database="mydb", table="dim_users"
        )
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is not None
        assert result.platform == "hive"
        assert result.dataset_name == "mydb.dim_users"


class TestPlatformResolverCaching:
    """Caching behavior: catalog info and table connector results are cached."""

    def test_catalog_info_cached_across_tables(self) -> None:
        client = _mock_sql_client()
        client.get_catalog_info.return_value = {
            "type": "jdbc",
            "option:base-url": "jdbc:postgresql://host:5432/",
        }

        resolver = PlatformResolver(client)
        ref1 = CatalogTableReference(catalog="pg_cat", database="db", table="t1")
        ref2 = CatalogTableReference(catalog="pg_cat", database="db", table="t2")

        resolver.resolve(ref1, NodeRole.SOURCE)
        resolver.resolve(ref2, NodeRole.SOURCE)

        # Catalog info fetched once despite two resolves
        client.get_catalog_info.assert_called_once_with("pg_cat")

    def test_table_connector_cached_for_same_table(self) -> None:
        client = _mock_sql_client()
        client.get_catalog_info.return_value = {"type": "generic_in_memory"}
        client.get_table_connector.return_value = {
            "connector": "kafka",
            "topic": "t",
        }

        resolver = PlatformResolver(client)
        ref = CatalogTableReference(
            catalog="default_catalog", database="default_database", table="t"
        )

        resolver.resolve(ref, NodeRole.SOURCE)
        resolver.resolve(ref, NodeRole.SOURCE)

        # Table connector fetched once despite two resolves
        client.get_table_connector.assert_called_once()


class TestPlatformResolverNoSqlGateway:
    """Without SQL Gateway, all resolution returns None."""

    def test_resolve_returns_none(self) -> None:
        resolver = PlatformResolver(sql_gateway_client=None)
        ref = CatalogTableReference(catalog="cat", database="db", table="t")
        assert resolver.resolve(ref, NodeRole.SOURCE) is None

    def test_empty_catalog_returns_none(self) -> None:
        """SINK_WRITER pattern has no catalog info."""
        client = _mock_sql_client()
        resolver = PlatformResolver(client)
        ref = CatalogTableReference(catalog="", database="", table="my_sink")
        assert resolver.resolve(ref, NodeRole.SOURCE) is None


class TestPlatformResolverBuildContext:
    """build_catalog_context pre-fetches all catalogs."""

    def test_prefetches_all_catalogs(self) -> None:
        client = _mock_sql_client()
        client.get_catalogs.return_value = ["cat_a", "cat_b"]
        client.get_catalog_info.return_value = {"type": "generic_in_memory"}

        resolver = PlatformResolver(client)
        resolver.build_catalog_context()

        assert client.get_catalog_info.call_count == 2

    def test_propagates_exception_on_failure(self) -> None:
        """build_catalog_context raises so source.py's try/except can report it."""
        client = _mock_sql_client()
        client.get_catalogs.side_effect = RuntimeError("connection failed")

        resolver = PlatformResolver(client)
        with pytest.raises(RuntimeError, match="connection failed"):
            resolver.build_catalog_context()


class TestCatalogPlatformMap:
    """catalog_platform_map config takes priority over SQL Gateway auto-detection.

    Use cases:
    - Flink < 1.20: DESCRIBE CATALOG unavailable; SHOW CREATE TABLE returns no
      'connector' property for Iceberg/Paimon catalogs.
    - Override incorrect auto-detection results.
    - Catalogs defined outside the SQL Gateway session.
    """

    def test_iceberg_config_takes_priority_no_sql_gateway_calls(self) -> None:
        """Config platform is returned immediately — SQL Gateway is not called."""
        client = _mock_sql_client()

        resolver = PlatformResolver(
            client,
            catalog_platform_map={
                "ice_catalog": CatalogPlatformDetail(platform="iceberg"),
            },
        )
        ref = CatalogTableReference(
            catalog="ice_catalog", database="lake", table="events"
        )
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is not None
        assert result.platform == "iceberg"
        assert result.dataset_name == "lake.events"
        client.get_catalog_info.assert_not_called()
        client.get_table_connector.assert_not_called()

    def test_paimon_resolved_via_config(self) -> None:
        client = _mock_sql_client()

        resolver = PlatformResolver(
            client,
            catalog_platform_map={
                "pm_catalog": CatalogPlatformDetail(platform="paimon"),
            },
        )
        ref = CatalogTableReference(catalog="pm_catalog", database="mydb", table="logs")
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is not None
        assert result.platform == "paimon"
        assert result.dataset_name == "mydb.logs"
        client.get_catalog_info.assert_not_called()

    def test_config_platform_overrides_sql_gateway(self) -> None:
        """Recipe config takes priority: config says 'iceberg', SQL Gateway says 'kafka'."""
        client = _mock_sql_client()
        client.get_catalog_info.return_value = {"type": "generic_in_memory"}
        client.get_table_connector.return_value = {
            "connector": "kafka",
            "topic": "orders",
        }

        resolver = PlatformResolver(
            client,
            catalog_platform_map={
                "default_catalog": CatalogPlatformDetail(platform="iceberg"),
            },
        )
        ref = CatalogTableReference(
            catalog="default_catalog",
            database="default_database",
            table="orders",
        )
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is not None
        assert result.platform == "iceberg"  # config wins
        client.get_catalog_info.assert_not_called()
        client.get_table_connector.assert_not_called()

    def test_sql_gateway_used_when_no_config_platform(self) -> None:
        """Without config platform, SQL Gateway auto-detection is used."""
        client = _mock_sql_client()
        client.get_catalog_info.side_effect = RuntimeError("not supported")
        client.get_table_connector.return_value = {
            "connector": "kafka",
            "topic": "orders",
        }

        resolver = PlatformResolver(
            client,
            catalog_platform_map={
                # Only platform_instance, no platform override
                "default_catalog": CatalogPlatformDetail(platform_instance="prod"),
            },
        )
        ref = CatalogTableReference(
            catalog="default_catalog",
            database="default_database",
            table="orders",
        )
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is not None
        assert result.platform == "kafka"  # SQL Gateway result

    def test_returns_none_when_no_config_and_no_detection(self) -> None:
        """No config + no auto-detection = unresolvable."""
        client = _mock_sql_client()
        client.get_catalog_info.side_effect = RuntimeError("not supported")
        client.get_table_connector.return_value = {}  # no connector property

        resolver = PlatformResolver(client)  # no catalog_platform_map
        ref = CatalogTableReference(catalog="unknown_catalog", database="db", table="t")
        result = resolver.resolve(ref, NodeRole.SOURCE)

        assert result is None
