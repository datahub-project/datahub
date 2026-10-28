"""Debezium source connector lineage: the runtime-topic fallback when no
table list is configured, DataHub-backed table discovery, EventRouter edge
cases, and platform-specific naming."""

import sys
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, cast
from unittest.mock import patch

import jpype
import jpype.imports
import pytest

from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    DebeziumSourceConnector,
)
from datahub.sql_parsing.schema_resolver import SchemaResolverInterface

POSTGRES = "io.debezium.connector.postgresql.PostgresConnector"
MYSQL = "io.debezium.connector.mysql.MySqlConnector"
SQLSERVER = "io.debezium.connector.sqlserver.SqlServerConnector"
REGEX_ROUTER = "org.apache.kafka.connect.transforms.RegexRouter"
EVENT_ROUTER = "io.debezium.transforms.outbox.EventRouter"


@pytest.fixture(scope="session", autouse=True)
def ensure_jvm_started() -> None:
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath())


class _StubSchemaResolver(SchemaResolverInterface):
    """The slice of SchemaResolver the connector reads: the cached URN set."""

    platform_instance: Optional[str] = None
    graph = None
    env = "PROD"

    def __init__(
        self,
        platform: str,
        urns: Iterable[str] = (),
        error: Optional[Exception] = None,
    ) -> None:
        self._platform = platform
        self._urns: Set[str] = set(urns)
        self._error = error

    @property
    def platform(self) -> str:
        return self._platform

    def includes_temp_tables(self) -> bool:
        return False

    def get_urns(self) -> Set[str]:
        if self._error is not None:
            raise self._error
        return self._urns

    def resolve_table(self, table: Any) -> Tuple[str, Optional[Dict[str, str]]]:
        return (
            f"urn:li:dataset:(urn:li:dataPlatform:{self._platform},{table.table},PROD)",
            None,
        )

    def get_urn_for_table(self, table: Any) -> str:
        return (
            f"urn:li:dataset:(urn:li:dataPlatform:{self._platform},{table.table},PROD)"
        )


def _urn(platform: str, name: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},PROD)"


def _debezium(
    config: Dict[str, str],
    topic_names: Optional[List[str]] = None,
    schema_resolver: Optional[SchemaResolverInterface] = None,
    cluster_topics: Optional[List[str]] = None,
) -> DebeziumSourceConnector:
    manifest = ConnectorManifest(
        name="cdc",
        type="source",
        config=config,
        tasks=[],
        topic_names=topic_names or [],
    )
    source_config = KafkaConnectSourceConfig(
        use_schema_resolver=schema_resolver is not None
    )
    connector = DebeziumSourceConnector(
        manifest, source_config, KafkaConnectSourceReport()
    )
    connector.schema_resolver = cast(Any, schema_resolver)
    connector.all_cluster_topics = cluster_topics
    return connector


def _mapping(connector: DebeziumSourceConnector) -> Dict[str, Optional[str]]:
    return {
        lineage.target_dataset: lineage.source_dataset
        for lineage in connector.extract_lineages()
    }


class TestRuntimeTopicFallback:
    """No table.include.list and no SchemaResolver: tables are read back from the
    topics the connector is actually producing."""

    def test_postgres_topics_map_back_to_schema_tables(self) -> None:
        connector = _debezium(
            {
                "connector.class": POSTGRES,
                "topic.prefix": "srv",
                "database.dbname": "mydb",
            },
            topic_names=[
                "srv.public.users",
                "srv.public.orders",
                "srv.schema-changes",
                "dbhistory.srv",
                "srv.transaction",
                "heartbeat",
            ],
        )

        assert _mapping(connector) == {
            "srv.public.users": "mydb.public.users",
            "srv.public.orders": "mydb.public.orders",
        }

    def test_sqlserver_topics_carry_database_and_filter_other_databases(
        self,
    ) -> None:
        connector = _debezium(
            {
                "connector.class": SQLSERVER,
                "database.server.name": "srv",
                "database.names": "salesdb",
            },
            topic_names=[
                "srv.salesdb.dbo.orders",
                "srv.otherdb.dbo.orders",
                "srv.salesdb.legacy",
                "srv.orphan",
            ],
        )

        assert _mapping(connector) == {
            "srv.salesdb.dbo.orders": "salesdb.dbo.orders",
            "srv.salesdb.legacy": "salesdb.legacy",
        }

    def test_mysql_topics_without_database_use_topic_remainder(self) -> None:
        connector = _debezium(
            {"connector.class": MYSQL, "topic.prefix": "srv"},
            topic_names=["srv.inventory.customers", "inventory.audit"],
        )

        assert _mapping(connector) == {
            "srv.inventory.customers": "inventory.customers",
            "inventory.audit": "inventory.audit",
        }

    def test_topics_not_under_the_prefix_are_parsed_best_effort(self) -> None:
        connector = _debezium(
            {"connector.class": SQLSERVER, "topic.prefix": "srv"},
            topic_names=["salesdb.dbo.orders"],
        )

        assert _mapping(connector) == {"salesdb.dbo.orders": "salesdb.dbo.orders"}

    def test_schema_level_topics_without_prefix_assume_schema_table(self) -> None:
        connector = _debezium(
            {
                "connector.class": POSTGRES,
                "topic.prefix": "srv",
                "database.dbname": "mydb",
            },
            topic_names=["public.archive"],
        )

        assert _mapping(connector) == {"public.archive": "mydb.public.archive"}

    def test_only_internal_topics_yields_no_lineage(self) -> None:
        connector = _debezium(
            {"connector.class": POSTGRES, "topic.prefix": "srv"},
            topic_names=["srv.schema-changes", "__debezium-heartbeat.srv"],
        )

        assert connector.extract_lineages() == []

    def test_no_topics_and_no_table_list_yields_no_lineage(self) -> None:
        connector = _debezium({"connector.class": POSTGRES, "topic.prefix": "srv"})

        assert connector.extract_lineages() == []


class TestDataHubTableDiscovery:
    def test_tables_discovered_for_configured_database(self) -> None:
        resolver = _StubSchemaResolver(
            "postgres",
            [
                _urn("postgres", "mydb.public.users"),
                _urn("postgres", "mydb.public.orders"),
                _urn("postgres", "otherdb.public.users"),
                "not-a-urn",
            ],
        )
        connector = _debezium(
            {
                "connector.class": POSTGRES,
                "topic.prefix": "srv",
                "database.dbname": "mydb",
            },
            schema_resolver=resolver,
        )

        assert _mapping(connector) == {
            "srv.public.users": "mydb.public.users",
            "srv.public.orders": "mydb.public.orders",
        }

    def test_discovery_needs_a_database_name(self) -> None:
        resolver = _StubSchemaResolver("mysql", [_urn("mysql", "shop.customers")])
        connector = _debezium(
            {"connector.class": MYSQL, "topic.prefix": "srv"},
            schema_resolver=resolver,
        )

        assert connector.extract_lineages() == []
        assert connector.get_topics_from_config() == []

    def test_datahub_failure_during_discovery_yields_no_lineage(self) -> None:
        connector = _debezium(
            {
                "connector.class": POSTGRES,
                "topic.prefix": "srv",
                "database.dbname": "mydb",
            },
            schema_resolver=_StubSchemaResolver(
                "postgres", error=RuntimeError("datahub timed out")
            ),
        )

        assert connector.extract_lineages() == []

    def test_empty_datahub_cache_yields_no_lineage(self) -> None:
        connector = _debezium(
            {
                "connector.class": POSTGRES,
                "topic.prefix": "srv",
                "database.dbname": "mydb",
            },
            schema_resolver=_StubSchemaResolver("postgres"),
        )

        assert connector.extract_lineages() == []

    def test_discovered_tables_filtered_to_live_topics(self) -> None:
        resolver = _StubSchemaResolver(
            "postgres",
            [
                _urn("postgres", "mydb.public.users"),
                _urn("postgres", "mydb.public.tmp"),
            ],
        )
        connector = _debezium(
            {
                "connector.class": POSTGRES,
                "topic.prefix": "srv",
                "database.dbname": "mydb",
            },
            topic_names=["srv.public.users"],
            schema_resolver=resolver,
        )

        assert _mapping(connector) == {"srv.public.users": "mydb.public.users"}

    def test_schema_exclude_list_can_remove_every_discovered_table(self) -> None:
        resolver = _StubSchemaResolver(
            "postgres",
            [_urn("postgres", "mydb.audit.log"), _urn("postgres", "mydb.audit.events")],
        )
        connector = _debezium(
            {
                "connector.class": POSTGRES,
                "topic.prefix": "srv",
                "database.dbname": "mydb",
                "schema.exclude.list": "audit",
            },
            schema_resolver=resolver,
        )

        assert connector.get_topics_from_config() == []

    def test_schema_exclude_list_keeps_tables_without_a_schema(self) -> None:
        resolver = _StubSchemaResolver(
            "postgres",
            [_urn("postgres", "mydb.audit.log"), _urn("postgres", "mydb.flat_table")],
        )
        connector = _debezium(
            {
                "connector.class": POSTGRES,
                "topic.prefix": "srv",
                "database.dbname": "mydb",
                "schema.exclude.list": "audit",
            },
            schema_resolver=resolver,
        )

        assert connector.get_topics_from_config() == ["srv.flat_table"]

    def test_blank_table_list_yields_no_lineage(self) -> None:
        connector = _debezium(
            {
                "connector.class": POSTGRES,
                "topic.prefix": "srv",
                "database.dbname": "mydb",
                "table.include.list": " ",
            }
        )

        assert connector.extract_lineages() == []

    def test_table_exclude_list_can_remove_every_discovered_table(self) -> None:
        resolver = _StubSchemaResolver(
            "postgres", [_urn("postgres", "mydb.public.tmp_users")]
        )
        connector = _debezium(
            {
                "connector.class": POSTGRES,
                "topic.prefix": "srv",
                "database.dbname": "mydb",
                "table.exclude.list": "public\\.tmp_.*",
            },
            schema_resolver=resolver,
        )

        assert connector.get_topics_from_config() == []


class TestPatternExpansionAgainstDataHub:
    def _connector(self, resolver: SchemaResolverInterface) -> DebeziumSourceConnector:
        return _debezium(
            {
                "connector.class": POSTGRES,
                "topic.prefix": "srv",
                "database.dbname": "mydb",
                "table.include.list": "public\\..*",
            },
            schema_resolver=resolver,
        )

    def test_connection_failure_is_a_reported_failure(self) -> None:
        connector = self._connector(
            _StubSchemaResolver("postgres", error=ConnectionError("unreachable"))
        )

        assert (
            connector._query_tables_from_datahub("public\\..*", "postgres", "mydb")
            == []
        )
        assert len(connector.report.failures) == 1

    def test_other_errors_degrade_to_no_expansion(self) -> None:
        connector = self._connector(
            _StubSchemaResolver("postgres", error=RuntimeError("boom"))
        )

        assert (
            connector._query_tables_from_datahub("public\\..*", "postgres", "mydb")
            == []
        )
        assert connector.report.failures == []

    def test_python_regex_used_when_java_regex_unavailable(self) -> None:
        resolver = _StubSchemaResolver(
            "postgres",
            [
                _urn("postgres", "mydb.public.users"),
                _urn("postgres", "mydb.audit.log"),
                "not-a-urn",
            ],
        )
        connector = self._connector(resolver)

        with patch.dict(sys.modules, {"java.util.regex": None}):
            tables = connector._query_tables_from_datahub(
                "public\\..*", "postgres", "mydb"
            )

        assert tables == ["mydb.public.users"]


class TestEventRouter:
    def _config(self, **extra: str) -> Dict[str, str]:
        return {
            "connector.class": MYSQL,
            "topic.prefix": "srv",
            "transforms": "outbox",
            "transforms.outbox.type": EVENT_ROUTER,
            **extra,
        }

    def test_without_table_list_nothing_can_be_attributed(self) -> None:
        connector = _debezium(self._config(), cluster_topics=["outbox.event.orders"])

        assert connector.extract_lineages() == []

    def test_literal_regex_router_replacement_scopes_cluster_topics(self) -> None:
        connector = _debezium(
            self._config(
                **{
                    "table.include.list": "shop.outbox",
                    "transforms": "outbox,route",
                    "transforms.route.type": REGEX_ROUTER,
                    "transforms.route.regex": ".*",
                    "transforms.route.replacement": "shop.events",
                }
            ),
            cluster_topics=["shop.events", "shop.events.v2", "billing.events"],
        )

        assert _mapping(connector) == {
            "shop.events": "shop.outbox",
            "shop.events.v2": "shop.outbox",
        }

    def test_without_regex_router_every_cluster_topic_is_a_candidate(self) -> None:
        connector = _debezium(
            self._config(**{"table.include.list": "shop.outbox"}),
            cluster_topics=["a", "b"],
        )

        assert set(_mapping(connector)) == {"a", "b"}


class TestPlatformSpecificNaming:
    def test_v2_cloud_connector_ignores_database_server_name(self) -> None:
        connector = _debezium(
            {
                "connector.class": "PostgresCdcSourceV2",
                "topic.prefix": "v2prefix",
                "database.server.name": "legacy",
                "database.dbname": "mydb",
                "table.include.list": "public.users",
            }
        )

        assert _mapping(connector) == {"v2prefix.public.users": "mydb.public.users"}

    def test_unknown_transform_types_leave_derived_topics_alone(self) -> None:
        connector = _debezium(
            {
                "connector.class": POSTGRES,
                "topic.prefix": "srv",
                "database.dbname": "mydb",
                "table.include.list": "public.users",
                "transforms": "custom",
                "transforms.custom.type": "com.example.smt.Custom",
            }
        )

        assert _mapping(connector) == {"srv.public.users": "mydb.public.users"}

    def test_vitess_uses_keyspace_as_database(self) -> None:
        connector = _debezium(
            {
                "connector.class": "io.debezium.connector.vitess.VitessConnector",
                "vitess.keyspace": "commerce",
            }
        )

        assert connector.get_parser(connector.connector_manifest).database_name == (
            "commerce"
        )

    def test_sqlserver_with_several_databases_is_reported_not_raised(self) -> None:
        connector = _debezium(
            {
                "connector.class": SQLSERVER,
                "topic.prefix": "srv",
                "database.names": "sales,billing",
                "table.include.list": "dbo.orders",
            }
        )

        assert connector.extract_lineages() == []
        assert len(connector.report.warnings) == 1

    def test_topics_without_a_prefix_are_bare_table_names(self) -> None:
        postgres = _debezium(
            {
                "connector.class": POSTGRES,
                "database.dbname": "mydb",
                "table.include.list": "public.users",
            }
        )
        sqlserver = _debezium(
            {
                "connector.class": SQLSERVER,
                "database.names": "salesdb",
                "table.include.list": "dbo.orders",
            }
        )

        assert _mapping(postgres) == {"public.users": "mydb.public.users"}
        assert _mapping(sqlserver) == {"salesdb.dbo.orders": "salesdb.dbo.orders"}
