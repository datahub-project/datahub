"""Debezium source connector lineage: the runtime-topic fallback when no
table list is configured, DataHub-backed table discovery, EventRouter edge
cases, and platform-specific naming."""

import sys
from typing import Dict, List, Optional
from unittest.mock import patch

import pytest

from datahub.ingestion.source.kafka_connect.common import (
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    DebeziumSourceConnector,
)
from datahub.sql_parsing.schema_resolver import SchemaResolverInterface
from tests.unit.kafka_connect.conftest import (
    StubSchemaResolver,
    attach_resolver,
    dataset_urn,
    lineage_map,
    make_manifest,
    reported,
)

POSTGRES = "io.debezium.connector.postgresql.PostgresConnector"
MYSQL = "io.debezium.connector.mysql.MySqlConnector"
SQLSERVER = "io.debezium.connector.sqlserver.SqlServerConnector"
REGEX_ROUTER = "org.apache.kafka.connect.transforms.RegexRouter"
EVENT_ROUTER = "io.debezium.transforms.outbox.EventRouter"
CONNECTOR = "cdc"


def _debezium(
    config: Dict[str, str],
    topic_names: Optional[List[str]] = None,
    schema_resolver: Optional[SchemaResolverInterface] = None,
    cluster_topics: Optional[List[str]] = None,
) -> DebeziumSourceConnector:
    """``topic_names`` is what the self-hosted ``/connectors/{name}/topics`` API
    reports; ``cluster_topics`` is the Confluent Cloud cluster-wide list."""
    connector = DebeziumSourceConnector(
        make_manifest(config, name=CONNECTOR, topic_names=topic_names),
        KafkaConnectSourceConfig(use_schema_resolver=schema_resolver is not None),
        KafkaConnectSourceReport(),
    )
    attach_resolver(connector, schema_resolver)
    connector.all_cluster_topics = cluster_topics
    return connector


class TestRuntimeTopicFallback:
    """No table.include.list and no SchemaResolver: tables are read back from the
    topics the connector is actually producing."""

    def test_postgres_topics_map_back_to_schema_tables(self) -> None:
        """Internal Debezium topics (schema history, transaction metadata,
        heartbeat) must not become tables."""
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

        assert lineage_map(connector) == {
            "srv.public.users": "mydb.public.users",
            "srv.public.orders": "mydb.public.orders",
        }

    def test_sqlserver_topics_carry_database_and_filter_other_databases(
        self,
    ) -> None:
        """SQL Server topics are {server}.{database}.{schema}.{table}; a topic
        from a database other than the configured one is not this connector's."""
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

        assert lineage_map(connector) == {
            "srv.salesdb.dbo.orders": "salesdb.dbo.orders",
            "srv.salesdb.legacy": "salesdb.legacy",
        }

    def test_mysql_topics_without_database_use_topic_remainder(self) -> None:
        connector = _debezium(
            {"connector.class": MYSQL, "topic.prefix": "srv"},
            topic_names=["srv.inventory.customers", "inventory.audit"],
        )

        assert lineage_map(connector) == {
            "srv.inventory.customers": "inventory.customers",
            "inventory.audit": "inventory.audit",
        }

    def test_topics_not_under_the_prefix_are_parsed_best_effort(self) -> None:
        """A RegexRouter can strip the server prefix, and the runtime API only
        lists this connector's topics, so a topic outside the prefix is still
        attributed to it."""
        connector = _debezium(
            {"connector.class": SQLSERVER, "topic.prefix": "srv"},
            topic_names=["salesdb.dbo.orders"],
        )

        assert lineage_map(connector) == {"salesdb.dbo.orders": "salesdb.dbo.orders"}

    @pytest.mark.xfail(
        strict=True,
        reason="_extract_lineages_from_topics applies the configured-database filter "
        "only on the server-name branch; the best-effort branch for topics outside "
        "the prefix attributes any database to this connector",
    )
    def test_topics_outside_the_prefix_still_honour_the_configured_database(
        self,
    ) -> None:
        connector = _debezium(
            {
                "connector.class": SQLSERVER,
                "database.server.name": "srv",
                "database.names": "salesdb",
            },
            topic_names=["otherdb.dbo.orders"],
        )

        assert connector.extract_lineages() == []

    def test_schema_level_topics_without_prefix_assume_schema_table(self) -> None:
        connector = _debezium(
            {
                "connector.class": POSTGRES,
                "topic.prefix": "srv",
                "database.dbname": "mydb",
            },
            topic_names=["public.archive"],
        )

        assert lineage_map(connector) == {"public.archive": "mydb.public.archive"}

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
    """No table.include.list but a SchemaResolver: the tables are the datasets
    DataHub knows under the configured database."""

    def test_tables_discovered_for_configured_database(self) -> None:
        resolver = StubSchemaResolver(
            "postgres",
            [
                dataset_urn("postgres", "mydb.public.users"),
                dataset_urn("postgres", "mydb.public.orders"),
                dataset_urn("postgres", "otherdb.public.users"),
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

        assert lineage_map(connector) == {
            "srv.public.users": "mydb.public.users",
            "srv.public.orders": "mydb.public.orders",
        }

    def test_discovery_needs_a_database_name(self) -> None:
        resolver = StubSchemaResolver("mysql", [dataset_urn("mysql", "shop.customers")])
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
            schema_resolver=StubSchemaResolver(
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
            schema_resolver=StubSchemaResolver("postgres"),
        )

        assert connector.extract_lineages() == []

    def test_discovered_tables_filtered_to_live_topics(self) -> None:
        """When the runtime topic list is also available, a discovered table
        whose topic is not being produced is dropped."""
        resolver = StubSchemaResolver(
            "postgres",
            [
                dataset_urn("postgres", "mydb.public.users"),
                dataset_urn("postgres", "mydb.public.tmp"),
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

        assert lineage_map(connector) == {"srv.public.users": "mydb.public.users"}

    def test_schema_exclude_list_can_remove_every_discovered_table(self) -> None:
        resolver = StubSchemaResolver(
            "postgres",
            [
                dataset_urn("postgres", "mydb.audit.log"),
                dataset_urn("postgres", "mydb.audit.events"),
            ],
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
        """A two-part dataset name has no schema segment to match, so a schema
        filter cannot exclude it."""
        resolver = StubSchemaResolver(
            "postgres",
            [
                dataset_urn("postgres", "mydb.audit.log"),
                dataset_urn("postgres", "mydb.flat_table"),
            ],
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
        resolver = StubSchemaResolver(
            "postgres", [dataset_urn("postgres", "mydb.public.tmp_users")]
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
            StubSchemaResolver("postgres", error=ConnectionError("unreachable"))
        )

        assert (
            connector._query_tables_from_datahub("public\\..*", "postgres", "mydb")
            == []
        )
        assert reported(connector.report.failures, CONNECTOR)

    def test_other_errors_degrade_to_no_expansion(self) -> None:
        connector = self._connector(
            StubSchemaResolver("postgres", error=RuntimeError("boom"))
        )

        assert (
            connector._query_tables_from_datahub("public\\..*", "postgres", "mydb")
            == []
        )
        assert connector.report.failures == []

    def test_python_regex_used_when_java_regex_unavailable(self) -> None:
        resolver = StubSchemaResolver(
            "postgres",
            [
                dataset_urn("postgres", "mydb.public.users"),
                dataset_urn("postgres", "mydb.audit.log"),
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
    """EventRouter routes by record content, so the produced topics can only
    be found in the live cluster list, scoped by any literal RegexRouter."""

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

        assert lineage_map(connector) == {
            "shop.events": "shop.outbox",
            "shop.events.v2": "shop.outbox",
        }

    def test_without_regex_router_every_cluster_topic_is_a_candidate(self) -> None:
        connector = _debezium(
            self._config(**{"table.include.list": "shop.outbox"}),
            cluster_topics=["a", "b"],
        )

        assert set(lineage_map(connector)) == {"a", "b"}


class TestPlatformSpecificNaming:
    def test_v2_cloud_connector_ignores_database_server_name(self) -> None:
        """The V2 Confluent Cloud CDC connectors only read topic.prefix; a stale
        database.server.name from a V1 recipe must not win."""
        connector = _debezium(
            {
                "connector.class": "PostgresCdcSourceV2",
                "topic.prefix": "v2prefix",
                "database.server.name": "legacy",
                "database.dbname": "mydb",
                "table.include.list": "public.users",
            }
        )

        assert lineage_map(connector) == {"v2prefix.public.users": "mydb.public.users"}

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

        assert lineage_map(connector) == {"srv.public.users": "mydb.public.users"}

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
        """database.names with more than one entry is ambiguous for naming; the
        connector must surface that on the report instead of failing the run."""
        connector = _debezium(
            {
                "connector.class": SQLSERVER,
                "topic.prefix": "srv",
                "database.names": "sales,billing",
                "table.include.list": "dbo.orders",
            }
        )

        assert connector.extract_lineages() == []
        assert reported(connector.report.warnings, CONNECTOR)

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

        assert lineage_map(postgres) == {"public.users": "mydb.public.users"}
        assert lineage_map(sqlserver) == {"salesdb.dbo.orders": "salesdb.dbo.orders"}
