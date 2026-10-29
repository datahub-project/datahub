"""Self-hosted JDBC source connector: table-to-topic matching through transforms,
schema resolution per platform, and platform detection fallbacks."""

from typing import Any, Dict, List, Optional

import pytest

from datahub.ingestion.source.kafka_connect.common import (
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.connector_registry import (
    ConnectorRegistry,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    JDBC_SOURCE_CONNECTOR_CLASS,
    ConfluentJDBCSourceConnector,
)
from tests.unit.kafka_connect.conftest import lineage_map, make_manifest, reported

REGEX_ROUTER = "org.apache.kafka.connect.transforms.RegexRouter"
CONNECTOR = "jdbc-source"
POSTGRES_URL = "jdbc:postgresql://db.example:5432/mydb"


def _jdbc(
    config: Dict[str, str],
    topic_names: List[str],
    tables: Optional[str] = None,
    tasks: Optional[List[Dict[str, Any]]] = None,
) -> ConfluentJDBCSourceConnector:
    """A JdbcSourceConnector as the Connect API reports it: the table list lives
    in the task configs, not the connector config."""
    if tasks is None:
        tasks = [{"id": 0, "config": {"tables": tables}}] if tables is not None else []
    manifest = make_manifest(
        {"connector.class": JDBC_SOURCE_CONNECTOR_CLASS, **config},
        name=CONNECTOR,
        tasks=tasks,
        topic_names=topic_names,
    )
    return ConfluentJDBCSourceConnector(
        manifest, KafkaConnectSourceConfig(), KafkaConnectSourceReport()
    )


class TestTransformOrchestration:
    def test_multiple_tables_matched_to_routed_topics(self) -> None:
        """With several source tables each table's topic is predicted through
        the transform chain and matched against the runtime topics."""
        connector = _jdbc(
            {
                "connection.url": POSTGRES_URL,
                "topic.prefix": "pg_",
                "transforms": "route",
                "transforms.route.type": REGEX_ROUTER,
                "transforms.route.regex": "pg_(.*)",
                "transforms.route.replacement": "clean_$1",
            },
            topic_names=["clean_users", "clean_orders", "unrelated"],
            tables='"public"."users","public"."orders"',
        )

        assert lineage_map(connector) == {
            "clean_users": "mydb.public.users",
            "clean_orders": "mydb.public.orders",
        }

    def test_tables_whose_predicted_topic_is_absent_produce_no_lineage(self) -> None:
        connector = _jdbc(
            {
                "connection.url": POSTGRES_URL,
                "topic.prefix": "pg_",
                "transforms": "route",
                "transforms.route.type": REGEX_ROUTER,
                "transforms.route.regex": "pg_(.*)",
                "transforms.route.replacement": "clean_$1",
            },
            topic_names=["legacy_users", "legacy_orders"],
            tables='"public"."users","public"."orders"',
        )

        assert connector.extract_lineages() == []

    def test_transforms_without_any_table_config_yield_nothing(self) -> None:
        connector = _jdbc(
            {
                "connection.url": POSTGRES_URL,
                "transforms": "route",
                "transforms.route.type": REGEX_ROUTER,
                "transforms.route.regex": "(.*)",
                "transforms.route.replacement": "x_$1",
            },
            topic_names=["x_users"],
        )

        assert connector.extract_lineages() == []

    def test_complex_transform_is_reported_as_fallback(self) -> None:
        """A transform the pipeline cannot predict keeps the direct prefix
        mapping and tells the operator so."""
        connector = _jdbc(
            {
                "connection.url": POSTGRES_URL,
                "topic.prefix": "pg_",
                "transforms": "ts",
                "transforms.ts.type": "org.apache.kafka.connect.transforms.TimestampConverter",
                "transforms.ts.field": "created_at",
            },
            topic_names=["pg_users", "pg_orders"],
            tables='"public"."users","public"."orders"',
        )

        assert lineage_map(connector) == {
            "pg_users": "mydb.public.users",
            "pg_orders": "mydb.public.orders",
        }
        assert connector.report.infos


class TestSchemaResolution:
    def test_two_level_platform_keeps_bare_table_name(self) -> None:
        connector = _jdbc(
            {
                "connection.url": "jdbc:mysql://db.example:3306/shop",
                "topic.prefix": "mysql_",
            },
            topic_names=["mysql_customers"],
            tables="`customers`",
        )

        assert lineage_map(connector) == {"mysql_customers": "shop.customers"}

    def test_three_level_platform_without_schema_excludes_source(self) -> None:
        """The task reported a bare table name; without a schema the postgres
        URN would be wrong, so the lineage keeps the topic but drops the source."""
        connector = _jdbc(
            {"connection.url": POSTGRES_URL, "topic.prefix": "pg_"},
            topic_names=["pg_users"],
            tables='"users"',
        )

        assert lineage_map(connector) == {"pg_users": None}
        assert reported(connector.report.warnings, CONNECTOR)


class TestTopicsFromConfig:
    def test_falls_back_to_config_prefix_when_url_has_no_database(self) -> None:
        connector = _jdbc(
            {
                "connection.url": "jdbc:postgresql://db.example:5432",
                "topic.prefix": "pg_",
                "table.whitelist": "public.users,public.orders",
            },
            topic_names=[],
        )

        assert connector.get_topics_from_config() == ["pg_users", "pg_orders"]

    def test_without_prefix_topic_is_the_table_name(self) -> None:
        """The JDBC source names topics ``<prefix><table>``; the schema is never
        part of the topic."""
        connector = _jdbc(
            {"connection.url": POSTGRES_URL},
            topic_names=[],
            tables='"public"."users"',
        )

        assert connector.get_topics_from_config() == ["users"]


class TestPlatformDetection:
    @pytest.mark.parametrize(
        "url,expected",
        [
            ("jdbc:postgresql://db.example:5432", "postgres"),
            ("jdbc:db2://db.example:50000", "db2"),
            ("jdbc:notadb://db.example:1", "unknown"),
        ],
    )
    def test_platform_from_url_when_parser_cannot_build(
        self, url: str, expected: str
    ) -> None:
        """A URL without a database name makes the parser fail; the platform
        must still be read from the JDBC scheme so the flow is categorised."""
        connector = _jdbc({"connection.url": url}, topic_names=[])

        assert connector.get_platform() == expected

    def test_platform_unknown_without_connection_details(self) -> None:
        connector = _jdbc({"database.hostname": "db.example"}, topic_names=[])

        assert connector.get_platform() == "unknown"


class TestTopicPrefixEdgeCases:
    def test_prefix_without_separator_still_strips_leading_dot(self) -> None:
        connector = _jdbc(
            {"connection.url": POSTGRES_URL, "topic.prefix": "pg"},
            topic_names=["pg.users"],
            tables='"public"."users"',
        )

        assert lineage_map(connector) == {"pg.users": "mydb.public.users"}

    def test_topic_outside_the_prefix_keeps_topic_but_no_source(self) -> None:
        connector = _jdbc(
            {"connection.url": POSTGRES_URL, "topic.prefix": "pg_"},
            topic_names=["pg_users", "audit"],
            tables='"public"."users"',
        )

        assert lineage_map(connector) == {
            "pg_users": "mydb.public.users",
            "audit": None,
        }

    @pytest.mark.parametrize(
        "connector_class,schema,prefix,expected",
        [
            ("MySqlSource", "", "", "users"),
            ("MySqlSource", "", "srv", "srv.users"),
            ("PostgresCdcSource", "public", "", "public.users"),
            ("PostgresCdcSource", "public", "srv", "srv.public.users"),
            (JDBC_SOURCE_CONNECTOR_CLASS, "public", "pg_", "pg_users"),
            ("com.example.Custom", "", "pg_", "pg_users"),
        ],
    )
    def test_original_topic_naming_convention_per_connector_family(
        self, connector_class: str, schema: str, prefix: str, expected: str
    ) -> None:
        """The orchestrator predicts each table's pre-transform topic from the
        connector family's naming rule before running the transform chain."""
        connector = _jdbc({"connection.url": POSTGRES_URL}, topic_names=[])

        assert (
            connector._generate_original_topic_name(
                schema, "users", prefix, connector_class
            )
            == expected
        )


class TestUnifiedFallback:
    """``_extract_lineages_unified`` is the fallback ``extract_lineages`` takes
    when the environment-aware extraction raises."""

    @pytest.mark.xfail(
        strict=True,
        reason="_derive_source_dataset_from_topic rebuilds the table from the topic "
        "remainder without consulting the task table list, so three-level "
        "platforms lose the schema (mydb.users instead of mydb.public.users)",
    )
    def test_config_derived_lineage_keeps_the_schema(self) -> None:
        connector = _jdbc(
            {"connection.url": POSTGRES_URL, "topic.prefix": "pg_"},
            topic_names=["pg_users"],
            tables='"public"."users"',
        )
        parser = connector.get_parser(connector.connector_manifest)

        lineages = connector._extract_lineages_unified(parser)

        assert [lineage.source_dataset for lineage in lineages] == ["mydb.public.users"]

    @pytest.mark.xfail(
        strict=True,
        raises=AttributeError,
        reason="the unified fallback's own except clause calls default_get_lineages, "
        "which re-reads the task configs that raised in the first place, so the "
        "error escapes extract_lineages instead of degrading to no lineage",
    )
    def test_malformed_task_payload_degrades_to_no_lineage(self) -> None:
        connector = _jdbc(
            {"connection.url": POSTGRES_URL, "topic.prefix": "pg_"},
            topic_names=["pg_users"],
            tasks=[{"id": 0, "config": "garbage"}],
        )

        assert connector.extract_lineages() == []


class TestConfluentCloudJdbcSources:
    """ConfluentJDBCSourceConnector carries Confluent Cloud strategies (single
    table mode, topic.prefix inference), but ConnectorRegistry routes every
    CLOUD_JDBC_SOURCE_CLASSES name to DebeziumSourceConnector, so ingestion
    never runs them. These pin what a cloud MySqlSource recipe should yield."""

    _BASE = {
        "database.hostname": "db.example",
        "database.port": "3306",
        "database.dbname": "shop",
    }

    def _registered(
        self, config: Dict[str, str], cluster_topics: List[str]
    ) -> Dict[str, Optional[str]]:
        manifest = make_manifest({**self._BASE, **config}, name="cloud-source")
        connector = ConnectorRegistry.get_connector_for_manifest(
            manifest, KafkaConnectSourceConfig(), KafkaConnectSourceReport()
        )
        assert connector is not None
        connector.all_cluster_topics = cluster_topics
        return lineage_map(connector)

    @pytest.mark.xfail(
        strict=True,
        reason="ConnectorRegistry dispatches MySqlSource to DebeziumSourceConnector, "
        "which has no single-table (kafka.topic + table.name) strategy",
    )
    def test_single_table_mode_maps_configured_table_to_its_topic(self) -> None:
        assert self._registered(
            {
                "connector.class": "MySqlSource",
                "kafka.topic": "shop-users",
                "table.name": "users",
            },
            cluster_topics=["shop-users", "unrelated"],
        ) == {"shop-users": "shop.users"}

    @pytest.mark.xfail(
        strict=True,
        reason="ConnectorRegistry dispatches MySqlSource to DebeziumSourceConnector, "
        "which never infers tables from topic.prefix over the cluster topics",
    )
    def test_topic_prefix_infers_tables_from_cluster_topics(self) -> None:
        assert self._registered(
            {"connector.class": "MySqlSource", "topic.prefix": "mysql_"},
            cluster_topics=["mysql_users", "mysql_orders", "unrelated"],
        ) == {"mysql_users": "shop.users", "mysql_orders": "shop.orders"}

    @pytest.mark.xfail(
        strict=True,
        reason="_extract_lineages_from_prefix_inference maps every cluster topic, "
        "prefixed or not, so unrelated topics become tables of this connector",
    )
    def test_prefix_inference_ignores_topics_outside_the_prefix(self) -> None:
        connector = ConfluentJDBCSourceConnector(
            make_manifest(
                {
                    **self._BASE,
                    "connector.class": "MySqlSource",
                    "topic.prefix": "mysql_",
                }
            ),
            KafkaConnectSourceConfig(),
            KafkaConnectSourceReport(),
        )
        parser = connector.get_parser(connector.connector_manifest)

        lineages = connector._extract_lineages_from_prefix_inference(
            parser, ["mysql_users", "unrelated"]
        )

        assert [lineage.target_dataset for lineage in lineages] == ["mysql_users"]
