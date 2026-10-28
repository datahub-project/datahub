"""Self-hosted JDBC source connector: table-to-topic matching through transforms,
schema resolution per platform, and platform detection fallbacks."""

from typing import Any, Dict, List, Optional

import jpype
import jpype.imports
import pytest

from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    JDBC_SOURCE_CONNECTOR_CLASS,
    ConfluentJDBCSourceConnector,
)

REGEX_ROUTER = "org.apache.kafka.connect.transforms.RegexRouter"


@pytest.fixture(scope="session", autouse=True)
def ensure_jvm_started() -> None:
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath())


def _jdbc(
    config: Dict[str, str],
    topic_names: List[str],
    tables: Optional[str] = None,
) -> ConfluentJDBCSourceConnector:
    """A JdbcSourceConnector as the Connect API reports it: the table list lives
    in the task configs, not the connector config."""
    full_config = {"connector.class": JDBC_SOURCE_CONNECTOR_CLASS, **config}
    tasks: List[Dict[str, Any]] = (
        [{"id": 0, "config": {"tables": tables}}] if tables is not None else []
    )
    manifest = ConnectorManifest(
        name="jdbc-source",
        type="source",
        config=full_config,
        tasks=tasks,
        topic_names=topic_names,
    )
    return ConfluentJDBCSourceConnector(
        manifest, KafkaConnectSourceConfig(), KafkaConnectSourceReport()
    )


def _mapping(connector: ConfluentJDBCSourceConnector) -> Dict[str, Optional[str]]:
    return {
        lineage.target_dataset: lineage.source_dataset
        for lineage in connector.extract_lineages()
    }


class TestTransformOrchestration:
    def test_multiple_tables_matched_to_routed_topics(self) -> None:
        connector = _jdbc(
            {
                "connection.url": "jdbc:postgresql://db.example:5432/mydb",
                "topic.prefix": "pg_",
                "transforms": "route",
                "transforms.route.type": REGEX_ROUTER,
                "transforms.route.regex": "pg_(.*)",
                "transforms.route.replacement": "clean_$1",
            },
            topic_names=["clean_users", "clean_orders", "unrelated"],
            tables='"public"."users","public"."orders"',
        )

        assert _mapping(connector) == {
            "clean_users": "mydb.public.users",
            "clean_orders": "mydb.public.orders",
        }

    def test_tables_whose_predicted_topic_is_absent_produce_no_lineage(self) -> None:
        connector = _jdbc(
            {
                "connection.url": "jdbc:postgresql://db.example:5432/mydb",
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
                "connection.url": "jdbc:postgresql://db.example:5432/mydb",
                "transforms": "route",
                "transforms.route.type": REGEX_ROUTER,
                "transforms.route.regex": "(.*)",
                "transforms.route.replacement": "x_$1",
            },
            topic_names=["x_users"],
        )

        assert connector.extract_lineages() == []

    def test_complex_transform_is_reported_as_fallback(self) -> None:
        connector = _jdbc(
            {
                "connection.url": "jdbc:postgresql://db.example:5432/mydb",
                "topic.prefix": "pg_",
                "transforms": "ts",
                "transforms.ts.type": "org.apache.kafka.connect.transforms.TimestampConverter",
                "transforms.ts.field": "created_at",
            },
            topic_names=["pg_users", "pg_orders"],
            tables='"public"."users","public"."orders"',
        )

        assert _mapping(connector) == {
            "pg_users": "mydb.public.users",
            "pg_orders": "mydb.public.orders",
        }
        assert len(connector.report.infos) == 1


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

        assert _mapping(connector) == {"mysql_customers": "shop.customers"}

    def test_three_level_platform_without_schema_excludes_source(self) -> None:
        # The task reported a bare table name; without a schema the postgres URN
        # would be wrong, so the lineage keeps the topic but drops the source.
        connector = _jdbc(
            {
                "connection.url": "jdbc:postgresql://db.example:5432/mydb",
                "topic.prefix": "pg_",
            },
            topic_names=["pg_users"],
            tables='"users"',
        )

        assert _mapping(connector) == {"pg_users": None}
        assert len(connector.report.warnings) == 1


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
        connector = _jdbc(
            {"connection.url": "jdbc:postgresql://db.example:5432/mydb"},
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
        # A URL without a database name makes the parser fail; the platform
        # must still be read from the JDBC scheme so the flow is categorised.
        connector = _jdbc({"connection.url": url}, topic_names=[])

        assert connector.get_platform() == expected

    def test_platform_unknown_without_connection_details(self) -> None:
        connector = _jdbc({"database.hostname": "db.example"}, topic_names=[])

        assert connector.get_platform() == "unknown"


class TestTopicPrefixEdgeCases:
    def test_prefix_without_separator_still_strips_leading_dot(self) -> None:
        connector = _jdbc(
            {
                "connection.url": "jdbc:postgresql://db.example:5432/mydb",
                "topic.prefix": "pg",
            },
            topic_names=["pg.users"],
            tables='"public"."users"',
        )

        assert _mapping(connector) == {"pg.users": "mydb.public.users"}

    def test_topic_outside_the_prefix_keeps_topic_but_no_source(self) -> None:
        connector = _jdbc(
            {
                "connection.url": "jdbc:postgresql://db.example:5432/mydb",
                "topic.prefix": "pg_",
            },
            topic_names=["pg_users", "audit"],
            tables='"public"."users"',
        )

        assert _mapping(connector) == {"pg_users": "mydb.public.users", "audit": None}


def _cloud_jdbc(
    config: Dict[str, str], cluster_topics: List[str]
) -> ConfluentJDBCSourceConnector:
    """A Confluent Cloud managed connector handled by the JDBC strategy, given
    the cluster topic list. extract_lineages() bails out on empty topic_names
    before choosing a strategy, so the list is also set there."""
    manifest = ConnectorManifest(
        name="cloud-source",
        type="source",
        config={
            "database.hostname": "db.example",
            "database.port": "5432",
            "database.dbname": "shop",
            **config,
        },
        tasks=[],
        topic_names=list(cluster_topics),
    )
    connector = ConfluentJDBCSourceConnector(
        manifest, KafkaConnectSourceConfig(), KafkaConnectSourceReport()
    )
    connector.all_cluster_topics = cluster_topics
    return connector


class TestCloudStrategies:
    def test_single_table_mode_maps_configured_table_to_its_topic(self) -> None:
        connector = _cloud_jdbc(
            {
                "connector.class": "MySqlSource",
                "kafka.topic": "shop-users",
                "table.name": "users",
            },
            cluster_topics=["shop-users", "unrelated"],
        )

        assert _mapping(connector) == {"shop-users": "shop.users"}

    def test_topic_prefix_infers_tables_from_cluster_topics(self) -> None:
        connector = _cloud_jdbc(
            {"connector.class": "MySqlSource", "topic.prefix": "mysql_"},
            cluster_topics=["mysql_users", "mysql_orders"],
        )

        assert _mapping(connector) == {
            "mysql_users": "shop.users",
            "mysql_orders": "shop.orders",
        }

    def test_routed_topics_matched_through_forward_transforms(self) -> None:
        connector = _cloud_jdbc(
            {
                "connector.class": "PostgresCdcSource",
                "database.server.name": "srv",
                "table.include.list": "public.users",
                "transforms": "route",
                "transforms.route.type": REGEX_ROUTER,
                "transforms.route.regex": "srv\\.public\\.(.*)",
                "transforms.route.replacement": "clean_$1",
            },
            cluster_topics=["clean_users", "noise"],
        )

        assert _mapping(connector) == {"clean_users": "shop.public.users"}

    @pytest.mark.parametrize(
        "connector_class,schema,prefix,expected",
        [
            ("MySqlSource", "", "", "users"),
            ("MySqlSource", "", "srv", "srv.users"),
            ("PostgresCdcSource", "public", "", "public.users"),
            ("PostgresCdcSource", "public", "srv", "srv.public.users"),
            (
                "io.confluent.connect.jdbc.JdbcSourceConnector",
                "public",
                "pg_",
                "pg_users",
            ),
            ("com.example.Custom", "", "pg_", "pg_users"),
        ],
    )
    def test_original_topic_naming_convention_per_connector_family(
        self, connector_class: str, schema: str, prefix: str, expected: str
    ) -> None:
        connector = _cloud_jdbc({"connector.class": "MySqlSource"}, [])

        assert (
            connector._generate_original_topic_name(
                schema, "users", prefix, connector_class
            )
            == expected
        )

    def test_forward_transforms_predict_outbox_and_skip_unknown_types(self) -> None:
        connector = _cloud_jdbc({"connector.class": "MySqlSource"}, [])

        predicted = connector._apply_transforms_forward(
            ["srv.outbox"],
            [
                {"type": "io.debezium.transforms.outbox.EventRouter"},
                {"type": "org.apache.kafka.connect.transforms.InsertField$Value"},
            ],
        )

        assert predicted == ["outbox.event.srv.outbox"]

    def test_without_transforms_expected_topics_are_the_original_names(self) -> None:
        connector = _cloud_jdbc(
            {"connector.class": "MySqlSource", "database.server.name": "srv"}, []
        )
        parser = connector.get_parser(connector.connector_manifest)

        assert connector._apply_forward_transforms(["users"], parser) == ["srv.users"]
