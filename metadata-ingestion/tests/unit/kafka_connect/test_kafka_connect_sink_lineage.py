"""Sink connector lineage: target naming rules per sink, credential masking in
flow properties, and how malformed sink config degrades."""

from typing import Any, Dict, List, Optional, Type, TypeVar

from datahub.ingestion.source.kafka_connect.common import (
    BaseConnector,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.sink_connectors import (
    BigQuerySinkConnector,
    ClickHouseSinkConnector,
    ConfluentS3SinkConnector,
    IcebergSinkConnector,
    JdbcSinkConnector,
    SnowflakeSinkConnector,
)
from tests.unit.kafka_connect.conftest import make_manifest, reported

CONNECTOR = "sink"
JDBC_SINK = "io.confluent.connect.jdbc.JdbcSinkConnector"

SinkT = TypeVar("SinkT", bound=BaseConnector)


def _sink(
    sink_class: Type[SinkT], config: Dict[str, Any], topic_names: List[str]
) -> SinkT:
    return sink_class(
        make_manifest(
            config, name=CONNECTOR, connector_type="sink", topic_names=topic_names
        ),
        KafkaConnectSourceConfig(),
        KafkaConnectSourceReport(),
    )


def _targets(connector: BaseConnector) -> Dict[str, str]:
    return {
        str(lineage.source_dataset): lineage.target_dataset
        for lineage in connector.extract_lineages()
    }


class TestS3Sink:
    def test_missing_bucket_is_a_reported_config_error(self) -> None:
        connector = _sink(
            ConfluentS3SinkConnector,
            {
                "connector.class": "io.confluent.connect.s3.S3SinkConnector",
                "topics": "orders",
            },
            ["orders"],
        )

        assert connector.extract_lineages() == []
        assert reported(connector.report.warnings, CONNECTOR)

    def test_complex_transform_is_surfaced_as_info(self) -> None:
        connector = _sink(
            ConfluentS3SinkConnector,
            {
                "s3.bucket.name": "bucket",
                "topics": "orders",
                "transforms": "unwrap",
                "transforms.unwrap.type": "io.confluent.connect.transforms.ExtractField",
                "transforms.unwrap.field": "payload",
            },
            ["orders"],
        )

        assert _targets(connector) == {"orders": "bucket/topics/orders"}
        assert reported(connector.report.infos, CONNECTOR)

    def test_flow_properties_drop_aws_credentials(self) -> None:
        connector = _sink(
            ConfluentS3SinkConnector,
            {
                "s3.bucket.name": "bucket",
                "aws.access.key.id": "AKIA-PLANTED",
                "aws.secret.access.key": "secret-planted",
                "s3.sse.customer.key": "sse-planted",
                "s3.proxy.password": "proxy-planted",
                "topics": "orders",
            },
            ["orders"],
        )

        properties = connector.extract_flow_property_bag()

        assert "planted" not in str(properties).lower()
        assert properties["s3.bucket.name"] == "bucket"


class TestSnowflakeSink:
    def _connector(
        self, extra: Dict[str, str], topics: List[str]
    ) -> SnowflakeSinkConnector:
        return _sink(
            SnowflakeSinkConnector,
            {
                "snowflake.database.name": "DB",
                "snowflake.schema.name": "PUBLIC",
                "topics": ",".join(topics),
                **extra,
            },
            topics,
        )

    def test_topic_starting_with_digit_gets_leading_underscore(self) -> None:
        """Snowflake's default table naming prefixes an underscore to a name
        that would otherwise start with a digit; the URN must match that."""
        connector = self._connector({}, ["2024-events"])

        assert _targets(connector) == {"2024-events": "DB.PUBLIC._2024_events"}

    def test_malformed_topic_to_table_map_entries_are_ignored(self) -> None:
        connector = self._connector(
            {"snowflake.topic2table.map": "orders:ORDERS_T,users"}, ["orders", "users"]
        )

        assert _targets(connector) == {
            "orders": "DB.PUBLIC.ORDERS_T",
            "users": "DB.PUBLIC.users",
        }

    def test_flow_properties_drop_private_key_material(self) -> None:
        connector = self._connector(
            {
                "snowflake.private.key": "key-planted",
                "snowflake.private.key.passphrase": "pass-planted",
                "value.converter.basic.auth.user.info": "sr-planted",
            },
            ["orders"],
        )

        assert "planted" not in str(connector.extract_flow_property_bag())


class TestClickHouseSink:
    def test_non_string_topic_map_is_ignored(self) -> None:
        connector = _sink(
            ClickHouseSinkConnector,
            {"topics": "orders", "topic2TableMap": 42},
            ["orders"],
        )

        assert _targets(connector) == {"orders": "default.orders"}

    def test_malformed_topic_to_table_entries_fall_back_to_topic_name(self) -> None:
        connector = _sink(
            ClickHouseSinkConnector,
            {
                "database": "analytics",
                "topics": "orders,users",
                "topic2TableMap": "orders=orders_t,users",
            },
            ["orders", "users"],
        )

        assert _targets(connector) == {
            "orders": "analytics.orders_t",
            "users": "analytics.users",
        }


class TestIcebergSink:
    def test_non_string_config_value_is_reported_not_raised(self) -> None:
        """Connect REST payloads are not validated; a boolean where a string is
        expected must degrade to a warning for this connector only."""
        connector = _sink(
            IcebergSinkConnector,
            {"iceberg.tables.dynamic-enabled": True, "topics": "orders"},
            ["orders"],
        )

        assert connector.extract_lineages() == []
        assert reported(connector.report.warnings, CONNECTOR)

    def test_flow_properties_drop_anything_secret_like(self) -> None:
        connector = _sink(
            IcebergSinkConnector,
            {
                "iceberg.tables": "db.t",
                "iceberg.catalog.credential": "cred-planted",
                "iceberg.catalog.s3.secret-access-key": "secret-planted",
                "iceberg.catalog.token": "token-planted",
                "iceberg.catalog.uri": "https://catalog.example",
            },
            ["orders"],
        )

        properties = connector.extract_flow_property_bag()

        assert "planted" not in str(properties)
        assert properties["iceberg.catalog.uri"] == "https://catalog.example"


class TestBigQuerySink:
    def _connector(
        self, extra: Dict[str, str], topics: List[str], subscribe: Optional[str] = None
    ) -> BigQuerySinkConnector:
        return _sink(
            BigQuerySinkConnector,
            {"project": "proj", "topics": subscribe or ",".join(topics), **extra},
            topics,
        )

    def test_legacy_datasets_regex_routes_topics_and_renames_tables(self) -> None:
        """The legacy ``datasets``/``topicsToTables`` regex maps route each topic
        to a dataset and table; a topic matching no dataset is reported."""
        connector = self._connector(
            {
                "datasets": "orders.*=sales,users=crm",
                "topicsToTables": "orders.*=orders_all",
                "sanitizeTopics": "true",
            },
            ["orders-eu", "users", "audit"],
        )

        assert _targets(connector) == {
            "orders-eu": "proj.sales.orders_all",
            "users": "proj.crm.users",
        }
        assert reported(connector.report.warnings, "audit")

    def test_legacy_mapping_entries_without_equals_are_ignored(self) -> None:
        connector = self._connector({"datasets": "garbage,.*=warehouse"}, ["orders"])

        assert _targets(connector) == {"orders": "proj.warehouse.orders"}

    def test_default_dataset_with_explicit_topic_to_table_map(self) -> None:
        connector = self._connector(
            {
                "defaultDataset": "warehouse",
                "topic2table.map": "orders:fact_orders",
            },
            ["orders", "users"],
        )

        assert _targets(connector) == {
            "orders": "proj.warehouse.fact_orders",
            "users": "proj.warehouse.users",
        }

    def test_topic_may_encode_its_own_dataset(self) -> None:
        connector = self._connector({"defaultDataset": "warehouse"}, ["finance:ledger"])

        assert _targets(connector) == {"finance:ledger": "proj.finance.ledger"}

    def test_flow_properties_drop_keyfile(self) -> None:
        connector = self._connector(
            {"defaultDataset": "warehouse", "keyfile": '{"private_key": "planted"}'},
            ["orders"],
        )

        assert "planted" not in str(connector.extract_flow_property_bag())


class TestJdbcSink:
    def test_unparseable_connection_url_leaves_platform_unknown(self) -> None:
        connector = _sink(
            JdbcSinkConnector,
            {
                "connector.class": JDBC_SINK,
                "connection.url": "not-a-jdbc-url",
                "topics": "orders",
            },
            ["orders"],
        )

        assert connector.get_platform() == "unknown"
        assert reported(connector.report.warnings, CONNECTOR)

    def test_non_string_table_format_is_reported_not_raised(self) -> None:
        connector = _sink(
            JdbcSinkConnector,
            {
                "connector.class": JDBC_SINK,
                "connection.url": "jdbc:mysql://db.example:3306/shop",
                "topics": "orders",
                "table.name.format": 42,
            },
            ["orders"],
        )

        assert connector.extract_lineages() == []
        assert reported(connector.report.warnings, CONNECTOR)

    def test_literal_table_name_format_targets_one_table(self) -> None:
        """A ``table.name.format`` without ``${topic}`` sends every topic to
        the same table."""
        connector = _sink(
            JdbcSinkConnector,
            {
                "connector.class": JDBC_SINK,
                "connection.url": "jdbc:mysql://db.example:3306/shop",
                "topics": "orders,orders-dlq",
                "table.name.format": "all_orders",
            },
            ["orders", "orders-dlq"],
        )

        assert _targets(connector) == {
            "orders": "shop.all_orders",
            "orders-dlq": "shop.all_orders",
        }
