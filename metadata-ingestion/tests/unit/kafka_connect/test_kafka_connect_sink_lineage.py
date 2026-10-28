"""Sink connector lineage: target naming rules per sink, credential masking in
flow properties, and how malformed sink config degrades."""

from typing import Dict, List, Optional, cast

import jpype
import jpype.imports
import pytest

from datahub.ingestion.source.kafka_connect.common import (
    BaseConnector,
    ConnectorManifest,
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


@pytest.fixture(scope="session", autouse=True)
def ensure_jvm_started() -> None:
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath())


def _manifest(config: Dict[str, str], topic_names: List[str]) -> ConnectorManifest:
    return ConnectorManifest(
        name="sink", type="sink", config=config, tasks=[], topic_names=topic_names
    )


def _targets(connector: BaseConnector) -> Dict[str, str]:
    return {
        str(lineage.source_dataset): lineage.target_dataset
        for lineage in connector.extract_lineages()
    }


def _deps() -> tuple[KafkaConnectSourceConfig, KafkaConnectSourceReport]:
    return KafkaConnectSourceConfig(), KafkaConnectSourceReport()


class TestS3Sink:
    def test_missing_bucket_is_a_reported_config_error(self) -> None:
        config, report = _deps()
        connector = ConfluentS3SinkConnector(
            _manifest(
                {
                    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
                    "topics": "orders",
                },
                ["orders"],
            ),
            config,
            report,
        )

        assert connector.extract_lineages() == []
        assert len(report.warnings) == 1

    def test_complex_transform_is_surfaced_as_info(self) -> None:
        config, report = _deps()
        connector = ConfluentS3SinkConnector(
            _manifest(
                {
                    "s3.bucket.name": "bucket",
                    "topics": "orders",
                    "transforms": "unwrap",
                    "transforms.unwrap.type": "io.confluent.connect.transforms.ExtractField",
                    "transforms.unwrap.field": "payload",
                },
                ["orders"],
            ),
            config,
            report,
        )

        assert _targets(connector) == {"orders": "bucket/topics/orders"}
        assert len(report.infos) == 1

    def test_flow_properties_drop_aws_credentials(self) -> None:
        config, report = _deps()
        connector = ConfluentS3SinkConnector(
            _manifest(
                {
                    "s3.bucket.name": "bucket",
                    "aws.access.key.id": "AKIA-PLANTED",
                    "aws.secret.access.key": "secret-planted",
                    "s3.sse.customer.key": "sse-planted",
                    "s3.proxy.password": "proxy-planted",
                    "topics": "orders",
                },
                ["orders"],
            ),
            config,
            report,
        )

        properties = connector.extract_flow_property_bag()

        assert "planted" not in str(properties).lower()
        assert properties["s3.bucket.name"] == "bucket"


class TestSnowflakeSink:
    def _connector(
        self, extra: Dict[str, str], topics: List[str]
    ) -> SnowflakeSinkConnector:
        config, report = _deps()
        return SnowflakeSinkConnector(
            _manifest(
                {
                    "snowflake.database.name": "DB",
                    "snowflake.schema.name": "PUBLIC",
                    "topics": ",".join(topics),
                    **extra,
                },
                topics,
            ),
            config,
            report,
        )

    def test_topic_starting_with_digit_gets_leading_underscore(self) -> None:
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
        config, report = _deps()
        connector = ClickHouseSinkConnector(
            _manifest(
                cast(Dict[str, str], {"topics": "orders", "topic2TableMap": 42}),
                ["orders"],
            ),
            config,
            report,
        )

        assert _targets(connector) == {"orders": "default.orders"}

    def test_malformed_topic_to_table_entries_fall_back_to_topic_name(self) -> None:
        config, report = _deps()
        connector = ClickHouseSinkConnector(
            _manifest(
                {
                    "database": "analytics",
                    "topics": "orders,users",
                    "topic2TableMap": "orders=orders_t,users",
                },
                ["orders", "users"],
            ),
            config,
            report,
        )

        assert _targets(connector) == {
            "orders": "analytics.orders_t",
            "users": "analytics.users",
        }


class TestIcebergSink:
    def test_non_string_config_value_is_reported_not_raised(self) -> None:
        # Connect REST payloads are not validated; a boolean where a string is
        # expected must degrade to a warning for this connector only.
        config, report = _deps()
        connector = IcebergSinkConnector(
            _manifest(
                cast(
                    Dict[str, str],
                    {"iceberg.tables.dynamic-enabled": True, "topics": "orders"},
                ),
                ["orders"],
            ),
            config,
            report,
        )

        assert connector.extract_lineages() == []
        assert len(report.warnings) == 1

    def test_flow_properties_drop_anything_secret_like(self) -> None:
        config, report = _deps()
        connector = IcebergSinkConnector(
            _manifest(
                {
                    "iceberg.tables": "db.t",
                    "iceberg.catalog.credential": "cred-planted",
                    "iceberg.catalog.s3.secret-access-key": "secret-planted",
                    "iceberg.catalog.token": "token-planted",
                    "iceberg.catalog.uri": "https://catalog.example",
                },
                ["orders"],
            ),
            config,
            report,
        )

        properties = connector.extract_flow_property_bag()

        assert "planted" not in str(properties)
        assert properties["iceberg.catalog.uri"] == "https://catalog.example"


class TestBigQuerySink:
    def _connector(
        self, extra: Dict[str, str], topics: List[str], subscribe: Optional[str] = None
    ) -> BigQuerySinkConnector:
        config, report = _deps()
        return BigQuerySinkConnector(
            _manifest(
                {"project": "proj", "topics": subscribe or ",".join(topics), **extra},
                topics,
            ),
            config,
            report,
        )

    def test_legacy_datasets_regex_routes_topics_and_renames_tables(self) -> None:
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
        assert len(connector.report.warnings) == 1

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
        config, report = _deps()
        connector = JdbcSinkConnector(
            _manifest(
                {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                    "connection.url": "not-a-jdbc-url",
                    "topics": "orders",
                },
                ["orders"],
            ),
            config,
            report,
        )

        assert connector.get_platform() == "unknown"
        assert len(report.warnings) == 1

    def test_non_string_table_format_is_reported_not_raised(self) -> None:
        config, report = _deps()
        connector = JdbcSinkConnector(
            _manifest(
                cast(
                    Dict[str, str],
                    {
                        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                        "connection.url": "jdbc:mysql://db.example:3306/shop",
                        "topics": "orders",
                        "table.name.format": 42,
                    },
                ),
                ["orders"],
            ),
            config,
            report,
        )

        assert connector.extract_lineages() == []
        assert len(report.warnings) == 1

    def test_literal_table_name_format_targets_one_table(self) -> None:
        config, report = _deps()
        connector = JdbcSinkConnector(
            _manifest(
                {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                    "connection.url": "jdbc:mysql://db.example:3306/shop",
                    "topics": "orders,orders-dlq",
                    "table.name.format": "all_orders",
                },
                ["orders", "orders-dlq"],
            ),
            config,
            report,
        )

        assert _targets(connector) == {
            "orders": "shop.all_orders",
            "orders-dlq": "shop.all_orders",
        }
