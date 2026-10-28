"""BaseConnector helpers shared by every connector type."""

from typing import Any, Dict, List, Optional, Set, Tuple, cast
from unittest.mock import MagicMock

import jpype
import jpype.imports
import pytest

from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.sink_connectors import (
    ConfluentS3SinkConnector,
)
from datahub.sql_parsing.schema_resolver import SchemaResolverInterface


@pytest.fixture(scope="session", autouse=True)
def ensure_jvm_started() -> None:
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath())


class _StubSchemaResolver(SchemaResolverInterface):
    platform_instance: Optional[str] = None
    env = "PROD"

    def __init__(self, graph: Optional[MagicMock]) -> None:
        self.graph = graph

    @property
    def platform(self) -> str:
        return "kafka"

    def includes_temp_tables(self) -> bool:
        return False

    def get_urns(self) -> Set[str]:
        return set()

    def resolve_table(self, table: Any) -> Tuple[str, Optional[Dict[str, str]]]:
        return f"urn:li:dataset:(urn:li:dataPlatform:kafka,{table.table},PROD)", None

    def get_urn_for_table(self, table: Any) -> str:
        return f"urn:li:dataset:(urn:li:dataPlatform:kafka,{table.table},PROD)"


def _kafka_urn(topic: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:kafka,{topic},PROD)"


def _regex_sink(graph: Optional[MagicMock]) -> ConfluentS3SinkConnector:
    """A Confluent Cloud sink subscribed by topics.regex with no cluster topic
    list at hand, so the pattern can only be expanded against DataHub."""
    manifest = ConnectorManifest(
        name="s3-sink",
        type="sink",
        config={"s3.bucket.name": "bucket", "topics.regex": "orders.*"},
        tasks=[],
        topic_names=[],
    )
    connector = ConfluentS3SinkConnector(
        manifest,
        KafkaConnectSourceConfig(use_schema_resolver=True),
        KafkaConnectSourceReport(),
    )
    connector.schema_resolver = cast(Any, _StubSchemaResolver(graph))
    return connector


def _targets(connector: ConfluentS3SinkConnector) -> List[str]:
    return sorted(lineage.target_dataset for lineage in connector.extract_lineages())


class TestTopicRegexExpansionViaDataHub:
    def test_pattern_expanded_against_kafka_topics_known_to_datahub(self) -> None:
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = iter(
            [_kafka_urn("orders-eu"), _kafka_urn("orders-us"), _kafka_urn("users")]
        )

        targets = _targets(_regex_sink(graph))

        assert targets == ["bucket/topics/orders-eu", "bucket/topics/orders-us"]
        kwargs = cast(MagicMock, graph.get_urns_by_filter).call_args.kwargs
        assert kwargs["platform"] == "kafka"

    def test_datahub_query_failure_yields_no_topics(self) -> None:
        graph = MagicMock()
        graph.get_urns_by_filter.side_effect = RuntimeError("datahub down")

        assert _targets(_regex_sink(graph)) == []

    def test_no_graph_means_no_expansion(self) -> None:
        assert _targets(_regex_sink(None)) == []


class TestSourceFineGrainedLineageGuard:
    def test_kafka_sourced_dataset_never_gets_source_column_lineage(self) -> None:
        # The source-side helper resolves the source table through a database
        # SchemaResolver; handed a Kafka topic it must opt out, not query.
        connector = _regex_sink(MagicMock())
        resolver = cast(MagicMock, connector.schema_resolver)

        result = connector._extract_fine_grained_lineage(
            source_dataset="orders", source_platform="kafka", target_dataset="t"
        )

        assert result is None
        assert not resolver.graph.get_aspect.called
