"""BaseConnector helpers shared by every connector type."""

from typing import List, Optional
from unittest.mock import MagicMock

from datahub.ingestion.source.kafka_connect.common import (
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.sink_connectors import (
    ConfluentS3SinkConnector,
)
from tests.unit.kafka_connect.conftest import (
    StubSchemaResolver,
    attach_resolver,
    dataset_urn,
    make_manifest,
    mock_graph,
)


def _regex_sink(graph: Optional[MagicMock]) -> ConfluentS3SinkConnector:
    """A Confluent Cloud sink subscribed by topics.regex with no cluster topic
    list at hand, so the pattern can only be expanded against DataHub."""
    connector = ConfluentS3SinkConnector(
        make_manifest(
            {"s3.bucket.name": "bucket", "topics.regex": "orders.*"},
            name="s3-sink",
            connector_type="sink",
        ),
        KafkaConnectSourceConfig(use_schema_resolver=True),
        KafkaConnectSourceReport(),
    )
    attach_resolver(connector, StubSchemaResolver(graph=graph))
    return connector


def _targets(connector: ConfluentS3SinkConnector) -> List[str]:
    return sorted(lineage.target_dataset for lineage in connector.extract_lineages())


class TestTopicRegexExpansionViaDataHub:
    def test_pattern_expanded_against_kafka_topics_known_to_datahub(self) -> None:
        graph = mock_graph()
        graph.get_urns_by_filter.return_value = iter(
            [
                dataset_urn("kafka", "orders-eu"),
                dataset_urn("kafka", "orders-us"),
                dataset_urn("kafka", "users"),
            ]
        )

        targets = _targets(_regex_sink(graph))

        assert targets == ["bucket/topics/orders-eu", "bucket/topics/orders-us"]
        assert graph.get_urns_by_filter.call_args.kwargs["platform"] == "kafka"

    def test_datahub_query_failure_yields_no_topics(self) -> None:
        graph = mock_graph()
        graph.get_urns_by_filter.side_effect = RuntimeError("datahub down")

        assert _targets(_regex_sink(graph)) == []

    def test_no_graph_means_no_expansion(self) -> None:
        assert _targets(_regex_sink(None)) == []


class TestSourceFineGrainedLineageGuard:
    def test_kafka_sourced_dataset_never_gets_source_column_lineage(self) -> None:
        """The source-side helper resolves the source table through a database
        SchemaResolver; handed a Kafka topic it must opt out, not query."""
        graph = mock_graph()
        connector = _regex_sink(graph)

        result = connector._extract_fine_grained_lineage(
            source_dataset="orders", source_platform="kafka", target_dataset="t"
        )

        assert result is None
        graph.get_aspect.assert_not_called()
