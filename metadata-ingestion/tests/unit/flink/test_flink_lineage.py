from unittest.mock import MagicMock

from datahub.ingestion.source.flink.client import FlinkPlanNode
from datahub.ingestion.source.flink.lineage import (
    FlinkLineageOrchestrator,
    KafkaLineageExtractor,
    LineageExtractor,
    NodeRole,
)


def _node(node_id: str, description: str) -> FlinkPlanNode:
    return FlinkPlanNode(
        id=node_id, description=description, operator="", parallelism=1
    )


class TestKafkaLineageExtractor:
    def setup_method(self) -> None:
        self.extractor = KafkaLineageExtractor()

    # --- DataStream API sources ---

    def test_datastream_source_extracts_topic(self) -> None:
        node = _node("1", "Source: KafkaSource-transactions -> Filter")
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SOURCE)
        assert result is not None
        assert result.platform == "kafka"
        assert result.dataset_name == "transactions"

    def test_datastream_source_hyphenated_topic(self) -> None:
        node = _node("1", "Source: KafkaSource-user-click-events -> Map")
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SOURCE)
        assert result is not None
        assert result.dataset_name == "user-click-events"

    # --- DataStream API sinks ---

    def test_datastream_sink_extracts_topic(self) -> None:
        node = _node("2", "Sink: KafkaSink-alerts")
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SINK)
        assert result is not None
        assert result.platform == "kafka"
        assert result.dataset_name == "alerts"

    # --- SQL/Table API sources ---

    def test_table_source_scan(self) -> None:
        desc = "[1]:TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[order_id])"
        node = _node("1", desc)
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SOURCE)
        assert result is not None
        assert result.dataset_name == "orders"

    # --- SQL/Table API sinks (legacy format) ---

    def test_table_sink_legacy_format(self) -> None:
        desc = "Sink: Sink(table=[[default_catalog, default_database, enriched_orders]], fields=[order_id])"
        node = _node("2", desc)
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SINK)
        assert result is not None
        assert result.dataset_name == "enriched_orders"

    # --- SQL/Table API sinks (operator-chained Writer format, verified on Flink 1.19) ---

    def test_sink_writer_format(self) -> None:
        """Operator-chained sink uses 'tableName[N]: Writer' format."""
        desc = "enriched_orders[2]: Writer"
        node = _node("2", desc)
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SINK)
        assert result is not None
        assert result.dataset_name == "enriched_orders"

    def test_sink_writer_with_committer(self) -> None:
        """Writer + Committer pattern (two-phase commit sinks)."""
        desc = "ds_sink[2]: Writer<br/>   +- ds_sink[2]: Committer"
        node = _node("2", desc)
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SINK)
        assert result is not None
        assert result.dataset_name == "ds_sink"


class TestFlinkLineageOrchestrator:
    def setup_method(self) -> None:
        self.orchestrator = FlinkLineageOrchestrator()

    # --- DataStream API: separate nodes ---

    def test_datastream_separate_nodes(self) -> None:
        nodes = [
            _node("1", "Source: KafkaSource-transactions -> Filter"),
            _node("2", "KeyBy -> Window -> Aggregate"),
            _node("3", "Sink: KafkaSink-alerts"),
        ]
        result = self.orchestrator.extract(nodes)
        assert len(result.sources) == 1
        assert len(result.sinks) == 1
        assert len(result.unclassified) == 0
        assert result.sources[0].dataset_name == "transactions"
        assert result.sinks[0].dataset_name == "alerts"

    def test_multiple_sources_extracted(self) -> None:
        """Job reading from 2+ Kafka topics (e.g., a join)."""
        nodes = [
            _node("1", "Source: KafkaSource-orders -> Map"),
            _node("2", "Source: KafkaSource-users -> Map"),
            _node("3", "KeyBy -> Join"),
            _node("4", "Sink: KafkaSink-enriched-orders"),
        ]
        result = self.orchestrator.extract(nodes)
        assert len(result.sources) == 2
        assert len(result.sinks) == 1
        assert len(result.unclassified) == 0
        source_names = {s.dataset_name for s in result.sources}
        assert source_names == {"orders", "users"}

    # --- SQL/Table API: operator-chained merged node (verified on Flink 1.19) ---

    def test_merged_node_extracts_both_source_and_sink(self) -> None:
        """Operator chaining merges source+sink into one node for simple SQL jobs."""
        desc = (
            "[1]:TableSourceScan(table=[[default_catalog, default_database, orders]], "
            "fields=[order_id, amount])<br/>"
            "+- enriched_orders[2]: Writer<br/>"
            "   +- enriched_orders[2]: Committer<br/>"
        )
        nodes = [_node("1", desc)]
        result = self.orchestrator.extract(nodes)
        assert len(result.sources) == 1
        assert len(result.sinks) == 1
        assert result.sources[0].dataset_name == "orders"
        assert result.sinks[0].dataset_name == "enriched_orders"

    def test_merged_node_with_calc(self) -> None:
        """Merged node with filter/calc between source and sink."""
        desc = (
            "[3]:TableSourceScan(table=[[default_catalog, default_database, clicks]], "
            "fields=[id, name, ts])<br/>"
            "+- [4]:Calc(select=[id, name, ts], where=[(id &gt; 0)])<br/>"
            "+- user_activity[5]: Writer<br/>"
            "   +- user_activity[5]: Committer<br/>"
        )
        nodes = [_node("1", desc)]
        result = self.orchestrator.extract(nodes)
        assert len(result.sources) == 1
        assert len(result.sinks) == 1
        assert result.sources[0].dataset_name == "clicks"
        assert result.sinks[0].dataset_name == "user_activity"

    # --- SQL/Table API: separate nodes (datagen/blackhole) ---

    def test_separate_nodes_datagen_blackhole(self) -> None:
        """Datagen source and blackhole sink produce 2 separate nodes."""
        nodes = [
            _node(
                "1",
                "[6]:TableSourceScan(table=[[default_catalog, default_database, gen_src]], fields=[id, val])",
            ),
            _node("2", "bh_sink[7]: Writer"),
        ]
        result = self.orchestrator.extract(nodes)
        assert len(result.sources) == 1
        assert len(result.sinks) == 1
        assert result.sources[0].dataset_name == "gen_src"
        assert result.sinks[0].dataset_name == "bh_sink"

    # --- Edge cases ---

    def test_unclassified_when_no_extractor_matches(self) -> None:
        nodes = [
            _node("1", "Source: JdbcSource-my_table -> Map"),
            _node("2", "Sink: KafkaSink-output"),
        ]
        result = self.orchestrator.extract(nodes)
        assert len(result.sources) == 0
        assert len(result.sinks) == 1
        assert len(result.unclassified) == 1

    def test_transform_only_node_ignored(self) -> None:
        nodes = [_node("1", "KeyBy -> Window -> Aggregate")]
        result = self.orchestrator.extract(nodes)
        assert len(result.sources) == 0
        assert len(result.sinks) == 0
        assert len(result.unclassified) == 0

    def test_extractor_exception_does_not_crash_extraction(self) -> None:
        """A broken extractor is caught and the node goes to unclassified."""
        broken_extractor = MagicMock(spec=LineageExtractor)
        broken_extractor.can_extract.return_value = True
        broken_extractor.extract_dataset.side_effect = RuntimeError("bug")

        orchestrator = FlinkLineageOrchestrator(extractors=[broken_extractor])
        nodes = [_node("1", "Source: KafkaSource-topic1 -> Map")]
        result = orchestrator.extract(nodes)
        assert len(result.sources) == 0
        assert len(result.unclassified) == 1
