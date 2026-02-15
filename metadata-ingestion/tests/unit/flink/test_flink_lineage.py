from datahub.ingestion.source.flink.flink_client import FlinkPlanNode
from datahub.ingestion.source.flink.flink_lineage import (
    FlinkLineageOrchestrator,
    KafkaLineageExtractor,
    NodeRole,
)


def _make_node(node_id: str, description: str) -> FlinkPlanNode:
    return FlinkPlanNode(
        id=node_id,
        description=description,
        operator="",
        parallelism=1,
        inputs=[],
    )


class TestKafkaLineageExtractor:
    def setup_method(self):
        self.extractor = KafkaLineageExtractor()

    def test_datastream_source(self):
        node = _make_node("1", "Source: KafkaSource-transactions -> Filter")
        assert self.extractor.can_extract(node.description, node)
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SOURCE)
        assert result is not None
        assert result.platform == "kafka"
        assert result.dataset_name == "transactions"

    def test_datastream_source_hyphenated_topic(self):
        node = _make_node("1", "Source: KafkaSource-user-click-events -> Map")
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SOURCE)
        assert result is not None
        assert result.dataset_name == "user-click-events"

    def test_datastream_sink(self):
        node = _make_node("2", "Sink: KafkaSink-alerts")
        assert self.extractor.can_extract(node.description, node)
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SINK)
        assert result is not None
        assert result.platform == "kafka"
        assert result.dataset_name == "alerts"

    def test_table_source_scan_post_flip195(self):
        desc = "[1]:TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[order_id, amount])"
        node = _make_node("1", desc)
        assert self.extractor.can_extract(node.description, node)
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SOURCE)
        assert result is not None
        assert result.dataset_name == "orders"

    def test_table_source_scan_pre_flip195(self):
        desc = "Source: TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[order_id])"
        node = _make_node("1", desc)
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SOURCE)
        assert result is not None
        assert result.dataset_name == "orders"

    def test_table_sink(self):
        desc = "Sink: Sink(table=[[default_catalog, default_database, enriched_orders]], fields=[order_id])"
        node = _make_node("2", desc)
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SINK)
        assert result is not None
        assert result.dataset_name == "enriched_orders"

    def test_transform_not_extractable(self):
        node = _make_node("3", "KeyBy -> Window -> Map")
        assert not self.extractor.can_extract(node.description, node)


class TestFlinkLineageOrchestrator:
    def setup_method(self):
        self.orchestrator = FlinkLineageOrchestrator()

    def test_full_pipeline_extraction(self):
        nodes = [
            _make_node("1", "Source: KafkaSource-transactions -> Filter"),
            _make_node("2", "KeyBy -> Window -> Aggregate"),
            _make_node("3", "Sink: KafkaSink-alerts"),
        ]
        result = self.orchestrator.extract(nodes)
        assert len(result.sources) == 1
        assert len(result.sinks) == 1
        assert result.sources[0].dataset_name == "transactions"
        assert result.sinks[0].dataset_name == "alerts"
        assert len(result.unclassified) == 0

    def test_unclassified_source_node(self):
        nodes = [
            _make_node("1", "Source: JdbcSource-my_table -> Map"),
            _make_node("2", "Sink: KafkaSink-output"),
        ]
        result = self.orchestrator.extract(nodes)
        assert len(result.sources) == 0
        assert len(result.sinks) == 1
        assert len(result.unclassified) == 1

    def test_empty_plan(self):
        result = self.orchestrator.extract([])
        assert len(result.sources) == 0
        assert len(result.sinks) == 0

    def test_multiple_sources_and_sinks(self):
        nodes = [
            _make_node("1", "Source: KafkaSource-orders -> Map"),
            _make_node("2", "Source: KafkaSource-users -> Map"),
            _make_node("3", "KeyBy -> Join"),
            _make_node("4", "Sink: KafkaSink-enriched-orders"),
            _make_node("5", "Sink: KafkaSink-audit-log"),
        ]
        result = self.orchestrator.extract(nodes)
        assert len(result.sources) == 2
        assert len(result.sinks) == 2
        source_topics = {s.dataset_name for s in result.sources}
        sink_topics = {s.dataset_name for s in result.sinks}
        assert source_topics == {"orders", "users"}
        assert sink_topics == {"enriched-orders", "audit-log"}
