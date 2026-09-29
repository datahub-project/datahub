from unittest.mock import MagicMock

from datahub.ingestion.source.flink.client import FlinkPlanNode
from datahub.ingestion.source.flink.lineage import (
    CatalogTableReference,
    ClassifiedNode,
    DataStreamKafkaExtractor,
    FlinkLineageOrchestrator,
    LineageExtractor,
    NodeRole,
    PlatformResolver,
    SqlTableExtractor,
)


def _node(node_id: str, description: str) -> FlinkPlanNode:
    return FlinkPlanNode(
        id=node_id, description=description, operator="", parallelism=1
    )


class TestDataStreamKafkaExtractor:
    def setup_method(self) -> None:
        self.extractor = DataStreamKafkaExtractor()

    def test_source_extracts_topic(self) -> None:
        node = _node("1", "Source: KafkaSource-transactions -> Filter")
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SOURCE)
        assert result is not None
        assert result.platform == "kafka"
        assert result.dataset_name == "transactions"

    def test_source_hyphenated_topic(self) -> None:
        node = _node("1", "Source: KafkaSource-user-click-events -> Map")
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SOURCE)
        assert result is not None
        assert result.dataset_name == "user-click-events"

    def test_sink_extracts_topic(self) -> None:
        node = _node("2", "Sink: KafkaSink-alerts")
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SINK)
        assert result is not None
        assert result.platform == "kafka"
        assert result.dataset_name == "alerts"

    def test_does_not_match_table_source_scan(self) -> None:
        desc = "[1]:TableSourceScan(table=[[cat, db, orders]], fields=[id])"
        node = _node("1", desc)
        assert not self.extractor.can_extract(node.description, node)

    def test_does_not_match_sink_writer(self) -> None:
        desc = "enriched_orders[2]: Writer"
        node = _node("2", desc)
        assert not self.extractor.can_extract(node.description, node)

    def test_sink_flink19_writer_suffix(self) -> None:
        """Flink 1.19 Kafka Sink V2 appends ': Writer' to the topic name."""
        node = _node("2", "KafkaSink-alerts: Writer")
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SINK)
        assert result is not None
        assert result.platform == "kafka"
        assert result.dataset_name == "alerts"

    def test_sink_flink19_indexed_writer_suffix(self) -> None:
        """Flink 1.19 with explicit partition index: KafkaSink-topic[N]: Writer."""
        node = _node("2", "KafkaSink-alerts[0]: Writer")
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SINK)
        assert result is not None
        assert result.platform == "kafka"
        assert result.dataset_name == "alerts"


class TestSqlTableExtractor:
    def setup_method(self) -> None:
        self.extractor = SqlTableExtractor()

    def test_table_source_scan_extracts_catalog_ref(self) -> None:
        desc = "[1]:TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[order_id])"
        node = _node("1", desc)
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SOURCE)
        assert result is not None
        assert result.platform is None
        assert result.dataset_name is None
        assert result.catalog_ref == CatalogTableReference(
            catalog="default_catalog", database="default_database", table="orders"
        )

    def test_table_sink_legacy_extracts_catalog_ref(self) -> None:
        desc = "Sink: Sink(table=[[default_catalog, default_database, enriched_orders]], fields=[order_id])"
        node = _node("2", desc)
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SINK)
        assert result is not None
        assert result.platform is None
        assert result.catalog_ref is not None
        assert result.catalog_ref.table == "enriched_orders"

    def test_sink_single_bracket_dotted_path(self) -> None:
        """JDBC and other Sink V2 connectors use Sink(table=[catalog.db.table]) format."""
        desc = "[3]:Sink(table=[pg_catalog.flink_catalog.public.products], fields=[product_id, title, price, in_stock])"
        node = _node("3", desc)
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SINK)
        assert result is not None
        assert result.catalog_ref is not None
        assert result.catalog_ref.catalog == "pg_catalog"
        assert result.catalog_ref.database == "flink_catalog"
        assert result.catalog_ref.table == "public.products"

    def test_sink_single_bracket_default_catalog(self) -> None:
        """Print/blackhole sinks also use single-bracket format."""
        desc = "[3]:Sink(table=[default_catalog.default_database.temp_sink], fields=[order_id, amount])"
        node = _node("3", desc)
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SINK)
        assert result is not None
        assert result.catalog_ref is not None
        assert result.catalog_ref.catalog == "default_catalog"
        assert result.catalog_ref.database == "default_database"
        assert result.catalog_ref.table == "temp_sink"

    def test_sink_writer_extracts_table_name(self) -> None:
        desc = "enriched_orders[2]: Writer"
        node = _node("2", desc)
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SINK)
        assert result is not None
        assert result.catalog_ref is not None
        assert result.catalog_ref.table == "enriched_orders"
        assert result.catalog_ref.catalog == ""

    def test_sink_writer_hyphenated_table_name(self) -> None:
        desc = "my-sink-table[2]: Writer"
        node = _node("2", desc)
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SINK)
        assert result is not None
        assert result.catalog_ref is not None
        assert result.catalog_ref.table == "my-sink-table"

    def test_sink_writer_with_committer(self) -> None:
        desc = "ds_sink[2]: Writer<br/>   +- ds_sink[2]: Committer"
        node = _node("2", desc)
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SINK)
        assert result is not None
        assert result.catalog_ref is not None
        assert result.catalog_ref.table == "ds_sink"

    def test_table_source_scan_with_schema_prefix(self) -> None:
        """Postgres JDBC tables may have schema prefix in table name (e.g., public.users)."""
        desc = (
            "[1]:TableSourceScan(table=[[pg_catalog, mydb, public.users]], fields=[id])"
        )
        node = _node("1", desc)
        result = self.extractor.extract_dataset(node.description, node, NodeRole.SOURCE)
        assert result is not None
        assert result.catalog_ref is not None
        assert result.catalog_ref.table == "public.users"

    def test_iceberg_sink_extracts_catalog_ref(self) -> None:
        """Iceberg Flink connector produces 'Sink: IcebergSink catalog.db.table'."""
        desc = "IcebergFilesCommitter<br/>+- Sink: IcebergSink ice_catalog.lake.events_copy<br/>+- IcebergStreamWriter"
        node = _node("2", desc)
        result = self.extractor.extract_dataset(desc, node, NodeRole.SINK)
        assert result is not None
        assert result.catalog_ref is not None
        assert result.catalog_ref.catalog == "ice_catalog"
        assert result.catalog_ref.database == "lake"
        assert result.catalog_ref.table == "events_copy"

    def test_does_not_match_datastream_kafka(self) -> None:
        node = _node("1", "Source: KafkaSource-transactions -> Filter")
        assert not self.extractor.can_extract(node.description, node)


class TestFlinkLineageOrchestrator:
    def setup_method(self) -> None:
        self.orchestrator = FlinkLineageOrchestrator()

    def test_datastream_separate_nodes(self) -> None:
        nodes = [
            _node("1", "Source: KafkaSource-transactions -> Filter"),
            _node("2", "KeyBy -> Window -> Aggregate"),
            _node("3", "Sink: KafkaSink-alerts"),
        ]
        result = self.orchestrator.extract(nodes)
        assert len(result.sources) == 1
        assert len(result.sinks) == 1
        assert result.sources[0].dataset_name == "transactions"
        assert result.sinks[0].dataset_name == "alerts"

    def test_multiple_sources_extracted(self) -> None:
        nodes = [
            _node("1", "Source: KafkaSource-orders -> Map"),
            _node("2", "Source: KafkaSource-users -> Map"),
            _node("3", "KeyBy -> Join"),
            _node("4", "Sink: KafkaSink-enriched-orders"),
        ]
        result = self.orchestrator.extract(nodes)
        assert len(result.sources) == 2
        assert len(result.sinks) == 1
        source_names = {s.dataset_name for s in result.sources}
        assert source_names == {"orders", "users"}

    def test_multiple_sinks_extracted(self) -> None:
        nodes = [
            _node("1", "Source: KafkaSource-events -> Map"),
            _node("2", "Sink: KafkaSink-alerts"),
            _node("3", "Sink: KafkaSink-metrics"),
        ]
        result = self.orchestrator.extract(nodes)
        assert len(result.sources) == 1
        assert len(result.sinks) == 2
        sink_names = {s.dataset_name for s in result.sinks}
        assert sink_names == {"alerts", "metrics"}

    def test_sql_table_without_resolver_goes_to_unclassified(self) -> None:
        """SQL/Table API nodes without a PlatformResolver are reported as unresolved."""
        nodes = [
            _node("1", "[1]:TableSourceScan(table=[[cat, db, orders]], fields=[id])"),
            _node("2", "Sink: KafkaSink-output"),
        ]
        result = self.orchestrator.extract(nodes)
        assert not result.sources
        assert len(result.sinks) == 1
        assert len(result.unclassified) == 1
        assert "cat.db.orders" in result.unclassified[0]

    def test_unclassified_when_no_extractor_matches(self) -> None:
        nodes = [
            _node("1", "Source: JdbcSource-my_table -> Map"),
            _node("2", "Sink: KafkaSink-output"),
        ]
        result = self.orchestrator.extract(nodes)
        assert not result.sources
        assert len(result.sinks) == 1
        assert len(result.unclassified) == 1

    def test_transform_only_node_ignored(self) -> None:
        nodes = [_node("1", "KeyBy -> Window -> Aggregate")]
        result = self.orchestrator.extract(nodes)
        assert not result.sources
        assert not result.sinks
        assert not result.unclassified

    def test_extractor_exception_does_not_crash_extraction(self) -> None:
        broken_extractor = MagicMock(spec=LineageExtractor)
        broken_extractor.can_extract.return_value = True
        broken_extractor.extract_dataset.side_effect = RuntimeError("bug")

        orchestrator = FlinkLineageOrchestrator(extractors=[broken_extractor])
        nodes = [_node("1", "Source: KafkaSource-topic1 -> Map")]
        result = orchestrator.extract(nodes)
        assert not result.sources
        assert len(result.unclassified) == 1
        assert len(result.extractor_warnings) == 1
        assert result.extractor_warnings[0].node_id == "1"

    def test_merged_node_datastream_kafka(self) -> None:
        """Merged source+sink with DataStream Kafka operators (legacy format)."""
        desc = "Source: KafkaSource-input -> Filter<br/>Sink: KafkaSink-output"
        nodes = [_node("1", desc)]
        result = self.orchestrator.extract(nodes)
        assert len(result.sources) == 1
        assert len(result.sinks) == 1
        assert result.sources[0].dataset_name == "input"
        assert result.sinks[0].dataset_name == "output"

    def test_merged_node_flink19_datastream_kafka_writer(self) -> None:
        """Flink 1.19: source + sink chained into one node; sink uses ': Writer' suffix."""
        desc = "Source: KafkaSource-transactions -> Filter<br/>KafkaSink-alerts: Writer"
        nodes = [_node("1", desc)]
        result = self.orchestrator.extract(nodes)
        assert len(result.sources) == 1
        assert len(result.sinks) == 1
        assert result.sources[0].dataset_name == "transactions"
        assert result.sinks[0].dataset_name == "alerts"

    def test_merged_node_sql_sink_writer(self) -> None:
        """SQL/Table API: TableSourceScan + SINK_WRITER chained in one node.

        The source goes to unclassified (no platform_resolver). The sink also
        goes to unclassified because SINK_WRITER carries no catalog/database.
        """
        desc = "TableSourceScan(table=[[hive_catalog, flink_db, orders]])<br/>enriched_orders[0]: Writer"
        nodes = [_node("1", desc)]
        result = self.orchestrator.extract(nodes)
        assert not result.sources
        assert not result.sinks
        assert "hive_catalog.flink_db.orders" in result.unclassified

    def test_merged_node_sql_single_bracket(self) -> None:
        """SQL/Table API: TableSourceScan + single-bracket Sink chained in one node.

        Both the source and sink carry full catalog context, so they are
        resolvable by a PlatformResolver. Without one they go to unclassified.
        """
        desc = "TableSourceScan(table=[[pg_catalog, mydb, users]])<br/>Sink(table=[pg_catalog.mydb.output])"
        nodes = [_node("1", desc)]
        result = self.orchestrator.extract(nodes)
        assert not result.sources
        assert not result.sinks
        assert "pg_catalog.mydb.users" in result.unclassified
        assert "pg_catalog.mydb.output" in result.unclassified

    def test_sql_table_resolved_with_platform_resolver(self) -> None:
        """End-to-end: SQL table node resolved via PlatformResolver."""
        mock_resolver = MagicMock(spec=PlatformResolver)
        mock_resolver.resolve.return_value = ClassifiedNode(
            node_id="",
            description="",
            role=NodeRole.SOURCE,
            platform="postgres",
            dataset_name="mydb.public.users",
            catalog_ref=CatalogTableReference(
                catalog="pg_catalog", database="mydb", table="public.users"
            ),
        )
        orchestrator = FlinkLineageOrchestrator(platform_resolver=mock_resolver)

        nodes = [
            _node(
                "1",
                "[1]:TableSourceScan(table=[[pg_catalog, mydb, public.users]], fields=[id])",
            ),
            _node("2", "Sink: KafkaSink-output"),
        ]
        result = orchestrator.extract(nodes)

        assert len(result.sources) == 1
        assert result.sources[0].platform == "postgres"
        assert result.sources[0].dataset_name == "mydb.public.users"
        assert len(result.sinks) == 1
