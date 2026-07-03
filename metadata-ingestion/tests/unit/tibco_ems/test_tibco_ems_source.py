from typing import Dict, List, Optional
from unittest.mock import MagicMock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.tibco_ems.models import (
    BridgeTarget,
    DestinationType,
    TibcoBridge,
    TibcoDestination,
)
from datahub.ingestion.source.tibco_ems.source import TibcoEmsSource
from datahub.metadata.schema_classes import (
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    UpstreamLineageClass,
)

_BASE_URL = "https://ems.example.com:8080"


class _FakeGraph:
    # Returns a schema only for urns present in the supplied map, mimicking the graph
    # having schemas for some destinations (e.g. populated by a schema-registry source)
    # and none for others.
    def __init__(self, fields_by_urn: Dict[str, List[str]]) -> None:
        self._fields_by_urn = fields_by_urn

    def get_schema_metadata(self, entity_urn: str) -> Optional[SchemaMetadataClass]:
        fields = self._fields_by_urn.get(entity_urn)
        if fields is None:
            return None
        return SchemaMetadataClass(
            schemaName="t",
            platform="urn:li:dataPlatform:tibco-ems",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[
                SchemaFieldClass(
                    fieldPath=name,
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="string",
                )
                for name in fields
            ],
        )


def _source(**config_overrides: object) -> TibcoEmsSource:
    config = {
        "base_url": _BASE_URL,
        "username": "u",
        "password": "p",
        "stateful_ingestion": {"enabled": False},
        **config_overrides,
    }
    return TibcoEmsSource.create(config, PipelineContext(run_id="test"))


def _queue(name: str) -> TibcoDestination:
    return TibcoDestination(name=name, destination_type=DestinationType.QUEUE)


def _topic(name: str) -> TibcoDestination:
    return TibcoDestination(name=name, destination_type=DestinationType.TOPIC)


def test_dataset_name_prefixed_by_type() -> None:
    source = _source()
    assert (
        source._dataset_name(DestinationType.QUEUE, "orders.new") == "queue.orders.new"
    )
    assert (
        source._dataset_name(DestinationType.TOPIC, "orders.new") == "topic.orders.new"
    )


def test_queue_and_topic_same_name_have_distinct_urns() -> None:
    source = _source()
    queue_urn = source._dataset_urn(source._dataset_name(DestinationType.QUEUE, "x"))
    topic_urn = source._dataset_urn(source._dataset_name(DestinationType.TOPIC, "x"))
    assert queue_urn != topic_urn


def test_custom_properties_formatting() -> None:
    source = _source()
    destination = TibcoDestination(
        name="q",
        destination_type=DestinationType.QUEUE,
        is_global=True,
        max_msgs=10,
        prefetch=None,
    )
    props = source._custom_properties(destination)
    assert props["destination_type"] == "queue"
    assert props["global"] == "true"
    assert props["max_msgs"] == "10"
    assert "prefetch" not in props


def test_system_destination_filtered_by_default() -> None:
    source = _source()
    assert source._allowed(_queue("$sys.admin")) is False
    assert source._allowed(_queue("orders.new")) is True


def test_system_destination_included_when_configured() -> None:
    source = _source(include_system_destinations=True)
    assert source._allowed(_queue("$sys.admin")) is True


def test_queue_pattern_applies_only_to_queues() -> None:
    source = _source(queue_pattern={"deny": ["orders.*"]})
    assert source._allowed(_queue("orders.new")) is False
    assert source._allowed(_topic("orders.new")) is True


def _orders_to_audit_bridge() -> TibcoBridge:
    return TibcoBridge(
        source_name="orders.new",
        source_type=DestinationType.QUEUE,
        targets=[
            BridgeTarget(name="events.audit", destination_type=DestinationType.TOPIC)
        ],
    )


def _mock_client(
    source: TibcoEmsSource,
    *,
    queues: List[TibcoDestination],
    topics: List[TibcoDestination],
    bridges: List[TibcoBridge],
) -> MagicMock:
    client = MagicMock()
    client.fetch_queues.return_value = queues
    client.fetch_topics.return_value = topics
    client.fetch_bridges.return_value = bridges
    source.client = client
    return client


def _lineage_workunits(source: TibcoEmsSource) -> List[MetadataWorkUnit]:
    return [
        wu
        for wu in source.get_workunits_internal()
        if isinstance(getattr(wu.metadata, "aspect", None), UpstreamLineageClass)
    ]


def test_bridge_lineage_emitted_between_ingested_destinations() -> None:
    source = _source()
    _mock_client(
        source,
        queues=[_queue("orders.new")],
        topics=[_topic("events.audit")],
        bridges=[_orders_to_audit_bridge()],
    )

    lineage = _lineage_workunits(source)
    assert len(lineage) == 1
    entity_urn = lineage[0].metadata.entityUrn  # type: ignore[union-attr]
    assert entity_urn is not None and "topic.events.audit" in entity_urn
    aspect = lineage[0].metadata.aspect  # type: ignore[union-attr]
    assert isinstance(aspect, UpstreamLineageClass)
    assert "queue.orders.new" in aspect.upstreams[0].dataset
    assert source.report.lineage_edges_emitted == 1


def test_bridge_to_filtered_destination_still_emits_lineage() -> None:
    # A concrete endpoint excluded from dataset ingestion still has a
    # deterministic urn (same platform/instance/env), so lineage is emitted.
    source = _source(topic_pattern={"deny": [".*"]})
    _mock_client(
        source,
        queues=[_queue("orders.new")],
        topics=[_topic("events.audit")],
        bridges=[_orders_to_audit_bridge()],
    )

    lineage = _lineage_workunits(source)
    assert len(lineage) == 1
    aspect = lineage[0].metadata.aspect  # type: ignore[union-attr]
    assert isinstance(aspect, UpstreamLineageClass)
    assert "queue.orders.new" in aspect.upstreams[0].dataset
    assert source.report.lineage_edges_emitted == 1
    assert source.report.lineage_edges_unresolved == 0


def test_bridge_wildcard_endpoint_is_unresolved() -> None:
    source = _source()
    _mock_client(
        source,
        queues=[_queue("orders.new")],
        topics=[],
        bridges=[
            TibcoBridge(
                source_name="orders.new",
                source_type=DestinationType.QUEUE,
                targets=[
                    BridgeTarget(
                        name="events.>", destination_type=DestinationType.TOPIC
                    )
                ],
            )
        ],
    )

    list(source.get_workunits_internal())
    assert source.report.lineage_edges_emitted == 0
    assert source.report.lineage_edges_unresolved == 1
    assert "events.>" in source.report.unresolved_bridge_endpoints


def test_bridges_skipped_when_disabled() -> None:
    source = _source(include_bridges=False)
    client = _mock_client(source, queues=[_queue("orders.new")], topics=[], bridges=[])

    list(source.get_workunits_internal())
    client.fetch_bridges.assert_not_called()
    assert source.report.datasets_emitted == 1


def _orders_urn(source: TibcoEmsSource) -> str:
    return source._dataset_urn(
        source._dataset_name(DestinationType.QUEUE, "orders.new")
    )


def _audit_urn(source: TibcoEmsSource) -> str:
    return source._dataset_urn(
        source._dataset_name(DestinationType.TOPIC, "events.audit")
    )


def test_bridge_column_lineage_matches_shared_fields() -> None:
    source = _source(emit_column_lineage=True)
    _mock_client(
        source,
        queues=[_queue("orders.new")],
        topics=[_topic("events.audit")],
        bridges=[_orders_to_audit_bridge()],
    )
    # Source and target share id + payload; target's extra "ts" has no match.
    source.ctx.graph = _FakeGraph(  # type: ignore[assignment]
        {
            _orders_urn(source): ["id", "payload"],
            _audit_urn(source): ["id", "payload", "ts"],
        }
    )

    lineage = _lineage_workunits(source)
    aspect = lineage[0].metadata.aspect  # type: ignore[union-attr]
    assert isinstance(aspect, UpstreamLineageClass)
    assert aspect.fineGrainedLineages is not None
    downstreams = {
        fine.downstreams[0]  # type: ignore[index]
        for fine in aspect.fineGrainedLineages
    }
    assert downstreams == {
        f"urn:li:schemaField:({_audit_urn(source)},id)",
        f"urn:li:schemaField:({_audit_urn(source)},payload)",
    }
    assert source.report.column_lineage_edges_emitted == 2


def test_bridge_column_lineage_matches_across_casing() -> None:
    source = _source(emit_column_lineage=True)
    _mock_client(
        source,
        queues=[_queue("orders.new")],
        topics=[_topic("events.audit")],
        bridges=[_orders_to_audit_bridge()],
    )
    # Same field, different casing on each side; urns must keep each side's real case.
    source.ctx.graph = _FakeGraph(  # type: ignore[assignment]
        {
            _orders_urn(source): ["OrderId"],
            _audit_urn(source): ["orderid"],
        }
    )

    lineage = _lineage_workunits(source)
    aspect = lineage[0].metadata.aspect  # type: ignore[union-attr]
    assert isinstance(aspect, UpstreamLineageClass)
    assert aspect.fineGrainedLineages is not None
    fine = aspect.fineGrainedLineages[0]
    assert fine.upstreams == [f"urn:li:schemaField:({_orders_urn(source)},OrderId)"]
    assert fine.downstreams == [f"urn:li:schemaField:({_audit_urn(source)},orderid)"]
    assert source.report.column_lineage_edges_emitted == 1


def test_bridge_column_lineage_skipped_without_schema() -> None:
    source = _source(emit_column_lineage=True)
    _mock_client(
        source,
        queues=[_queue("orders.new")],
        topics=[_topic("events.audit")],
        bridges=[_orders_to_audit_bridge()],
    )
    # Only the source has a schema in the graph; nothing to match against.
    source.ctx.graph = _FakeGraph({_orders_urn(source): ["id"]})  # type: ignore[assignment]

    lineage = _lineage_workunits(source)
    aspect = lineage[0].metadata.aspect  # type: ignore[union-attr]
    assert isinstance(aspect, UpstreamLineageClass)
    assert aspect.fineGrainedLineages is None
    assert source.report.lineage_edges_emitted == 1
    assert source.report.column_lineage_edges_emitted == 0


def test_bridge_column_lineage_disabled_by_default() -> None:
    source = _source()  # emit_column_lineage defaults to False
    _mock_client(
        source,
        queues=[_queue("orders.new")],
        topics=[_topic("events.audit")],
        bridges=[_orders_to_audit_bridge()],
    )
    source.ctx.graph = _FakeGraph(  # type: ignore[assignment]
        {
            _orders_urn(source): ["id"],
            _audit_urn(source): ["id"],
        }
    )

    lineage = _lineage_workunits(source)
    aspect = lineage[0].metadata.aspect  # type: ignore[union-attr]
    assert isinstance(aspect, UpstreamLineageClass)
    assert aspect.fineGrainedLineages is None
    assert source.report.column_lineage_edges_emitted == 0
