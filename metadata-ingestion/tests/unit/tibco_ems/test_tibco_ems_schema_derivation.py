from typing import Dict, List, Optional, Sequence, Tuple
from unittest.mock import MagicMock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.tibco_ems.constants import (
    DEFAULT_SERVER_GROUP,
    PROPERTY_SCHEMA_SOURCE,
    SCHEMA_SOURCE_DERIVED,
)
from datahub.ingestion.source.tibco_ems.models import (
    DestinationType,
    TibcoBridge,
    TibcoDestination,
    TibcoEmsListing,
)
from datahub.ingestion.source.tibco_ems.source import TibcoEmsSource
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)

_BASE_URL = "https://ems.example.com:8080"
_LANDING = "urn:li:dataset:(urn:li:dataPlatform:databricks,bronze.orders_raw,PROD)"
_WAREHOUSE = "urn:li:dataset:(urn:li:dataPlatform:databricks,silver.orders,PROD)"


class _RelatedEntity:
    def __init__(self, urn: str) -> None:
        self.urn = urn


class _FakeGraph:
    """Stands in for DataHub: who reads a destination, and what shape they landed.

    Consumers are keyed by the destination urn because derivation walks incoming
    `DownstreamOf` edges - the same direction the real graph reports them in.
    """

    def __init__(
        self,
        consumers: Dict[str, List[str]],
        fields: Dict[str, Sequence[Tuple[str, str]]],
        properties: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> None:
        self._consumers = consumers
        self._fields = fields
        self._properties = properties or {}

    def get_related_entities(
        self, entity_urn: str, relationship_types: List[str], direction: object
    ) -> List[_RelatedEntity]:
        return [_RelatedEntity(urn) for urn in self._consumers.get(entity_urn, [])]

    def get_schema_metadata(self, entity_urn: str) -> Optional[SchemaMetadataClass]:
        fields = self._fields.get(entity_urn)
        if fields is None:
            return None
        return SchemaMetadataClass(
            schemaName="t",
            platform="urn:li:dataPlatform:databricks",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[
                SchemaFieldClass(
                    fieldPath=name,
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType=native,
                )
                for name, native in fields
            ],
        )

    def get_aspect(self, entity_urn: str, aspect_type: object) -> object:
        properties = self._properties.get(entity_urn)
        if properties is None:
            return None
        return DatasetPropertiesClass(customProperties=properties)


def _source(**overrides: object) -> TibcoEmsSource:
    config = {
        "base_url": _BASE_URL,
        "username": "u",
        "password": "p",
        "stateful_ingestion": {"enabled": False},
        **overrides,
    }
    return TibcoEmsSource.create(config, PipelineContext(run_id="test"))


def _with_orders_queue(source: TibcoEmsSource) -> None:
    client = MagicMock()
    client.fetch_queues.return_value = TibcoEmsListing[TibcoDestination](
        records=[
            TibcoDestination(
                name="orders.new",
                destination_type=DestinationType.QUEUE,
                server_group=DEFAULT_SERVER_GROUP,
            )
        ]
    )
    client.fetch_topics.return_value = TibcoEmsListing[TibcoDestination](records=[])
    client.fetch_bridges.return_value = TibcoEmsListing[TibcoBridge](records=[])
    source.client = client


def _orders_urn(source: TibcoEmsSource) -> str:
    return source._dataset_urn(
        source._dataset_name(DEFAULT_SERVER_GROUP, DestinationType.QUEUE, "orders.new")
    )


def _emitted_schema(source: TibcoEmsSource) -> Optional[SchemaMetadataClass]:
    for workunit in source.get_workunits_internal():
        aspect = workunit.metadata.aspect  # type: ignore[union-attr]
        if isinstance(aspect, SchemaMetadataClass):
            return aspect
    return None


def _emitted_properties(source: TibcoEmsSource) -> Dict[str, str]:
    for workunit in source.get_workunits_internal():
        aspect = workunit.metadata.aspect  # type: ignore[union-attr]
        if isinstance(aspect, DatasetPropertiesClass):
            return aspect.customProperties or {}
    return {}


def _string(*names: str) -> Sequence[Tuple[str, str]]:
    return [(name, "string") for name in names]


def test_derived_schema_unions_fields_across_consumers() -> None:
    # Union, not intersection: consumers keep different subsets of a message, and
    # anything one of them landed must have been on the wire.
    source = _source(derive_schemas_from_lineage=True)
    _with_orders_queue(source)
    source.ctx.graph = _FakeGraph(  # type: ignore[assignment]
        consumers={_orders_urn(source): [_LANDING, _WAREHOUSE]},
        fields={
            _LANDING: _string("orderId", "grossAmount"),
            _WAREHOUSE: _string("orderId", "customerRef"),
        },
    )

    schema = _emitted_schema(source)
    assert schema is not None
    assert {f.fieldPath for f in schema.fields} == {
        "orderId",
        "grossAmount",
        "customerRef",
    }
    assert source.report.derived_schemas_emitted == 1
    assert source.report.derived_schema_fields_emitted == 3


def test_derived_schema_excludes_pipeline_generated_columns() -> None:
    # A column the landing job writes was never on the message; carrying it back
    # would assert the bus published a value the pipeline invented.
    source = _source(derive_schemas_from_lineage=True)
    _with_orders_queue(source)
    source.ctx.graph = _FakeGraph(  # type: ignore[assignment]
        consumers={_orders_urn(source): [_LANDING]},
        fields={
            _LANDING: _string("orderId", "ingested_at", "_source_topic", "etl_batch_id")
        },
    )

    schema = _emitted_schema(source)
    assert schema is not None
    assert [f.fieldPath for f in schema.fields] == ["orderId"]
    assert source.report.derived_fields_excluded == 3


def test_derived_schema_is_marked_as_derived() -> None:
    # Downstream-shaped by construction, so it must never read as a contract.
    source = _source(derive_schemas_from_lineage=True)
    _with_orders_queue(source)
    source.ctx.graph = _FakeGraph(  # type: ignore[assignment]
        consumers={_orders_urn(source): [_LANDING]},
        fields={_LANDING: _string("orderId")},
    )

    assert _emitted_properties(source)[PROPERTY_SCHEMA_SOURCE] == SCHEMA_SOURCE_DERIVED


def test_declared_schema_is_never_replaced_by_a_derived_one() -> None:
    # A schema the publisher declared knows what was sent; this one only knows what
    # was kept. The declared one wins.
    source = _source(derive_schemas_from_lineage=True)
    _with_orders_queue(source)
    orders = _orders_urn(source)
    source.ctx.graph = _FakeGraph(  # type: ignore[assignment]
        consumers={orders: [_LANDING]},
        fields={orders: _string("declaredField"), _LANDING: _string("orderId")},
        properties={orders: {PROPERTY_SCHEMA_SOURCE: "tibco-bw-ear"}},
    )

    assert _emitted_schema(source) is None
    assert source.report.derived_schemas_emitted == 0


def test_previously_derived_schema_is_refreshed() -> None:
    # Its inputs move as consumers change, so a derived schema is not left frozen.
    source = _source(derive_schemas_from_lineage=True)
    _with_orders_queue(source)
    orders = _orders_urn(source)
    source.ctx.graph = _FakeGraph(  # type: ignore[assignment]
        consumers={orders: [_LANDING]},
        fields={orders: _string("stale"), _LANDING: _string("orderId")},
        properties={orders: {PROPERTY_SCHEMA_SOURCE: SCHEMA_SOURCE_DERIVED}},
    )

    schema = _emitted_schema(source)
    assert schema is not None
    assert [f.fieldPath for f in schema.fields] == ["orderId"]


def test_schema_of_unknown_origin_is_left_alone() -> None:
    # No provenance recorded means no way to tell whose schema it is. Overwriting
    # someone else's silently is the worse failure.
    source = _source(derive_schemas_from_lineage=True)
    _with_orders_queue(source)
    orders = _orders_urn(source)
    source.ctx.graph = _FakeGraph(  # type: ignore[assignment]
        consumers={orders: [_LANDING]},
        fields={orders: _string("someoneElsesField"), _LANDING: _string("orderId")},
    )

    assert _emitted_schema(source) is None


def test_destination_without_consumers_is_reported() -> None:
    # Nothing downstream is in DataHub yet, so there is no evidence to work from.
    source = _source(derive_schemas_from_lineage=True)
    _with_orders_queue(source)
    source.ctx.graph = _FakeGraph(consumers={}, fields={})  # type: ignore[assignment]

    assert _emitted_schema(source) is None
    assert "orders.new" in source.report.destinations_without_consumers


def test_conflicting_field_types_keep_the_first_and_report() -> None:
    # Nothing on the bus itself breaks the tie, so the conflict is surfaced rather
    # than silently resolved.
    source = _source(derive_schemas_from_lineage=True)
    _with_orders_queue(source)
    source.ctx.graph = _FakeGraph(  # type: ignore[assignment]
        consumers={_orders_urn(source): [_LANDING, _WAREHOUSE]},
        fields={
            _LANDING: [("grossAmount", "string")],
            _WAREHOUSE: [("grossAmount", "decimal")],
        },
    )

    schema = _emitted_schema(source)
    assert schema is not None
    assert schema.fields[0].nativeDataType == "string"
    assert source.report.derived_field_type_conflicts


def test_derivation_is_disabled_by_default() -> None:
    source = _source()
    _with_orders_queue(source)
    source.ctx.graph = _FakeGraph(  # type: ignore[assignment]
        consumers={_orders_urn(source): [_LANDING]},
        fields={_LANDING: _string("orderId")},
    )

    assert _emitted_schema(source) is None
    assert source.report.derived_schemas_emitted == 0


def test_derivation_without_a_graph_warns_rather_than_failing() -> None:
    source = _source(derive_schemas_from_lineage=True)
    _with_orders_queue(source)

    assert _emitted_schema(source) is None
    assert source.report.warnings
    assert source.report.datasets_emitted == 1
