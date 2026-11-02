from typing import Any, Dict, Iterator, List, Optional

from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.microstrategy.client import MicroStrategyAPIError
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.lineage import MicroStrategyLineageExtractor
from datahub.ingestion.source.microstrategy.microstrategy_semantic_model import (
    MicroStrategySemanticModelMapper,
)
from datahub.ingestion.source.microstrategy.models import MicroStrategyObject
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.metadata.schema_classes import (
    ERModelRelationshipCardinalityClass,
    MetricInfoClass,
    MetricRelationshipsClass,
    MetricUpstreamsClass,
    SchemaMetadataClass,
    SemanticFieldAnnotationClass,
    SemanticFieldTypeClass,
    SemanticModelInfoClass,
    SemanticModelPropertiesClass,
)
from datahub.metadata.urns import SchemaFieldUrn


class _ProjectKey(ContainerKey):
    project_id: str


class FakeSemanticModelClient:
    def __init__(
        self,
        *,
        metrics: Optional[List[MicroStrategyObject]] = None,
        metric_models: Optional[Dict[str, Dict[str, Any]]] = None,
        attribute_relationships: Optional[Dict[str, Dict[str, Any]]] = None,
        consolidation_models: Optional[Dict[str, Dict[str, Any]]] = None,
        search_metrics_fails: bool = False,
    ) -> None:
        self._metrics = metrics or []
        self._metric_models = metric_models or {}
        self._attribute_relationships = attribute_relationships or {}
        self._consolidation_models = consolidation_models or {}
        self._search_metrics_fails = search_metrics_fails
        self.attribute_relationship_calls: List[str] = []
        self.consolidation_calls: List[str] = []

    def search_metrics(self, project_id: str) -> Iterator[MicroStrategyObject]:
        if self._search_metrics_fails:
            raise MicroStrategyAPIError("metric search unavailable")
        return iter(self._metrics)

    def get_metric_model(self, project_id: str, metric_id: str) -> Dict[str, Any]:
        return self._metric_models.get(metric_id, {})

    def get_attribute_relationships(
        self, project_id: str, attribute_id: str
    ) -> Dict[str, Any]:
        self.attribute_relationship_calls.append(attribute_id)
        if attribute_id in self._attribute_relationships:
            return self._attribute_relationships[attribute_id]
        raise MicroStrategyAPIError(f"no fixture for {attribute_id}")

    def get_consolidation_model(
        self, project_id: str, consolidation_id: str
    ) -> Dict[str, Any]:
        self.consolidation_calls.append(consolidation_id)
        if consolidation_id in self._consolidation_models:
            return self._consolidation_models[consolidation_id]
        raise MicroStrategyAPIError(f"no fixture for {consolidation_id}")


def _config(**overrides: object) -> MicroStrategyConfig:
    base: Dict[str, object] = {
        "base_url": "https://mstr.example.com/MicroStrategyLibrary",
        "emit_semantic_model_entities": True,
    }
    base.update(overrides)
    return MicroStrategyConfig.model_validate(base)


def _mapper(
    client: FakeSemanticModelClient, **config_overrides: object
) -> MicroStrategySemanticModelMapper:
    config = _config(**config_overrides)
    report = MicroStrategyReport()
    return MicroStrategySemanticModelMapper(
        config=config,
        report=report,
        client=client,  # type: ignore[arg-type]
        lineage=MicroStrategyLineageExtractor(config, report),
    )


def _project_container_key() -> ContainerKey:
    return _ProjectKey(
        platform="microstrategy",
        instance=None,
        env="PROD",
        project_id="project-1",
    )


def _aspects_by_urn(wus: List[MetadataWorkUnit]) -> Dict[str, Dict[str, object]]:
    out: Dict[str, Dict[str, object]] = {}
    for wu in wus:
        urn = wu.get_urn()
        aspect = wu.metadata.aspect  # type: ignore[union-attr]
        if aspect is None:
            continue
        out.setdefault(urn, {})[type(aspect).__name__] = aspect
    return out


def _attribute(
    object_id: str, name: str, description: Optional[str] = None
) -> Dict[str, Any]:
    information: Dict[str, Any] = {"objectId": object_id, "name": name}
    if description:
        information["description"] = description
    return {"information": information, "forms": []}


def _fact(object_id: str, name: str) -> Dict[str, Any]:
    return {"information": {"objectId": object_id, "name": name}}


def _table(
    table_name: str,
    attributes: List[Dict[str, Any]],
    facts: List[Dict[str, Any]],
    logical_table_id: Optional[str] = None,
    logical_table_name: Optional[str] = None,
    physical_table_name: Optional[str] = None,
) -> Dict[str, Any]:
    # The logical table (this dict's own top-level "information") and the
    # physical table it maps to (nested under "physicalTable") are distinct
    # MicroStrategy objects with distinct object ids -- the attribute
    # hierarchy API's relationshipTable refers to the logical table, and more
    # than one logical table can map to the same physical table (role-
    # playing). Defaulting the logical id/name to the table_name keeps
    # existing single-role fixtures unchanged while still exercising real,
    # distinct ids end to end.
    return {
        "information": {
            "objectId": logical_table_id or f"LGT-{table_name}",
            "name": logical_table_name or table_name,
        },
        "physicalTable": {
            "tableName": physical_table_name or table_name,
            "information": {
                "objectId": f"TBL-{physical_table_name or table_name}",
                "name": physical_table_name or table_name,
            },
        },
        "attributes": attributes,
        "facts": facts,
    }


def test_table_becomes_semantic_model_dataset_with_dimension_and_measure_fields() -> (
    None
):
    mapper = _mapper(FakeSemanticModelClient())
    model_tables = [
        _table(
            "SALES",
            attributes=[_attribute("ATTR1", "Region")],
            facts=[_fact("FACT1", "Revenue")],
        )
    ]

    aspects = _aspects_by_urn(
        list(
            mapper.emit(
                project_id="project-1",
                project_name="Project One",
                model_tables=model_tables,
                warehouse_context=None,
                project_container_key=_project_container_key(),
            )
        )
    )

    model_urn = (
        "urn:li:semanticModel:(urn:li:dataPlatform:microstrategy,project-1,schema)"
    )
    assert model_urn in aspects
    info = aspects[model_urn]["SemanticModelInfoClass"]
    assert isinstance(info, SemanticModelInfoClass)
    assert info.name == "Project One"

    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:microstrategy,project-1.SALES,PROD)"
    )
    assert dataset_urn in aspects
    assert aspects[dataset_urn]["SubTypesClass"].typeNames == [  # type: ignore[attr-defined]
        DatasetSubTypes.SEMANTIC_MODEL_DATASET
    ]
    props = aspects[dataset_urn]["SemanticModelPropertiesClass"]
    assert isinstance(props, SemanticModelPropertiesClass)
    assert props.alias == "SALES"
    assert props.semanticModel == model_urn

    schema_meta = aspects[dataset_urn]["SchemaMetadataClass"]
    assert isinstance(schema_meta, SchemaMetadataClass)
    field_paths = {f.fieldPath for f in schema_meta.fields}
    assert field_paths == {"Region", "Revenue"}

    region_field_urn = SchemaFieldUrn(dataset_urn, "Region").urn()
    revenue_field_urn = SchemaFieldUrn(dataset_urn, "Revenue").urn()
    region_annotation = aspects[region_field_urn]["SemanticFieldAnnotationClass"]
    revenue_annotation = aspects[revenue_field_urn]["SemanticFieldAnnotationClass"]
    assert isinstance(region_annotation, SemanticFieldAnnotationClass)
    assert isinstance(revenue_annotation, SemanticFieldAnnotationClass)
    assert region_annotation.type == SemanticFieldTypeClass.DIMENSION
    assert revenue_annotation.type == SemanticFieldTypeClass.MEASURE


def test_table_with_no_usable_fields_is_skipped_with_warning() -> None:
    mapper = _mapper(FakeSemanticModelClient())
    model_tables = [_table("EMPTY", attributes=[], facts=[])]

    workunits = list(
        mapper.emit(
            project_id="project-1",
            project_name="Project One",
            model_tables=model_tables,
            warehouse_context=None,
            project_container_key=_project_container_key(),
        )
    )

    assert workunits == []
    assert mapper.report.warnings


def test_role_playing_logical_tables_get_distinct_dataset_identities() -> None:
    # Regression: two logical tables role-playing one physical table (e.g.
    # "Order Date"/"Ship Date" both backed by one shared date-lookup table)
    # used to collide on the physical table name as their dataset alias, so
    # the second table's SemanticModelDataset silently overwrote the first's
    # under one URN.
    mapper = _mapper(FakeSemanticModelClient())
    model_tables = [
        _table(
            "ORDER_DATE",
            attributes=[_attribute("ATTR-ORDER-DATE", "Order Date")],
            facts=[],
            logical_table_id="LGT-ORDER-DATE",
            logical_table_name="Order Date",
            physical_table_name="LU_DATE",
        ),
        _table(
            "SHIP_DATE",
            attributes=[_attribute("ATTR-SHIP-DATE", "Ship Date")],
            facts=[],
            logical_table_id="LGT-SHIP-DATE",
            logical_table_name="Ship Date",
            physical_table_name="LU_DATE",
        ),
    ]

    aspects = _aspects_by_urn(
        list(
            mapper.emit(
                project_id="project-1",
                project_name="Project One",
                model_tables=model_tables,
                warehouse_context=None,
                project_container_key=_project_container_key(),
            )
        )
    )

    order_date_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:microstrategy,project-1.Order Date,PROD)"
    )
    ship_date_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:microstrategy,project-1.Ship Date,PROD)"
    )
    assert order_date_urn in aspects
    assert ship_date_urn in aspects

    order_schema = aspects[order_date_urn]["SchemaMetadataClass"]
    ship_schema = aspects[ship_date_urn]["SchemaMetadataClass"]
    assert isinstance(order_schema, SchemaMetadataClass)
    assert isinstance(ship_schema, SchemaMetadataClass)
    assert {f.fieldPath for f in order_schema.fields} == {"Order Date"}
    assert {f.fieldPath for f in ship_schema.fields} == {"Ship Date"}


def test_relationship_emitted_for_attribute_shared_across_two_tables() -> None:
    client = FakeSemanticModelClient(
        attribute_relationships={
            "ATTR-SHARED": {
                "relationships": [
                    {
                        "parent": {"objectId": "ATTR-SHARED", "subType": "attribute"},
                        "child": {"objectId": "ATTR-CHILD", "subType": "attribute"},
                        "relationshipTable": {
                            "objectId": "LGT-PARENT",
                            "subType": "logical_table",
                        },
                        "relationshipType": "one_to_many",
                    }
                ]
            }
        }
    )
    mapper = _mapper(client)
    model_tables = [
        _table(
            "PARENT",
            attributes=[_attribute("ATTR-SHARED", "Quarter")],
            facts=[],
        ),
        _table(
            "CHILD",
            attributes=[
                _attribute("ATTR-SHARED", "Quarter"),
                _attribute("ATTR-CHILD", "Month"),
            ],
            facts=[],
        ),
    ]

    aspects = _aspects_by_urn(
        list(
            mapper.emit(
                project_id="project-1",
                project_name="Project One",
                model_tables=model_tables,
                warehouse_context=None,
                project_container_key=_project_container_key(),
            )
        )
    )

    model_urn = (
        "urn:li:semanticModel:(urn:li:dataPlatform:microstrategy,project-1,schema)"
    )
    info = aspects[model_urn]["SemanticModelInfoClass"]
    assert isinstance(info, SemanticModelInfoClass)
    assert info.relationships
    relationship = info.relationships[0]
    assert relationship.from_ == "CHILD"
    assert relationship.to == "PARENT"
    assert relationship.fromColumns == ["Month"]
    assert relationship.toColumns == ["Quarter"]
    # "one_to_many" parent->child is inverted for the child->parent edge this
    # module emits, so it becomes many-to-one (N_ONE).
    assert relationship.cardinality == ERModelRelationshipCardinalityClass.N_ONE
    assert client.attribute_relationship_calls == ["ATTR-SHARED"]


def test_attribute_on_single_table_never_triggers_relationship_fetch() -> None:
    client = FakeSemanticModelClient()
    mapper = _mapper(client)
    model_tables = [
        _table("ONLY", attributes=[_attribute("ATTR1", "Region")], facts=[])
    ]

    list(
        mapper.emit(
            project_id="project-1",
            project_name="Project One",
            model_tables=model_tables,
            warehouse_context=None,
            project_container_key=_project_container_key(),
        )
    )

    assert client.attribute_relationship_calls == []


def test_relationship_fetch_failure_warns_and_is_skipped() -> None:
    client = FakeSemanticModelClient()  # no fixture -> raises MicroStrategyAPIError
    mapper = _mapper(client)
    model_tables = [
        _table("A", attributes=[_attribute("ATTR-SHARED", "X")], facts=[]),
        _table("B", attributes=[_attribute("ATTR-SHARED", "X")], facts=[]),
    ]

    workunits = list(
        mapper.emit(
            project_id="project-1",
            project_name="Project One",
            model_tables=model_tables,
            warehouse_context=None,
            project_container_key=_project_container_key(),
        )
    )

    assert workunits  # datasets/model still emitted
    assert mapper.report.semantic_model_attribute_relationship_api_failures == 1


def test_unrecognized_relationship_type_warns_but_still_emits_relationship() -> None:
    client = FakeSemanticModelClient(
        attribute_relationships={
            "ATTR-SHARED": {
                "relationships": [
                    {
                        "parent": {"objectId": "ATTR-SHARED", "subType": "attribute"},
                        "child": {"objectId": "ATTR-CHILD", "subType": "attribute"},
                        "relationshipTable": {"objectId": "LGT-PARENT"},
                        "relationshipType": "weird_type",
                    }
                ]
            }
        }
    )
    mapper = _mapper(client)
    model_tables = [
        _table("PARENT", attributes=[_attribute("ATTR-SHARED", "Quarter")], facts=[]),
        _table(
            "CHILD",
            attributes=[
                _attribute("ATTR-SHARED", "Quarter"),
                _attribute("ATTR-CHILD", "Month"),
            ],
            facts=[],
        ),
    ]

    aspects = _aspects_by_urn(
        list(
            mapper.emit(
                project_id="project-1",
                project_name="Project One",
                model_tables=model_tables,
                warehouse_context=None,
                project_container_key=_project_container_key(),
            )
        )
    )

    model_urn = (
        "urn:li:semanticModel:(urn:li:dataPlatform:microstrategy,project-1,schema)"
    )
    info = aspects[model_urn]["SemanticModelInfoClass"]
    assert isinstance(info, SemanticModelInfoClass)
    assert info.relationships
    assert info.relationships[0].cardinality is None
    assert mapper.report.warnings


def _metric_object(metric_id: str, name: str) -> MicroStrategyObject:
    return MicroStrategyObject.model_validate({"id": metric_id, "name": name})


def test_base_metric_gets_upstream_datasets_and_no_derived_from() -> None:
    client = FakeSemanticModelClient(
        metrics=[_metric_object("METRIC1", "Revenue Metric")],
        metric_models={
            "METRIC1": {
                "expression": {
                    "text": "Sum(Revenue)",
                    "tokens": [{"objectId": "FACT1", "subType": "fact"}],
                }
            }
        },
    )
    mapper = _mapper(client)
    model_tables = [_table("SALES", attributes=[], facts=[_fact("FACT1", "Revenue")])]

    aspects = _aspects_by_urn(
        list(
            mapper.emit(
                project_id="project-1",
                project_name="Project One",
                model_tables=model_tables,
                warehouse_context=None,
                project_container_key=_project_container_key(),
            )
        )
    )

    metric_urn = "urn:li:metric:(urn:li:dataPlatform:microstrategy,project-1,METRIC1)"
    assert metric_urn in aspects
    metric_info = aspects[metric_urn]["MetricInfoClass"]
    assert isinstance(metric_info, MetricInfoClass)
    assert metric_info.expression is not None
    upstreams = aspects[metric_urn]["MetricUpstreamsClass"]
    assert isinstance(upstreams, MetricUpstreamsClass)
    assert upstreams.datasetUpstreams
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:microstrategy,project-1.SALES,PROD)"
    )
    assert upstreams.datasetUpstreams[0].destinationUrn == dataset_urn
    relationships = aspects[metric_urn]["MetricRelationshipsClass"]
    assert isinstance(relationships, MetricRelationshipsClass)
    assert relationships.derivedFrom == []


def test_derived_metric_gets_derived_from_referenced_metric() -> None:
    client = FakeSemanticModelClient(
        metrics=[
            _metric_object("BASE1", "Base Metric"),
            _metric_object("DERIVED1", "Derived Metric"),
        ],
        metric_models={
            "BASE1": {
                "expression": {
                    "tokens": [{"objectId": "FACT1", "subType": "fact"}],
                }
            },
            "DERIVED1": {
                "expression": {
                    "tokens": [{"objectId": "BASE1", "subType": "metric"}],
                }
            },
        },
    )
    mapper = _mapper(client)
    model_tables = [_table("SALES", attributes=[], facts=[_fact("FACT1", "Revenue")])]

    aspects = _aspects_by_urn(
        list(
            mapper.emit(
                project_id="project-1",
                project_name="Project One",
                model_tables=model_tables,
                warehouse_context=None,
                project_container_key=_project_container_key(),
            )
        )
    )

    derived_urn = "urn:li:metric:(urn:li:dataPlatform:microstrategy,project-1,DERIVED1)"
    base_urn = "urn:li:metric:(urn:li:dataPlatform:microstrategy,project-1,BASE1)"
    relationships = aspects[derived_urn]["MetricRelationshipsClass"]
    assert isinstance(relationships, MetricRelationshipsClass)
    assert [d.destinationUrn for d in relationships.derivedFrom] == [base_urn]


def test_consolidated_metric_resolves_upstream_through_consolidation_attributes() -> (
    None
):
    client = FakeSemanticModelClient(
        metrics=[
            _metric_object("CONSOL_METRIC", "YoY Growth"),
            _metric_object("CONSOL_METRIC2", "YoY Growth 2"),
        ],
        metric_models={
            "CONSOL_METRIC": {
                "expression": {
                    "tokens": [{"objectId": "CONSOL1", "subType": "consolidation"}],
                }
            },
            "CONSOL_METRIC2": {
                "expression": {
                    "tokens": [{"objectId": "CONSOL1", "subType": "consolidation"}],
                }
            },
        },
        consolidation_models={
            "CONSOL1": {
                "elements": [
                    {
                        "expression": {
                            "tree": {
                                "type": "elements_object",
                                "elements": [
                                    {
                                        "attribute": {
                                            "objectId": "ATTR1",
                                            "subType": "attribute",
                                        }
                                    }
                                ],
                            }
                        }
                    }
                ]
            }
        },
    )
    mapper = _mapper(client)
    model_tables = [_table("DIM", attributes=[_attribute("ATTR1", "Region")], facts=[])]

    aspects = _aspects_by_urn(
        list(
            mapper.emit(
                project_id="project-1",
                project_name="Project One",
                model_tables=model_tables,
                warehouse_context=None,
                project_container_key=_project_container_key(),
            )
        )
    )

    metric_urn = (
        "urn:li:metric:(urn:li:dataPlatform:microstrategy,project-1,CONSOL_METRIC)"
    )
    upstreams = aspects[metric_urn]["MetricUpstreamsClass"]
    assert isinstance(upstreams, MetricUpstreamsClass)
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:microstrategy,project-1.DIM,PROD)"
    )
    assert upstreams.datasetUpstreams
    assert upstreams.datasetUpstreams[0].destinationUrn == dataset_urn
    # Two metrics share one consolidation; its definition is only fetched once.
    assert client.consolidation_calls == ["CONSOL1"]


def test_metric_model_fetch_failure_warns_and_skips_that_metric() -> None:
    client = FakeSemanticModelClient(
        metrics=[_metric_object("BROKEN", "Broken Metric")],
        metric_models={},  # no fixture -> get_metric_model raises via override below
    )

    def _raise(project_id: str, metric_id: str) -> Dict[str, Any]:
        raise MicroStrategyAPIError("boom")

    client.get_metric_model = _raise  # type: ignore[method-assign]
    mapper = _mapper(client)
    model_tables = [_table("SALES", attributes=[], facts=[_fact("FACT1", "Revenue")])]

    aspects = _aspects_by_urn(
        list(
            mapper.emit(
                project_id="project-1",
                project_name="Project One",
                model_tables=model_tables,
                warehouse_context=None,
                project_container_key=_project_container_key(),
            )
        )
    )

    metric_urn = "urn:li:metric:(urn:li:dataPlatform:microstrategy,project-1,BROKEN)"
    assert metric_urn not in aspects
    assert mapper.report.metric_expression_api_failures == 1
    assert list(mapper.report.failed_metric_model_ids) == ["BROKEN"]


def test_metric_search_failure_preserves_model_and_dataset_entities() -> None:
    # Regression: metric discovery is a separate API call from the tables
    # fetch that already succeeded by the time this runs. A failure here
    # used to propagate out of emit() before it yielded anything, dropping
    # the structural semantic-model entities (model, logical datasets) along
    # with the metrics.
    client = FakeSemanticModelClient(search_metrics_fails=True)
    mapper = _mapper(client)
    model_tables = [
        _table(
            "SALES",
            attributes=[_attribute("ATTR1", "Region")],
            facts=[_fact("FACT1", "Revenue")],
        )
    ]

    aspects = _aspects_by_urn(
        list(
            mapper.emit(
                project_id="project-1",
                project_name="Project One",
                model_tables=model_tables,
                warehouse_context=None,
                project_container_key=_project_container_key(),
            )
        )
    )

    model_urn = (
        "urn:li:semanticModel:(urn:li:dataPlatform:microstrategy,project-1,schema)"
    )
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:microstrategy,project-1.SALES,PROD)"
    )
    assert model_urn in aspects
    assert dataset_urn in aspects
    assert mapper.report.semantic_model_metric_search_api_failures == 1
