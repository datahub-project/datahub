from typing import Any, Dict, List, Optional, Type, TypeVar

from datahub.errors import SdkUsageError
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.dbt.dbt_common import (
    DBTCommonConfig,
    DBTMetric,
    DBTNode,
    DBTSourceReport,
    parse_semantic_model_definition,
)
from datahub.ingestion.source.dbt.dbt_core import extract_dbt_metrics
from datahub.ingestion.source.dbt.dbt_semantic_model import DbtSemanticModelMapper
from datahub.metadata.schema_classes import (
    DateTypeClass,
    DialectClass,
    DialectExpressionClass,
    ERModelRelationshipCardinalityClass,
    GlobalTagsClass,
    MetricExpressionClass,
    MetricInfoClass,
    MetricRelationshipsClass,
    MetricUpstreamsClass,
    NumberTypeClass,
    SchemaMetadataClass,
    SemanticFieldAnnotationClass,
    SemanticFieldTypeClass,
    SemanticModelInfoClass,
    SemanticModelPropertiesClass,
    SemanticModelRelationshipClass,
    StringTypeClass,
    SubTypesClass,
    TimeTypeClass,
    UpstreamLineageClass,
)
from datahub.sdk.semantic_model import SemanticModel, SemanticModelDataset

_A = TypeVar("_A")

_PROJECT = "jaffle_shop"


def _mapper(
    *, project_name: str = _PROJECT, **overrides: Any
) -> DbtSemanticModelMapper:
    base: Dict[str, Any] = {"target_platform": "bigquery"}
    base.update(overrides)
    return DbtSemanticModelMapper(
        config=DBTCommonConfig.model_validate(base),
        report=DBTSourceReport(),
        project_name=project_name,
    )


def _sm_node(
    name: str,
    raw: Dict[str, Any],
    *,
    upstreams: Optional[List[str]] = None,
    package_name: str = _PROJECT,
    description: str = "",
) -> DBTNode:
    return DBTNode(
        dbt_name=f"semantic_model.{package_name}.{name}",
        dbt_adapter="bigquery",
        dbt_package_name=package_name,
        database="db",
        schema="sc",
        name=name,
        alias=name,
        dbt_file_path="models/semantic_models.yml",
        node_type="semantic_model",
        max_loaded_at=None,
        comment="",
        description=description,
        upstream_nodes=upstreams or [],
        materialization=None,
        catalog_type=None,
        missing_from_catalog=False,
        meta={},
        query_tag={},
        tags=[],
        owner=None,
        language="yaml",
        columns=[],
        compiled_code=None,
        raw_code=None,
        semantic_model_def=parse_semantic_model_definition(raw),
    )


def _model_node(name: str) -> DBTNode:
    return DBTNode(
        dbt_name=f"model.{_PROJECT}.{name}",
        dbt_adapter="bigquery",
        dbt_package_name=_PROJECT,
        database="db",
        schema="sc",
        name=name,
        alias=name,
        dbt_file_path="",
        node_type="model",
        max_loaded_at=None,
        comment="",
        description="",
        language="sql",
        raw_code="select 1",
        materialization="table",
        catalog_type="table",
        missing_from_catalog=False,
        owner=None,
    )


def _emit(
    mapper: DbtSemanticModelMapper,
    nodes: List[DBTNode],
    metrics: Optional[List[DBTMetric]] = None,
    extra_nodes: Optional[List[DBTNode]] = None,
) -> List[MetadataWorkUnit]:
    all_nodes_map = {node.dbt_name: node for node in nodes}
    for node in extra_nodes or []:
        all_nodes_map[node.dbt_name] = node
    return list(
        mapper.emit(
            semantic_model_nodes=nodes,
            metric_definitions=metrics or [],
            all_nodes_map=all_nodes_map,
        )
    )


def _aspects(
    workunits: List[MetadataWorkUnit], aspect_type: Type[_A]
) -> List[tuple[str, _A]]:
    found = []
    for wu in workunits:
        aspect = getattr(wu.metadata, "aspect", None)
        urn = getattr(wu.metadata, "entityUrn", None)
        if isinstance(aspect, aspect_type) and urn is not None:
            found.append((urn, aspect))
    return found


def _one(workunits: List[MetadataWorkUnit], aspect_type: Type[_A]) -> _A:
    matches = _aspects(workunits, aspect_type)
    assert len(matches) == 1, f"expected exactly one {aspect_type.__name__}"
    return matches[0][1]


def _expression_of(
    expression: Optional[MetricExpressionClass],
) -> DialectExpressionClass:
    assert expression is not None
    assert len(expression.dialects) == 1
    return expression.dialects[0]


def _destinations(edges: Optional[List[Any]]) -> List[str]:
    assert edges is not None
    return [edge.destinationUrn for edge in edges]


def _relationships(
    info: SemanticModelInfoClass,
) -> List[SemanticModelRelationshipClass]:
    assert info.relationships is not None
    return info.relationships


def _is_time(annotation: SemanticFieldAnnotationClass) -> bool:
    assert annotation.dimension is not None
    return annotation.dimension.isTime


def _annotations(
    workunits: List[MetadataWorkUnit],
) -> Dict[str, SemanticFieldAnnotationClass]:
    return {
        urn.rsplit(",", 1)[1].rstrip(")"): aspect
        for urn, aspect in _aspects(workunits, SemanticFieldAnnotationClass)
    }


_ORDERS = {
    "entities": [
        {"name": "order_id", "type": "primary", "description": "The order key"},
        {"name": "customer_id", "type": "foreign"},
    ],
    "dimensions": [
        {
            "name": "ordered_at",
            "type": "time",
            "type_params": {"time_granularity": "day"},
        },
        {"name": "status", "type": "categorical", "description": "Order status"},
    ],
    "measures": [
        {"name": "order_total", "agg": "sum", "create_metric": True},
        {"name": "order_count", "agg": "count"},
    ],
}
_CUSTOMERS = {
    "entities": [{"name": "customer_id", "type": "primary"}],
    "dimensions": [{"name": "country", "type": "categorical"}],
    "measures": [{"name": "customer_ltv", "agg": "sum", "expr": "ltv * 0.9"}],
}


# --- URNs and structure -----------------------------------------------------


def test_one_semantic_model_per_project_and_one_dataset_per_semantic_model():
    mapper = _mapper()
    workunits = _emit(
        mapper, [_sm_node("orders", _ORDERS), _sm_node("customers", _CUSTOMERS)]
    )

    model_urns = {urn for urn, _ in _aspects(workunits, SemanticModelInfoClass)}
    assert model_urns == {
        "urn:li:semanticModel:(urn:li:dataPlatform:dbt,jaffle_shop,semantic_layer)"
    }

    properties = _aspects(workunits, SemanticModelPropertiesClass)
    assert {urn for urn, _ in properties} == {
        "urn:li:dataset:(urn:li:dataPlatform:dbt,jaffle_shop.semantic_layer.customers,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:dbt,jaffle_shop.semantic_layer.orders,PROD)",
    }
    assert {aspect.alias for _, aspect in properties} == {"orders", "customers"}
    assert all(
        aspect.semanticModel == next(iter(model_urns)) for _, aspect in properties
    )

    assert all(
        DatasetSubTypes.SEMANTIC_MODEL_DATASET in aspect.typeNames
        for _, aspect in _aspects(workunits, SubTypesClass)
    )
    assert mapper.report.num_semantic_model_entities_emitted == 1
    assert mapper.report.num_semantic_model_datasets_emitted == 2


def test_semantic_model_info_name_is_the_project():
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS)])
    assert _one(workunits, SemanticModelInfoClass).name == _PROJECT


def test_platform_instance_is_folded_into_path_but_not_the_dataset_name():
    """semanticModelKey has no platform_instance field; a dataset URN does."""
    workunits = _emit(
        _mapper(platform_instance="analytics"), [_sm_node("orders", _ORDERS)]
    )

    assert _aspects(workunits, SemanticModelInfoClass)[0][0] == (
        "urn:li:semanticModel:"
        "(urn:li:dataPlatform:dbt,analytics.jaffle_shop,semantic_layer)"
    )
    # The dataset builder prefixes the instance itself, so the name must not.
    assert _aspects(workunits, SemanticModelPropertiesClass)[0][0] == (
        "urn:li:dataset:(urn:li:dataPlatform:dbt,analytics.jaffle_shop.semantic_layer.orders,PROD)"
    )


def test_platform_instance_equal_to_project_name_is_not_doubled():
    """DataHub's documented multi-project dbt setup sets both to the project."""
    workunits = _emit(
        _mapper(platform_instance=_PROJECT), [_sm_node("orders", _ORDERS)]
    )
    assert _aspects(workunits, SemanticModelInfoClass)[0][0] == (
        "urn:li:semanticModel:(urn:li:dataPlatform:dbt,jaffle_shop,semantic_layer)"
    )


def test_dataset_name_casing_follows_convert_urns_to_lowercase():
    workunits = _emit(
        _mapper(project_name="Jaffle_Shop", convert_urns_to_lowercase=False),
        [_sm_node("Orders", _ORDERS)],
    )
    assert _aspects(workunits, SemanticModelPropertiesClass)[0][0].endswith(
        "Jaffle_Shop.semantic_layer.Orders,PROD)"
    )


# --- Field mapping ----------------------------------------------------------


def test_entities_dimensions_and_measures_become_annotated_schema_fields():
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS)])
    annotations = _annotations(workunits)

    assert annotations["order_id"].type == SemanticFieldTypeClass.DIMENSION
    assert annotations["status"].type == SemanticFieldTypeClass.DIMENSION
    assert annotations["order_total"].type == SemanticFieldTypeClass.MEASURE
    assert annotations["order_total"].aggregationFunction == "sum"
    assert annotations["order_count"].aggregationFunction == "count"
    # A measure has no Dimension sub-record to carry isTime.
    assert annotations["order_total"].dimension is None

    schema = _aspects(workunits, SchemaMetadataClass)[0][1]
    field_types = {f.fieldPath: type(f.type.type).__name__ for f in schema.fields}
    assert field_types["order_id"] == StringTypeClass.__name__
    assert field_types["status"] == StringTypeClass.__name__
    assert field_types["order_total"] == NumberTypeClass.__name__

    key_fields = {f.fieldPath for f in schema.fields if f.isPartOfKey}
    assert key_fields == {"order_id"}


def test_time_dimension_sets_is_time_and_resolves_to_a_date_type():
    """A "timestamp" native type would resolve to BytesType, not a datetime."""
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS)])

    assert _is_time(_annotations(workunits)["ordered_at"])

    schema = _aspects(workunits, SchemaMetadataClass)[0][1]
    ordered_at = next(f for f in schema.fields if f.fieldPath == "ordered_at")
    assert isinstance(ordered_at.type.type, DateTypeClass)


def test_sub_day_time_granularity_resolves_to_a_time_type():
    node = _sm_node(
        "events",
        {
            "entities": [{"name": "event_id", "type": "primary"}],
            "dimensions": [
                {
                    "name": "happened_at",
                    "type": "time",
                    "type_params": {"time_granularity": "second"},
                }
            ],
        },
    )
    workunits = _emit(_mapper(), [node])

    schema = _aspects(workunits, SchemaMetadataClass)[0][1]
    happened_at = next(f for f in schema.fields if f.fieldPath == "happened_at")
    assert isinstance(happened_at.type.type, TimeTypeClass)
    assert _is_time(_annotations(workunits)["happened_at"])


def test_primary_entity_declared_on_the_model_marks_the_key_field():
    node = _sm_node(
        "orders",
        {
            "primary_entity": "order_id",
            "entities": [{"name": "order_id"}],
            "measures": [{"name": "total", "agg": "sum"}],
        },
    )
    workunits = _emit(_mapper(), [node])

    schema = _aspects(workunits, SchemaMetadataClass)[0][1]
    assert {f.fieldPath for f in schema.fields if f.isPartOfKey} == {"order_id"}


def test_field_expr_is_passed_through_and_absent_expr_is_alias_qualified():
    """Matches Snowflake's mapper and the SDK's own synthesized default."""
    workunits = _emit(_mapper(), [_sm_node("customers", _CUSTOMERS)])
    annotations = _annotations(workunits)

    assert (
        _expression_of(annotations["customer_ltv"].expression).expression == "ltv * 0.9"
    )
    assert (
        _expression_of(annotations["country"].expression).expression
        == "customers.country"
    )


def test_duplicate_field_name_across_kinds_is_dropped_with_a_warning():
    """The SDK raises on a duplicate field_path, so dedupe before it does."""
    node = _sm_node(
        "orders",
        {
            "entities": [{"name": "status", "type": "primary"}],
            "dimensions": [{"name": "status", "type": "categorical"}],
            "measures": [{"name": "total", "agg": "sum"}],
        },
    )
    mapper = _mapper()
    workunits = _emit(mapper, [node])

    schema = _aspects(workunits, SchemaMetadataClass)[0][1]
    assert [f.fieldPath for f in schema.fields] == ["status", "total"]
    # Entities win over dimensions, so the surviving field is the key.
    assert next(f for f in schema.fields if f.fieldPath == "status").isPartOfKey
    assert any(
        w.title == "Duplicate dbt semantic model field name"
        for w in mapper.report.warnings
    )


def test_unnamed_field_is_skipped_with_a_warning():
    node = _sm_node(
        "orders",
        {
            "entities": [{"name": "order_id", "type": "primary"}, {"name": "  "}],
            "measures": [{"name": "total", "agg": "sum"}],
        },
    )
    mapper = _mapper()
    workunits = _emit(mapper, [node])

    schema = _aspects(workunits, SchemaMetadataClass)[0][1]
    assert [f.fieldPath for f in schema.fields] == ["order_id", "total"]
    assert any(
        w.title == "dbt semantic model field has no name"
        for w in mapper.report.warnings
    )


# --- Lineage ----------------------------------------------------------------


def test_logical_dataset_gets_upstream_lineage_to_the_physical_table():
    workunits = _emit(
        _mapper(),
        [_sm_node("orders", _ORDERS, upstreams=[f"model.{_PROJECT}.stg_orders"])],
        extra_nodes=[_model_node("stg_orders")],
    )

    lineage = _one(workunits, UpstreamLineageClass)
    assert [u.dataset for u in lineage.upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,db.sc.stg_orders,PROD)"
    ]


def test_no_upstream_lineage_aspect_when_nothing_resolves():
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS, upstreams=["model.x.y"])])
    assert _aspects(workunits, UpstreamLineageClass) == []


# --- Relationships ----------------------------------------------------------


def test_foreign_to_key_entity_derives_an_n_one_relationship():
    workunits = _emit(
        _mapper(), [_sm_node("orders", _ORDERS), _sm_node("customers", _CUSTOMERS)]
    )

    relationships = _relationships(_one(workunits, SemanticModelInfoClass))
    assert len(relationships) == 1
    relationship = relationships[0]
    assert relationship.from_ == "orders"
    assert relationship.fromColumns == ["customer_id"]
    assert relationship.to == "customers"
    assert relationship.toColumns == ["customer_id"]
    assert relationship.cardinality == ERModelRelationshipCardinalityClass.N_ONE


def test_unique_target_is_also_n_one_not_one_one():
    """dbt joins a single unique key to multiple foreign keys."""
    dates = {
        "entities": [{"name": "date_key", "type": "unique"}],
        "dimensions": [{"name": "day_of_week", "type": "categorical"}],
    }
    orders = {
        "entities": [
            {"name": "order_id", "type": "primary"},
            {"name": "date_key", "type": "foreign"},
        ],
        "measures": [{"name": "total", "agg": "sum"}],
    }
    workunits = _emit(_mapper(), [_sm_node("orders", orders), _sm_node("dates", dates)])

    relationships = _relationships(_one(workunits, SemanticModelInfoClass))
    assert len(relationships) == 1
    assert relationships[0].cardinality == ERModelRelationshipCardinalityClass.N_ONE
    assert relationships[0].to == "dates"


def test_join_column_is_the_entity_name_not_its_expr():
    """An `expr` is not a schema field, so the SDK could not resolve it."""
    orders = {
        "entities": [
            {"name": "order_id", "type": "primary"},
            {"name": "customer_id", "type": "foreign", "expr": "lower(cust_id)"},
        ],
        "measures": [{"name": "total", "agg": "sum"}],
    }
    workunits = _emit(
        _mapper(), [_sm_node("orders", orders), _sm_node("customers", _CUSTOMERS)]
    )

    relationships = _relationships(_one(workunits, SemanticModelInfoClass))
    assert relationships[0].fromColumns == ["customer_id"]


def test_self_referencing_entity_produces_no_relationship():
    node = _sm_node(
        "orders",
        {
            "entities": [
                {"name": "order_id", "type": "primary"},
                {"name": "order_id", "type": "foreign"},
            ],
            "measures": [{"name": "total", "agg": "sum"}],
        },
    )
    workunits = _emit(_mapper(), [node])
    assert _one(workunits, SemanticModelInfoClass).relationships is None


def test_ambiguous_join_target_is_skipped_with_a_warning():
    owner = {"entities": [{"name": "customer_id", "type": "primary"}]}
    mapper = _mapper()
    workunits = _emit(
        mapper,
        [
            _sm_node("orders", _ORDERS),
            _sm_node("customers", {**owner, "measures": [{"name": "a", "agg": "sum"}]}),
            _sm_node("people", {**owner, "measures": [{"name": "b", "agg": "sum"}]}),
        ],
    )

    assert _one(workunits, SemanticModelInfoClass).relationships is None
    assert any(
        w.title == "Ambiguous dbt semantic model join" for w in mapper.report.warnings
    )


def test_unresolved_join_target_is_recorded_without_a_warning():
    """A foreign entity may point outside the ingested scope."""
    mapper = _mapper()
    workunits = _emit(mapper, [_sm_node("orders", _ORDERS)])

    assert _one(workunits, SemanticModelInfoClass).relationships is None
    assert list(mapper.report.semantic_model_relationships_unresolved) == [
        f"semantic_model.{_PROJECT}.orders.customer_id"
    ]
    assert mapper.report.warnings == []


def test_semantic_models_sharing_a_name_get_distinct_aliases_and_urns():
    mapper = _mapper()
    workunits = _emit(
        mapper,
        [
            _sm_node("orders", _ORDERS, package_name=_PROJECT),
            _sm_node("orders", _CUSTOMERS, package_name="other_pkg"),
        ],
    )

    properties = dict(_aspects(workunits, SemanticModelPropertiesClass))
    assert len(properties) == 2
    assert {a.alias for a in properties.values()} == {"orders", "other_pkg_orders"}
    assert any(
        w.title == "Duplicate dbt semantic model name" for w in mapper.report.warnings
    )


# --- Metrics ----------------------------------------------------------------


def test_create_metric_measure_emits_a_metric_with_expression_and_upstream():
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS)])

    metrics = dict(_aspects(workunits, MetricInfoClass))
    assert list(metrics) == [
        "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,order_total)"
    ]
    info = next(iter(metrics.values()))
    assert info.semanticModel == (
        "urn:li:semanticModel:(urn:li:dataPlatform:dbt,jaffle_shop,semantic_layer)"
    )
    assert _expression_of(info.expression).expression == "sum(orders.order_total)"
    assert _expression_of(info.expression).dialect == DialectClass.ANSI_SQL

    upstreams = dict(_aspects(workunits, MetricUpstreamsClass))
    assert _destinations(next(iter(upstreams.values())).datasetUpstreams) == [
        "urn:li:dataset:(urn:li:dataPlatform:dbt,jaffle_shop.semantic_layer.orders,PROD)"
    ]


def test_measure_without_create_metric_emits_no_metric():
    workunits = _emit(_mapper(), [_sm_node("customers", _CUSTOMERS)])
    assert _aspects(workunits, MetricInfoClass) == []


def test_measure_without_agg_emits_a_metric_without_an_expression():
    node = _sm_node(
        "orders",
        {
            "entities": [{"name": "order_id", "type": "primary"}],
            "measures": [{"name": "custom", "agg": "", "create_metric": True}],
        },
    )
    workunits = _emit(_mapper(), [node])
    assert _aspects(workunits, MetricInfoClass)[0][1].expression is None


def test_metric_relationships_and_upstreams_are_always_emitted():
    """Root metrics are listed via hasParentMetric=false, so it must index."""
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS)])

    assert _one(workunits, MetricRelationshipsClass).derivedFrom == []
    assert len(_aspects(workunits, MetricUpstreamsClass)) == 1


def test_dialect_is_inferred_from_the_target_platform():
    for target_platform, expected in (
        ("snowflake", DialectClass.SNOWFLAKE),
        ("databricks", DialectClass.DATABRICKS),
        ("bigquery", DialectClass.ANSI_SQL),
    ):
        workunits = _emit(
            _mapper(target_platform=target_platform), [_sm_node("orders", _ORDERS)]
        )
        info = _aspects(workunits, MetricInfoClass)[0][1]
        assert _expression_of(info.expression).dialect == expected


def _metrics(raw: Dict[str, Any]) -> List[DBTMetric]:
    return extract_dbt_metrics(raw, "dbt:")


def test_manifest_metric_resolves_upstream_via_type_params_measure():
    workunits = _emit(
        _mapper(),
        [_sm_node("orders", _ORDERS)],
        _metrics(
            {
                "metric.jaffle_shop.revenue": {
                    "name": "revenue",
                    "label": "Revenue",
                    "description": "Order revenue",
                    "type": "simple",
                    "type_params": {"measure": {"name": "order_count"}},
                }
            }
        ),
    )

    metrics = dict(_aspects(workunits, MetricInfoClass))
    revenue_urn = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,revenue)"
    assert metrics[revenue_urn].name == "Revenue"
    # A simple metric borrows its measure's aggregation as its expression.
    assert (
        _expression_of(metrics[revenue_urn].expression).expression
        == "count(orders.order_count)"
    )

    upstreams = dict(_aspects(workunits, MetricUpstreamsClass))
    assert _destinations(upstreams[revenue_urn].datasetUpstreams) == [
        "urn:li:dataset:(urn:li:dataPlatform:dbt,jaffle_shop.semantic_layer.orders,PROD)"
    ]


def test_manifest_metric_resolves_upstream_via_depends_on():
    workunits = _emit(
        _mapper(),
        [_sm_node("orders", _ORDERS)],
        _metrics(
            {
                "metric.jaffle_shop.opaque": {
                    "name": "opaque",
                    "label": "Opaque",
                    "description": "",
                    "type": "simple",
                    "type_params": {},
                    "depends_on": {
                        "nodes": [f"semantic_model.{_PROJECT}.orders", "model.x.y"]
                    },
                }
            }
        ),
    )

    upstreams = dict(_aspects(workunits, MetricUpstreamsClass))
    opaque_urn = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,opaque)"
    assert _destinations(upstreams[opaque_urn].datasetUpstreams) == [
        "urn:li:dataset:(urn:li:dataPlatform:dbt,jaffle_shop.semantic_layer.orders,PROD)"
    ]


def test_derived_metric_derives_from_referenced_metrics():
    workunits = _emit(
        _mapper(),
        [_sm_node("orders", _ORDERS)],
        _metrics(
            {
                "metric.jaffle_shop.revenue": {
                    "name": "revenue",
                    "label": "Revenue",
                    "description": "",
                    "type": "simple",
                    "type_params": {"measure": "order_total"},
                },
                "metric.jaffle_shop.margin": {
                    "name": "margin",
                    "label": "Margin",
                    "description": "",
                    "type": "derived",
                    "type_params": {
                        "expr": "revenue * 0.4",
                        "metrics": [{"name": "revenue"}],
                    },
                },
            }
        ),
    )

    relationships = dict(_aspects(workunits, MetricRelationshipsClass))
    margin_urn = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,margin)"
    assert _destinations(relationships[margin_urn].derivedFrom) == [
        "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,revenue)"
    ]


def test_ratio_metric_synthesizes_an_expression_from_its_inputs():
    workunits = _emit(
        _mapper(),
        [_sm_node("orders", _ORDERS)],
        _metrics(
            {
                "metric.jaffle_shop.revenue": {
                    "name": "revenue",
                    "label": "R",
                    "description": "",
                    "type": "simple",
                    "type_params": {"measure": "order_total"},
                },
                "metric.jaffle_shop.orders_count": {
                    "name": "orders_count",
                    "label": "O",
                    "description": "",
                    "type": "simple",
                    "type_params": {"measure": "order_count"},
                },
                "metric.jaffle_shop.aov": {
                    "name": "aov",
                    "label": "AOV",
                    "description": "",
                    "type": "ratio",
                    "type_params": {
                        "numerator": "revenue",
                        "denominator": {"name": "orders_count"},
                    },
                },
            }
        ),
    )

    metrics = dict(_aspects(workunits, MetricInfoClass))
    aov_urn = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,aov)"
    assert (
        _expression_of(metrics[aov_urn].expression).expression
        == "revenue / orders_count"
    )
    relationships = dict(_aspects(workunits, MetricRelationshipsClass))
    assert set(_destinations(relationships[aov_urn].derivedFrom)) == {
        "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,revenue)",
        "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,orders_count)",
    }


def test_measure_valued_ratio_input_becomes_an_upstream_not_a_derived_edge():
    """dbt 1.6 ratios named measures where modern dbt names metrics."""
    mapper = _mapper()
    workunits = _emit(
        mapper,
        [_sm_node("orders", _ORDERS), _sm_node("customers", _CUSTOMERS)],
        _metrics(
            {
                "metric.jaffle_shop.arpu": {
                    "name": "arpu",
                    "label": "ARPU",
                    "description": "",
                    "type": "ratio",
                    "type_params": {
                        "numerator": "order_count",
                        "denominator": "customer_ltv",
                    },
                }
            }
        ),
    )

    arpu_urn = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,arpu)"
    relationships = dict(_aspects(workunits, MetricRelationshipsClass))
    assert relationships[arpu_urn].derivedFrom == []
    upstreams = dict(_aspects(workunits, MetricUpstreamsClass))
    assert set(_destinations(upstreams[arpu_urn].datasetUpstreams)) == {
        "urn:li:dataset:(urn:li:dataPlatform:dbt,jaffle_shop.semantic_layer.orders,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:dbt,jaffle_shop.semantic_layer.customers,PROD)",
    }
    assert mapper.report.warnings == []


def test_unresolvable_metric_reference_is_dropped_with_a_warning():
    """derivedFrom is indexed as lineage, so a wrong edge is worse than none."""
    mapper = _mapper()
    workunits = _emit(
        mapper,
        [_sm_node("orders", _ORDERS)],
        _metrics(
            {
                "metric.jaffle_shop.margin": {
                    "name": "margin",
                    "label": "Margin",
                    "description": "",
                    "type": "derived",
                    "type_params": {
                        "expr": "gone * 2",
                        "metrics": [{"name": "gone"}],
                    },
                }
            }
        ),
    )

    relationships = dict(_aspects(workunits, MetricRelationshipsClass))
    margin_urn = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,margin)"
    assert relationships[margin_urn].derivedFrom == []
    assert any(
        w.title == "dbt metric references an unknown metric"
        for w in mapper.report.warnings
    )


def test_manifest_metric_shadows_a_same_named_create_metric_measure():
    mapper = _mapper()
    workunits = _emit(
        mapper,
        [_sm_node("orders", _ORDERS)],
        _metrics(
            {
                "metric.jaffle_shop.order_total": {
                    "name": "order_total",
                    "label": "Order Total",
                    "description": "The richer definition",
                    "type": "simple",
                    "type_params": {"measure": "order_total"},
                }
            }
        ),
    )

    metrics = dict(_aspects(workunits, MetricInfoClass))
    assert list(metrics) == [
        "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,order_total)"
    ]
    info = next(iter(metrics.values()))
    assert info.name == "Order Total"
    assert info.description == "The richer definition"
    assert any(
        w.title == "dbt metric shadows a create_metric measure"
        for w in mapper.report.warnings
    )


# --- Degenerate inputs ------------------------------------------------------


def test_semantic_model_with_an_empty_definition_is_skipped():
    mapper = _mapper()
    node = _sm_node("empty", {})
    workunits = _emit(mapper, [node])

    assert workunits == []
    assert list(mapper.report.semantic_models_skipped) == [node.dbt_name]


def test_nothing_is_emitted_without_semantic_models():
    mapper = _mapper()
    assert _emit(mapper, [], _metrics({})) == []
    assert mapper.report.num_semantic_model_entities_emitted == 0


def test_metrics_are_not_emitted_without_a_semantic_model_to_attach_to():
    """A metric requires a semanticModel URN, so it cannot stand alone."""
    mapper = _mapper()
    workunits = _emit(
        mapper,
        [],
        _metrics(
            {
                "metric.jaffle_shop.revenue": {
                    "name": "revenue",
                    "label": "R",
                    "description": "",
                    "type": "simple",
                    "type_params": {},
                }
            }
        ),
    )

    assert workunits == []
    assert any(
        w.title == "No dbt semantic models could be emitted"
        for w in mapper.report.warnings
    )


# --- URN collision and case handling ---------------------------------------


def test_case_differing_semantic_model_names_get_distinct_dataset_urns():
    """convert_urns_to_lowercase defaults true, so "Orders"/"orders" collide."""
    mapper = _mapper()
    workunits = _emit(
        mapper, [_sm_node("Orders", _ORDERS), _sm_node("orders", _CUSTOMERS)]
    )

    urns = {urn for urn, _ in _aspects(workunits, SemanticModelPropertiesClass)}
    assert len(urns) == 2
    assert any(
        w.title == "Duplicate dbt semantic model name" for w in mapper.report.warnings
    )


def test_logical_dataset_name_cannot_collide_with_a_two_part_dbt_node():
    """DBTNode.get_db_fqn drops a falsy database, so nodes can be 2-part too."""
    workunits = _emit(_mapper(project_name="pagila"), [_sm_node("orders", _ORDERS)])

    urn = _aspects(workunits, SemanticModelPropertiesClass)[0][0]
    # Three parts, so <schema>.<name> can never resolve to the same URN.
    assert "pagila.semantic_layer.orders" in urn


def test_case_differing_entity_names_still_resolve_a_relationship():
    """Owners are indexed case-insensitively, so membership must be too."""
    orders = {
        "entities": [
            {"name": "order_id", "type": "primary"},
            {"name": "Customer_Id", "type": "foreign"},
        ],
        "measures": [{"name": "total", "agg": "sum"}],
    }
    mapper = _mapper()
    workunits = _emit(
        mapper, [_sm_node("orders", orders), _sm_node("customers", _CUSTOMERS)]
    )

    relationships = _relationships(_one(workunits, SemanticModelInfoClass))
    assert len(relationships) == 1
    # Each side uses the field path present in its own schema.
    assert relationships[0].fromColumns == ["Customer_Id"]
    assert relationships[0].toColumns == ["customer_id"]
    assert mapper.report.warnings == []


def test_primary_entity_without_a_matching_entity_is_not_a_join_target():
    """dbt allows primary_entity precisely when no such column exists."""
    regions = {
        "primary_entity": "region_id",
        "entities": [{"name": "territory_id", "type": "unique"}],
        "dimensions": [{"name": "region_name", "type": "categorical"}],
    }
    orders = {
        "entities": [
            {"name": "order_id", "type": "primary"},
            {"name": "region_id", "type": "foreign"},
        ],
        "measures": [{"name": "total", "agg": "sum"}],
    }
    mapper = _mapper()
    workunits = _emit(
        mapper, [_sm_node("orders", orders), _sm_node("regions", regions)]
    )

    # No fabricated column, and no relationship to a target with no column.
    assert _one(workunits, SemanticModelInfoClass).relationships is None
    schemas = {
        urn: {f.fieldPath for f in aspect.fields}
        for urn, aspect in _aspects(workunits, SchemaMetadataClass)
    }
    regions_fields = next(v for k, v in schemas.items() if "regions" in k)
    assert "region_id" not in regions_fields
    assert any(
        "declared as a key but has no matching entity" in entry
        for entry in mapper.report.semantic_model_relationships_unresolved
    )


def test_duplicate_measure_name_across_models_is_reported():
    """Measure names are unique per model, not per project."""
    a = {
        "entities": [{"name": "a_id", "type": "primary"}],
        "measures": [{"name": "revenue", "agg": "sum"}],
    }
    b = {
        "entities": [{"name": "b_id", "type": "primary"}],
        "measures": [{"name": "revenue", "agg": "max"}],
    }
    mapper = _mapper()
    _emit(mapper, [_sm_node("a", a), _sm_node("b", b)])

    assert any(w.title == "Ambiguous dbt measure name" for w in mapper.report.warnings)


# --- derivedFrom canonical case ---------------------------------------------


def test_derived_from_uses_the_defining_case_not_the_referencing_case():
    """A dangling derivedFrom edge is worse than a missing one."""
    mapper = _mapper()
    workunits = _emit(
        mapper,
        [_sm_node("orders", _ORDERS)],
        _metrics(
            {
                "metric.jaffle_shop.revenue": {
                    "name": "Revenue",
                    "label": "Revenue",
                    "description": "",
                    "type": "simple",
                    "type_params": {"measure": "order_total"},
                },
                "metric.jaffle_shop.margin": {
                    "name": "margin",
                    "label": "Margin",
                    "description": "",
                    "type": "derived",
                    # Referenced in a different case than it was defined.
                    "type_params": {
                        "expr": "revenue * 0.4",
                        "metrics": [{"name": "revenue"}],
                    },
                },
            }
        ),
    )

    emitted = {urn for urn, _ in _aspects(workunits, MetricInfoClass)}
    relationships = dict(_aspects(workunits, MetricRelationshipsClass))
    margin_urn = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,margin)"
    derived = _destinations(relationships[margin_urn].derivedFrom)
    assert derived == ["urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,Revenue)"]
    # The edge must point at a metric that was actually emitted.
    assert set(derived) <= emitted
    assert mapper.report.warnings == []


# --- metric filter ----------------------------------------------------------


def test_metric_filter_is_folded_into_the_expression():
    """Dropping the filter would publish a broader metric as authoritative."""
    workunits = _emit(
        _mapper(),
        [_sm_node("orders", _ORDERS)],
        _metrics(
            {
                "metric.jaffle_shop.us_revenue": {
                    "name": "us_revenue",
                    "label": "US Revenue",
                    "description": "",
                    "type": "simple",
                    "type_params": {"measure": "order_total"},
                    "filter": {
                        "where_filters": [{"where_sql_template": "region = 'US'"}]
                    },
                }
            }
        ),
    )

    metrics = dict(_aspects(workunits, MetricInfoClass))
    urn = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,us_revenue)"
    assert (
        _expression_of(metrics[urn].expression).expression
        == "sum(orders.order_total) FILTER (WHERE region = 'US')"
    )


def test_metric_tags_reach_the_emitted_metric():
    workunits = _emit(
        _mapper(),
        [_sm_node("orders", _ORDERS)],
        _metrics(
            {
                "metric.jaffle_shop.revenue": {
                    "name": "revenue",
                    "label": "Revenue",
                    "description": "",
                    "type": "simple",
                    "type_params": {"measure": "order_total"},
                    "tags": ["certified"],
                }
            }
        ),
    )

    tags = dict(_aspects(workunits, GlobalTagsClass))
    urn = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,revenue)"
    assert [t.tag for t in tags[urn].tags] == ["urn:li:tag:dbt:certified"]


# --- report reconciliation --------------------------------------------------


def test_metric_counters_reconcile_with_the_emitted_total():
    mapper = _mapper()
    _emit(
        mapper,
        [_sm_node("orders", _ORDERS)],
        _metrics(
            {
                # Shadows the create_metric measure, so it must not be
                # double-counted.
                "metric.jaffle_shop.order_total": {
                    "name": "order_total",
                    "label": "Order Total",
                    "description": "",
                    "type": "simple",
                    "type_params": {"measure": "order_total"},
                },
                "metric.jaffle_shop.other": {
                    "name": "other",
                    "label": "Other",
                    "description": "",
                    "type": "simple",
                    "type_params": {},
                },
            }
        ),
    )

    report = mapper.report
    assert (
        report.num_metrics_from_measures + report.num_metrics_from_manifest
        == report.num_metrics_emitted
    )


# --- emission failure isolation ---------------------------------------------


def test_one_unrepresentable_dataset_does_not_lose_the_project(monkeypatch):
    mapper = _mapper()
    nodes = [_sm_node("orders", _ORDERS), _sm_node("customers", _CUSTOMERS)]
    original = SemanticModelDataset.as_workunits

    def as_workunits(self):
        if self.urn.name.endswith("orders"):
            raise SdkUsageError("cannot represent this dataset")
        return original(self)

    monkeypatch.setattr(SemanticModelDataset, "as_workunits", as_workunits)
    workunits = _emit(mapper, nodes)

    # The other dataset and the semanticModel still land.
    assert _aspects(workunits, SemanticModelInfoClass)
    assert len(_aspects(workunits, SemanticModelPropertiesClass)) == 1
    assert any(
        w.title == "Failed to emit a dbt semantic model entity"
        for w in mapper.report.warnings
    )


def test_a_bad_semantic_model_is_a_failure_not_a_warning(monkeypatch):
    """Losing the semanticModel loses the whole project, so it must fail."""
    mapper = _mapper()
    nodes = [_sm_node("orders", _ORDERS)]

    def raise_sdk_error(self):
        raise SdkUsageError("bad alias")

    monkeypatch.setattr(SemanticModel, "as_workunits", raise_sdk_error)
    workunits = _emit(mapper, nodes)

    assert workunits == []
    assert any(
        f.title == "Failed to emit dbt semantic model entities"
        for f in mapper.report.failures
    )
