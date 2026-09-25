from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar
from unittest import mock

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.errors import SdkUsageError
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dbt.dbt_common import (
    DBTCommonConfig,
    DBTMetric,
    DBTNode,
    DBTSourceReport,
    parse_semantic_model,
)
from datahub.ingestion.source.dbt.dbt_core import (
    DBTCoreConfig,
    DBTCoreSource,
    _resolve_database_schema,
    extract_dbt_metrics,
)
from datahub.ingestion.source.dbt.dbt_semantic_model import DbtSemanticModelMapper
from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    DatasetPropertiesClass,
    DialectClass,
    DialectExpressionClass,
    ERModelRelationshipCardinalityClass,
    GlobalTagsClass,
    MetricExpressionClass,
    MetricInfoClass,
    MetricRelationshipsClass,
    MetricUpstreamsClass,
    SchemaMetadataClass,
    SemanticFieldAnnotationClass,
    SemanticFieldTypeClass,
    SemanticModelInfoClass,
    SemanticModelPropertiesClass,
    SemanticModelRelationshipClass,
    StructuredPropertiesClass,
    SubTypesClass,
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
    convert_urns_to_lowercase: bool = False,
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
        description="",
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
        convert_urns_to_lowercase=convert_urns_to_lowercase,
        semantic_model_def=parse_semantic_model(raw).definition,
    )


def _emit(
    mapper: DbtSemanticModelMapper,
    nodes: List[DBTNode],
    metrics: Optional[List[DBTMetric]] = None,
) -> List[MetadataWorkUnit]:
    return list(
        mapper.emit(
            semantic_model_nodes=nodes,
            metric_definitions=metrics or [],
        )
    )


def _aspects(
    workunits: List[MetadataWorkUnit], aspect_type: Type[_A]
) -> List[Tuple[str, _A]]:
    found: List[Tuple[str, _A]] = []
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


def _metrics(raw: Dict[str, Any]) -> List[DBTMetric]:
    return extract_dbt_metrics(manifest_metrics=raw, tag_prefix="").metrics


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


# --- the layer is additive --------------------------------------------------


def test_the_dataset_urn_is_the_one_the_dbt_path_already_emitted():
    node = _sm_node("orders", _ORDERS)
    workunits = _emit(_mapper(), [node])

    urns = [urn for urn, _ in _aspects(workunits, SemanticModelPropertiesClass)]
    assert urns == [node.get_urn("dbt", "PROD", None)]


def test_no_dataset_aspect_other_than_semantic_model_properties_is_written():
    # The ordinary dbt path owns datasetProperties, schemaMetadata, subTypes,
    # tags, owners and upstreamLineage for this urn, and runs them through
    # write_semantics. Re-emitting any of them here would silently overwrite.
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS)])

    dataset_aspects = {
        getattr(wu.metadata, "aspectName", None)
        for wu in workunits
        if getattr(wu.metadata, "entityUrn", "").startswith("urn:li:dataset:")
    }
    assert dataset_aspects == {SemanticModelPropertiesClass.ASPECT_NAME}
    for forbidden in (
        DatasetPropertiesClass,
        SchemaMetadataClass,
        SubTypesClass,
        GlobalTagsClass,
        UpstreamLineageClass,
    ):
        assert not [
            urn
            for urn, _ in _aspects(workunits, forbidden)
            if urn.startswith("urn:li:dataset:")
        ]


def test_an_unvetted_field_aspect_is_dropped_rather_than_emitted(monkeypatch):
    # The dbt path writes schemaField-anchored aspects of its own
    # (structuredProperties, from column_meta_mapping), so "anchored on a
    # field" is not on its own a licence to emit.
    real_as_mcps = SemanticModelDataset.as_mcps

    def with_extra(self: SemanticModelDataset, **kwargs: Any) -> Any:
        mcps = real_as_mcps(self, **kwargs)
        field_urn = str(mcps[-1].entityUrn)
        return mcps + [
            MetadataChangeProposalWrapper(
                entityUrn=field_urn,
                aspect=StructuredPropertiesClass(properties=[]),
            )
        ]

    monkeypatch.setattr(SemanticModelDataset, "as_mcps", with_extra)
    mapper = _mapper()
    workunits = _emit(mapper, [_sm_node("orders", _ORDERS)])

    assert not _aspects(workunits, StructuredPropertiesClass)
    assert mapper.report.warnings


def test_dataset_urn_follows_convert_urns_to_lowercase():
    node = _sm_node("Orders", _ORDERS, convert_urns_to_lowercase=True)
    workunits = _emit(_mapper(convert_urns_to_lowercase=True), [node])

    urn, _ = _aspects(workunits, SemanticModelPropertiesClass)[0]
    assert urn.endswith("semantic_model.jaffle_shop.orders,PROD)")


# --- urns and structure -----------------------------------------------------


def test_one_semantic_model_per_project():
    workunits = _emit(
        _mapper(), [_sm_node("orders", _ORDERS), _sm_node("customers", _CUSTOMERS)]
    )

    model_urns = {urn for urn, _ in _aspects(workunits, SemanticModelInfoClass)}
    assert model_urns == {
        "urn:li:semanticModel:(urn:li:dataPlatform:dbt,jaffle_shop,semantic_layer)"
    }


def test_every_dataset_points_back_at_the_project_semantic_model():
    workunits = _emit(
        _mapper(), [_sm_node("orders", _ORDERS), _sm_node("customers", _CUSTOMERS)]
    )

    props = _aspects(workunits, SemanticModelPropertiesClass)
    assert {aspect.alias for _, aspect in props} == {"orders", "customers"}
    assert {aspect.semanticModel for _, aspect in props} == {
        "urn:li:semanticModel:(urn:li:dataPlatform:dbt,jaffle_shop,semantic_layer)"
    }


def test_platform_instance_is_folded_into_the_semantic_model_path():
    # semanticModelKey has no platform_instance field, so it has to live in
    # the path or two instances of one project would collide.
    workunits = _emit(
        _mapper(platform_instance="prod_dbt"), [_sm_node("orders", _ORDERS)]
    )

    model_urns = {urn for urn, _ in _aspects(workunits, SemanticModelInfoClass)}
    assert model_urns == {
        "urn:li:semanticModel:(urn:li:dataPlatform:dbt,prod_dbt.jaffle_shop,semantic_layer)"
    }


def test_platform_instance_equal_to_project_name_is_not_doubled():
    workunits = _emit(
        _mapper(platform_instance=_PROJECT), [_sm_node("orders", _ORDERS)]
    )

    model_urns = {urn for urn, _ in _aspects(workunits, SemanticModelInfoClass)}
    assert model_urns == {
        "urn:li:semanticModel:(urn:li:dataPlatform:dbt,jaffle_shop,semantic_layer)"
    }


def test_semantic_models_sharing_a_name_get_distinct_aliases():
    workunits = _emit(
        _mapper(),
        [
            _sm_node("orders", _ORDERS),
            _sm_node("orders", _CUSTOMERS, package_name="other_pkg"),
        ],
    )

    aliases = {
        aspect.alias for _, aspect in _aspects(workunits, SemanticModelPropertiesClass)
    }
    assert aliases == {"orders", "other_pkg_orders"}


# --- field annotations ------------------------------------------------------


def test_entities_dimensions_and_measures_become_annotated_fields():
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS)])

    annotations = _annotations(workunits)
    assert set(annotations) == {
        "order_id",
        "customer_id",
        "ordered_at",
        "status",
        "order_total",
        "order_count",
    }
    assert annotations["order_id"].type == SemanticFieldTypeClass.DIMENSION
    assert annotations["order_total"].type == SemanticFieldTypeClass.MEASURE
    assert annotations["order_total"].aggregationFunction == "sum"


def test_time_dimension_sets_is_time():
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS)])

    annotations = _annotations(workunits)
    assert _is_time(annotations["ordered_at"])
    assert not _is_time(annotations["status"])


def test_primary_entity_declared_on_the_model_is_a_join_target():
    # dbt lets a model name its key with `primary_entity` instead of listing
    # an entity of type `primary`; MetricFlow joins on it either way.
    workunits = _emit(
        _mapper(),
        [
            _sm_node("orders", {"entities": [{"name": "order_id", "type": "foreign"}]}),
            _sm_node(
                "lines",
                {
                    "entities": [{"name": "order_id", "type": "foreign"}],
                    "primary_entity": "order_id",
                },
            ),
        ],
    )

    rels = _relationships(_one(workunits, SemanticModelInfoClass))
    assert [(rel.from_, rel.to) for rel in rels] == [("orders", "lines")]


def test_field_expr_is_passed_through_and_absent_expr_is_alias_qualified():
    node = _sm_node(
        "orders",
        {
            "dimensions": [
                {"name": "day", "type": "categorical", "expr": "date_trunc(d)"},
                {"name": "plain", "type": "categorical"},
            ]
        },
    )
    annotations = _annotations(_emit(_mapper(), [node]))
    assert _expression_of(annotations["day"].expression).expression == "date_trunc(d)"
    assert _expression_of(annotations["plain"].expression).expression == "orders.plain"


def test_duplicate_field_name_across_kinds_is_dropped_with_a_warning():
    # dbt only enforces uniqueness within each of entities/dimensions/measures.
    mapper = _mapper()
    node = _sm_node(
        "orders",
        {
            "entities": [{"name": "amount", "type": "primary"}],
            "measures": [{"name": "amount", "agg": "sum"}],
        },
    )
    annotations = _annotations(_emit(mapper, [node]))

    assert annotations["amount"].type == SemanticFieldTypeClass.DIMENSION
    assert mapper.report.warnings


def test_unnamed_field_is_skipped_but_the_rest_survive():
    mapper = _mapper()
    node = _sm_node(
        "orders",
        {
            "entities": [{"name": "order_id", "type": "primary"}, {"type": "foreign"}],
        },
    )
    annotations = _annotations(_emit(mapper, [node]))

    assert set(annotations) == {"order_id"}
    assert mapper.report.warnings


def test_dialect_is_inferred_from_the_target_platform():
    node = _sm_node(
        "orders",
        {"dimensions": [{"name": "day", "type": "categorical", "expr": "trunc(d)"}]},
    )
    annotation = _annotations(_emit(_mapper(target_platform="snowflake"), [node]))[
        "day"
    ]
    assert _expression_of(annotation.expression).dialect == DialectClass.SNOWFLAKE


def test_unknown_target_platform_falls_back_to_ansi_sql():
    node = _sm_node(
        "orders",
        {"dimensions": [{"name": "day", "type": "categorical", "expr": "trunc(d)"}]},
    )
    annotation = _annotations(_emit(_mapper(target_platform="postgres"), [node]))["day"]
    assert _expression_of(annotation.expression).dialect == DialectClass.ANSI_SQL


# --- relationships ----------------------------------------------------------


def test_foreign_to_key_entity_derives_an_n_one_relationship():
    workunits = _emit(
        _mapper(), [_sm_node("orders", _ORDERS), _sm_node("customers", _CUSTOMERS)]
    )

    rels = _relationships(_one(workunits, SemanticModelInfoClass))
    assert len(rels) == 1
    assert (rels[0].from_, rels[0].to) == ("orders", "customers")
    assert rels[0].fromColumns == ["customer_id"]
    assert rels[0].toColumns == ["customer_id"]
    assert rels[0].cardinality == ERModelRelationshipCardinalityClass.N_ONE


def test_unique_target_is_also_n_one_not_one_one():
    # dbt documents joining one unique key to many foreign keys, so a unique
    # target is still the "one" side of a many-to-one.
    workunits = _emit(
        _mapper(),
        [
            _sm_node(
                "orders", {"entities": [{"name": "customer_id", "type": "foreign"}]}
            ),
            _sm_node(
                "customers", {"entities": [{"name": "customer_id", "type": "unique"}]}
            ),
        ],
    )

    rels = _relationships(_one(workunits, SemanticModelInfoClass))
    assert rels[0].cardinality == ERModelRelationshipCardinalityClass.N_ONE


def test_join_column_is_the_entity_name_not_its_expr():
    # `name` is the field path in the schema; the SDK raises on a join column
    # it cannot find there.
    workunits = _emit(
        _mapper(),
        [
            _sm_node(
                "orders",
                {
                    "entities": [
                        {"name": "customer_id", "type": "foreign", "expr": "cust_fk"}
                    ]
                },
            ),
            _sm_node(
                "customers",
                {
                    "entities": [
                        {"name": "customer_id", "type": "primary", "expr": "id"}
                    ]
                },
            ),
        ],
    )

    rels = _relationships(_one(workunits, SemanticModelInfoClass))
    assert rels[0].fromColumns == ["customer_id"]
    assert rels[0].toColumns == ["customer_id"]


def test_self_referencing_entity_produces_no_relationship():
    mapper = _mapper()
    workunits = _emit(
        mapper,
        [_sm_node("orders", {"entities": [{"name": "order_id", "type": "unique"}]})],
    )

    assert _one(workunits, SemanticModelInfoClass).relationships is None


def test_a_key_shared_across_models_joins_to_each_of_them():
    # Valid dbt: MetricFlow joins the referencing model to both, so emitting
    # neither edge would lose joins rather than avoid a guess.
    workunits = _emit(
        _mapper(),
        [
            _sm_node(
                "orders", {"entities": [{"name": "customer_id", "type": "foreign"}]}
            ),
            _sm_node(
                "customers", {"entities": [{"name": "customer_id", "type": "primary"}]}
            ),
            _sm_node(
                "accounts", {"entities": [{"name": "customer_id", "type": "primary"}]}
            ),
        ],
    )

    rels = _relationships(_one(workunits, SemanticModelInfoClass))
    assert {rel.to for rel in rels} == {"customers", "accounts"}


def test_unresolved_join_target_is_recorded_without_a_warning():
    # Legitimate when the referenced model is outside the ingested scope.
    mapper = _mapper()
    _emit(
        mapper,
        [
            _sm_node(
                "orders", {"entities": [{"name": "customer_id", "type": "foreign"}]}
            )
        ],
    )

    assert list(mapper.report.semantic_model_relationships_unresolved) == [
        "semantic_model.jaffle_shop.orders.customer_id"
    ]
    assert not mapper.report.warnings


def test_case_differing_entity_names_still_resolve_a_relationship():
    workunits = _emit(
        _mapper(),
        [
            _sm_node(
                "orders", {"entities": [{"name": "Customer_ID", "type": "foreign"}]}
            ),
            _sm_node(
                "customers", {"entities": [{"name": "customer_id", "type": "primary"}]}
            ),
        ],
    )

    rels = _relationships(_one(workunits, SemanticModelInfoClass))
    assert rels[0].fromColumns == ["Customer_ID"]
    assert rels[0].toColumns == ["customer_id"]


def test_primary_entity_without_a_matching_entity_is_not_a_join_target():
    # dbt allows primary_entity precisely for models with no such column, so
    # synthesizing one would fabricate a column that does not exist.
    mapper = _mapper()
    workunits = _emit(
        mapper,
        [
            _sm_node(
                "orders", {"entities": [{"name": "customer_id", "type": "foreign"}]}
            ),
            _sm_node(
                "customers",
                {
                    "dimensions": [{"name": "country", "type": "categorical"}],
                    "primary_entity": "customer_id",
                },
            ),
        ],
    )

    assert _one(workunits, SemanticModelInfoClass).relationships is None
    assert any(
        "cannot be joined to" in entry
        for entry in mapper.report.semantic_model_relationships_unresolved
    )


# --- metrics from measures --------------------------------------------------


def test_create_metric_measure_emits_a_metric_with_expression_and_upstream():
    node = _sm_node("orders", _ORDERS)
    workunits = _emit(_mapper(), [node])

    metrics = _aspects(workunits, MetricInfoClass)
    assert [urn for urn, _ in metrics] == [
        "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,order_total)"
    ]
    assert (
        _expression_of(metrics[0][1].expression).expression == "sum(orders.order_total)"
    )
    upstreams = _one(workunits, MetricUpstreamsClass)
    assert _destinations(upstreams.datasetUpstreams) == [
        node.get_urn("dbt", "PROD", None)
    ]


def test_measure_without_create_metric_emits_no_metric():
    workunits = _emit(
        _mapper(), [_sm_node("orders", {"measures": [{"name": "total", "agg": "sum"}]})]
    )
    assert not _aspects(workunits, MetricInfoClass)


def test_measure_without_agg_emits_a_metric_without_an_expression():
    # A fabricated aggregation would be worse than an absent one.
    workunits = _emit(
        _mapper(),
        [_sm_node("orders", {"measures": [{"name": "total", "create_metric": True}]})],
    )
    assert _one(workunits, MetricInfoClass).expression is None


def test_metric_relationships_and_upstreams_are_always_emitted():
    # metricRelationships makes hasParentMetric index as false; metricUpstreams
    # clears stale upstreams on re-emit.
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS)])
    assert _aspects(workunits, MetricRelationshipsClass)
    assert _aspects(workunits, MetricUpstreamsClass)


def test_a_create_metric_measure_with_no_name_does_not_abort_the_run():
    # MetricUrn rejects an empty id with InvalidUrnError, which is not an
    # SdkUsageError, so it would escape every guard and end the whole run.
    mapper = _mapper()
    workunits = _emit(
        mapper,
        [
            _sm_node(
                "orders",
                {
                    "measures": [
                        {"agg": "sum", "create_metric": True},
                        {"name": "total", "agg": "sum", "create_metric": True},
                    ]
                },
            )
        ],
    )

    assert [urn for urn, _ in _aspects(workunits, MetricInfoClass)] == [
        "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,total)"
    ]
    assert mapper.report.warnings


def test_a_measure_dropped_as_a_duplicate_gets_no_metric():
    # Its metric's expression would qualify a field path the dataset's
    # annotated fields do not contain.
    mapper = _mapper()
    workunits = _emit(
        mapper,
        [
            _sm_node(
                "orders",
                {
                    "entities": [{"name": "amount", "type": "primary"}],
                    "measures": [
                        {"name": "amount", "agg": "sum", "create_metric": True}
                    ],
                },
            )
        ],
    )

    assert not _aspects(workunits, MetricInfoClass)
    assert mapper.report.warnings


def test_two_create_metric_measures_sharing_a_name_emit_one_metric():
    mapper = _mapper()
    measure = {"name": "total", "agg": "sum", "create_metric": True}
    workunits = _emit(
        mapper,
        [
            _sm_node("orders", {"measures": [measure]}),
            _sm_node("payments", {"measures": [measure]}),
        ],
    )

    assert len(_aspects(workunits, MetricInfoClass)) == 1
    assert mapper.report.warnings


# --- metrics from the manifest ----------------------------------------------


def test_manifest_metric_resolves_upstream_via_type_params_measure():
    node = _sm_node("orders", _ORDERS)
    metrics = _metrics(
        {
            "metric.jaffle_shop.revenue": {
                "name": "revenue",
                "label": "Revenue",
                "type": "simple",
                "type_params": {"measure": {"name": "order_count"}},
            }
        }
    )
    workunits = _emit(_mapper(), [node], metrics)

    info = {urn: aspect for urn, aspect in _aspects(workunits, MetricInfoClass)}
    revenue_urn = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,revenue)"
    assert info[revenue_urn].name == "Revenue"
    assert (
        _expression_of(info[revenue_urn].expression).expression
        == "count(orders.order_count)"
    )
    upstreams = {
        urn: aspect for urn, aspect in _aspects(workunits, MetricUpstreamsClass)
    }
    assert _destinations(upstreams[revenue_urn].datasetUpstreams) == [
        node.get_urn("dbt", "PROD", None)
    ]


def test_manifest_metric_resolves_upstream_via_depends_on():
    node = _sm_node("orders", _ORDERS)
    metrics = _metrics(
        {
            "metric.jaffle_shop.anything": {
                "name": "anything",
                "type": "simple",
                "depends_on": {"nodes": ["semantic_model.jaffle_shop.orders"]},
            }
        }
    )
    workunits = _emit(_mapper(), [node], metrics)

    upstreams = {
        urn: aspect for urn, aspect in _aspects(workunits, MetricUpstreamsClass)
    }
    anything = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,anything)"
    assert _destinations(upstreams[anything].datasetUpstreams) == [
        node.get_urn("dbt", "PROD", None)
    ]


def test_derived_metric_derives_from_referenced_metrics():
    metrics = _metrics(
        {
            "metric.jaffle_shop.base": {
                "name": "base",
                "type": "simple",
                "type_params": {"measure": {"name": "order_count"}},
            },
            "metric.jaffle_shop.discounted": {
                "name": "discounted",
                "type": "derived",
                "type_params": {"expr": "base * 0.9", "metrics": [{"name": "base"}]},
            },
        }
    )
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS)], metrics)

    rels = {
        urn: aspect for urn, aspect in _aspects(workunits, MetricRelationshipsClass)
    }
    discounted = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,discounted)"
    assert _destinations(rels[discounted].derivedFrom) == [
        "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,base)"
    ]


def test_ratio_metric_renders_both_sides_of_the_division():
    metrics = _metrics(
        {
            "metric.jaffle_shop.aov": {
                "name": "aov",
                "type": "ratio",
                "type_params": {
                    "numerator": {"name": "order_total"},
                    "denominator": {"name": "order_count"},
                },
            }
        }
    )
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS)], metrics)

    info = {urn: aspect for urn, aspect in _aspects(workunits, MetricInfoClass)}
    aov = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,aov)"
    assert (
        _expression_of(info[aov].expression).expression
        == "sum(orders.order_total) / count(orders.order_count)"
    )


def test_a_ratio_keeps_lineage_to_the_dataset_its_expression_reads():
    # Its sides resolve to metrics for derivedFrom, but the expression is
    # rendered from the underlying measures, so the Metric -> Semantic Model
    # Dataset edge has to survive too.
    node = _sm_node("orders", _ORDERS)
    metrics = _metrics(
        {
            "metric.jaffle_shop.order_total": {
                "name": "order_total",
                "type": "simple",
                "type_params": {"measure": {"name": "order_total"}},
            },
            "metric.jaffle_shop.aov": {
                "name": "aov",
                "type": "ratio",
                "type_params": {
                    "numerator": {"name": "order_total"},
                    "denominator": {"name": "order_count"},
                },
            },
        }
    )
    workunits = _emit(_mapper(), [node], metrics)

    upstreams = {
        urn: aspect for urn, aspect in _aspects(workunits, MetricUpstreamsClass)
    }
    aov = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,aov)"
    assert _destinations(upstreams[aov].datasetUpstreams) == [
        node.get_urn("dbt", "PROD", None)
    ]
    rels = {
        urn: aspect for urn, aspect in _aspects(workunits, MetricRelationshipsClass)
    }
    assert _destinations(rels[aov].derivedFrom) == [
        "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,order_total)"
    ]


def test_unresolvable_metric_reference_is_dropped_with_a_warning():
    # derivedFrom is indexed as lineage, so a wrong edge is worse than none.
    mapper = _mapper()
    metrics = _metrics(
        {
            "metric.jaffle_shop.derived": {
                "name": "derived",
                "type": "derived",
                "type_params": {"expr": "x", "metrics": [{"name": "nowhere"}]},
            }
        }
    )
    workunits = _emit(mapper, [_sm_node("orders", _ORDERS)], metrics)

    rels = {
        urn: aspect for urn, aspect in _aspects(workunits, MetricRelationshipsClass)
    }
    derived = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,derived)"
    assert _destinations(rels[derived].derivedFrom) == []
    assert mapper.report.warnings


def test_dbts_own_copy_of_a_create_metric_measure_is_not_warned_about():
    # dbt materializes a create_metric measure into `metrics:` itself, so the
    # name overlap is dbt's doing, not an author's mistake.
    mapper = _mapper()
    metrics = _metrics(
        {
            "metric.jaffle_shop.order_total": {
                "name": "order_total",
                "type": "simple",
                "type_params": {"measure": {"name": "order_total"}},
            }
        }
    )
    workunits = _emit(mapper, [_sm_node("orders", _ORDERS)], metrics)

    assert len(_aspects(workunits, MetricInfoClass)) == 1
    assert not mapper.report.warnings


def test_a_genuine_name_collision_with_a_measure_is_warned_about():
    mapper = _mapper()
    metrics = _metrics(
        {
            "metric.jaffle_shop.order_total": {
                "name": "order_total",
                "type": "derived",
                "type_params": {"expr": "something_else * 2"},
            }
        }
    )
    _emit(mapper, [_sm_node("orders", _ORDERS)], metrics)

    assert mapper.report.warnings


def test_metric_without_a_name_is_skipped():
    mapper = _mapper()
    metrics = _metrics({"metric.jaffle_shop.x": {"type": "simple"}})
    workunits = _emit(mapper, [_sm_node("orders", _ORDERS)], metrics)

    assert len(_aspects(workunits, MetricInfoClass)) == 1  # only order_total
    assert mapper.report.warnings


def test_metric_counters_reconcile_with_the_emitted_total():
    mapper = _mapper()
    metrics = _metrics(
        {
            "metric.jaffle_shop.revenue": {
                "name": "revenue",
                "type": "simple",
                "type_params": {"measure": {"name": "order_count"}},
            }
        }
    )
    _emit(mapper, [_sm_node("orders", _ORDERS)], metrics)

    report = mapper.report
    assert (
        report.num_metrics_from_measures + report.num_metrics_from_manifest
        == report.num_metrics_emitted + report.num_metrics_dropped
    )


# --- metric attributes metricInfo has no field for --------------------------


def test_metric_type_is_emitted_as_a_subtype():
    metrics = _metrics(
        {
            "metric.jaffle_shop.aov": {
                "name": "aov",
                "type": "ratio",
                "type_params": {
                    "numerator": {"name": "order_total"},
                    "denominator": {"name": "order_count"},
                },
            }
        }
    )
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS)], metrics)

    subtypes = {
        urn: aspect.typeNames
        for urn, aspect in _aspects(workunits, SubTypesClass)
        if urn.startswith("urn:li:metric:")
    }
    assert subtypes["urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,aov)"] == [
        "Ratio"
    ]


def test_metric_filter_is_folded_into_the_expression():
    # metricInfo.expression has no filter field, and a dropped filter computes
    # a different number than the dbt definition.
    metrics = _metrics(
        {
            "metric.jaffle_shop.big": {
                "name": "big",
                "type": "simple",
                "type_params": {"measure": {"name": "order_total"}},
                "filter": {"where_filters": [{"where_sql_template": "amount > 100"}]},
            }
        }
    )
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS)], metrics)

    info = {urn: aspect for urn, aspect in _aspects(workunits, MetricInfoClass)}
    big = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,big)"
    assert (
        _expression_of(info[big].expression).expression
        == "sum(orders.order_total) FILTER (WHERE amount > 100)"
    )


def test_two_predicates_are_parenthesised_so_or_keeps_its_scope():
    metrics = _metrics(
        {
            "metric.jaffle_shop.big": {
                "name": "big",
                "type": "simple",
                "type_params": {
                    "measure": {
                        "name": "order_total",
                        "filter": {"where_filters": [{"where_sql_template": "a OR b"}]},
                    }
                },
                "filter": {"where_filters": [{"where_sql_template": "c OR d"}]},
            }
        }
    )
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS)], metrics)

    info = {urn: aspect for urn, aspect in _aspects(workunits, MetricInfoClass)}
    big = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,big)"
    assert _expression_of(info[big].expression).expression == (
        "sum(orders.order_total) FILTER (WHERE (a OR b) AND (c OR d))"
    )


def test_a_cumulative_metric_says_how_it_accumulates():
    # Without the note a 7-day running total reads exactly like a plain total.
    metrics = _metrics(
        {
            "metric.jaffle_shop.running": {
                "name": "running",
                "type": "cumulative",
                "type_params": {
                    "measure": {"name": "order_total"},
                    "window": {"count": 7, "granularity": "day"},
                },
            }
        }
    )
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS)], metrics)

    info = {urn: aspect for urn, aspect in _aspects(workunits, MetricInfoClass)}
    running = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,running)"
    assert _expression_of(info[running].expression).expression == (
        "sum(orders.order_total) /* cumulative over 7 day */"
    )


def test_a_simple_metric_gets_no_accumulation_note():
    metrics = _metrics(
        {
            "metric.jaffle_shop.plain": {
                "name": "plain",
                "type": "simple",
                "type_params": {"measure": {"name": "order_total"}},
            }
        }
    )
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS)], metrics)

    info = {urn: aspect for urn, aspect in _aspects(workunits, MetricInfoClass)}
    plain = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,plain)"
    assert (
        _expression_of(info[plain].expression).expression == "sum(orders.order_total)"
    )


def test_a_filter_on_a_ratio_is_stated_not_hung_off_the_denominator():
    # SQL's FILTER attaches to an aggregate call; `(a / b) FILTER (...)` would
    # constrain both sides, which is not what dbt means.
    metrics = _metrics(
        {
            "metric.jaffle_shop.aov": {
                "name": "aov",
                "type": "ratio",
                "type_params": {
                    "numerator": {"name": "order_total"},
                    "denominator": {"name": "order_count"},
                },
                "filter": {"where_filters": [{"where_sql_template": "region = 'EU'"}]},
            }
        }
    )
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS)], metrics)

    info = {urn: aspect for urn, aspect in _aspects(workunits, MetricInfoClass)}
    aov = "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,aov)"
    assert _expression_of(info[aov].expression).expression == (
        "sum(orders.order_total) / count(orders.order_count) "
        "/* filtered: region = 'EU' */"
    )


# --- browse paths -----------------------------------------------------------


def test_browse_paths_are_emitted_for_the_semantic_model_and_its_metrics():
    # Neither entity type has a container aspect, so without this GMS drops
    # them into a literal "Default" folder.
    workunits = _emit(_mapper(), [_sm_node("orders", _ORDERS)])

    paths = {urn for urn, _ in _aspects(workunits, BrowsePathsV2Class)}
    assert paths == {
        "urn:li:semanticModel:(urn:li:dataPlatform:dbt,jaffle_shop,semantic_layer)",
        "urn:li:metric:(urn:li:dataPlatform:dbt,jaffle_shop,order_total)",
    }


def test_browse_path_leads_with_the_platform_instance_when_set():
    workunits = _emit(
        _mapper(platform_instance="prod_dbt"), [_sm_node("orders", _ORDERS)]
    )
    _, path = _aspects(workunits, BrowsePathsV2Class)[0]
    assert [entry.id for entry in path.path] == ["prod_dbt", "jaffle_shop"]


def test_browse_path_folds_an_instance_equal_to_the_project_name():
    workunits = _emit(
        _mapper(platform_instance=_PROJECT), [_sm_node("orders", _ORDERS)]
    )
    _, path = _aspects(workunits, BrowsePathsV2Class)[0]
    assert [entry.id for entry in path.path] == ["jaffle_shop"]


# --- degradation ------------------------------------------------------------


def test_semantic_model_with_an_empty_definition_is_skipped():
    mapper = _mapper()
    workunits = _emit(mapper, [_sm_node("orders", {})])

    assert workunits == []
    assert list(mapper.report.semantic_models_skipped) == [
        "semantic_model.jaffle_shop.orders"
    ]


def test_nothing_is_emitted_without_semantic_models():
    mapper = _mapper()
    assert _emit(mapper, []) == []
    assert not mapper.report.warnings


def test_metrics_are_not_emitted_without_a_semantic_model_to_attach_to():
    mapper = _mapper()
    metrics = _metrics({"metric.jaffle_shop.revenue": {"name": "revenue"}})
    workunits = _emit(mapper, [], metrics)

    assert workunits == []
    assert mapper.report.warnings


def test_a_project_level_sdk_failure_is_a_failure_not_a_warning(monkeypatch):
    # Losing the semanticModel loses the relationships and every membership
    # back-reference, so the whole layer is unusable.
    mapper = _mapper()

    def boom(self: SemanticModel, change_type: Any = None) -> Any:
        raise SdkUsageError("bad model")

    monkeypatch.setattr(SemanticModel, "as_mcps", boom)
    workunits = _emit(mapper, [_sm_node("orders", _ORDERS)])

    assert workunits == []
    assert mapper.report.failures
    assert mapper.report.num_semantic_model_datasets_dropped == 1
    assert mapper.report.num_metrics_dropped == 1


def test_a_project_level_failure_leaves_the_datasets_alone():
    # The connector emits the datasets before this mapper runs, so a failure
    # here must not be able to retract them - it simply yields nothing.
    mapper = _mapper()
    workunits = _emit(mapper, [_sm_node("orders", {})])
    assert not [
        wu
        for wu in workunits
        if getattr(wu.metadata, "entityUrn", "").startswith("urn:li:dataset:")
    ]


def test_one_unbuildable_dataset_does_not_cost_the_project(monkeypatch):
    # The SDK validates at construction as well as at emit time.
    real_init = SemanticModelDataset.__init__

    def selective_init(self: SemanticModelDataset, **kwargs: Any) -> None:
        if kwargs["alias"] == "bad":
            raise SdkUsageError("unrepresentable")
        real_init(self, **kwargs)

    monkeypatch.setattr(SemanticModelDataset, "__init__", selective_init)
    mapper = _mapper()
    workunits = _emit(mapper, [_sm_node("bad", _ORDERS), _sm_node("good", _CUSTOMERS)])

    aliases = {
        aspect.alias for _, aspect in _aspects(workunits, SemanticModelPropertiesClass)
    }
    assert aliases == {"good"}
    assert mapper.report.num_semantic_model_datasets_dropped == 1
    assert mapper.report.warnings


def test_an_unexpected_error_is_not_laundered_into_a_warning(monkeypatch):
    # Only SdkUsageError is a user modelling problem; anything else is a bug
    # in this mapper and must crash rather than be silently reported.
    mapper = _mapper()

    def boom(self: SemanticModel, change_type: Any = None) -> Any:
        raise RuntimeError("bug")

    monkeypatch.setattr(SemanticModel, "as_mcps", boom)
    with pytest.raises(RuntimeError):
        _emit(mapper, [_sm_node("orders", _ORDERS)])


def test_a_non_string_agg_does_not_abort_emission():
    workunits = _emit(
        _mapper(),
        [
            _sm_node(
                "orders",
                {"measures": [{"name": "total", "agg": 7, "create_metric": True}]},
            )
        ],
    )
    assert _one(workunits, MetricInfoClass).expression is None


# --- parsing tolerance ------------------------------------------------------


def test_a_malformed_section_costs_only_that_section():
    parsed = parse_semantic_model(
        {
            "entities": "not a list",
            "dimensions": [{"name": "country", "type": "categorical"}],
            "measures": [{"name": "total", "agg": "sum"}],
        }
    )

    assert not parsed.definition.entities
    assert [d.name for d in parsed.definition.dimensions] == ["country"]
    assert [m.name for m in parsed.definition.measures] == ["total"]
    assert parsed.discarded


def test_a_malformed_entry_costs_only_that_entry():
    parsed = parse_semantic_model(
        {"measures": ["not an object", {"name": "total", "agg": "sum"}]}
    )

    assert [m.name for m in parsed.definition.measures] == ["total"]
    assert parsed.discarded == ["measures[0] is str, expected an object"]


def test_an_explicit_null_agg_is_absent_not_malformed():
    parsed = parse_semantic_model({"measures": [{"name": "total", "agg": None}]})

    assert parsed.definition.measures[0].aggregation is None
    assert not parsed.discarded


def test_a_non_object_type_params_costs_only_the_granularity():
    parsed = parse_semantic_model(
        {"dimensions": [{"name": "d", "type": "time", "type_params": "oops"}]}
    )

    dimension = parsed.definition.dimensions[0]
    assert dimension.is_time
    assert dimension.time_granularity is None
    assert parsed.discarded


def test_camel_case_keys_from_the_dbt_cloud_api_are_read():
    parsed = parse_semantic_model(
        {
            "primaryEntity": "order_id",
            "dimensions": [
                {"name": "d", "type": "time", "typeParams": {"timeGranularity": "day"}}
            ],
            "measures": [{"name": "m", "agg": "sum", "createMetric": True}],
        }
    )

    assert parsed.definition.primary_entity == "order_id"
    assert parsed.definition.dimensions[0].time_granularity == "day"
    assert parsed.definition.measures[0].create_metric


def test_a_malformed_metric_costs_only_that_metric():
    parsed = extract_dbt_metrics(
        manifest_metrics={
            "metric.p.bad": {"name": "bad", "type_params": "not an object"},
            "metric.p.good": {"name": "good", "type": "simple"},
        },
        tag_prefix="",
    )

    assert [m.name for m in parsed.metrics] == ["good"]
    # Handed back rather than reported at parse time; the source reports it
    # only on a run that would have emitted metrics.
    assert [key for key, _ in parsed.unreadable] == ["metric.p.bad"]


def test_a_bare_string_measure_reference_from_dbt_1_6_is_read():
    metrics = _metrics(
        {"metric.p.m": {"name": "m", "type_params": {"input_measures": ["total"]}}}
    )
    assert [i.name for i in metrics[0].measures] == ["total"]


def test_dbt_1_9_nested_conversion_measures_are_read():
    metrics = _metrics(
        {
            "metric.p.conv": {
                "name": "conv",
                "type": "conversion",
                "type_params": {
                    "conversion_type_params": {
                        "base_measure": {"name": "visits"},
                        "conversion_measure": {"name": "signups"},
                    }
                },
            }
        }
    )
    assert {i.name for i in metrics[0].measures} == {"visits", "signups"}


def test_node_relation_schema_name_is_read():
    # dbt writes `schema_name`; NodeRelation sets additionalProperties: false,
    # so `schema` never appears in a real manifest.
    assert _resolve_database_schema(
        {"database": "db", "schema_name": "sc"}, {}, {}
    ) == ("db", "sc")


# --- the tri-state gate, as the dbt source drives it ------------------------


def _source(**overrides: Any) -> DBTCoreSource:
    config: Dict[str, Any] = {
        "manifest_path": "temp/",
        "catalog_path": "temp/",
        "sources_path": "temp/",
        "target_platform": "postgres",
        "write_semantics": "OVERRIDE",
    }
    graph = overrides.pop("graph", None)
    config.update(overrides)
    ctx = PipelineContext(run_id="test-run-id")
    ctx.graph = graph
    return DBTCoreSource(DBTCoreConfig(**config), ctx)


def _cloud_graph(*, version: str = "2.5.0", is_cloud: bool = True) -> Any:
    graph = mock.MagicMock()
    graph.server_config.is_datahub_cloud = is_cloud
    graph.server_config.service_version = version
    graph.server_config.supports_feature.return_value = True
    graph.execute_graphql.return_value = {"appConfig": {"featureFlags": {}}}
    return graph


def test_unset_without_a_graph_stays_off_and_does_not_warn():
    source = _source()
    assert not source._emit_semantic_model_entities()
    assert not source.report.warnings
    assert source.report.semantic_model_emission_effective is False
    assert source.report.semantic_model_emission_reason


def test_unset_on_a_capable_cloud_server_turns_on():
    source = _source(graph=_cloud_graph())
    assert source._emit_semantic_model_entities()
    assert not source.report.warnings


def test_explicit_true_without_a_graph_turns_on():
    source = _source(emit_semantic_model_entities=True)
    assert source._emit_semantic_model_entities()


def test_explicit_false_forces_off_against_a_capable_server():
    source = _source(emit_semantic_model_entities=False, graph=_cloud_graph())
    assert not source._emit_semantic_model_entities()
    assert not source.report.warnings


def test_an_explicit_request_the_server_refuses_is_warned_about():
    graph = _cloud_graph()
    graph.server_config.supports_feature.return_value = False
    source = _source(emit_semantic_model_entities=True, graph=graph)

    assert not source._emit_semantic_model_entities()
    assert source.report.warnings


def test_a_failed_metrics_probe_warns_even_on_the_auto_enable_path():
    # Fails closed, so the recipe-request warning cannot fire; without this
    # warning the run would silently stay off.
    graph = _cloud_graph()
    graph.execute_graphql.side_effect = RuntimeError("network")
    source = _source(graph=graph)

    assert not source._emit_semantic_model_entities()
    assert source.report.warnings


# --- project name -----------------------------------------------------------


def test_config_pins_the_project_name_over_the_manifest():
    source = _source(semantic_model_project_name="pinned")
    source._project_name = "from_manifest"
    assert source._resolve_semantic_model_project_name([]) == "pinned"


def test_the_package_name_is_the_fallback_when_there_is_no_manifest_metadata():
    # dbt Cloud has no manifest metadata, but the Discovery API returns
    # packageName for each semantic model.
    source = _source()
    assert (
        source._resolve_semantic_model_project_name(
            [_sm_node("orders", _ORDERS, package_name="cloud_project")]
        )
        == "cloud_project"
    )


def test_several_packages_pick_the_most_common_one_with_a_warning():
    source = _source()
    nodes = [
        _sm_node("a", _ORDERS, package_name="root"),
        _sm_node("b", _ORDERS, package_name="root"),
        _sm_node("c", _ORDERS, package_name="installed_pkg"),
    ]
    assert source._resolve_semantic_model_project_name(nodes) == "root"
    assert source.report.warnings


def test_an_undeterminable_project_name_is_a_failure_not_a_guess():
    # The project name is urn identity; entities minted under a placeholder
    # would need a hard delete to correct.
    source = _source()
    node = _sm_node("orders", _ORDERS)
    node.dbt_package_name = None

    assert source._resolve_semantic_model_project_name([node]) is None
    assert source.report.failures


@pytest.mark.parametrize("value", ["", "   ", "a,b", "a(b", "a)b"])
def test_an_unusable_project_name_override_is_rejected_at_config_time(
    value: str,
) -> None:
    with pytest.raises(ValueError):
        DBTCommonConfig.model_validate(
            {"target_platform": "postgres", "semantic_model_project_name": value}
        )
