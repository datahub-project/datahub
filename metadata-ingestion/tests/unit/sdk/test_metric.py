"""Unit tests for the Metric SDK.

These tests assert the producer contract for the ``metric`` entity: URN
pattern, ``metricInfo`` shape (with optional expression and semantic-model
back-ref), the always-emitted ``metricRelationships`` (so ``hasParentMetric``
indexes as false), the always-emitted ``metricUpstreams`` (empty clears stale
upstreams), aiContext-only-when-non-empty, and
``metricUpstreams.datasetUpstreams`` for Metric → SMD lineage.
"""

from datetime import datetime, timezone
from typing import Any

import pytest

from datahub.metadata.schema_classes import (
    AiContextClass,
    DerivedMetricInputClass,
    DialectClass,
    EdgeClass,
    MetricExpressionClass,
    MetricInfoClass,
    MetricRelationshipsClass,
    MetricUpstreamsClass,
    StatusClass,
)
from datahub.metadata.urns import DataPlatformUrn, MetricUrn
from datahub.sdk.entity import Entity
from datahub.sdk.metric import Metric
from datahub.sdk.semantic_model import (
    AiContextInput,
    DialectExpressionInput,
)


def _aspects_by_name(entity: Entity) -> dict[str, Any]:
    return {mcp.aspectName: mcp.aspect for mcp in entity.as_mcps() if mcp.aspectName}


def test_metric_urn_and_core_aspects() -> None:
    model_urn = (
        "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)"
    )
    metric = Metric(
        platform="snowflake",
        path="analytics",
        id="total_revenue",
        semantic_model=model_urn,
        name="Total Revenue",
        description="Sum of order amounts.",
    )
    assert metric.urn == MetricUrn(
        "urn:li:dataPlatform:snowflake", "analytics", "total_revenue"
    )
    assert str(metric.urn) == (
        "urn:li:metric:(urn:li:dataPlatform:snowflake,analytics,total_revenue)"
    )
    aspects = _aspects_by_name(metric)
    assert "status" in aspects
    assert isinstance(aspects["status"], StatusClass)
    assert aspects["status"].removed is False
    info = aspects["metricInfo"]
    assert isinstance(info, MetricInfoClass)
    assert info.name == "Total Revenue"
    assert info.description == "Sum of order amounts."
    assert info.semanticModel == model_urn
    # No expression provided -> omitted (never fabricated).
    assert info.expression is None
    # metricRelationships always emitted, even with empty derivedFrom.
    assert "metricRelationships" in aspects
    rels = aspects["metricRelationships"]
    assert isinstance(rels, MetricRelationshipsClass)
    assert rels.derivedFrom == []
    assert rels.parentMetric is None
    # metricUpstreams always emitted (empty clears stale upstreams on re-emit).
    assert "metricUpstreams" in aspects
    upstreams = aspects["metricUpstreams"]
    assert isinstance(upstreams, MetricUpstreamsClass)
    assert upstreams.datasetUpstreams == []
    # No aiContext when none provided.
    assert "aiContext" not in aspects


def test_metric_upstream_datasets() -> None:
    smd_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders_model.orders_ds,PROD)"
    customers_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders_model.customers_ds,PROD)"
    metric = Metric(
        platform="snowflake",
        path="analytics",
        id="total_revenue",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        upstream_datasets=[smd_urn],
    )
    assert metric.upstream_datasets == [smd_urn]
    aspects = _aspects_by_name(metric)
    upstreams = aspects["metricUpstreams"]
    assert isinstance(upstreams, MetricUpstreamsClass)
    assert upstreams.datasetUpstreams == [EdgeClass(destinationUrn=smd_urn)]

    metric.set_upstream_datasets([smd_urn, customers_urn])
    assert metric.upstream_datasets == [smd_urn, customers_urn]
    aspects = _aspects_by_name(metric)
    assert aspects["metricUpstreams"].datasetUpstreams == [
        EdgeClass(destinationUrn=smd_urn),
        EdgeClass(destinationUrn=customers_urn),
    ]


def test_metric_name_defaults_to_id() -> None:
    metric = Metric(
        platform="snowflake",
        path="analytics",
        id="total_revenue",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
    )
    assert metric.name == "total_revenue"


def test_metric_expression_with_dialect() -> None:
    metric = Metric(
        platform="snowflake",
        path="analytics",
        id="total_revenue",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        expression=DialectExpressionInput(
            expression="SUM(ORDERS.amount)", dialect=DialectClass.SNOWFLAKE
        ),
    )
    expr = metric.expression
    assert isinstance(expr, MetricExpressionClass)
    assert expr.dialects[0].dialect == DialectClass.SNOWFLAKE
    assert expr.dialects[0].expression == "SUM(ORDERS.amount)"


def test_metric_expression_bare_string_defaults_to_ansi_sql() -> None:
    metric = Metric(
        platform="snowflake",
        path="analytics",
        id="total_revenue",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        expression="2 * total_revenue",
    )
    expr = metric.expression
    assert expr is not None
    assert expr.dialects[0].dialect == DialectClass.ANSI_SQL
    assert expr.dialects[0].expression == "2 * total_revenue"


def test_metric_expression_omitted_when_absent() -> None:
    metric = Metric(
        platform="snowflake",
        path="analytics",
        id="total_revenue",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
    )
    assert metric.expression is None
    info = _aspects_by_name(metric)["metricInfo"]
    assert info.expression is None


def test_metric_relationships_always_emitted() -> None:
    metric = Metric(
        platform="snowflake",
        path="analytics",
        id="total_revenue",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
    )
    rels = _aspects_by_name(metric)["metricRelationships"]
    assert isinstance(rels, MetricRelationshipsClass)
    assert rels.derivedFrom == []
    assert rels.parentMetric is None


def test_metric_derived_from_records_destination_urns() -> None:
    parent_urn = "urn:li:metric:(urn:li:dataPlatform:snowflake,analytics,total_revenue)"
    metric = Metric(
        platform="snowflake",
        path="analytics",
        id="double_revenue",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        derived_from=[parent_urn],
    )
    rels = _aspects_by_name(metric)["metricRelationships"]
    assert len(rels.derivedFrom) == 1
    assert isinstance(rels.derivedFrom[0], DerivedMetricInputClass)
    assert rels.derivedFrom[0].destinationUrn == parent_urn
    assert rels.parentMetric is None


def test_metric_add_derived_from_dedupes() -> None:
    parent_urn = "urn:li:metric:(urn:li:dataPlatform:snowflake,analytics,total_revenue)"
    metric = Metric(
        platform="snowflake",
        path="analytics",
        id="double_revenue",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        derived_from=[parent_urn],
    )
    metric.add_derived_from(parent_urn)  # duplicate -> no-op
    metric.add_derived_from(
        "urn:li:metric:(urn:li:dataPlatform:snowflake,analytics,other_metric)"
    )
    assert [d.destinationUrn for d in metric.derived_from] == [
        parent_urn,
        "urn:li:metric:(urn:li:dataPlatform:snowflake,analytics,other_metric)",
    ]


def test_metric_ai_context_only_emitted_when_non_empty() -> None:
    # Empty aiContext -> no aspect.
    metric = Metric(
        platform="snowflake",
        path="analytics",
        id="total_revenue",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        ai_context=AiContextInput(),
    )
    assert "aiContext" not in _aspects_by_name(metric)
    assert metric.ai_context is None

    # Non-empty aiContext -> aspect emitted.
    metric2 = Metric(
        platform="snowflake",
        path="analytics",
        id="total_revenue",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        ai_context=AiContextInput(synonyms=["revenue"]),
    )
    ai = metric2.ai_context
    assert isinstance(ai, AiContextClass)
    assert ai.synonyms == ["revenue"]


def test_metric_created_last_modified_roundtrip() -> None:
    ts = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    metric = Metric(
        platform="snowflake",
        path="analytics",
        id="total_revenue",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        created=ts,
        last_modified=ts,
    )
    assert metric.created == ts
    assert metric.last_modified == ts


def test_metric_semantic_model_back_ref() -> None:
    model_urn = (
        "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)"
    )
    metric = Metric(
        platform="snowflake",
        path="analytics",
        id="total_revenue",
        semantic_model=model_urn,
    )
    assert metric.semantic_model == model_urn
    info = _aspects_by_name(metric)["metricInfo"]
    assert info.semanticModel == model_urn


def test_metric_platform() -> None:
    metric = Metric(
        platform="snowflake",
        path="analytics",
        id="total_revenue",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
    )
    assert metric.platform == DataPlatformUrn("urn:li:dataPlatform:snowflake")


def test_derived_from_mutators_preserve_sibling_relationship_fields() -> None:
    # Read-modify-write must not clobber parentMetric/relatedMetrics when only
    # derivedFrom changes.
    from datahub.metadata.schema_classes import EdgeClass

    parent = "urn:li:metric:(urn:li:dataPlatform:snowflake,analytics,base_revenue)"
    related = "urn:li:metric:(urn:li:dataPlatform:snowflake,analytics,related_revenue)"
    existing = "urn:li:metric:(urn:li:dataPlatform:snowflake,analytics,seed_revenue)"
    new = "urn:li:metric:(urn:li:dataPlatform:snowflake,analytics,extra_revenue)"

    metric = Metric(
        platform="snowflake",
        path="analytics",
        id="total_revenue",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
    )
    # Simulate a graph-hydrated aspect carrying server-set sibling fields.
    metric._set_aspect(
        MetricRelationshipsClass(
            parentMetric=parent,
            relatedMetrics=[EdgeClass(destinationUrn=related)],
            derivedFrom=[DerivedMetricInputClass(destinationUrn=existing)],
        )
    )

    metric.add_derived_from(new)
    rels = metric._get_aspect(MetricRelationshipsClass)
    assert rels is not None
    assert rels.parentMetric == parent
    assert [e.destinationUrn for e in rels.relatedMetrics] == [related]
    assert {d.destinationUrn for d in rels.derivedFrom} == {existing, new}

    metric.set_derived_from([new])
    rels = metric._get_aspect(MetricRelationshipsClass)
    assert rels is not None
    assert rels.parentMetric == parent
    assert [e.destinationUrn for e in rels.relatedMetrics] == [related]
    assert [d.destinationUrn for d in rels.derivedFrom] == [new]


_SM = "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)"


def test_derived_from_scalar_string_is_single_edge() -> None:
    # A bare URN string must be one edge, not one edge per character.
    from datahub.sdk import require_metrics_support  # noqa: F401  (import sanity)

    metric = Metric(
        platform="snowflake",
        path="analytics",
        id="revenue",
        semantic_model=_SM,
        derived_from="urn:li:metric:(urn:li:dataPlatform:snowflake,analytics,base)",
    )
    edges = metric.derived_from
    assert len(edges) == 1
    assert (
        edges[0].destinationUrn
        == "urn:li:metric:(urn:li:dataPlatform:snowflake,analytics,base)"
    )


def test_semantic_model_reference_rejects_blank_and_malformed() -> None:
    from datahub.errors import SdkUsageError

    for bad in ("", "   ", "urn:li:placeholder"):
        with pytest.raises(SdkUsageError):
            Metric(
                platform="snowflake",
                path="analytics",
                id="revenue",
                semantic_model=bad,
            )


def test_set_semantic_model_rejects_blank() -> None:
    from datahub.errors import SdkUsageError

    metric = Metric(
        platform="snowflake", path="analytics", id="revenue", semantic_model=_SM
    )
    with pytest.raises(SdkUsageError):
        metric.set_semantic_model("")


def test_clear_hydrated_ai_context_emits_empty_overwrite() -> None:
    # Clearing an aiContext that was read from the graph must emit an empty
    # aspect to overwrite the server value, not silently drop it.
    metric = Metric(
        platform="snowflake",
        path="analytics",
        id="revenue",
        semantic_model=_SM,
        ai_context=AiContextInput(synonyms=["rev"]),
    )
    hydrated = Metric._new_from_graph(metric.urn, dict(metric._aspects))
    assert hydrated.ai_context is not None

    hydrated.set_ai_context(AiContextInput())  # clear
    emitted = {mcp.aspectName: mcp.aspect for mcp in hydrated.as_mcps()}
    assert "aiContext" in emitted  # overwrite emitted, not dropped
    cleared = emitted["aiContext"]
    assert isinstance(cleared, AiContextClass)
    assert cleared.synonyms is None
    assert cleared.instructions is None
    assert cleared.examples is None
    assert cleared.customInstructions is None
