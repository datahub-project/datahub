"""Unit tests for the Metric SDK.

These tests assert the producer contract for the ``metric`` entity: URN
pattern, ``metricInfo`` shape (with optional expression and semantic-model
back-ref), the always-emitted ``metricRelationships`` (so ``hasParentMetric``
indexes as false), aiContext-only-when-non-empty, and the no-``metricUpstreams``
rule for semantic-model-backed metrics.
"""

from datetime import datetime, timezone
from typing import Any

from datahub.metadata.schema_classes import (
    AiContextClass,
    DerivedMetricInputClass,
    DialectClass,
    MetricExpressionClass,
    MetricInfoClass,
    MetricRelationshipsClass,
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
    # No metricUpstreams for semantic-model-backed metrics.
    assert "upstreamLineage" not in aspects
    # No aiContext when none provided.
    assert "aiContext" not in aspects


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
