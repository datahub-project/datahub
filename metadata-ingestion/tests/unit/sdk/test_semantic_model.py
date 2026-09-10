"""Unit tests for the SemanticModel + SemanticModelDataset SDK.

These tests assert the producer contract for the ``semanticModel`` entity and
its logical ``dataset`` entities: URN patterns, aspect shapes, the
``Semantic Model Dataset`` subtype, the ``semanticModelProperties`` back-ref,
the schemaField-anchored ``semanticFieldAnnotation`` + ``aiContext`` MCPs,
the required-expression fallback, and aiContext-only-when-non-empty.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Callable

import pytest

from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.metadata.schema_classes import (
    AiContextClass,
    DialectClass,
    ERModelRelationshipCardinalityClass,
    MetricRelationshipsClass,
    SemanticFieldTypeClass,
    SemanticModelInfoClass,
    SemanticModelPropertiesClass,
    StatusClass,
    SubTypesClass,
)
from datahub.metadata.urns import (
    DataPlatformUrn,
    DatasetUrn,
    SchemaFieldUrn,
    SemanticModelUrn,
)
from datahub.sdk.entity import Entity
from datahub.sdk.metric import Metric
from datahub.sdk.semantic_model import (
    AiContextInput,
    DialectExpressionInput,
    SemanticFieldInput,
    SemanticModel,
    SemanticModelDataset,
    SemanticModelRelationshipInput,
)


def _aspects_by_name(entity: Entity) -> dict[str, Any]:
    return {mcp.aspectName: mcp.aspect for mcp in entity.as_mcps() if mcp.aspectName}


def _mcps_by_urn(entity: Entity) -> dict[str, list[Any]]:
    out: dict[str, list[Any]] = {}
    for mcp in entity.as_mcps():
        if mcp.entityUrn:
            out.setdefault(mcp.entityUrn, []).append(mcp.aspect)
    return out


def test_semantic_model_urn_and_core_aspects() -> None:
    model = SemanticModel(
        platform="snowflake",
        path="analytics",
        id="orders_model",
        name="Orders Model",
        description="Orders semantic model",
    )
    assert model.urn == SemanticModelUrn(
        "urn:li:dataPlatform:snowflake", "analytics", "orders_model"
    )
    assert str(model.urn) == (
        "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)"
    )
    aspects = _aspects_by_name(model)
    assert "status" in aspects
    assert isinstance(aspects["status"], StatusClass)
    assert aspects["status"].removed is False
    assert "semanticModelInfo" in aspects
    info = aspects["semanticModelInfo"]
    assert isinstance(info, SemanticModelInfoClass)
    assert info.name == "Orders Model"
    assert info.description == "Orders semantic model"
    assert info.relationships is None
    # No aiContext when none provided.
    assert "aiContext" not in aspects


def test_semantic_model_name_defaults_to_id() -> None:
    model = SemanticModel(platform="snowflake", path="analytics", id="orders_model")
    assert model.name == "orders_model"


def test_semantic_model_created_last_modified_roundtrip() -> None:
    ts = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    model = SemanticModel(
        platform="snowflake",
        path="analytics",
        id="orders_model",
        created=ts,
        last_modified=ts,
    )
    assert model.created == ts
    assert model.last_modified == ts


def test_semantic_model_native_definition() -> None:
    model = SemanticModel(
        platform="snowflake",
        path="analytics",
        id="orders_model",
        native_definition="CREATE SEMANTIC VIEW ...",
    )
    assert model.native_definition == "CREATE SEMANTIC VIEW ..."


def test_semantic_model_add_dataset_records_urn_and_back_ref() -> None:
    model = SemanticModel(platform="snowflake", path="analytics", id="orders_model")
    ds = SemanticModelDataset(
        platform="snowflake",
        name="analytics.orders_model.orders_ds",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,placeholder)",
        alias="ORDERS",
        schema=[
            SemanticFieldInput(
                field_path="order_id",
                type="int",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
            )
        ],
    )
    model.add_dataset(ds)
    assert model.datasets == [str(ds.urn)]
    # Back-ref must be reconciled to the model's URN.
    props = ds._get_aspect(SemanticModelPropertiesClass)
    assert props is not None
    assert props.semanticModel == str(model.urn)
    assert props.alias == "ORDERS"
    assert ds.alias == "ORDERS"


def test_semantic_model_add_dataset_rejects_urn_string() -> None:
    from datahub.errors import SdkUsageError

    model = SemanticModel(platform="snowflake", path="analytics", id="orders_model")
    with pytest.raises(SdkUsageError, match="SemanticModelDataset"):
        model.add_dataset(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders_model.orders_ds,PROD)"  # type: ignore[arg-type]
        )


def test_semantic_model_datasets_preserve_insertion_order() -> None:
    model = SemanticModel(platform="snowflake", path="analytics", id="orders_model")
    ds1 = SemanticModelDataset(
        platform="snowflake",
        name="analytics.orders_model.orders_ds",
        semantic_model=model.urn,
        alias="ORDERS",
        schema=[
            SemanticFieldInput(
                field_path="order_id",
                type="int",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
            )
        ],
    )
    ds2 = SemanticModelDataset(
        platform="snowflake",
        name="analytics.orders_model.customers_ds",
        semantic_model=model.urn,
        alias="CUSTOMERS",
        schema=[
            SemanticFieldInput(
                field_path="customer_id",
                type="int",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
            )
        ],
    )
    model.set_datasets([ds1, ds2])
    assert model.datasets == [str(ds1.urn), str(ds2.urn)]


def test_semantic_model_relationships() -> None:
    model = SemanticModel(
        platform="snowflake",
        path="analytics",
        id="orders_model",
        relationships=[
            SemanticModelRelationshipInput(
                from_alias="ORDERS",
                from_columns=["customer_id"],
                to_alias="CUSTOMERS",
                to_columns=["id"],
                name="orders_to_customers",
                cardinality=ERModelRelationshipCardinalityClass.N_ONE,
            )
        ],
    )
    rels = model.relationships
    assert rels is not None and len(rels) == 1
    rel = rels[0]
    assert rel.name == "orders_to_customers"
    assert rel.from_ == "ORDERS"
    assert rel.fromColumns == ["customer_id"]
    assert rel.to == "CUSTOMERS"
    assert rel.toColumns == ["id"]
    assert rel.cardinality == ERModelRelationshipCardinalityClass.N_ONE


def test_semantic_model_ai_context_only_emitted_when_non_empty() -> None:
    # Empty aiContext -> no aspect.
    model = SemanticModel(
        platform="snowflake",
        path="analytics",
        id="orders_model",
        ai_context=AiContextInput(),
    )
    assert "aiContext" not in _aspects_by_name(model)
    assert model.ai_context is None

    # Non-empty aiContext -> aspect emitted with all four fields.
    model2 = SemanticModel(
        platform="snowflake",
        path="analytics",
        id="orders_model",
        ai_context=AiContextInput(
            synonyms=["orders model"],
            instructions="Use for revenue analysis.",
            examples=["SELECT * FROM orders"],
            custom_instructions="Always filter by date.",
        ),
    )
    ai = model2.ai_context
    assert isinstance(ai, AiContextClass)
    assert ai.synonyms == ["orders model"]
    assert ai.instructions == "Use for revenue analysis."
    assert ai.examples == ["SELECT * FROM orders"]
    assert ai.customInstructions == "Always filter by date."


def test_semantic_model_platform() -> None:
    model = SemanticModel(platform="snowflake", path="analytics", id="orders_model")
    assert model.platform == DataPlatformUrn("urn:li:dataPlatform:snowflake")


def test_logical_dataset_urn_and_subtype() -> None:
    ds = SemanticModelDataset(
        platform="snowflake",
        name="analytics.orders_model.orders_ds",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        alias="ORDERS",
        schema=[
            SemanticFieldInput(
                field_path="order_id",
                type="int",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
            )
        ],
    )
    assert ds.urn == DatasetUrn(
        "urn:li:dataPlatform:snowflake", "analytics.orders_model.orders_ds", "PROD"
    )
    assert ds.subtype == DatasetSubTypes.SEMANTIC_MODEL_DATASET
    aspects = _aspects_by_name(ds)
    assert isinstance(aspects["subTypes"], SubTypesClass)
    assert aspects["subTypes"].typeNames == [DatasetSubTypes.SEMANTIC_MODEL_DATASET]


def test_logical_dataset_semantic_model_properties_back_ref() -> None:
    model_urn = (
        "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)"
    )
    ds = SemanticModelDataset(
        platform="snowflake",
        name="analytics.orders_model.orders_ds",
        semantic_model=model_urn,
        alias="ORDERS",
        schema=[
            SemanticFieldInput(
                field_path="order_id",
                type="int",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
            )
        ],
    )
    props = ds._get_aspect(SemanticModelPropertiesClass)
    assert props is not None
    assert props.alias == "ORDERS"
    assert props.semanticModel == model_urn


def test_logical_dataset_emits_schemafield_anchored_annotations() -> None:
    ds = SemanticModelDataset(
        platform="snowflake",
        name="analytics.orders_model.orders_ds",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        alias="ORDERS",
        schema=[
            SemanticFieldInput(
                field_path="order_id",
                type="int",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
                is_part_of_key=True,
            ),
            SemanticFieldInput(
                field_path="amount",
                type="float",
                semantic_type=SemanticFieldTypeClass.MEASURE,
                aggregation_function="SUM",
            ),
        ],
    )
    by_urn = _mcps_by_urn(ds)
    ds_urn = str(ds.urn)
    # Dataset-anchored aspects.
    assert "schemaMetadata" in [a.ASPECT_NAME for a in by_urn[ds_urn]]
    # Each field has a schemaField-anchored semanticFieldAnnotation.
    for field_path in ("order_id", "amount"):
        field_urn = SchemaFieldUrn(ds_urn, field_path).urn()
        anns = by_urn.get(field_urn, [])
        assert any(a.ASPECT_NAME == "semanticFieldAnnotation" for a in anns), (
            f"missing annotation for {field_path}"
        )
    # No aiContext MCPs when none of the fields carry ai_context.
    assert all(a.ASPECT_NAME != "aiContext" for anns in by_urn.values() for a in anns)


def test_logical_dataset_annotation_required_expression_fallback() -> None:
    ds = SemanticModelDataset(
        platform="snowflake",
        name="analytics.orders_model.orders_ds",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        alias="ORDERS",
        schema=[
            SemanticFieldInput(
                field_path="order_id",
                type="int",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
                # No expression -> must auto-synthesize "ORDERS.order_id".
            )
        ],
    )
    by_urn = _mcps_by_urn(ds)
    field_urn = SchemaFieldUrn(str(ds.urn), "order_id").urn()
    ann = next(
        a for a in by_urn[field_urn] if a.ASPECT_NAME == "semanticFieldAnnotation"
    )
    assert ann.expression is not None
    assert ann.expression.dialects[0].expression == "ORDERS.order_id"
    assert ann.type == SemanticFieldTypeClass.DIMENSION
    assert ann.dimension is not None
    assert ann.dimension.isTime is False


def test_logical_dataset_annotation_explicit_expression_preserved() -> None:
    ds = SemanticModelDataset(
        platform="snowflake",
        name="analytics.orders_model.orders_ds",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        alias="ORDERS",
        schema=[
            SemanticFieldInput(
                field_path="amount",
                type="float",
                semantic_type=SemanticFieldTypeClass.MEASURE,
                expression=DialectExpressionInput(
                    expression="SUM(amount)", dialect=DialectClass.SNOWFLAKE
                ),
                aggregation_function="SUM",
            )
        ],
    )
    by_urn = _mcps_by_urn(ds)
    field_urn = SchemaFieldUrn(str(ds.urn), "amount").urn()
    ann = next(
        a for a in by_urn[field_urn] if a.ASPECT_NAME == "semanticFieldAnnotation"
    )
    assert ann.expression.dialects[0].expression == "SUM(amount)"
    assert ann.expression.dialects[0].dialect == DialectClass.SNOWFLAKE
    assert ann.aggregationFunction == "SUM"
    assert ann.dimension is None  # only set for DIMENSION


def test_logical_dataset_time_dimension() -> None:
    ds = SemanticModelDataset(
        platform="snowflake",
        name="analytics.orders_model.orders_ds",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        alias="ORDERS",
        schema=[
            SemanticFieldInput(
                field_path="order_ts",
                type="timestamp",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
                is_time_dimension=True,
            )
        ],
    )
    by_urn = _mcps_by_urn(ds)
    ann = next(
        a
        for a in by_urn[SchemaFieldUrn(str(ds.urn), "order_ts").urn()]
        if a.ASPECT_NAME == "semanticFieldAnnotation"
    )
    assert ann.dimension is not None
    assert ann.dimension.isTime is True


def test_logical_dataset_field_ai_context_only_when_non_empty() -> None:
    ds = SemanticModelDataset(
        platform="snowflake",
        name="analytics.orders_model.orders_ds",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        alias="ORDERS",
        schema=[
            SemanticFieldInput(
                field_path="amount",
                type="float",
                semantic_type=SemanticFieldTypeClass.MEASURE,
                ai_context=AiContextInput(synonyms=["revenue"]),
            ),
            SemanticFieldInput(
                field_path="order_id",
                type="int",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
                # No ai_context -> no aiContext MCP for this field.
            ),
        ],
    )
    by_urn = _mcps_by_urn(ds)
    amount_urn = SchemaFieldUrn(str(ds.urn), "amount").urn()
    order_id_urn = SchemaFieldUrn(str(ds.urn), "order_id").urn()
    assert any(a.ASPECT_NAME == "aiContext" for a in by_urn[amount_urn])
    assert not any(a.ASPECT_NAME == "aiContext" for a in by_urn[order_id_urn])


def test_end_to_end_semantic_model_with_metrics() -> None:
    """Build a small model with two logical datasets, a relationship, and two
    metrics (one derived from the other), then assert the full aspect set and
    the lineage wiring (Metric -> Logical Dataset -> Physical; SM is a container).
    """
    model_urn_str = (
        "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)"
    )

    orders_ds = SemanticModelDataset(
        platform="snowflake",
        name="analytics.orders_model.orders_ds",
        semantic_model=model_urn_str,
        alias="ORDERS",
        schema=[
            SemanticFieldInput(
                field_path="order_id",
                type="int",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
                is_part_of_key=True,
            ),
            SemanticFieldInput(
                field_path="customer_id",
                type="int",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
            ),
            SemanticFieldInput(
                field_path="order_ts",
                type="timestamp",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
                is_time_dimension=True,
            ),
            SemanticFieldInput(
                field_path="amount",
                type="float",
                semantic_type=SemanticFieldTypeClass.MEASURE,
                expression=DialectExpressionInput(
                    expression="SUM(amount)", dialect=DialectClass.SNOWFLAKE
                ),
                aggregation_function="SUM",
            ),
        ],
        upstreams=["urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.orders,PROD)"],
    )
    customers_ds = SemanticModelDataset(
        platform="snowflake",
        name="analytics.orders_model.customers_ds",
        semantic_model=model_urn_str,
        alias="CUSTOMERS",
        schema=[
            SemanticFieldInput(
                field_path="customer_id",
                type="int",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
                is_part_of_key=True,
            ),
            SemanticFieldInput(
                field_path="customer_name",
                type="varchar",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
            ),
        ],
        upstreams=["urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.customers,PROD)"],
    )

    total_revenue = Metric(
        platform="snowflake",
        path="analytics",
        id="total_revenue",
        semantic_model=model_urn_str,
        name="Total Revenue",
        expression=DialectExpressionInput(
            expression="SUM(ORDERS.amount)", dialect=DialectClass.SNOWFLAKE
        ),
        upstream_datasets=[orders_ds.urn],
        ai_context=AiContextInput(synonyms=["revenue"]),
    )
    double_revenue = Metric(
        platform="snowflake",
        path="analytics",
        id="double_revenue",
        semantic_model=model_urn_str,
        name="Double Revenue",
        expression="2 * total_revenue",
        derived_from=[total_revenue.urn],
    )

    model = SemanticModel(
        platform="snowflake",
        path="analytics",
        id="orders_model",
        name="Orders Model",
        description="Orders semantic model",
        datasets=[orders_ds, customers_ds],
        relationships=[
            SemanticModelRelationshipInput(
                from_alias="ORDERS",
                from_columns=["customer_id"],
                to_alias="CUSTOMERS",
                to_columns=["customer_id"],
                cardinality=ERModelRelationshipCardinalityClass.N_ONE,
            )
        ],
    )

    # --- Semantic model aspects ---
    model_aspects = _aspects_by_name(model)
    assert isinstance(model_aspects["status"], StatusClass)
    info = model_aspects["semanticModelInfo"]
    # Membership is member-side only; SemanticModelInfo no longer lists datasets/metrics.
    assert info.relationships is not None and len(info.relationships) == 1
    assert info.relationships[0].from_ == "ORDERS"
    assert info.relationships[0].to == "CUSTOMERS"

    # --- Logical dataset aspects ---
    for ds in (orders_ds, customers_ds):
        ds_aspects = _aspects_by_name(ds)
        assert ds_aspects["subTypes"].typeNames == [
            DatasetSubTypes.SEMANTIC_MODEL_DATASET
        ]
        props = ds_aspects["semanticModelProperties"]
        assert props.semanticModel == str(model.urn)
        assert "schemaMetadata" in ds_aspects
        assert "upstreamLineage" in ds_aspects
        assert ds_aspects["semanticModelProperties"].semanticModel == str(model.urn)

    # Field-anchored annotations exist for every field.
    orders_by_urn = _mcps_by_urn(orders_ds)
    for field_path in ("order_id", "order_ts", "amount"):
        field_urn = SchemaFieldUrn(str(orders_ds.urn), field_path).urn()
        assert any(
            a.ASPECT_NAME == "semanticFieldAnnotation" for a in orders_by_urn[field_urn]
        )

    # --- Metric aspects ---
    tr_aspects = _aspects_by_name(total_revenue)
    assert tr_aspects["metricInfo"].semanticModel == str(model.urn)
    assert tr_aspects["metricInfo"].expression is not None
    assert tr_aspects["metricInfo"].expression.dialects[0].dialect == (
        DialectClass.SNOWFLAKE
    )
    assert isinstance(tr_aspects["aiContext"], AiContextClass)
    assert tr_aspects["aiContext"].synonyms == ["revenue"]
    assert isinstance(tr_aspects["metricRelationships"], MetricRelationshipsClass)
    assert tr_aspects["metricRelationships"].derivedFrom == []
    # Metric → SMD lineage via metricUpstreams.
    assert "metricUpstreams" in tr_aspects
    assert [
        e.destinationUrn for e in tr_aspects["metricUpstreams"].datasetUpstreams
    ] == [str(orders_ds.urn)]

    dr_aspects = _aspects_by_name(double_revenue)
    assert dr_aspects["metricInfo"].expression.dialects[0].dialect == (
        DialectClass.ANSI_SQL
    )
    assert [
        d.destinationUrn for d in dr_aspects["metricRelationships"].derivedFrom
    ] == [str(total_revenue.urn)]
    # No aiContext when none provided.
    assert "aiContext" not in dr_aspects

    # --- Lineage / containment wiring ---
    # Containment: Metrics reference their SemanticModel via ModeledBy.
    assert total_revenue.semantic_model == str(model.urn)
    assert double_revenue.semantic_model == str(model.urn)
    # Containment: SemanticModel → Logical Datasets (local attach list only).
    assert str(orders_ds.urn) in model.datasets
    assert str(customers_ds.urn) in model.datasets
    # Lineage: Metric → Logical Dataset → Physical.
    assert total_revenue.upstream_datasets == [str(orders_ds.urn)]
    assert orders_ds.upstreams is not None
    assert customers_ds.upstreams is not None
    assert (
        orders_ds.upstreams.upstreams[0].dataset
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.orders,PROD)"
    )
    assert (
        customers_ds.upstreams.upstreams[0].dataset
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.customers,PROD)"
    )


def test_logical_dataset_duplicate_field_path_raises() -> None:
    """Two SemanticFieldInput specs with the same field_path must raise rather
    than silently dropping one of the per-field annotations.
    """
    from datahub.errors import SdkUsageError

    try:
        SemanticModelDataset(
            platform="snowflake",
            name="analytics.orders_model.orders_ds",
            semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
            alias="ORDERS",
            schema=[
                SemanticFieldInput(
                    field_path="order_id",
                    type="int",
                    semantic_type=SemanticFieldTypeClass.DIMENSION,
                ),
                SemanticFieldInput(
                    field_path="order_id",
                    type="int",
                    semantic_type=SemanticFieldTypeClass.DIMENSION,
                ),
            ],
        )
    except SdkUsageError as e:
        assert "order_id" in str(e)
    else:
        raise AssertionError("expected SdkUsageError for duplicate field_path")


def test_logical_dataset_tag_validation_runs() -> None:
    """SemanticFieldInput(tags=[...]) must route through the canonical tag
    parser so a bare tag name (not a URN) is rejected rather than silently
    emitting an invalid tag URN.
    """
    from datahub.utilities.urns.error import InvalidUrnError

    with pytest.raises(InvalidUrnError):
        SemanticModelDataset(
            platform="snowflake",
            name="analytics.orders_model.orders_ds",
            semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
            alias="ORDERS",
            schema=[
                SemanticFieldInput(
                    field_path="order_id",
                    type="int",
                    semantic_type=SemanticFieldTypeClass.DIMENSION,
                    tags=["PII"],
                )
            ],
        )


def test_relationship_alias_mismatch_warns(caplog: Any) -> None:
    """A relationship alias with no matching attached dataset should emit a
    warning (best-effort, never raise).
    """
    import logging

    model_urn_str = (
        "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)"
    )
    orders_ds = SemanticModelDataset(
        platform="snowflake",
        name="analytics.orders_model.orders_ds",
        semantic_model=model_urn_str,
        alias="ORDERS",
        schema=[
            SemanticFieldInput(
                field_path="order_id",
                type="int",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
            )
        ],
    )
    model = SemanticModel(
        platform="snowflake",
        path="analytics",
        id="orders_model",
        datasets=[orders_ds],
    )
    with caplog.at_level(logging.WARNING, logger="datahub.sdk.semantic_model"):
        model.set_relationships(
            [
                SemanticModelRelationshipInput(
                    from_alias="ORDERS",
                    from_columns=["customer_id"],
                    to_alias="TYPO_CUSTOMERS",
                    to_columns=["customer_id"],
                )
            ]
        )
    assert any("TYPO_CUSTOMERS" in r.message for r in caplog.records)


def _build_governance_kwargs():
    return dict(
        owners=["urn:li:corpuser:datahub"],
        links=["https://example.com/docs"],
        tags=["urn:li:tag:PII"],
        terms=["urn:li:glossaryTerm:Revenue.abc"],
        domain="urn:li:domain:Analytics",
        structured_properties={"urn:li:structuredProperty:team": ["data"]},
    )


def _governance_aspect_names():
    return {
        "institutionalMemory",
        "ownership",
        "globalTags",
        "glossaryTerms",
        "domains",
        "structuredProperties",
    }


@pytest.mark.parametrize(
    "builder",
    [
        pytest.param(
            lambda: SemanticModel(
                platform="snowflake",
                path="analytics",
                id="orders_model",
                **_build_governance_kwargs(),
            ),
            id="SemanticModel",
        ),
        pytest.param(
            lambda: Metric(
                platform="snowflake",
                path="analytics",
                id="total_revenue",
                semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
                **_build_governance_kwargs(),
            ),
            id="Metric",
        ),
        pytest.param(
            lambda: SemanticModelDataset(
                platform="snowflake",
                name="analytics.orders_model.orders_ds",
                semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
                alias="ORDERS",
                schema=[
                    SemanticFieldInput(
                        field_path="order_id",
                        type="int",
                        semantic_type=SemanticFieldTypeClass.DIMENSION,
                    )
                ],
                **_build_governance_kwargs(),
            ),
            id="SemanticModelDataset",
        ),
    ],
)
def test_governance_kwargs_land_in_aspects(builder: Callable[[], Entity]) -> None:
    entity = builder()
    aspects = _aspects_by_name(entity)
    for name in _governance_aspect_names():
        assert name in aspects, f"{name} missing from {type(entity).__name__}"


def test_require_metrics_support_cloud_below_min_raises() -> None:
    from unittest.mock import MagicMock

    from datahub.errors import SdkUsageError
    from datahub.sdk import require_metrics_support

    graph = MagicMock()
    graph.server_config.supports_feature.return_value = False
    graph.server_config.is_datahub_cloud = True
    graph.server_config.service_version = "2.0.0"
    with pytest.raises(SdkUsageError):
        require_metrics_support(graph)


def test_require_metrics_support_oss_fails_open() -> None:
    # OSS reports SEMANTIC_MODEL_ENTITIES unsupported (no core requirement); the
    # SDK must not block OSS emits — it's operator/recipe-driven there.
    from unittest.mock import MagicMock

    from datahub.sdk import require_metrics_support

    graph = MagicMock()
    graph.server_config.supports_feature.return_value = False
    graph.server_config.is_datahub_cloud = False
    require_metrics_support(graph)  # should not raise


def test_require_metrics_support_supported_is_noop() -> None:
    from unittest.mock import MagicMock

    from datahub.sdk import require_metrics_support

    graph = MagicMock()
    graph.server_config.supports_feature.return_value = True
    require_metrics_support(graph)  # should not raise


def test_require_metrics_support_no_server_config_fail_open() -> None:
    from datahub.sdk import require_metrics_support

    class _BareGraph:
        pass

    require_metrics_support(_BareGraph())  # should not raise


def test_require_metrics_support_non_semver_fails_open() -> None:
    # A non-semver build makes supports_feature raise ValueError; the preflight
    # helper must fail open, not leak a parse error.
    from unittest.mock import MagicMock

    from datahub.sdk import require_metrics_support

    graph = MagicMock()
    graph.server_config.supports_feature.side_effect = ValueError(
        "Invalid version format: dev-snapshot"
    )
    require_metrics_support(graph)  # should not raise


def test_entity_classes_dataset_not_overwritten() -> None:
    # Regression: SemanticModelDataset is a Dataset subtype sharing the
    # "dataset" entity type. It must NOT be registered in ENTITY_CLASSES, or it
    # would overwrite Dataset and every dataset URN would hydrate as a
    # SemanticModelDataset on read-back.
    from datahub.sdk._all_entities import ENTITY_CLASSES
    from datahub.sdk.dataset import Dataset

    assert ENTITY_CLASSES["dataset"] is Dataset
    assert ENTITY_CLASSES[SemanticModel.get_urn_type().ENTITY_TYPE] is SemanticModel
    assert ENTITY_CLASSES[Metric.get_urn_type().ENTITY_TYPE] is Metric


def test_metric_requires_semantic_model() -> None:
    # The whole feature hinges on metrics being semantic-model-backed;
    # constructing one without a semantic_model must be rejected.
    with pytest.raises(TypeError):
        Metric(platform="snowflake", path="analytics", id="revenue")  # type: ignore[call-arg]


def test_field_expression_empty_rejected() -> None:
    from datahub.errors import SdkUsageError

    with pytest.raises(SdkUsageError):
        SemanticModelDataset(
            platform="snowflake",
            name="analytics.orders_model.orders_ds",
            semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
            alias="ORDERS",
            schema=[
                SemanticFieldInput(
                    field_path="amount",
                    type="double",
                    semantic_type=SemanticFieldTypeClass.DIMENSION,
                    expression="   ",
                )
            ],
        )


def test_metric_expression_empty_rejected() -> None:
    from datahub.errors import SdkUsageError

    with pytest.raises(SdkUsageError):
        Metric(
            platform="snowflake",
            path="analytics",
            id="revenue",
            semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
            expression="",
        )


def test_relationship_alias_mismatch_raises_at_emit() -> None:
    # With full alias coverage (every dataset attached as a SemanticModelDataset),
    # a mistyped relationship alias must be caught at emit time, not silently
    # shipped to the server.
    from datahub.errors import SdkUsageError

    orders_ds = SemanticModelDataset(
        platform="snowflake",
        name="analytics.orders_model.orders_ds",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        alias="ORDERS",
        schema=[
            SemanticFieldInput(
                field_path="order_id",
                type="int",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
            )
        ],
    )
    model = SemanticModel(
        platform="snowflake",
        path="analytics",
        id="orders_model",
        datasets=[orders_ds],
        relationships=[
            SemanticModelRelationshipInput(
                from_alias="ORDERS",
                from_columns=["customer_id"],
                to_alias="TYPO_CUSTOMERS",
                to_columns=["customer_id"],
            )
        ],
    )
    with pytest.raises(SdkUsageError):
        model.as_mcps()


def test_field_annotation_mcps_inherit_change_type() -> None:
    # schemaField-anchored aspects must follow the requested change_type, not
    # silently default to UPSERT (e.g. EntityClient.create requests CREATE).
    from datahub.metadata.schema_classes import ChangeTypeClass

    ds = SemanticModelDataset(
        platform="snowflake",
        name="analytics.orders_model.orders_ds",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        alias="ORDERS",
        schema=[
            SemanticFieldInput(
                field_path="order_id",
                type="int",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
                ai_context=AiContextInput(synonyms=["oid"]),
            )
        ],
    )
    mcps = ds.as_mcps(change_type=ChangeTypeClass.CREATE)
    field_mcps = [m for m in mcps if m.entityUrn and "schemaField" in m.entityUrn]
    assert field_mcps  # both semanticFieldAnnotation and aiContext are anchored here
    assert all(m.changeType == ChangeTypeClass.CREATE for m in field_mcps)


def _orders_ds(schema: list) -> SemanticModelDataset:
    return SemanticModelDataset(
        platform="snowflake",
        name="analytics.orders_model.orders_ds",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        alias="ORDERS",
        schema=schema,
    )


def test_relationship_no_datasets_raises_at_emit() -> None:
    # Relationships with zero datasets attached can't resolve any alias; the
    # strict emit-time check must catch this instead of returning early.
    from datahub.errors import SdkUsageError

    model = SemanticModel(
        platform="snowflake",
        path="analytics",
        id="orders_model",
        relationships=[
            SemanticModelRelationshipInput(
                from_alias="ORDERS",
                from_columns=["customer_id"],
                to_alias="CUSTOMERS",
                to_columns=["customer_id"],
            )
        ],
    )
    with pytest.raises(SdkUsageError):
        model.as_mcps()


def test_relationship_duplicate_dataset_does_not_defeat_coverage() -> None:
    # A duplicated attachment must not make full-coverage detection fail and
    # downgrade a real alias mismatch to a warning.
    from datahub.errors import SdkUsageError

    orders_ds = _orders_ds(
        [
            SemanticFieldInput(
                field_path="customer_id",
                type="int",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
            )
        ]
    )
    model = SemanticModel(
        platform="snowflake",
        path="analytics",
        id="orders_model",
        datasets=[orders_ds, orders_ds],  # duplicate on purpose
        relationships=[
            SemanticModelRelationshipInput(
                from_alias="ORDERS",
                from_columns=["customer_id"],
                to_alias="TYPO_CUSTOMERS",
                to_columns=["customer_id"],
            )
        ],
    )
    with pytest.raises(SdkUsageError):
        model.as_mcps()


def test_relationship_missing_join_column_raises_at_emit() -> None:
    # Aliases match, but the join column is absent from the dataset schema.
    from datahub.errors import SdkUsageError

    orders_ds = _orders_ds(
        [
            SemanticFieldInput(
                field_path="order_id",
                type="int",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
            )
        ]
    )
    customers_ds = SemanticModelDataset(
        platform="snowflake",
        name="analytics.orders_model.customers_ds",
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        alias="CUSTOMERS",
        schema=[
            SemanticFieldInput(
                field_path="customer_id",
                type="int",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
            )
        ],
    )
    model = SemanticModel(
        platform="snowflake",
        path="analytics",
        id="orders_model",
        datasets=[orders_ds, customers_ds],
        relationships=[
            SemanticModelRelationshipInput(
                from_alias="ORDERS",
                from_columns=["customer_id"],  # not in ORDERS schema
                to_alias="CUSTOMERS",
                to_columns=["customer_id"],
            )
        ],
    )
    with pytest.raises(SdkUsageError):
        model.as_mcps()


def test_metric_expression_empty_list_rejected() -> None:
    from datahub.errors import SdkUsageError

    with pytest.raises(SdkUsageError):
        Metric(
            platform="snowflake",
            path="analytics",
            id="revenue",
            semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
            expression=[],
        )


def test_metric_expression_prebuilt_empty_dialects_rejected() -> None:
    from datahub.errors import SdkUsageError
    from datahub.metadata.schema_classes import MetricExpressionClass

    with pytest.raises(SdkUsageError):
        Metric(
            platform="snowflake",
            path="analytics",
            id="revenue",
            semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
            expression=MetricExpressionClass(dialects=[]),
        )


def test_require_metrics_support_accepts_client() -> None:
    # require_metrics_support unwraps DataHubClient._graph (the client keeps its
    # graph private), so passing the client works as the docs show.
    from unittest.mock import MagicMock

    from datahub.errors import SdkUsageError
    from datahub.sdk import DataHubClient, require_metrics_support

    graph = MagicMock()
    graph.server_config.supports_feature.return_value = False
    graph.server_config.is_datahub_cloud = True
    graph.server_config.service_version = "1.0.0"
    client = DataHubClient(graph=graph)
    with pytest.raises(SdkUsageError):
        require_metrics_support(client)


def _ds(alias: str, name: str, cols: list) -> SemanticModelDataset:
    return SemanticModelDataset(
        platform="snowflake",
        name=name,
        semantic_model="urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)",
        alias=alias,
        schema=[
            SemanticFieldInput(
                field_path=c, type="int", semantic_type=SemanticFieldTypeClass.DIMENSION
            )
            for c in cols
        ],
    )


def _model_with(
    datasets: list, relationship: SemanticModelRelationshipInput
) -> SemanticModel:
    return SemanticModel(
        platform="snowflake",
        path="analytics",
        id="orders_model",
        datasets=datasets,
        relationships=[relationship],
    )


def test_datasets_rejects_bare_urn_string() -> None:
    from datahub.errors import SdkUsageError

    urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders_model.orders_ds,PROD)"
    with pytest.raises(SdkUsageError, match="SemanticModelDataset"):
        SemanticModel(
            platform="snowflake",
            path="analytics",
            id="orders_model",
            datasets=urn,  # type: ignore[arg-type]
        )


def test_semantic_model_dataset_rejects_blank_semantic_model() -> None:
    from datahub.errors import SdkUsageError

    with pytest.raises(SdkUsageError):
        SemanticModelDataset(
            platform="snowflake",
            name="analytics.orders_model.orders_ds",
            semantic_model="",
            alias="ORDERS",
            schema=[
                SemanticFieldInput(
                    field_path="order_id",
                    type="int",
                    semantic_type=SemanticFieldTypeClass.DIMENSION,
                )
            ],
        )


def test_relationship_blank_alias_raises_at_emit() -> None:
    from datahub.errors import SdkUsageError

    orders = _ds("ORDERS", "analytics.orders_model.orders_ds", ["customer_id"])
    model = _model_with(
        [orders],
        SemanticModelRelationshipInput(
            from_alias="",
            from_columns=["customer_id"],
            to_alias="ORDERS",
            to_columns=["customer_id"],
        ),
    )
    with pytest.raises(SdkUsageError):
        model.as_mcps()


def test_relationship_unequal_column_counts_raises_at_emit() -> None:
    from datahub.errors import SdkUsageError

    orders = _ds(
        "ORDERS", "analytics.orders_model.orders_ds", ["customer_id", "region_id"]
    )
    customers = _ds("CUSTOMERS", "analytics.orders_model.customers_ds", ["customer_id"])
    model = _model_with(
        [orders, customers],
        SemanticModelRelationshipInput(
            from_alias="ORDERS",
            from_columns=["customer_id", "region_id"],
            to_alias="CUSTOMERS",
            to_columns=["customer_id"],
        ),
    )
    with pytest.raises(SdkUsageError):
        model.as_mcps()


def test_relationship_empty_columns_raises_at_emit() -> None:
    from datahub.errors import SdkUsageError

    orders = _ds("ORDERS", "analytics.orders_model.orders_ds", ["customer_id"])
    customers = _ds("CUSTOMERS", "analytics.orders_model.customers_ds", ["customer_id"])
    model = _model_with(
        [orders, customers],
        SemanticModelRelationshipInput(
            from_alias="ORDERS",
            from_columns=[],
            to_alias="CUSTOMERS",
            to_columns=[],
        ),
    )
    with pytest.raises(SdkUsageError):
        model.as_mcps()


def test_relationship_duplicate_alias_distinct_datasets_raises_at_emit() -> None:
    from datahub.errors import SdkUsageError

    ds1 = _ds("DUP", "analytics.orders_model.a_ds", ["customer_id"])
    ds2 = _ds("DUP", "analytics.orders_model.b_ds", ["customer_id"])
    model = _model_with(
        [ds1, ds2],
        SemanticModelRelationshipInput(
            from_alias="DUP",
            from_columns=["customer_id"],
            to_alias="DUP",
            to_columns=["customer_id"],
        ),
    )
    with pytest.raises(SdkUsageError):
        model.as_mcps()


def test_semantic_model_clear_hydrated_ai_context_emits_empty_overwrite() -> None:
    model = SemanticModel(
        platform="snowflake",
        path="analytics",
        id="orders_model",
        ai_context=AiContextInput(synonyms=["orders"]),
    )
    hydrated = SemanticModel._new_from_graph(model.urn, dict(model._aspects))
    assert hydrated.ai_context is not None

    hydrated.set_ai_context(AiContextInput())  # clear
    emitted = {mcp.aspectName: mcp.aspect for mcp in hydrated.as_mcps()}
    assert "aiContext" in emitted
    cleared = emitted["aiContext"]
    assert isinstance(cleared, AiContextClass)
    assert cleared.synonyms is None


def test_hydrated_model_partial_add_dataset_warns_on_other_aliases(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """get() does not load members; adding one SMD must not raise on other aliases."""
    model = SemanticModel(
        platform="snowflake",
        path="analytics",
        id="orders_model",
        relationships=[
            SemanticModelRelationshipInput(
                name="orders_customers",
                from_alias="ORDERS",
                from_columns=["customer_id"],
                to_alias="CUSTOMERS",
                to_columns=["id"],
            ),
            SemanticModelRelationshipInput(
                name="orders_items",
                from_alias="ORDERS",
                from_columns=["order_id"],
                to_alias="ITEMS",
                to_columns=["order_id"],
            ),
        ],
    )
    hydrated = SemanticModel._new_from_graph(model.urn, dict(model._aspects))
    hydrated.add_dataset(
        _ds("ORDERS", "analytics.orders_model.orders_ds", ["customer_id", "order_id"])
    )

    with caplog.at_level(logging.WARNING, logger="datahub.sdk.semantic_model"):
        mcps = hydrated.as_mcps()
    assert mcps
    warning_text = " ".join(r.message for r in caplog.records)
    assert "CUSTOMERS" in warning_text or "ITEMS" in warning_text


def test_hydrated_no_edit_omits_semantic_model_info() -> None:
    """Hydrated get → emit with no edits must not emit semanticModelInfo."""
    stale = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders_model.orders_ds,PROD)",
    ]
    model = SemanticModel(platform="snowflake", path="analytics", id="orders_model")
    model._ensure_model_props().datasets = list(stale)
    hydrated = SemanticModel._new_from_graph(model.urn, dict(model._aspects))

    aspect_names = {mcp.aspectName for mcp in hydrated.as_mcps()}
    assert "semanticModelInfo" not in aspect_names
    assert hydrated._ensure_model_props().datasets == stale


def test_hydrated_edit_native_definition_emits_with_datasets_cleared() -> None:
    stale = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders_model.orders_ds,PROD)",
    ]
    model = SemanticModel(platform="snowflake", path="analytics", id="orders_model")
    model._ensure_model_props().datasets = list(stale)
    hydrated = SemanticModel._new_from_graph(model.urn, dict(model._aspects))

    hydrated.set_native_definition("CREATE SEMANTIC VIEW ...")
    info = next(
        mcp.aspect
        for mcp in hydrated.as_mcps()
        if mcp.aspectName == "semanticModelInfo"
    )
    assert isinstance(info, SemanticModelInfoClass)
    assert info.datasets == []
    assert info.nativeDefinition == "CREATE SEMANTIC VIEW ..."
    assert hydrated._ensure_model_props().datasets == stale


def test_hydrated_add_dataset_does_not_mutate_in_memory_aspect() -> None:
    stale = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders_model.orders_ds,PROD)",
    ]
    model = SemanticModel(
        platform="snowflake",
        path="analytics",
        id="orders_model",
        datasets=[
            _ds("ORDERS", "analytics.orders_model.orders_ds", ["order_id"]),
        ],
    )
    model._ensure_model_props().datasets = list(stale)
    hydrated = SemanticModel._new_from_graph(model.urn, dict(model._aspects))
    before = list(hydrated._ensure_model_props().datasets)

    hydrated.add_dataset(
        _ds("CUSTOMERS", "analytics.orders_model.customers_ds", ["customer_id"])
    )
    hydrated.as_mcps()

    assert hydrated._ensure_model_props().datasets == before


def test_hydrated_model_clears_deprecated_datasets_on_emit() -> None:
    """get → edit → upsert must not re-emit stale semanticModelInfo.datasets."""
    stale = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders_model.orders_ds,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders_model.customers_ds,PROD)",
    ]
    model = SemanticModel(platform="snowflake", path="analytics", id="orders_model")
    model._ensure_model_props().datasets = list(stale)
    hydrated = SemanticModel._new_from_graph(model.urn, dict(model._aspects))
    assert hydrated._ensure_model_props().datasets == stale

    hydrated.set_description("updated")
    info = next(
        mcp.aspect
        for mcp in hydrated.as_mcps()
        if mcp.aspectName == "semanticModelInfo"
    )
    assert isinstance(info, SemanticModelInfoClass)
    assert info.datasets == []
    assert hydrated._ensure_model_props().datasets == stale
