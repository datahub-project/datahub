"""Unit tests for the SemanticModel + SemanticModelDataset SDK.

These tests assert the producer contract for the ``semanticModel`` entity and
its logical ``dataset`` entities: URN patterns, aspect shapes, the
``Semantic Model Dataset`` subtype, the ``semanticModelProperties`` back-ref,
the schemaField-anchored ``semanticFieldAnnotation`` + ``aiContext`` MCPs,
the required-expression fallback, and aiContext-only-when-non-empty.
"""

from datetime import datetime, timezone
from typing import Any

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
    assert info.datasets == []
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
        semantic_model="urn:li:placeholder",
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


def test_semantic_model_add_dataset_accepts_urn_string() -> None:
    model = SemanticModel(platform="snowflake", path="analytics", id="orders_model")
    model.add_dataset(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders_model.orders_ds,PROD)"
    )
    assert model.datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders_model.orders_ds,PROD)"
    ]


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
    the lineage wiring (Metric -> SemanticModel -> Logical Dataset -> Physical).
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

    total_revenue = Metric(
        platform="snowflake",
        path="analytics",
        id="total_revenue",
        semantic_model=str(model.urn),
        name="Total Revenue",
        expression=DialectExpressionInput(
            expression="SUM(ORDERS.amount)", dialect=DialectClass.SNOWFLAKE
        ),
        ai_context=AiContextInput(synonyms=["revenue"]),
    )
    double_revenue = Metric(
        platform="snowflake",
        path="analytics",
        id="double_revenue",
        semantic_model=str(model.urn),
        name="Double Revenue",
        expression="2 * total_revenue",
        derived_from=[total_revenue.urn],
    )

    # --- Semantic model aspects ---
    model_aspects = _aspects_by_name(model)
    assert isinstance(model_aspects["status"], StatusClass)
    info = model_aspects["semanticModelInfo"]
    assert info.datasets == [str(orders_ds.urn), str(customers_ds.urn)]
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
        # A dataset never carries metricUpstreams; assert the logical dataset's
        # actual back-ref is present instead.
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
    # No metricUpstreams for semantic-model-backed metrics.
    assert "metricUpstreams" not in tr_aspects

    dr_aspects = _aspects_by_name(double_revenue)
    assert dr_aspects["metricInfo"].expression.dialects[0].dialect == (
        DialectClass.ANSI_SQL
    )
    assert [
        d.destinationUrn for d in dr_aspects["metricRelationships"].derivedFrom
    ] == [str(total_revenue.urn)]
    # No aiContext when none provided.
    assert "aiContext" not in dr_aspects

    # --- Lineage chain wiring ---
    # Metric -> SemanticModel (ModeledBy) via metricInfo.semanticModel.
    assert total_revenue.semantic_model == str(model.urn)
    assert double_revenue.semantic_model == str(model.urn)
    # SemanticModel -> Logical Datasets (Contains) via semanticModelInfo.datasets.
    assert str(orders_ds.urn) in model.datasets
    assert str(customers_ds.urn) in model.datasets
    # Logical Dataset -> Physical (upstreamLineage).
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
                    tags=["PII"],
                )
            ],
        )
    except (InvalidUrnError, AssertionError):
        pass
    else:
        raise AssertionError("expected error for bare tag name 'PII'")


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
def test_governance_kwargs_land_in_aspects(builder) -> None:
    entity = builder()
    aspects = _aspects_by_name(entity)
    for name in _governance_aspect_names():
        assert name in aspects, f"{name} missing from {type(entity).__name__}"


def test_require_metrics_support_saas_below_min_raises() -> None:
    from unittest.mock import MagicMock

    from datahub.sdk import require_metrics_support

    graph = MagicMock()
    graph.server_config.is_datahub_cloud = True
    graph.server_config.is_version_at_least.return_value = False
    graph.server_config.service_version = "2.0.0"

    from datahub.errors import SdkUsageError

    try:
        require_metrics_support(graph)
    except SdkUsageError as e:
        assert "v2.0.0" in str(e)
        assert "2.1.0" in str(e)
    else:
        raise AssertionError("expected SdkUsageError for old SaaS server")


def test_require_metrics_support_saas_at_or_above_min_ok() -> None:
    from unittest.mock import MagicMock

    from datahub.sdk import require_metrics_support

    graph = MagicMock()
    graph.server_config.is_datahub_cloud = True
    graph.server_config.is_version_at_least.return_value = True
    graph.server_config.service_version = "2.1.0"
    require_metrics_support(graph)  # should not raise


def test_require_metrics_support_oss_is_noop() -> None:
    from unittest.mock import MagicMock

    from datahub.sdk import require_metrics_support

    graph = MagicMock()
    graph.server_config.is_datahub_cloud = False
    require_metrics_support(graph)  # should not raise, no version probe


def test_require_metrics_support_no_server_config_fail_open() -> None:
    from datahub.sdk import require_metrics_support

    class _BareGraph:
        pass

    require_metrics_support(_BareGraph())  # should not raise
