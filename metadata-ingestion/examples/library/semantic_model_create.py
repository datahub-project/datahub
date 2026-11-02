"""Emit a semantic model with two logical datasets and two metrics.

This example builds the full lineage chain
``Metric -> Logical Dataset -> Physical Dataset`` (with SemanticModel as a
container of its datasets and metrics) using the high-level ``datahub.sdk``
builders, then writes every emitted MCP to a JSON file so the resulting
aspect shapes can be inspected.

Run with::

    python -m examples.library.semantic_model_create

The output is written to ``semantic_model_create.json`` in the current
directory. Inspect it to confirm the aspect shapes match the producer contract:
URN patterns, the ``Semantic Model Dataset`` subtype, the
``semanticModelProperties`` / ``metricInfo.semanticModel`` membership pointers,
the schemaField-anchored ``semanticFieldAnnotation`` MCPs, the required-expression
fallback (``ORDERS.order_id``), aiContext-only-when-non-empty, and
``metricUpstreams.datasetUpstreams`` pointing at SMDs.
"""

import json
from typing import Any, List

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DialectClass,
    ERModelRelationshipCardinalityClass,
    SemanticFieldTypeClass,
)
from datahub.metadata.urns import SemanticModelUrn
from datahub.sdk import (
    AiContextInput,
    DialectExpressionInput,
    Metric,
    SemanticFieldInput,
    SemanticModel,
    SemanticModelDataset,
    SemanticModelRelationshipInput,
)
from datahub.sdk.entity import Entity


def build_graph() -> tuple[SemanticModel, List[Entity]]:
    platform = "snowflake"
    model_urn = SemanticModelUrn(platform=platform, path="analytics", id="orders_model")

    orders_ds = SemanticModelDataset(
        platform=platform,
        name="analytics.orders_model.orders_ds",
        semantic_model=model_urn,
        alias="ORDERS",
        schema=[
            SemanticFieldInput(
                field_path="order_id",
                type="int",
                semantic_type=SemanticFieldTypeClass.DIMENSION,
                is_part_of_key=True,
            ),
            # Foreign key the ORDERS -> CUSTOMERS relationship joins on.
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
                ai_context=AiContextInput(synonyms=["revenue"]),
            ),
        ],
        upstreams=[make_dataset_urn(platform, "raw.orders")],
    )

    customers_ds = SemanticModelDataset(
        platform=platform,
        name="analytics.orders_model.customers_ds",
        semantic_model=model_urn,
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
        upstreams=[make_dataset_urn(platform, "raw.customers")],
    )

    total_revenue = Metric(
        platform=platform,
        path="analytics",
        id="total_revenue",
        semantic_model=str(model_urn),
        name="Total Revenue",
        description="Sum of all order amounts.",
        expression=DialectExpressionInput(
            expression="SUM(ORDERS.amount)", dialect=DialectClass.SNOWFLAKE
        ),
        upstream_datasets=[orders_ds.urn],
        ai_context=AiContextInput(synonyms=["revenue"]),
    )
    double_revenue = Metric(
        platform=platform,
        path="analytics",
        id="double_revenue",
        semantic_model=str(model_urn),
        name="Double Revenue",
        expression="2 * total_revenue",
        derived_from=[total_revenue.urn],
    )

    model = SemanticModel(
        platform=platform,
        path="analytics",
        id="orders_model",
        name="Orders Model",
        description="A semantic model over the raw orders and customers tables.",
        datasets=[orders_ds, customers_ds],
        relationships=[
            SemanticModelRelationshipInput(
                from_alias="ORDERS",
                from_columns=["customer_id"],
                to_alias="CUSTOMERS",
                to_columns=["customer_id"],
                name="orders_to_customers",
                cardinality=ERModelRelationshipCardinalityClass.N_ONE,
            )
        ],
        ai_context=AiContextInput(
            synonyms=["orders model"],
            instructions="Use for revenue and customer analytics.",
        ),
    )

    return model, [orders_ds, customers_ds, total_revenue, double_revenue]


def main() -> None:
    model, entities = build_graph()

    all_mcps: list[MetadataChangeProposalWrapper] = []
    all_mcps.extend(model.as_mcps())
    for entity in entities:
        all_mcps.extend(entity.as_mcps())

    records: list[dict[str, Any]] = [dict(mcp.to_obj()) for mcp in all_mcps]
    with open("semantic_model_create.json", "w") as f:
        json.dump(records, f, indent=2, default=str)
    print(f"Wrote {len(all_mcps)} MCPs to semantic_model_create.json")

    # When emitting to a live server instead of a file, call the opt-in
    # preflight helper first to get a clear error on an unsupported server:
    #
    #   from datahub.sdk import DataHubClient, require_metrics_support
    #   client = DataHubClient(server=..., token=...)
    #   require_metrics_support(client)  # raises if the server version is too old
    #   for entity in [model, *entities]:
    #       client.entities.upsert(entity)


if __name__ == "__main__":
    main()
