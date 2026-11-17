#!/usr/bin/env python3
"""Seed demo data products for marketplace UI development.

Run from repo root (DataHub must be running):

    eval $(scripts/dev/datahub-dev.sh shell-env)
    ./gradlew :smoke-test:installDev -q
    smoke-test/venv/bin/python scripts/dev/seed_marketplace_demo.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "smoke-test"))

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
)
from conftest import build_auth_session, build_graph_client
from tests.consistency_utils import wait_for_writes_to_sync

logger = logging.getLogger(__name__)

DEMO_PREFIX = "marketplace_demo"

CREATE_DATA_PRODUCT = """
mutation createDataProduct($input: CreateDataProductInput!) {
  createDataProduct(input: $input) {
    urn
  }
}
"""

BATCH_SET_DATA_PRODUCT = """
mutation batchSetDataProduct($input: BatchSetDataProductInput!) {
  batchSetDataProduct(input: $input)
}
"""

SCROLL_DOMAINS = """
query scrollDomains($input: ScrollAcrossEntitiesInput!) {
  scrollAcrossEntities(input: $input) {
    searchResults {
      entity {
        urn
        type
        ... on Domain {
          properties { name }
        }
      }
    }
  }
}
"""


def resolve_domain_urn(graph, preferred_names: list[str]) -> str | None:
    result = graph.execute_graphql(
        SCROLL_DOMAINS,
        {
            "input": {
                "query": "*",
                "types": ["DOMAIN"],
                "count": 200,
            }
        },
    )
    domains = [
        r["entity"]
        for r in result.get("scrollAcrossEntities", {}).get("searchResults", [])
        if r.get("entity", {}).get("type") == "DOMAIN"
    ]
    name_to_urn = {
        (d.get("properties") or {}).get("name", "").lower(): d["urn"] for d in domains
    }
    for name in preferred_names:
        urn = name_to_urn.get(name.lower())
        if urn:
            return urn
    return domains[0]["urn"] if domains else None


def emit_dataset(graph, platform: str, name: str, description: str) -> str:
    urn = make_dataset_urn(platform, name, "PROD")
    graph.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DatasetPropertiesClass(name=name.split(".")[-1], description=description),
        )
    )
    return urn


def create_data_product(
    graph,
    *,
    domain_urn: str,
    name: str,
    description: str,
    parent_urn: str | None = None,
) -> str:
    properties: dict = {"name": name, "description": description}
    if parent_urn:
        properties["parentDataProduct"] = parent_urn

    result = graph.execute_graphql(
        CREATE_DATA_PRODUCT,
        {"input": {"domainUrn": domain_urn, "properties": properties}},
    )
    urn = result["createDataProduct"]["urn"]
    logger.info("Created data product %s (%s)", name, urn)
    return urn


def attach_assets(graph, data_product_urn: str, asset_urns: list[str]) -> None:
    if not asset_urns:
        return
    graph.execute_graphql(
        BATCH_SET_DATA_PRODUCT,
        {"input": {"dataProductUrn": data_product_urn, "resourceUrns": asset_urns}},
    )


def attach_tags(graph, data_product_urn: str, tag_urns: list[str]) -> None:
    if not tag_urns:
        return
    graph.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=data_product_urn,
            aspect=GlobalTagsClass(
                tags=[TagAssociationClass(tag=tag_urn) for tag_urn in tag_urns]
            ),
        )
    )


def attach_owner(graph, data_product_urn: str, owner_urn: str) -> None:
    graph.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=data_product_urn,
            aspect=OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=owner_urn,
                        type=OwnershipTypeClass.BUSINESS_OWNER,
                    )
                ]
            ),
        )
    )


def seed() -> None:
    auth = build_auth_session()
    graph = build_graph_client(auth)

    marketing_urn = resolve_domain_urn(graph, ["Marketing", "E-Commerce"])
    engineering_urn = resolve_domain_urn(graph, ["Engineering", "Engineering Division"])
    if not marketing_urn:
        raise RuntimeError("Could not find a Marketing/E-Commerce domain.")
    if not engineering_urn:
        engineering_urn = marketing_urn

    me = graph.execute_graphql("query { me { corpUser { urn } } }", {})
    owner_urn = me["me"]["corpUser"]["urn"]

    tag_production = graph.create_tag(f"{DEMO_PREFIX}_production")
    tag_analytics = graph.create_tag(f"{DEMO_PREFIX}_analytics")

    datasets = {
        "customers": emit_dataset(
            graph,
            "snowflake",
            f"{DEMO_PREFIX}.marketing.customers",
            "Customer profile dimension table",
        ),
        "campaigns": emit_dataset(
            graph,
            "snowflake",
            f"{DEMO_PREFIX}.marketing.campaigns",
            "Marketing campaign performance facts",
        ),
        "orders": emit_dataset(
            graph,
            "snowflake",
            f"{DEMO_PREFIX}.commerce.orders",
            "Order header records",
        ),
        "events": emit_dataset(
            graph,
            "snowflake",
            f"{DEMO_PREFIX}.commerce.order_events",
            "Streaming order event log",
        ),
        "pipelines": emit_dataset(
            graph,
            "snowflake",
            f"{DEMO_PREFIX}.engineering.pipelines",
            "Data pipeline run metadata",
        ),
    }

    customer_360 = create_data_product(
        graph,
        domain_urn=marketing_urn,
        name="Customer 360",
        description="Unified customer profiles, segments, and engagement history for go-to-market teams.",
    )
    attach_assets(graph, customer_360, [datasets["customers"], datasets["campaigns"]])
    attach_tags(graph, customer_360, [tag_production, tag_analytics])
    attach_owner(graph, customer_360, owner_urn)

    campaign_insights = create_data_product(
        graph,
        domain_urn=marketing_urn,
        name="Campaign Insights",
        description="Nested product for campaign-level KPIs and attribution reporting.",
        parent_urn=customer_360,
    )
    attach_assets(graph, campaign_insights, [datasets["campaigns"]])
    attach_tags(graph, campaign_insights, [tag_analytics])

    order_hub = create_data_product(
        graph,
        domain_urn=marketing_urn,
        name="Order Hub",
        description="Commerce order data product spanning checkout, fulfillment, and returns.",
    )
    attach_assets(graph, order_hub, [datasets["orders"], datasets["events"]])
    attach_tags(graph, order_hub, [tag_production])
    attach_owner(graph, order_hub, owner_urn)

    order_events = create_data_product(
        graph,
        domain_urn=marketing_urn,
        name="Order Events Stream",
        description="Real-time order events consumed by analytics and ops dashboards.",
        parent_urn=order_hub,
    )
    attach_assets(graph, order_events, [datasets["events"]])

    pipeline_ops = create_data_product(
        graph,
        domain_urn=engineering_urn,
        name="Pipeline Operations",
        description="Operational visibility into ingestion pipelines, SLAs, and failure modes.",
    )
    attach_assets(graph, pipeline_ops, [datasets["pipelines"]])
    attach_tags(graph, pipeline_ops, [tag_production, tag_analytics])
    attach_owner(graph, pipeline_ops, owner_urn)

    wait_for_writes_to_sync()
    auth.destroy()

    print("\nMarketplace demo seed complete:")
    print(f"  - Customer 360 (+ nested Campaign Insights) on {marketing_urn}")
    print(f"  - Order Hub (+ nested Order Events Stream) on {marketing_urn}")
    print(f"  - Pipeline Operations on {engineering_urn}")
    print("Refresh the Marketplace page to browse the new data products.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    seed()
