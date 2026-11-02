"""Register applications with composability edges and service health via incidents.

Demonstrates three Application relationship types:

- ``checkout-cart-app`` **part of** ``checkout-app`` (ApplicationPartOf, set on the
  child via ``parentApplication``)
- ``checkout-app`` **consumes** the ``placeOrder`` api (Consumes, applicationLineage input edge)
- ``checkout-app`` **produces** ``commerce.orders_fct`` (Produces, applicationLineage output edge)

Also shows:

- A ``Service`` entity (subtype SERVICE, platform kubernetes) whose operational
  health is tracked via the incidents subsystem rather than a bespoke health
  aspect — the Incidents tab on the service profile surfaces it.
- A read-merge pattern on ``AIAgentDependencies`` to append a tool to an agent
  without clobbering existing dependencies.

Run AFTER register_rest_services.py (needs the placeOrder api to exist).

Usage::

    export DATAHUB_GMS_URL=http://localhost:8080
    python register_app_and_health.py
"""

from __future__ import annotations

import logging

from datahub.emitter.mce_builder import (
    get_sys_time,
    make_data_platform_urn,
    make_dataset_urn,
    make_group_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.metadata.schema_classes import (
    AIAgentDependenciesClass,
    ApplicationLineageClass,
    ApplicationPropertiesClass,
    AuditStampClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    DomainsClass,
    EdgeClass,
    IncidentInfoClass,
    IncidentSourceClass,
    IncidentSourceTypeClass,
    IncidentStageClass,
    IncidentStateClass,
    IncidentStatusClass,
    IncidentTypeClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    ServiceLifecycleClass,
    ServicePropertiesClass,
    StatusClass,
    SubTypesClass,
)
from datahub.metadata.urns import IncidentUrn

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CHECKOUT_APP_URN = "urn:li:application:checkout-app"
CHECKOUT_CART_APP_URN = "urn:li:application:checkout-cart-app"
AUCTION_SVC_URN = "urn:li:service:auction-svc"
PLACE_ORDER_API_URN = "urn:li:api:order-entry-api.post.orders"
FX_AGENT_URN = "urn:li:aiAgent:fx-risk-scoring-agent"
ORDERS_FCT_URN = make_dataset_urn("snowflake", "commerce.orders_fct", "PROD")

COMMERCE_DOMAIN = "urn:li:domain:commerce"
OWNER_GROUP = make_group_urn("commerce-platform-team")


def _ownership() -> OwnershipClass:
    return OwnershipClass(
        owners=[OwnerClass(owner=OWNER_GROUP, type=OwnershipTypeClass.TECHNICAL_OWNER)]
    )


def register_auction_service(graph: DataHubGraph) -> None:
    """A SERVICE-subtype service on kubernetes, DEGRADED, to show a 2nd health state."""
    existing = graph.get_aspect(AUCTION_SVC_URN, ServicePropertiesClass)
    source_repository = existing.sourceRepository if existing else None
    aspects = [
        ServicePropertiesClass(
            displayName="Auction Service",
            description="Runs live order auctions and matching.",
            lifecycle=ServiceLifecycleClass.PRODUCTION,
            sourceRepository=source_repository,
        ),
        SubTypesClass(typeNames=["SERVICE"]),
        DataPlatformInstanceClass(platform=make_data_platform_urn("kubernetes")),
        _ownership(),
        StatusClass(removed=False),
    ]
    for aspect in aspects:
        graph.emit_mcp(
            MetadataChangeProposalWrapper(entityUrn=AUCTION_SVC_URN, aspect=aspect)
        )
    logger.info("Registered Service %s", AUCTION_SVC_URN)


def raise_service_incident(graph: DataHubGraph) -> None:
    """Raise an active OPERATIONAL incident on the auction service.

    Service health is derived from the shared incidents subsystem (not a bespoke
    health aspect): an active, high-priority incident makes the service's health
    badge render as unhealthy and populates its Incidents tab — the same
    mechanism datasets/dashboards already use.
    """
    # Stable id so re-running the seed updates the same incident instead of
    # piling up duplicates.
    incident_urn = str(IncidentUrn("auction-svc-partial-outage"))
    # get_sys_time() already returns epoch millis (what AuditStamp.time expects).
    audit_stamp = AuditStampClass(
        time=get_sys_time(),
        actor=make_user_urn("datahub"),
    )
    graph.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=incident_urn,
            aspect=IncidentInfoClass(
                type=IncidentTypeClass.OPERATIONAL,
                title="Auction matching latency breaching SLO",
                description=(
                    "p99 match latency is above the 250ms SLO and order auctions "
                    "are backing up. Elevated error rate on the matching workers."
                ),
                entities=[AUCTION_SVC_URN],
                status=IncidentStatusClass(
                    state=IncidentStateClass.ACTIVE,
                    stage=IncidentStageClass.INVESTIGATION,
                    message="On-call is investigating the matching worker pool.",
                    lastUpdated=audit_stamp,
                ),
                priority=1,  # HIGH (0=CRITICAL, 1=HIGH, 2=MEDIUM, 3=LOW)
                source=IncidentSourceClass(type=IncidentSourceTypeClass.MANUAL),
                created=audit_stamp,
            ),
        )
    )
    logger.info("Raised active incident %s on %s", incident_urn, AUCTION_SVC_URN)


def register_orders_dataset(graph: DataHubGraph) -> None:
    graph.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=ORDERS_FCT_URN,
            aspect=DatasetPropertiesClass(
                name="orders_fct",
                description="Fact table of placed orders, produced by the checkout app.",
            ),
        )
    )
    graph.emit_mcp(
        MetadataChangeProposalWrapper(entityUrn=ORDERS_FCT_URN, aspect=_ownership())
    )
    logger.info("Registered dataset %s", ORDERS_FCT_URN)


def register_sub_application(graph: DataHubGraph) -> None:
    aspects = [
        ApplicationPropertiesClass(
            name="Checkout Cart App",
            description="Manages the shopping cart within checkout.",
            # app-of-apps: the child points at its parent (ApplicationPartOf).
            parentApplication=CHECKOUT_APP_URN,
        ),
        _ownership(),
        DomainsClass(domains=[COMMERCE_DOMAIN]),
        StatusClass(removed=False),
    ]
    for aspect in aspects:
        graph.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=CHECKOUT_CART_APP_URN, aspect=aspect
            )
        )
    logger.info("Registered sub-Application %s", CHECKOUT_CART_APP_URN)


def register_checkout_application(graph: DataHubGraph) -> None:
    aspects = [
        ApplicationPropertiesClass(
            name="Checkout App",
            description="Customer-facing checkout: places orders, produces the orders fact table.",
        ),
        # Consumes/produces edges live on the applicationLineage aspect (Edge-based,
        # same pattern as DataJobInputOutput), not on applicationProperties.
        ApplicationLineageClass(
            inputEdges=[EdgeClass(destinationUrn=PLACE_ORDER_API_URN)],
            outputEdges=[EdgeClass(destinationUrn=ORDERS_FCT_URN)],
        ),
        _ownership(),
        DomainsClass(domains=[COMMERCE_DOMAIN]),
        StatusClass(removed=False),
    ]
    for aspect in aspects:
        graph.emit_mcp(
            MetadataChangeProposalWrapper(entityUrn=CHECKOUT_APP_URN, aspect=aspect)
        )
    logger.info(
        "Registered Application %s (composed of %s, consumes %s, produces %s)",
        CHECKOUT_APP_URN,
        CHECKOUT_CART_APP_URN,
        PLACE_ORDER_API_URN,
        ORDERS_FCT_URN,
    )


def wire_fx_agent_to_place_order(graph: DataHubGraph) -> None:
    """Append placeOrder to the FX agent's tools so it shows as an api consumer."""
    deps = graph.get_aspect(FX_AGENT_URN, AIAgentDependenciesClass)
    if deps is None:
        logger.warning(
            "FX agent %s not found -- run fx_risk_scoring_agent.py first to see it "
            "as a consumer of %s.",
            FX_AGENT_URN,
            PLACE_ORDER_API_URN,
        )
        return
    tools = list(deps.tools or [])
    if PLACE_ORDER_API_URN not in tools:
        tools.append(PLACE_ORDER_API_URN)
    deps.tools = tools
    graph.emit_mcp(MetadataChangeProposalWrapper(entityUrn=FX_AGENT_URN, aspect=deps))
    logger.info("Wired AgentUsesTool: %s -> %s", FX_AGENT_URN, PLACE_ORDER_API_URN)


def main() -> None:
    graph = get_default_graph()
    register_auction_service(graph)
    raise_service_incident(graph)
    register_orders_dataset(graph)
    register_sub_application(graph)
    register_checkout_application(graph)
    wire_fx_agent_to_place_order(graph)


if __name__ == "__main__":
    main()
