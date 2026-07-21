"""End-to-end demo: register an AI agent and its dependencies in DataHub.

This mirrors the "Call createAgent via GraphQL or DataHub SDK" flow:

1. Define two structured properties (framework, version) so the agent can be
   tagged with its runtime framework.
2. Register the APIs the agent invokes (Api entities).
3. Register the high-level skill the agent adopts (AgentSkill entity), sourced
   from a git repo and requiring those tools.
4. Register the agent itself (AIAgent entity): framework + version as
   structured properties, an owning corpGroup, a domain, the adopted skill,
   the invoked tools, and the datasets it consumes as upstream lineage.

The script reads connection details from the environment (DATAHUB_GMS_URL /
DATAHUB_GMS_TOKEN via ``get_default_graph``) and is idempotent — re-running it
upserts the same URNs.

Usage::

    export DATAHUB_GMS_URL=http://localhost:8080
    python fx_risk_scoring_agent.py
"""

from __future__ import annotations

import logging
from typing import List

from datahub.api.entities.agent.agent import Agent, AgentOwner
from datahub.api.entities.agent.agent_skill import AgentSkill, SkillSourceRepository
from datahub.api.entities.agent.api import (
    API_SUBTYPE_FUNCTION,
    API_SUBTYPE_MCP_TOOL,
    API_SUBTYPE_REST_ENDPOINT,
    Api,
    ApiParam,
)
from datahub.emitter.mce_builder import make_group_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.metadata.schema_classes import (
    AIAgentSourceTypeClass,
    OwnershipTypeClass,
    StructuredPropertyDefinitionClass,
)
from datahub.metadata.urns import StructuredPropertyUrn

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Structured property qualified names. These double as the bare id passed to
# StructuredPropertyUrn (the urn is urn:li:structuredProperty:<qualifiedName>).
FRAMEWORK_PROPERTY = "io.acryl.agent.framework"
VERSION_PROPERTY = "io.acryl.agent.version"

AI_AGENT_ENTITY_TYPE = "urn:li:entityType:datahub.aiAgent"


def define_structured_properties(graph: DataHubGraph) -> None:
    """Create the framework + version structured property definitions.

    Assigning a structured property requires the definition to exist first, so
    we upsert both definitions before registering the agent.
    """
    definitions: List[StructuredPropertyDefinitionClass] = [
        StructuredPropertyDefinitionClass(
            qualifiedName=FRAMEWORK_PROPERTY,
            displayName="Agent Framework",
            valueType="urn:li:dataType:datahub.string",
            cardinality="SINGLE",
            entityTypes=[AI_AGENT_ENTITY_TYPE],
            description="The agent framework this agent is built on (e.g. LangChain).",
            immutable=False,
        ),
        StructuredPropertyDefinitionClass(
            qualifiedName=VERSION_PROPERTY,
            displayName="Framework Version",
            valueType="urn:li:dataType:datahub.string",
            cardinality="SINGLE",
            entityTypes=[AI_AGENT_ENTITY_TYPE],
            description="The version of the agent framework in use.",
            immutable=False,
        ),
    ]
    for definition in definitions:
        graph.emit(
            MetadataChangeProposalWrapper(
                entityUrn=str(StructuredPropertyUrn(definition.qualifiedName)),
                aspect=definition,
            )
        )
        logger.info("Defined structured property %s", definition.qualifiedName)


def main() -> None:
    graph = get_default_graph()

    # 1. Structured property definitions must exist before assignment.
    define_structured_properties(graph)

    # 2. Tools the agent invokes.
    tools = [
        Api(
            id="order-lookup-mcp",
            name="Order Lookup MCP",
            subtypes=[API_SUBTYPE_MCP_TOOL],
            description="Looks up order details by id from the order entry system.",
            external_url="https://tools.example.com/order-lookup-mcp",
            parameters=[
                ApiParam(
                    name="order_id",
                    data_type="string",
                    required=True,
                    description="The order identifier to look up.",
                ),
                ApiParam(
                    name="include_line_items",
                    data_type="boolean",
                    required=False,
                    description="Whether to include line items in the response.",
                ),
            ],
            returns=[
                ApiParam(
                    name="order_id",
                    data_type="string",
                    required=True,
                    description="The looked-up order identifier.",
                ),
                ApiParam(
                    name="status",
                    data_type="string",
                    required=True,
                    description="Current order status (e.g. FILLED, PENDING).",
                ),
                ApiParam(
                    name="notional_amount",
                    data_type="number",
                    required=True,
                    description="Order notional in the base currency.",
                ),
                ApiParam(
                    name="currency_pair",
                    data_type="string",
                    required=True,
                    description="The FX currency pair, e.g. EUR/USD.",
                ),
            ],
        ),
        Api(
            id="fx-rate-api",
            name="FX Rate REST API",
            subtypes=[API_SUBTYPE_REST_ENDPOINT],
            description="Returns current and historical FX rates for a currency pair.",
            external_url="https://tools.example.com/fx-rate-api",
        ),
        Api(
            id="exposure-calculator",
            name="Exposure Calculator",
            subtypes=[API_SUBTYPE_FUNCTION],
            description="Computes net FX exposure from a set of positions.",
        ),
    ]
    tool_urns = [tool.emit(graph) for tool in tools]
    for tool_urn in tool_urns:
        logger.info("Registered tool %s", tool_urn)

    # 3. The skill the agent adopts, sourced from a git repo and requiring the
    #    tools above.
    skill = AgentSkill(
        id="fx-risk-scoring-skill",
        name="FX Risk Scoring",
        description="Scores the FX risk of an order using market data and exposure.",
        instructions=(
            "Given an order, fetch the relevant FX rates and positions, compute "
            "net exposure, and return a risk score with a short rationale."
        ),
        source_repository=SkillSourceRepository(
            url="https://github.com/example-org/agent-skills",
            path="fx-risk-scoring/SKILL.md",
        ),
        required_tools=tool_urns,
    )
    skill_urn = skill.emit(graph)
    logger.info("Registered skill %s", skill_urn)

    # 4. The agent itself.
    consumed_datasets = [
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,fx.market_data,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,fx.positions,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,fx.orders,PROD)",
    ]
    agent = Agent(
        id="fx-risk-scoring-agent",
        name="FX Risk Scoring Agent",
        description=(
            "Scores the FX risk of incoming orders. Use when a question is about "
            "FX exposure, order risk, or currency-pair risk scoring."
        ),
        source_type=AIAgentSourceTypeClass.EXTERNAL,
        structured_properties={
            FRAMEWORK_PROPERTY: "LangChain",
            VERSION_PROPERTY: "v0.2",
        },
        owners=[
            AgentOwner(
                id=make_group_urn("quant-risk-team"),
                type=OwnershipTypeClass.TECHNICAL_OWNER,
            )
        ],
        domain="FX Risk",
        skills=[skill_urn],
        tools=tool_urns,
        # The LLM the agent runs on (a seeded Bedrock MLModel, so it resolves a
        # name in the Dependencies tab). Sonnet fits a risk-scoring workload.
        models=["urn:li:mlModel:(urn:li:dataPlatform:bedrock,claude-sonnet,PROD)"],
        consumes_datasets=consumed_datasets,
    )
    agent_urn = agent.emit(graph)
    logger.info("Registered agent %s", agent_urn)

    print("Created entities:")
    print(f"  agent:  {agent_urn}")
    print(f"  skill:  {skill_urn}")
    for tool_urn in tool_urns:
        print(f"  tool:   {tool_urn}")


if __name__ == "__main__":
    main()
