#!/usr/bin/env python3
"""
Register an AI agent — and its skill, tool, model, and data lineage — with the
DataHub Agent Registry using the Python SDK.

This is the "developer path": no UI, just a few SDK calls. Point it at any
DataHub instance via DATAHUB_GMS_URL (and DATAHUB_GMS_TOKEN if auth is on):

    export DATAHUB_GMS_URL=http://localhost:8080
    python register_support_agent.py

It registers a small but complete graph:

    Support Agent ── adopts ──▶ Order Operations (skill)
                  ── calls  ──▶ Get Order Status (tool, typed signature)
                  ── runs on ─▶ Claude Haiku (model)
                  ── reads  ──▶ snowflake support.tickets (dataset lineage)

Everything is URN-idempotent, so re-running it just updates in place.
"""

from __future__ import annotations

from datahub.api.entities.agent.agent import Agent, AgentOwner
from datahub.api.entities.agent.agent_skill import AgentSkill, SkillSourceRepository
from datahub.api.entities.agent.api import Api, ApiParam
from datahub.ingestion.graph.client import get_default_graph

# Existing catalog entities the agent depends on — reused so the model node and
# dataset lineage render with real neighbours.
CLAUDE_HAIKU = "urn:li:mlModel:(urn:li:dataPlatform:bedrock,claude-haiku,PROD)"
SUPPORT_TICKETS = "urn:li:dataset:(urn:li:dataPlatform:snowflake,support.tickets,PROD)"


def main() -> None:
    with get_default_graph() as emitter:
        # 1. A tool — a low-level capability the agent can invoke. Its typed
        #    signature reuses DataHub's SchemaField, so inputs/outputs show up
        #    exactly like a dataset's schema.
        tool = Api(
            id="get-order-status",
            name="Get Order Status",
            subtypes=["FUNCTION"],
            description="Look up an order's current status and last update by id.",
            parameters=[
                ApiParam(
                    name="order_id",
                    data_type="string",
                    required=True,
                    description="The order identifier to look up.",
                ),
            ],
            returns=[
                ApiParam(
                    name="status",
                    data_type="string",
                    description="Current fulfilment status.",
                ),
                ApiParam(
                    name="updated_at",
                    data_type="datetime",
                    description="When the status last changed.",
                ),
            ],
        )
        tool.emit(emitter)
        print(
            f"  ✓ api        {tool.name}  "
            f"(typed: order_id:string → status:string, updated_at:datetime)"
        )

        # 2. A skill — a reusable capability, backed by git, that requires the
        #    tool above. Skills are what other agents discover and adopt.
        skill = AgentSkill(
            id="order-ops",
            name="Order Operations",
            description="Look up, track, and explain the status of customer orders.",
            instructions="Use Get Order Status to answer order questions; never guess.",
            source_repository=SkillSourceRepository(
                url="https://github.com/acme/agent-skills",
                path="order-ops/SKILL.md",
            ),
            required_tools=[tool.urn],
        )
        skill.emit(emitter)
        print(f"  ✓ agentSkill  {skill.name}  (requires: {tool.name})")

        # 3. The agent — a first-class, governed entity. It adopts the skill,
        #    calls the tool, runs on a model, and reads a dataset (modeled as
        #    upstream lineage, so it lands in the same impact-analysis graph as
        #    the rest of your data).
        agent = Agent(
            id="support-agent",
            name="Support Agent",
            source_type="EXTERNAL",
            description=(
                "Answers customer questions about their orders. Ask it about "
                "order status, delays, or delivery estimates."
            ),
            skills=[skill.urn],
            tools=[tool.urn],
            models=[CLAUDE_HAIKU],
            consumes_datasets=[SUPPORT_TICKETS],
            owners=[AgentOwner(id="urn:li:corpGroup:support-eng-team")],
        )
        agent.emit(emitter)
        print(
            f"  ✓ aiAgent     {agent.name}  "
            f"→ [{skill.name}, {tool.name}, Claude Haiku]  "
            f"+ lineage: snowflake support.tickets"
        )

        print()
        print(f"Registered. Open → {emitter.config.server}/aiAgent/{agent.urn}")


if __name__ == "__main__":
    main()
