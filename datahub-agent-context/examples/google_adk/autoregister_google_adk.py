#!/usr/bin/env python3
"""
Example: auto-registering a Google ADK agent into the DataHub Agent Registry.

Two patterns are shown:
  1. DataHubBeforeModelCallback — attaches to the agent as a before_model_callback
     and fires registration on the first model invocation.
  2. register_google_adk_agent — one-shot eager registration right after the
     agent is constructed, without running it.

Prerequisites:
    pip install google-adk datahub-agent-context
    DATAHUB_GMS_URL / DATAHUB_GMS_TOKEN set in the environment (or ~/.datahubenv)
    GOOGLE_API_KEY set for Gemini (only needed to actually run the agent)
"""

from __future__ import annotations

import os

from google.adk.agents import Agent

from datahub_agent_context._registration_core import AgentRegistrar
from datahub_agent_context.google_adk_registration import (
    DataHubBeforeModelCallback,
    datahub_tool,
    register_google_adk_agent,
)

DATASET_ORDERS = "urn:li:dataset:(urn:li:dataPlatform:bigquery,myproject.orders,PROD)"
DATASET_CUSTOMERS = (
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,myproject.customers,PROD)"
)


@datahub_tool(datasets=[DATASET_ORDERS, DATASET_CUSTOMERS])
def get_order_summary(customer_id: str) -> str:
    """Return a summary of orders for a given customer_id."""
    return f"3 orders for customer {customer_id}"


@datahub_tool(
    datasets=[DATASET_CUSTOMERS],
    external_url="https://wiki.example.com/tools/lookup-customer",
)
def lookup_customer(customer_id: str) -> str:
    """Look up a customer record by ID."""
    return f"Customer {customer_id}: Alice"


# ---------------------------------------------------------------------------
# Pattern 1: DataHubBeforeModelCallback
# ---------------------------------------------------------------------------
#
# The registrar is created once and shared with the callback.  Registration
# fires the first time Google ADK calls the callback before a model request.
# The agent itself never knows DataHub exists.

registrar = AgentRegistrar(
    framework="Google ADK",
    framework_version=None,  # auto-detected at runtime if google-adk is installed
    platform="google-adk",
    agent_id="order-assistant",
    agent_name="Order Assistant",
    description="Helps customers look up orders and account details.",
)

datahub_callback = DataHubBeforeModelCallback(
    registrar,
    # Pre-load tools so the first callback fires immediately.
    # Alternatively, omit this and they are read from callback_context.agent.tools.
    tools=[get_order_summary, lookup_customer],
)

agent_with_callback = Agent(
    model=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"),
    name="order_assistant",
    description="Helps customers look up orders and account details.",
    instruction="You help users query their order history and customer records.",
    tools=[get_order_summary, lookup_customer],
    before_model_callback=datahub_callback,
)

# ---------------------------------------------------------------------------
# Pattern 2: register_google_adk_agent (eager, one-shot)
# ---------------------------------------------------------------------------
#
# No callback needed.  Call register_google_adk_agent right after building
# the agent.  Useful for batch/offline registration or when you don't control
# the agent's execution loop.

agent_simple = Agent(
    model=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"),
    name="simple_lookup_agent",
    description="A simple dataset lookup agent.",
    instruction="Answer questions about datasets.",
    tools=[lookup_customer],
)

register_google_adk_agent(
    agent_simple,
    agent_id="simple-lookup-agent",
    agent_name="Simple Lookup Agent",
    description="A simple dataset lookup agent.",
)

if __name__ == "__main__":
    print("Agent graph emitted to DataHub (if DATAHUB_GMS_URL is configured).")
    print(f"  Agent 1 urn: urn:li:aiAgent:order-assistant")
    print(f"  Agent 2 urn: urn:li:aiAgent:simple-lookup-agent")
