#!/usr/bin/env python3
"""Auto-register a LangChain agent into DataHub's Agent Registry.

Two patterns are shown:

1. Callback handler — attach to an existing AgentExecutor so registration
   happens automatically when the chain finishes.
2. register_langchain_agent — call once after building the executor if you
   prefer an explicit, side-effect-free snapshot.

Prerequisites:
    pip install langchain langchain-openai "datahub-agent-context[langchain]"

Environment variables:
    DATAHUB_GMS_URL    — e.g. http://localhost:8080
    DATAHUB_GMS_TOKEN  — access token (omit if auth is disabled)
    OPENAI_API_KEY     — only needed for the live-run section at the bottom
"""

from __future__ import annotations

from typing import Any, List

from datahub_agent_context._registration_core import AgentRegistrar
from datahub_agent_context.langchain_registration import (
    DataHubCallbackHandler,
    datahub_tool,
    register_langchain_agent,
)

# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

# Plain tool — no DataHub annotations; only name + description are captured.


def search_datasets(query: str) -> str:
    """Full-text search across all datasets in the data catalog."""
    return f"Search results for: {query}"


# Annotated tool — datasets and skill surface in the DataHub graph.
@datahub_tool(
    datasets=["urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.orders,PROD)"],
    skill="data-discovery",
)
def get_order_stats(start_date: str, end_date: str) -> str:
    """Return aggregate order statistics for the given date range."""
    return f"Orders from {start_date} to {end_date}: 42 000"


# ---------------------------------------------------------------------------
# Pattern 1 — DataHubCallbackHandler
# ---------------------------------------------------------------------------


def build_with_callback_handler() -> None:
    """Register automatically when the chain finishes its first run."""
    # Real usage: replace with build_langchain_tools(client) or your own tools.
    # Here we use lightweight stubs so the example runs without a live LangChain.
    try:
        from langchain_openai import ChatOpenAI
    except ImportError:
        print("[pattern-1] langchain / langchain-openai not installed — skipping live demo")
        return

    _llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

    registrar = AgentRegistrar(
        framework="LangChain",
        framework_version=None,
        platform="langchain",
        agent_id="orders-assistant",
        agent_name="Orders Assistant",
        description="Answers questions about order data.",
    )

    handler = DataHubCallbackHandler(
        registrar,
        tools=[search_datasets, get_order_stats],
    )

    # Attach the handler to your executor.  Registration fires on chain_end.
    print("[pattern-1] Handler attached — registration will fire on first chain_end.")
    _ = handler  # attach to AgentExecutor.callbacks in real usage


# ---------------------------------------------------------------------------
# Pattern 2 — register_langchain_agent (one-shot snapshot)
# ---------------------------------------------------------------------------


def build_with_register_function() -> None:
    """Snapshot the agent structure once, without modifying the executor."""
    try:
        from langchain_openai import ChatOpenAI  # noqa: F401
    except ImportError:
        print("[pattern-2] langchain / langchain-openai not installed — skipping live demo")
        return

    # Construct a minimal stub executor so the example is self-contained.
    class _StubExecutor:
        tools: List[Any] = [search_datasets, get_order_stats]

        class agent:
            class llm:
                model_name = "gpt-4o-mini"

    executor = _StubExecutor()

    register_langchain_agent(
        executor,
        agent_id="orders-assistant-v2",
        agent_name="Orders Assistant v2",
        description="Snapshot-registered agent using register_langchain_agent.",
    )

    print("[pattern-2] Agent registered in DataHub.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    build_with_callback_handler()
    build_with_register_function()
