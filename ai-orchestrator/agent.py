"""
Model-agnostic agent loop.

The loop is deliberately decoupled from any one provider. It only deals with an
abstract "call the model, maybe run tools, repeat" contract.

Yields SSE-friendly token strings as they stream from the model.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import AsyncIterator

import pii_tagger
from local_tools import ToolRegistry
from llm_clients import ModelTurn, TextDelta, create_llm_client
from mcp_tools import get_mcp

logger = logging.getLogger("agent")

# Sentinel emitted after the final answer when a fresh PII proposal is awaiting the
# user's confirmation. main.py turns this into a distinct {"confirm": true} SSE event
# and keeps it out of the saved transcript. Uses a control char so it can never collide
# with real model output.
CONFIRM_SENTINEL = "\x00CONFIRM\x00"

DEFAULT_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-5")
# 1024 truncated mid-table once a proposal was involved.
MAX_TOKENS = int(os.environ.get("ANTHROPIC_MAX_TOKENS", "4096"))
# search -> propose -> confirm -> apply fits well inside this. Raising it further mostly
# buys the model room to wander, and every wasted step is a round trip.
MAX_TOOL_ITERATIONS = int(os.environ.get("MAX_TOOL_ITERATIONS", "10"))

SYSTEM_PROMPT = (
    "You are the DataHub AI Assistant, embedded in the Privacy Data Catalog. "
    "You help privacy engineers and data stewards answer questions about datasets, "
    "schemas, tags, PII, and lineage. Use the provided tools to look up real data "
    "from DataHub before answering. Be concise and accurate. If the user is viewing a "
    "specific dataset (entityUrn provided in context), prefer that dataset.\n\n"
    "Base every PII statement on what the tools returned. Never infer it from a table or "
    "column name: if no tool reported a tag and you have not run a classification, you do "
    "not know. Do not call a dataset likely or probably personal — in a privacy catalog a "
    "guess reads as a finding.\n\n"
    "Answer only what was asked. Do not end a reply by offering further work or listing "
    "what else you could do. The one exception is the confirmation step below, which is "
    "required.\n\n"
    "Tagging is two steps:\n"
    "1. Call propose_pii_tags. It reads the schema itself, so do not look it up first. "
    "Show the result as a markdown table of column, tag, confidence, and reason. Note any "
    "columns it skipped as already tagged, and list uncertain rows separately as needing a "
    "judgement call.\n"
    "2. Stop and ask them to confirm. Only after they confirm, on a later turn, call "
    "apply_pii_tags — even if their first message said to tag the dataset. Show what you "
    "would write first.\n"
    "Pass rejected columns in exclude_columns. Tag one dataset per confirmation."
)


async def run_agent(
    user_message: str,
    context: dict | None,
    api_key: str,
    model: str = DEFAULT_MODEL,
    history: list[dict] | None = None,
    provider: str | None = None,
    classifier_api_key: str | None = None,
    classifier_provider: str | None = None,
    classifier_model: str = "claude-haiku-4-5",
) -> AsyncIterator[str]:
    """
    Run the agentic loop. Yields text tokens as they arrive.

    Handles the tool-use cycle:
      user msg -> model -> (tool_use?) -> run tool -> feed result -> model -> ... -> final text

    history: prior conversation turns as [{role, content}, ...].
    """
    client = create_llm_client(provider, model, api_key)

    ctx_note = ""
    if context:
        ctx_note = f"\n\n[Page context: {json.dumps(context)}]"

    # Build messages: prior history first, then new user message
    messages: list[dict] = list(history or [])
    messages.append({"role": "user", "content": user_message + ctx_note})

    # Shared MCP session — reused across requests, not spawned per call. Wrapped so the
    # model sees MCP's reads plus the two guarded PII tools, and no raw write tool.
    tools = ToolRegistry(
        await get_mcp(),
        classifier_api_key=classifier_api_key,
        classifier_provider=classifier_provider,
        classifier_model=classifier_model,
    )

    for iteration in range(1, MAX_TOOL_ITERATIONS + 1):
        assistant_blocks: list[dict] = []
        tool_uses: list[dict] = []
        started = time.perf_counter()
        first_token_ms: float | None = None

        # Rebuilt each iteration so a proposal made mid-turn is named in the prompt;
        # otherwise the model keeps reasoning from "no proposal on record".
        system = SYSTEM_PROMPT + pii_tagger.pending_prompt_note()

        final: ModelTurn | None = None
        async for event in client.stream(
            messages, tools.definitions, model, system, MAX_TOKENS
        ):
            if isinstance(event, TextDelta):
                if first_token_ms is None:
                    first_token_ms = (time.perf_counter() - started) * 1000
                yield event.text
            else:
                final = event

        if final is None:
            raise RuntimeError("LLM stream ended without a final response")

        # Per-iteration timing: this loop is the dominant cost of a turn, and without it
        # a slow turn is indistinguishable from a slow tool.
        logger.info(
            "iteration %d: %.0f ms (first token %s), %d output tokens, %d tool call(s)",
            iteration,
            (time.perf_counter() - started) * 1000,
            f"{first_token_ms:.0f} ms" if first_token_ms else "none",
            final.output_tokens,
            sum(1 for b in final.content if b.get("type") == "tool_use"),
        )

        # Collect assistant content blocks (text + tool_use) for the transcript.
        for block in final.content:
            if block.get("type") == "text":
                assistant_blocks.append(block)
            elif block.get("type") == "tool_use":
                assistant_blocks.append(block)
                tool_uses.append(
                    {"id": block["id"], "name": block["name"], "input": block.get("input")}
                )

        messages.append({"role": "assistant", "content": assistant_blocks})

        if not tool_uses:
            # A fresh (non-reused) proposal was armed this turn and the model has now
            # stopped to ask for confirmation. Signal the UI to show Apply/Cancel/Custom
            # buttons instead of making the user type "yes".
            if tools.proposed_this_turn:
                yield CONFIRM_SENTINEL
            return  # model produced a final answer, we're done

        # Sequential on purpose. Running these concurrently would let an apply land
        # before the propose it depends on, and the same-turn gate would never fire.
        tool_results = []
        for tu in tool_uses:
            result = await tools.execute_tool(tu["name"], tu["input"] or {})
            tool_results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": tu["id"],
                    "content": json.dumps(result),
                }
            )
        messages.append({"role": "user", "content": tool_results})

    yield "\n\n[Reached max tool iterations]"
