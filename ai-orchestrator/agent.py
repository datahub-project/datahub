"""
Model-agnostic agent loop.

The loop is deliberately decoupled from Claude so the LLM can be swapped later
(Rovo, GPT, Gemini, etc). Only `LLMClient` knows about Anthropic; the loop itself
only deals with an abstract "call the model, maybe run tools, repeat" contract.

Yields SSE-friendly token strings as they stream from the model.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import AsyncIterator

import anthropic

import pii_tagger
from local_tools import ToolRegistry
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


class LLMClient:
    """Thin abstraction over the LLM provider. Swap this to change models."""

    def __init__(self, api_key: str) -> None:
        self._client = anthropic.AsyncAnthropic(api_key=api_key)

    def stream(self, messages: list[dict], tools: list[dict], model: str, system: str):
        return self._client.messages.stream(
            model=model,
            max_tokens=MAX_TOKENS,
            system=system,
            tools=tools,
            messages=messages,
        )


async def run_agent(
    user_message: str,
    context: dict | None,
    api_key: str,
    model: str = DEFAULT_MODEL,
    history: list[dict] | None = None,
) -> AsyncIterator[str]:
    """
    Run the agentic loop. Yields text tokens as they arrive.

    Handles the tool-use cycle:
      user msg -> model -> (tool_use?) -> run tool -> feed result -> model -> ... -> final text

    history: prior conversation turns as [{role, content}, ...] — gives Claude memory of prior turns.
    """
    client = LLMClient(api_key=api_key)

    ctx_note = ""
    if context:
        ctx_note = f"\n\n[Page context: {json.dumps(context)}]"

    # Build messages: prior history first, then new user message
    messages: list[dict] = list(history or [])
    messages.append({"role": "user", "content": user_message + ctx_note})

    # Shared MCP session — reused across requests, not spawned per call. Wrapped so the
    # model sees MCP's reads plus the two guarded PII tools, and no raw write tool.
    tools = ToolRegistry(await get_mcp(), api_key=api_key)

    for iteration in range(1, MAX_TOOL_ITERATIONS + 1):
        assistant_blocks: list[dict] = []
        tool_uses: list[dict] = []
        started = time.perf_counter()
        first_token_ms: float | None = None

        # Rebuilt each iteration so a proposal made mid-turn is named in the prompt;
        # otherwise the model keeps reasoning from "no proposal on record".
        system = SYSTEM_PROMPT + pii_tagger.pending_prompt_note()

        async with client.stream(messages, tools.definitions, model, system) as stream:
            async for event in stream:
                if event.type == "content_block_delta" and event.delta.type == "text_delta":
                    if first_token_ms is None:
                        first_token_ms = (time.perf_counter() - started) * 1000
                    yield event.delta.text  # stream text tokens to the UI

            final = await stream.get_final_message()

        # Per-iteration timing: this loop is the dominant cost of a turn, and without it
        # a slow turn is indistinguishable from a slow tool.
        logger.info(
            "iteration %d: %.0f ms (first token %s), %d output tokens, %d tool call(s)",
            iteration,
            (time.perf_counter() - started) * 1000,
            f"{first_token_ms:.0f} ms" if first_token_ms else "none",
            final.usage.output_tokens,
            sum(1 for b in final.content if b.type == "tool_use"),
        )

        # Collect assistant content blocks (text + tool_use) for the transcript.
        for block in final.content:
            if block.type == "text":
                assistant_blocks.append({"type": "text", "text": block.text})
            elif block.type == "tool_use":
                assistant_blocks.append(
                    {"type": "tool_use", "id": block.id, "name": block.name, "input": block.input}
                )
                tool_uses.append({"id": block.id, "name": block.name, "input": block.input})

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
