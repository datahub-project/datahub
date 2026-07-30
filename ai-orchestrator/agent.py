"""
Model-agnostic agent loop.

The loop is deliberately decoupled from Claude so the LLM can be swapped later
(Rovo, GPT, Gemini, etc). Only `LLMClient` knows about Anthropic; the loop itself
only deals with an abstract "call the model, maybe run tools, repeat" contract.

Yields SSE-friendly token strings as they stream from the model.
"""
from __future__ import annotations

import json
import os
from typing import AsyncIterator

import anthropic

from mcp_tools import get_mcp

DEFAULT_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-5")
MAX_TOKENS = int(os.environ.get("ANTHROPIC_MAX_TOKENS", "1024"))
MAX_TOOL_ITERATIONS = 6

SYSTEM_PROMPT = (
    "You are the DataHub AI Assistant, embedded in the Privacy Data Catalog. "
    "You help privacy engineers and data stewards answer questions about datasets, "
    "schemas, tags, PII, and lineage. Use the provided tools to look up real data "
    "from DataHub before answering. Be concise and accurate. If the user is viewing a "
    "specific dataset (entityUrn provided in context), prefer that dataset. "
    "When you report PII, base it strictly on tags returned by the tools."
)


class LLMClient:
    """Thin abstraction over the LLM provider. Swap this to change models."""

    def __init__(self, api_key: str) -> None:
        self._client = anthropic.AsyncAnthropic(api_key=api_key)

    def stream(self, messages: list[dict], tools: list[dict], model: str):
        return self._client.messages.stream(
            model=model,
            max_tokens=MAX_TOKENS,
            system=SYSTEM_PROMPT,
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

    # Shared MCP session — reused across requests, not spawned per call.
    tools = await get_mcp()
    for _ in range(MAX_TOOL_ITERATIONS):
        assistant_blocks: list[dict] = []
        tool_uses: list[dict] = []

        async with client.stream(messages, tools.definitions, model) as stream:
            async for event in stream:
                if event.type == "content_block_delta" and event.delta.type == "text_delta":
                    yield event.delta.text  # stream text tokens to the UI

            final = await stream.get_final_message()

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
            return  # model produced a final answer, we're done

        # Execute each requested tool via MCP and feed results back.
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
