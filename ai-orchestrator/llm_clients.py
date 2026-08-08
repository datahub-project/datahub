"""Provider-neutral clients for the orchestrator's model calls."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, AsyncIterator, Protocol

import anthropic
from openai import AsyncOpenAI


@dataclass(frozen=True)
class TextDelta:
    """A text fragment emitted while a model response is streaming."""

    text: str


@dataclass(frozen=True)
class ModelTurn:
    """The normalized model response consumed by the agent loop."""

    content: list[dict[str, Any]]
    output_tokens: int | None


class LLMClient(Protocol):
    """Provider-neutral interface shared by Anthropic and OpenAI clients."""

    def stream(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        model: str,
        system: str,
        max_tokens: int,
    ) -> AsyncIterator[TextDelta | ModelTurn]:
        """Stream a model turn and finish with its normalized response."""

    async def complete_with_tool(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        tool_name: str,
        model: str,
        system: str,
        max_tokens: int,
    ) -> ModelTurn:
        """Run a non-streaming turn with one required tool call."""


class AnthropicLLMClient:
    """Anthropic implementation of the provider-neutral client interface."""

    def __init__(self, api_key: str) -> None:
        self._client = anthropic.AsyncAnthropic(api_key=api_key)

    async def stream(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        model: str,
        system: str,
        max_tokens: int,
    ) -> AsyncIterator[TextDelta | ModelTurn]:
        """Stream an Anthropic response as normalized events."""
        async with self._client.messages.stream(
            model=model,
            max_tokens=max_tokens,
            system=system,
            tools=tools,
            messages=messages,
        ) as stream:
            async for event in stream:
                if event.type == "content_block_delta" and event.delta.type == "text_delta":
                    yield TextDelta(event.delta.text)

            final = await stream.get_final_message()
            yield ModelTurn(
                content=[_anthropic_block_to_dict(block) for block in final.content],
                output_tokens=final.usage.output_tokens,
            )

    async def complete_with_tool(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        tool_name: str,
        model: str,
        system: str,
        max_tokens: int,
    ) -> ModelTurn:
        """Run an Anthropic response with a required tool call."""
        response = await self._client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system,
            tools=tools,
            tool_choice={"type": "tool", "name": tool_name},
            messages=messages,
        )
        return ModelTurn(
            content=[_anthropic_block_to_dict(block) for block in response.content],
            output_tokens=response.usage.output_tokens,
        )


class OpenAILLMClient:
    """OpenAI Responses API implementation of the shared client interface."""

    def __init__(self, api_key: str) -> None:
        self._client = AsyncOpenAI(api_key=api_key)

    async def stream(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        model: str,
        system: str,
        max_tokens: int,
    ) -> AsyncIterator[TextDelta | ModelTurn]:
        """Stream an OpenAI Responses API response as normalized events."""
        stream = await self._client.responses.create(
            model=model,
            instructions=system,
            input=_openai_input(messages),
            tools=_openai_tools(tools),
            max_output_tokens=max_tokens,
            stream=True,
        )
        async for event in stream:
            if event.type == "response.output_text.delta":
                yield TextDelta(event.delta)
            elif event.type == "response.completed":
                yield _openai_response_to_turn(event.response)

    async def complete_with_tool(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        tool_name: str,
        model: str,
        system: str,
        max_tokens: int,
    ) -> ModelTurn:
        """Run an OpenAI Responses API request with a required function call."""
        response = await self._client.responses.create(
            model=model,
            instructions=system,
            input=_openai_input(messages),
            tools=_openai_tools(tools),
            tool_choice={"type": "function", "name": tool_name},
            max_output_tokens=max_tokens,
        )
        return _openai_response_to_turn(response)


def create_llm_client(provider: str | None, model: str, api_key: str) -> LLMClient:
    """Create the client matching the configured provider or model prefix."""
    resolved_provider = (provider or _provider_for_model(model)).lower()
    if resolved_provider == "claude":
        return AnthropicLLMClient(api_key)
    if resolved_provider == "openai":
        return OpenAILLMClient(api_key)
    raise ValueError(f"Unsupported LLM provider: {resolved_provider}")


def _provider_for_model(model: str) -> str:
    if model.startswith("claude-"):
        return "claude"
    if model.startswith(("gpt-", "o1-", "o3-", "o4-")):
        return "openai"
    raise ValueError(f"Cannot infer LLM provider from model: {model}")


def _anthropic_block_to_dict(block: Any) -> dict[str, Any]:
    if block.type == "text":
        return {"type": "text", "text": block.text}
    if block.type == "tool_use":
        return {
            "type": "tool_use",
            "id": block.id,
            "name": block.name,
            "input": block.input,
        }
    return {"type": block.type}


def _openai_tools(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "type": "function",
            "name": tool["name"],
            "description": tool.get("description", ""),
            "parameters": tool.get("input_schema", {}),
        }
        for tool in tools
    ]


def _openai_input(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    converted: list[dict[str, Any]] = []
    for message in messages:
        content = message.get("content")
        if isinstance(content, str):
            converted.append({"role": message["role"], "content": content})
            continue
        for block in content or []:
            if block["type"] == "text":
                converted.append({"role": message["role"], "content": block["text"]})
            elif block["type"] == "tool_use":
                converted.append(
                    {
                        "type": "function_call",
                        "call_id": block["id"],
                        "name": block["name"],
                        "arguments": json.dumps(block.get("input", {})),
                    }
                )
            elif block["type"] == "tool_result":
                converted.append(
                    {
                        "type": "function_call_output",
                        "call_id": block["tool_use_id"],
                        "output": block.get("content", ""),
                    }
                )
    return converted


def _openai_response_to_turn(response: Any) -> ModelTurn:
    content: list[dict[str, Any]] = []
    for item in response.output:
        if item.type == "message":
            for part in item.content:
                if part.type == "output_text":
                    content.append({"type": "text", "text": part.text})
        elif item.type == "function_call":
            content.append(
                {
                    "type": "tool_use",
                    "id": item.call_id,
                    "name": item.name,
                    "input": json.loads(item.arguments),
                }
            )
    usage = getattr(response, "usage", None)
    return ModelTurn(
        content=content,
        output_tokens=getattr(usage, "output_tokens", None),
    )
