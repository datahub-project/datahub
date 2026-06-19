#!/usr/bin/env python3
"""
Basic LangChain 1.x agent example using DataHub MCP tools with AWS Bedrock.

This example demonstrates how to create a simple LangChain agent that can:
- Search for datasets in DataHub
- Get detailed entity information
- Query lineage relationships
- Update metadata like tags and descriptions

Prerequisites:
    pip install langchain-aws boto3 "datahub-agent-context[langchain]"
    AWS credentials configured (via ~/.aws/credentials or environment variables)
    Access to Claude 4.5 Haiku in AWS Bedrock

Environment variables:
    AWS_REGION: AWS region for Bedrock (default: us-west-2)
    DATAHUB_MCP_URL: DataHub MCP server URL
    DATAHUB_MCP_TOKEN: DataHub MCP server token (optional)
"""

import asyncio
import os
import sys

from langchain.agents import create_agent
from langchain_aws import ChatBedrockConverse
from langchain_mcp_adapters.client import MultiServerMCPClient

# ANSI color codes
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
MAGENTA = "\033[35m"
RED = "\033[31m"
WHITE = "\033[37m"

# Disable colors when not a TTY
if not sys.stdout.isatty():
    RESET = BOLD = DIM = CYAN = GREEN = YELLOW = BLUE = MAGENTA = RED = WHITE = ""


def print_header(text: str) -> None:
    width = 80
    print(f"\n{BOLD}{BLUE}{'═' * width}{RESET}")
    print(f"{BOLD}{BLUE}  {text}{RESET}")
    print(f"{BOLD}{BLUE}{'═' * width}{RESET}")


def print_divider() -> None:
    print(f"{DIM}{'─' * 80}{RESET}")


async def get_mcp_tools() -> list:
    """Load LangChain tools from the DataHub MCP HTTP server."""
    mcp_url = os.getenv("DATAHUB_MCP_URL")
    mcp_token = os.getenv("DATAHUB_MCP_TOKEN")

    connection_config: dict = {
        "transport": "http",
        "url": mcp_url,
    }
    if mcp_token:
        connection_config["headers"] = {"Authorization": f"Bearer {mcp_token}"}

    print(f"{DIM}Connecting to DataHub MCP server at {mcp_url}...{RESET}")
    client = MultiServerMCPClient({"datahub": connection_config})
    return await client.get_tools()


def _extract_text(content: str | list) -> str:
    """Extract plain text from an AIMessage content field."""
    if isinstance(content, str):
        return content
    return "".join(
        b.get("text", "")
        for b in content
        if isinstance(b, dict) and b.get("type") == "text"
    )


def _register_tool_calls(msg, pending_tool_calls: dict) -> None:
    """Store tool call name+args keyed by id so ToolMessage can display them."""
    import json as _json

    for tc in getattr(msg, "tool_calls", []):
        tc_id = tc.get("id")
        if tc_id and tc_id not in pending_tool_calls:
            name = tc.get("name", "tool")
            args_str = _json.dumps(tc.get("args", {}), separators=(",", ":"))
            pending_tool_calls[tc_id] = (name, args_str)


def _handle_ai_message_chunk(
    msg, content: str | list, streaming: bool, pending_tool_calls: dict
) -> bool:
    _register_tool_calls(msg, pending_tool_calls)
    text = _extract_text(content) if content else ""
    if text:
        if not streaming:
            print()  # blank line before first token
            streaming = True
        print(text, end="", flush=True)
    return streaming


def _handle_ai_message(
    msg, content: str | list, streaming: bool, pending_tool_calls: dict
) -> bool:
    additional_kwargs = getattr(msg, "additional_kwargs", {}) or {}
    response_metadata = getattr(msg, "response_metadata", {}) or {}
    stop_reason = additional_kwargs.get("stop_reason") or response_metadata.get(
        "stopReason"
    )
    if stop_reason == "tool_use":
        if streaming:
            print()
            streaming = False
        thinking = _extract_text(content) if content else ""
        if thinking:
            print(f"\n{MAGENTA}  💭 {DIM}{thinking}{RESET}")
        _register_tool_calls(msg, pending_tool_calls)
    elif not streaming and content:
        text = _extract_text(content)
        if text:
            print(f"\n{text}")
    return streaming


def _handle_tool_message(
    msg, content: str | list, streaming: bool, pending_tool_calls: dict
) -> bool:
    if streaming:
        print()
        streaming = False
    tc_id = getattr(msg, "tool_call_id", None)
    if tc_id and tc_id in pending_tool_calls:
        name, args_str = pending_tool_calls.pop(tc_id)
        if len(args_str) > 120:
            args_str = args_str[:117] + "..."
        print(f"\n{YELLOW}  ⚙ {BOLD}{name}{RESET}{YELLOW}  {DIM}{args_str}{RESET}")
    out_str = _extract_text(content) if content else str(content)
    if len(out_str) > 200:
        out_str = out_str[:197] + "..."
    print(f"  {DIM}↳ {out_str}{RESET}")
    return streaming


async def run_agent_streaming(agent, user_input: str) -> None:
    """Stream agent messages, printing tool calls and AI tokens as they arrive."""
    print(f"\n{BOLD}{GREEN}Agent:{RESET}")
    streaming = False
    pending_tool_calls: dict = {}

    async for msg, _ in agent.astream(
        {"messages": [{"role": "user", "content": user_input}]},
        stream_mode="messages",
    ):
        msg_type = type(msg).__name__
        content = getattr(msg, "content", None)

        if msg_type == "AIMessageChunk":
            streaming = _handle_ai_message_chunk(
                msg, content, streaming, pending_tool_calls
            )
        elif msg_type == "AIMessage":
            streaming = _handle_ai_message(msg, content, streaming, pending_tool_calls)
        elif msg_type == "ToolMessage":
            streaming = _handle_tool_message(
                msg, content, streaming, pending_tool_calls
            )

    if streaming:
        print()  # final newline


async def main() -> None:
    """Run the basic agent example using LangChain 1.x create_agent API."""
    tools = await get_mcp_tools()
    print(f"{GREEN}✓ Loaded {len(tools)} tools from DataHub MCP server{RESET}")

    aws_region = os.getenv("AWS_REGION", "us-west-2")
    print(f"{DIM}Connecting to AWS Bedrock ({aws_region})...{RESET}")

    model = ChatBedrockConverse(
        model="us.anthropic.claude-haiku-4-5-20251001-v1:0",
        region_name=aws_region,
        max_tokens=4096,
        temperature=0,
    )

    system_prompt = """You are a helpful data discovery assistant with access to DataHub.

You can help users:
- Find datasets by searching keywords
- Understand data schemas and documentation
- Trace data lineage to see where data comes from and where it goes
- Add metadata like tags to datasets

When searching, always examine the top results to find the most relevant dataset.
When asked about lineage, explain the relationships clearly.
Be concise but informative in your responses."""

    agent = create_agent(model, tools=tools, system_prompt=system_prompt)

    print_header("DataHub LangChain Agent  ·  Interactive Mode")
    print(f"\n{DIM}Example queries:{RESET}")
    examples = [
        "Find all datasets related to 'customers'",
        "What are the schemas of the top 2 customer datasets?",
        "Show me the upstream lineage for the first customer dataset you found",
    ]
    for i, example in enumerate(examples, 1):
        print(f"  {DIM}{i}. {example}{RESET}")
    print(f"\n{DIM}Type 'quit' to exit.{RESET}")
    print_divider()

    while True:
        try:
            user_input = input(f"\n{BOLD}{CYAN}You:{RESET} ").strip()
            if not user_input:
                continue
            if user_input.lower() in ["quit", "exit", "q"]:
                print(f"\n{DIM}Goodbye!{RESET}")
                break

            await run_agent_streaming(agent, user_input)
            print_divider()

        except KeyboardInterrupt:
            print(f"\n{DIM}Goodbye!{RESET}")
            break
        except Exception as e:
            print(f"\n{RED}❌ Error: {e}{RESET}")


if __name__ == "__main__":
    asyncio.run(main())
