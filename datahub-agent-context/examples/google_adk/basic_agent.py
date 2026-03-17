#!/usr/bin/env python3
"""
Basic Google ADK agent example using DataHub MCP tools with Gemini.

This example demonstrates how to create a simple Google ADK agent that can:
- Search for datasets in DataHub
- Get detailed entity information
- Query lineage relationships
- Update metadata like tags and descriptions

Prerequisites:
    pip install google-adk datahub-agent-context
    Set GOOGLE_API_KEY environment variable (or use Vertex AI ADC)

Environment variables:
    GOOGLE_API_KEY: Google AI API key (for Gemini Developer API)
    DATAHUB_GMS_URL: DataHub GMS endpoint (default: http://localhost:8080)
    DATAHUB_GMS_TOKEN: DataHub access token (optional)
"""

import asyncio
import os

from google.adk.agents import Agent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from datahub.sdk.main_client import DataHubClient
from datahub_agent_context.google_adk_tools import build_google_adk_tools

SYSTEM_PROMPT = """You are a helpful data discovery assistant with access to DataHub.

You can help users:
- Find datasets by searching keywords
- Understand data schemas and documentation
- Trace data lineage to see where data comes from and where it goes
- Add metadata like tags to datasets

When searching, always examine the top results to find the most relevant dataset.
When asked about lineage, explain the relationships clearly.
Be concise but informative in your responses."""


async def run_query(
    runner: Runner,
    session_id: str,
    query: str,
) -> str:
    response_text = ""
    async for event in runner.run_async(
        user_id="user",
        session_id=session_id,
        new_message=types.Content(
            role="user", parts=[types.Part(text=query)]
        ),
    ):
        if event.is_final_response() and event.content and event.content.parts:
            response_text = event.content.parts[0].text
    return response_text


async def main() -> None:
    # Initialize DataHub connection
    print("Connecting to DataHub...")
    datahub_gms_url = os.getenv("DATAHUB_GMS_URL")
    if datahub_gms_url is None:
        client = DataHubClient.from_env()
    else:
        client = DataHubClient(
            server=datahub_gms_url, token=os.getenv("DATAHUB_GMS_TOKEN")
        )

    # Create tools - includes mutation tools for tagging
    tools = build_google_adk_tools(client, include_mutations=True)

    # Create agent
    agent = Agent(
        model="gemini-2.0-flash",
        name="datahub_agent",
        description="A data discovery assistant with access to DataHub metadata.",
        instruction=SYSTEM_PROMPT,
        tools=tools,
    )

    session_service = InMemorySessionService()
    session = await session_service.create_session(
        app_name="datahub_basic_agent",
        user_id="user",
    )
    runner = Runner(
        agent=agent,
        app_name="datahub_basic_agent",
        session_service=session_service,
    )

    # Example queries
    examples = [
        "Find all datasets related to 'customers'",
        "What are the schemas of the top 2 customer datasets?",
        "Show me the upstream lineage for the first customer dataset you found",
    ]

    print("\n" + "=" * 80)
    print("DataHub Google ADK Agent - Interactive Demo")
    print("=" * 80)

    print("\nExample Queries:")
    for i, example in enumerate(examples, 1):
        print(f"{i}. {example}")

    print("\n" + "=" * 80)
    print("Running first example query...\n")

    answer = await run_query(runner, session.id, examples[0])
    print("\n" + "=" * 80)
    print(f"Final Answer:\n{answer}")
    print("=" * 80)

    # Interactive mode — each turn gets a fresh session so context is stateless
    print("\nInteractive Mode - Type 'quit' to exit")
    print("=" * 80)

    while True:
        try:
            user_input = input("\nYou: ").strip()
            if not user_input:
                continue
            if user_input.lower() in ["quit", "exit", "q"]:
                print("Goodbye!")
                break

            # New session per turn keeps memory usage bounded
            turn_session = await session_service.create_session(
                app_name="datahub_basic_agent",
                user_id="user",
            )
            answer = await run_query(runner, turn_session.id, user_input)
            print(f"\nAgent: {answer}")

        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"\nError: {e}")


if __name__ == "__main__":
    asyncio.run(main())
