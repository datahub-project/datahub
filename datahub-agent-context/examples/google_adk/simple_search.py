#!/usr/bin/env python3
"""
Minimal example: Simple dataset search with Google ADK using Gemini.

This is the absolute minimum code needed to use DataHub tools with Google ADK.

Prerequisites:
    - Google API key set in GOOGLE_API_KEY environment variable
      (or configure via Application Default Credentials for Vertex AI)

Usage:
    python simple_search.py
"""

import asyncio
import os

from google.adk.agents import Agent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from datahub.sdk.main_client import DataHubClient
from datahub_agent_context.google_adk_tools import build_google_adk_tools

# 1. Connect to DataHub
datahub_gms_url = os.getenv("DATAHUB_GMS_URL")
if datahub_gms_url is None:
    client = DataHubClient.from_env()
else:
    client = DataHubClient(server=datahub_gms_url, token=os.getenv("DATAHUB_GMS_TOKEN"))

# 2. Create tools using the builder (read-only tools)
tools = build_google_adk_tools(client, include_mutations=False)

# 3. Create agent
agent = Agent(
    model="gemini-2.5-flash",
    name="datahub_agent",
    instruction="You help users find datasets in DataHub. Provide clear, concise answers.",
    tools=tools,
)


async def main() -> None:
    session_service = InMemorySessionService()
    session = await session_service.create_session(
        app_name="datahub_simple_search",
        user_id="user",
    )

    runner = Runner(
        agent=agent,
        app_name="datahub_simple_search",
        session_service=session_service,
    )

    query = "Find datasets about users"
    print(f"Query: {query}\n")

    async for event in runner.run_async(
        user_id="user",
        session_id=session.id,
        new_message=types.Content(role="user", parts=[types.Part(text=query)]),
    ):
        if event.is_final_response() and event.content and event.content.parts:
            print(f"Agent: {event.content.parts[0].text}")


if __name__ == "__main__":
    asyncio.run(main())
