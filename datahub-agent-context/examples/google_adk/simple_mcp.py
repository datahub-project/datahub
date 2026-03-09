#!/usr/bin/env python3
"""
Minimal example: Simple dataset search with Google ADK using Gemini.

This is the absolute minimum code needed to use DataHub tools with Google ADK.

Prerequisites:
    - Google API key set in GOOGLE_API_KEY environment variable
      (or configure via Application Default Credentials for Vertex AI)

Usage:
    python simple_mcp.py
"""

import asyncio
import os

from google.adk.agents import Agent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools.mcp_tool import McpToolset
from google.adk.tools.mcp_tool.mcp_session_manager import StreamableHTTPConnectionParams
from google.genai import types

from datahub.sdk.main_client import DataHubClient

# 1. Connect to DataHub
datahub_gms_url = os.getenv("DATAHUB_GMS_URL")
if datahub_gms_url is None:
    client = DataHubClient.from_env()
else:
    client = DataHubClient(server=datahub_gms_url, token=os.getenv("DATAHUB_GMS_TOKEN"))

datahub_mcp_server_url = os.getenv(
    "DATAHUB_MCP_SERVER_URL", "http://localhost:8080/mcp"
)


async def main() -> None:
    session_service = InMemorySessionService()
    session = await session_service.create_session(
        app_name="datahub_simple_search",
        user_id="user",
    )

    toolset = McpToolset(
        connection_params=StreamableHTTPConnectionParams(
            url=datahub_mcp_server_url,
            headers={"Authorization": f"Bearer {client._graph._token}"},
        ),
    )
    # Eagerly initialize the MCP session in this task so that the AsyncExitStack
    # is owned here — otherwise ADK creates it in a spawned task and close()
    # raises "Attempted to exit cancel scope in a different task".
    await toolset.get_tools()

    # 3. Create agent
    agent = Agent(
        model="gemini-2.5-flash",
        name="datahub_agent",
        instruction="You help users find datasets in DataHub. Provide clear, concise answers.",
        tools=[toolset],
    )

    runner = Runner(
        agent=agent,
        app_name="datahub_simple_search",
        session_service=session_service,
    )

    query = "Find datasets about users"
    print(f"Query: {query}\n")

    try:
        async for event in runner.run_async(
            user_id="user",
            session_id=session.id,
            new_message=types.Content(role="user", parts=[types.Part(text=query)]),
        ):
            if event.is_final_response() and event.content and event.content.parts:
                print(f"Agent: {event.content.parts[0].text}")
    finally:
        await toolset.close()


if __name__ == "__main__":
    asyncio.run(main())
