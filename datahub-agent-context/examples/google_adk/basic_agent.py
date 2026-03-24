#!/usr/bin/env python3
"""
Basic Google ADK agent example using DataHub MCP tools with Gemini.

This example demonstrates how to create a simple Google ADK agent that can:
- Search for datasets in DataHub
- Get detailed entity information
- Query lineage relationships
- Update metadata like tags and descriptions
- Query BigQuery directly (list datasets/tables, run SQL, get insights)

Prerequisites:
    pip install google-adk>=1.1.0 datahub-agent-context
    Set GOOGLE_API_KEY environment variable (or use Vertex AI ADC)
    For BigQuery: set up Application Default Credentials (gcloud auth application-default login)

Environment variables:
    GOOGLE_API_KEY: Google AI API key (for Gemini Developer API)
    DATAHUB_GMS_URL: DataHub GMS endpoint (default: http://localhost:8080)
    DATAHUB_GMS_TOKEN: DataHub access token (optional)
    BIGQUERY_PROJECT_ID: GCP project ID for BigQuery queries (optional)
"""

import asyncio
import logging
import os
import sys
import warnings

import google.auth
from google.adk.agents import Agent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools.bigquery import BigQueryCredentialsConfig, BigQueryToolset
from google.adk.tools.bigquery.config import BigQueryToolConfig, WriteMode
from google.genai import types

from datahub.sdk.main_client import DataHubClient
from datahub_agent_context.google_adk_tools import build_google_adk_tools

warnings.filterwarnings("ignore", message=".*GOOGLE_TOOL.*")
logging.getLogger("google.genai.types").setLevel(logging.ERROR)

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

# Disable colors when not a TTY
if not sys.stdout.isatty():
    RESET = BOLD = DIM = CYAN = GREEN = YELLOW = BLUE = MAGENTA = RED = ""

SYSTEM_PROMPT = """You are a senior data analyst with access to two tools: DataHub (a metadata catalog) \
and BigQuery (the data warehouse).

## Your workflow

When a user asks a question about data:
1. **Discover** — Search DataHub first to find relevant tables, understand their schemas, \
descriptions, and ownership. Use DataHub as your source of truth for what data exists and what it means.
2. **Plan** — Based on schema information from DataHub, identify the right tables and fields to query.
3. **Query** — Execute SQL in BigQuery to get actual data. Always use project `acryl-staging` and \
dataset `fiction_bank`.
4. **Synthesize** — Interpret the results in plain language. Surface insights, patterns, and anomalies. \
Don't just dump raw data — tell the user what it means.

## Guidelines

- **Always check DataHub before writing SQL.** Never guess table or column names — look them up.
- **Search documentation.** Use the `search_documents` tool to find internal knowledge articles, \
runbooks, and FAQs (e.g. from Notion or Confluence). These often contain business context — such as \
how a metric is defined or how a pipeline works — that is essential for writing correct queries.
- **Write efficient SQL.** Use `LIMIT` clauses when exploring, avoid `SELECT *` on large tables, \
and filter early.
- **Explain your reasoning.** When you choose a table or approach, briefly say why.
- **Handle ambiguity.** If the user's question could map to multiple tables, ask a clarifying question \
or query the most likely one and confirm.
- **Be concise.** Lead with the answer or insight, then provide supporting data.
- **Lineage.** When asked where data comes from or flows to, use DataHub lineage tools and explain \
the pipeline in plain terms.

## BigQuery scope
- Project: `acryl-staging`
- Dataset: `fiction_bank`
- Write operations are blocked — read-only access only.
"""


def print_header(text: str) -> None:
    width = 80
    print(f"\n{BOLD}{BLUE}{'═' * width}{RESET}")
    print(f"{BOLD}{BLUE}  {text}{RESET}")
    print(f"{BOLD}{BLUE}{'═' * width}{RESET}")


def print_divider() -> None:
    print(f"{DIM}{'─' * 80}{RESET}")


async def run_agent_streaming(runner: Runner, session_id: str, user_input: str) -> None:
    """Stream agent events, printing tool calls and final response as they arrive."""
    print(f"\n{BOLD}{GREEN}Agent:{RESET}")
    final_text = ""

    async for event in runner.run_async(
        user_id="user",
        session_id=session_id,
        new_message=types.Content(role="user", parts=[types.Part(text=user_input)]),
    ):
        # Show function/tool calls as they happen
        if event.content and event.content.parts:
            for part in event.content.parts:
                if part.function_call:
                    fc = part.function_call
                    import json as _json

                    args_str = _json.dumps(
                        dict(fc.args) if fc.args else {}, separators=(",", ":")
                    )
                    if len(args_str) > 120:
                        args_str = args_str[:117] + "..."
                    print(
                        f"\n{YELLOW}  ⚙ {BOLD}{fc.name}{RESET}{YELLOW}  {DIM}{args_str}{RESET}"
                    )
                elif part.function_response:
                    fr = part.function_response
                    result_str = str(fr.response) if fr.response else ""
                    if len(result_str) > 200:
                        result_str = result_str[:197] + "..."
                    print(f"  {DIM}↳ {result_str}{RESET}")

        # Capture final response text
        if event.is_final_response() and event.content and event.content.parts:
            for part in event.content.parts:
                if part.text:
                    final_text = part.text

    if final_text:
        print(f"\n{final_text}")


async def main() -> None:
    # Initialize DataHub connection
    print(f"{DIM}Connecting to DataHub...{RESET}")
    datahub_gms_url = os.getenv("DATAHUB_GMS_URL")
    if datahub_gms_url is None:
        client = DataHubClient.from_env()
    else:
        client = DataHubClient(
            server=datahub_gms_url, token=os.getenv("DATAHUB_GMS_TOKEN")
        )

    # Create DataHub tools - includes mutation tools for tagging
    datahub_tools = build_google_adk_tools(client, include_mutations=True)

    # Optionally enable BigQuery tools if GCP credentials are available.
    # Set BIGQUERY_PROJECT_ID to control which project is billed for queries;
    # if unset, the project from Application Default Credentials is used.
    extra_tools: list = []
    try:
        credentials, project = google.auth.default()
        compute_project = os.getenv("BIGQUERY_PROJECT_ID") or project
        bigquery_toolset = BigQueryToolset(
            credentials_config=BigQueryCredentialsConfig(credentials=credentials),
            bigquery_tool_config=BigQueryToolConfig(
                write_mode=WriteMode.BLOCKED,
                **({"compute_project_id": compute_project} if compute_project else {}),
            ),
        )
        extra_tools.append(bigquery_toolset)
        print(f"{GREEN}BigQuery tools enabled{' (project: ' + compute_project + ')' if compute_project else ''}{RESET}")
    except google.auth.exceptions.DefaultCredentialsError:
        print(f"{DIM}BigQuery tools disabled (no GCP credentials found — run 'gcloud auth application-default login' to enable){RESET}")

    # Create agent
    agent = Agent(
        model="gemini-3-flash-preview",
        name="datahub_agent",
        description="A data discovery assistant with access to DataHub metadata and BigQuery.",
        instruction=SYSTEM_PROMPT,
        tools=[*datahub_tools, *extra_tools],
    )

    # Single persistent session — all turns share conversation history
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

    print_header("DataHub Google ADK Data Analyst Agent  ·  Interactive Mode")
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

            await run_agent_streaming(runner, session.id, user_input)
            print_divider()

        except KeyboardInterrupt:
            print(f"\n{DIM}Goodbye!{RESET}")
            break
        except Exception as e:
            print(f"\n{RED}❌ Error: {e}{RESET}")


if __name__ == "__main__":
    asyncio.run(main())
