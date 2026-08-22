"""
DataHub tools exposed to the LLM agent.

Each tool has:
  1. A JSON schema (TOOL_DEFINITIONS) -> tells Claude the tool exists + when to use it.
  2. An executor (execute_tool) -> actually runs the query against DataHub's GraphQL API.

For the hackathon these call DataHub GraphQL directly. In the GMS port, these become
direct EntityService/TagService calls, or reuse the DataHub MCP tools.
"""
from __future__ import annotations

import os
from typing import Any

import httpx

# DataHub GraphQL endpoint (the frontend Play server proxies to GMS).
DATAHUB_GMS_URL = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080")
DATAHUB_GRAPHQL_URL = f"{DATAHUB_GMS_URL}/api/graphql"
# System token for GMS (quickstart uses a fixed token when metadata auth enabled;
# for open quickstart no auth header is needed).
DATAHUB_TOKEN = os.environ.get("DATAHUB_TOKEN", "")


def _headers() -> dict[str, str]:
    h = {"Content-Type": "application/json"}
    if DATAHUB_TOKEN:
        h["Authorization"] = f"Bearer {DATAHUB_TOKEN}"
    return h


async def _graphql(query: str, variables: dict[str, Any]) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(
            DATAHUB_GRAPHQL_URL,
            headers=_headers(),
            json={"query": query, "variables": variables},
        )
        resp.raise_for_status()
        return resp.json()


# ─── Tool definitions (sent to Claude) ──────────────────────────────────────────

TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "name": "search_datasets",
        "description": (
            "Search DataHub for datasets/tables by a keyword or name. "
            "Use this when the user refers to a dataset by name and you need its URN."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search keywords, e.g. 'users' or 'fct_users'"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_dataset_schema",
        "description": (
            "Get the schema (fields/columns) of a dataset by its URN, including field-level tags. "
            "Use this to answer questions about columns, data types, or which fields are PII."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "urn": {"type": "string", "description": "Dataset URN"},
            },
            "required": ["urn"],
        },
    },
    {
        "name": "get_dataset_tags",
        "description": (
            "Get dataset-level tags and glossary terms for a dataset by URN. "
            "Use this to answer questions about classifications like PII, sensitivity, or governance."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "urn": {"type": "string", "description": "Dataset URN"},
            },
            "required": ["urn"],
        },
    },
]


# ─── Tool executors ─────────────────────────────────────────────────────────────

async def _search_datasets(query: str) -> dict[str, Any]:
    gql = """
    query search($input: SearchInput!) {
      search(input: $input) {
        searchResults {
          entity {
            urn
            ... on Dataset {
              name
              platform { name }
              properties { name description }
            }
          }
        }
      }
    }
    """
    data = await _graphql(gql, {"input": {"type": "DATASET", "query": query, "start": 0, "count": 5}})
    results = (
        data.get("data", {}).get("search", {}).get("searchResults", []) if data.get("data") else []
    )
    datasets = []
    for r in results:
        ent = r.get("entity", {})
        datasets.append(
            {
                "urn": ent.get("urn"),
                "name": (ent.get("properties") or {}).get("name") or ent.get("name"),
                "platform": (ent.get("platform") or {}).get("name"),
            }
        )
    return {"datasets": datasets}


async def _get_dataset_schema(urn: str) -> dict[str, Any]:
    gql = """
    query ds($urn: String!) {
      dataset(urn: $urn) {
        urn
        name
        schemaMetadata {
          fields {
            fieldPath
            type
            nativeDataType
            globalTags { tags { tag { urn properties { name } } } }
          }
        }
      }
    }
    """
    data = await _graphql(gql, {"urn": urn})
    ds = (data.get("data") or {}).get("dataset")
    if not ds:
        return {"error": f"Dataset not found: {urn}"}
    fields = []
    for f in ((ds.get("schemaMetadata") or {}).get("fields") or []):
        tags = [
            (t.get("tag", {}).get("properties") or {}).get("name")
            or t.get("tag", {}).get("urn")
            for t in ((f.get("globalTags") or {}).get("tags") or [])
        ]
        fields.append(
            {
                "name": f.get("fieldPath"),
                "type": f.get("nativeDataType") or f.get("type"),
                "tags": tags,
            }
        )
    return {"urn": ds.get("urn"), "name": ds.get("name"), "fields": fields}


async def _get_dataset_tags(urn: str) -> dict[str, Any]:
    gql = """
    query ds($urn: String!) {
      dataset(urn: $urn) {
        urn
        name
        tags { tags { tag { urn properties { name } } } }
        glossaryTerms { terms { term { urn properties { name } } } }
      }
    }
    """
    data = await _graphql(gql, {"urn": urn})
    ds = (data.get("data") or {}).get("dataset")
    if not ds:
        return {"error": f"Dataset not found: {urn}"}
    tags = [
        (t.get("tag", {}).get("properties") or {}).get("name") or t.get("tag", {}).get("urn")
        for t in ((ds.get("tags") or {}).get("tags") or [])
    ]
    terms = [
        (t.get("term", {}).get("properties") or {}).get("name") or t.get("term", {}).get("urn")
        for t in ((ds.get("glossaryTerms") or {}).get("terms") or [])
    ]
    return {"urn": ds.get("urn"), "name": ds.get("name"), "tags": tags, "glossaryTerms": terms}


_EXECUTORS = {
    "search_datasets": lambda args: _search_datasets(args["query"]),
    "get_dataset_schema": lambda args: _get_dataset_schema(args["urn"]),
    "get_dataset_tags": lambda args: _get_dataset_tags(args["urn"]),
}


async def execute_tool(name: str, args: dict[str, Any]) -> dict[str, Any]:
    """Run a tool by name. Returns a JSON-serializable dict."""
    executor = _EXECUTORS.get(name)
    if not executor:
        return {"error": f"Unknown tool: {name}"}
    try:
        return await executor(args)
    except Exception as exc:  # noqa: BLE001
        return {"error": f"Tool {name} failed: {exc}"}
