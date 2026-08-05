"""Register MCP servers into DataHub as first-class ``Service`` entities.

A ``Service`` (``urn:li:service:<id>``) is a catalog entry for an external
service such as an MCP (Model Context Protocol) server. Unlike a Dataset, a
Service has no schema/columns -- it carries identity, a description, a subtype,
connection details, and the ``api`` entities it composes (its tools).

The Service's connection aspects are emitted directly via
:class:`MetadataChangeProposalWrapper`; the tools it exposes are emitted as
first-class ``api`` entities (via the SDK ``Api`` helper) and linked back to the
service through ``ServiceProperties.apis`` (the ``ServiceComposesApi``
relationship). Everything upserts by URN, so the script is idempotent.

The primary example is DataHub's own MCP server (``@acryldata/mcp-server-datahub``),
which exposes search / lineage / metadata tools.

Usage::

    export DATAHUB_GMS_URL=http://localhost:8080   # GMS, not the frontend
    # export DATAHUB_GMS_TOKEN=<token>             # if auth is enabled
    python register_mcp_servers.py

    # View a profile at (frontend):
    #   http://localhost:9002/service/urn:li:service:datahub-mcp-server
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from datahub.api.entities.agent.api import API_SUBTYPE_MCP_TOOL, Api, ApiParam
from datahub.api.entities.common.large_string import make_large_string
from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    McpServerPropertiesClass,
    McpTransportClass,
    ServiceDefinitionClass,
    ServiceDefinitionFormatClass,
    ServicePropertiesClass,
    StatusClass,
    SubTypesClass,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Subtype stored in the standard subTypes aspect (matches ServiceSubType.MCP).
MCP_SUBTYPE = "MCP"


def make_service_urn(service_id: str) -> str:
    # Mirrors ServiceKey.pdl: the key is a single ``id`` field.
    return f"urn:li:service:{service_id}"


@dataclass
class McpServerSpec:
    id: str
    display_name: str
    description: str
    url: str
    platform: str
    transport: str = McpTransportClass.HTTP
    timeout: Optional[float] = None
    custom_headers: Dict[str, str] = field(default_factory=dict)
    # The API entities (tools) this server composes; emitted + linked via
    # ServiceProperties.apis (the ServiceComposesApi relationship).
    tools: List[Api] = field(default_factory=list)
    # The whole-contract document (serviceContract aspect). For an MCP server the
    # faithful contract is its JSON-RPC tools/list payload (JSON Schema), NOT an
    # OpenAPI doc -- MCP does not speak OpenAPI.
    contract: Optional[ServiceDefinitionClass] = None


def _params_to_json_schema(params: List[ApiParam]) -> dict:
    """Render an ApiParam list as a JSON Schema object (an MCP tool signature)."""
    return {
        "type": "object",
        "properties": {
            p.name: {"type": p.data_type, "description": p.description or ""}
            for p in params
        },
        "required": [p.name for p in params if p.required],
    }


def build_mcp_tools_list_contract(tools: List[Api]) -> ServiceDefinitionClass:
    """Build a serviceContract from an MCP server's tools, as its tools/list result.

    This mirrors what an MCP client receives from the server's ``tools/list``
    JSON-RPC method: each tool carries an ``inputSchema`` (and ``outputSchema``)
    in JSON Schema. That payload IS the MCP server's contract.
    """
    tools_list = {
        "tools": [
            {
                "name": t.name,
                "description": t.description or "",
                "inputSchema": _params_to_json_schema(t.parameters or []),
                "outputSchema": _params_to_json_schema(t.returns or []),
            }
            for t in tools
        ]
    }
    return ServiceDefinitionClass(
        format=ServiceDefinitionFormatClass.JSON_SCHEMA,
        rawSpec=make_large_string(json.dumps(tools_list, indent=2)),
        version="2025-06-18",  # MCP protocol revision this tools/list conforms to
    )


# The tools the DataHub MCP server exposes, as first-class MCP_TOOL APIs.
DATAHUB_MCP_TOOLS: List[Api] = [
    Api(
        id="datahub-mcp-search",
        name="search",
        subtypes=[API_SUBTYPE_MCP_TOOL],
        description="Search across the DataHub catalog for entities matching a query.",
        platform="datahub",
        parameters=[
            ApiParam(
                name="query",
                data_type="string",
                required=True,
                description="The search query.",
            ),
            ApiParam(
                name="entity_types",
                data_type="array<string>",
                required=False,
                description="Optional entity types to restrict the search to.",
            ),
        ],
        returns=[
            ApiParam(
                name="results",
                data_type="array<SearchResult>",
                required=True,
                description="Matching entities with their urns and metadata.",
            ),
            ApiParam(
                name="total",
                data_type="number",
                required=True,
                description="Total number of matches.",
            ),
        ],
    ),
    Api(
        id="datahub-mcp-get-lineage",
        name="get_lineage",
        subtypes=[API_SUBTYPE_MCP_TOOL],
        description="Fetch upstream and downstream lineage for an entity urn.",
        platform="datahub",
        parameters=[
            ApiParam(
                name="urn",
                data_type="string",
                required=True,
                description="The entity urn to trace lineage for.",
            ),
            ApiParam(
                name="direction",
                data_type="string",
                required=False,
                description="UPSTREAM or DOWNSTREAM (default both).",
            ),
        ],
        returns=[
            ApiParam(
                name="upstreams",
                data_type="array<string>",
                required=True,
                description="Upstream entity urns.",
            ),
            ApiParam(
                name="downstreams",
                data_type="array<string>",
                required=True,
                description="Downstream entity urns.",
            ),
        ],
    ),
    Api(
        id="datahub-mcp-get-entity",
        name="get_entity",
        subtypes=[API_SUBTYPE_MCP_TOOL],
        description="Fetch the full metadata for a single entity by urn.",
        platform="datahub",
        parameters=[
            ApiParam(
                name="urn",
                data_type="string",
                required=True,
                description="The entity urn to fetch.",
            ),
        ],
        returns=[
            ApiParam(
                name="entity",
                data_type="Entity",
                required=True,
                description="The entity's aspects (properties, ownership, ...).",
            ),
        ],
    ),
]


# ---------------------------------------------------------------------------
# The fleet of MCP servers to register. DataHub's own MCP server is first.
# ---------------------------------------------------------------------------
MCP_SERVERS: List[McpServerSpec] = [
    McpServerSpec(
        id="datahub-mcp-server",
        display_name="DataHub MCP Server",
        description=(
            "Official DataHub MCP server (`@acryldata/mcp-server-datahub`). "
            "Exposes DataHub search, lineage, and metadata tools to AI agents. "
            "Launched over stdio with `npx -y @acryldata/mcp-server-datahub`."
        ),
        # stdio servers have no HTTP endpoint; record the launch command as the URL.
        url="npx -y @acryldata/mcp-server-datahub",
        platform="datahub",
        transport=McpTransportClass.HTTP,
        custom_headers={"X-Client": "datahub-mcp-example"},
        tools=DATAHUB_MCP_TOOLS,
        # An MCP server's real contract is its tools/list payload (JSON Schema).
        contract=build_mcp_tools_list_contract(DATAHUB_MCP_TOOLS),
    ),
    McpServerSpec(
        id="internal-search-mcp",
        display_name="Internal Search MCP",
        description="A generic internal MCP server exposing enterprise search tools.",
        url="https://mcp.internal.example.com/v1",
        platform="mcp",
        transport=McpTransportClass.SSE,
        timeout=120.0,
    ),
    McpServerSpec(
        id="weather-mcp",
        display_name="Weather MCP",
        description="A generic MCP server exposing weather-lookup tools.",
        url="https://mcp.weather.example.com/v1",
        platform="mcp",
        transport=McpTransportClass.HTTP,
    ),
]


def build_service_mcps(
    spec: McpServerSpec, api_urns: List[str]
) -> List[MetadataChangeProposalWrapper]:
    urn = make_service_urn(spec.id)

    aspects: List[
        ServicePropertiesClass
        | McpServerPropertiesClass
        | ServiceDefinitionClass
        | SubTypesClass
        | DataPlatformInstanceClass
        | StatusClass
    ] = [
        ServicePropertiesClass(
            displayName=spec.display_name,
            description=spec.description,
            apis=api_urns or None,
        ),
        McpServerPropertiesClass(
            url=spec.url,
            transport=spec.transport,
            timeout=spec.timeout,
            customHeaders=spec.custom_headers or None,
        ),
        SubTypesClass(typeNames=[MCP_SUBTYPE]),
        DataPlatformInstanceClass(platform=make_data_platform_urn(spec.platform)),
        StatusClass(removed=False),
    ]
    if spec.contract is not None:
        aspects.append(spec.contract)

    return [
        MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect)
        for aspect in aspects
    ]


def main() -> None:
    graph = get_default_graph()

    created: List[str] = []
    for spec in MCP_SERVERS:
        # Emit the tools this server composes, then link them on the service.
        api_urns = [tool.emit(graph) for tool in spec.tools]
        urn = make_service_urn(spec.id)
        for mcp in build_service_mcps(spec, api_urns):
            graph.emit_mcp(mcp)
        created.append(urn)
        logger.info(
            "Registered Service %s (%s) composing %d API(s)",
            urn,
            spec.display_name,
            len(api_urns),
        )

    logger.info("Registered %d MCP server Service entities:", len(created))
    for urn in created:
        logger.info("  %s", urn)


if __name__ == "__main__":
    main()
