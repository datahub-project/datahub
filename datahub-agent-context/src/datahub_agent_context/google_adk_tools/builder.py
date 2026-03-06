"""Builder for Google ADK tools from DataHub MCP tools."""

from typing import TYPE_CHECKING, Callable, List

from datahub_agent_context.mcp_tools import get_me
from datahub_agent_context.mcp_tools.assertions import get_dataset_assertions
from datahub_agent_context.mcp_tools.documents import grep_documents, search_documents
from datahub_agent_context.mcp_tools.domains import remove_domains, set_domains
from datahub_agent_context.mcp_tools.structured_properties import (
    add_structured_properties,
    remove_structured_properties,
)
from datahub_agent_context.utils import create_context_wrapper

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient

from datahub_agent_context.mcp_tools.descriptions import update_description
from datahub_agent_context.mcp_tools.entities import get_entities, list_schema_fields
from datahub_agent_context.mcp_tools.lineage import (
    get_lineage,
    get_lineage_paths_between,
)
from datahub_agent_context.mcp_tools.owners import add_owners, remove_owners
from datahub_agent_context.mcp_tools.queries import get_dataset_queries
from datahub_agent_context.mcp_tools.save_document import save_document
from datahub_agent_context.mcp_tools.search import search
from datahub_agent_context.mcp_tools.tags import add_tags, remove_tags
from datahub_agent_context.mcp_tools.terms import (
    add_glossary_terms,
    remove_glossary_terms,
)


def build_google_adk_tools(
    client: "DataHubClient",
    include_mutations: bool = False,
) -> List[Callable]:
    """Build Google ADK tools with automatic context management.

    Returns plain Python functions wrapped to automatically set the
    DataHubClient in context before execution. These can be passed directly
    to a Google ADK Agent's ``tools`` parameter.

    Args:
        client: DataHubClient instance
        include_mutations: Whether to include mutation tools (default: False)

    Returns:
        List of callables ready for use as Google ADK tools

    Example:
        from google.adk.agents import Agent
        from datahub.sdk.main_client import DataHubClient
        from datahub_agent_context.google_adk_tools import build_google_adk_tools

        client = DataHubClient.from_env()
        tools = build_google_adk_tools(client, include_mutations=True)

        agent = Agent(
            model="gemini-2.0-flash",
            name="datahub_agent",
            instruction="You are a data discovery assistant with access to DataHub.",
            tools=tools,
        )
    """
    tools: List[Callable] = [
        create_context_wrapper(search_documents, client),
        create_context_wrapper(grep_documents, client),
        create_context_wrapper(get_entities, client),
        create_context_wrapper(list_schema_fields, client),
        create_context_wrapper(get_me, client),
        create_context_wrapper(get_lineage, client),
        create_context_wrapper(get_lineage_paths_between, client),
        create_context_wrapper(get_dataset_queries, client),
        create_context_wrapper(get_dataset_assertions, client),
        create_context_wrapper(search, client),
    ]

    if include_mutations:
        tools.extend(
            [
                create_context_wrapper(update_description, client),
                create_context_wrapper(set_domains, client),
                create_context_wrapper(remove_domains, client),
                create_context_wrapper(add_owners, client),
                create_context_wrapper(remove_owners, client),
                create_context_wrapper(add_structured_properties, client),
                create_context_wrapper(remove_structured_properties, client),
                create_context_wrapper(add_tags, client),
                create_context_wrapper(remove_tags, client),
                create_context_wrapper(add_glossary_terms, client),
                create_context_wrapper(remove_glossary_terms, client),
                create_context_wrapper(save_document, client),
            ]
        )

    return tools
