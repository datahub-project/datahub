"""Builder for LangChain tools from DataHub MCP tools."""

import functools
from typing import TYPE_CHECKING, Callable

from datahub_agent_context.context import set_graph
from datahub_agent_context.mcp_tools import get_me
from datahub_agent_context.mcp_tools.documents import grep_documents, search_documents
from datahub_agent_context.mcp_tools.domains import remove_domains, set_domains
from datahub_agent_context.mcp_tools.structured_properties import (
    add_structured_properties,
    remove_structured_properties,
)

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient

try:
    from langchain_core.tools import tool  # type: ignore[import-not-found]
    from langchain_core.tools.base import BaseTool  # type: ignore[import-not-found]
except ImportError as e:
    raise ImportError(
        "langchain-core is required for LangChain tools. "
        "Install with: pip install 'datahub-agent-context[langchain]'"
    ) from e

from datahub_agent_context.mcp_tools.descriptions import update_description
from datahub_agent_context.mcp_tools.entities import get_entities, list_schema_fields
from datahub_agent_context.mcp_tools.lineage import (
    get_lineage,
    get_lineage_paths_between,
)
from datahub_agent_context.mcp_tools.owners import add_owners, remove_owners
from datahub_agent_context.mcp_tools.queries import get_dataset_queries
from datahub_agent_context.mcp_tools.search import search
from datahub_agent_context.mcp_tools.tags import add_tags, remove_tags
from datahub_agent_context.mcp_tools.terms import (
    add_glossary_terms,
    remove_glossary_terms,
)


def create_context_wrapper(func: Callable, client: "DataHubClient") -> Callable:
    """Create a wrapper that sets DataHubGraph context before calling the function.

    This wrapper uses contextvars to set the graph in context for the duration
    of the function call, allowing the tool to retrieve it using get_graph().

    Args:
        func: The tool function that retrieves graph from context
        client: DataHubClient instance whose graph will be set in context

    Returns:
        Wrapped function that sets context before execution
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Set graph in context for this function call
        token = set_graph(client._graph)
        try:
            return func(*args, **kwargs)
        finally:
            # Always reset context, even if function raises
            from datahub_agent_context.context import reset_graph

            reset_graph(token)

    return wrapper


def build_langchain_tools(
    client: "DataHubClient",
    include_mutations: bool = False,
) -> list[BaseTool]:
    """Build LangChain tools with automatic context management.

    Each tool is wrapped to automatically set the DataHubGraph in context
    before execution, allowing tools to retrieve it using get_graph().

    Args:
        client: DataHubClient instance
        include_mutations: Whether to include mutation tools (default: False)

    Returns:
        List of LangChain BaseTool instances

    Example:
        from datahub.sdk.main_client import DataHubClient
        from datahub_agent_context.langchain_tools import build_langchain_tools

        client = DataHubClient(...)
        tools = build_langchain_tools(client, include_mutations=True)

        # Use with LangChain agents - context is managed automatically
        agent = create_react_agent(llm, tools, prompt)
        result = agent.invoke({"input": "search for datasets"})
    """
    tools = []

    # Wrap each tool to manage context automatically
    tools.append(tool(create_context_wrapper(search_documents, client)))
    tools.append(tool(create_context_wrapper(grep_documents, client)))

    tools.append(tool(create_context_wrapper(get_entities, client)))
    tools.append(tool(create_context_wrapper(list_schema_fields, client)))
    tools.append(tool(create_context_wrapper(get_me, client)))
    tools.append(tool(create_context_wrapper(get_lineage, client)))
    tools.append(tool(create_context_wrapper(get_lineage_paths_between, client)))

    tools.append(tool(create_context_wrapper(get_dataset_queries, client)))
    tools.append(tool(create_context_wrapper(search, client)))

    if include_mutations:
        tools.append(tool(create_context_wrapper(update_description, client)))
        tools.append(tool(create_context_wrapper(set_domains, client)))
        tools.append(tool(create_context_wrapper(remove_domains, client)))
        tools.append(tool(create_context_wrapper(add_owners, client)))
        tools.append(tool(create_context_wrapper(remove_owners, client)))
        tools.append(tool(create_context_wrapper(add_structured_properties, client)))
        tools.append(tool(create_context_wrapper(remove_structured_properties, client)))
        tools.append(tool(create_context_wrapper(add_tags, client)))
        tools.append(tool(create_context_wrapper(remove_tags, client)))
        tools.append(tool(create_context_wrapper(add_glossary_terms, client)))
        tools.append(tool(create_context_wrapper(remove_glossary_terms, client)))

    return tools
