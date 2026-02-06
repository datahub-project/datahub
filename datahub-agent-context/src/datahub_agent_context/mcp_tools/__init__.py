"""MCP tools for interacting with DataHub metadata."""

from datahub_agent_context.mcp_tools.descriptions import update_description
from datahub_agent_context.mcp_tools.documents import grep_documents, search_documents
from datahub_agent_context.mcp_tools.domains import remove_domains, set_domains
from datahub_agent_context.mcp_tools.entities import get_entities, list_schema_fields
from datahub_agent_context.mcp_tools.get_me import get_me
from datahub_agent_context.mcp_tools.lineage import (
    get_lineage,
    get_lineage_paths_between,
)
from datahub_agent_context.mcp_tools.owners import add_owners, remove_owners
from datahub_agent_context.mcp_tools.queries import get_dataset_queries
from datahub_agent_context.mcp_tools.search import search
from datahub_agent_context.mcp_tools.structured_properties import (
    add_structured_properties,
    remove_structured_properties,
)
from datahub_agent_context.mcp_tools.tags import add_tags, remove_tags
from datahub_agent_context.mcp_tools.terms import (
    add_glossary_terms,
    remove_glossary_terms,
)

__all__ = [
    "search",
    "get_entities",
    "list_schema_fields",
    "get_lineage",
    "get_lineage_paths_between",
    "get_dataset_queries",
    "search_documents",
    "grep_documents",
    "add_tags",
    "remove_tags",
    "update_description",
    "set_domains",
    "remove_domains",
    "add_owners",
    "remove_owners",
    "add_glossary_terms",
    "remove_glossary_terms",
    "add_structured_properties",
    "remove_structured_properties",
    "get_me",
]
