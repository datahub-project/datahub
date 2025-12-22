"""MCP tools for DataHub integrations."""

from datahub_integrations.mcp.tools.get_me import get_me
from datahub_integrations.mcp.tools.owners import add_owners, remove_owners
from datahub_integrations.mcp.tools.tags import add_tags, remove_tags
from datahub_integrations.mcp.tools.terms import (
    add_glossary_terms,
    remove_glossary_terms,
)

__all__ = [
    "add_tags",
    "remove_tags",
    "add_glossary_terms",
    "remove_glossary_terms",
    "add_owners",
    "remove_owners",
    "get_me",
]
