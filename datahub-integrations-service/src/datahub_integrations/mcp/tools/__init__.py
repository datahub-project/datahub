"""MCP tools for DataHub integrations."""

from datahub_integrations.mcp.tools.tags import add_tags, remove_tags
from datahub_integrations.mcp.tools.terms import (
    add_glossary_terms,
    remove_glossary_terms,
)

__all__ = ["add_tags", "remove_tags", "add_glossary_terms", "remove_glossary_terms"]
