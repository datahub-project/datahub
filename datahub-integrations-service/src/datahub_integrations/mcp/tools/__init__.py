"""MCP tools for DataHub integrations."""

from datahub_integrations.mcp.tools.descriptions import update_description
from datahub_integrations.mcp.tools.domains import remove_domains, set_domains
from datahub_integrations.mcp.tools.get_me import get_me
from datahub_integrations.mcp.tools.owners import add_owners, remove_owners
from datahub_integrations.mcp.tools.structured_properties import (
    add_structured_properties,
    remove_structured_properties,
)
from datahub_integrations.mcp.tools.tags import add_tags, remove_tags
from datahub_integrations.mcp.tools.terms import (
    add_glossary_terms,
    remove_glossary_terms,
)

# Note: grep_documents and search_documents are not exported here to avoid
# circular imports. Import them directly from tools.documents when needed.
__all__ = [
    "add_tags",
    "remove_tags",
    "add_glossary_terms",
    "remove_glossary_terms",
    "add_owners",
    "remove_owners",
    "get_me",
    "set_domains",
    "remove_domains",
    "update_description",
    "add_structured_properties",
    "remove_structured_properties",
]
