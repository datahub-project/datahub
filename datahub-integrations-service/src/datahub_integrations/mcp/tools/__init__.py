"""MCP tools for DataHub integrations."""

from .descriptions import update_description
from .domains import remove_domains, set_domains
from .get_me import get_me
from .owners import add_owners, remove_owners
from .structured_properties import (
    add_structured_properties,
    remove_structured_properties,
)
from .tags import add_tags, remove_tags
from .terms import (
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
