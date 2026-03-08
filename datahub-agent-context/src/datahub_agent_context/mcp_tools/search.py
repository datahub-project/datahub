"""Search tools for DataHub."""

import logging
import pathlib
import textwrap
from typing import Any, Dict, Literal, Optional

from datahub.sdk.search_client import compile_filters
from datahub_agent_context.context import get_graph
from datahub_agent_context.mcp_tools.base import (
    clean_gql_response,
    execute_graphql,
    fetch_global_default_view,
)
from datahub_agent_context.mcp_tools.search_filter_parser import (
    FILTER_DOCS,
    parse_filter_string,
)

logger = logging.getLogger(__name__)

# Load GraphQL queries
_gql_dir = pathlib.Path(__file__).parent / "gql"
search_gql = (_gql_dir / "search.gql").read_text()


def search(
    query: str = "*",
    filter: Optional[str] = None,
    num_results: int = 10,
    sort_by: Optional[str] = None,
    sort_order: Optional[Literal["asc", "desc"]] = "desc",
    offset: int = 0,
) -> dict:
    """Search across DataHub entities using structured full-text search.
    Results are ordered by relevance and importance - examine top results first.

    SEARCH SYNTAX:
    - Structured full-text search - **always start queries with /q**
    - **Recommended: Use + operator for AND** (handles punctuation better than quotes)
    - Supports full boolean logic: AND (default), OR, NOT, parentheses, field searches
    - Examples:
      * /q user+transaction -> requires both terms (better for field names with _ or punctuation)
      * /q point+sale+app -> requires all terms (works with point_of_sale_app_usage)
      * /q wizard OR pet -> entities containing either term
      * /q revenue* -> wildcard matching (revenue_2023, revenue_2024, revenue_monthly, etc.)
      * /q tag:PII -> search by tag name
      * /q "exact table name" -> exact phrase matching (use sparingly)
      * /q (sales OR revenue) AND quarterly -> complex boolean combinations
    - Fast and precise for exact matching, technical terms, and complex queries
    - Best for: entity names, identifiers, column names, or any search needing boolean logic

    PAGINATION:
    - num_results: Number of results to return per page (max: 50)
    - offset: Starting position in results (default: 0)
    - Examples:
      * First page: offset=0, num_results=10
      * Second page: offset=10, num_results=10
      * Third page: offset=20, num_results=10

    FACET EXPLORATION - Discover metadata without returning results:
    - Set num_results=0 to get ONLY facets (no search results)
    - Facets show ALL tags, glossaryTerms, platforms, domains used in the catalog
    - Example: search(query="*", filter="entity_type = dataset", num_results=0)
      -> Returns facets showing all tags/glossaryTerms applied to datasets
    - Use this to discover what metadata exists before doing filtered searches

    TYPICAL WORKFLOW:
    1. Facet exploration: search(query="*", filter="entity_type = dataset", num_results=0)
       -> Examine tags/glossaryTerms facets to see what metadata exists
    2. Filtered search: search(query="*", filter="tag = urn:li:tag:pii", num_results=30)
       -> Get entities with specific tag using URN from step 1
    3. Get details: Use get_entities() on specific results

    {FILTER_DOCS}

    SEARCH STRATEGY EXAMPLES:
    - /q customer+behavior -> finds tables with both terms (works with customer_behavior fields)
    - /q customer OR user -> finds tables with either term
    - /q (financial OR revenue) AND metrics -> complex boolean logic

    SORTING - Order results by specific fields:
    - sort_by: Field name to sort by (optional)
    - sort_order: "desc" (default) or "asc"

    Available sort fields:
    - lastOperationTime: Last modified timestamp in source system

    Sorting examples:
    - Most recently updated:
      search(query="*", filter="entity_type = dataset", sort_by="lastOperationTime", sort_order="desc", num_results=10)

    Note: If sort_by is not provided, search results use default ranking by relevance and
    importance. When using sort_by, results are strictly ordered by that field.

    Args:
        query: Search query string (use /q prefix for structured queries)
        filter: Optional SQL-like filter string
        num_results: Number of results to return (max 50)
        sort_by: Optional field name to sort by
        sort_order: Sort order ("asc" or "desc")
        offset: Starting position for pagination

    Returns:
        Dictionary with search results, facets, and metadata

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = search(query="/q users", filter="entity_type = dataset")
    """
    graph = get_graph()
    # Cap num_results at 50 to prevent excessive requests
    num_results = min(num_results, 50)

    parsed_filter = (
        parse_filter_string(filter.strip()) if isinstance(filter, str) else filter
    )

    types, compiled_filters = compile_filters(parsed_filter)

    # Fetch and apply default view (returns None if disabled or not configured)
    view_urn = fetch_global_default_view(graph)
    if view_urn:
        logger.debug(f"Applying default view: {view_urn}")
    else:
        logger.debug("No default view to apply")

    variables: Dict[str, Any] = {
        "query": query,
        "types": types,
        "orFilters": compiled_filters,
        "count": max(num_results, 1),  # 0 is not a valid value for count.
        "start": offset,
        "viewUrn": view_urn,  # Will be None if disabled or not set
    }

    # Add sorting if requested
    if sort_by is not None:
        sort_order_enum = "ASCENDING" if sort_order == "asc" else "DESCENDING"
        variables["sortInput"] = {
            "sortCriteria": [{"field": sort_by, "sortOrder": sort_order_enum}]
        }

    # Use keyword search
    response = execute_graphql(
        graph,
        query=search_gql,
        variables=variables,
        operation_name="search",
    )["searchAcrossEntities"]

    if num_results == 0 and isinstance(response, dict):
        # Hack to support num_results=0 without support for it in the backend.
        response.pop("searchResults", None)
        response.pop("count", None)

    return clean_gql_response(response)


search.__doc__ = (search.__doc__ or "").format(
    FILTER_DOCS=textwrap.indent(FILTER_DOCS, "    ")
)
