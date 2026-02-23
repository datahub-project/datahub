"""Search tools for DataHub."""

import json
import logging
import pathlib
from typing import Any, Dict, Literal, Optional

from datahub.sdk.search_client import compile_filters
from datahub.sdk.search_filters import Filter, load_filters
from datahub_agent_context.context import get_graph
from datahub_agent_context.mcp_tools.base import (
    clean_gql_response,
    execute_graphql,
    fetch_global_default_view,
)

logger = logging.getLogger(__name__)

# Load GraphQL queries
_gql_dir = pathlib.Path(__file__).parent / "gql"
search_gql = (_gql_dir / "search.gql").read_text()


def _convert_custom_filter_format(filters_obj: Any) -> Any:
    """
    Convert chatbot's intuitive {"custom": {...}} format to the format expected by _CustomCondition.

    Transforms:
    {"custom": {"field": "urn", "condition": "EQUAL", "values": [...]}}

    Into:
    {"field": "urn", "condition": "EQUAL", "values": [...]}

    This allows the discriminator to correctly identify it as _custom.
    """
    if isinstance(filters_obj, dict):
        # Check if this is a "custom" or "custom_condition" wrapper that needs unwrapping
        if len(filters_obj) == 1 and (
            "custom" in filters_obj or "custom_condition" in filters_obj
        ):
            wrapper_key = "custom" if "custom" in filters_obj else "custom_condition"
            custom_content = filters_obj[wrapper_key]
            # Ensure it has the expected structure for _CustomCondition
            if isinstance(custom_content, dict) and "field" in custom_content:
                return custom_content

        # Recursively process nested filters (for "and", "or", etc.)
        result = {}
        for key, value in filters_obj.items():
            if isinstance(value, (list, dict)):
                result[key] = _convert_custom_filter_format(value)
            else:
                result[key] = value
        return result
    elif isinstance(filters_obj, list):
        # Process list of filters
        return [_convert_custom_filter_format(item) for item in filters_obj]
    else:
        # Return primitive values unchanged
        return filters_obj


def search(
    query: str = "*",
    filters: Optional[Filter | str] = None,
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
      • /q user+transaction → requires both terms (better for field names with _ or punctuation)
      • /q point+sale+app → requires all terms (works with point_of_sale_app_usage)
      • /q wizard OR pet → entities containing either term
      • /q revenue* → wildcard matching (revenue_2023, revenue_2024, revenue_monthly, etc.)
      • /q tag:PII → search by tag name
      • /q "exact table name" → exact phrase matching (use sparingly)
      • /q (sales OR revenue) AND quarterly → complex boolean combinations
    - Fast and precise for exact matching, technical terms, and complex queries
    - Best for: entity names, identifiers, column names, or any search needing boolean logic

    PAGINATION:
    - num_results: Number of results to return per page (max: 50)
    - offset: Starting position in results (default: 0)
    - Examples:
      • First page: offset=0, num_results=10
      • Second page: offset=10, num_results=10
      • Third page: offset=20, num_results=10

    FACET EXPLORATION - Discover metadata without returning results:
    - Set num_results=0 to get ONLY facets (no search results)
    - Facets show ALL tags, glossaryTerms, platforms, domains used in the catalog
    - Example: search(query="*", filters={"entity_type": ["DATASET"]}, num_results=0)
      → Returns facets showing all tags/glossaryTerms applied to datasets
    - Use this to discover what metadata exists before doing filtered searches

    TYPICAL WORKFLOW:
    1. Facet exploration: search(query="*", filters={"entity_type": ["DATASET"]}, num_results=0)
       → Examine tags/glossaryTerms facets to see what metadata exists
    2. Filtered search: search(query="*", filters={"tag": ["urn:li:tag:pii"]}, num_results=30)
       → Get entities with specific tag using URN from step 1
    3. Get details: Use get_entities() on specific results

    Here are some example filters:
    - All Looker assets
    ```
    {"platform": ["looker"]}
    ```
    - Production environment warehouse assets
    ```
    {
      "and": [
        {"env": ["PROD"]},
        {"platform": ["snowflake", "bigquery", "redshift"]}
      ]
    }
    ```
    - Filter by domain (MUST use full URN format)
    ```
    {"domain": ["urn:li:domain:marketing"]}
    {"domain": ["urn:li:domain:9f8e7d6c-5b4a-3928-1765-432109876543", "urn:li:domain:7c6b5a49-3827-1654-9032-8f7e6d5c4b3a"]}
    ```
    IMPORTANT: Domain filters require full URN format starting with "urn:li:domain:",
    NOT short names like "marketing" or "customer". Domain URNs can be readable names
    or GUIDs. Always search with {"entity_type": ["domain"]}
    to find valid domain URNs first, then use the exact URN from the results.

    SUPPORTED FILTER TYPES (only these will work):
    - entity_type: ["dataset"], ["dashboard", "chart"], ["corp_user"], ["corp_group"]
    - entity_subtype: ["Table"], ["View", "Model"]
    - platform: ["snowflake"], ["looker", "tableau"]
    - domain: ["urn:li:domain:marketing"] (full URN required)
    - container: ["urn:li:container:..."] (full URN required)
    - tag: ["urn:li:tag:PII"] (full tag URN required)
    - glossary_term: ["urn:li:glossaryTerm:uuid"] (full term URN required)
    - owner: ["urn:li:corpuser:alice", "urn:li:corpGroup:marketing"] (full user or group URN required)
    - custom: {"field": "fieldName", "condition": "EQUAL", "values": [...]}
    - status: ["NOT_SOFT_DELETED"] (for non-deleted entities)
    - env: ["PROD"], ["DEV", "STAGING"] (Should not use unless explicitly requested)
    - and: [filter1, filter2] (combines multiple filters)
    - or: [filter1, filter2] (matches any filter)
    - not: {"entity_type": ["dataset"]} (excludes matches)

    CRITICAL: Use only ONE discriminator key per filter object. Never mix
    entity_type with custom, domain, etc. at the same level. Use "and" or "or" to combine.

    SEARCH STRATEGY EXAMPLES:
    - /q customer+behavior → finds tables with both terms (works with customer_behavior fields)
    - /q customer OR user → finds tables with either term
    - /q (financial OR revenue) AND metrics → complex boolean logic

    SORTING - Order results by specific fields:
    - sort_by: Field name to sort by (optional)
    - sort_order: "desc" (default) or "asc"

    Note: If sort_by is not provided, search results use default ranking by relevance and
    importance. When using sort_by, results are strictly ordered by that field.

    Args:
        query: Search query string (use /q prefix for structured queries)
        filters: Optional filter object or JSON string
        num_results: Number of results to return (max 50)
        sort_by: Optional field name to sort by
        sort_order: Sort order ("asc" or "desc")
        offset: Starting position for pagination

    Returns:
        Dictionary with search results, facets, and metadata

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = search(query="/q users", filters={"entity_type": ["dataset"]})
    """
    graph = get_graph()
    # Cap num_results at 50 to prevent excessive requests
    num_results = min(num_results, 50)

    # Handle stringified JSON filters or dict filters
    if isinstance(filters, str):
        # Parse JSON first to allow preprocessing
        filters_dict = json.loads(filters)

        # Convert "custom" wrapper to direct _custom format for compatibility
        filters_dict = _convert_custom_filter_format(filters_dict)

        filters = load_filters(filters_dict)
    elif isinstance(filters, dict):
        # Convert dict to Filter object
        filters_dict = _convert_custom_filter_format(filters)
        filters = load_filters(filters_dict)

    types, compiled_filters = compile_filters(filters)

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
