"""SEARCH_DATAHUB UDF generator."""

from datahub_agent_context.snowflake.udfs.base import generate_python_udf_code


def generate_search_datahub_udf() -> str:
    """Generate SEARCH_DATAHUB UDF using datahub-agent-context.

    This UDF wraps datahub_agent_context.mcp_tools.search() to enable searching
    across DataHub entities using structured full-text search from Snowflake.

    Search Syntax:
        - Structured full-text search - always start queries with /q
        - Use + operator for AND (handles punctuation better than quotes)
        - Supports boolean logic: AND (default), OR, NOT, parentheses, field searches
        - Examples:
          • /q user+transaction → requires both terms
          • /q wizard OR pet → entities containing either term
          • /q revenue* → wildcard matching
          • /q tag:PII → search by tag name

    Parameters:
        search_query (STRING): Search query string (use /q prefix for structured queries)
        entity_type (STRING): Optional entity type filter as JSON array string
                             (e.g., '["dataset"]', '["dashboard", "chart"]')
                             Use NULL to search across all entity types

    Returns:
        VARIANT: Dictionary with search results, facets, and metadata
    """
    function_body = """from datahub_agent_context.mcp_tools import search
import json
try:
    datahub_url = _snowflake.get_generic_secret_string('datahub_url_secret')
    datahub_token = _snowflake.get_generic_secret_string('datahub_token_secret')
    datahub_url = datahub_url.rstrip('/')

    client = DataHubClient(server=datahub_url, token=datahub_token)

    # Parse entity_type filter if provided
    filters = {}
    if entity_type:
        try:
            entity_type_list = json.loads(entity_type) if isinstance(entity_type, str) else entity_type
            filters["entity_type"] = entity_type_list
        except json.JSONDecodeError:
            # If not valid JSON, treat as single entity type
            filters["entity_type"] = [entity_type]

    with DataHubContext(client):
        return search(
            query=search_query,
            filters=filters if filters else None,
            num_results=10
        )

except Exception as e:
    return {
        'success': False,
        'error': str(e),
        'query': search_query
    }"""

    return generate_python_udf_code(
        function_name="SEARCH_DATAHUB",
        parameters=[("search_query", "STRING"), ("entity_type", "STRING")],
        return_type="VARIANT",
        function_body=function_body,
    )
