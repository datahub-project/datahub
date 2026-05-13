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
        filter (STRING): Optional SQL-like filter string, e.g.:
                         'entity_type = dataset'
                         'entity_type = dataset AND platform = snowflake'
                         'platform IN (snowflake, bigquery) AND env = PROD'
                         'tag = urn:li:tag:PII'
                         Pass NULL to search across all entity types and platforms.

    Returns:
        VARIANT: Dictionary with search results, facets, and metadata

    Examples:
        - Search all entities: SEARCH_DATAHUB('/q revenue', NULL)
        - Search datasets only: SEARCH_DATAHUB('*', 'entity_type = dataset')
        - Search Snowflake datasets: SEARCH_DATAHUB('*', 'entity_type = dataset AND platform = snowflake')
        - Search by tag: SEARCH_DATAHUB('*', 'tag = urn:li:tag:PII')
        - Search prod assets: SEARCH_DATAHUB('/q sales', 'platform IN (snowflake, bigquery) AND env = PROD')
    """
    function_body = """from datahub_agent_context.mcp_tools import search
try:
    datahub_url = _snowflake.get_generic_secret_string('datahub_url_secret')
    datahub_token = _snowflake.get_generic_secret_string('datahub_token_secret')
    datahub_url = datahub_url.rstrip('/')

    client = DataHubClient(server=datahub_url, token=datahub_token)

    with DataHubContext(client):
        return search(
            query=search_query,
            filter=filter if filter else None,
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
        parameters=[("search_query", "STRING"), ("filter", "STRING")],
        return_type="VARIANT",
        function_body=function_body,
    )
