"""SEARCH_DOCUMENTS UDF generator."""

from datahub_agent_context.snowflake.udfs.base import generate_python_udf_code


def generate_search_documents_udf() -> str:
    """Generate SEARCH_DOCUMENTS UDF using datahub-agent-context.

    This UDF wraps datahub_agent_context.mcp_tools.search_documents() to search for
    documents stored in DataHub (runbooks, FAQs, knowledge articles from Notion,
    Confluence, etc.).

    Returns document metadata without content. Use GET_ENTITIES with document URN
    to retrieve full content.

    Parameters:
        search_query (STRING): Search query string (use /q prefix for structured queries)
        num_results (NUMBER): Number of results to return (max 50, default: 10)

    Returns:
        VARIANT: Dictionary with search results and facets showing document metadata

    Examples:
        - Find deployment docs: SEARCH_DOCUMENTS('deployment', 10)
        - Structured search: SEARCH_DOCUMENTS('/q kubernetes OR k8s', 20)
    """
    function_body = """from datahub_agent_context.mcp_tools import search_documents
try:
    datahub_url = _snowflake.get_generic_secret_string('datahub_url_secret')
    datahub_token = _snowflake.get_generic_secret_string('datahub_token_secret')
    datahub_url = datahub_url.rstrip('/')

    client = DataHubClient(server=datahub_url, token=datahub_token)

    with DataHubContext(client):
        return search_documents(
            query=search_query,
            num_results=int(num_results) if num_results else 10
        )

except Exception as e:
    return {
        'success': False,
        'error': str(e),
        'query': search_query
    }"""

    return generate_python_udf_code(
        function_name="SEARCH_DOCUMENTS",
        parameters=[
            ("search_query", "STRING"),
            ("num_results", "NUMBER"),
        ],
        return_type="VARIANT",
        function_body=function_body,
    )
