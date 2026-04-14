"""GREP_DOCUMENTS UDF generator."""

from datahub_agent_context.snowflake.udfs.base import generate_python_udf_code


def generate_grep_documents_udf() -> str:
    """Generate GREP_DOCUMENTS UDF using datahub-agent-context.

    This UDF wraps datahub_agent_context.mcp_tools.grep_documents() to search within
    document content using regex patterns (similar to ripgrep/grep).

    Use SEARCH_DOCUMENTS first to find relevant document URNs, then use this tool
    to search within their content.

    Parameters:
        urns (STRING): JSON array of document URNs to search within (e.g., '["urn:li:document:doc1"]')
        pattern (STRING): Regex pattern to search for (e.g., 'kubernetes', '(?i)deploy.*production')
        context_chars (NUMBER): Characters to show before/after matches (default: 200)
        max_matches_per_doc (NUMBER): Maximum matches per document (default: 5)

    Returns:
        VARIANT: Dictionary with:
                - results: List of documents with matching excerpts
                - total_matches: Total matches across all documents
                - documents_with_matches: Number of documents containing matches

    Examples:
        - Find kubectl commands: GREP_DOCUMENTS('["urn:li:document:runbook1"]', 'kubectl apply', 300, 5)
        - Case insensitive: GREP_DOCUMENTS('["urn:li:document:doc1"]', '(?i)error|exception', 200, 10)
    """
    function_body = """from datahub_agent_context.mcp_tools import grep_documents
import json
try:
    datahub_url = _snowflake.get_generic_secret_string('datahub_url_secret')
    datahub_token = _snowflake.get_generic_secret_string('datahub_token_secret')
    datahub_url = datahub_url.rstrip('/')

    client = DataHubClient(server=datahub_url, token=datahub_token)

    urn_list = json.loads(urns) if isinstance(urns, str) else urns

    with DataHubContext(client):
        return grep_documents(
            urns=urn_list,
            pattern=pattern,
            context_chars=int(context_chars) if context_chars else 200,
            max_matches_per_doc=int(max_matches_per_doc) if max_matches_per_doc else 5
        )

except Exception as e:
    return {
        'success': False,
        'error': str(e),
        'urns': urns,
        'pattern': pattern
    }"""

    return generate_python_udf_code(
        function_name="GREP_DOCUMENTS",
        parameters=[
            ("urns", "STRING"),
            ("pattern", "STRING"),
            ("context_chars", "NUMBER"),
            ("max_matches_per_doc", "NUMBER"),
        ],
        return_type="VARIANT",
        function_body=function_body,
    )
