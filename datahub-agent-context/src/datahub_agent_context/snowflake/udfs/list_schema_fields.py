"""LIST_SCHEMA_FIELDS UDF generator."""

from datahub_agent_context.snowflake.udfs.base import generate_python_udf_code


def generate_list_schema_fields_udf() -> str:
    """Generate LIST_SCHEMA_FIELDS UDF using datahub-agent-context.

    This UDF wraps datahub_agent_context.mcp_tools.list_schema_fields() to list
    schema fields for a dataset with optional keyword filtering and pagination.

    Useful when schema fields were truncated in search results and you need to
    explore specific columns. Supports pagination for large schemas.

    Parameters:
        dataset_urn (STRING): Dataset URN
        keywords (STRING): Optional keywords to filter schema fields (OR matching).
                          Can be a JSON array string or a single keyword.
                          - Single string: Treated as one keyword for exact field name/phrase
                          - JSON array: Multiple keywords, matches any (OR logic)
                          - Empty/null: Returns all fields in priority order
                          Matches against fieldPath, description, label, tags, and glossary terms.
        limit (NUMBER): Maximum number of fields to return (default: 100)

    Returns:
        VARIANT: Dictionary with:
                - urn: The dataset URN
                - fields: List of schema fields (paginated)
                - totalFields: Total number of fields in the schema
                - returned: Number of fields actually returned
                - remainingCount: Number of fields not included
                - matchingCount: Number of fields that matched keywords (if provided)

    Examples:
        - Single keyword: LIST_SCHEMA_FIELDS(urn, 'user_email', 100)
        - Multiple keywords: LIST_SCHEMA_FIELDS(urn, '["email", "user"]', 100)
        - All fields: LIST_SCHEMA_FIELDS(urn, NULL, 100)
    """
    function_body = """from datahub_agent_context.mcp_tools import list_schema_fields
import json
try:
    datahub_url = _snowflake.get_generic_secret_string('datahub_url_secret')
    datahub_token = _snowflake.get_generic_secret_string('datahub_token_secret')
    datahub_url = datahub_url.rstrip('/')

    client = DataHubClient(server=datahub_url, token=datahub_token)

    keyword_list = None
    if keywords:
        try:
            keyword_list = json.loads(keywords) if isinstance(keywords, str) else keywords
        except json.JSONDecodeError:
            keyword_list = [keywords]

    with DataHubContext(client):
        return list_schema_fields(
            urn=dataset_urn,
            keywords=keyword_list,
            limit=limit
        )

except Exception as e:
    return {
        'success': False,
        'error': str(e),
        'urn': dataset_urn
    }"""

    return generate_python_udf_code(
        function_name="LIST_SCHEMA_FIELDS",
        parameters=[
            ("dataset_urn", "STRING"),
            ("keywords", "STRING"),
            ("limit", "NUMBER"),
        ],
        return_type="VARIANT",
        function_body=function_body,
    )
