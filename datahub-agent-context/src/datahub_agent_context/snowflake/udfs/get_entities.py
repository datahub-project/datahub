"""GET_ENTITIES UDF generator."""

from datahub_agent_context.snowflake.udfs.base import generate_python_udf_code


def generate_get_entities_udf() -> str:
    """Generate GET_ENTITIES UDF using datahub-agent-context.

    This UDF wraps datahub_agent_context.mcp_tools.get_entities() to retrieve
    detailed information about entities by their DataHub URNs from Snowflake.

    The underlying function accepts arrays of URNs for efficient batch retrieval,
    but this UDF is simplified to accept a single URN string.

    Parameters:
        entity_urn (STRING): Entity URN (e.g., "urn:li:dataset:(...)")

    Returns:
        VARIANT: Dictionary with entity details including schema metadata, ownership,
                tags, glossary terms, and other metadata aspects
    """
    function_body = """from datahub_agent_context.mcp_tools import get_entities
try:
    datahub_url = _snowflake.get_generic_secret_string('datahub_url_secret')
    datahub_token = _snowflake.get_generic_secret_string('datahub_token_secret')
    datahub_url = datahub_url.rstrip('/')

    client = DataHubClient(server=datahub_url, token=datahub_token)

    with DataHubContext(client):
        return get_entities([entity_urn])

except Exception as e:
    return {
        'success': False,
        'error': str(e),
        'urn': entity_urn
    }"""

    return generate_python_udf_code(
        function_name="GET_ENTITIES",
        parameters=[("entity_urn", "STRING")],
        return_type="VARIANT",
        function_body=function_body,
    )
