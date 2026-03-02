"""REMOVE_DOMAINS UDF generator."""

from datahub_agent_context.snowflake.udfs.base import generate_python_udf_code


def generate_remove_domains_udf() -> str:
    """Generate REMOVE_DOMAINS UDF using datahub-agent-context.

    This UDF wraps datahub_agent_context.mcp_tools.remove_domains() to remove
    domain assignments from multiple DataHub entities.

    Parameters:
        entity_urns (STRING): JSON array of entity URNs to remove domain from

    Returns:
        VARIANT: Dictionary with success status and message
    """
    function_body = """from datahub_agent_context.mcp_tools import remove_domains
import json
try:
    datahub_url = _snowflake.get_generic_secret_string('datahub_url_secret')
    datahub_token = _snowflake.get_generic_secret_string('datahub_token_secret')
    datahub_url = datahub_url.rstrip('/')

    client = DataHubClient(server=datahub_url, token=datahub_token)

    entity_urn_list = json.loads(entity_urns) if isinstance(entity_urns, str) else entity_urns

    with DataHubContext(client):
        return remove_domains(entity_urns=entity_urn_list)

except Exception as e:
    return {
        'success': False,
        'error': str(e)
    }"""

    return generate_python_udf_code(
        function_name="REMOVE_DOMAINS",
        parameters=[("entity_urns", "STRING")],
        return_type="VARIANT",
        function_body=function_body,
    )
