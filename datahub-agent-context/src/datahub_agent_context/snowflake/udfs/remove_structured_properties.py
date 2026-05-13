"""REMOVE_STRUCTURED_PROPERTIES UDF generator."""

from datahub_agent_context.snowflake.udfs.base import generate_python_udf_code


def generate_remove_structured_properties_udf() -> str:
    """Generate REMOVE_STRUCTURED_PROPERTIES UDF using datahub-agent-context.

    This UDF wraps datahub_agent_context.mcp_tools.remove_structured_properties()
    to remove structured properties from multiple DataHub entities.

    Parameters:
        property_urns (STRING): JSON array of structured property URNs to remove
        entity_urns (STRING): JSON array of entity URNs to remove properties from

    Returns:
        VARIANT: Dictionary with success status and message

    Example:
        - Remove: REMOVE_STRUCTURED_PROPERTIES('["urn:li:structuredProperty:retentionTime"]', '["urn:li:dataset:(...)"]')
    """
    function_body = """from datahub_agent_context.mcp_tools import remove_structured_properties
import json
try:
    datahub_url = _snowflake.get_generic_secret_string('datahub_url_secret')
    datahub_token = _snowflake.get_generic_secret_string('datahub_token_secret')
    datahub_url = datahub_url.rstrip('/')

    client = DataHubClient(server=datahub_url, token=datahub_token)

    property_urn_list = json.loads(property_urns) if isinstance(property_urns, str) else property_urns
    entity_urn_list = json.loads(entity_urns) if isinstance(entity_urns, str) else entity_urns

    with DataHubContext(client):
        return remove_structured_properties(
            property_urns=property_urn_list,
            entity_urns=entity_urn_list
        )

except Exception as e:
    return {
        'success': False,
        'error': str(e)
    }"""

    return generate_python_udf_code(
        function_name="REMOVE_STRUCTURED_PROPERTIES",
        parameters=[
            ("property_urns", "STRING"),
            ("entity_urns", "STRING"),
        ],
        return_type="VARIANT",
        function_body=function_body,
    )
