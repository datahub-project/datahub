"""ADD_OWNERS UDF generator."""

from datahub_agent_context.snowflake.udfs.base import generate_python_udf_code


def generate_add_owners_udf() -> str:
    """Generate ADD_OWNERS UDF using datahub-agent-context.

    This UDF wraps datahub_agent_context.mcp_tools.add_owners() to add owners
    to multiple DataHub entities.

    Parameters:
        owner_urns (STRING): JSON array of owner URNs (CorpUser or CorpGroup)
        entity_urns (STRING): JSON array of entity URNs to assign ownership
        ownership_type_urn (STRING): Optional ownership type URN (use NULL for default)

    Returns:
        VARIANT: Dictionary with success status and message

    Example:
        - Add owners: ADD_OWNERS('["urn:li:corpuser:john"]', '["urn:li:dataset:(...)"]', NULL)
    """
    function_body = """from datahub_agent_context.mcp_tools import add_owners
import json
try:
    datahub_url = _snowflake.get_generic_secret_string('datahub_url_secret')
    datahub_token = _snowflake.get_generic_secret_string('datahub_token_secret')
    datahub_url = datahub_url.rstrip('/')

    client = DataHubClient(server=datahub_url, token=datahub_token)

    owner_urn_list = json.loads(owner_urns) if isinstance(owner_urns, str) else owner_urns
    entity_urn_list = json.loads(entity_urns) if isinstance(entity_urns, str) else entity_urns

    with DataHubContext(client):
        return add_owners(
            owner_urns=owner_urn_list,
            entity_urns=entity_urn_list,
            ownership_type_urn=ownership_type_urn if ownership_type_urn else None
        )

except Exception as e:
    return {
        'success': False,
        'error': str(e)
    }"""

    return generate_python_udf_code(
        function_name="ADD_OWNERS",
        parameters=[
            ("owner_urns", "STRING"),
            ("entity_urns", "STRING"),
            ("ownership_type_urn", "STRING"),
        ],
        return_type="VARIANT",
        function_body=function_body,
    )
