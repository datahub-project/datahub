"""REMOVE_TAGS UDF generator."""

from datahub_agent_context.snowflake.udfs.base import generate_python_udf_code


def generate_remove_tags_udf() -> str:
    """Generate REMOVE_TAGS UDF using datahub-agent-context.

    This UDF wraps datahub_agent_context.mcp_tools.remove_tags() to remove tags
    from multiple DataHub entities or their columns.

    Parameters:
        tag_urns (STRING): JSON array of tag URNs to remove
        entity_urns (STRING): JSON array of entity URNs to untag
        column_paths (STRING): Optional JSON array of column names (use NULL for entity-level)

    Returns:
        VARIANT: Dictionary with success status and message
    """
    function_body = """from datahub_agent_context.mcp_tools import remove_tags
import json
try:
    datahub_url = _snowflake.get_generic_secret_string('datahub_url_secret')
    datahub_token = _snowflake.get_generic_secret_string('datahub_token_secret')
    datahub_url = datahub_url.rstrip('/')

    client = DataHubClient(server=datahub_url, token=datahub_token)

    tag_urn_list = json.loads(tag_urns) if isinstance(tag_urns, str) else tag_urns
    entity_urn_list = json.loads(entity_urns) if isinstance(entity_urns, str) else entity_urns
    column_path_list = json.loads(column_paths) if column_paths and isinstance(column_paths, str) else None

    with DataHubContext(client):
        return remove_tags(
            tag_urns=tag_urn_list,
            entity_urns=entity_urn_list,
            column_paths=column_path_list
        )

except Exception as e:
    return {
        'success': False,
        'error': str(e)
    }"""

    return generate_python_udf_code(
        function_name="REMOVE_TAGS",
        parameters=[
            ("tag_urns", "STRING"),
            ("entity_urns", "STRING"),
            ("column_paths", "STRING"),
        ],
        return_type="VARIANT",
        function_body=function_body,
    )
