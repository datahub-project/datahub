"""UPDATE_DESCRIPTION UDF generator."""

from datahub_agent_context.snowflake.udfs.base import generate_python_udf_code


def generate_update_description_udf() -> str:
    """Generate UPDATE_DESCRIPTION UDF using datahub-agent-context.

    This UDF wraps datahub_agent_context.mcp_tools.update_description() to update
    descriptions for DataHub entities or their columns.

    Parameters:
        entity_urn (STRING): Entity URN to update
        operation (STRING): Operation type - 'replace', 'append', or 'remove'
        description (STRING): Description text (not needed for 'remove')
        column_path (STRING): Optional column name (use NULL for entity-level)

    Returns:
        VARIANT: Dictionary with success status and message

    Examples:
        - Replace: UPDATE_DESCRIPTION(urn, 'replace', 'New description', NULL)
        - Append: UPDATE_DESCRIPTION(urn, 'append', ' (PII)', 'email')
        - Remove: UPDATE_DESCRIPTION(urn, 'remove', NULL, 'old_field')
    """
    function_body = """from datahub_agent_context.mcp_tools import update_description
try:
    datahub_url = _snowflake.get_generic_secret_string('datahub_url_secret')
    datahub_token = _snowflake.get_generic_secret_string('datahub_token_secret')
    datahub_url = datahub_url.rstrip('/')

    client = DataHubClient(server=datahub_url, token=datahub_token)

    with DataHubContext(client):
        return update_description(
            entity_urn=entity_urn,
            operation=operation,
            description=description if description else None,
            column_path=column_path if column_path else None
        )

except Exception as e:
    return {
        'success': False,
        'error': str(e)
    }"""

    return generate_python_udf_code(
        function_name="UPDATE_DESCRIPTION",
        parameters=[
            ("entity_urn", "STRING"),
            ("operation", "STRING"),
            ("description", "STRING"),
            ("column_path", "STRING"),
        ],
        return_type="VARIANT",
        function_body=function_body,
    )
