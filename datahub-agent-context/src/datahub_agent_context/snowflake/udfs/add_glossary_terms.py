"""ADD_GLOSSARY_TERMS UDF generator."""

from datahub_agent_context.snowflake.udfs.base import generate_python_udf_code


def generate_add_glossary_terms_udf() -> str:
    """Generate ADD_GLOSSARY_TERMS UDF using datahub-agent-context.

    This UDF wraps datahub_agent_context.mcp_tools.add_glossary_terms() to add
    glossary terms to multiple DataHub entities or their columns.

    Parameters:
        term_urns (STRING): JSON array of glossary term URNs
        entity_urns (STRING): JSON array of entity URNs to annotate
        column_paths (STRING): Optional JSON array of column names (use NULL for entity-level)

    Returns:
        VARIANT: Dictionary with success status and message

    Examples:
        - Add terms: ADD_GLOSSARY_TERMS('["urn:li:glossaryTerm:CustomerData"]', '["urn:li:dataset:(...)"]', NULL)
        - Add to columns: ADD_GLOSSARY_TERMS('["urn:li:glossaryTerm:Email"]', '["urn:li:dataset:(...)"]', '["email"]')
    """
    function_body = """from datahub_agent_context.mcp_tools import add_glossary_terms
import json
try:
    datahub_url = _snowflake.get_generic_secret_string('datahub_url_secret')
    datahub_token = _snowflake.get_generic_secret_string('datahub_token_secret')
    datahub_url = datahub_url.rstrip('/')

    client = DataHubClient(server=datahub_url, token=datahub_token)

    term_urn_list = json.loads(term_urns) if isinstance(term_urns, str) else term_urns
    entity_urn_list = json.loads(entity_urns) if isinstance(entity_urns, str) else entity_urns
    column_path_list = json.loads(column_paths) if column_paths and isinstance(column_paths, str) else None

    with DataHubContext(client):
        return add_glossary_terms(
            term_urns=term_urn_list,
            entity_urns=entity_urn_list,
            column_paths=column_path_list
        )

except Exception as e:
    return {
        'success': False,
        'error': str(e)
    }"""

    return generate_python_udf_code(
        function_name="ADD_GLOSSARY_TERMS",
        parameters=[
            ("term_urns", "STRING"),
            ("entity_urns", "STRING"),
            ("column_paths", "STRING"),
        ],
        return_type="VARIANT",
        function_body=function_body,
    )
