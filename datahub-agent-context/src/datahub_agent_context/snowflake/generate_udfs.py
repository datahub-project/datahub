"""
Generate Snowflake UDFs that use datahub-agent-context wrapper methods.

This module generates Python UDF code that uses the datahub-agent-context package
to interact with DataHub, instead of making direct HTTP API calls.
"""

import logging
from pathlib import Path

import click

from datahub_agent_context.snowflake.udfs.add_glossary_terms import (
    generate_add_glossary_terms_udf,
)
from datahub_agent_context.snowflake.udfs.add_owners import generate_add_owners_udf
from datahub_agent_context.snowflake.udfs.add_structured_properties import (
    generate_add_structured_properties_udf,
)
from datahub_agent_context.snowflake.udfs.add_tags import generate_add_tags_udf

# Cloud-only UDF generators
from datahub_agent_context.snowflake.udfs.ask_datahub_chat import (
    generate_ask_datahub_chat_udf,
)
from datahub_agent_context.snowflake.udfs.get_datahub_chat import (
    generate_get_datahub_chat_udf,
)
from datahub_agent_context.snowflake.udfs.get_dataset_assertions import (
    generate_get_dataset_assertions_udf,
)
from datahub_agent_context.snowflake.udfs.get_dataset_queries import (
    generate_get_dataset_queries_udf,
)
from datahub_agent_context.snowflake.udfs.get_entities import generate_get_entities_udf
from datahub_agent_context.snowflake.udfs.get_lineage import generate_get_lineage_udf
from datahub_agent_context.snowflake.udfs.get_lineage_paths_between import (
    generate_get_lineage_paths_between_udf,
)
from datahub_agent_context.snowflake.udfs.get_me import generate_get_me_udf
from datahub_agent_context.snowflake.udfs.grep_documents import (
    generate_grep_documents_udf,
)
from datahub_agent_context.snowflake.udfs.list_schema_fields import (
    generate_list_schema_fields_udf,
)
from datahub_agent_context.snowflake.udfs.remove_domains import (
    generate_remove_domains_udf,
)
from datahub_agent_context.snowflake.udfs.remove_glossary_terms import (
    generate_remove_glossary_terms_udf,
)
from datahub_agent_context.snowflake.udfs.remove_owners import (
    generate_remove_owners_udf,
)
from datahub_agent_context.snowflake.udfs.remove_structured_properties import (
    generate_remove_structured_properties_udf,
)
from datahub_agent_context.snowflake.udfs.remove_tags import generate_remove_tags_udf
from datahub_agent_context.snowflake.udfs.search_datahub import (
    generate_search_datahub_udf,
)
from datahub_agent_context.snowflake.udfs.search_documents import (
    generate_search_documents_udf,
)
from datahub_agent_context.snowflake.udfs.set_domains import generate_set_domains_udf
from datahub_agent_context.snowflake.udfs.update_description import (
    generate_update_description_udf,
)

logger = logging.getLogger(__name__)


def extract_function_signature(udf_sql: str) -> str:
    """Extract function parameter signature from UDF SQL.

    Args:
        udf_sql: The SQL CREATE FUNCTION statement

    Returns:
        String of Snowflake parameter types (e.g., "STRING, NUMBER")
        Empty string if function has no parameters
    """
    import re

    # Match the function parameters between parentheses
    # The SQL format is: CREATE OR REPLACE FUNCTION name(params) RETURNS ...
    match = re.search(r"FUNCTION\s+\w+\s*\((.*?)\)\s*RETURNS", udf_sql, re.DOTALL)
    if not match:
        return ""

    params_str = match.group(1).strip()
    if not params_str:
        return ""

    # Extract just the types (STRING, NUMBER, etc.)
    # Parameter format is: param_name TYPE
    param_types = []
    for param in params_str.split(","):
        param = param.strip()
        if param:
            # Split on whitespace and take the last part (the type)
            parts = param.split()
            if len(parts) >= 2:
                param_types.append(parts[-1])

    return ", ".join(param_types) if param_types else ""


def generate_all_udfs(
    include_mutations: bool = True,
    include_cloud: bool = False,
) -> dict[str, str]:
    """Generate all DataHub UDFs from datahub-agent-context tools.

    Returns tools from datahub-agent-context as Snowflake UDFs, including
    both read operations (search, get_entities, etc.) and write operations (add_tags,
    update_description, etc.).

    Write operations enable automated governance workflows from Snowflake, such as:
    - Tagging datasets based on query analysis
    - Enriching metadata with descriptions and owners
    - Bulk operations on multiple entities from SQL

    Args:
        include_mutations: Whether to include mutation/write tools (default: True)
        include_cloud: Whether to include Cloud-only tools like Ask DataHub (default: False)

    Returns:
        Dictionary mapping function names to their SQL definitions
    """
    udfs = {
        # Core search and entity tools (read-only)
        "SEARCH_DATAHUB": generate_search_datahub_udf(),
        "GET_ENTITIES": generate_get_entities_udf(),
        "LIST_SCHEMA_FIELDS": generate_list_schema_fields_udf(),
        # Lineage tools (read-only)
        "GET_LINEAGE": generate_get_lineage_udf(),
        "GET_LINEAGE_PATHS_BETWEEN": generate_get_lineage_paths_between_udf(),
        # Query analysis tools (read-only)
        "GET_DATASET_QUERIES": generate_get_dataset_queries_udf(),
        # Data quality tools (read-only)
        "GET_DATASET_ASSERTIONS": generate_get_dataset_assertions_udf(),
        # Document search tools (read-only)
        "SEARCH_DOCUMENTS": generate_search_documents_udf(),
        "GREP_DOCUMENTS": generate_grep_documents_udf(),
        # User info tool (read-only)
        "GET_ME": generate_get_me_udf(),
    }

    if include_mutations:
        # Mutation/write tools - only include if enabled
        udfs.update(
            {
                # Tag management tools
                "ADD_TAGS": generate_add_tags_udf(),
                "REMOVE_TAGS": generate_remove_tags_udf(),
                # Description management tool
                "UPDATE_DESCRIPTION": generate_update_description_udf(),
                # Domain management tools
                "SET_DOMAINS": generate_set_domains_udf(),
                "REMOVE_DOMAINS": generate_remove_domains_udf(),
                # Owner management tools
                "ADD_OWNERS": generate_add_owners_udf(),
                "REMOVE_OWNERS": generate_remove_owners_udf(),
                # Glossary term management tools
                "ADD_GLOSSARY_TERMS": generate_add_glossary_terms_udf(),
                "REMOVE_GLOSSARY_TERMS": generate_remove_glossary_terms_udf(),
                # Structured property management tools
                "ADD_STRUCTURED_PROPERTIES": generate_add_structured_properties_udf(),
                "REMOVE_STRUCTURED_PROPERTIES": generate_remove_structured_properties_udf(),
            }
        )

    if include_cloud:
        udfs.update(
            {
                "ASK_DATAHUB_CHAT": generate_ask_datahub_chat_udf(),
                "GET_DATAHUB_CHAT": generate_get_datahub_chat_udf(),
            }
        )

    return udfs


def generate_datahub_udfs_sql(
    include_mutations: bool = True,
    include_cloud: bool = False,
) -> str:
    """Generate complete SQL script with DataHub UDFs using datahub-agent-context.

    Args:
        include_mutations: Whether to include mutation/write tools (default: True)
        include_cloud: Whether to include Cloud-only tools (default: False)
    """
    read_only_udfs = generate_all_udfs(include_mutations=False, include_cloud=False)
    read_ops_count = len(read_only_udfs)

    mutation_udfs = generate_all_udfs(include_mutations=True, include_cloud=False)
    write_ops_count = len(mutation_udfs) - read_ops_count

    cloud_udfs = generate_all_udfs(include_mutations=False, include_cloud=True)
    cloud_ops_count = len(cloud_udfs) - read_ops_count

    all_udfs = generate_all_udfs(
        include_mutations=include_mutations, include_cloud=include_cloud
    )
    total_udfs = len(all_udfs)

    udf_sections = []
    grant_statements = []
    show_statements = []
    function_list = []

    for function_name, udf_sql in all_udfs.items():
        udf_sections.append(f"""-- ============================================================================
-- UDF: {function_name}
-- ============================================================================
{udf_sql}""")

        # Generate GRANT statement based on function signature extracted from SQL
        signature = extract_function_signature(udf_sql)
        grant_statements.append(
            f"GRANT USAGE ON FUNCTION {function_name}({signature}) TO ROLE IDENTIFIER($SF_ROLE);"
        )

        show_statements.append(f"SHOW FUNCTIONS LIKE '{function_name}';")
        function_list.append(
            f"    $SF_DATABASE || '.' || $SF_SCHEMA || '.{function_name}' AS {function_name.lower()}"
        )

    function_list_joined = ("," + chr(10)).join(function_list)

    write_ops_section = (
        f"""--
-- Write Operations ({write_ops_count}):
--   - ADD_TAGS, REMOVE_TAGS: Tag management
--   - UPDATE_DESCRIPTION: Description management
--   - SET_DOMAINS, REMOVE_DOMAINS: Domain management
--   - ADD_OWNERS, REMOVE_OWNERS: Owner management
--   - ADD_GLOSSARY_TERMS, REMOVE_GLOSSARY_TERMS: Glossary term management
--   - ADD_STRUCTURED_PROPERTIES, REMOVE_STRUCTURED_PROPERTIES: Structured property management
"""
        if include_mutations
        else ""
    )

    cloud_ops_section = (
        f"""--
-- Cloud Operations ({cloud_ops_count}, requires DataHub Cloud):
--   - ASK_DATAHUB_CHAT: Send a message to the DataHub AI assistant
--   - GET_DATAHUB_CHAT: Retrieve messages from an AI conversation
"""
        if include_cloud
        else ""
    )

    return f"""-- ============================================================================
-- Step 2: DataHub API UDFs for Cortex Agent (using datahub-agent-context)
-- ============================================================================
-- This script creates {total_udfs} Python UDFs that enable Snowflake Intelligence to
-- query DataHub for metadata{" and manage metadata programmatically" if include_mutations else ""}.
--
-- These UDFs use the datahub-agent-context package wrapper methods.
--
-- UDFs included:
-- Read Operations ({read_ops_count}):
--   - SEARCH_DATAHUB: Search for entities
--   - GET_ENTITIES: Get entity details
--   - LIST_SCHEMA_FIELDS: List schema fields with filtering
--   - GET_LINEAGE: Get upstream/downstream lineage
--   - GET_LINEAGE_PATHS_BETWEEN: Get detailed transformation paths
--   - GET_DATASET_QUERIES: Get SQL queries using a dataset
--   - GET_DATASET_ASSERTIONS: Get data quality assertions for a dataset
--   - SEARCH_DOCUMENTS: Search organization documents
--   - GREP_DOCUMENTS: Regex search within documents
--   - GET_ME: Get authenticated user information
{write_ops_section}{cloud_ops_section}--
-- Prerequisites:
-- - Run 00_configuration.sql first to set variables
-- - Run 01_network_rules.sql to create network rules and secrets
-- - You must have appropriate privileges to create functions
-- ============================================================================

USE DATABASE IDENTIFIER($SF_DATABASE);
USE SCHEMA IDENTIFIER($SF_SCHEMA);
USE WAREHOUSE IDENTIFIER($SF_WAREHOUSE);

{chr(10).join(udf_sections)}

-- ============================================================================
-- Grant Usage Permissions
-- ============================================================================
{chr(10).join(grant_statements)}

-- ============================================================================
-- Verify All UDFs Were Created
-- ============================================================================
{chr(10).join(show_statements)}

SELECT
    'All {total_udfs} DataHub UDFs created successfully!' AS status,
{function_list_joined};
"""


@click.command()
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, writable=True),
    help="Output file path for generated SQL (default: print to stdout)",
)
@click.option(
    "--enable-mutations/--no-enable-mutations",
    default=True,
    help="Include mutation/write tools (tags, descriptions, owners, etc.). Default: enabled",
)
@click.option(
    "--include-cloud/--no-include-cloud",
    default=False,
    help="Include Cloud-only tools (Ask DataHub AI chat). Default: disabled",
)
def main(output: str | None, enable_mutations: bool, include_cloud: bool) -> None:
    """Generate Snowflake UDF SQL for DataHub integration.

    This command generates SQL scripts that create Snowflake User-Defined Functions (UDFs)
    for interacting with DataHub metadata from Snowflake.

    Generates all UDFs using the datahub-agent-context package.

    Examples:
        # Print SQL to stdout with all tools (read + write)
        python -m datahub.ai.snowflake.generate_udfs

        # Generate read-only tools (no mutations)
        python -m datahub.ai.snowflake.generate_udfs --no-enable-mutations

        # Save to file with mutations enabled
        python -m datahub.ai.snowflake.generate_udfs -o datahub_udfs.sql
    """
    sql_content = generate_datahub_udfs_sql(
        include_mutations=enable_mutations, include_cloud=include_cloud
    )

    if output:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(sql_content)
        udf_count = len(
            generate_all_udfs(
                include_mutations=enable_mutations, include_cloud=include_cloud
            )
        )
        click.echo(f"✓ Generated {udf_count} Snowflake UDF(s) to: {output_path}")
        logger.info(f"Generated Snowflake UDF SQL to {output_path}")
    else:
        click.echo(sql_content)


if __name__ == "__main__":
    main()
