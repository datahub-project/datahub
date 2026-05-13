"""Generate Snowflake configuration SQL."""


def generate_configuration_sql(
    sf_account: str | None,
    sf_user: str | None,
    sf_role: str | None,
    sf_warehouse: str | None,
    sf_database: str | None,
    sf_schema: str | None,
    datahub_url: str,
    datahub_token: str,
    agent_name: str,
    agent_display_name: str,
    agent_color: str,
    execute: bool = False,
) -> str:
    """Generate configuration SQL based on provided parameters.

    If parameters are None, uses Snowflake SQL functions to auto-detect values.
    In non-execute mode, uses placeholder for token for security.

    Args:
        execute: If True, includes actual token. If False, uses placeholder.
    """
    # Use SQL functions for auto-detection when values are not provided
    account_value = f"'{sf_account}'" if sf_account else "CURRENT_ACCOUNT()"
    user_value = f"'{sf_user}'" if sf_user else "CURRENT_USER()"
    role_value = f"'{sf_role}'" if sf_role else "CURRENT_ROLE()"
    warehouse_value = f"'{sf_warehouse}'" if sf_warehouse else "CURRENT_WAREHOUSE()"
    database_value = f"'{sf_database}'" if sf_database else "CURRENT_DATABASE()"
    schema_value = f"'{sf_schema}'" if sf_schema else "CURRENT_SCHEMA()"

    # Use placeholder for token in non-execute mode for security
    token_value = datahub_token if execute else "<DATAHUB_TOKEN>"

    return f"""-- ============================================================================
-- CONFIGURATION FILE - Customize these values for your environment
-- ============================================================================
-- Values are auto-detected from your current session where possible
-- Then run the numbered scripts in order (01, 02, 03, 04, 05)

-- ============================================================================
-- SNOWFLAKE CONFIGURATION
-- ============================================================================

-- Your Snowflake account identifier (e.g., 'xy12345' or 'xy12345.us-east-1')
SET SF_ACCOUNT = {account_value};

-- Your Snowflake user (the user who will run the agent)
SET SF_USER = {user_value};

-- Your Snowflake role (must have permissions to create objects)
SET SF_ROLE = {role_value};

-- Your Snowflake warehouse name
SET SF_WAREHOUSE = {warehouse_value};

-- Your Snowflake database name
SET SF_DATABASE = {database_value};

-- Your Snowflake schema name (where UDFs, procedures, and agent will be created)
SET SF_SCHEMA = {schema_value};

-- Set the database, schema, and warehouse context
USE DATABASE IDENTIFIER($SF_DATABASE);
USE SCHEMA IDENTIFIER($SF_SCHEMA);
USE WAREHOUSE IDENTIFIER($SF_WAREHOUSE);

-- ============================================================================
-- DATAHUB CONFIGURATION
-- ============================================================================

-- Your DataHub instance URL (without /gms or trailing slash)
-- Examples:
--   https://fieldeng.acryl.io
--   https://your-company.acryl.io
--   https://datahub.your-company.com
SET DATAHUB_URL = '{datahub_url}';

-- Your DataHub Personal Access Token (PAT)
-- Get this from DataHub UI: Settings > Access Tokens > Create Token
-- Create Snowflake secrets to store credentials securely
CREATE OR REPLACE SECRET datahub_url
  TYPE = GENERIC_STRING
  SECRET_STRING = '{datahub_url}';

CREATE OR REPLACE SECRET datahub_token
  TYPE = GENERIC_STRING
  SECRET_STRING = '{token_value}';

-- ============================================================================
-- AGENT CONFIGURATION
-- ============================================================================

-- Agent name (will be created as: <DATABASE>.<SCHEMA>.<AGENT_NAME>)
SET AGENT_NAME = '{agent_name}';

-- Agent display name (shown in Snowflake Intelligence UI)
SET AGENT_DISPLAY_NAME = '{agent_display_name}';

-- Agent color (for UI display)
SET AGENT_COLOR = '{agent_color}';
"""
