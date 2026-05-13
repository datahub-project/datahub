"""Generate Snowflake network rules SQL."""


def generate_network_rules_sql(datahub_domain: str) -> str:
    """Generate network rules SQL that uses configuration variables.

    Args:
        datahub_domain: The domain extracted from the DataHub URL (e.g., 'fieldeng.acryl.io')
    """
    return f"""-- ============================================================================
-- Step 1: Network Rules and External Access Integration for DataHub API
-- ============================================================================
-- This script creates the network rule and external access integration
-- required for UDFs to make outbound calls to DataHub
--
-- Prerequisites:
-- - Run 00_configuration.sql first to set variables
-- - You must have ACCOUNTADMIN role or equivalent privileges
-- ============================================================================

-- Use the configured database and schema
USE DATABASE IDENTIFIER($SF_DATABASE);
USE SCHEMA IDENTIFIER($SF_SCHEMA);
USE WAREHOUSE IDENTIFIER($SF_WAREHOUSE);

-- Step 1: Create network rule for DataHub API
-- For EGRESS mode with external access, use HOST_PORT type with domain name
-- Note: Snowflake resolves the domain to IPs automatically
CREATE OR REPLACE NETWORK RULE datahub_api_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('{datahub_domain}')
  COMMENT = 'Network rule for {datahub_domain} DataHub API access';

-- Step 2: Create external access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION datahub_access
  ALLOWED_NETWORK_RULES = (datahub_api_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (datahub_url, datahub_token)
  ENABLED = TRUE
  COMMENT = 'External access integration for DataHub API calls';

-- Step 3: Grant usage on integration to the specified role
GRANT USAGE ON INTEGRATION datahub_access TO ROLE IDENTIFIER($SF_ROLE);

-- Verify the setup
SHOW NETWORK RULES LIKE 'datahub_api_rule';
SHOW INTEGRATIONS LIKE 'datahub_access';

SELECT
    'Network rule and external access integration created successfully!' AS status,
    '{datahub_domain}' AS datahub_domain,
    $SF_DATABASE || '.' || $SF_SCHEMA AS location;
"""
