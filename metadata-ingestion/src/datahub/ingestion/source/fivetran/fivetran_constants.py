"""
Constants and mappings used across Fivetran source modules.
"""

from enum import Enum


class FivetranMode(Enum):
    """Fivetran source operation modes."""

    ENTERPRISE = "enterprise"
    STANDARD = "standard"
    AUTO = "auto"


class DataJobMode(Enum):
    """DataJob generation modes."""

    CONSOLIDATED = "consolidated"
    PER_TABLE = "per_table"


# Lineage and job processing limits
DEFAULT_MAX_TABLE_LINEAGE_PER_CONNECTOR = 120
MAX_COLUMN_LINEAGE_PER_CONNECTOR = 10000
MAX_JOBS_PER_CONNECTOR = 500

# Mapping of Fivetran service types to officially supported DataHub platforms
# This leverages the 'service' field from Fivetran API instead of hardcoded connector aliases
# Only includes platforms that are officially supported by DataHub
FIVETRAN_PLATFORM_TO_DATAHUB_PLATFORM = {
    # Database platforms (source & destination connectors)
    "postgres": "postgres",
    "postgres_rds": "postgres",
    "postgresql": "postgres",
    "google_cloud_mysql": "mysql",
    "aurora": "mysql",
    "mysql": "mysql",
    "mysql_rds": "mysql",
    "oracle": "oracle",
    "sql_server": "mssql",
    "sql_server_rds": "mssql",
    "mssql": "mssql",
    "synapse": "mssql",
    "azure_sql": "mssql",
    "mariadb": "mariadb",
    "snowflake": "snowflake",
    "snowflake_db": "snowflake",  # Fivetran service name for Snowflake
    "redshift": "redshift",
    "bigquery": "bigquery",
    "google_bigquery": "bigquery",
    "databricks": "databricks",
    # Cloud storage (source & destination)
    "s3": "s3",
    "gcs": "gcs",
    "google_cloud_storage": "gcs",
    # Message brokers
    "kafka": "kafka",
    "confluent_cloud": "kafka",
    # Applications and other supported platforms
    "salesforce": "salesforce",
    "mongodb": "mongodb",
    "slack": "slack",
    # Fivetran internal services
    "fivetran_log": "fivetran",  # Internal Fivetran logging tables
}


def get_platform_from_fivetran_service(service_name: str) -> str:
    """
    Get DataHub platform name from Fivetran service name.

    This function leverages the service field from Fivetran API instead of
    relying on hardcoded connector aliases like 'glitch_axis'.

    Args:
        service_name: The service name from Fivetran API (e.g., 'slack', 'google_cloud_mysql')

    Returns:
        DataHub platform name
    """
    if not service_name:
        return "unknown"

    service = service_name.lower().strip()

    # Check direct mapping first
    if service in FIVETRAN_PLATFORM_TO_DATAHUB_PLATFORM:
        return FIVETRAN_PLATFORM_TO_DATAHUB_PLATFORM[service]

    # Check partial matches for complex service names
    # e.g., 'google_cloud_mysql' should match 'mysql'
    for (
        fivetran_service,
        datahub_platform,
    ) in FIVETRAN_PLATFORM_TO_DATAHUB_PLATFORM.items():
        if fivetran_service in service or service in fivetran_service:
            return datahub_platform

    # Handle common patterns not in the mapping (only for supported platforms)
    if "mysql" in service:
        return "mysql"
    elif "postgres" in service:
        return "postgres"
    elif "snowflake" in service:
        return "snowflake"
    elif "bigquery" in service:
        return "bigquery"
    elif "redshift" in service:
        return "redshift"
    elif "databricks" in service:
        return "databricks"
    elif "oracle" in service:
        return "oracle"
    elif "sql_server" in service or "mssql" in service:
        return "mssql"
    elif "mongo" in service:
        return "mongodb"
    elif "kafka" in service:
        return "kafka"
    elif "s3" in service:
        return "s3"
    elif "gcs" in service or "google_cloud_storage" in service:
        return "gcs"
    elif "salesforce" in service:
        return "salesforce"
    elif "slack" in service:
        return "slack"

    # If no mapping found, use the service name itself
    # This eliminates the need for hardcoded connector ID aliases
    return service
