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


# Default configuration values
DEFAULT_MAX_TABLE_LINEAGE_PER_CONNECTOR = 300

# Comprehensive mapping of Fivetran connector types to DataHub platforms
FIVETRAN_PLATFORM_TO_DATAHUB_PLATFORM = {
    # Database platforms (source connectors)
    "postgres": "postgres",
    "postgres_rds": "postgres",
    "postgresql": "postgres",
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
    # Cloud data warehouses (source & destination)
    "snowflake": "snowflake",
    "redshift": "redshift",
    "bigquery": "bigquery",
    "google_bigquery": "bigquery",
    "databricks": "databricks",
    # Cloud storage (source & destination)
    "s3": "s3",
    "azure_blob_storage": "abs",
    "gcs": "gcs",
    "google_cloud_storage": "gcs",
    # Applications and SaaS (source connectors)
    "salesforce": "salesforce",
    "mongodb": "mongodb",
    "kafka": "kafka",
    "confluent_cloud": "kafka",
}
