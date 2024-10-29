from datahub.utilities.str_enum import StrEnum


class SnowflakeCloudProvider(StrEnum):
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"


SNOWFLAKE_DEFAULT_CLOUD = SnowflakeCloudProvider.AWS


class SnowflakeEdition(StrEnum):
    STANDARD = "Standard"

    # We use this to represent Enterprise Edition or higher
    ENTERPRISE = "Enterprise or above"


# See https://docs.snowflake.com/en/user-guide/admin-account-identifier#non-vps-account-locator-formats-by-cloud-platform-and-region
# Includes only exceptions to format <provider>_<cloud region with hyphen replaced by _>
SNOWFLAKE_REGION_CLOUD_REGION_MAPPING = {
    "aws_us_east_1_gov": (SnowflakeCloudProvider.AWS, "us-east-1"),
    "azure_westus2": (SnowflakeCloudProvider.AZURE, "west-us-2"),
    "azure_centralus": (SnowflakeCloudProvider.AZURE, "central-us"),
    "azure_southcentralus": (SnowflakeCloudProvider.AZURE, "south-central-us"),
    "azure_eastus2": (SnowflakeCloudProvider.AZURE, "east-us-2"),
    "azure_usgovvirginia": (SnowflakeCloudProvider.AZURE, "us-gov-virginia"),
    "azure_canadacentral": (SnowflakeCloudProvider.AZURE, "canada-central"),
    "azure_uksouth": (SnowflakeCloudProvider.AZURE, "uk-south"),
    "azure_northeurope": (SnowflakeCloudProvider.AZURE, "north-europe"),
    "azure_westeurope": (SnowflakeCloudProvider.AZURE, "west-europe"),
    "azure_switzerlandnorth": (SnowflakeCloudProvider.AZURE, "switzerland-north"),
    "azure_uaenorth": (SnowflakeCloudProvider.AZURE, "uae-north"),
    "azure_centralindia": (SnowflakeCloudProvider.AZURE, "central-india"),
    "azure_japaneast": (SnowflakeCloudProvider.AZURE, "japan-east"),
    "azure_southeastasia": (SnowflakeCloudProvider.AZURE, "southeast-asia"),
    "azure_australiaeast": (SnowflakeCloudProvider.AZURE, "australia-east"),
}

# https://docs.snowflake.com/en/sql-reference/snowflake-db.html
SNOWFLAKE_DATABASE = "SNOWFLAKE"


# We will always compare with lowercase
# Complete list for objectDomain - https://docs.snowflake.com/en/sql-reference/account-usage/access_history.html
class SnowflakeObjectDomain(StrEnum):
    TABLE = "table"
    EXTERNAL_TABLE = "external table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized view"
    DATABASE = "database"
    SCHEMA = "schema"
    COLUMN = "column"
    ICEBERG_TABLE = "iceberg table"


GENERIC_PERMISSION_ERROR_KEY = "permission-error"
LINEAGE_PERMISSION_ERROR = "lineage-permission-error"


# Snowflake connection arguments
# https://docs.snowflake.com/en/user-guide/python-connector-api.html#connect
CLIENT_PREFETCH_THREADS = "client_prefetch_threads"
CLIENT_SESSION_KEEP_ALIVE = "client_session_keep_alive"
