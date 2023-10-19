from enum import Enum


class SnowflakeCloudProvider(str, Enum):
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"


SNOWFLAKE_DEFAULT_CLOUD = SnowflakeCloudProvider.AWS


class SnowflakeEdition(str, Enum):
    STANDARD = "Standard"

    # We use this to represent Enterprise Edition or higher
    ENTERPRISE = "Enterprise or above"


# See https://docs.snowflake.com/en/user-guide/admin-account-identifier#non-vps-account-locator-formats-by-cloud-platform-and-region
# Includes only exceptions to format <provider>_<cloud region with hyphen replaced by _>
SNOWFLAKE_REGION_CLOUD_REGION_MAPPING = {
    "aws_us_east_1_gov": (SnowflakeCloudProvider.AWS, "us-east-1"),
    "aws_us_gov_west_1": (SnowflakeCloudProvider.AWS, "us-west-1"),
    "aws_us_gov_west_1_fhplus": (SnowflakeCloudProvider.AWS, "us-west-1"),
    "aws_us_east_2": (SnowflakeCloudProvider.AWS, "us-east-2"),
    "aws_us_east_1": (SnowflakeCloudProvider.AWS, "us-east-1"),
    "aws_us_gov_east_1": (SnowflakeCloudProvider.AWS, "us-east-1"),
    "aws_ca_central_1": (SnowflakeCloudProvider.AWS, "ca-central-1"),
    "aws_sa_east_1": (SnowflakeCloudProvider.AWS, "sa-east-1"),
    "aws_eu_west_1": (SnowflakeCloudProvider.AWS, "eu-west-1"),
    "aws_eu_west_2": (SnowflakeCloudProvider.AWS, "eu-west-2"),
    "aws_eu_west_3": (SnowflakeCloudProvider.AWS, "eu-west-3"),
    "aws_eu_central_1": (SnowflakeCloudProvider.AWS, "eu-central-1"),
    "aws_eu_north_1": (SnowflakeCloudProvider.AWS, "eu-north-1"),
    "aws_ap_northeast_1": (SnowflakeCloudProvider.AWS, "ap-northeast-1"),
    "aws_ap_northeast_2": (SnowflakeCloudProvider.AWS, "ap-northeast-2"),
    "aws_ap_northeast_3": (SnowflakeCloudProvider.AWS, "ap-northeast-3"),
    "aws_ap_south_1": (SnowflakeCloudProvider.AWS, "ap-south-1"),
    "aws_ap_southeast_1": (SnowflakeCloudProvider.AWS, "ap-southeast-1"),
    "aws_ap_southeast_2": (SnowflakeCloudProvider.AWS, "ap-southeast-2"),
    "aws_ap_southeast_3": (SnowflakeCloudProvider.AWS, "ap-southeast-3"),
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
    "gcp_us_central1": (SnowflakeCloudProvider.GCP, "us-central1"),
    "gcp_us_east4": (SnowflakeCloudProvider.GCP, "us-east4"),
    "gcp_europe_west2": (SnowflakeCloudProvider.GCP, "europe-west2"),
    "gcp_europe_west4": (SnowflakeCloudProvider.GCP, "europe-west4"),
}

# https://docs.snowflake.com/en/sql-reference/snowflake-db.html
SNOWFLAKE_DATABASE = "SNOWFLAKE"


# We will always compare with lowercase
# Complete list for objectDomain - https://docs.snowflake.com/en/sql-reference/account-usage/access_history.html
class SnowflakeObjectDomain(str, Enum):
    TABLE = "table"
    EXTERNAL_TABLE = "external table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized view"
    DATABASE = "database"
    SCHEMA = "schema"
    COLUMN = "column"


GENERIC_PERMISSION_ERROR_KEY = "permission-error"
LINEAGE_PERMISSION_ERROR = "lineage-permission-error"


# Snowflake connection arguments
# https://docs.snowflake.com/en/user-guide/python-connector-api.html#connect
CLIENT_PREFETCH_THREADS = "client_prefetch_threads"
CLIENT_SESSION_KEEP_ALIVE = "client_session_keep_alive"
