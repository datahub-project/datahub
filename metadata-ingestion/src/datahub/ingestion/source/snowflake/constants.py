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


# See https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#region-ids
# Includes only exceptions to format <provider>_<cloud region with hyphen replaced by _>
SNOWFLAKE_REGION_CLOUD_REGION_MAPPING = {
    "aws_us_east_1_gov": (SnowflakeCloudProvider.AWS, "us-east-1"),
    "azure_uksouth": (SnowflakeCloudProvider.AZURE, "uk-south"),
    "azure_centralindia": (SnowflakeCloudProvider.AZURE, "central-india.azure"),
}
