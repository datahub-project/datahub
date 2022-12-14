from enum import Enum


class SnowflakeCloudProvider(str, Enum):
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"


SNOWFLAKE_DEFAULT_CLOUD_REGION_ID = "us-west-2"
SNOWFLAKE_DEFAULT_CLOUD = SnowflakeCloudProvider.AWS


class SnowflakeEdition(str, Enum):
    STANDARD = "Standard"

    # We use this to represent Enterprise Edition or higher
    ENTERPRISE = "Enterprise or above"
