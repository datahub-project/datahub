from datahub.utilities.str_enum import StrEnum


class SnowflakeCloudProvider(StrEnum):
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"


SNOWFLAKE_DEFAULT_CLOUD = SnowflakeCloudProvider.AWS

DEFAULT_SNOWFLAKE_DOMAIN = "snowflakecomputing.com"

# Snowflake auto-creates an INFORMATION_SCHEMA in every database; ingestion never
# emits it. Single source of truth for the extraction SQL and the agent probe.
SNOWFLAKE_INFORMATION_SCHEMA = "INFORMATION_SCHEMA"
SNOWFLAKE_DEFAULT_SCHEMAS = frozenset({SNOWFLAKE_INFORMATION_SCHEMA})


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
    "azure_swedencentral": (SnowflakeCloudProvider.AZURE, "sweden-central"),
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
    SEMANTIC_VIEW = "semantic view"
    DATABASE = "database"
    SCHEMA = "schema"
    COLUMN = "column"
    ICEBERG_TABLE = "iceberg table"
    STREAM = "stream"
    PROCEDURE = "procedure"
    DYNAMIC_TABLE = "dynamic table"
    STREAMLIT = "streamlit"
    STAGE = "stage"
    TASK = "task"
    PIPE = "pipe"


# The plural keyword naming an object class in a SHOW statement, e.g. `SHOW DYNAMIC TABLES`.
# Deliberately separate from SnowflakeObjectDomain, which carries the singular lowercase
# objectDomain values ACCESS_HISTORY reports ("dynamic table") - the two vocabularies differ
# in both number and case, so neither can stand in for the other.
class SnowflakeShowKind(StrEnum):
    VIEWS = "VIEWS"
    STREAMS = "STREAMS"
    DYNAMIC_TABLES = "DYNAMIC TABLES"
    ICEBERG_TABLES = "ICEBERG TABLES"


# SHOW ICEBERG TABLES.iceberg_table_type. SNOWFLAKE: Snowflake owns the metadata.
# UNMANAGED: another engine (e.g. Databricks UniForm, Spark) writes it, Snowflake only reads.
# The raw value is kept on the table as well, so a type Snowflake adds later still surfaces.
class SnowflakeIcebergTableType(StrEnum):
    SNOWFLAKE = "SNOWFLAKE"
    UNMANAGED = "UNMANAGED"


# DESC EXTERNAL VOLUME rows: parent_property / property / property_value. The storage
# locations sit under one parent, each as a JSON object, with ACTIVE naming the current one.
EXTERNAL_VOLUME_STORAGE_LOCATIONS_PARENT = "STORAGE_LOCATIONS"
EXTERNAL_VOLUME_ACTIVE_PROPERTY = "ACTIVE"
EXTERNAL_VOLUME_STORAGE_LOCATION_PREFIX = "STORAGE_LOCATION_"
EXTERNAL_VOLUME_LOCATION_NAME_KEY = "NAME"
EXTERNAL_VOLUME_STORAGE_BASE_URL_KEY = "STORAGE_BASE_URL"

# Custom properties added to Iceberg tables (upper-case like IS_ICEBERG / TARGET_LAG).
ICEBERG_TABLE_TYPE_PROPERTY = "ICEBERG_TABLE_TYPE"
ICEBERG_EXTERNAL_VOLUME_PROPERTY = "EXTERNAL_VOLUME"
ICEBERG_BASE_LOCATION_PROPERTY = "BASE_LOCATION"
ICEBERG_STORAGE_LOCATION_PROPERTY = "STORAGE_LOCATION"
ICEBERG_CATALOG_INTEGRATION_PROPERTY = "CATALOG_INTEGRATION"
ICEBERG_CATALOG_NAMESPACE_PROPERTY = "CATALOG_NAMESPACE"
ICEBERG_CATALOG_TABLE_NAME_PROPERTY = "CATALOG_TABLE_NAME"

# Custom property the Unity Catalog connector emits on external tables; the Iceberg
# lineage lookup matches a Snowflake table's storage location against it.
UNITY_CATALOG_STORAGE_LOCATION_PROPERTY = "storage_location"


GENERIC_PERMISSION_ERROR_KEY = "permission-error"
LINEAGE_PERMISSION_ERROR = "lineage-permission-error"

STREAMLIT_PLATFORM = "streamlit"


class SemanticViewColumnSubtype(StrEnum):
    """Subtype for semantic view columns: dimensions, facts, and metrics."""

    DIMENSION = "DIMENSION"
    FACT = "FACT"
    METRIC = "METRIC"


# Snowflake connection arguments
# https://docs.snowflake.com/en/user-guide/python-connector-api.html#connect
CLIENT_PREFETCH_THREADS = "client_prefetch_threads"
CLIENT_SESSION_KEEP_ALIVE = "client_session_keep_alive"

MARKETPLACE_LISTINGS_PERMISSION_ERROR = "marketplace-listings-permission-error"
MARKETPLACE_PURCHASES_PERMISSION_ERROR = "marketplace-purchases-permission-error"
MARKETPLACE_USAGE_PERMISSION_ERROR = "marketplace-usage-permission-error"


class SnowflakeMarketplaceObjectDomain(StrEnum):
    LISTING = "listing"
    PURCHASE = "purchase"
