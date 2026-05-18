from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.metadata.urns import DataPlatformUrn

HEX_PLATFORM_NAME = "hex"
HEX_PLATFORM_URN = DataPlatformUrn(platform_name=HEX_PLATFORM_NAME)
HEX_API_BASE_URL_DEFAULT = "https://app.hex.tech/api/v1"
HEX_API_PAGE_SIZE_DEFAULT = 100

# Job ID for the incremental ingestion checkpoint.
# Checkpoint-key versioning (to force re-bootstrap when connector logic changes)
# is tracked as a separate framework improvement via the @semantic_version
# decorator tech spec — applicable to all connectors, not just hex.
HEX_INCREMENTAL_JOB_ID = JobId("hex_incremental")

# Maps the `type` field returned by Hex's GET /v1/data-connections to
# DataHub platform names. Connections of any other type require a user-supplied
# entry in HexSourceConfig.connection_platform_map (keyed by connection ID).
CONNECTION_TYPE_TO_DATAHUB_PLATFORM: dict[str, str] = {
    # Defined in the API enum (DataConnectionApiType).
    "athena": "athena",
    "bigquery": "bigquery",
    "clickhouse": "clickhouse",
    "databricks": "databricks",
    "postgres": "postgres",
    "redshift": "redshift",
    "snowflake": "snowflake",
    "trino": "trino",
    # Present in the Hex UI but missing from the API enum
    "alloydb": "postgres",
    "cloudsql__postgres": "postgres",
    "cloudsql__mysql": "mysql",
    "cloudsql__sqlserver": "mssql",
    "mariadb": "mariadb",
    "sqlserver": "mssql",
    "mysql": "mysql",
    "presto": "presto",
    "starburst": "trino",
}
