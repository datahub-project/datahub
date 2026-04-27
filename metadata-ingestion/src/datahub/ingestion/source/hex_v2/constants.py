from datahub.metadata.urns import DataPlatformUrn

HEX_PLATFORM_NAME = "hex"
HEX_PLATFORM_URN = DataPlatformUrn(platform_name=HEX_PLATFORM_NAME)
HEX_BASE_URL_DEFAULT = "https://app.hex.tech"

# The Hex CLI API rejects page sizes > 30 with a malformed-request error.
HEX_CLI_PAGE_SIZE_MAX = 30

# connection_type values returned by `hex connection list` → DataHub platform names
CONNECTION_TYPE_TO_DATAHUB_PLATFORM: dict[str, str] = {
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "databricks": "databricks",
    "duckdb": "duckdb",
    "spark": "spark",
    "mssql": "mssql",
    "trino": "trino",
    "athena": "athena",
    "clickhouse": "clickhouse",
    "hive": "hive",
}
