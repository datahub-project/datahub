from datahub.metadata.urns import DataPlatformUrn

HEX_PLATFORM_NAME = "hex"
HEX_PLATFORM_URN = DataPlatformUrn(platform_name=HEX_PLATFORM_NAME)
HEX_API_BASE_URL_DEFAULT = "https://app.hex.tech/api/v1"
HEX_API_PAGE_SIZE_DEFAULT = 100

DATAHUB_API_PAGE_SIZE_DEFAULT = 100

# connection_type values from /v1/data-connections → DataHub platform names
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
