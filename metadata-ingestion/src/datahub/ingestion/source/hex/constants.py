from typing import Optional, Tuple

from datahub.metadata.urns import DataPlatformUrn

HEX_PLATFORM_NAME = "hex"
HEX_PLATFORM_URN = DataPlatformUrn(platform_name=HEX_PLATFORM_NAME)
HEX_API_BASE_URL_DEFAULT = "https://app.hex.tech/api/v1"
HEX_API_PAGE_SIZE_DEFAULT = 100

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
    "motherduck": "duckdb",
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


# (database_key, schema_key, schema_fallback) per Hex connection type, used to
# extract default_database/default_schema from `connectionDetails.<type>` for
# `sqlglot_lineage` so unqualified `FROM table` refs resolve to the canonical
# warehouse URN.
#
# - database_key=None for 2-part-URN platforms (mysql/mariadb/clickhouse).
#   The Hex `database` field on these is the schema slot, not the catalog slot
#   — populating a database here would produce a wrong 3-part URN.
# - schema_fallback is the engine's server-side default schema (`public` for
#   Postgres, `dbo` for SQL Server, etc.). Applied only when a database
#   resolved — otherwise the result would be an incomplete 3-part URN.
CONNECTION_TYPE_DEFAULTS: dict[
    str, Tuple[Optional[str], Optional[str], Optional[str]]
] = {
    "bigquery": ("projectId", None, None),
    "snowflake": ("database", "schema", None),
    "redshift": ("database", "schema", "public"),
    "postgres": ("database", "schema", "public"),
    "alloydb": ("database", "schema", "public"),
    "cloudsql__postgres": ("database", "schema", "public"),
    "sqlserver": ("database", "schema", "dbo"),
    "cloudsql__sqlserver": ("database", "schema", "dbo"),
    "databricks": ("catalog", "schema", "default"),
    "motherduck": ("database", "schema", "main"),
    "trino": ("catalog", "schema", None),
    "starburst": ("catalog", "schema", None),
    "presto": ("catalog", "schema", None),
    "mysql": (None, "database", None),
    "mariadb": (None, "database", None),
    "cloudsql__mysql": (None, "database", None),
    "clickhouse": (None, "database", None),
}
