from typing import Dict, Set

# Monte Carlo connection/warehouse types -> DataHub platform names. There is no
# canonical DataHub platform-name enum/registry usable from Python ingestion
# code (the closest, common/data_platforms.py's KNOWN_VALID_PLATFORM_NAMES, is
# incomplete and documented as unsuitable for validation), so this maps plain
# strings, matching every other connector's platform-name fields.
CONNECTION_TYPE_TO_PLATFORM: Dict[str, str] = {
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "databricks": "databricks",
    "databricks-metastore": "databricks",
    "databricks-sql": "databricks",
    "spark": "spark",
    "presto": "presto",
    "hive": "hive",
    "glue": "glue",
    "athena": "athena",
    "postgres": "postgres",
    "mysql": "mysql",
    "oracle": "oracle",
    "sql-server": "mssql",
    "synapse": "mssql",
    "teradata": "teradata",
    "transactional-db": "postgres",
}

# Warehouse platforms whose DataHub source emits lowercased dataset URNs by
# default (Snowflake sets convert_urns_to_lowercase=True; Redshift folds unquoted
# identifiers to lowercase). MC assertion URNs must match those exactly to attach
# to the right dataset, so we lowercase the table path for these platforms only.
# Case-preserving platforms (e.g. BigQuery) keep the original case. The
# convert_urns_to_lowercase config flag forces lowercase everywhere when set.
LOWERCASE_URN_PLATFORMS: Set[str] = {"snowflake", "redshift"}
