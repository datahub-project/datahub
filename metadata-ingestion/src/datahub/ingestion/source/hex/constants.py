from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.metadata.urns import DataPlatformUrn

HEX_PLATFORM_NAME = "hex"
HEX_PLATFORM_URN = DataPlatformUrn(platform_name=HEX_PLATFORM_NAME)
HEX_API_BASE_URL_DEFAULT = "https://app.hex.tech/api/v1"
HEX_API_PAGE_SIZE_DEFAULT = 100

# Connector semantic version.
# Bump minor or major when the set of emitted aspects changes — this rotates
# the checkpoint job_ids so existing incremental state is abandoned and the
# next run performs a full re-process.
# Patch bumps (bug fixes, perf, docs) do NOT rotate the checkpoint.
HEX_CONNECTOR_VERSION = "1.0.0"


def _checkpoint_version_suffix() -> str:
    """Return 'vMAJOR.MINOR' from HEX_CONNECTOR_VERSION."""
    major, minor, _ = HEX_CONNECTOR_VERSION.split(".")
    return f"v{major}.{minor}"


# Job IDs for stateful ingestion checkpoints.
HEX_STALE_REMOVAL_JOB_ID = JobId(
    f"hex_stale_entity_removal_{_checkpoint_version_suffix()}"
)
HEX_INCREMENTAL_JOB_ID = JobId(f"hex_incremental_{_checkpoint_version_suffix()}")

DATAHUB_API_PAGE_SIZE_DEFAULT = 100

# connection_type values from /v1/data-connections → DataHub platform names.
# Used for constructing dataset URNs (urn:li:dataPlatform:<name>).
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
