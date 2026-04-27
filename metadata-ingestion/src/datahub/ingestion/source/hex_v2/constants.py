from pathlib import Path

from datahub.metadata.urns import DataPlatformUrn

HEX_PLATFORM_NAME = "hex"
HEX_PLATFORM_URN = DataPlatformUrn(platform_name=HEX_PLATFORM_NAME)
HEX_BASE_URL_DEFAULT = "https://app.hex.tech"

# The Hex CLI API rejects page sizes > 30 with a malformed-request error.
HEX_CLI_PAGE_SIZE_MAX = 30

# Pinned Hex CLI version — tested and validated against this release.
# Update after validating against a newer release.
HEX_CLI_VERSION = "1.2.2"

# GitHub release asset names per (system, machine) — mirrors the hex-cli release page.
# system: platform.system() returns "Darwin" or "Linux"
# machine: platform.machine() returns "arm64" / "aarch64" or "x86_64"
HEX_CLI_RELEASE_ASSETS: dict[tuple[str, str], str] = {
    ("Darwin", "arm64"): "hex-aarch64-apple-darwin.tar.xz",
    ("Darwin", "x86_64"): "hex-x86_64-apple-darwin.tar.xz",
    ("Linux", "aarch64"): "hex-aarch64-unknown-linux-gnu.tar.xz",
    ("Linux", "x86_64"): "hex-x86_64-unknown-linux-gnu.tar.xz",
}

HEX_CLI_CACHE_DIR = Path.home() / ".datahub" / "tools" / "hex"

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
