"""Configuration classes for the Pentaho ingestion source."""

from typing import Dict, Optional

from pydantic import Field

from datahub.configuration.common import ConfigModel


class PentahoSourceConfig(ConfigModel):
    """Configuration for the Pentaho ingestion source."""

    base_folder: str = Field(
        description="Root folder path to scan for Pentaho .ktr and .kjb files"
    )

    platform_instance: Optional[str] = Field(
        default=None, description="Platform instance name"
    )

    env: str = Field(
        default="PROD", description="Environment name for dataset and job URNs"
    )

    default_owner: str = Field(
        default="pentaho_admin", description="Default owner username"
    )

    file_size_limit_mb: int = Field(
        default=50, description="Maximum file size in MB to process"
    )

    # Platform mapping configuration
    platform_mappings: Dict[str, str] = Field(
        default_factory=lambda: {
            "googlebigquery": "bigquery",
            "bigquery": "bigquery",
            "vertica5": "vertica",
            "vertica": "vertica",
            "postgresql": "postgres",
            "postgres": "postgres",
            "mysql": "mysql",
            "oracle": "oracle",
            "mssqlnative": "mssql",
            "mssql": "mssql",
            "redshift": "redshift",
            "snowflake": "snowflake",
            "hive": "hive",
            "teradata": "teradata",
            "db2": "db",
            "csv": "file",
            "textfile": "file",
            "excel": "file",
        },
        description="Mapping from Pentaho connection types to DataHub platforms",
    )
