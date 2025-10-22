"""
SAP HANA configuration for DataHub ingestion.

This module contains the configuration class for the SAP HANA DataHub source,
including connection settings, filtering patterns, and ingestion options.
"""

from typing import Optional

from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig


class HanaConfig(BasicSQLAlchemyConfig):
    """Configuration for SAP HANA source."""

    scheme: str = Field(default="hana", description="Database scheme", exclude=True)
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern(deny=["sys"]),
        description="Regex patterns for schemas to filter in ingestion. By default, excludes system schemas.",
    )
    max_workers: int = Field(
        default=5,
        description="Maximum concurrent SQL connections to the SAP HANA instance.",
    )
    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns for databases to filter in ingestion. "
            "Note: this is not used if `database` or `sqlalchemy_uri` are provided."
        ),
    )
    database: Optional[str] = Field(
        default=None,
        description="Database (catalog). If set to None, all databases will be considered for ingestion.",
    )
