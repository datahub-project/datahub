import re
from typing import Dict, List, Optional

from pydantic import Field, validator
from sqlglot import dialects

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig


class SSLConfig(ConfigModel):
    """SSL Certificate configuration"""

    cert_path: Optional[str] = Field(
        default=None,
        description="Path to SSL certificate file",
    )
    cert_content: Optional[str] = Field(
        default=None,
        description="Base64 encoded certificate content",
    )
    cert_type: str = Field(
        default="pem",
        description="Certificate type (pem, jks, p12)",
    )
    cert_password: Optional[str] = Field(
        default=None,
        description="Certificate password if required",
    )

    @validator("cert_type")
    def validate_cert_type(cls, v: str) -> str:
        valid_types = ["pem", "jks", "p12"]
        if v.lower() not in valid_types:
            raise ValueError(f"cert_type must be one of: {', '.join(valid_types)}")
        return v.lower()


class JDBCConnectionConfig(ConfigModel):
    """JDBC Connection configuration"""

    uri: str = Field(
        description="JDBC URI (jdbc:protocol://host:port/database)",
    )
    username: Optional[str] = Field(
        default=None,
        description="Database username",
    )
    password: Optional[str] = Field(
        default=None,
        description="Database password",
    )
    properties: Dict[str, str] = Field(
        default_factory=dict,
        description="Additional JDBC properties",
    )
    ssl_config: Optional[SSLConfig] = Field(
        default=None,
        description="SSL configuration",
    )

    @validator("uri")
    def validate_uri(cls, v: str) -> str:
        if not v.startswith("jdbc:"):
            raise ValueError("URI must start with 'jdbc:'")
        return v


class JDBCDriverConfig(ConfigModel):
    """JDBC Driver configuration"""

    driver_class: str = Field(
        description="Fully qualified JDBC driver class name",
    )
    driver_path: Optional[str] = Field(
        default=None,
        description="Path to JDBC driver JAR",
    )
    maven_coordinates: Optional[str] = Field(
        default=None,
        description="Maven coordinates (groupId:artifactId:version)",
    )

    @validator("driver_class")
    def validate_driver_class(cls, v: str) -> str:
        if not v:
            raise ValueError("driver_class must be specified")
        return v

    @validator("maven_coordinates")
    def validate_maven_coordinates(cls, v: Optional[str]) -> Optional[str]:
        if v and not re.match(r"^[^:]+:[^:]+:[^:]+$", v):
            raise ValueError(
                "maven_coordinates must be in format 'groupId:artifactId:version'"
            )
        return v


class JDBCSourceConfig(
    StatefulIngestionConfigBase,
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
):
    """Configuration for JDBC metadata extraction"""

    driver: JDBCDriverConfig = Field(
        description="JDBC driver configuration",
    )
    connection: JDBCConnectionConfig = Field(
        description="Database connection configuration",
    )
    platform: str = Field(
        description="Name of platform being ingested.",
    )
    include_tables: bool = Field(
        default=True,
        description="Include tables in extraction",
    )
    include_views: bool = Field(
        default=True,
        description="Include views in extraction",
    )
    include_stored_procedures: bool = Field(
        default=False,
        description="Include stored procedures in extraction",
    )
    sqlglot_dialect: Optional[str] = Field(
        default=None,
        description="sqlglot dialect to use for SQL transpiling",
    )
    jvm_args: List[str] = Field(
        default=[],
        description="JVM arguments for JDBC driver",
    )
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for schemas",
    )
    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables",
    )
    view_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for views",
    )
    usage: BaseUsageConfig = Field(
        description="Usage statistics configuration",
        default=BaseUsageConfig(),
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @validator("sqlglot_dialect")
    def validate_dialect(cls, v):
        if v is None:
            return v
        valid_dialects = [d for d in dir(dialects) if not d.startswith("_")]
        if v not in valid_dialects:
            raise ValueError(
                f"Invalid dialect '{v}'. Must be one of: {', '.join(sorted(valid_dialects))}"
            )
        return v
