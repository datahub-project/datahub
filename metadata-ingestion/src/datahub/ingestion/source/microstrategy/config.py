from typing import Optional

from pydantic import Field, SecretStr, field_validator

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
from datahub.utilities import config_clean


class MicroStrategyConnectionConfig(ConfigModel):
    """Connection configuration for MicroStrategy REST API."""

    base_url: str = Field(
        description=(
            "Base URL for MicroStrategy REST API (e.g., 'https://demo.microstrategy.com/MicroStrategyLibrary'). "
            "Should be the URL to the MicroStrategy Library web application. "
            "Do not include '/api' suffix - it will be appended automatically."
        )
    )

    username: Optional[str] = Field(
        default=None,
        description=(
            "MicroStrategy username for authentication. "
            "For demo instances, you can use anonymous access by setting use_anonymous=True and leaving this blank. "
            "For production, use a service account with metadata read permissions."
        ),
    )

    password: Optional[SecretStr] = Field(
        default=None,
        description=(
            "Password for MicroStrategy authentication. "
            "Required if username is provided. "
            "Stored securely and not logged."
        ),
    )

    use_anonymous: bool = Field(
        default=False,
        description=(
            "Use anonymous guest access (for demo instances). "
            "When enabled, username/password are not required. "
            "Only works on MicroStrategy instances configured for guest access."
        ),
    )

    timeout_seconds: int = Field(
        default=30,
        description="HTTP request timeout in seconds. Increase for slow networks or large metadata responses.",
    )

    max_retries: int = Field(
        default=3,
        description="Maximum number of retry attempts for failed API requests. Uses exponential backoff.",
    )

    @field_validator("base_url", mode="after")  # type: ignore[misc]
    @classmethod
    def validate_base_url(cls, v: str) -> str:
        """Validate and normalize base URL."""
        if not v.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        return config_clean.remove_trailing_slashes(v)


class MicroStrategyConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    """Configuration for MicroStrategy source."""

    connection: MicroStrategyConnectionConfig = Field(
        description="Connection settings for MicroStrategy REST API"
    )

    # Filtering patterns
    project_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description=(
            "Regex patterns to filter projects. "
            "Example: {'allow': ['^Production.*'], 'deny': ['^Test.*']}. "
            "Only matching projects will be ingested."
        ),
    )

    folder_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter folders within projects.",
    )

    dashboard_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description=(
            "Regex patterns to filter dashboards (dossiers). "
            "Applies to dashboard names. "
            "Use to exclude personal or test dashboards."
        ),
    )

    report_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter reports.",
    )

    # Feature flags
    include_lineage: bool = Field(
        default=True,
        description=(
            "Extract lineage between dashboards/reports and cubes/datasets. "
            "Shows data flow from cubes to visualizations. "
            "Critical for impact analysis."
        ),
    )

    include_usage: bool = Field(
        default=False,
        description=(
            "Extract usage statistics (view counts, user access). "
            "Requires access to audit logs API. "
            "Increases ingestion time. Disabled by default."
        ),
    )

    include_ownership: bool = Field(
        default=True,
        description=(
            "Extract ownership information (creators, owners). "
            "Automatically links dashboards/reports to their creators. "
            "Enabled by default."
        ),
    )

    include_cube_schema: bool = Field(
        default=True,
        description=(
            "Extract schema (attributes, metrics) from Intelligent Cubes. "
            "Required for column-level lineage. "
            "Enabled by default."
        ),
    )

    # Stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description=(
            "Automatically remove deleted dashboards/reports from DataHub. "
            "Maintains catalog accuracy when objects are deleted in MicroStrategy. "
            "Recommended for production environments."
        ),
    )
