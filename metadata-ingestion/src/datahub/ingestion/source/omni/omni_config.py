from typing import Dict, Optional

import pydantic
from pydantic import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class OmniSourceConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    """Configuration for the Omni BI platform DataHub source."""

    base_url: str = Field(
        description=(
            "Omni instance base URL including the /api suffix, "
            "e.g. https://myorg.omniapp.co/api. "
            "Found in your Omni organization settings."
        )
    )
    api_key: pydantic.SecretStr = Field(
        description=(
            "Omni Organization API key (not a Personal Access Token). "
            "Generate in Omni Admin → API Keys. "
            "The key must have read access to models, documents, and connections."
        )
    )
    page_size: int = Field(
        default=50,
        ge=1,
        le=100,
        description="Number of records per page for paginated Omni API endpoints. Lower values reduce memory usage; higher values speed up ingestion.",
    )
    max_requests_per_minute: int = Field(
        default=50,
        ge=1,
        le=60,
        description=(
            "Client-side throttle cap for Omni API requests (requests per minute). "
            "Omni's default rate limit is 60 req/min; set lower to leave headroom for other API consumers."
        ),
    )
    timeout_seconds: int = Field(
        default=30,
        ge=5,
        le=120,
        description="HTTP request timeout in seconds for Omni API calls.",
    )
    include_deleted: bool = Field(
        default=False,
        description="Include soft-deleted Omni entities (models, documents) where the API supports it.",
    )
    include_workbook_only: bool = Field(
        default=False,
        description=(
            "Include workbook-only documents that have not been published as a dashboard. "
            "When False (default), only documents with hasDashboard=true are ingested."
        ),
    )
    model_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex allow/deny patterns applied to Omni model IDs. "
            "Use to restrict ingestion to specific models. "
            "Example: allow: ['^prod-.*']"
        ),
    )
    document_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex allow/deny patterns applied to Omni document identifiers. "
            "Use to restrict ingestion to specific dashboards or workbooks."
        ),
    )
    include_column_lineage: bool = Field(
        default=True,
        description=(
            "Extract column-level (fine-grained) lineage from dashboard query fields "
            "back to Omni semantic view fields. "
            "Enables precise field-level impact analysis in DataHub."
        ),
    )
    connection_to_platform: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Map Omni connection IDs to DataHub platform names. "
            "Required when the platform cannot be auto-detected from the connection dialect. "
            "Example: {'abc-123': 'snowflake', 'def-456': 'bigquery'}"
        ),
    )
    connection_to_platform_instance: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Map Omni connection IDs to DataHub platform instance names. "
            "Must exactly match the platform_instance used when ingesting the warehouse. "
            "Example: {'abc-123': 'prod_snowflake'}"
        ),
    )
    connection_to_database: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Map Omni connection IDs to canonical database names used in DataHub URNs. "
            "Use when the database name in Omni differs from the name registered in DataHub."
        ),
    )
    normalize_snowflake_names: bool = Field(
        default=True,
        description=(
            "Upper-case database, schema, and table name components in URNs when the "
            "resolved platform is Snowflake. Snowflake identifiers are case-insensitive "
            "and DataHub's Snowflake connector stores them in upper case by default."
        ),
    )
