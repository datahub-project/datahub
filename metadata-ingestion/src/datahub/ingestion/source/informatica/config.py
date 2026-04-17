import logging
from typing import Dict, List, Optional

from pydantic import Field, model_validator

from datahub.configuration.common import AllowDenyPattern, TransparentSecretStr
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulIngestionConfigBase,
    StatefulStaleMetadataRemovalConfig,
)

logger = logging.getLogger(__name__)

ORCHESTRATOR = "informatica"

# IDMC connParams["Connection Type"] → DataHub platform.
CONNECTION_TYPE_MAP: Dict[str, str] = {
    "Oracle": "oracle",
    "Snowflake_Cloud_Data_Warehouse": "snowflake",
    "SqlServer_ODBC": "mssql",
    "MySql": "mysql",
    "PostgreSql": "postgres",
    "Redshift": "redshift",
    "TOOLKIT_CCI_DB2": "db2",
    "TOOLKIT_CCI_SAP_HANA": "hana",
    "S3": "s3",
    "TOOLKIT_CCI_GOOGLE_BIG_QUERY": "bigquery",
    "TOOLKIT_CCI_AZURE_SQL": "mssql",
    "TOOLKIT_CCI_AZURE_SYNAPSE": "mssql",
    "TOOLKIT_CCI_DATABRICKS": "databricks",
    "TOOLKIT_CCI_TERADATA": "teradata",
}

# Default login URLs per IDMC region.
IDMC_REGION_LOGIN_URLS: Dict[str, str] = {
    "us": "https://dm-us.informaticacloud.com",
    "us2": "https://dm2-us.informaticacloud.com",
    "emea": "https://dm-em.informaticacloud.com",
    "apac": "https://dm-ap.informaticacloud.com",
}

DEFAULT_PAGE_SIZE = 200
DEFAULT_EXPORT_BATCH_SIZE = 1000
DEFAULT_EXPORT_POLL_TIMEOUT_SECS = 300
DEFAULT_EXPORT_POLL_INTERVAL_SECS = 5


class InformaticaSourceConfig(
    PlatformInstanceConfigMixin, EnvConfigMixin, StatefulIngestionConfigBase
):
    """Configuration for Informatica Cloud (IDMC) ingestion source."""

    # Authentication
    login_url: str = Field(
        default="https://dm-us.informaticacloud.com",
        description=(
            "Informatica Cloud login URL. This is the regional pod URL, not the "
            "runtime serverUrl. After login, the connector discovers the actual "
            "API base URL from the login response. "
            "Common values: https://dm-us.informaticacloud.com (US), "
            "https://dm2-us.informaticacloud.com (US2), "
            "https://dm-em.informaticacloud.com (EMEA), "
            "https://dm-ap.informaticacloud.com (APAC)."
        ),
    )

    username: str = Field(
        description="Informatica Cloud username (email or service account name).",
    )

    password: TransparentSecretStr = Field(
        description="Informatica Cloud password.",
    )

    # Filtering — Layer 1: Scope by org structure
    project_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter IDMC projects by name. "
            "Only projects matching these patterns will be ingested. "
            "Example: allow: ['Production.*'], deny: ['.*_sandbox']"
        ),
    )

    folder_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter IDMC folders by name. "
            "Only folders matching these patterns will be ingested."
        ),
    )

    # Filtering — Layer 2: Fine-tune by name
    mapping_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter mappings by name. "
            "Applied globally across all matched projects/folders."
        ),
    )

    taskflow_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter taskflows by name. "
            "Applied globally across all matched projects/folders."
        ),
    )

    # Filtering — Layer 3: Tag-based (recommended for large orgs)
    tag_filter_names: List[str] = Field(
        default=[],
        description=(
            "List of literal IDMC tag names. When set, only objects tagged with "
            "at least one of these tags will be ingested. Tags are matched exactly "
            "(not regex). This is the recommended filtering approach for large orgs — "
            "IDMC admins tag objects in the UI and the connector picks them up."
        ),
    )

    # Features
    extract_lineage: bool = Field(
        default=True,
        description=(
            "Whether to extract table-level lineage from mapping definitions. "
            "Requires the 'Asset - export' privilege on the service account. "
            "When enabled, uses the v3 Export API to fetch full mapping definitions."
        ),
    )

    extract_ownership: bool = Field(
        default=True,
        description=(
            "Whether to extract ownership from IDMC object createdBy/updatedBy fields."
        ),
    )

    extract_tags: bool = Field(
        default=True,
        description="Whether to extract tags from IDMC objects.",
    )

    # Connection platform override
    connection_type_overrides: Dict[str, str] = Field(
        default={},
        description=(
            "Manual overrides mapping IDMC connection ID to DataHub platform name. "
            "Use when automatic platform detection via connParams fails. "
            "Example: {'01DM180B000000000008': 'snowflake'}"
        ),
    )

    # Performance tuning
    page_size: int = Field(
        default=DEFAULT_PAGE_SIZE,
        description="Number of objects to fetch per API page (max 200 for v3 objects).",
        ge=1,
        le=200,
    )

    export_batch_size: int = Field(
        default=DEFAULT_EXPORT_BATCH_SIZE,
        description="Number of mappings per v3 export batch job (max 1000).",
        ge=1,
        le=1000,
    )

    export_poll_timeout_secs: int = Field(
        default=DEFAULT_EXPORT_POLL_TIMEOUT_SECS,
        description="Timeout in seconds for polling export job completion.",
        ge=30,
    )

    export_poll_interval_secs: int = Field(
        default=DEFAULT_EXPORT_POLL_INTERVAL_SECS,
        description="Interval in seconds between export job status polls.",
        ge=1,
    )

    # Stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Configuration for stateful ingestion and stale entity removal.",
    )

    @model_validator(mode="after")
    def validate_login_url(self) -> "InformaticaSourceConfig":
        url = self.login_url.rstrip("/")
        if not url.startswith("https://"):
            raise ValueError(
                f"login_url must start with https://, got: {self.login_url}"
            )
        self.login_url = url
        return self
