from typing import Annotated, Dict, Optional, Sequence

from pydantic import Field, SecretStr, field_validator

from datahub.configuration.common import AllowDenyPattern, Filters, HiddenFromDocs
from datahub.configuration.source_common import (
    DatasetLineageProviderConfigBase,
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.agent.verdicts import Verdict, VerdictContext
from datahub.ingestion.source.common.subtypes import BIContainerSubTypes
from datahub.ingestion.source.grafana.grafana_selection import (
    dashboard_verdict,
    folder_verdict,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities import config_clean


class PlatformConnectionConfig(
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
):
    """Platform connection configuration for mapping Grafana datasources to their actual platforms."""

    platform: str = Field(
        description="The platform name (e.g., 'postgres', 'mysql', 'snowflake')"
    )
    database: Optional[str] = Field(default=None, description="Default database name")
    database_schema: Optional[str] = Field(
        default=None, description="Default schema name"
    )


class GrafanaSourceConfig(
    DatasetLineageProviderConfigBase,
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    """Configuration for Grafana source"""

    platform: HiddenFromDocs[str] = Field(default="grafana")
    url: str = Field(
        description="Grafana URL in the format http://your-grafana-instance with no trailing slash"
    )
    service_account_token: SecretStr = Field(
        description="Service account token for Grafana"
    )
    verify_ssl: bool = Field(
        default=True,
        description="Whether to verify SSL certificates when connecting to Grafana",
    )

    # API pagination configuration
    page_size: int = Field(
        default=100,
        description="Number of items to fetch per API call when paginating through folders and dashboards",
    )

    # Extraction mode configuration
    basic_mode: bool = Field(
        default=False,
        description="Enable basic extraction mode for users with limited permissions. "
        "In basic mode, only dashboard metadata is extracted without detailed panel information, "
        "lineage, or folder hierarchy. This requires only basic dashboard read permissions.",
    )

    # Content filtering
    dashboard_pattern: Annotated[
        AllowDenyPattern, Filters(BIContainerSubTypes.GRAFANA_DASHBOARD)
    ] = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex pattern to filter dashboards for ingestion",
    )
    folder_pattern: Annotated[
        AllowDenyPattern, Filters(BIContainerSubTypes.GRAFANA_FOLDER)
    ] = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex pattern to filter folders for ingestion",
    )

    # Feature toggles
    ingest_tags: bool = Field(
        default=True, description="Whether to ingest dashboard and chart tags"
    )
    ingest_owners: bool = Field(
        default=True, description="Whether to ingest dashboard ownership information"
    )
    remove_email_suffix: bool = Field(
        True,
        description="Remove Grafana user email suffix for example, @acryl.io, "
        "when assigning ownership.",
    )
    skip_text_panels: bool = Field(
        default=False,
        description="Whether to skip text panels during ingestion. "
        "Text panels don't contain data visualizations and may not be relevant for data lineage.",
    )

    include_lineage: bool = Field(
        default=True,
        description="Whether to extract lineage between charts and data sources. "
        "When enabled, the source will parse SQL queries and datasource configurations "
        "to build lineage relationships.",
    )
    include_column_lineage: bool = Field(
        default=True,
        description="Whether to extract column-level lineage from SQL queries. "
        "Only applicable when include_lineage is enabled.",
    )

    # Platform connection mappings
    connection_to_platform_map: Dict[str, PlatformConnectionConfig] = Field(
        default_factory=dict,
        description="Map of Grafana datasource types/UIDs to platform connection configs for lineage extraction",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description="Stateful ingestion configuration"
    )

    @field_validator("url", mode="after")
    @classmethod
    def remove_trailing_slash(cls, v: str) -> str:
        return config_clean.remove_trailing_slashes(v)

    @classmethod
    def probe_provider_class(cls) -> type:
        # Late import: grafana_probe imports this module, and ingestion never
        # needs the probe.
        from datahub.ingestion.source.grafana.grafana_probe import (
            GrafanaMetadataProbe,
        )

        return GrafanaMetadataProbe

    def probe_ancestor_kinds(self, kind: str) -> Optional[Sequence[str]]:
        """Neither kind lists an ancestor. Grafana ingestion fetches folders
        and dashboards as two independent listings, so a dashboard inside a
        folder folder_pattern drops is still ingested; a --parent folder must
        not exclude it."""
        return {
            str(BIContainerSubTypes.GRAFANA_FOLDER): (),
            str(BIContainerSubTypes.GRAFANA_DASHBOARD): (),
        }.get(kind)

    def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
        """Both kinds through grafana_selection, which ingestion also calls:
        a folder is dropped in basic_mode whatever folder_pattern says."""
        if ctx.kind == BIContainerSubTypes.GRAFANA_FOLDER:
            return folder_verdict(self, ctx.target)
        if ctx.kind == BIContainerSubTypes.GRAFANA_DASHBOARD:
            return dashboard_verdict(self, ctx.target)
        return None
