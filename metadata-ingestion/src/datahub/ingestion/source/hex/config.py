from typing import Dict, Optional

from pydantic import Field, SecretStr, field_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.ingestion.source.hex.constants import (
    HEX_API_BASE_URL_DEFAULT,
    HEX_API_PAGE_SIZE_DEFAULT,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class HexConnectionDetail(ConfigModel):
    """Per-connection override for upstream lineage URN construction."""

    platform: Optional[str] = Field(
        default=None,
        description="DataHub platform name. Required only when Hex's connection "
        "type cannot be auto-resolved (deleted connections, permission gaps, "
        "custom types).",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="DataHub platform_instance the underlying warehouse was "
        "ingested under. Leave unset for warehouses ingested without one "
        "(e.g. typical BigQuery).",
    )
    default_database: Optional[str] = Field(
        default=None,
        description=(
            "Default outer-scope qualifier for unqualified table refs in SQL "
            "cells. For BigQuery this is the GCP project ID; for "
            "Snowflake/Postgres/Redshift/MSSQL the database; for "
            "Trino/Databricks/Presto the catalog. Leave empty for 2-part platforms "
            "(MySQL/MariaDB/Clickhouse) — set only `default_schema` there. "
            "Overrides the value auto-extracted from Hex's "
            "/v1/data-connections response."
        ),
    )
    default_schema: Optional[str] = Field(
        default=None,
        description=(
            "Default inner-scope qualifier for unqualified table refs in SQL "
            "cells. For BigQuery this is the dataset; for "
            "Snowflake/Postgres/Redshift/MSSQL/Trino/Databricks/Presto/Athena "
            "the schema; for MySQL/MariaDB/Clickhouse the database name. "
            "Overrides the value auto-extracted from Hex's "
            "/v1/data-connections response."
        ),
    )


class HexSourceConfig(
    StatefulIngestionConfigBase, PlatformInstanceConfigMixin, EnvConfigMixin
):
    workspace_name: str = Field(
        description="Hex workspace name. Find it in the workspace switcher dropdown in the top-left corner of the Hex app.",
    )
    workspace_id: Optional[str] = Field(
        default=None,
        description=(
            "Hex workspace (org) UUID, used to build external URLs to the "
            "Hex app (e.g. https://app.hex.tech/<workspace_id>/hex/<project_id>). "
            "If left unset, the connector calls /users/me to auto-discover it — "
            "which requires the token to have 'Users → Read access'. Set this "
            "explicitly to avoid granting that scope. Find the UUID in any Hex "
            "project URL."
        ),
    )
    token: SecretStr = Field(
        description=(
            "Hex Workspace Token with the 'Read projects' scope. "
            "Create one at Settings → API → Workspace tokens. "
            "The 'Read projects' scope is required to access project cells for lineage; "
            "tokens without it can enumerate projects but not read their content. "
            "See https://learn.hex.tech/docs/api-integrations/api/overview for token types."
        ),
    )
    base_url: str = Field(
        default=HEX_API_BASE_URL_DEFAULT,
        description="Hex API base URL. For most Hex users, this will be https://app.hex.tech/api/v1. "
        "Single-tenant app users should replace this with the URL they use to access Hex.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Configuration for stateful ingestion and stale metadata removal.",
    )
    include_components: bool = Field(
        default=True,
        description="Include Hex Components in the ingestion",
    )
    page_size: int = Field(
        default=HEX_API_PAGE_SIZE_DEFAULT,
        ge=1,
        description="Number of items to fetch per Hex API call.",
    )
    patch_metadata: bool = Field(
        default=False,
        description="Emit metadata as patch events",
    )
    collections_as_tags: bool = Field(
        default=True,
        description="Emit Hex Collections as tags",
    )
    status_as_tag: bool = Field(
        default=True,
        description="Emit Hex Status as tags",
    )
    categories_as_tags: bool = Field(
        default=True,
        description="Emit Hex Category as tags",
    )
    project_title_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex pattern for project titles to filter in ingestion.",
    )
    component_title_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex pattern for component titles to filter in ingestion.",
    )
    set_ownership_from_email: bool = Field(
        default=True,
        description="Set ownership identity from owner/creator email",
    )
    include_lineage: bool = Field(
        default=True,
        description="Extract upstream lineage. Uses queriedTables API (Hex Enterprise "
        "workspaces) or falls back to parsing SQL from cells (all workspaces). "
        "No warehouse ingestion dependency required.",
    )
    use_queried_tables_lineage: bool = Field(
        default=False,
        description=(
            "Use Hex's queriedTables API (Hex Enterprise workspaces only) as the "
            "primary lineage source for published projects and components. Unpublished "
            "entities always fall back to SQL-cell parsing since queriedTables is only "
            "populated for published runs. Set to False to force SQL-cell parsing for "
            "everything."
        ),
    )
    connection_platform_map: Dict[str, HexConnectionDetail] = Field(
        default_factory=dict,
        description=(
            "Per-connection lineage configuration, keyed by Hex dataConnectionId (UUID). "
            "Pins platform and platform_instance so upstream URNs match the warehouse's "
            "ingestion. "
            'Example: {"<uuid>": {"platform": "snowflake", "platform_instance": "prod_snowflake"}}'
        ),
    )

    include_run_history: bool = Field(
        default=True,
        description="Emit the most recent COMPLETED run as a DashboardInfo PATCH "
        "setting lastRefreshed.",
    )
    include_context_documents: bool = Field(
        default=False,
        description="Emit a DataHub Document per Project and per Component containing "
        "SQL sources, visualisation metadata, and notebook documentation. Documents "
        "are hidden from global search and linked to the Dashboard/Chart for AI agent "
        "retrieval.",
    )
    category_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex pattern for categories to filter in ingestion. This will exclude any project or component that has any category denied or not explicitly allowed.",
    )
    max_projects: Optional[int] = Field(
        default=None,
        ge=1,
        description="Maximum number of projects to process. Useful for testing or staged rollouts. "
        "Components discovered during project processing are not counted. "
        "Defaults to None (process all projects). "
        "WARNING: with stateful ingestion enabled, projects beyond this limit are soft-deleted "
        "on the next run.",
    )

    @field_validator("base_url")
    @classmethod
    def _validate_base_url(cls, value: str) -> str:
        if not value.startswith(("http://", "https://")):
            raise ValueError(
                f"base_url must start with 'http://' or 'https://', got: {value!r}"
            )
        return value.rstrip("/")

    # Removed fields — emit a clear warning rather than silently ignoring.
    # These were used by the old DataHub-query-fetcher lineage path which searched
    # DataHub for Query entities tagged with Hex metadata comments. Lineage now comes
    # directly from the Hex REST API and none of these fields have any effect.
    _lineage_start_time_removed = pydantic_removed_field(
        "lineage_start_time", month="May", year=2026
    )
    _lineage_end_time_removed = pydantic_removed_field(
        "lineage_end_time", month="May", year=2026
    )
    _datahub_page_size_removed = pydantic_removed_field(
        "datahub_page_size", month="May", year=2026
    )
