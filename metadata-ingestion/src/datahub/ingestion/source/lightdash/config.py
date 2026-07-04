"""Pydantic v2 config for the Lightdash DataHub source.

Layout follows the BI-tool pattern from ``standards/main.md``: a nested
``LightdashConnectionConfig`` for transport details, a top-level
``LightdashSourceConfig`` mixing in the standard
:class:`PlatformInstanceConfigMixin`, :class:`EnvConfigMixin` and
:class:`StatefulIngestionConfigBase`.
"""

from __future__ import annotations

from typing import Optional

import pydantic
from pydantic import Field, field_validator

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

PLATFORM_NAME = "lightdash"

# Lightdash ``warehouseType`` → DataHub platform name.
# Anything not in this table falls back to ``PLATFORM_NAME`` and a report warning,
# so charts still get *some* upstream URN. Users can override via
# ``warehouse_platform`` when their warehouse needs a non-standard DataHub name.
WAREHOUSE_TYPE_TO_PLATFORM: dict[str, str] = {
    "clickhouse": "clickhouse",
    "postgres": "postgres",
    "redshift": "redshift",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "databricks": "databricks",
    "trino": "trino",
    "duckdb": "duckdb",
}


class LightdashConnectionConfig(ConfigModel):
    """Transport-level config for the Lightdash API."""

    base_url: str = Field(
        description=(
            "Public base URL of the Lightdash instance, e.g. "
            "``https://lightdash.example.com``. Trailing slash is trimmed."
        ),
    )
    personal_access_token: pydantic.SecretStr = Field(
        description=(
            "Lightdash Personal Access Token (``ldpat_...``). Create one under "
            "*Settings → Personal access tokens* on the Lightdash UI."
        ),
    )
    timeout_seconds: int = Field(
        default=30,
        description="HTTP request timeout in seconds.",
    )
    max_retries: int = Field(
        default=3,
        description="Max HTTP retries on 429/5xx responses (exponential backoff).",
    )
    verify_ssl: bool = Field(
        default=True,
        description="Whether to verify the TLS certificate of the Lightdash server.",
    )

    @field_validator("base_url")
    @classmethod
    def _validate_base_url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        return v.rstrip("/")


class LightdashSourceConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    """Top-level config for the Lightdash source."""

    connection: LightdashConnectionConfig = Field(
        description="Connection details for the Lightdash instance.",
    )

    # ---- scope ----
    project_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex allow/deny patterns matched against Lightdash project names.",
    )
    space_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex allow/deny patterns matched against Lightdash space names.",
    )
    dashboard_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex allow/deny patterns matched against dashboard names.",
    )
    chart_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex allow/deny patterns matched against chart names.",
    )
    include_organization_container: bool = Field(
        default=False,
        description=(
            "When True, emit the Lightdash Organization as a top-level container "
            "and parent each project under it. Most installations leave this off "
            "because they only have one Lightdash Organization and it adds an "
            "uninformative layer above the project hierarchy."
        ),
    )

    # ---- features ----
    extract_owners: bool = Field(
        default=True,
        description=(
            "Attach Dashboard/Chart ownership from Lightdash's ``updatedByUser``. "
            "Owner URN format: ``urn:li:corpuser:<email>`` if email is exposed by "
            "the Lightdash API, else ``urn:li:corpuser:<userUuid>``."
        ),
    )
    extract_lineage: bool = Field(
        default=True,
        description="Emit Chart → upstream warehouse Dataset lineage from Explores.",
    )

    # ---- warehouse URN resolution ----
    warehouse_platform: Optional[str] = Field(
        default=None,
        description=(
            "DataHub data-platform name for the warehouse charts read from "
            "(e.g. ``clickhouse``, ``snowflake``, ``bigquery``). When omitted, "
            "the source auto-detects from the project's ``warehouseType``. "
            "Set this explicitly if your DataHub ClickHouse/Snowflake source "
            "uses a non-default platform name."
        ),
    )
    warehouse_platform_instance: Optional[str] = Field(
        default=None,
        description=(
            "Optional ``platformInstance`` for the upstream warehouse — must match "
            "the value used by your existing DataHub warehouse source so chart "
            "lineage joins onto the right Dataset URNs."
        ),
    )
    warehouse_database_override: Optional[str] = Field(
        default=None,
        description=(
            "Force a specific database name in upstream URNs (e.g. ``default`` for "
            "ClickHouse). When omitted, the source uses each Explore's ``schema`` "
            "field — which matches the warehouse's compiled SQL path."
        ),
    )
    warehouse_env: Optional[str] = Field(
        default=None,
        description=(
            "Fabric/environment for upstream warehouse URNs (PROD/DEV/STAGING). "
            "Defaults to the source's top-level ``env`` setting."
        ),
    )

    # ---- stateful ingestion ----
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description=(
            "Stateful ingestion configuration — enables soft-deletion of dashboards "
            "and charts that disappear from Lightdash between runs."
        ),
    )

    def resolve_warehouse_platform(
        self, warehouse_type: Optional[str]
    ) -> tuple[str, bool]:
        """Return ``(platform_name, was_auto_detected)``.

        Explicit ``warehouse_platform`` always wins. Otherwise we look up
        ``warehouse_type`` (the value Lightdash reports under
        ``project.warehouseConnection.type``) in :data:`WAREHOUSE_TYPE_TO_PLATFORM`,
        falling back to the Lightdash platform itself when unknown — that way
        charts still get an upstream URN, just one the user is expected to
        retarget via ``warehouse_platform``.
        """
        if self.warehouse_platform:
            return self.warehouse_platform, False
        if warehouse_type and warehouse_type in WAREHOUSE_TYPE_TO_PLATFORM:
            return WAREHOUSE_TYPE_TO_PLATFORM[warehouse_type], True
        return PLATFORM_NAME, True
