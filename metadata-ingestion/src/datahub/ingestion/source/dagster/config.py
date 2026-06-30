from dataclasses import dataclass, field
from typing import Optional

import pydantic
from pydantic import Field, field_validator, model_validator
from typing_extensions import Literal

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.metadata.schema_classes import OwnershipTypeClass
from datahub.utilities.lossy_collections import LossyList

DAGSTER_PLATFORM = "dagster"

_VALID_OWNERSHIP_TYPES = {
    value
    for value in vars(OwnershipTypeClass).values()
    if isinstance(value, str) and value.isupper()
}


class DagsterSourceConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    """Configuration for the pull-based Dagster ingestion source."""

    host: str = Field(
        default="http://localhost:3000",
        description=(
            "Base URL of the Dagster webserver. For Dagster+, use the "
            "organization host, e.g. `https://my-org.dagster.cloud`."
        ),
    )

    is_cloud: bool = Field(
        default=False,
        description=(
            "Set to true for Dagster+ (Cloud). Selects the deployment-scoped "
            "GraphQL URL shape and enables token authentication."
        ),
    )

    deployment: Optional[str] = Field(
        default="prod",
        description=(
            "Dagster+ deployment name (or branch deployment). Used to build the "
            "GraphQL URL `<host>/<deployment>/graphql` and to scope entity ids. "
            "Ignored for OSS."
        ),
    )

    token: Optional[pydantic.SecretStr] = Field(
        default=None,
        description="Dagster+ user token, sent as the `Dagster-Cloud-Api-Token` header. Required when `is_cloud` is true.",
    )

    timeout_seconds: int = Field(
        default=30,
        description="HTTP request timeout in seconds.",
    )

    max_retries: int = Field(
        default=3,
        description="Maximum number of retries for transient HTTP failures (429, 5xx).",
    )

    asset_page_size: int = Field(
        default=1000,
        description=(
            "Number of assets to fetch per GraphQL request when paginating a "
            "repository's assets. Lower this if the Dagster webserver times out on "
            "large repositories."
        ),
    )

    extraction_mode: Literal["enrich", "full"] = Field(
        default="enrich",
        description=(
            "How the source writes to DataHub:\n"
            "- `enrich` (default): only enrich assets that already exist in DataHub "
            "(typically created by the Dagster plugin), using non-destructive PATCH "
            "for descriptions, tags, ownership, and lineage. Assets not present are "
            "skipped, and no entity-defining aspects are written. Requires a DataHub "
            "connection (a sink or `datahub_api`).\n"
            "- `full`: create/own the assets as standalone Datasets (table schema, "
            "subtypes, etc.). Use for initial load or when running the connector "
            "without the Dagster plugin."
        ),
    )

    repository_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for repositories / code locations to include.",
    )

    job_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for Dagster jobs to include.",
    )

    asset_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for assets (matched against the dotted asset key) to include.",
    )

    include_assets: bool = Field(
        default=True,
        description="Whether to emit Software-Defined Assets as DataHub Datasets.",
    )

    include_jobs: bool = Field(
        default=False,
        description=(
            "Whether to emit Dagster jobs as DataFlow and ops as DataJob. Disabled by "
            "default so the lineage graph shows only assets; the asset-to-asset "
            "dependency edges are emitted independently via `include_asset_lineage`."
        ),
    )

    include_asset_lineage: bool = Field(
        default=True,
        description="Whether to emit upstream (table-level) lineage between asset Datasets.",
    )

    include_column_lineage: bool = Field(
        default=True,
        description=(
            "Whether to emit column-level (fine-grained) lineage between asset Datasets, "
            "parsed from Dagster's column-lineage asset metadata when present."
        ),
    )

    include_ownership: bool = Field(
        default=True,
        description="Whether to emit ownership extracted from asset/job owners.",
    )

    include_tags: bool = Field(
        default=True,
        description="Whether to emit tags from Dagster tags, kinds, compute kind, and asset group.",
    )

    include_metadata: bool = Field(
        default=True,
        description="Whether to emit descriptions, documentation links, and table schema from asset metadata.",
    )

    default_ownership_type: str = Field(
        default=OwnershipTypeClass.TECHNICAL_OWNER,
        description="DataHub ownership type applied to owners extracted from Dagster.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion configuration (enables stale-entity deletion detection).",
    )

    @field_validator("host")
    @classmethod
    def host_must_be_url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("host must start with http:// or https://")
        return v.rstrip("/")

    @field_validator("default_ownership_type")
    @classmethod
    def ownership_type_is_valid(cls, v: str) -> str:
        if v not in _VALID_OWNERSHIP_TYPES:
            raise ValueError(
                f"default_ownership_type must be one of {sorted(_VALID_OWNERSHIP_TYPES)}"
            )
        return v

    @model_validator(mode="after")
    def cloud_requires_token(self) -> "DagsterSourceConfig":
        if self.is_cloud and not self.token:
            raise ValueError("token is required when is_cloud is true")
        return self


@dataclass
class DagsterSourceReport(StaleEntityRemovalSourceReport):
    repositories_scanned: int = 0
    jobs_scanned: int = 0
    assets_scanned: int = 0
    assets_enriched: int = 0
    assets_skipped_absent: int = 0
    repositories_filtered: int = 0
    jobs_filtered: int = 0
    assets_filtered: int = 0
    filtered: LossyList[str] = field(default_factory=LossyList)

    def report_repository_filtered(self, name: str) -> None:
        self.repositories_filtered += 1
        self.filtered.append(f"repository={name}")

    def report_job_filtered(self, name: str) -> None:
        self.jobs_filtered += 1
        self.filtered.append(f"job={name}")

    def report_asset_filtered(self, name: str) -> None:
        self.assets_filtered += 1
        self.filtered.append(f"asset={name}")
