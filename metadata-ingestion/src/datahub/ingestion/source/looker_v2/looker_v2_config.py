"""
Looker V2 Source Configuration.

Unified source for Looker dashboards, explores, and LookML views.
"""

from __future__ import annotations

from typing import Any, Dict, Literal, Optional, Union

import pydantic
from pydantic import Field, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.ingestion.source.looker.looker_config import (
    LookerCommonConfig,
    LookerConnectionDefinition,
)
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPIConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)

__all__ = [
    "LookerCommonConfig",
    "LookerConnectionDefinition",
    "LookerV2Config",
    "LookerV2GitInfo",
]


class LookerV2GitInfo(ConfigModel):
    """Git repository information for cloning LookML projects."""

    repo: str = Field(description="Git repository URL")
    branch: str = Field(default="main", description="Git branch to checkout")
    deploy_key: Optional[pydantic.SecretStr] = Field(
        default=None, description="SSH deploy key for private repositories"
    )


class LookerV2Config(
    LookerAPIConfig,
    LookerCommonConfig,
    StatefulIngestionConfigBase,
):
    """
    Looker V2 source configuration.

    Extracts dashboards, looks, explores, and views from Looker.
    """

    # ==================== What to Extract ====================
    extract_dashboards: bool = Field(
        default=True,
        description="Extract dashboards and their charts.",
    )
    extract_looks: bool = Field(
        default=True,
        description="Extract standalone Looks.",
    )
    extract_explores: bool = Field(
        default=True,
        description="Extract LookML Explores as datasets.",
    )
    extract_views: bool = Field(
        default=False,
        description="Extract LookML Views. Requires base_folder or git_info.",
    )

    # ==================== LookML Project ====================
    base_folder: Optional[str] = Field(
        default=None,
        description="Path to LookML project folder. Alternative to git_info.",
    )
    git_info: Optional[LookerV2GitInfo] = Field(
        default=None,
        description="Git repository for LookML files. Alternative to base_folder.",
    )
    project_name: Optional[str] = Field(
        default=None,
        description="LookML project name. Auto-detected from manifest.lkml if not set.",
    )
    project_dependencies: Dict[str, Union[str, LookerV2GitInfo]] = Field(
        default_factory=dict,
        description="Map dependent project names to local paths or git repos.",
    )

    # ==================== Filtering ====================
    dashboard_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filter dashboards by title.",
    )
    chart_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filter charts (dashboard elements) by their Looker element ID.",
    )
    explore_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filter explores by name.",
    )
    view_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filter views by name.",
    )
    model_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filter LookML models by name.",
    )
    folder_path_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filter dashboards by folder path (e.g., 'Shared/Sales').",
    )
    emit_used_explores_only: bool = Field(
        default=True,
        description="When enabled, only explores referenced by at least one dashboard or look are emitted. "
        "Disable to emit all explores regardless of usage.",
    )

    # ==================== Common Options ====================
    extract_owners: bool = Field(
        default=True,
        description="Extract ownership from Looker.",
    )
    extract_usage_history: bool = Field(
        default=True,
        description="Extract usage statistics for dashboards and looks.",
    )
    extract_usage_history_for_interval: str = Field(
        default="30 days",
        description="Interval to extract Looker usage history for. "
        "See https://docs.looker.com/reference/filter-expressions#date_and_time.",
    )
    skip_personal_folders: bool = Field(
        default=False,
        description="Skip dashboards and looks in personal folders.",
    )
    include_deleted: bool = Field(
        default=False,
        description="Include deleted dashboards/looks.",
    )
    strip_user_ids_from_email: bool = Field(
        default=False,
        description="When enabled, converts Looker user emails of the form name@domain.com "
        "to urn:li:corpuser:name when assigning ownership.",
    )
    include_platform_instance_in_urns: bool = Field(
        default=False,
        description="When enabled, platform instance will be added in dashboard and chart URNs. "
        "Required for migration compatibility if V1 had platform_instance set.",
    )
    external_base_url: Optional[str] = Field(
        default=None,
        description="Optional URL for constructing external URLs to Looker if base_url "
        "differs from the user-facing URL. Defaults to base_url.",
    )
    extract_embed_urls: bool = Field(
        default=True,
        description="Extract embed URLs for dashboards and explores.",
    )

    # ==================== Lineage ====================
    connection_to_platform_map: Dict[str, LookerConnectionDefinition] = Field(
        default_factory=dict,
        description="Map Looker connections to DataHub platforms for lineage. "
        "Connections not listed here will be auto-discovered from the Looker API.",
    )
    enable_api_sql_lineage: bool = Field(
        default=True,
        description="Use both Looker's PDT dependency graph API and generate_sql_query API to resolve "
        "lineage for views reachable from explores. The PDT graph provides PDT-to-PDT edges; "
        "generate_sql_query returns fully rendered SQL (resolving all Liquid templates and LookML "
        "references) from which table-level and column-level lineage are extracted. Results from both "
        "APIs are combined. Falls back to SQL regex parsing if API calls fail or the view is not "
        "reachable from any explore. Requires the 'develop' permission on the Looker API client; "
        "set to False if your credentials lack it.",
    )
    api_sql_lineage_field_chunk_size: int = Field(
        default=100,
        description="When resolving lineage via generate_sql_query, views with more fields than this "
        "threshold are split into multiple API calls to avoid SQL generation failures on large field sets.",
    )
    api_sql_lineage_individual_field_fallback: bool = Field(
        default=True,
        description="When a field chunk fails SQL generation, retry each field individually to maximise "
        "lineage coverage and isolate problematic fields.",
    )
    emit_unreachable_views: bool = Field(
        default=False,
        description="Emit views not referenced by any explore.",
    )
    process_refinements: bool = Field(
        default=False,
        description="Process LookML view refinements.",
    )
    expand_refinement_lineage: bool = Field(
        default=False,
        description="When False (default), refinements are merged into the base view "
        "for V1 compatibility. When True, each refinement node becomes a separate "
        "Dataset entity with a lineage chain.",
    )

    # ==================== Performance ====================
    max_concurrent_requests: int = Field(
        default=10,
        description="Max concurrent API requests. Reduce if hitting rate limits.",
    )

    # ==================== Template Variables ====================
    liquid_variables: Dict[str, Union[str, int, float, bool]] = Field(
        default_factory=dict,
        description="Variables for Liquid template substitution in LookML SQL. "
        "These resolve {{ variable }} syntax in sql_table_name and derived_table.sql.",
    )
    lookml_constants: Dict[str, str] = Field(
        default_factory=dict,
        description="LookML constants for @{constant_name} substitution. "
        "If a constant is defined in manifest.lkml, the manifest value takes precedence.",
    )
    looker_environment: Literal["prod", "dev"] = Field(
        default="prod",
        description="Looker environment (prod or dev). Controls evaluation of "
        "'-- if prod --' / '-- if dev --' comment directives in LookML SQL.",
    )

    # ==================== Stateful Ingestion ====================
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Enable stateful ingestion for stale entity removal.",
    )

    # ==================== Remove Deprecated Inherited Fields ====================
    _remove_explore_browse_pattern = pydantic_removed_field("explore_browse_pattern")
    _remove_view_browse_pattern = pydantic_removed_field("view_browse_pattern")

    # ==================== Validators ====================
    @model_validator(mode="before")
    @classmethod
    def external_url_defaults_to_base_url(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Default external_base_url to base_url if not provided."""
        if isinstance(values, dict):
            if "external_base_url" not in values or values["external_base_url"] is None:
                values["external_base_url"] = values.get("base_url")
        return values

    @model_validator(mode="after")
    def validate_views_require_lookml_access(self) -> "LookerV2Config":
        """Validate that base_folder or git_info is provided for views."""
        if self.extract_views and not self.base_folder and not self.git_info:
            raise ValueError(
                "Either 'base_folder' or 'git_info' must be provided when 'extract_views' is True"
            )
        return self

    @model_validator(mode="after")
    def validate_looks_require_stateful_ingestion(self) -> "LookerV2Config":
        """Validate that stateful ingestion is enabled when extracting Looks.

        Without stale entity removal, Looks that are deleted in Looker will
        remain as stale entities in DataHub indefinitely.
        """
        if self.extract_looks and (
            self.stateful_ingestion is None or not self.stateful_ingestion.enabled
        ):
            raise ValueError(
                "stateful_ingestion.enabled must be set to true when extract_looks is enabled. "
                "Without stateful ingestion, deleted Looks will accumulate as stale entities in DataHub."
            )
        return self

    @field_validator("project_dependencies", mode="before")
    @classmethod
    def parse_project_dependencies(
        cls, v: Dict[str, Any]
    ) -> Dict[str, Union[str, LookerV2GitInfo]]:
        """Parse project dependencies, converting dicts to GitInfo."""
        if not v:
            return {}
        result: Dict[str, Union[str, LookerV2GitInfo]] = {}
        for name, value in v.items():
            if isinstance(value, str):
                result[name] = value
            elif isinstance(value, dict):
                result[name] = LookerV2GitInfo(**value)
            elif isinstance(value, LookerV2GitInfo):
                result[name] = value
            else:
                raise ValueError(f"Invalid project dependency for '{name}': {value}")
        return result
