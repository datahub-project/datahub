from typing import Any, Dict, List, Optional

from pydantic import Field, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.git import GitInfo
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
from datahub.ingestion.source.zipline.constants import DEFAULT_STAGING_QUERY_DIALECT
from datahub.metadata.schema_classes import OwnershipTypeClass

# Standard ownership-type enum values; a custom ownership type may instead be a
# `urn:li:ownershipType:` URN, which is accepted as-is.
_VALID_OWNERSHIP_TYPES = frozenset(
    value
    for key, value in vars(OwnershipTypeClass).items()
    if key.isupper() and isinstance(value, str)
)
_OWNERSHIP_TYPE_URN_PREFIX = "urn:li:ownershipType:"


class ZiplineOwnerMapping(ConfigModel):
    team_name: str = Field(description="Chronon team name to match on.")
    datahub_owner_urn: str = Field(
        description="DataHub owner URN (e.g. `urn:li:corpGroup:analytics`)."
    )
    datahub_ownership_type: str = Field(
        default=OwnershipTypeClass.TECHNICAL_OWNER,
        description=(
            "Ownership type: a standard value (e.g. `TECHNICAL_OWNER`, "
            "`BUSINESS_OWNER`) or a `urn:li:ownershipType:` URN."
        ),
    )

    @field_validator("datahub_ownership_type")
    def _validate_ownership_type(cls, value: str) -> str:
        if value.startswith(_OWNERSHIP_TYPE_URN_PREFIX):
            return value
        if value not in _VALID_OWNERSHIP_TYPES:
            raise ValueError(
                f"datahub_ownership_type must be one of "
                f"{sorted(_VALID_OWNERSHIP_TYPES)} or a "
                f"'{_OWNERSHIP_TYPE_URN_PREFIX}' URN"
            )
        return value


class ZiplinePlatformDetail(ConfigModel):
    """Per-namespace mapping of a Chronon source namespace to a DataHub platform.

    Modeled on the Airbyte connector's `PlatformDetail` so backing-source URNs
    stitch to the native connector that also ingests those tables.
    """

    platform: Optional[str] = Field(
        default=None,
        description=(
            "DataHub platform for tables in this namespace (e.g. `snowflake`). "
            "Falls back to `default_source_platform` when unset."
        ),
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description=(
            "Platform instance for tables in this namespace. Falls back to "
            "`source_platform_instance`."
        ),
    )
    env: Optional[str] = Field(
        default=None,
        description="Environment (fabric) for tables in this namespace.",
    )
    default_db: Optional[str] = Field(
        default=None,
        description=(
            "Database to prepend when a Chronon table name in this namespace is "
            "under-qualified (i.e. `<namespace>.<table>`) and the platform is "
            "three-tier (e.g. Snowflake `db.schema.table`). Left unset for "
            "two-tier platforms (e.g. Hive `schema.table`)."
        ),
    )
    include_schema_in_urn: Optional[bool] = Field(
        default=None,
        description=(
            "Whether the namespace segment is a schema that belongs in the URN. "
            "`None` auto-detects (three-tier when `default_db` is set and differs "
            "from the namespace); `True` forces `db.schema.table`; `False` forces "
            "`db.table`."
        ),
    )
    convert_urns_to_lowercase: Optional[bool] = Field(
        default=None,
        description=(
            "Lowercase table and column names for this namespace. Falls back to "
            "the top-level `convert_urns_to_lowercase`."
        ),
    )


class ZiplineConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    path: Optional[str] = Field(
        default=None,
        description=(
            "Path to the compiled Chronon/Zipline output directory (containing "
            "`group_bys/`, `joins/` and `staging_queries/` sub-directories), or a "
            "repository root containing it. Both compiled-output layouts are "
            "auto-detected: `production/` (OSS Chronon `compile.py`) and "
            "`compiled/` (the `zipline compile` CLI); for canary output point this "
            "at `compiled_canary/` explicitly. This is the compiled thrift-as-JSON "
            "output, NOT the Python config repo. Run ingestion after compiling so "
            "metadata reflects the latest compile. When `git_info` is set, this is "
            "interpreted relative to the repository checkout (e.g. `path: compiled`) "
            "and defaults to the repository root."
        ),
    )
    git_info: Optional[GitInfo] = Field(
        default=None,
        description=(
            "Git repository to shallow-clone and scan for compiled Chronon/Zipline "
            "output, authenticated with an SSH deploy key. When set, `path` is "
            "resolved relative to the repository checkout. Use this to ingest "
            "directly from GitHub/GitLab instead of a pre-fetched local directory."
        ),
    )

    default_source_platform: str = Field(
        default="hive",
        description=(
            "DataHub platform to use for backing batch source tables when the "
            "table's namespace is not found in `source_platform_map`. Chronon "
            "`Source` tables reference warehouse tables whose platform cannot be "
            "inferred from the config alone."
        ),
    )
    stream_platform: str = Field(
        default="kafka",
        description="DataHub platform to use for streaming `topic` sources.",
    )
    source_platform_map: Dict[str, ZiplinePlatformDetail] = Field(
        default_factory=dict,
        description=(
            "Maps the first path segment (namespace) of a backing source table to "
            "a DataHub platform and URN-shaping options. A bare platform string is "
            'accepted as shorthand (e.g. `{"prod_db": "snowflake"}`); the object '
            "form additionally controls platform_instance, env, two-/three-tier "
            "layout and lowercasing per namespace. Namespaces not listed fall back "
            "to `default_source_platform`. Matching is case-insensitive."
        ),
    )
    staging_query_dialect: str = Field(
        default=DEFAULT_STAGING_QUERY_DIALECT,
        description=(
            "SQL dialect used to parse `StagingQuery.query` for lineage. Defaults "
            "to `spark` because Chronon runs staging queries on Spark; override "
            "only if your staging queries target a different engine."
        ),
    )
    convert_urns_to_lowercase: bool = Field(
        default=False,
        description=(
            "Lowercase backing source table names when building dataset URNs. "
            "Enable to stitch lineage to native connectors (e.g. Snowflake, Hive) "
            "that emit lowercased URNs, when Chronon references mixed-case tables."
        ),
    )
    source_platform_instance: Optional[str] = Field(
        default=None,
        description=(
            "Optional platform instance for backing source datasets (the warehouse "
            "the source tables live in). Independent of the connector's own "
            "`platform_instance`."
        ),
    )

    include_group_by_lineage: bool = Field(
        default=True,
        description=(
            "Emit each GroupBy as a DataJob producing its output table, with "
            "source-column to feature-column lineage. Disable to emit only the "
            "ML feature-table entities without the backing compute lineage."
        ),
    )
    include_joins: bool = Field(
        default=True,
        description="Emit Chronon Joins as DataJobs with input/output lineage.",
    )
    include_staging_queries: bool = Field(
        default=True,
        description="Emit Chronon StagingQueries as DataJobs.",
    )
    include_staging_query_lineage: bool = Field(
        default=True,
        description=(
            "Parse `StagingQuery.query` SQL to derive table-level lineage for the "
            "staging query DataJob. Requires `include_staging_queries`."
        ),
    )

    team_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for teams to include in ingestion.",
    )
    feature_table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns for feature tables (GroupBys) to include, matched on "
            "the fully-qualified `<team>.<name>` object name."
        ),
    )

    enable_tag_extraction: bool = Field(
        default=False,
        description=(
            "If enabled, extract tags stored in each object's `MetaData.customJson` "
            "(`groupby_tags`/`join_tags`/`column_tags`) as DataHub tags."
        ),
    )

    enable_owner_extraction: bool = Field(
        default=False,
        description=(
            "If disabled, owners are never emitted. If enabled, `owner_mappings` is "
            "required and maps a Chronon team to a DataHub owner."
        ),
    )
    owner_mappings: Optional[List[ZiplineOwnerMapping]] = Field(
        default=None,
        description=(
            "Mapping of Chronon team name to a DataHub owner. Only used when "
            "`enable_owner_extraction` is true."
        ),
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @field_validator("source_platform_map", mode="before")
    def _coerce_platform_map(cls, value: Any) -> Any:
        # Accept the shorthand `{"namespace": "snowflake"}` by promoting the bare
        # platform string to a ZiplinePlatformDetail.
        if isinstance(value, dict):
            return {
                key: ({"platform": detail} if isinstance(detail, str) else detail)
                for key, detail in value.items()
            }
        return value

    @model_validator(mode="after")
    def _validate_owner_extraction(self) -> "ZiplineConfig":
        if self.enable_owner_extraction and not self.owner_mappings:
            raise ValueError(
                "owner_mappings is required when enable_owner_extraction is enabled"
            )
        return self

    @model_validator(mode="after")
    def _validate_path_source(self) -> "ZiplineConfig":
        if self.git_info is None and not self.path:
            raise ValueError("path is required when git_info is not set")
        return self
