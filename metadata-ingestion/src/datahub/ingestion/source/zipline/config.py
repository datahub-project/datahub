from typing import Dict, List, Optional

from pydantic import Field, model_validator

from datahub.configuration.common import AllowDenyPattern
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


class ZiplineConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    path: str = Field(
        description=(
            "Path to the compiled Chronon/Zipline output directory produced by "
            "`compile.py` (the `production/` folder, containing "
            "`group_bys/`, `joins/` and `staging_queries/` sub-directories). "
            "This is the compiled thrift-as-JSON output, NOT the Python config repo. "
            "Run ingestion after `compile.py` so metadata reflects the latest compile."
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
    source_platform_map: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Maps the first path segment (namespace/database) of a backing source "
            'table to a DataHub platform. Example: `{"prod_db": "snowflake"}` '
            "resolves `prod_db.events` to the Snowflake platform. Namespaces not "
            "listed fall back to `default_source_platform`."
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
    owner_mappings: Optional[List[Dict[str, str]]] = Field(
        default=None,
        description=(
            "Mapping of Chronon team name to a DataHub owner. Each entry supports "
            "`team_name`, `datahub_owner_urn` and optional `datahub_ownership_type` "
            "(defaults to `TECHNICAL_OWNER`). Only used when "
            "`enable_owner_extraction` is true."
        ),
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @model_validator(mode="after")
    def _validate_owner_extraction(self) -> "ZiplineConfig":
        if self.enable_owner_extraction and not self.owner_mappings:
            raise ValueError(
                "owner_mappings is required when enable_owner_extraction is enabled"
            )
        return self
