import logging
from copy import deepcopy
from dataclasses import dataclass, field as dataclass_field
from datetime import timedelta
from typing import Any, Dict, Literal, Optional, Union

import pydantic
from pydantic import field_validator, model_validator
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.git import GitInfo
from datahub.configuration.source_common import EnvConfigMixin
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.ingestion.source.looker.looker_config import (
    LookerCommonConfig,
    LookerConnectionDefinition,
)
from datahub.ingestion.source.looker.looker_lib_wrapper import (
    LookerAPI,
    LookerAPIConfig,
    TransportOptionsConfig,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.stats_collections import TopKDict, float_top_k_dict

logger = logging.getLogger(__name__)

BASE_PROJECT_NAME = "__BASE"

EXPLORE_FILE_EXTENSION = ".explore.lkml"
VIEW_FILE_EXTENSION = ".view.lkml"
MODEL_FILE_EXTENSION = ".model.lkml"

DERIVED_VIEW_SUFFIX = r".sql_table_name"

DERIVED_VIEW_PATTERN: str = r"\$\{([^}]*)\}"


@dataclass
class LookMLSourceReport(StaleEntityRemovalSourceReport):
    git_clone_latency: Optional[timedelta] = None
    looker_query_api_latency_seconds: TopKDict[str, float] = dataclass_field(
        default_factory=float_top_k_dict
    )
    models_discovered: int = 0
    models_dropped: LossyList[str] = dataclass_field(default_factory=LossyList)
    views_discovered: int = 0
    views_dropped: LossyList[str] = dataclass_field(default_factory=LossyList)
    views_dropped_unreachable: LossyList[str] = dataclass_field(
        default_factory=LossyList
    )
    query_parse_attempts: int = 0
    query_parse_failures: int = 0
    query_parse_failure_views: LossyList[str] = dataclass_field(
        default_factory=LossyList
    )
    _looker_api: Optional[LookerAPI] = None

    def report_models_scanned(self) -> None:
        self.models_discovered += 1

    def report_views_scanned(self) -> None:
        self.views_discovered += 1

    def report_models_dropped(self, model: str) -> None:
        self.models_dropped.append(model)

    def report_views_dropped(self, view: str) -> None:
        self.views_dropped.append(view)

    def report_unreachable_view_dropped(self, view: str) -> None:
        self.views_dropped_unreachable.append(view)

    def compute_stats(self) -> None:
        if self._looker_api:
            self.api_stats = self._looker_api.compute_stats()
        return super().compute_stats()

    def report_looker_query_api_latency(
        self, view_urn: str, latency: timedelta
    ) -> None:
        self.looker_query_api_latency_seconds[view_urn] = latency.total_seconds()


class LookMLSourceConfig(
    LookerCommonConfig, StatefulIngestionConfigBase, EnvConfigMixin
):
    git_info: Optional[GitInfo] = Field(
        None,
        description="Reference to your git location. If present, supplies handy links to your lookml on the dataset "
        "entity page.",
    )
    _github_info_deprecated = pydantic_renamed_field("github_info", "git_info")
    base_folder: Optional[pydantic.DirectoryPath] = Field(
        None,
        description="Required if not providing github configuration and deploy keys. A pointer to a local directory ("
        "accessible to the ingestion system) where the root of the LookML repo has been checked out ("
        "typically via a git clone). This is typically the root folder where the `*.model.lkml` and "
        "`*.view.lkml` files are stored. e.g. If you have checked out your LookML repo under "
        "`/Users/jdoe/workspace/my-lookml-repo`, then set `base_folder` to "
        "`/Users/jdoe/workspace/my-lookml-repo`.",
    )
    project_dependencies: Dict[str, Union[pydantic.DirectoryPath, GitInfo]] = Field(
        {},
        description="A map of project_name to local directory (accessible to the ingestion system) or Git credentials. "
        "Every local_dependencies or private remote_dependency listed in the main project's manifest.lkml file should "
        "have a corresponding entry here."
        "If a deploy key is not provided, the ingestion system will use the same deploy key as the main project. ",
    )
    connection_to_platform_map: Optional[Dict[str, LookerConnectionDefinition]] = Field(
        None,
        description="A mapping of [Looker connection names]("
        "https://docs.looker.com/reference/model-params/connection-for-model) to DataHub platform, "
        "database, and schema values.",
    )
    model_pattern: AllowDenyPattern = Field(
        AllowDenyPattern.allow_all(),
        description="List of regex patterns for LookML models to include in the extraction.",
    )
    view_pattern: AllowDenyPattern = Field(
        AllowDenyPattern.allow_all(),
        description="List of regex patterns for LookML views to include in the extraction.",
    )
    parse_table_names_from_sql: bool = Field(True, description="See note below.")
    use_api_for_view_lineage: bool = Field(
        False,
        description="When enabled, uses Looker API to get SQL representation of views for lineage parsing instead of parsing LookML files directly. Requires 'api' configuration to be provided."
        "Coverage of regex based lineage extraction has limitations, it only supportes ${TABLE}.column_name syntax, See (https://cloud.google.com/looker/docs/reference/param-field-sql#sql_for_dimensions) to"
        "understand the other substitutions and cross-references allowed in LookML.",
    )
    use_api_cache_for_view_lineage: bool = Field(
        False,
        description="When enabled, uses Looker API server-side caching for query execution. Requires 'api' configuration to be provided.",
    )
    api: Optional[LookerAPIConfig] = None
    project_name: Optional[str] = Field(
        None,
        description="Required if you don't specify the `api` section. The project name within which all the model "
        "files live. See (https://docs.looker.com/data-modeling/getting-started/how-project-works) to "
        "understand what the Looker project name should be. The simplest way to see your projects is to "
        "click on `Develop` followed by `Manage LookML Projects` in the Looker application.",
    )
    transport_options: Optional[TransportOptionsConfig] = Field(
        None,
        description="Populates the [TransportOptions](https://github.com/looker-open-source/sdk-codegen/blob"
        "/94d6047a0d52912ac082eb91616c1e7c379ab262/python/looker_sdk/rtl/transport.py#L70) struct for "
        "looker client",
    )
    max_file_snippet_length: int = Field(
        512000,  # 512KB should be plenty
        description="When extracting the view definition from a lookml file, the maximum number of characters to "
        "extract.",
    )
    emit_reachable_views_only: bool = Field(
        True,
        description=(
            "When enabled, only views that are reachable from explores defined in the model files are emitted. "
            "If set to False, all views imported in model files are emitted. Views that are unreachable i.e. not explicitly defined in the model files are currently not emitted however reported as warning for debugging purposes."
        ),
    )
    populate_sql_logic_for_missing_descriptions: bool = Field(
        False,
        description="When enabled, field descriptions will include the sql logic for computed fields if descriptions "
        "are missing",
    )
    process_isolation_for_sql_parsing: bool = Field(
        False,
        description="When enabled, sql parsing will be executed in a separate process to prevent memory leaks.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description=""
    )
    process_refinements: bool = Field(
        False,
        description="When enabled, looker refinement will be processed to adapt an existing view.",
    )

    liquid_variables: Dict[Any, Any] = Field(
        {},
        description="A dictionary containing Liquid variables with their corresponding values, utilized in SQL-defined "
        "derived views. The Liquid template will be resolved in view.derived_table.sql and "
        "view.sql_table_name. Defaults to an empty dictionary.",
    )

    _liquid_variable_deprecated = pydantic_renamed_field(
        old_name="liquid_variable", new_name="liquid_variables", print_warning=True
    )

    lookml_constants: Dict[str, str] = Field(
        {},
        description=(
            "A dictionary containing LookML constants (`@{constant_name}`) and their values. "
            "If a constant is defined in the `manifest.lkml` file, its value will be used. "
            "If not found in the manifest, the value from this config will be used instead. "
            "Defaults to an empty dictionary."
        ),
    )

    looker_environment: Literal["prod", "dev"] = Field(
        "prod",
        description="A looker prod or dev environment. "
        "It helps to evaluate looker if comments i.e. -- if prod --. "
        "All if comments are evaluated to true for configured looker_environment value",
    )

    field_threshold_for_splitting: int = Field(
        100,
        description="When the total number of fields returned by Looker API exceeds this threshold, "
        "the fields will be split into multiple API calls to avoid SQL parsing failures. "
        "This helps provide partial column and table lineage when dealing with large field sets.",
    )

    allow_partial_lineage_results: bool = Field(
        True,
        description="When enabled, allows partial lineage results to be returned even when some field chunks "
        "fail or when there are SQL parsing errors. This provides better resilience for large field sets "
        "and ensures some lineage information is available rather than complete failure.",
    )

    enable_individual_field_fallback: bool = Field(
        True,
        description="When enabled, if a field chunk fails, the system will attempt to process each field "
        "individually to maximize information and isolate problematic fields. This helps identify "
        "which specific fields are causing issues while still getting lineage for working fields.",
    )

    max_workers_for_parallel_processing: int = Field(
        10,
        description="Maximum number of worker threads to use for parallel processing of field chunks and individual fields. "
        "Set to 1 to process everything sequentially. Higher values can improve performance but may increase memory usage. "
        "Maximum allowed value is 100 to prevent resource exhaustion.",
    )

    @field_validator("max_workers_for_parallel_processing")
    def validate_max_workers(cls, v: int) -> int:
        if v < 1:
            raise ValueError(
                f"max_workers_for_parallel_processing must be at least 1, got {v}"
            )
        if v > 100:
            logger.warning(
                f"max_workers_for_parallel_processing is set to {v}, which exceeds the recommended maximum of 100. "
                f"This may cause resource exhaustion. Using 100 instead."
            )
            return 100
        return v

    @model_validator(mode="before")
    @classmethod
    def convert_string_to_connection_def(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values = deepcopy(values)
        conn_map = values.get("connection_to_platform_map")
        if conn_map:
            # Previous version of config supported strings in connection map. This upconverts strings to ConnectionMap
            for key in conn_map:
                if isinstance(conn_map[key], str):
                    platform = conn_map[key]
                    if "." in platform:
                        platform_db_split = conn_map[key].split(".")
                        connection = LookerConnectionDefinition(
                            platform=platform_db_split[0],
                            default_db=platform_db_split[1],
                            default_schema="",
                        )
                        conn_map[key] = connection
                    else:
                        logger.warning(
                            f"Connection map for {key} provides platform {platform} but does not provide a default "
                            f"database name. This might result in failed resolution"
                        )
                        conn_map[key] = LookerConnectionDefinition(
                            platform=platform, default_db="", default_schema=""
                        )
        return values

    @model_validator(mode="after")
    def check_either_connection_map_or_connection_provided(self):
        """Validate that we must either have a connection map or an api credential"""
        if not (self.connection_to_platform_map or {}) and not (self.api):
            raise ValueError(
                "Neither api not connection_to_platform_map config was found. LookML source requires either api "
                "credentials for Looker or a map of connection names to platform identifiers to work correctly"
            )
        return self

    @model_validator(mode="after")
    def check_either_project_name_or_api_provided(self):
        """Validate that we must either have a project name or an api credential to fetch project names"""
        if not self.project_name and not self.api:
            raise ValueError(
                "Neither project_name not an API credential was found. LookML source requires either api credentials "
                "for Looker or a project_name to accurately name views and models."
            )
        return self

    @model_validator(mode="after")
    def check_api_provided_for_view_lineage(self):
        """Validate that we must have an api credential to use Looker API for view's column lineage"""
        if not self.api and self.use_api_for_view_lineage:
            raise ValueError(
                "API credential was not found. LookML source requires api credentials "
                "for Looker to use Looker APIs for view's column lineage extraction."
                "Set `use_api_for_view_lineage` to False to skip using Looker APIs."
            )
        return self

    @model_validator(mode="after")
    def check_base_folder_if_not_provided(self):
        if self.base_folder is None:
            if self.git_info:
                if not self.git_info.deploy_key:
                    logger.warning(
                        "git_info is provided, but no SSH key is present. If the repo is not public, we'll fail to "
                        "clone it."
                    )
            else:
                raise ValueError("Neither base_folder nor git_info has been provided.")
        return self
