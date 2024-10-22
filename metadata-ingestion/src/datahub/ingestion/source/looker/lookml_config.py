import logging
from dataclasses import dataclass, field as dataclass_field
from datetime import timedelta
from typing import Any, Dict, List, Literal, Optional, Union

import pydantic
from pydantic import root_validator, validator
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

logger = logging.getLogger(__name__)

NAME: str = "name"

_BASE_PROJECT_NAME = "__BASE"

_EXPLORE_FILE_EXTENSION = ".explore.lkml"

_VIEW_FILE_EXTENSION = ".view.lkml"

_MODEL_FILE_EXTENSION = ".model.lkml"

VIEW_LANGUAGE_LOOKML: str = "lookml"

VIEW_LANGUAGE_SQL: str = "sql"

DERIVED_VIEW_SUFFIX = r".sql_table_name"

DERIVED_VIEW_PATTERN: str = r"\$\{([^}]*)\}"


@dataclass
class LookMLSourceReport(StaleEntityRemovalSourceReport):
    git_clone_latency: Optional[timedelta] = None
    models_discovered: int = 0
    models_dropped: List[str] = dataclass_field(default_factory=LossyList)
    views_discovered: int = 0
    views_dropped: List[str] = dataclass_field(default_factory=LossyList)
    views_dropped_unreachable: List[str] = dataclass_field(default_factory=LossyList)
    query_parse_attempts: int = 0
    query_parse_failures: int = 0
    query_parse_failure_views: List[str] = dataclass_field(default_factory=LossyList)
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
    api: Optional[LookerAPIConfig]
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
        description="When enabled, only views that are reachable from explores defined in the model files are emitted",
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

    liquid_variable: Dict[Any, Any] = Field(
        {},
        description="A dictionary containing Liquid variables and their corresponding values, utilized in SQL-defined "
        "derived views. The Liquid template will be resolved in view.derived_table.sql and "
        "view.sql_table_name. Defaults to an empty dictionary.",
    )

    looker_environment: Literal["prod", "dev"] = Field(
        "prod",
        description="A looker prod or dev environment. "
        "It helps to evaluate looker if comments i.e. -- if prod --. "
        "All if comments are evaluated to true for configured looker_environment value",
    )

    @validator("connection_to_platform_map", pre=True)
    def convert_string_to_connection_def(cls, conn_map):
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
        return conn_map

    @root_validator(skip_on_failure=True)
    def check_either_connection_map_or_connection_provided(cls, values):
        """Validate that we must either have a connection map or an api credential"""
        if not values.get("connection_to_platform_map", {}) and not values.get(
            "api", {}
        ):
            raise ValueError(
                "Neither api not connection_to_platform_map config was found. LookML source requires either api "
                "credentials for Looker or a map of connection names to platform identifiers to work correctly"
            )
        return values

    @root_validator(skip_on_failure=True)
    def check_either_project_name_or_api_provided(cls, values):
        """Validate that we must either have a project name or an api credential to fetch project names"""
        if not values.get("project_name") and not values.get("api"):
            raise ValueError(
                "Neither project_name not an API credential was found. LookML source requires either api credentials "
                "for Looker or a project_name to accurately name views and models."
            )
        return values

    @validator("base_folder", always=True)
    def check_base_folder_if_not_provided(
        cls, v: Optional[pydantic.DirectoryPath], values: Dict[str, Any]
    ) -> Optional[pydantic.DirectoryPath]:
        if v is None:
            git_info: Optional[GitInfo] = values.get("git_info")
            if git_info:
                if not git_info.deploy_key:
                    logger.warning(
                        "git_info is provided, but no SSH key is present. If the repo is not public, we'll fail to "
                        "clone it."
                    )
            else:
                raise ValueError("Neither base_folder nor git_info has been provided.")
        return v
