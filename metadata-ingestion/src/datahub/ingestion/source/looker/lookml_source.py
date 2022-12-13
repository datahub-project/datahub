import glob
import itertools
import logging
import pathlib
import re
import tempfile
from dataclasses import dataclass, field as dataclass_field, replace
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Type, Union

import lkml
import lkml.simple
import pydantic
from looker_sdk.error import SDKError
from looker_sdk.sdk.api31.models import DBConnection
from pydantic import root_validator, validator
from pydantic.fields import Field

import datahub.emitter.mce_builder as builder
from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.configuration.github import GitHubInfo
from datahub.configuration.source_common import EnvBasedSourceConfigBase
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.registry import import_path
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.git.git_import import GitClone
from datahub.ingestion.source.looker.looker_common import (
    LookerCommonConfig,
    LookerExplore,
    LookerUtil,
    LookerViewId,
    ProjectInclude,
    ViewField,
    ViewFieldType,
)
from datahub.ingestion.source.looker.looker_lib_wrapper import (
    LookerAPI,
    LookerAPIConfig,
    TransportOptionsConfig,
)
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import BrowsePaths, Status
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    FineGrainedLineageDownstreamType,
    UpstreamClass,
    UpstreamLineage,
    ViewProperties,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageUpstreamTypeClass,
    SubTypesClass,
)
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.source_helpers import (
    auto_stale_entity_removal,
    auto_status_aspect,
)
from datahub.utilities.sql_parser import SQLParser

logger = logging.getLogger(__name__)

_BASE_PROJECT_NAME = "__BASE"

# Patch lkml to support the local_dependency and remote_dependency keywords.
lkml.simple.PLURAL_KEYS = (
    *lkml.simple.PLURAL_KEYS,
    "local_dependency",
    "remote_dependency",
)


def _get_bigquery_definition(
    looker_connection: DBConnection,
) -> Tuple[str, Optional[str], Optional[str]]:
    platform = "bigquery"
    # bigquery project ids are returned in the host field
    db = looker_connection.host
    schema = looker_connection.database
    return (platform, db, schema)


def _get_generic_definition(
    looker_connection: DBConnection, platform: Optional[str] = None
) -> Tuple[str, Optional[str], Optional[str]]:
    if platform is None:
        # We extract the platform from the dialect name
        dialect_name = looker_connection.dialect_name
        assert dialect_name is not None
        # generally the first part of the dialect name before _ is the name of the platform
        # versions are encoded as numbers and can be removed
        # e.g. spark1 or hive2 or druid_18
        platform = re.sub(r"[0-9]+", "", dialect_name.split("_")[0])

    assert (
        platform is not None
    ), f"Failed to extract a valid platform from connection {looker_connection}"
    db = looker_connection.database
    schema = looker_connection.schema  # ok for this to be None
    return (platform, db, schema)


class LookerConnectionDefinition(ConfigModel):
    platform: str
    default_db: str
    default_schema: Optional[str]  # Optional since some sources are two-level only
    platform_instance: Optional[str] = None
    platform_env: Optional[str] = Field(
        default=None,
        description="The environment that the platform is located in. Leaving this empty will inherit defaults from the top level Looker configuration",
    )

    @validator("platform_env")
    def platform_env_must_be_one_of(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            return EnvBasedSourceConfigBase.env_must_be_one_of(v)
        return v

    @validator("platform", "default_db", "default_schema")
    def lower_everything(cls, v):
        """We lower case all strings passed in to avoid casing issues later"""
        if v is not None:
            return v.lower()

    @classmethod
    def from_looker_connection(
        cls, looker_connection: DBConnection
    ) -> "LookerConnectionDefinition":
        """Dialect definitions are here: https://docs.looker.com/setup-and-management/database-config"""
        extractors: Dict[str, Any] = {
            "^bigquery": _get_bigquery_definition,
            ".*": _get_generic_definition,
        }

        if looker_connection.dialect_name is None:
            raise ConfigurationError(
                f"Unable to fetch a fully filled out connection for {looker_connection.name}. Please check your API permissions."
            )
        for extractor_pattern, extracting_function in extractors.items():
            if re.match(extractor_pattern, looker_connection.dialect_name):
                (platform, db, schema) = extracting_function(looker_connection)
                return cls(platform=platform, default_db=db, default_schema=schema)
        raise ConfigurationError(
            f"Could not find an appropriate platform for looker_connection: {looker_connection.name} with dialect: {looker_connection.dialect_name}"
        )


class LookMLSourceConfig(LookerCommonConfig, StatefulIngestionConfigBase):
    base_folder: Optional[pydantic.DirectoryPath] = Field(
        None,
        description="Required if not providing github configuration and deploy keys. A pointer to a local directory (accessible to the ingestion system) where the root of the LookML repo has been checked out (typically via a git clone). This is typically the root folder where the `*.model.lkml` and `*.view.lkml` files are stored. e.g. If you have checked out your LookML repo under `/Users/jdoe/workspace/my-lookml-repo`, then set `base_folder` to `/Users/jdoe/workspace/my-lookml-repo`.",
    )
    github_info: Optional[GitHubInfo] = Field(
        None,
        description="Reference to your github location. If present, supplies handy links to your lookml on the dataset entity page.",
    )
    project_dependencies: Dict[str, Union[pydantic.DirectoryPath, GitHubInfo]] = Field(
        {},
        description="A map of project_name to local directory (accessible to the ingestion system) or Git credentials. "
        "Every local_dependencies or private remote_dependency listed in the main project's manifest.lkml file should have a corresponding entry here. "
        "If a deploy key is not provided, the ingestion system will use the same deploy key as the main project. ",
    )
    connection_to_platform_map: Optional[Dict[str, LookerConnectionDefinition]] = Field(
        None,
        description="A mapping of [Looker connection names](https://docs.looker.com/reference/model-params/connection-for-model) to DataHub platform, database, and schema values.",
    )
    model_pattern: AllowDenyPattern = Field(
        AllowDenyPattern.allow_all(),
        description="List of regex patterns for LookML models to include in the extraction.",
    )
    view_pattern: AllowDenyPattern = Field(
        AllowDenyPattern.allow_all(),
        description="List of regex patterns for LookML views to include in the extraction.",
    )
    parse_table_names_from_sql: bool = Field(False, description="See note below.")
    sql_parser: str = Field(
        "datahub.utilities.sql_parser.DefaultSQLParser", description="See note below."
    )
    api: Optional[LookerAPIConfig]
    project_name: Optional[str] = Field(
        None,
        description="Required if you don't specify the `api` section. The project name within which all the model files live. See (https://docs.looker.com/data-modeling/getting-started/how-project-works) to understand what the Looker project name should be. The simplest way to see your projects is to click on `Develop` followed by `Manage LookML Projects` in the Looker application.",
    )
    transport_options: Optional[TransportOptionsConfig] = Field(
        None,
        description="Populates the [TransportOptions](https://github.com/looker-open-source/sdk-codegen/blob/94d6047a0d52912ac082eb91616c1e7c379ab262/python/looker_sdk/rtl/transport.py#L70) struct for looker client",
    )
    max_file_snippet_length: int = Field(
        512000,  # 512KB should be plenty
        description="When extracting the view definition from a lookml file, the maximum number of characters to extract.",
    )
    emit_reachable_views_only: bool = Field(
        True,
        description="When enabled, only views that are reachable from explores defined in the model files are emitted",
    )
    populate_sql_logic_for_missing_descriptions: bool = Field(
        False,
        description="When enabled, field descriptions will include the sql logic for computed fields if descriptions are missing",
    )
    process_isolation_for_sql_parsing: bool = Field(
        True,
        description="When enabled, sql parsing will be executed in a separate process to prevent memory leaks.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description=""
    )

    @validator("platform_instance")
    def platform_instance_not_supported(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            raise ConfigurationError(
                "LookML Source doesn't support platform instance at the top level. However connection-specific platform instances are supported for generating lineage edges. Read the documentation to find out more."
            )
        return v

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
                        f"Connection map for {key} provides platform {platform} but does not provide a default database name. This might result in failed resolution"
                    )
                    conn_map[key] = LookerConnectionDefinition(
                        platform=platform, default_db="", default_schema=""
                    )
        return conn_map

    @root_validator()
    def check_either_connection_map_or_connection_provided(cls, values):
        """Validate that we must either have a connection map or an api credential"""
        if not values.get("connection_to_platform_map", {}) and not values.get(
            "api", {}
        ):
            raise ConfigurationError(
                "Neither api not connection_to_platform_map config was found. LookML source requires either api credentials for Looker or a map of connection names to platform identifiers to work correctly"
            )
        return values

    @root_validator()
    def check_either_project_name_or_api_provided(cls, values):
        """Validate that we must either have a project name or an api credential to fetch project names"""
        if not values.get("project_name") and not values.get("api"):
            raise ConfigurationError(
                "Neither project_name not an API credential was found. LookML source requires either api credentials for Looker or a project_name to accurately name views and models."
            )
        return values

    @validator("base_folder", always=True)
    def check_base_folder_if_not_provided(
        cls, v: Optional[pydantic.DirectoryPath], values: Dict[str, Any]
    ) -> Optional[pydantic.DirectoryPath]:
        if v is None:
            github_info: Optional[GitHubInfo] = values.get("github_info", None)
            if github_info and github_info.deploy_key:
                # We have github_info populated correctly, base folder is not needed
                pass
            else:
                raise ValueError(
                    "base_folder is not provided. Neither has a github deploy_key or deploy_key_file been provided"
                )
        return v


@dataclass
class LookMLSourceReport(StaleEntityRemovalSourceReport):
    git_clone_latency: Optional[timedelta] = None
    models_discovered: int = 0
    models_dropped: List[str] = dataclass_field(default_factory=LossyList)
    views_discovered: int = 0
    views_dropped: List[str] = dataclass_field(default_factory=LossyList)
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

    def compute_stats(self) -> None:
        if self._looker_api:
            self.api_stats = self._looker_api.compute_stats()
        return super().compute_stats()


@dataclass
class LookerModel:
    connection: str
    includes: List[str]
    explores: List[dict]
    resolved_includes: List[ProjectInclude]

    @staticmethod
    def from_looker_dict(
        looker_model_dict: dict,
        base_project_name: str,
        base_folder: str,
        base_projects_folders: Dict[str, pathlib.Path],
        path: str,
        reporter: LookMLSourceReport,
    ) -> "LookerModel":
        logger.debug(f"Loading model from {path}")
        connection = looker_model_dict["connection"]
        includes = looker_model_dict.get("includes", [])
        resolved_includes = LookerModel.resolve_includes(
            includes,
            base_project_name,
            base_folder,
            base_projects_folders,
            path,
            reporter,
            seen_so_far=set(),
            traversal_path=pathlib.Path(path).stem,
        )
        logger.debug(f"{path} has resolved_includes: {resolved_includes}")
        explores = looker_model_dict.get("explores", [])

        return LookerModel(
            connection=connection,
            includes=includes,
            resolved_includes=resolved_includes,
            explores=explores,
        )

    @staticmethod
    def resolve_includes(
        includes: List[str],
        project_name: str,
        base_folder: str,
        base_projects_folder: Dict[str, pathlib.Path],
        path: str,
        reporter: LookMLSourceReport,
        seen_so_far: Set[str],
        traversal_path: str = "",  # a cosmetic parameter to aid debugging
    ) -> List[ProjectInclude]:
        """Resolve ``include`` statements in LookML model files to a list of ``.lkml`` files.

        For rules on how LookML ``include`` statements are written, see
            https://docs.looker.com/data-modeling/getting-started/ide-folders#wildcard_examples
        """
        resolved = []
        for inc in includes:
            resolved_project_name = project_name
            # Filter out dashboards - we get those through the looker source.
            if (
                inc.endswith(".dashboard")
                or inc.endswith(".dashboard.lookml")
                or inc.endswith(".dashboard.lkml")
            ):
                logger.debug(f"include '{inc}' is a dashboard, skipping it")
                continue

            # Massage the looker include into a valid glob wildcard expression
            if inc.startswith("//"):
                # remote include, let's see if we have the project checked out locally
                (remote_project, project_local_path) = inc[2:].split("/", maxsplit=1)
                if remote_project in base_projects_folder:
                    glob_expr = (
                        f"{base_projects_folder[remote_project]}/{project_local_path}"
                    )
                    resolved_project_name = remote_project
                else:
                    logger.warning(
                        f"Resolving {inc} failed. Could not find a locally checked out reference for {remote_project}"
                    )
                    continue
            elif inc.startswith("/"):
                glob_expr = f"{base_folder}{inc}"
            else:
                # Need to handle a relative path.
                glob_expr = str(pathlib.Path(path).parent / inc)
            # "**" matches an arbitrary number of directories in LookML
            # we also resolve these paths to absolute paths so we can de-dup effectively later on
            included_files = [
                str(pathlib.Path(p).resolve())
                for p in sorted(
                    glob.glob(glob_expr, recursive=True)
                    + glob.glob(f"{glob_expr}.lkml", recursive=True)
                )
            ]
            logger.debug(
                f"traversal_path={traversal_path}, included_files = {included_files}, seen_so_far: {seen_so_far}"
            )
            if "*" not in inc and not included_files:
                reporter.report_failure(path, f"cannot resolve include {inc}")
            elif not included_files:
                reporter.report_failure(
                    path, f"did not resolve anything for wildcard include {inc}"
                )
            # only load files that we haven't seen so far
            included_files = [x for x in included_files if x not in seen_so_far]
            for included_file in included_files:
                # Filter out dashboards - we get those through the looker source.
                if (
                    included_file.endswith(".dashboard")
                    or included_file.endswith(".dashboard.lookml")
                    or included_file.endswith(".dashboard.lkml")
                ):
                    logger.debug(
                        f"include '{included_file}' is a dashboard, skipping it"
                    )
                    continue

                logger.debug(
                    f"Will be loading {included_file}, traversed here via {traversal_path}"
                )
                try:
                    with open(included_file, "r") as file:
                        parsed = lkml.load(file)
                        seen_so_far.add(included_file)
                        if "includes" in parsed:  # we have more includes to resolve!
                            resolved.extend(
                                LookerModel.resolve_includes(
                                    parsed["includes"],
                                    resolved_project_name,
                                    base_folder,
                                    base_projects_folder,
                                    included_file,
                                    reporter,
                                    seen_so_far,
                                    traversal_path=traversal_path
                                    + "."
                                    + pathlib.Path(included_file).stem,
                                )
                            )
                except Exception as e:
                    reporter.report_warning(
                        path, f"Failed to load {included_file} due to {e}"
                    )
                    # continue in this case, as it might be better to load and resolve whatever we can
                    pass

            resolved.extend(
                [
                    ProjectInclude(project=resolved_project_name, include=f)
                    for f in included_files
                ]
            )
        return resolved


@dataclass
class LookerViewFile:
    absolute_file_path: str
    connection: Optional[str]
    includes: List[str]
    resolved_includes: List[ProjectInclude]
    views: List[Dict]
    raw_file_content: str

    @classmethod
    def from_looker_dict(
        cls,
        absolute_file_path: str,
        looker_view_file_dict: dict,
        project_name: str,
        base_folder: str,
        base_projects_folder: Dict[str, pathlib.Path],
        raw_file_content: str,
        reporter: LookMLSourceReport,
    ) -> "LookerViewFile":
        logger.debug(f"Loading view file at {absolute_file_path}")
        includes = looker_view_file_dict.get("includes", [])
        resolved_path = str(pathlib.Path(absolute_file_path).resolve())
        seen_so_far = set()
        seen_so_far.add(resolved_path)
        resolved_includes = LookerModel.resolve_includes(
            includes,
            project_name,
            base_folder,
            base_projects_folder,
            absolute_file_path,
            reporter,
            seen_so_far=seen_so_far,
        )
        logger.debug(
            f"resolved_includes for {absolute_file_path} is {resolved_includes}"
        )
        views = looker_view_file_dict.get("views", [])

        return cls(
            absolute_file_path=absolute_file_path,
            connection=None,
            includes=includes,
            resolved_includes=resolved_includes,
            views=views,
            raw_file_content=raw_file_content,
        )


@dataclass
class SQLInfo:
    table_names: List[str]
    column_names: List[str]


class LookerViewFileLoader:
    """
    Loads the looker viewfile at a :path and caches the LookerViewFile in memory
    This is to avoid reloading the same file off of disk many times during the recursive include resolution process
    """

    def __init__(
        self,
        base_folder: str,
        base_projects_folder: Dict[str, pathlib.Path],
        reporter: LookMLSourceReport,
    ) -> None:
        self.viewfile_cache: Dict[str, LookerViewFile] = {}
        self._base_folder = base_folder
        self._base_projects_folder = base_projects_folder
        self.reporter = reporter

    def is_view_seen(self, path: str) -> bool:
        return path in self.viewfile_cache

    def _load_viewfile(
        self, project_name: str, path: str, reporter: LookMLSourceReport
    ) -> Optional[LookerViewFile]:
        # always fully resolve paths to simplify de-dup
        path = str(pathlib.Path(path).resolve())
        if not path.endswith(".view.lkml"):
            # not a view file
            logger.debug(
                f"Skipping file {path} because it doesn't appear to be a view file"
            )
            return None

        if self.is_view_seen(str(path)):
            return self.viewfile_cache[path]

        try:
            with open(path, "r") as file:
                raw_file_content = file.read()
        except Exception as e:
            self.reporter.report_failure(path, f"failed to load view file: {e}")
            return None
        try:
            with open(path, "r") as file:
                logger.debug(f"Loading viewfile {path}")
                parsed = lkml.load(file)
                looker_viewfile = LookerViewFile.from_looker_dict(
                    absolute_file_path=path,
                    looker_view_file_dict=parsed,
                    project_name=project_name,
                    base_folder=self._base_folder,
                    base_projects_folder=self._base_projects_folder,
                    raw_file_content=raw_file_content,
                    reporter=reporter,
                )
                logger.debug(f"adding viewfile for path {path} to the cache")
                self.viewfile_cache[path] = looker_viewfile
                return looker_viewfile
        except Exception as e:
            self.reporter.report_failure(path, f"failed to load view file: {e}")
            return None

    def load_viewfile(
        self,
        path: str,
        project_name: str,
        connection: Optional[LookerConnectionDefinition],
        reporter: LookMLSourceReport,
    ) -> Optional[LookerViewFile]:
        viewfile = self._load_viewfile(
            project_name=project_name, path=path, reporter=reporter
        )
        if viewfile is None:
            return None

        return replace(viewfile, connection=connection)


VIEW_LANGUAGE_LOOKML: str = "lookml"
VIEW_LANGUAGE_SQL: str = "sql"


def _find_view_from_resolved_includes(
    connection: Optional[LookerConnectionDefinition],
    resolved_includes: List[ProjectInclude],
    looker_viewfile_loader: LookerViewFileLoader,
    target_view_name: str,
    reporter: LookMLSourceReport,
) -> Optional[Tuple[ProjectInclude, dict]]:
    # It could live in one of the included files. We do not know which file the base view
    # lives in, so we try them all!
    for include in resolved_includes:
        included_looker_viewfile = looker_viewfile_loader.load_viewfile(
            include.include, include.project, connection, reporter
        )
        if not included_looker_viewfile:
            continue
        for raw_view in included_looker_viewfile.views:
            raw_view_name = raw_view["name"]
            # Make sure to skip loading view we are currently trying to resolve
            if raw_view_name == target_view_name:
                return include, raw_view

    return None


_SQL_FUNCTIONS = ["UNNEST"]


@dataclass
class LookerView:
    id: LookerViewId
    absolute_file_path: str
    connection: LookerConnectionDefinition
    sql_table_names: List[str]
    fields: List[ViewField]
    raw_file_content: str
    view_details: Optional[ViewProperties] = None

    @classmethod
    def _import_sql_parser_cls(cls, sql_parser_path: str) -> Type[SQLParser]:
        assert "." in sql_parser_path, "sql_parser-path must contain a ."
        parser_cls = import_path(sql_parser_path)

        if not issubclass(parser_cls, SQLParser):
            raise ValueError(f"must be derived from {SQLParser}; got {parser_cls}")
        return parser_cls

    @classmethod
    def _get_sql_info(
        cls, sql: str, sql_parser_path: str, use_external_process: bool = True
    ) -> SQLInfo:
        parser_cls = cls._import_sql_parser_cls(sql_parser_path)

        try:
            parser_instance: SQLParser = parser_cls(
                sql, use_external_process=use_external_process
            )
        except Exception as e:
            logger.warning(f"Sql parser failed on {sql} with {e}")
            return SQLInfo(table_names=[], column_names=[])

        sql_table_names: List[str]
        try:
            sql_table_names = parser_instance.get_tables()
        except Exception as e:
            logger.warning(f"Sql parser failed on {sql} with {e}")
            sql_table_names = []

        try:
            column_names: List[str] = parser_instance.get_columns()
        except Exception as e:
            logger.warning(f"Sql parser failed on {sql} with {e}")
            column_names = []

        logger.debug(f"Column names parsed = {column_names}")
        # Drop table names with # in them
        sql_table_names = [t for t in sql_table_names if "#" not in t]

        # Remove quotes from table names
        sql_table_names = [t.replace('"', "") for t in sql_table_names]
        sql_table_names = [t.replace("`", "") for t in sql_table_names]
        # Remove reserved words from table names
        sql_table_names = [
            t for t in sql_table_names if t.upper() not in _SQL_FUNCTIONS
        ]

        return SQLInfo(table_names=sql_table_names, column_names=column_names)

    @classmethod
    def _get_fields(
        cls,
        field_list: List[Dict],
        type_cls: ViewFieldType,
        extract_column_level_lineage: bool,
        populate_sql_logic_in_descriptions: bool,
    ) -> List[ViewField]:
        fields = []
        for field_dict in field_list:
            is_primary_key = field_dict.get("primary_key", "no") == "yes"
            name = field_dict["name"]
            native_type = field_dict.get("type", "string")
            default_description = (
                f"sql:{field_dict['sql']}"
                if "sql" in field_dict and populate_sql_logic_in_descriptions
                else ""
            )
            description = field_dict.get("description", default_description)
            label = field_dict.get("label", "")
            upstream_fields = []
            if type_cls == ViewFieldType.DIMENSION and extract_column_level_lineage:
                if field_dict.get("sql") is not None:
                    for upstream_field_match in re.finditer(
                        r"\${TABLE}\.[\"]*([\.\w]+)", field_dict["sql"]
                    ):
                        matched_field = upstream_field_match.group(1)
                        # Remove quotes from field names
                        matched_field = (
                            matched_field.replace('"', "").replace("`", "").lower()
                        )
                        upstream_fields.append(matched_field)
            upstream_fields = sorted(list(set(upstream_fields)))

            field = ViewField(
                name=name,
                type=native_type,
                label=label,
                description=description,
                is_primary_key=is_primary_key,
                field_type=type_cls,
                upstream_fields=upstream_fields,
            )
            fields.append(field)
        return fields

    @classmethod
    def from_looker_dict(
        cls,
        project_name: str,
        model_name: str,
        looker_view: dict,
        connection: LookerConnectionDefinition,
        looker_viewfile: LookerViewFile,
        looker_viewfile_loader: LookerViewFileLoader,
        reporter: LookMLSourceReport,
        max_file_snippet_length: int,
        parse_table_names_from_sql: bool = False,
        sql_parser_path: str = "datahub.utilities.sql_parser.DefaultSQLParser",
        extract_col_level_lineage: bool = False,
        populate_sql_logic_in_descriptions: bool = False,
        process_isolation_for_sql_parsing: bool = True,
    ) -> Optional["LookerView"]:
        view_name = looker_view["name"]
        logger.debug(f"Handling view {view_name} in model {model_name}")
        # The sql_table_name might be defined in another view and this view is extending that view,
        # so we resolve this field while taking that into account.
        sql_table_name: Optional[str] = LookerView.get_including_extends(
            view_name=view_name,
            looker_view=looker_view,
            connection=connection,
            looker_viewfile=looker_viewfile,
            looker_viewfile_loader=looker_viewfile_loader,
            field="sql_table_name",
            reporter=reporter,
        )

        # Some sql_table_name fields contain quotes like: optimizely."group", just remove the quotes
        sql_table_name = (
            sql_table_name.replace('"', "").replace("`", "")
            if sql_table_name is not None
            else None
        )
        derived_table = LookerView.get_including_extends(
            view_name=view_name,
            looker_view=looker_view,
            connection=connection,
            looker_viewfile=looker_viewfile,
            looker_viewfile_loader=looker_viewfile_loader,
            field="derived_table",
            reporter=reporter,
        )

        dimensions = cls._get_fields(
            looker_view.get("dimensions", []),
            ViewFieldType.DIMENSION,
            extract_col_level_lineage,
            populate_sql_logic_in_descriptions=populate_sql_logic_in_descriptions,
        )
        dimension_groups = cls._get_fields(
            looker_view.get("dimension_groups", []),
            ViewFieldType.DIMENSION_GROUP,
            extract_col_level_lineage,
            populate_sql_logic_in_descriptions=populate_sql_logic_in_descriptions,
        )
        measures = cls._get_fields(
            looker_view.get("measures", []),
            ViewFieldType.MEASURE,
            extract_col_level_lineage,
            populate_sql_logic_in_descriptions=populate_sql_logic_in_descriptions,
        )
        fields: List[ViewField] = dimensions + dimension_groups + measures

        # also store the view logic and materialization
        view_logic = looker_viewfile.raw_file_content[:max_file_snippet_length]

        # Parse SQL from derived tables to extract dependencies
        if derived_table is not None:
            fields, sql_table_names = cls._extract_metadata_from_sql_query(
                reporter,
                parse_table_names_from_sql,
                sql_parser_path,
                view_name,
                sql_table_name,
                derived_table,
                fields,
                use_external_process=process_isolation_for_sql_parsing,
            )
            if "sql" in derived_table:
                view_logic = derived_table["sql"]
                view_lang = VIEW_LANGUAGE_SQL
            if "explore_source" in derived_table:
                view_logic = str(derived_table["explore_source"])
                view_lang = VIEW_LANGUAGE_LOOKML

            materialized = False
            for k in derived_table:
                if k in ["datagroup_trigger", "sql_trigger_value", "persist_for"]:
                    materialized = True
            if "materialized_view" in derived_table:
                materialized = derived_table["materialized_view"] == "yes"

            view_details = ViewProperties(
                materialized=materialized, viewLogic=view_logic, viewLanguage=view_lang
            )

            return LookerView(
                id=LookerViewId(
                    project_name=project_name,
                    model_name=model_name,
                    view_name=view_name,
                ),
                absolute_file_path=looker_viewfile.absolute_file_path,
                connection=connection,
                sql_table_names=sql_table_names,
                fields=fields,
                raw_file_content=looker_viewfile.raw_file_content,
                view_details=view_details,
            )

        # If not a derived table, then this view essentially wraps an existing
        # object in the database. If sql_table_name is set, there is a single
        # dependency in the view, on the sql_table_name.
        # Otherwise, default to the view name as per the docs:
        # https://docs.looker.com/reference/view-params/sql_table_name-for-view
        sql_table_names = [view_name] if sql_table_name is None else [sql_table_name]
        output_looker_view = LookerView(
            id=LookerViewId(
                project_name=project_name, model_name=model_name, view_name=view_name
            ),
            absolute_file_path=looker_viewfile.absolute_file_path,
            sql_table_names=sql_table_names,
            connection=connection,
            fields=fields,
            raw_file_content=looker_viewfile.raw_file_content,
            view_details=ViewProperties(
                materialized=False,
                viewLogic=view_logic,
                viewLanguage=VIEW_LANGUAGE_LOOKML,
            ),
        )
        return output_looker_view

    @classmethod
    def _extract_metadata_from_sql_query(
        cls: Type,
        reporter: LookMLSourceReport,
        parse_table_names_from_sql: bool,
        sql_parser_path: str,
        view_name: str,
        sql_table_name: Optional[str],
        derived_table: dict,
        fields: List[ViewField],
        use_external_process: bool,
    ) -> Tuple[List[ViewField], List[str]]:
        sql_table_names: List[str] = []
        if parse_table_names_from_sql and "sql" in derived_table:
            logger.debug(f"Parsing sql from derived table section of view: {view_name}")
            sql_query = derived_table["sql"]
            reporter.query_parse_attempts += 1

            # Skip queries that contain liquid variables. We currently don't parse them correctly
            if "{%" in sql_query:
                try:
                    # test if parsing works
                    sql_info: SQLInfo = cls._get_sql_info(
                        sql_query, sql_parser_path, use_external_process
                    )
                    if not sql_info.table_names:
                        raise Exception("Failed to find any tables")
                except Exception:
                    logger.debug(
                        f"{view_name}: SQL Parsing didn't return any tables, trying a hail-mary"
                    )
                    # A hail-mary simple parse.
                    for maybe_table_match in re.finditer(
                        r"FROM\s*([a-zA-Z0-9_.`]+)", sql_query
                    ):
                        if maybe_table_match.group(1) not in sql_table_names:
                            sql_table_names.append(maybe_table_match.group(1))
                    return fields, sql_table_names
            # Looker supports sql fragments that omit the SELECT and FROM parts of the query
            # Add those in if we detect that it is missing
            if not re.search(r"SELECT\s", sql_query, flags=re.I):
                # add a SELECT clause at the beginning
                sql_query = f"SELECT {sql_query}"
            if not re.search(r"FROM\s", sql_query, flags=re.I):
                # add a FROM clause at the end
                sql_query = f"{sql_query} FROM {sql_table_name if sql_table_name is not None else view_name}"
                # Get the list of tables in the query
            try:
                sql_info = cls._get_sql_info(
                    sql_query, sql_parser_path, use_external_process
                )
                sql_table_names = sql_info.table_names
                column_names = sql_info.column_names
                if not fields:
                    # it seems like the view is defined purely as sql, let's try using the column names to populate the schema
                    fields = [
                        # set types to unknown for now as our sql parser doesn't give us column types yet
                        ViewField(c, "", "unknown", "", ViewFieldType.UNKNOWN)
                        for c in sorted(column_names)
                    ]
                if not sql_info.table_names:
                    reporter.query_parse_failures += 1
                    reporter.query_parse_failure_views.append(view_name)
            except Exception as e:
                reporter.query_parse_failures += 1
                reporter.report_warning(
                    f"looker-view-{view_name}",
                    f"Failed to parse sql query, lineage will not be accurate. Exception: {e}",
                )

        # remove fields or sql tables that contain liquid variables
        fields = [f for f in fields if "{%" not in f.name]
        sql_table_names = [table for table in sql_table_names if "{%" not in table]

        return fields, sql_table_names

    @classmethod
    def resolve_extends_view_name(
        cls,
        connection: LookerConnectionDefinition,
        looker_viewfile: LookerViewFile,
        looker_viewfile_loader: LookerViewFileLoader,
        target_view_name: str,
        reporter: LookMLSourceReport,
    ) -> Optional[dict]:
        # The view could live in the same file.
        for raw_view in looker_viewfile.views:
            raw_view_name = raw_view["name"]
            if raw_view_name == target_view_name:
                return raw_view

        # Or, it could live in one of the imports.
        view = _find_view_from_resolved_includes(
            connection,
            looker_viewfile.resolved_includes,
            looker_viewfile_loader,
            target_view_name,
            reporter,
        )
        if view:
            return view[1]
        else:
            logger.warning(
                f"failed to resolve view {target_view_name} included from {looker_viewfile.absolute_file_path}"
            )
            return None

    @classmethod
    def get_including_extends(
        cls,
        view_name: str,
        looker_view: dict,
        connection: LookerConnectionDefinition,
        looker_viewfile: LookerViewFile,
        looker_viewfile_loader: LookerViewFileLoader,
        field: str,
        reporter: LookMLSourceReport,
    ) -> Optional[Any]:
        extends = list(
            itertools.chain.from_iterable(
                looker_view.get("extends", looker_view.get("extends__all", []))
            )
        )

        # First, check the current view.
        if field in looker_view:
            return looker_view[field]

        # Then, check the views this extends, following Looker's precedence rules.
        for extend in reversed(extends):
            assert extend != view_name, "a view cannot extend itself"
            extend_view = LookerView.resolve_extends_view_name(
                connection, looker_viewfile, looker_viewfile_loader, extend, reporter
            )
            if not extend_view:
                raise NameError(
                    f"failed to resolve extends view {extend} in view {view_name} of file {looker_viewfile.absolute_file_path}"
                )
            if field in extend_view:
                return extend_view[field]

        return None


@dataclass
class LookerRemoteDependency:
    name: str
    url: str
    ref: Optional[str]


@dataclass
class LookerManifest:
    # This must be set if the manifest has local_dependency entries.
    # See https://cloud.google.com/looker/docs/reference/param-manifest-project-name
    project_name: Optional[str]

    local_dependencies: List[str]
    remote_dependencies: List[LookerRemoteDependency]


@platform_name("Looker")
@config_class(LookMLSourceConfig)
@support_status(SupportStatus.CERTIFIED)
class LookMLSource(StatefulIngestionSourceBase):
    """
    This plugin extracts the following:
    - LookML views from model files in a project
    - Name, upstream table names, metadata for dimensions, measures, and dimension groups attached as tags
    - If API integration is enabled (recommended), resolves table and view names by calling the Looker API, otherwise supports offline resolution of these names.

    :::note
    To get complete Looker metadata integration (including Looker dashboards and charts and lineage to the underlying Looker views, you must ALSO use the `looker` source module.
    :::
    """

    platform = "lookml"
    source_config: LookMLSourceConfig
    reporter: LookMLSourceReport
    looker_client: Optional[LookerAPI] = None

    # This is populated during the git clone step.
    base_projects_folder: Dict[str, pathlib.Path] = {}
    remote_projects_github_info: Dict[str, GitHubInfo] = {}

    def __init__(self, config: LookMLSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.reporter = LookMLSourceReport()
        if self.source_config.api:
            self.looker_client = LookerAPI(self.source_config.api)
            self.reporter._looker_api = self.looker_client
            try:
                self.looker_client.all_connections()
            except SDKError:
                raise ValueError(
                    "Failed to retrieve connections from looker client. Please check to ensure that you have manage_models permission enabled on this API key."
                )
        # Create and register the stateful ingestion use-case handlers.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            source=self,
            config=self.source_config,
            state_type_class=GenericCheckpointState,
            pipeline_name=self.ctx.pipeline_name,
            run_id=self.ctx.run_id,
        )

    def _load_model(self, path: str) -> LookerModel:
        with open(path, "r") as file:
            logger.debug(f"Loading model from file {path}")
            parsed = lkml.load(file)
            looker_model = LookerModel.from_looker_dict(
                parsed,
                _BASE_PROJECT_NAME,
                str(self.source_config.base_folder),
                self.base_projects_folder,
                path,
                self.reporter,
            )
        return looker_model

    def _platform_names_have_2_parts(self, platform: str) -> bool:
        return platform in {"hive", "mysql", "athena"}

    def _generate_fully_qualified_name(
        self, sql_table_name: str, connection_def: LookerConnectionDefinition
    ) -> str:
        """Returns a fully qualified dataset name, resolved through a connection definition.
        Input sql_table_name can be in three forms: table, db.table, db.schema.table"""
        # TODO: This function should be extracted out into a Platform specific naming class since name translations are required across all connectors

        # Bigquery has "project.db.table" which can be mapped to db.schema.table form
        # All other relational db's follow "db.schema.table"
        # With the exception of mysql, hive, athena which are "db.table"

        # first detect which one we have
        parts = len(sql_table_name.split("."))

        if parts == 3:
            # fully qualified, but if platform is of 2-part, we drop the first level
            if self._platform_names_have_2_parts(connection_def.platform):
                sql_table_name = ".".join(sql_table_name.split(".")[1:])
            return sql_table_name.lower()

        if parts == 1:
            # Bare table form
            if self._platform_names_have_2_parts(connection_def.platform):
                dataset_name = f"{connection_def.default_db}.{sql_table_name}"
            else:
                dataset_name = f"{connection_def.default_db}.{connection_def.default_schema}.{sql_table_name}"
            return dataset_name.lower()

        if parts == 2:
            # if this is a 2 part platform, we are fine
            if self._platform_names_have_2_parts(connection_def.platform):
                return sql_table_name.lower()
            # otherwise we attach the default top-level container
            dataset_name = f"{connection_def.default_db}.{sql_table_name}"
            return dataset_name.lower()

        self.reporter.report_warning(
            key=sql_table_name, reason=f"{sql_table_name} has more than 3 parts."
        )
        return sql_table_name.lower()

    def _construct_datalineage_urn(
        self, sql_table_name: str, looker_view: LookerView
    ) -> str:
        logger.debug(f"sql_table_name={sql_table_name}")
        connection_def: LookerConnectionDefinition = looker_view.connection

        # Check if table name matches cascading derived tables pattern
        # derived tables can be referred to using aliases that look like table_name.SQL_TABLE_NAME
        # See https://docs.looker.com/data-modeling/learning-lookml/derived-tables#syntax_for_referencing_a_derived_table
        if re.fullmatch(r"\w+\.SQL_TABLE_NAME", sql_table_name, flags=re.I):
            sql_table_name = sql_table_name.lower().split(".")[0]
            # upstream dataset is a looker view based on current view id's project and model
            view_id = LookerViewId(
                project_name=looker_view.id.project_name,
                model_name=looker_view.id.model_name,
                view_name=sql_table_name,
            )
            return view_id.get_urn(self.source_config)

        # Ensure sql_table_name is in canonical form (add in db, schema names)
        sql_table_name = self._generate_fully_qualified_name(
            sql_table_name, connection_def
        )

        return builder.make_dataset_urn_with_platform_instance(
            platform=connection_def.platform,
            name=sql_table_name.lower(),
            platform_instance=connection_def.platform_instance,
            env=connection_def.platform_env or self.source_config.env,
        )

    def _get_connection_def_based_on_connection_string(
        self, connection: str
    ) -> Optional[LookerConnectionDefinition]:
        if self.source_config.connection_to_platform_map is None:
            self.source_config.connection_to_platform_map = {}
        assert self.source_config.connection_to_platform_map is not None
        if connection in self.source_config.connection_to_platform_map:
            return self.source_config.connection_to_platform_map[connection]
        elif self.looker_client:
            looker_connection: Optional[DBConnection] = None
            try:
                looker_connection = self.looker_client.connection(connection)
            except SDKError:
                logger.error(f"Failed to retrieve connection {connection} from Looker")

            if looker_connection:
                try:
                    connection_def: LookerConnectionDefinition = (
                        LookerConnectionDefinition.from_looker_connection(
                            looker_connection
                        )
                    )

                    # Populate the cache (using the config map) to avoid calling looker again for this connection
                    self.source_config.connection_to_platform_map[
                        connection
                    ] = connection_def
                    return connection_def
                except ConfigurationError:
                    self.reporter.report_warning(
                        f"connection-{connection}",
                        "Failed to load connection from Looker",
                    )

        return None

    def _get_upstream_lineage(
        self, looker_view: LookerView
    ) -> Optional[UpstreamLineage]:
        upstreams = []
        for sql_table_name in looker_view.sql_table_names:

            sql_table_name = sql_table_name.replace('"', "").replace("`", "")
            upstream_dataset_urn: str = self._construct_datalineage_urn(
                sql_table_name, looker_view
            )
            fine_grained_lineages: List[FineGrainedLineageClass] = []
            if self.source_config.extract_column_level_lineage and (
                looker_view.view_details is not None
                and looker_view.view_details.viewLanguage
                != VIEW_LANGUAGE_SQL  # we currently only map col-level lineage for views without sql
            ):
                for field in looker_view.fields:
                    if field.upstream_fields:
                        fine_grained_lineage = FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            upstreams=[
                                make_schema_field_urn(
                                    upstream_dataset_urn, upstream_field
                                )
                                for upstream_field in field.upstream_fields
                            ],
                            downstreamType=FineGrainedLineageDownstreamType.FIELD,
                            downstreams=[
                                make_schema_field_urn(
                                    looker_view.id.get_urn(self.source_config),
                                    field.name,
                                )
                            ],
                        )
                        fine_grained_lineages.append(fine_grained_lineage)

            upstream = UpstreamClass(
                dataset=upstream_dataset_urn,
                type=DatasetLineageTypeClass.VIEW,
            )
            upstreams.append(upstream)

        if upstreams != []:
            return UpstreamLineage(
                upstreams=upstreams, fineGrainedLineages=fine_grained_lineages or None
            )
        else:
            return None

    def _get_custom_properties(self, looker_view: LookerView) -> DatasetPropertiesClass:
        assert self.source_config.base_folder  # this is always filled out
        if looker_view.id.project_name == _BASE_PROJECT_NAME:
            base_folder = self.source_config.base_folder
        else:
            base_folder = self.base_projects_folder.get(
                looker_view.id.project_name, self.source_config.base_folder
            )
        try:
            file_path = str(
                pathlib.Path(looker_view.absolute_file_path).relative_to(
                    base_folder.resolve()
                )
            )
        except Exception:
            file_path = None
            logger.warning(
                f"Failed to resolve relative path for file {looker_view.absolute_file_path} w.r.t. folder {self.source_config.base_folder}"
            )

        custom_properties = {
            "looker.file.path": file_path or looker_view.absolute_file_path,
        }
        dataset_props = DatasetPropertiesClass(
            name=looker_view.id.view_name, customProperties=custom_properties
        )

        maybe_github_info = self.source_config.project_dependencies.get(
            looker_view.id.project_name,
            self.remote_projects_github_info.get(looker_view.id.project_name),
        )
        if isinstance(maybe_github_info, GitHubInfo):
            github_info: Optional[GitHubInfo] = maybe_github_info
        else:
            github_info = self.source_config.github_info
        if github_info is not None and file_path:
            # It should be that looker_view.id.project_name is the base project.
            github_file_url = github_info.get_url_for_file_path(file_path)
            dataset_props.externalUrl = github_file_url

        return dataset_props

    def _build_dataset_mcps(
        self, looker_view: LookerView
    ) -> List[MetadataChangeProposalWrapper]:
        subTypeEvent = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=looker_view.id.get_urn(self.source_config),
            aspectName="subTypes",
            aspect=SubTypesClass(typeNames=["view"]),
        )
        events = [subTypeEvent]
        if looker_view.view_details is not None:
            viewEvent = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=looker_view.id.get_urn(self.source_config),
                aspectName="viewProperties",
                aspect=looker_view.view_details,
            )
            events.append(viewEvent)

        return events

    def _build_dataset_mce(self, looker_view: LookerView) -> MetadataChangeEvent:
        """
        Creates MetadataChangeEvent for the dataset, creating upstream lineage links
        """
        logger.debug(f"looker_view = {looker_view.id}")

        dataset_snapshot = DatasetSnapshot(
            urn=looker_view.id.get_urn(self.source_config),
            aspects=[],  # we append to this list later on
        )
        browse_paths = BrowsePaths(
            paths=[looker_view.id.get_browse_path(self.source_config)]
        )
        dataset_snapshot.aspects.append(browse_paths)
        dataset_snapshot.aspects.append(Status(removed=False))
        upstream_lineage = self._get_upstream_lineage(looker_view)
        if upstream_lineage is not None:
            dataset_snapshot.aspects.append(upstream_lineage)
        schema_metadata = LookerUtil._get_schema(
            self.source_config.platform_name,
            looker_view.id.view_name,
            looker_view.fields,
            self.reporter,
        )
        if schema_metadata is not None:
            dataset_snapshot.aspects.append(schema_metadata)
        dataset_snapshot.aspects.append(self._get_custom_properties(looker_view))

        return MetadataChangeEvent(proposedSnapshot=dataset_snapshot)

    def get_project_name(self, model_name: str) -> str:
        if self.source_config.project_name is not None:
            return self.source_config.project_name

        assert (
            self.looker_client is not None
        ), "Failed to find a configured Looker API client"
        try:
            model = self.looker_client.lookml_model(model_name, fields="project_name")
            assert (
                model.project_name is not None
            ), f"Failed to find a project name for model {model_name}"
            return model.project_name
        except SDKError:
            raise ValueError(
                f"Could not locate a project name for model {model_name}. Consider configuring a static project name in your config file"
            )

    def get_manifest_if_present(self, folder: pathlib.Path) -> Optional[LookerManifest]:
        manifest_file = folder / "manifest.lkml"
        if manifest_file.exists():
            with manifest_file.open() as fp:
                manifest_dict = lkml.load(fp)

            manifest = LookerManifest(
                project_name=manifest_dict.get("project_name"),
                local_dependencies=[
                    x["project"] for x in manifest_dict.get("local_dependencys", [])
                ],
                remote_dependencies=[
                    LookerRemoteDependency(
                        name=x["name"], url=x["url"], ref=x.get("ref")
                    )
                    for x in manifest_dict.get("remote_dependencys", [])
                ],
            )
            return manifest
        else:
            return None

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        return auto_stale_entity_removal(
            self.stale_entity_removal_handler,
            auto_status_aspect(self.get_workunits_internal()),
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        with tempfile.TemporaryDirectory("lookml_tmp") as tmp_dir:
            # Clone the base_folder if necessary.
            if not self.source_config.base_folder:
                assert self.source_config.github_info
                # we don't have a base_folder, so we need to clone the repo and process it locally
                start_time = datetime.now()
                git_clone = GitClone(tmp_dir)
                # github info deploy key is always populated
                assert self.source_config.github_info.deploy_key
                assert self.source_config.github_info.repo_ssh_locator
                checkout_dir = git_clone.clone(
                    ssh_key=self.source_config.github_info.deploy_key,
                    repo_url=self.source_config.github_info.repo_ssh_locator,
                )
                self.reporter.git_clone_latency = datetime.now() - start_time
                self.source_config.base_folder = checkout_dir.resolve()

            self.base_projects_folder[
                _BASE_PROJECT_NAME
            ] = self.source_config.base_folder
            visited_projects: Set[str] = set()

            # We clone everything that we're pointed at.
            for project, p_ref in self.source_config.project_dependencies.items():
                # If we were given GitHub info, we need to clone the project.
                if isinstance(p_ref, GitHubInfo):
                    assert p_ref.repo_ssh_locator

                    p_cloner = GitClone(f"{tmp_dir}/_included_/{project}")
                    try:
                        p_checkout_dir = p_cloner.clone(
                            ssh_key=(
                                # If a deploy key was provided, use it. Otherwise, fall back
                                # to the main project deploy key.
                                p_ref.deploy_key
                                or (
                                    self.source_config.github_info.deploy_key
                                    if self.source_config.github_info
                                    else None
                                )
                            ),
                            repo_url=p_ref.repo_ssh_locator,
                        )

                        p_ref = p_checkout_dir.resolve()
                    except Exception as e:
                        logger.warning(
                            f"Failed to clone remote project {project}. This can lead to failures in parsing lookml files later on",
                            e,
                        )
                        visited_projects.add(project)
                        continue

                self.base_projects_folder[project] = p_ref

            self._recursively_check_manifests(
                tmp_dir, _BASE_PROJECT_NAME, visited_projects
            )

            yield from self.get_internal_workunits()

    def _recursively_check_manifests(
        self, tmp_dir: str, project_name: str, project_visited: Set[str]
    ) -> None:
        if project_name in project_visited:
            return
        project_visited.add(project_name)

        project_path = self.base_projects_folder.get(project_name)
        if not project_path:
            logger.warning(
                f"Could not find {project_name} in the project_dependencies config. This can lead to failures in parsing lookml files later on.",
            )
            return

        manifest = self.get_manifest_if_present(project_path)
        if manifest:
            # Clone the remote project dependencies.
            for remote_project in manifest.remote_dependencies:
                if remote_project.name in project_visited:
                    continue

                p_cloner = GitClone(f"{tmp_dir}/_remote_/{project_name}")
                try:
                    # TODO: For 100% correctness, we should be consulting
                    # the manifest lock file for the exact ref to use.

                    p_checkout_dir = p_cloner.clone(
                        ssh_key=(
                            self.source_config.github_info.deploy_key
                            if self.source_config.github_info
                            else None
                        ),
                        repo_url=remote_project.url,
                    )

                    self.base_projects_folder[
                        remote_project.name
                    ] = p_checkout_dir.resolve()
                    repo = p_cloner.get_last_repo_cloned()
                    assert repo
                    remote_github_info = GitHubInfo(
                        base_url=remote_project.url,
                        repo="dummy/dummy",  # set to dummy values to bypass validation
                        branch=repo.active_branch.name,
                    )
                    remote_github_info.repo = (
                        ""  # set to empty because url already contains the full path
                    )
                    self.remote_projects_github_info[
                        remote_project.name
                    ] = remote_github_info

                except Exception as e:
                    logger.warning(
                        f"Failed to clone remote project {project_name}. This can lead to failures in parsing lookml files later on",
                        e,
                    )
                    project_visited.add(project_name)
                else:
                    self._recursively_check_manifests(
                        tmp_dir, remote_project.name, project_visited
                    )

            for project in manifest.local_dependencies:
                self._recursively_check_manifests(tmp_dir, project, project_visited)

    def get_internal_workunits(self) -> Iterable[MetadataWorkUnit]:  # noqa: C901
        assert self.source_config.base_folder

        viewfile_loader = LookerViewFileLoader(
            str(self.source_config.base_folder),
            self.base_projects_folder,
            self.reporter,
        )

        # some views can be mentioned by multiple 'include' statements and can be included via different connections.
        # So this set is used to prevent creating duplicate events
        processed_view_map: Dict[str, Set[str]] = {}
        view_connection_map: Dict[str, Tuple[str, str]] = {}

        # The ** means "this directory and all subdirectories", and hence should
        # include all the files we want.
        model_files = sorted(self.source_config.base_folder.glob("**/*.model.lkml"))
        model_suffix_len = len(".model")

        for file_path in model_files:
            self.reporter.report_models_scanned()
            model_name = file_path.stem[:-model_suffix_len]

            if not self.source_config.model_pattern.allowed(model_name):
                self.reporter.report_models_dropped(model_name)
                continue
            try:
                logger.debug(f"Attempting to load model: {file_path}")
                model = self._load_model(str(file_path))
            except Exception as e:
                self.reporter.report_warning(
                    model_name, f"unable to load Looker model at {file_path}: {repr(e)}"
                )
                continue

            assert model.connection is not None
            connectionDefinition = self._get_connection_def_based_on_connection_string(
                model.connection
            )

            if connectionDefinition is None:
                self.reporter.report_warning(
                    f"model-{model_name}",
                    f"Failed to load connection {model.connection}. Check your API key permissions.",
                )
                self.reporter.report_models_dropped(model_name)
                continue

            explore_reachable_views: Set[ProjectInclude] = set()
            if self.source_config.emit_reachable_views_only:
                for explore_dict in model.explores:
                    try:
                        explore: LookerExplore = LookerExplore.from_dict(
                            model_name,
                            explore_dict,
                            model.resolved_includes,
                            viewfile_loader,
                            self.reporter,
                        )
                        if explore.upstream_views:
                            for view_name in explore.upstream_views:
                                explore_reachable_views.add(view_name)
                    except Exception as e:
                        self.reporter.report_warning(
                            f"{model}.explores",
                            f"failed to process {explore_dict} due to {e}. Run with --debug for full stacktrace",
                        )
                        logger.debug("Failed to process explore", exc_info=e)

            processed_view_files = processed_view_map.get(model.connection)
            if processed_view_files is None:
                processed_view_map[model.connection] = set()
                processed_view_files = processed_view_map[model.connection]

            project_name = self.get_project_name(model_name)
            logger.debug(f"Model: {model_name}; Includes: {model.resolved_includes}")

            for include in model.resolved_includes:
                logger.debug(f"Considering {include} for model {model_name}")
                if include.include in processed_view_files:
                    logger.debug(f"view '{include}' already processed, skipping it")
                    continue
                logger.debug(f"Attempting to load view file: {include}")
                looker_viewfile = viewfile_loader.load_viewfile(
                    path=include.include,
                    project_name=include.project,
                    connection=connectionDefinition,
                    reporter=self.reporter,
                )
                if looker_viewfile is not None:
                    for raw_view in looker_viewfile.views:
                        if (
                            self.source_config.emit_reachable_views_only
                            and ProjectInclude(_BASE_PROJECT_NAME, raw_view["name"])
                            not in explore_reachable_views
                        ):
                            logger.debug(
                                f"view {raw_view['name']} is not reachable from an explore, skipping.."
                            )
                            continue

                        self.reporter.report_views_scanned()
                        try:
                            maybe_looker_view = LookerView.from_looker_dict(
                                include.project
                                if include.project != _BASE_PROJECT_NAME
                                else project_name,
                                model_name,
                                raw_view,
                                connectionDefinition,
                                looker_viewfile,
                                viewfile_loader,
                                self.reporter,
                                self.source_config.max_file_snippet_length,
                                self.source_config.parse_table_names_from_sql,
                                self.source_config.sql_parser,
                                self.source_config.extract_column_level_lineage,
                                self.source_config.populate_sql_logic_for_missing_descriptions,
                                process_isolation_for_sql_parsing=self.source_config.process_isolation_for_sql_parsing,
                            )
                        except Exception as e:
                            self.reporter.report_warning(
                                include.include,
                                f"unable to load Looker view {raw_view}: {repr(e)}",
                            )
                            continue
                        if maybe_looker_view:
                            if self.source_config.view_pattern.allowed(
                                maybe_looker_view.id.view_name
                            ):
                                view_connection_mapping = view_connection_map.get(
                                    maybe_looker_view.id.view_name
                                )
                                if not view_connection_mapping:
                                    view_connection_map[
                                        maybe_looker_view.id.view_name
                                    ] = (model_name, model.connection)
                                    # first time we are discovering this view
                                    mce = self._build_dataset_mce(maybe_looker_view)
                                    workunit = MetadataWorkUnit(
                                        id=f"lookml-view-{maybe_looker_view.id}",
                                        mce=mce,
                                    )
                                    processed_view_files.add(include.include)
                                    self.reporter.report_workunit(workunit)
                                    yield workunit
                                    for mcp in self._build_dataset_mcps(
                                        maybe_looker_view
                                    ):
                                        # We want to treat mcp aspects as optional, so allowing failures in this aspect to be treated as warnings rather than failures
                                        workunit = MetadataWorkUnit(
                                            id=f"lookml-view-{mcp.aspectName}-{maybe_looker_view.id}",
                                            mcp=mcp,
                                            treat_errors_as_warnings=True,
                                        )
                                        self.reporter.report_workunit(workunit)
                                        yield workunit
                                else:
                                    (
                                        prev_model_name,
                                        prev_model_connection,
                                    ) = view_connection_mapping
                                    if prev_model_connection != model.connection:
                                        # this view has previously been discovered and emitted using a different connection
                                        logger.warning(
                                            f"view {maybe_looker_view.id.view_name} from model {model_name}, connection {model.connection} was previously processed via model {prev_model_name}, connection {prev_model_connection} and will likely lead to incorrect lineage to the underlying tables"
                                        )
                                        if (
                                            not self.source_config.emit_reachable_views_only
                                        ):
                                            logger.warning(
                                                "Consider enabling the `emit_reachable_views_only` flag to handle this case."
                                            )
                            else:
                                self.reporter.report_views_dropped(
                                    str(maybe_looker_view.id)
                                )

        if (
            self.source_config.tag_measures_and_dimensions
            and self.reporter.events_produced != 0
        ):
            # Emit tag MCEs for measures and dimensions:
            for tag_mce in LookerUtil.get_tag_mces():
                workunit = MetadataWorkUnit(
                    id=f"tag-{tag_mce.proposedSnapshot.urn}", mce=tag_mce
                )
                self.reporter.report_workunit(workunit)
                yield workunit

    def get_report(self):
        return self.reporter

    def get_platform_instance_id(self) -> str:
        return self.source_config.platform_instance or self.platform

    def close(self):
        self.prepare_for_commit()
