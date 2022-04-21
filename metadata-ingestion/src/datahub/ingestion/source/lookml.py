import glob
import importlib
import itertools
import logging
import os
import pathlib
import re
import sys
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from dataclasses import replace
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Type

import pydantic
from looker_sdk.error import SDKError
from looker_sdk.sdk.api31.methods import Looker31SDK
from looker_sdk.sdk.api31.models import DBConnection
from pydantic import root_validator, validator
from pydantic.fields import Field

from datahub.configuration.source_common import EnvBasedSourceConfigBase
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.looker_common import (
    LookerCommonConfig,
    LookerUtil,
    LookerViewId,
    ViewField,
    ViewFieldType,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetPropertiesClass,
    SubTypesClass,
)
from datahub.utilities.sql_parser import SQLParser

if sys.version_info >= (3, 7):
    import lkml
else:
    raise ModuleNotFoundError("The lookml plugin requires Python 3.7 or newer.")

import datahub.emitter.mce_builder as builder
from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.looker import (
    LookerAPI,
    LookerAPIConfig,
    TransportOptionsConfig,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import BrowsePaths, Status
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineage,
    ViewProperties,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent

assert sys.version_info[1] >= 7  # needed for mypy

logger = logging.getLogger(__name__)


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
    def platform_env_must_be_one_of(cls, v: str) -> str:
        return EnvBasedSourceConfigBase.env_must_be_one_of(v)

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

        if looker_connection.dialect_name is not None:
            for extractor_pattern, extracting_function in extractors.items():
                if re.match(extractor_pattern, looker_connection.dialect_name):
                    (platform, db, schema) = extracting_function(looker_connection)
                    return cls(platform=platform, default_db=db, default_schema=schema)
            raise ConfigurationError(
                f"Could not find an appropriate platform for looker_connection: {looker_connection.name} with dialect: {looker_connection.dialect_name}"
            )
        else:
            raise ConfigurationError(
                f"Unable to fetch a fully filled out connection for {looker_connection.name}. Please check your API permissions."
            )


class LookMLSourceConfig(LookerCommonConfig):
    base_folder: pydantic.DirectoryPath
    connection_to_platform_map: Optional[Dict[str, LookerConnectionDefinition]]
    model_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    view_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    parse_table_names_from_sql: bool = False
    sql_parser: str = "datahub.utilities.sql_parser.DefaultSQLParser"
    api: Optional[LookerAPIConfig]
    project_name: Optional[str]
    transport_options: Optional[TransportOptionsConfig]

    @validator("platform_instance")
    def platform_instance_not_supported(cls, v: str) -> str:
        raise ConfigurationError(
            "LookML Source doesn't support platform instance at the top level. However connection-specific platform instances are supported for generating lineage edges. Read the documentation to find out more."
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


@dataclass
class LookMLSourceReport(SourceReport):
    models_discovered: int = 0
    models_dropped: List[str] = dataclass_field(default_factory=list)
    views_discovered: int = 0
    views_dropped: List[str] = dataclass_field(default_factory=list)

    def report_models_scanned(self) -> None:
        self.models_discovered += 1

    def report_views_scanned(self) -> None:
        self.views_discovered += 1

    def report_models_dropped(self, model: str) -> None:
        self.models_dropped.append(model)

    def report_views_dropped(self, view: str) -> None:
        self.views_dropped.append(view)


@dataclass
class LookerModel:
    connection: str
    includes: List[str]
    explores: List[dict]
    resolved_includes: List[str]

    @staticmethod
    def from_looker_dict(
        looker_model_dict: dict,
        base_folder: str,
        path: str,
        reporter: LookMLSourceReport,
    ) -> "LookerModel":
        logger.debug(f"Loading model from {path}")
        connection = looker_model_dict["connection"]
        includes = looker_model_dict.get("includes", [])
        resolved_includes = LookerModel.resolve_includes(
            includes,
            base_folder,
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
        base_folder: str,
        path: str,
        reporter: LookMLSourceReport,
        seen_so_far: Set[str],
        traversal_path: str = "",  # a cosmetic parameter to aid debugging
    ) -> List[str]:
        """Resolve ``include`` statements in LookML model files to a list of ``.lkml`` files.

        For rules on how LookML ``include`` statements are written, see
            https://docs.looker.com/data-modeling/getting-started/ide-folders#wildcard_examples
        """
        resolved = []
        for inc in includes:
            # Filter out dashboards - we get those through the looker source.
            if (
                inc.endswith(".dashboard")
                or inc.endswith(".dashboard.lookml")
                or inc.endswith(".dashboard.lkml")
            ):
                logger.debug(f"include '{inc}' is a dashboard, skipping it")
                continue

            # Massage the looker include into a valid glob wildcard expression
            if inc.startswith("/"):
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
                    logger.debug(f"include '{inc}' is a dashboard, skipping it")
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
                                    base_folder,
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

            resolved.extend(included_files)
        return resolved


@dataclass
class LookerViewFile:
    absolute_file_path: str
    connection: Optional[str]
    includes: List[str]
    resolved_includes: List[str]
    views: List[Dict]
    raw_file_content: str

    @classmethod
    def from_looker_dict(
        cls,
        absolute_file_path: str,
        looker_view_file_dict: dict,
        base_folder: str,
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
            base_folder,
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

    def __init__(self, base_folder: str, reporter: LookMLSourceReport) -> None:
        self.viewfile_cache: Dict[str, LookerViewFile] = {}
        self._base_folder = base_folder
        self.reporter = reporter

    def is_view_seen(self, path: str) -> bool:
        return path in self.viewfile_cache

    def _load_viewfile(
        self, path: str, reporter: LookMLSourceReport
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
                    base_folder=self._base_folder,
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
        connection: LookerConnectionDefinition,
        reporter: LookMLSourceReport,
    ) -> Optional[LookerViewFile]:
        viewfile = self._load_viewfile(path, reporter)
        if viewfile is None:
            return None

        return replace(viewfile, connection=connection)


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
        module_name, cls_name = sql_parser_path.rsplit(".", 1)
        import sys

        logger.debug(sys.path)
        parser_cls = getattr(importlib.import_module(module_name), cls_name)
        if not issubclass(parser_cls, SQLParser):
            raise ValueError(f"must be derived from {SQLParser}; got {parser_cls}")

        return parser_cls

    @classmethod
    def _get_sql_info(cls, sql: str, sql_parser_path: str) -> SQLInfo:
        parser_cls = cls._import_sql_parser_cls(sql_parser_path)

        parser_instance: SQLParser = parser_cls(sql)

        sql_table_names: List[str] = parser_instance.get_tables()
        column_names: List[str] = parser_instance.get_columns()
        logger.debug(f"Column names parsed = {column_names}")
        # Drop table names with # in them
        sql_table_names = [t for t in sql_table_names if "#" not in t]

        # Remove quotes from table names
        sql_table_names = [t.replace('"', "") for t in sql_table_names]
        sql_table_names = [t.replace("`", "") for t in sql_table_names]

        return SQLInfo(table_names=sql_table_names, column_names=column_names)

    @classmethod
    def _get_fields(
        cls, field_list: List[Dict], type_cls: ViewFieldType
    ) -> List[ViewField]:
        fields = []
        for field_dict in field_list:
            is_primary_key = field_dict.get("primary_key", "no") == "yes"
            name = field_dict["name"]
            native_type = field_dict.get("type", "string")
            description = field_dict.get("description", "")
            field = ViewField(
                name=name,
                type=native_type,
                description=description,
                is_primary_key=is_primary_key,
                field_type=type_cls,
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
        parse_table_names_from_sql: bool = False,
        sql_parser_path: str = "datahub.utilities.sql_parser.DefaultSQLParser",
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
        derived_table = looker_view.get("derived_table", None)

        dimensions = cls._get_fields(
            looker_view.get("dimensions", []), ViewFieldType.DIMENSION
        )
        dimension_groups = cls._get_fields(
            looker_view.get("dimension_groups", []), ViewFieldType.DIMENSION_GROUP
        )
        measures = cls._get_fields(
            looker_view.get("measures", []), ViewFieldType.MEASURE
        )
        fields: List[ViewField] = dimensions + dimension_groups + measures

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
            )
            # also store the view logic and materialization
            view_logic = looker_viewfile.raw_file_content
            if "sql" in derived_table:
                view_logic = derived_table["sql"]
                view_lang = "sql"
            if "explore_source" in derived_table:
                view_logic = str(derived_table["explore_source"])
                view_lang = "lookml"

            materialized = False
            for k in derived_table:
                if k in ["datagroup_trigger", "sql_trigger_value", "persist_for"]:
                    materialized = True
            if "materialized_view" in derived_table:
                materialized = (
                    True if derived_table["materialized_view"] == "yes" else False
                )

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
        # object in the database.
        if sql_table_name is not None:
            # If sql_table_name is set, there is a single dependency in the view, on the sql_table_name.
            sql_table_names = [sql_table_name]
        else:
            # Otherwise, default to the view name as per the docs:
            # https://docs.looker.com/reference/view-params/sql_table_name-for-view
            sql_table_names = [view_name]

        output_looker_view = LookerView(
            id=LookerViewId(
                project_name=project_name, model_name=model_name, view_name=view_name
            ),
            absolute_file_path=looker_viewfile.absolute_file_path,
            sql_table_names=sql_table_names,
            connection=connection,
            fields=fields,
            raw_file_content=looker_viewfile.raw_file_content,
        )
        return output_looker_view

    @classmethod
    def _extract_metadata_from_sql_query(
        cls: Type,
        reporter: SourceReport,
        parse_table_names_from_sql: bool,
        sql_parser_path: str,
        view_name: str,
        sql_table_name: Optional[str],
        derived_table: dict,
        fields: List[ViewField],
    ) -> Tuple[List[ViewField], List[str]]:
        sql_table_names: List[str] = []
        if parse_table_names_from_sql and "sql" in derived_table:
            logger.debug(f"Parsing sql from derived table section of view: {view_name}")
            sql_query = derived_table["sql"]

            # Skip queries that contain liquid variables. We currently don't parse them correctly
            if "{%" in sql_query:
                logger.debug(
                    f"{view_name}: Skipping sql_query parsing since it contains liquid variables"
                )
                return fields, sql_table_names
            # Looker supports sql fragments that omit the SELECT and FROM parts of the query
            # Add those in if we detect that it is missing
            if not re.search(r"SELECT\s", sql_query, flags=re.I):
                # add a SELECT clause at the beginning
                sql_query = "SELECT " + sql_query
            if not re.search(r"FROM\s", sql_query, flags=re.I):
                # add a FROM clause at the end
                sql_query = f"{sql_query} FROM {sql_table_name if sql_table_name is not None else view_name}"
                # Get the list of tables in the query
            try:
                sql_info = cls._get_sql_info(sql_query, sql_parser_path)
                sql_table_names = sql_info.table_names
                column_names = sql_info.column_names
                if fields == []:
                    # it seems like the view is defined purely as sql, let's try using the column names to populate the schema
                    fields = [
                        # set types to unknown for now as our sql parser doesn't give us column types yet
                        ViewField(c, "unknown", "", ViewFieldType.UNKNOWN)
                        for c in column_names
                    ]
            except Exception as e:
                reporter.report_warning(
                    f"looker-view-{view_name}",
                    f"Failed to parse sql query, lineage will not be accurate. Exception: {e}",
                )

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

        # Or it could live in one of the included files. We do not know which file the base view
        # lives in, so we try them all!
        for include in looker_viewfile.resolved_includes:
            included_looker_viewfile = looker_viewfile_loader.load_viewfile(
                include, connection, reporter
            )
            if not included_looker_viewfile:
                logger.warning(
                    f"unable to load {include} (included from {looker_viewfile.absolute_file_path})"
                )
                continue
            for raw_view in included_looker_viewfile.views:
                raw_view_name = raw_view["name"]
                # Make sure to skip loading view we are currently trying to resolve
                if raw_view_name == target_view_name:
                    return raw_view

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


class LookMLSource(Source):
    source_config: LookMLSourceConfig
    reporter: LookMLSourceReport
    looker_client: Optional[Looker31SDK] = None

    def __init__(self, config: LookMLSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.reporter = LookMLSourceReport()
        if self.source_config.api:
            looker_api = LookerAPI(self.source_config.api)
            self.looker_client = looker_api.get_client()
            try:
                self.looker_client.all_connections()
            except SDKError:
                raise ValueError(
                    "Failed to retrieve connections from looker client. Please check to ensure that you have manage_models permission enabled on this API key."
                )

    @classmethod
    def create(cls, config_dict, ctx):
        config = LookMLSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def _load_model(self, path: str) -> LookerModel:
        with open(path, "r") as file:
            logger.debug(f"Loading model from file {path}")
            parsed = lkml.load(file)
            looker_model = LookerModel.from_looker_dict(
                parsed, str(self.source_config.base_folder), path, self.reporter
            )
        return looker_model

    def _platform_names_have_2_parts(self, platform: str) -> bool:
        if platform in ["hive", "mysql", "athena"]:
            return True
        else:
            return False

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
            # fully qualified
            return sql_table_name.lower()

        if parts == 1:
            # Bare table form
            if self._platform_names_have_2_parts(connection_def.platform):
                dataset_name = f"{connection_def.default_db}.{sql_table_name}"
            else:
                dataset_name = f"{connection_def.default_db}.{connection_def.default_schema}.{sql_table_name}"
            return dataset_name

        if parts == 2:
            # if this is a 2 part platform, we are fine
            if self._platform_names_have_2_parts(connection_def.platform):
                return sql_table_name
            # otherwise we attach the default top-level container
            dataset_name = f"{connection_def.default_db}.{sql_table_name}"
            return dataset_name

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

            upstream = UpstreamClass(
                dataset=self._construct_datalineage_urn(sql_table_name, looker_view),
                type=DatasetLineageTypeClass.VIEW,
            )
            upstreams.append(upstream)

        if upstreams != []:
            return UpstreamLineage(upstreams=upstreams)
        else:
            return None

    def _get_custom_properties(self, looker_view: LookerView) -> DatasetPropertiesClass:
        file_path = (
            str(pathlib.Path(looker_view.absolute_file_path).resolve())
            .replace(str(self.source_config.base_folder.resolve()), "")
            .lstrip(os.sep)
        )

        custom_properties = {
            "looker.file.content": looker_view.raw_file_content[
                0:512000
            ],  # grab a limited slice of characters from the file
            "looker.file.path": file_path,
        }
        dataset_props = DatasetPropertiesClass(
            name=looker_view.id.view_name, customProperties=custom_properties
        )

        if self.source_config.github_info is not None:
            github_file_url = self.source_config.github_info.get_url_for_file_path(
                file_path
            )
            dataset_props.externalUrl = github_file_url

        return dataset_props

    def _build_dataset_mcps(
        self, looker_view: LookerView
    ) -> List[MetadataChangeProposalWrapper]:
        events = []
        subTypeEvent = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=looker_view.id.get_urn(self.source_config),
            aspectName="subTypes",
            aspect=SubTypesClass(typeNames=["view"]),
        )
        events.append(subTypeEvent)
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

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)

        return mce

    def get_project_name(self, model_name: str) -> str:
        if self.source_config.project_name is not None:
            return self.source_config.project_name

        assert (
            self.looker_client is not None
        ), "Failed to find a configured Looker API client"
        try:
            model = self.looker_client.lookml_model(
                model_name,
                "project_name",
                transport_options=self.source_config.transport_options.get_transport_options()
                if self.source_config.transport_options is not None
                else None,
            )
            assert (
                model.project_name is not None
            ), f"Failed to find a project name for model {model_name}"
            return model.project_name
        except SDKError:
            raise ValueError(
                f"Could not locate a project name for model {model_name}. Consider configuring a static project name in your config file"
            )

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:  # noqa: C901
        viewfile_loader = LookerViewFileLoader(
            str(self.source_config.base_folder), self.reporter
        )

        # some views can be mentioned by multiple 'include' statements, so this set is used to prevent
        # creating duplicate MCE messages
        processed_view_files: Set[str] = set()

        # The ** means "this directory and all subdirectories", and hence should
        # include all the files we want.
        model_files = sorted(self.source_config.base_folder.glob("**/*.model.lkml"))
        model_suffix_len = len(".model")

        for file_path in model_files:
            self.reporter.report_models_scanned()
            model_name = file_path.stem[0:-model_suffix_len]

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

            project_name = self.get_project_name(model_name)

            for include in model.resolved_includes:
                logger.debug(f"Considering {include} for model {model_name}")
                if include in processed_view_files:
                    logger.debug(f"view '{include}' already processed, skipping it")
                    continue

                logger.debug(f"Attempting to load view file: {include}")
                looker_viewfile = viewfile_loader.load_viewfile(
                    include, connectionDefinition, self.reporter
                )
                if looker_viewfile is not None:
                    for raw_view in looker_viewfile.views:
                        self.reporter.report_views_scanned()
                        try:
                            maybe_looker_view = LookerView.from_looker_dict(
                                project_name,
                                model_name,
                                raw_view,
                                connectionDefinition,
                                looker_viewfile,
                                viewfile_loader,
                                self.reporter,
                                self.source_config.parse_table_names_from_sql,
                                self.source_config.sql_parser,
                            )
                        except Exception as e:
                            self.reporter.report_warning(
                                include,
                                f"unable to load Looker view {raw_view}: {repr(e)}",
                            )
                            continue
                        if maybe_looker_view:
                            if self.source_config.view_pattern.allowed(
                                maybe_looker_view.id.view_name
                            ):
                                mce = self._build_dataset_mce(maybe_looker_view)
                                workunit = MetadataWorkUnit(
                                    id=f"lookml-view-{maybe_looker_view.id}",
                                    mce=mce,
                                )
                                self.reporter.report_workunit(workunit)
                                processed_view_files.add(include)
                                yield workunit

                                for mcp in self._build_dataset_mcps(maybe_looker_view):
                                    # We want to treat mcp aspects as optional, so allowing failures in this aspect to be treated as warnings rather than failures
                                    workunit = MetadataWorkUnit(
                                        id=f"lookml-view-{mcp.aspectName}-{maybe_looker_view.id}",
                                        mcp=mcp,
                                        treat_errors_as_warnings=True,
                                    )
                                    self.reporter.report_workunit(workunit)
                                    yield workunit
                            else:
                                self.reporter.report_views_dropped(
                                    str(maybe_looker_view.id)
                                )

        if (
            self.source_config.tag_measures_and_dimensions
            and self.reporter.workunits_produced != 0
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

    def close(self):
        pass
