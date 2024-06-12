import itertools
import logging
import pathlib
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Type, cast

import lkml
import lkml.simple
from looker_sdk.error import SDKError
from looker_sdk.sdk.api40.models import DBConnection

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigurationError
from datahub.configuration.git import GitInfo
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.registry import import_path
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceCapability
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.git.git_import import GitClone
from datahub.ingestion.source.looker.lkml_patched import load_lkml
from datahub.ingestion.source.looker.looker_common import (
    CORPUSER_DATAHUB,
    LookerConnectionDefinition,
    LookerExplore,
    LookerUtil,
    LookerViewId,
    ProjectInclude,
    ViewField,
    ViewFieldType,
    ViewFieldValue,
    gen_project_key,
)
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
from datahub.ingestion.source.looker.lookml_config import (
    _BASE_PROJECT_NAME,
    _MODEL_FILE_EXTENSION,
    DERIVED_VIEW_PATTERN,
    VIEW_LANGUAGE_LOOKML,
    VIEW_LANGUAGE_SQL,
    LookMLSourceConfig,
    LookMLSourceReport,
)
from datahub.ingestion.source.looker.lookml_dataclasses import LookerViewFile
from datahub.ingestion.source.looker.lookml_resolver import (
    LookerModel,
    LookerRefinementResolver,
    LookerViewFileLoader,
    LookerViewIdCache,
    get_derived_view_urn,
    is_derived_view,
    resolve_derived_view_urn,
)
from datahub.ingestion.source.looker.lookml_sql_parser import SqlQuery, ViewFieldBuilder
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
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
    AuditStampClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ContainerClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageUpstreamTypeClass,
    SubTypesClass,
)
from datahub.sql_parsing.sqlglot_lineage import ColumnRef
from datahub.utilities.sql_parser import SQLParser

logger = logging.getLogger(__name__)


def _platform_names_have_2_parts(platform: str) -> bool:
    return platform in {"hive", "mysql", "athena"}


def _generate_fully_qualified_name(
    sql_table_name: str,
    connection_def: LookerConnectionDefinition,
    reporter: LookMLSourceReport,
) -> str:
    """Returns a fully qualified dataset name, resolved through a connection definition.
    Input sql_table_name can be in three forms: table, db.table, db.schema.table"""
    # TODO: This function should be extracted out into a Platform specific naming class since name translations
    #  are required across all connectors

    # Bigquery has "project.db.table" which can be mapped to db.schema.table form
    # All other relational db's follow "db.schema.table"
    # With the exception of mysql, hive, athena which are "db.table"

    # first detect which one we have
    parts = len(sql_table_name.split("."))

    if parts == 3:
        # fully qualified, but if platform is of 2-part, we drop the first level
        if _platform_names_have_2_parts(connection_def.platform):
            sql_table_name = ".".join(sql_table_name.split(".")[1:])
        return sql_table_name.lower()

    if parts == 1:
        # Bare table form
        if _platform_names_have_2_parts(connection_def.platform):
            dataset_name = f"{connection_def.default_db}.{sql_table_name}"
        else:
            dataset_name = f"{connection_def.default_db}.{connection_def.default_schema}.{sql_table_name}"
        return dataset_name.lower()

    if parts == 2:
        # if this is a 2 part platform, we are fine
        if _platform_names_have_2_parts(connection_def.platform):
            return sql_table_name.lower()
        # otherwise we attach the default top-level container
        dataset_name = f"{connection_def.default_db}.{sql_table_name}"
        return dataset_name.lower()

    reporter.report_warning(
        key=sql_table_name, reason=f"{sql_table_name} has more than 3 parts."
    )
    return sql_table_name.lower()


@dataclass
class SQLInfo:
    table_names: List[str]
    column_names: List[str]


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
            include.include,
            include.project,
            connection,
            reporter,
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
    upstream_explores: List[str]
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
            if extract_column_level_lineage:
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
                else:
                    # If no SQL is specified, we assume this is referencing an upstream field
                    # with the same name. This commonly happens for extends and derived tables.
                    upstream_fields.append(name)

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
    def determine_view_file_path(
        cls, base_folder_path: str, absolute_file_path: str
    ) -> str:
        splits: List[str] = absolute_file_path.split(base_folder_path, 1)
        if len(splits) != 2:
            logger.debug(
                f"base_folder_path({base_folder_path}) and absolute_file_path({absolute_file_path}) not matching"
            )
            return ViewFieldValue.NOT_AVAILABLE.value

        file_path: str = splits[1]
        logger.debug(f"file_path={file_path}")

        return file_path.strip(
            "/"
        )  # strip / from path to make it equivalent to source_file attribute of LookerModelExplore API

    @classmethod
    def from_looker_dict(
        cls,
        project_name: str,
        base_folder_path: str,
        model_name: str,
        looker_view: dict,
        connection: LookerConnectionDefinition,
        looker_viewfile: LookerViewFile,
        looker_viewfile_loader: LookerViewFileLoader,
        looker_refinement_resolver: LookerRefinementResolver,
        looker_view_id_cache: LookerViewIdCache,
        reporter: LookMLSourceReport,
        max_file_snippet_length: int,
        config: LookMLSourceConfig,
        ctx: PipelineContext,
        parse_table_names_from_sql: bool = False,
        extract_col_level_lineage: bool = False,
        populate_sql_logic_in_descriptions: bool = False,
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
            looker_refinement_resolver=looker_refinement_resolver,
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
            looker_refinement_resolver=looker_refinement_resolver,
            field="derived_table",
            reporter=reporter,
        )

        fields = ViewField.all_view_fields_from_dict(
            looker_view,
            extract_col_level_lineage,
            populate_sql_logic_in_descriptions=populate_sql_logic_in_descriptions,
        )

        # Prep "default" values for the view, which will be overridden by the logic below.
        view_logic = looker_viewfile.raw_file_content[:max_file_snippet_length]
        sql_table_names: List[str] = []
        upstream_explores: List[str] = []

        file_path = LookerView.determine_view_file_path(
            base_folder_path, looker_viewfile.absolute_file_path
        )

        looker_view_id: LookerViewId = LookerViewId(
            project_name=project_name,
            model_name=model_name,
            view_name=view_name,
            file_path=file_path,
        )

        if derived_table is not None:
            # Derived tables can either be a SQL query or a LookML explore.
            # See https://cloud.google.com/looker/docs/derived-tables.
            if "sql" in derived_table:
                view_logic = derived_table["sql"]
                view_lang = VIEW_LANGUAGE_SQL
                # Parse SQL to extract dependencies.
                if parse_table_names_from_sql:
                    (
                        fields,
                        sql_table_names,
                    ) = cls._extract_metadata_from_derived_table_sql(
                        reporter=reporter,
                        connection=connection,
                        ctx=ctx,
                        view_name=view_name,
                        view_urn=looker_view_id.get_urn(config=config),
                        sql_table_name=sql_table_name,
                        sql_query=view_logic,
                        fields=fields,
                        liquid_variable=config.liquid_variable,
                    )
                    # resolve view name ${<view-name>.SQL_TABLE_NAME} to urn used in derived_table
                    # https://cloud.google.com/looker/docs/derived-tables
                    (fields, sql_table_names,) = resolve_derived_view_urn(
                        base_folder_path=base_folder_path,
                        looker_view_id_cache=looker_view_id_cache,
                        fields=fields,
                        upstream_urns=sql_table_names,
                        config=config,
                    )
            elif "explore_source" in derived_table:
                # This is called a "native derived table".
                # See https://cloud.google.com/looker/docs/creating-ndts.
                explore_source = derived_table["explore_source"]

                # We want this to render the full lkml block
                # e.g. explore_source: source_name { ... }
                # As such, we use the full derived_table instead of the explore_source.
                view_logic = str(lkml.dump(derived_table))[:max_file_snippet_length]
                view_lang = VIEW_LANGUAGE_LOOKML

                (
                    fields,
                    upstream_explores,
                ) = cls._extract_metadata_from_derived_table_explore(
                    reporter, view_name, explore_source, fields
                )

            materialized = False
            for k in derived_table:
                if k in ["datagroup_trigger", "sql_trigger_value", "persist_for"]:
                    materialized = True
            if "materialized_view" in derived_table:
                materialized = derived_table["materialized_view"] == "yes"

            view_details = ViewProperties(
                materialized=materialized, viewLogic=view_logic, viewLanguage=view_lang
            )
        else:
            # If not a derived table, then this view essentially wraps an existing
            # object in the database or another lookml view. If sql_table_name is set, there is a single
            # dependency in the view, on the sql_table_name.
            # Otherwise, default to the view name as per the docs:
            # https://docs.looker.com/reference/view-params/sql_table_name-for-view

            sql_table_name = view_name if sql_table_name is None else sql_table_name
            view_urn: Optional[str]
            if is_derived_view(sql_table_name):
                extracted_view_name: str = re.sub(
                    DERIVED_VIEW_PATTERN, r"\1", sql_table_name
                )

                view_urn = get_derived_view_urn(
                    qualified_table_name=_generate_fully_qualified_name(
                        extracted_view_name.lower(), connection, reporter
                    ),
                    looker_view_id_cache=looker_view_id_cache,
                    base_folder_path=base_folder_path,
                    config=config,
                )
            else:
                # Ensure sql_table_name is in canonical form (add in db, schema names)
                view_urn = builder.make_dataset_urn_with_platform_instance(
                    platform=connection.platform,
                    name=_generate_fully_qualified_name(
                        sql_table_name.lower(), connection, reporter
                    ),
                    platform_instance=connection.platform_instance,
                    env=connection.platform_env or config.env,
                )

            if view_urn is None:
                reporter.report_warning(
                    f"looker-view-{view_name}",
                    f"failed to resolve urn for derived view {view_name}.",
                )
            else:
                sql_table_names = [view_urn]

            view_details = ViewProperties(
                materialized=False,
                viewLogic=view_logic,
                viewLanguage=VIEW_LANGUAGE_LOOKML,
            )

        return LookerView(
            id=looker_view_id,
            absolute_file_path=looker_viewfile.absolute_file_path,
            connection=connection,
            sql_table_names=sql_table_names,
            upstream_explores=upstream_explores,
            fields=fields,
            raw_file_content=looker_viewfile.raw_file_content,
            view_details=view_details,
        )

    @classmethod
    def _extract_metadata_from_derived_table_sql(
        cls,
        reporter: LookMLSourceReport,
        connection: LookerConnectionDefinition,
        ctx: PipelineContext,
        view_name: str,
        view_urn: str,
        sql_table_name: Optional[str],
        sql_query: str,
        fields: List[ViewField],
        liquid_variable: Dict[Any, Any],
    ) -> Tuple[List[ViewField], List[str]]:

        logger.debug(f"Parsing sql from derived table section of view: {view_name}")
        reporter.query_parse_attempts += 1
        upstream_urns: List[str] = []
        # TODO: also support ${EXTENDS} and ${TABLE}
        try:
            view_field_builder: ViewFieldBuilder = ViewFieldBuilder(
                fields=fields,
                sql_query=SqlQuery(
                    lookml_sql_query=sql_query,
                    liquid_variable=liquid_variable,
                    view_name=sql_table_name
                    if sql_table_name is not None
                    else view_name,
                ),
                reporter=reporter,
                ctx=ctx,
            )

            fields, upstream_urns = view_field_builder.create_or_update_fields(
                view_urn=view_urn,
                connection=connection,
            )

        except Exception as e:
            reporter.query_parse_failures += 1
            reporter.report_warning(
                f"looker-view-{view_name}",
                f"Failed to parse sql query, lineage will not be accurate. Exception: {e}",
            )

        return fields, upstream_urns

    @classmethod
    def _extract_metadata_from_derived_table_explore(
        cls,
        reporter: LookMLSourceReport,
        view_name: str,
        explore_source: dict,
        fields: List[ViewField],
    ) -> Tuple[List[ViewField], List[str]]:
        logger.debug(
            f"Parsing explore_source from derived table section of view: {view_name}"
        )

        upstream_explores = [explore_source["name"]]

        explore_columns = explore_source.get("columns", [])
        # TODO: We currently don't support column-level lineage for derived_column.
        # In order to support it, we'd need to parse the `sql` field of the derived_column.

        # The fields in the view are actually references to the fields in the explore.
        # As such, we need to perform an extra mapping step to update
        # the upstream column names.
        for field in fields:
            for i, upstream_field in enumerate(field.upstream_fields):
                # Find the matching column in the explore.
                for explore_column in explore_columns:
                    if explore_column["name"] == upstream_field:
                        field.upstream_fields[i] = explore_column.get(
                            "field", explore_column["name"]
                        )
                        break

        return fields, upstream_explores

    @classmethod
    def resolve_extends_view_name(
        cls,
        connection: LookerConnectionDefinition,
        looker_viewfile: LookerViewFile,
        looker_viewfile_loader: LookerViewFileLoader,
        looker_refinement_resolver: LookerRefinementResolver,
        target_view_name: str,
        reporter: LookMLSourceReport,
    ) -> Optional[dict]:
        # The view could live in the same file.
        for raw_view in looker_viewfile.views:
            raw_view_name = raw_view["name"]
            if raw_view_name == target_view_name:
                return looker_refinement_resolver.apply_view_refinement(raw_view)

        # Or, it could live in one of the imports.
        view = _find_view_from_resolved_includes(
            connection,
            looker_viewfile.resolved_includes,
            looker_viewfile_loader,
            target_view_name,
            reporter,
        )
        if view:
            return looker_refinement_resolver.apply_view_refinement(view[1])
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
        looker_refinement_resolver: LookerRefinementResolver,
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
                connection,
                looker_viewfile,
                looker_viewfile_loader,
                looker_refinement_resolver,
                extend,
                reporter,
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
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "Use the `platform_instance` and `connection_to_platform_map` fields",
)
@capability(SourceCapability.LINEAGE_COARSE, "Supported by default")
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, configured using `extract_column_level_lineage`",
)
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
    remote_projects_git_info: Dict[str, GitInfo] = {}

    def __init__(self, config: LookMLSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.ctx = ctx
        self.reporter = LookMLSourceReport()

        # To keep track of projects (containers) which have already been ingested
        self.processed_projects: List[str] = []

        if self.source_config.api:
            self.looker_client = LookerAPI(self.source_config.api)
            self.reporter._looker_api = self.looker_client
            try:
                self.looker_client.all_connections()
            except SDKError:
                raise ValueError(
                    "Failed to retrieve connections from looker client. Please check to ensure that you have "
                    "manage_models permission enabled on this API key."
                )

    def _load_model(self, path: str) -> LookerModel:
        logger.debug(f"Loading model from file {path}")
        parsed = load_lkml(path)
        looker_model = LookerModel.from_looker_dict(
            parsed,
            _BASE_PROJECT_NAME,
            self.source_config.project_name,
            self.base_projects_folder,
            path,
            self.reporter,
        )
        return looker_model

    def _get_connection_def_based_on_connection_string(
        self, connection: str
    ) -> Optional[LookerConnectionDefinition]:
        if self.source_config.connection_to_platform_map is None:
            self.source_config.connection_to_platform_map = {}
        assert self.source_config.connection_to_platform_map is not None
        connection_def: Optional[LookerConnectionDefinition] = None

        if connection in self.source_config.connection_to_platform_map:
            connection_def = self.source_config.connection_to_platform_map[connection]
        elif self.looker_client:
            try:
                looker_connection: DBConnection = self.looker_client.connection(
                    connection
                )
            except SDKError:
                logger.error(
                    f"Failed to retrieve connection {connection} from Looker. This usually happens when the "
                    f"credentials provided are not admin credentials."
                )
            else:
                try:
                    connection_def = LookerConnectionDefinition.from_looker_connection(
                        looker_connection
                    )

                    # Populate the cache (using the config map) to avoid calling looker again for this connection
                    self.source_config.connection_to_platform_map[
                        connection
                    ] = connection_def
                except ConfigurationError:
                    self.reporter.report_warning(
                        f"connection-{connection}",
                        "Failed to load connection from Looker",
                    )

        # set the platform_env in connectionDefinition as per description provided in
        # LookerConnectionDefinition.platform_env
        if connection_def:
            if connection_def.platform_env is None:
                connection_def.platform_env = self.source_config.env

        return connection_def

    def _get_upstream_lineage(
        self, looker_view: LookerView
    ) -> Optional[UpstreamLineage]:
        # Merge dataset upstreams with sql table upstreams.
        upstream_dataset_urns = []
        for upstream_explore in looker_view.upstream_explores:
            # We're creating a "LookerExplore" just to use the urn generator.
            upstream_dataset_urn = LookerExplore(
                name=upstream_explore, model_name=looker_view.id.model_name
            ).get_explore_urn(self.source_config)
            upstream_dataset_urns.append(upstream_dataset_urn)

        upstream_dataset_urns.extend(looker_view.sql_table_names)

        # Generate the upstream + fine grained lineage objects.
        upstreams = []
        observed_lineage_ts = datetime.now(tz=timezone.utc)
        fine_grained_lineages: List[FineGrainedLineageClass] = []
        for upstream_dataset_urn in upstream_dataset_urns:
            upstream = UpstreamClass(
                dataset=upstream_dataset_urn,
                type=DatasetLineageTypeClass.VIEW,
                auditStamp=AuditStampClass(
                    time=int(observed_lineage_ts.timestamp() * 1000),
                    actor=CORPUSER_DATAHUB,
                ),
            )
            upstreams.append(upstream)

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
                                for upstream_field in cast(
                                    List[str], field.upstream_fields
                                )
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
            else:
                # View is defined as SQL
                for field in looker_view.fields:
                    if field.upstream_fields:
                        fine_grained_lineage = FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            upstreams=[
                                make_schema_field_urn(cll_ref.table, cll_ref.column)
                                for cll_ref in cast(
                                    List[ColumnRef], field.upstream_fields
                                )
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

        if upstreams:
            return UpstreamLineage(
                upstreams=upstreams, fineGrainedLineages=fine_grained_lineages or None
            )
        else:
            return None

    def _get_custom_properties(self, looker_view: LookerView) -> DatasetPropertiesClass:
        assert self.source_config.base_folder  # this is always filled out
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
            "looker.model": looker_view.id.model_name,
        }
        dataset_props = DatasetPropertiesClass(
            name=looker_view.id.view_name, customProperties=custom_properties
        )

        maybe_git_info = self.source_config.project_dependencies.get(
            looker_view.id.project_name,
            self.remote_projects_git_info.get(looker_view.id.project_name),
        )
        if isinstance(maybe_git_info, GitInfo):
            git_info: Optional[GitInfo] = maybe_git_info
        else:
            git_info = self.source_config.git_info
        if git_info is not None and file_path:
            # It should be that looker_view.id.project_name is the base project.
            github_file_url = git_info.get_url_for_file_path(file_path)
            dataset_props.externalUrl = github_file_url

        return dataset_props

    def _build_dataset_mcps(
        self, looker_view: LookerView
    ) -> List[MetadataChangeProposalWrapper]:

        view_urn = looker_view.id.get_urn(self.source_config)

        subTypeEvent = MetadataChangeProposalWrapper(
            entityUrn=view_urn,
            aspect=SubTypesClass(typeNames=[DatasetSubTypes.VIEW]),
        )
        events = [subTypeEvent]
        if looker_view.view_details is not None:
            viewEvent = MetadataChangeProposalWrapper(
                entityUrn=view_urn,
                aspect=looker_view.view_details,
            )
            events.append(viewEvent)

        project_key = gen_project_key(self.source_config, looker_view.id.project_name)

        container = ContainerClass(container=project_key.as_urn())
        events.append(
            MetadataChangeProposalWrapper(entityUrn=view_urn, aspect=container)
        )

        events.append(
            MetadataChangeProposalWrapper(
                entityUrn=view_urn,
                aspect=looker_view.id.get_browse_path_v2(self.source_config),
            )
        )

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
                f"Could not locate a project name for model {model_name}. Consider configuring a static project name "
                f"in your config file"
            )

    def get_manifest_if_present(self, folder: pathlib.Path) -> Optional[LookerManifest]:
        manifest_file = folder / "manifest.lkml"
        if manifest_file.exists():
            manifest_dict = load_lkml(manifest_file)

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

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        with tempfile.TemporaryDirectory("lookml_tmp") as tmp_dir:
            # Clone the base_folder if necessary.
            if not self.source_config.base_folder:
                assert self.source_config.git_info
                # we don't have a base_folder, so we need to clone the repo and process it locally
                start_time = datetime.now()
                checkout_dir = self.source_config.git_info.clone(
                    tmp_path=tmp_dir,
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
                if isinstance(p_ref, GitInfo):
                    try:
                        p_checkout_dir = p_ref.clone(
                            tmp_path=f"{tmp_dir}/_included_/{project}",
                            # If a deploy key was provided, use it. Otherwise, fall back
                            # to the main project deploy key, if present.
                            fallback_deploy_key=(
                                self.source_config.git_info.deploy_key
                                if self.source_config.git_info
                                else None
                            ),
                        )

                        p_ref = p_checkout_dir.resolve()
                    except Exception as e:
                        logger.warning(
                            f"Failed to clone project dependency {project}. This can lead to failures in parsing lookml files later on: {e}",
                        )
                        visited_projects.add(project)
                        continue

                self.base_projects_folder[project] = p_ref

            self._recursively_check_manifests(
                tmp_dir, _BASE_PROJECT_NAME, visited_projects
            )

            yield from self.get_internal_workunits()

            if not self.report.events_produced and not self.report.failures:
                # Don't pass if we didn't produce any events.
                self.report.report_failure(
                    "<main>",
                    "No metadata was produced. Check the logs for more details.",
                )

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
        if not manifest:
            return

        # Special case handling if the root project has a name in the manifest file.
        if project_name == _BASE_PROJECT_NAME and manifest.project_name:
            if (
                self.source_config.project_name is not None
                and manifest.project_name != self.source_config.project_name
            ):
                logger.warning(
                    f"The project name in the manifest file '{manifest.project_name}'"
                    f"does not match the configured project name '{self.source_config.project_name}'. "
                    "This can lead to failures in LookML include resolution and lineage generation."
                )
            elif self.source_config.project_name is None:
                self.source_config.project_name = manifest.project_name

        # Clone the remote project dependencies.
        for remote_project in manifest.remote_dependencies:
            if remote_project.name in project_visited:
                continue
            if remote_project.name in self.base_projects_folder:
                # In case a remote_dependency is specified in the project_dependencies config,
                # we don't need to clone it again.
                continue

            p_cloner = GitClone(f"{tmp_dir}/_remote_/{remote_project.name}")
            try:
                # TODO: For 100% correctness, we should be consulting
                # the manifest lock file for the exact ref to use.

                p_checkout_dir = p_cloner.clone(
                    ssh_key=(
                        self.source_config.git_info.deploy_key
                        if self.source_config.git_info
                        else None
                    ),
                    repo_url=remote_project.url,
                )

                self.base_projects_folder[
                    remote_project.name
                ] = p_checkout_dir.resolve()
                repo = p_cloner.get_last_repo_cloned()
                assert repo
                remote_git_info = GitInfo(
                    url_template=remote_project.url,
                    repo="dummy/dummy",  # set to dummy values to bypass validation
                    branch=repo.active_branch.name,
                )
                remote_git_info.repo = (
                    ""  # set to empty because url already contains the full path
                )
                self.remote_projects_git_info[remote_project.name] = remote_git_info

            except Exception as e:
                logger.warning(
                    f"Failed to clone remote project {project_name}. This can lead to failures in parsing lookml files later on: {e}",
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
            self.source_config.project_name,
            self.base_projects_folder,
            self.reporter,
            self.source_config.liquid_variable,
        )

        # Some views can be mentioned by multiple 'include' statements and can be included via different connections.

        # This map is used to keep track of which views files have already been processed
        # for a connection in order to prevent creating duplicate events.
        # Key: connection name, Value: view file paths
        processed_view_map: Dict[str, Set[str]] = {}

        # This map is used to keep track of the connection that a view is processed with.
        # Key: view unique identifier - determined by variables present in config `view_naming_pattern`
        # Value: Tuple(model file name, connection name)
        view_connection_map: Dict[str, Tuple[str, str]] = {}

        # The ** means "this directory and all subdirectories", and hence should
        # include all the files we want.
        model_files = sorted(
            self.source_config.base_folder.glob(f"**/*{_MODEL_FILE_EXTENSION}")
        )
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
                    f"Failed to load connection {model.connection}. Check your API key permissions and/or connection_to_platform_map configuration.",
                )
                self.reporter.report_models_dropped(model_name)
                continue

            explore_reachable_views: Set[str] = set()
            looker_refinement_resolver: LookerRefinementResolver = (
                LookerRefinementResolver(
                    looker_model=model,
                    connection_definition=connectionDefinition,
                    looker_viewfile_loader=viewfile_loader,
                    source_config=self.source_config,
                    reporter=self.reporter,
                )
            )

            if self.source_config.emit_reachable_views_only:
                model_explores_map = {d["name"]: d for d in model.explores}
                for explore_dict in model.explores:
                    try:
                        if LookerRefinementResolver.is_refinement(explore_dict["name"]):
                            continue

                        explore_dict = (
                            looker_refinement_resolver.apply_explore_refinement(
                                explore_dict
                            )
                        )
                        explore: LookerExplore = LookerExplore.from_dict(
                            model_name,
                            explore_dict,
                            model.resolved_includes,
                            viewfile_loader,
                            self.reporter,
                            model_explores_map,
                        )
                        if explore.upstream_views:
                            for view_name in explore.upstream_views:
                                explore_reachable_views.add(view_name.include)
                    except Exception as e:
                        self.reporter.report_warning(
                            f"{model}.explores",
                            f"failed to process {explore_dict} due to {e}. Run with --debug for full stacktrace",
                        )
                        logger.debug("Failed to process explore", exc_info=e)

            processed_view_files = processed_view_map.setdefault(
                model.connection, set()
            )

            project_name = self.get_project_name(model_name)

            looker_view_id_cache: LookerViewIdCache = LookerViewIdCache(
                project_name=project_name,
                model_name=model_name,
                looker_model=model,
                looker_viewfile_loader=viewfile_loader,
                reporter=self.reporter,
            )

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
                        raw_view_name = raw_view["name"]
                        if LookerRefinementResolver.is_refinement(raw_view_name):
                            continue

                        if (
                            self.source_config.emit_reachable_views_only
                            and raw_view_name not in explore_reachable_views
                        ):
                            logger.debug(
                                f"view {raw_view_name} is not reachable from an explore, skipping.."
                            )
                            self.reporter.report_unreachable_view_dropped(raw_view_name)
                            continue

                        self.reporter.report_views_scanned()
                        try:
                            raw_view = looker_refinement_resolver.apply_view_refinement(
                                raw_view=raw_view,
                            )

                            current_project_name: str = (
                                include.project
                                if include.project != _BASE_PROJECT_NAME
                                else project_name
                            )

                            # if project is base project then it is available as self.base_projects_folder[
                            # _BASE_PROJECT_NAME]
                            base_folder_path: str = str(
                                self.base_projects_folder.get(
                                    current_project_name,
                                    self.base_projects_folder[_BASE_PROJECT_NAME],
                                )
                            )

                            maybe_looker_view = LookerView.from_looker_dict(
                                project_name=current_project_name,
                                base_folder_path=base_folder_path,
                                model_name=model_name,
                                looker_view=raw_view,
                                connection=connectionDefinition,
                                looker_viewfile=looker_viewfile,
                                looker_viewfile_loader=viewfile_loader,
                                looker_refinement_resolver=looker_refinement_resolver,
                                looker_view_id_cache=looker_view_id_cache,
                                reporter=self.reporter,
                                max_file_snippet_length=self.source_config.max_file_snippet_length,
                                parse_table_names_from_sql=self.source_config.parse_table_names_from_sql,
                                extract_col_level_lineage=self.source_config.extract_column_level_lineage,
                                populate_sql_logic_in_descriptions=self.source_config.populate_sql_logic_for_missing_descriptions,
                                config=self.source_config,
                                ctx=self.ctx,
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
                                view_urn = maybe_looker_view.id.get_urn(
                                    self.source_config
                                )
                                view_connection_mapping = view_connection_map.get(
                                    view_urn
                                )
                                if not view_connection_mapping:
                                    view_connection_map[view_urn] = (
                                        model_name,
                                        model.connection,
                                    )
                                    # first time we are discovering this view
                                    logger.debug(
                                        f"Generating MCP for view {raw_view['name']}"
                                    )

                                    if (
                                        maybe_looker_view.id.project_name
                                        not in self.processed_projects
                                    ):
                                        yield from self.gen_project_workunits(
                                            maybe_looker_view.id.project_name
                                        )

                                        self.processed_projects.append(
                                            maybe_looker_view.id.project_name
                                        )

                                    for mcp in self._build_dataset_mcps(
                                        maybe_looker_view
                                    ):
                                        yield mcp.as_workunit()
                                    mce = self._build_dataset_mce(maybe_looker_view)
                                    yield MetadataWorkUnit(
                                        id=f"lookml-view-{maybe_looker_view.id}",
                                        mce=mce,
                                    )
                                    processed_view_files.add(include.include)
                                else:
                                    (
                                        prev_model_name,
                                        prev_model_connection,
                                    ) = view_connection_mapping
                                    if prev_model_connection != model.connection:
                                        # this view has previously been discovered and emitted using a different
                                        # connection
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
                yield MetadataWorkUnit(
                    id=f"tag-{tag_mce.proposedSnapshot.urn}", mce=tag_mce
                )

    def gen_project_workunits(self, project_name: str) -> Iterable[MetadataWorkUnit]:
        project_key = gen_project_key(
            self.source_config,
            project_name,
        )
        yield from gen_containers(
            container_key=project_key,
            name=project_name,
            sub_types=[BIContainerSubTypes.LOOKML_PROJECT],
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=project_key.as_urn(),
            aspect=BrowsePathsV2Class(
                path=[BrowsePathEntryClass("Folders")],
            ),
        ).as_workunit()

    def get_report(self):
        return self.reporter
