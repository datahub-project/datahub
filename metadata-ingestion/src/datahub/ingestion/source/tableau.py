import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import dateutil.parser as dp
import tableauserverclient as TSC
from pydantic import root_validator, validator
from pydantic.fields import Field
from requests.adapters import ConnectionError
from sqllineage.runner import LineageRunner
from tableauserverclient import (
    PersonalAccessTokenAuth,
    Server,
    ServerResponseError,
    TableauAuth,
)
from tableauserverclient.server.endpoint.exceptions import NonXMLResponseError

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    ConfigurationError,
)
from datahub.configuration.pydantic_field_deprecation import pydantic_field_deprecated
from datahub.configuration.source_common import (
    DatasetLineageProviderConfigBase,
    DatasetSourceConfigMixin,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    PlatformKey,
    add_entity_to_container,
    gen_containers,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source import tableau_constant
from datahub.ingestion.source.common.subtypes import (
    BIContainerSubTypes,
    DatasetSubTypes,
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
from datahub.ingestion.source.tableau_common import (
    FIELD_TYPE_MAPPING,
    MetadataQueryException,
    TableauLineageOverrides,
    clean_query,
    custom_sql_graphql_query,
    dashboard_graphql_query,
    embedded_datasource_graphql_query,
    get_unique_custom_sql,
    make_table_urn,
    published_datasource_graphql_query,
    query_metadata,
    sheet_graphql_query,
    tableau_field_to_schema_field,
    workbook_graphql_query,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    ChangeAuditStamps,
    InputField,
    InputFields,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    ChartSnapshot,
    DashboardSnapshot,
    DatasetSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    NullTypeClass,
    OtherSchema,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    ChangeTypeClass,
    ChartInfoClass,
    ChartUsageStatisticsClass,
    DashboardInfoClass,
    DashboardUsageStatisticsClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    EmbedClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SubTypesClass,
    UpstreamClass,
    ViewPropertiesClass,
)
from datahub.utilities import config_clean
from datahub.utilities.source_helpers import (
    auto_stale_entity_removal,
    auto_status_aspect,
)

logger: logging.Logger = logging.getLogger(__name__)

# Replace / with |
REPLACE_SLASH_CHAR = "|"


class TableauConnectionConfig(ConfigModel):
    connect_uri: str = Field(description="Tableau host URL.")
    username: Optional[str] = Field(
        default=None,
        description="Tableau username, must be set if authenticating using username/password.",
    )
    password: Optional[str] = Field(
        default=None,
        description="Tableau password, must be set if authenticating using username/password.",
    )
    token_name: Optional[str] = Field(
        default=None,
        description="Tableau token name, must be set if authenticating using a personal access token.",
    )
    token_value: Optional[str] = Field(
        default=None,
        description="Tableau token value, must be set if authenticating using a personal access token.",
    )

    site: str = Field(
        default="",
        description="Tableau Site. Always required for Tableau Online. Use emptystring to connect with Default site on Tableau Server.",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="Unique relationship between the Tableau Server and site",
    )

    ssl_verify: Union[bool, str] = Field(
        default=True,
        description="Whether to verify SSL certificates. If using self-signed certificates, set to false or provide the path to the .pem certificate bundle.",
    )

    extract_column_level_lineage: bool = Field(
        True,
        description="When enabled, extracts column-level lineage from Tableau Datasources",
    )

    @validator("connect_uri")
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)

    def make_tableau_client(self) -> Server:
        # https://tableau.github.io/server-client-python/docs/api-ref#authentication
        authentication: Union[TableauAuth, PersonalAccessTokenAuth]
        if self.username and self.password:
            authentication = TableauAuth(
                username=self.username,
                password=self.password,
                site_id=self.site,
            )
        elif self.token_name and self.token_value:
            authentication = PersonalAccessTokenAuth(
                self.token_name, self.token_value, self.site
            )
        else:
            raise ConfigurationError(
                "Tableau Source: Either username/password or token_name/token_value must be set"
            )

        try:
            server = Server(
                self.connect_uri,
                use_server_version=True,
                http_options={
                    # As per https://community.tableau.com/s/question/0D54T00000F33bdSAB/tableauserverclient-signin-with-ssl-certificate
                    "verify": bool(self.ssl_verify),
                    **(
                        {"cert": self.ssl_verify}
                        if isinstance(self.ssl_verify, str)
                        else {}
                    ),
                },
            )

            # From https://stackoverflow.com/a/50159273/5004662.
            server._session.trust_env = False

            server.auth.sign_in(authentication)
            return server
        except ServerResponseError as e:
            if isinstance(authentication, PersonalAccessTokenAuth):
                # Docs on token expiry in Tableau:
                # https://help.tableau.com/current/server/en-us/security_personal_access_tokens.htm#token-expiry
                logger.info(
                    "Error authenticating with Tableau. Note that Tableau personal access tokens "
                    "expire if not used for 15 days or if over 1 year old"
                )
            raise ValueError(
                f"Unable to login (invalid/expired credentials or missing permissions): {str(e)}"
            ) from e
        except Exception as e:
            raise ValueError(
                f"Unable to login (check your Tableau connection and credentials): {str(e)}"
            ) from e


class TableauConfig(
    DatasetLineageProviderConfigBase,
    StatefulIngestionConfigBase,
    DatasetSourceConfigMixin,
    TableauConnectionConfig,
):
    projects: Optional[List[str]] = Field(
        default=["default"],
        description="[deprecated] Use project_pattern instead. List of tableau "
        "projects ",
    )
    _deprecate_projects = pydantic_field_deprecated("projects")
    # Tableau project pattern
    project_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filter for specific Tableau projects. For example, use 'My Project' to ingest a root-level Project with name 'My Project', or 'My Project/Nested Project' to ingest a nested Project with name 'Nested Project'. "
        "By default, all Projects nested inside a matching Project will be included in ingestion. "
        "You can both allow and deny projects based on their name using their name, or a Regex pattern. "
        "Deny patterns always take precedence over allow patterns. "
        "By default, all projects will be ingested.",
    )

    project_path_separator: str = Field(
        default="/",
        description="The separator used for the project_pattern field between project names. By default, we use a slash. "
        "You can change this if your Tableau projects contain slashes in their names, and you'd like to filter by project.",
    )

    default_schema_map: dict = Field(
        default={}, description="Default schema to use when schema is not found."
    )
    ingest_tags: Optional[bool] = Field(
        default=False,
        description="Ingest Tags from source. This will override Tags entered from UI",
    )
    ingest_owner: Optional[bool] = Field(
        default=False,
        description="Ingest Owner from source. This will override Owner info entered from UI",
    )
    ingest_tables_external: bool = Field(
        default=False,
        description="Ingest details for tables external to (not embedded in) tableau as entities.",
    )

    page_size: int = Field(
        default=10,
        description="[advanced] Number of metadata objects (e.g. CustomSQLTable, PublishedDatasource, etc) to query at a time using the Tableau API.",
    )
    # We've found that even with a small workbook page size (e.g. 10), the Tableau API often
    # returns warnings like this:
    # {
    # 	'message': 'Showing partial results. The request exceeded the 20000 node limit. Use pagination, additional filtering, or both in the query to adjust results.',
    # 	'extensions': {
    # 		'severity': 'WARNING',
    # 		'code': 'NODE_LIMIT_EXCEEDED',
    # 		'properties': {
    # 			'nodeLimit': 20000
    # 		}
    # 	}
    # }
    # Reducing the page size for the workbook queries helps to avoid this.
    workbook_page_size: int = Field(
        default=1,
        description="[advanced] Number of workbooks to query at a time using the Tableau API.",
    )

    env: str = Field(
        default=builder.DEFAULT_ENV,
        description="Environment to use in namespace when constructing URNs.",
    )

    lineage_overrides: Optional[TableauLineageOverrides] = Field(
        default=None,
        description="Mappings to change generated dataset urns. Use only if you really know what you are doing.",
    )

    extract_usage_stats: bool = Field(
        default=False,
        description="[experimental] Extract usage statistics for dashboards and charts.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description=""
    )

    ingest_embed_url: Optional[bool] = Field(
        default=True,
        description="Ingest a URL to render an embedded Preview of assets within Tableau.",
    )

    extract_project_hierarchy: bool = Field(
        default=True,
        description="Whether to extract entire project hierarchy for nested projects.",
    )

    extract_lineage_from_unsupported_custom_sql_queries: bool = Field(
        default=False,
        description="[Experimental] Whether to extract lineage from unsupported custom sql queries using SQL parsing",
    )

    # pre = True because we want to take some decision before pydantic initialize the configuration to default values
    @root_validator(pre=True)
    def projects_backward_compatibility(cls, values: Dict) -> Dict:
        projects = values.get("projects")
        project_pattern = values.get("project_pattern")
        if project_pattern is None and projects:
            logger.warning(
                "project_pattern is not set but projects is set. projects is deprecated, please use "
                "project_pattern instead."
            )
            logger.info("Initializing project_pattern from projects")
            values["project_pattern"] = AllowDenyPattern(
                allow=[f"^{prj}$" for prj in projects]
            )
        elif project_pattern != AllowDenyPattern.allow_all() and projects:
            raise ValueError("projects is deprecated. Please use project_pattern only.")

        return values


class WorkbookKey(PlatformKey):
    workbook_id: str


class ProjectKey(PlatformKey):
    project_id: str


@dataclass
class UsageStat:
    view_count: int


@dataclass
class TableauProject:
    id: str
    name: str
    description: str
    parent_id: Optional[str]
    path: List[str]


@platform_name("Tableau")
@config_class(TableauConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Requires transformer", supported=False)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(
    SourceCapability.USAGE_STATS,
    "Dashboard/Chart view counts, enabled using extract_usage_stats config",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default when stateful ingestion is turned on.",
)
@capability(SourceCapability.OWNERSHIP, "Requires recipe configuration")
@capability(SourceCapability.TAGS, "Requires recipe configuration")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
class TableauSource(StatefulIngestionSourceBase):
    config: TableauConfig
    report: StaleEntityRemovalSourceReport
    platform = "tableau"
    server: Optional[Server]
    upstream_tables: Dict[str, Tuple[Any, Optional[str], bool]] = {}
    tableau_stat_registry: Dict[str, UsageStat] = {}
    tableau_project_registry: Dict[str, TableauProject] = {}
    workbook_project_map: Dict[str, str] = {}
    datasource_project_map: Dict[str, str] = {}

    def __hash__(self):
        return id(self)

    def __init__(
        self,
        config: TableauConfig,
        ctx: PipelineContext,
    ):
        super().__init__(config, ctx)

        self.config = config
        self.report = StaleEntityRemovalSourceReport()
        self.server = None
        self.upstream_tables = {}
        self.tableau_stat_registry = {}
        self.tableau_project_registry = {}
        self.workbook_project_map = {}
        self.datasource_project_map = {}

        # This list keeps track of sheets in workbooks so that we retrieve those
        # when emitting sheets.
        self.sheet_ids: List[str] = []
        # This list keeps track of dashboards in workbooks so that we retrieve those
        # when emitting dashboards.
        self.dashboard_ids: List[str] = []
        # This list keeps track of embedded datasources in workbooks so that we retrieve those
        # when emitting embedded data sources.
        self.embedded_datasource_ids_being_used: List[str] = []
        # This list keeps track of datasource being actively used by workbooks so that we only retrieve those
        # when emitting published data sources.
        self.datasource_ids_being_used: List[str] = []
        # This list keeps track of datasource being actively used by workbooks so that we only retrieve those
        # when emitting custom SQL data sources.
        self.custom_sql_ids_being_used: List[str] = []

        # Create and register the stateful ingestion use-case handlers.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            source=self,
            config=self.config,
            state_type_class=GenericCheckpointState,
            pipeline_name=self.ctx.pipeline_name,
            run_id=self.ctx.run_id,
        )

        self._authenticate()

    def close(self) -> None:
        try:
            if self.server is not None:
                self.server.auth.sign_out()
        except ConnectionError as err:
            logger.warning(
                "During graceful closing of Tableau source a sign-out call was tried but ended up with"
                " a ConnectionError (%s). Continuing closing of the source",
                err,
            )
            self.server = None
        super().close()

    def _populate_usage_stat_registry(self):
        if self.server is None:
            return

        view: TSC.ViewItem
        for view in TSC.Pager(self.server.views, usage=True):
            if not view.id:
                continue
            self.tableau_stat_registry[view.id] = UsageStat(view_count=view.total_views)
        logger.debug("Tableau stats %s", self.tableau_stat_registry)

    def _get_all_project(self) -> Dict[str, TableauProject]:
        all_project_map: Dict[str, TableauProject] = {}

        def fetch_projects():
            if self.server is None:
                return all_project_map

            for project in TSC.Pager(self.server.projects):
                all_project_map[project.id] = TableauProject(
                    id=project.id,
                    name=project.name,
                    parent_id=project.parent_id,
                    description=project.description,
                    path=[],
                )

        def set_project_path():
            def form_path(project_id: str) -> List[str]:
                cur_proj = all_project_map[project_id]
                ancestors = [cur_proj.name]
                while cur_proj.parent_id is not None:
                    cur_proj = all_project_map[cur_proj.parent_id]
                    ancestors = [cur_proj.name, *ancestors]
                return ancestors

            for project in all_project_map.values():
                project.path = form_path(project_id=project.id)
                logger.debug(f"Project {project.name}({project.id})")
                logger.debug(f"Project path = {project.path}")

        fetch_projects()
        set_project_path()

        return all_project_map

    def _is_allowed_project(self, project: TableauProject) -> bool:
        # Either project name or project path should exist in allow
        is_allowed: bool = self.config.project_pattern.allowed(
            project.name
        ) or self.config.project_pattern.allowed(self._get_project_path(project))
        if is_allowed is False:
            logger.info(
                f"project({project.name}) is not allowed as per project_pattern"
            )
        return is_allowed

    def _is_denied_project(self, project: TableauProject) -> bool:
        # Either project name or project path should exist in deny
        for deny_pattern in self.config.project_pattern.deny:
            # Either name or project path is denied
            if re.match(
                deny_pattern, project.name, self.config.project_pattern.regex_flags
            ) or re.match(
                deny_pattern,
                self._get_project_path(project),
                self.config.project_pattern.regex_flags,
            ):
                return True
        logger.info(f"project({project.name}) is not denied as per project_pattern")
        return False

    def _init_tableau_project_registry(self, all_project_map: dict) -> None:
        list_of_skip_projects: List[TableauProject] = []

        for project in all_project_map.values():
            # Skip project if it is not allowed
            logger.debug(f"Evaluating project pattern for {project.name}")
            if self._is_allowed_project(project) is False:
                list_of_skip_projects.append(project)
                logger.debug(f"Project {project.name} is skipped")
                continue
            logger.debug(f"Project {project.name} is added in project registry")
            self.tableau_project_registry[project.id] = project

        if self.config.extract_project_hierarchy is False:
            logger.debug(
                "Skipping project hierarchy processing as configuration extract_project_hierarchy is "
                "disabled"
            )
            return

        logger.debug("Reevaluating projects as extract_project_hierarchy is enabled")

        for project in list_of_skip_projects:
            if (
                project.parent_id in self.tableau_project_registry
                and self._is_denied_project(project) is False
            ):
                logger.debug(f"Project {project.name} is added in project registry")
                self.tableau_project_registry[project.id] = project

    def _init_datasource_registry(self) -> None:
        if self.server is None:
            return

        for ds in TSC.Pager(self.server.datasources):
            if ds.project_id not in self.tableau_project_registry:
                logger.debug(
                    f"project id ({ds.project_id}) of datasource {ds.name} is not present in project "
                    f"registry"
                )
                continue
            self.datasource_project_map[ds.id] = ds.project_id

    def _init_workbook_registry(self) -> None:
        if self.server is None:
            return

        for wb in TSC.Pager(self.server.workbooks):
            if wb.project_id not in self.tableau_project_registry:
                logger.debug(
                    f"project id ({wb.project_id}) of workbook {wb.name} is not present in project "
                    f"registry"
                )
                continue
            self.workbook_project_map[wb.id] = wb.project_id

    def _populate_projects_registry(self):
        if self.server is None:
            return

        logger.info("Initializing site project registry")

        all_project_map: Dict[str, TableauProject] = self._get_all_project()

        self._init_tableau_project_registry(all_project_map)
        self._init_datasource_registry()
        self._init_workbook_registry()

        logger.debug(f"All site projects {all_project_map}")
        logger.debug(f"Projects selected for ingestion {self.tableau_project_registry}")
        logger.debug(
            f"Tableau data-sources {self.datasource_project_map}",
        )
        logger.debug(
            f"Tableau workbooks {self.workbook_project_map}",
        )

    def _authenticate(self):
        try:
            self.server = self.config.make_tableau_client()
            logger.info("Authenticated to Tableau server")
        # Note that we're not catching ConfigurationError, since we want that to throw.
        except ValueError as e:
            self.report.report_failure(
                key="tableau-login",
                reason=str(e),
            )

    def get_data_platform_instance(self) -> DataPlatformInstanceClass:
        return DataPlatformInstanceClass(
            platform=builder.make_data_platform_urn(self.platform),
            instance=builder.make_dataplatform_instance_urn(
                self.platform, self.config.platform_instance
            )
            if self.config.platform_instance
            else None,
        )

    def get_connection_object_page(
        self,
        query: str,
        connection_type: str,
        query_filter: str,
        count: int = 0,
        offset: int = 0,
        retry_on_auth_error: bool = True,
    ) -> Tuple[dict, int, int]:
        logger.debug(
            f"Query {connection_type} to get {count} objects with offset {offset}"
        )
        try:
            query_data = query_metadata(
                self.server, query, connection_type, count, offset, query_filter
            )
        except NonXMLResponseError:
            if not retry_on_auth_error:
                raise

            # If ingestion has been running for over 2 hours, the Tableau
            # temporary credentials will expire. If this happens, this exception
            # will be thrown and we need to re-authenticate and retry.
            self._authenticate()
            return self.get_connection_object_page(
                query, connection_type, query_filter, count, offset, False
            )

        if tableau_constant.ERRORS in query_data:
            errors = query_data[tableau_constant.ERRORS]
            if all(
                # The format of the error messages is highly unpredictable, so we have to
                # be extra defensive with our parsing.
                error
                and (error.get(tableau_constant.EXTENSIONS) or {}).get(
                    tableau_constant.SEVERITY
                )
                == tableau_constant.WARNING
                for error in errors
            ):
                self.report.report_warning(key=connection_type, reason=f"{errors}")
            else:
                raise RuntimeError(f"Query {connection_type} error: {errors}")

        connection_object = (
            query_data.get(tableau_constant.DATA).get(connection_type, {})
            if query_data.get(tableau_constant.DATA)
            else {}
        )

        total_count = connection_object.get(tableau_constant.TOTAL_COUNT, 0)
        has_next_page = connection_object.get(tableau_constant.PAGE_INFO, {}).get(
            tableau_constant.HAS_NEXT_PAGE, False
        )
        return connection_object, total_count, has_next_page

    def get_connection_objects(
        self,
        query: str,
        connection_type: str,
        query_filter: str,
        page_size_override: Optional[int] = None,
    ) -> Iterable[dict]:
        # Calls the get_connection_object_page function to get the objects,
        # and automatically handles pagination.

        page_size = page_size_override or self.config.page_size

        total_count = page_size
        has_next_page = 1
        offset = 0
        while has_next_page:
            count = (
                page_size if offset + page_size < total_count else total_count - offset
            )
            (
                connection_objects,
                total_count,
                has_next_page,
            ) = self.get_connection_object_page(
                query,
                connection_type,
                query_filter,
                count,
                offset,
            )

            offset += count

            for obj in connection_objects.get(tableau_constant.NODES, []):
                yield obj

    def emit_workbooks(self) -> Iterable[MetadataWorkUnit]:
        if self.tableau_project_registry:
            project_names: List[str] = [
                project.name for project in self.tableau_project_registry.values()
            ]
            project_names_str: str = json.dumps(project_names)
            projects = f"{tableau_constant.PROJECT_NAME_WITH_IN}: {project_names_str}"

            for workbook in self.get_connection_objects(
                workbook_graphql_query,
                tableau_constant.WORKBOOKS_CONNECTION,
                projects,
                page_size_override=self.config.workbook_page_size,
            ):
                # This check is needed as we are using projectNameWithin which return project as per project name so if
                # user want to ingest only nested project C from A->B->C then tableau might return more than one Project
                # if multiple project has name C. Ideal solution is to use projectLuidWithin to avoid duplicate project,
                # however Tableau supports projectLuidWithin in Tableau Cloud June 2022 / Server 2022.3 and later.
                project_luid: Optional[str] = self._get_workbook_project_luid(workbook)
                if project_luid not in self.tableau_project_registry.keys():
                    wrk_name: Optional[str] = workbook.get(tableau_constant.NAME)
                    wrk_id: Optional[str] = workbook.get(tableau_constant.ID)
                    prj_name: Optional[str] = workbook.get(
                        tableau_constant.PROJECT_NAME
                    )

                    logger.debug(
                        f"Skipping workbook {wrk_name}({wrk_id}) as it is project {prj_name}({project_luid}) not "
                        f"present in project registry"
                    )
                    continue

                yield from self.emit_workbook_as_container(workbook)

                for sheet in workbook.get(tableau_constant.SHEETS, []):
                    self.sheet_ids.append(sheet[tableau_constant.ID])

                for dashboard in workbook.get(tableau_constant.DASHBOARDS, []):
                    self.dashboard_ids.append(dashboard[tableau_constant.ID])

                for ds in workbook.get(tableau_constant.EMBEDDED_DATA_SOURCES, []):
                    self.embedded_datasource_ids_being_used.append(
                        ds[tableau_constant.ID]
                    )

    def _track_custom_sql_ids(self, field: dict) -> None:
        # Tableau shows custom sql datasource as a table in ColumnField's upstreamColumns.
        for column in field.get(tableau_constant.UPSTREAM_COLUMNS, []):
            table_id = (
                column.get(tableau_constant.TABLE, {}).get(tableau_constant.ID)
                if column.get(tableau_constant.TABLE)
                and column[tableau_constant.TABLE][tableau_constant.TYPE_NAME]
                == tableau_constant.CUSTOM_SQL_TABLE
                else None
            )

            if table_id is not None and table_id not in self.custom_sql_ids_being_used:
                self.custom_sql_ids_being_used.append(table_id)

    def _create_upstream_table_lineage(
        self,
        datasource: dict,
        browse_path: str,
        is_embedded_ds: bool = False,
    ) -> Tuple:
        upstream_tables: List[Upstream] = []
        fine_grained_lineages: List[FineGrainedLineage] = []
        table_id_to_urn = {}

        upstream_datasources = self.get_upstream_datasources(
            datasource, upstream_tables
        )
        upstream_tables.extend(upstream_datasources)

        # When tableau workbook connects to published datasource, it creates an embedded
        # datasource inside workbook that connects to published datasource. Both embedded
        # and published datasource have same upstreamTables in this case.
        if upstream_tables and is_embedded_ds:
            logger.debug(
                f"Embedded datasource {datasource.get(tableau_constant.ID)} has upstreamDatasources.\
                Setting only upstreamDatasources lineage. The upstreamTables lineage \
                    will be set via upstream published datasource."
            )
        else:
            # This adds an edge to upstream DatabaseTables using `upstreamTables`
            upstreams, id_to_urn = self.get_upstream_tables(
                datasource.get(tableau_constant.UPSTREAM_TABLES, []),
                datasource.get(tableau_constant.NAME),
                browse_path,
                is_custom_sql=False,
            )
            upstream_tables.extend(upstreams)
            table_id_to_urn.update(id_to_urn)

            # This adds an edge to upstream CustomSQLTables using `fields`.`upstreamColumns`.`table`
            csql_upstreams, csql_id_to_urn = self.get_upstream_csql_tables(
                datasource.get(tableau_constant.FIELDS),
            )
            upstream_tables.extend(csql_upstreams)
            table_id_to_urn.update(csql_id_to_urn)

        logger.debug(
            f"A total of {len(upstream_tables)} upstream table edges found for datasource {datasource[tableau_constant.ID]}"
        )

        if datasource.get(tableau_constant.FIELDS):
            datasource_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=datasource[tableau_constant.ID],
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            if self.config.extract_column_level_lineage:
                # Find fine grained lineage for datasource column to datasource column edge,
                # upstream columns may be from same datasource
                upstream_fields = self.get_upstream_fields_of_field_in_datasource(
                    datasource, datasource_urn
                )
                fine_grained_lineages.extend(upstream_fields)

                # Find fine grained lineage for table column to datasource column edge,
                upstream_columns = self.get_upstream_columns_of_fields_in_datasource(
                    datasource,
                    datasource_urn,
                    table_id_to_urn,
                )
                fine_grained_lineages.extend(upstream_columns)

                logger.debug(
                    f"A total of {len(fine_grained_lineages)} upstream column edges found for datasource {datasource[tableau_constant.ID]}"
                )

        return upstream_tables, fine_grained_lineages

    def get_upstream_datasources(self, datasource, upstream_tables):
        upstream_tables = []
        for ds in datasource.get(tableau_constant.UPSTREAM_DATA_SOURCES, []):
            if ds[tableau_constant.ID] not in self.datasource_ids_being_used:
                self.datasource_ids_being_used.append(ds[tableau_constant.ID])

            upstream_ds_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=ds[tableau_constant.ID],
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )
            upstream_table = Upstream(
                dataset=upstream_ds_urn,
                type=DatasetLineageType.TRANSFORMED,
            )
            upstream_tables.append(upstream_table)
        return upstream_tables

    def get_upstream_csql_tables(self, fields):
        upstream_csql_urns = set()
        csql_id_to_urn = {}

        for field in fields:
            if not field.get(tableau_constant.UPSTREAM_COLUMNS):
                continue
            for upstream_col in field.get(tableau_constant.UPSTREAM_COLUMNS):
                if (
                    upstream_col
                    and upstream_col.get(tableau_constant.TABLE)
                    and upstream_col.get(tableau_constant.TABLE)[
                        tableau_constant.TYPE_NAME
                    ]
                    == tableau_constant.CUSTOM_SQL_TABLE
                ):
                    upstream_table_id = upstream_col.get(tableau_constant.TABLE)[
                        tableau_constant.ID
                    ]

                    csql_urn = builder.make_dataset_urn_with_platform_instance(
                        platform=self.platform,
                        name=upstream_table_id,
                        platform_instance=self.config.platform_instance,
                        env=self.config.env,
                    )
                    csql_id_to_urn[upstream_table_id] = csql_urn

                    upstream_csql_urns.add(csql_urn)

        return [
            Upstream(dataset=csql_urn, type=DatasetLineageType.TRANSFORMED)
            for csql_urn in upstream_csql_urns
        ], csql_id_to_urn

    def get_upstream_tables(self, tables, datasource_name, browse_path, is_custom_sql):
        upstream_tables = []
        # Same table urn can be used when setting fine grained lineage,
        table_id_to_urn: Dict[str, str] = {}
        for table in tables:
            # skip upstream tables when there is no column info when retrieving datasource
            # Lineage and Schema details for these will be taken care in self.emit_custom_sql_datasources()
            if not is_custom_sql and not table.get(tableau_constant.COLUMNS):
                logger.debug(
                    f"Skipping upstream table with id {table[tableau_constant.ID]}, no columns: {table}"
                )
                continue
            elif table[tableau_constant.NAME] is None:
                logger.warning(
                    f"Skipping upstream table {table[tableau_constant.ID]} from lineage since its name is none: {table}"
                )
                continue

            schema = table.get(tableau_constant.SCHEMA, "")
            table_name = table.get(tableau_constant.NAME, "")
            full_name = table.get(tableau_constant.FULL_NAME, "")
            upstream_db = (
                table.get(tableau_constant.DATABASE, {}).get(tableau_constant.NAME, "")
                if table.get(tableau_constant.DATABASE) is not None
                else ""
            )
            logger.debug(
                "Processing Table with Connection Type: {0} and id {1}".format(
                    table.get(tableau_constant.CONNECTION_TYPE, ""),
                    table.get(tableau_constant.ID, ""),
                )
            )
            schema = self._get_schema(schema, upstream_db, full_name)
            # if the schema is included within the table name we omit it
            if (
                schema
                and table_name
                and full_name
                and table_name == full_name
                and schema in table_name
            ):
                logger.debug(
                    f"Omitting schema for upstream table {table[tableau_constant.ID]}, schema included in table name"
                )
                schema = ""

            table_urn = make_table_urn(
                self.config.env,
                upstream_db,
                table.get(tableau_constant.CONNECTION_TYPE, ""),
                schema,
                table_name,
                self.config.platform_instance_map,
                self.config.lineage_overrides,
            )
            table_id_to_urn[table[tableau_constant.ID]] = table_urn

            upstream_table = Upstream(
                dataset=table_urn,
                type=DatasetLineageType.TRANSFORMED,
            )
            upstream_tables.append(upstream_table)

            table_path = None
            if browse_path and datasource_name:
                table_path = f"{browse_path}/{datasource_name}"

            self.upstream_tables[table_urn] = (
                table.get(tableau_constant.COLUMNS, []),
                table_path,
                table.get(tableau_constant.IS_EMBEDDED) or False,
            )
        return upstream_tables, table_id_to_urn

    def get_upstream_columns_of_fields_in_datasource(
        self,
        datasource,
        datasource_urn,
        table_id_to_urn,
    ):
        fine_grained_lineages = []
        for field in datasource.get(tableau_constant.FIELDS):
            field_name = field.get(tableau_constant.NAME)
            # upstreamColumns lineage will be set via upstreamFields.
            # such as for CalculatedField
            if (
                not field_name
                or not field.get(tableau_constant.UPSTREAM_COLUMNS)
                or field.get(tableau_constant.UPSTREAM_FIELDS)
            ):
                continue
            input_columns = []
            for upstream_col in field.get(tableau_constant.UPSTREAM_COLUMNS):
                if not upstream_col:
                    continue
                name = upstream_col.get(tableau_constant.NAME)
                upstream_table_id = (
                    upstream_col.get(tableau_constant.TABLE)[tableau_constant.ID]
                    if upstream_col.get(tableau_constant.TABLE)
                    else None
                )
                if (
                    name
                    and upstream_table_id
                    and upstream_table_id in table_id_to_urn.keys()
                ):
                    input_columns.append(
                        builder.make_schema_field_urn(
                            parent_urn=table_id_to_urn[upstream_table_id],
                            field_path=name,
                        )
                    )

            if input_columns:
                fine_grained_lineages.append(
                    FineGrainedLineage(
                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                        downstreams=sorted(
                            [builder.make_schema_field_urn(datasource_urn, field_name)]
                        ),
                        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                        upstreams=sorted(input_columns),
                    )
                )

        return fine_grained_lineages

    def get_upstream_fields_of_field_in_datasource(self, datasource, datasource_urn):
        fine_grained_lineages = []
        for field in datasource.get(tableau_constant.FIELDS):
            field_name = field.get(tableau_constant.NAME)
            # It is observed that upstreamFields gives one-hop field
            # lineage, and not multi-hop field lineage
            # This behavior is as desired in our case.
            if not field_name or not field.get(tableau_constant.UPSTREAM_FIELDS):
                continue
            input_fields = []
            for upstream_field in field.get(tableau_constant.UPSTREAM_FIELDS):
                if not upstream_field:
                    continue
                name = upstream_field.get(tableau_constant.NAME)
                upstream_ds_id = (
                    upstream_field.get(tableau_constant.DATA_SOURCE)[
                        tableau_constant.ID
                    ]
                    if upstream_field.get(tableau_constant.DATA_SOURCE)
                    else None
                )
                if name and upstream_ds_id:
                    input_fields.append(
                        builder.make_schema_field_urn(
                            parent_urn=builder.make_dataset_urn_with_platform_instance(
                                self.platform,
                                upstream_ds_id,
                                self.config.platform_instance,
                                self.config.env,
                            ),
                            field_path=name,
                        )
                    )
            if input_fields:
                fine_grained_lineages.append(
                    FineGrainedLineage(
                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                        downstreams=[
                            builder.make_schema_field_urn(datasource_urn, field_name)
                        ],
                        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                        upstreams=input_fields,
                        transformOperation=self.get_transform_operation(field),
                    )
                )
        return fine_grained_lineages

    def get_transform_operation(self, field):
        field_type = field[tableau_constant.TYPE_NAME]
        if field_type in (
            tableau_constant.DATA_SOURCE_FIELD,
            tableau_constant.COLUMN_FIELD,
        ):
            op = tableau_constant.IDENTITY  # How to specify exact same
        elif field_type == tableau_constant.CALCULATED_FIELD:
            op = field_type
            if field.get(tableau_constant.FORMULA):
                op += f"formula: {field.get(tableau_constant.FORMULA)}"
        else:
            op = field_type  # BinField, CombinedField, etc

        return op

    def emit_custom_sql_datasources(self) -> Iterable[MetadataWorkUnit]:
        custom_sql_filter = f"{tableau_constant.ID_WITH_IN}: {json.dumps(self.custom_sql_ids_being_used)}"

        custom_sql_connection = list(
            self.get_connection_objects(
                custom_sql_graphql_query,
                tableau_constant.CUSTOM_SQL_TABLE_CONNECTION,
                custom_sql_filter,
            )
        )
        unique_custom_sql = get_unique_custom_sql(custom_sql_connection)

        for csql in unique_custom_sql:
            csql_id: str = csql[tableau_constant.ID]
            csql_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=csql_id,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )
            dataset_snapshot = DatasetSnapshot(
                urn=csql_urn,
                aspects=[self.get_data_platform_instance()],
            )
            logger.debug(f"Processing custom sql = {csql}")

            datasource_name = None
            project = None
            if len(csql[tableau_constant.DATA_SOURCES]) > 0:
                # CustomSQLTable id owned by exactly one tableau data source
                logger.debug(
                    f"Number of datasources referencing CustomSQLTable: {len(csql[tableau_constant.DATA_SOURCES])}"
                )

                datasource = csql[tableau_constant.DATA_SOURCES][0]
                datasource_name = datasource.get(tableau_constant.NAME)
                if datasource.get(
                    tableau_constant.TYPE_NAME
                ) == tableau_constant.EMBEDDED_DATA_SOURCE and datasource.get(
                    tableau_constant.WORKBOOK
                ):
                    datasource_name = (
                        f"{datasource.get(tableau_constant.WORKBOOK).get(tableau_constant.NAME)}/{datasource_name}"
                        if datasource_name
                        and datasource.get(tableau_constant.WORKBOOK).get(
                            tableau_constant.NAME
                        )
                        else None
                    )
                    logger.debug(
                        f"Adding datasource {datasource_name}({datasource.get('id')}) to container"
                    )
                    workunits = add_entity_to_container(
                        self.gen_workbook_key(datasource[tableau_constant.WORKBOOK]),
                        tableau_constant.DATASET,
                        dataset_snapshot.urn,
                    )
                    for wu in workunits:
                        self.report.report_workunit(wu)
                        yield wu
                project = self._get_project_browse_path_name(datasource)

                tables = csql.get(tableau_constant.TABLES, [])

                if tables:
                    # lineage from custom sql -> datasets/tables #
                    yield from self._create_lineage_to_upstream_tables(
                        csql_urn, tables, datasource
                    )
                elif self.config.extract_lineage_from_unsupported_custom_sql_queries:
                    # custom sql tables may contain unsupported sql, causing incomplete lineage
                    # we extract the lineage from the raw queries
                    yield from self._create_lineage_from_unsupported_csql(
                        csql_urn, csql
                    )

            #  Schema Metadata
            columns = csql.get(tableau_constant.COLUMNS, [])
            schema_metadata = self.get_schema_metadata_for_custom_sql(columns)
            if schema_metadata is not None:
                dataset_snapshot.aspects.append(schema_metadata)

            # Browse path

            if project and datasource_name:
                browse_paths = BrowsePathsClass(
                    paths=[
                        f"/{self.config.env.lower()}/{self.platform}/{project}/{datasource[tableau_constant.NAME]}"
                    ]
                )
                dataset_snapshot.aspects.append(browse_paths)
            else:
                logger.debug(f"Browse path not set for Custom SQL table {csql_id}")

            dataset_properties = DatasetPropertiesClass(
                name=csql.get(tableau_constant.NAME),
                description=csql.get(tableau_constant.DESCRIPTION),
            )

            dataset_snapshot.aspects.append(dataset_properties)

            view_properties = ViewPropertiesClass(
                materialized=False,
                viewLanguage=tableau_constant.SQL,
                viewLogic=clean_query(csql.get(tableau_constant.QUERY) or ""),
            )
            dataset_snapshot.aspects.append(view_properties)

            yield self.get_metadata_change_event(dataset_snapshot)
            yield self.get_metadata_change_proposal(
                dataset_snapshot.urn,
                aspect_name=tableau_constant.SUB_TYPES,
                aspect=SubTypesClass(
                    typeNames=[DatasetSubTypes.VIEW, tableau_constant.CUSTOM_SQL]
                ),
            )

    def get_schema_metadata_for_custom_sql(
        self, columns: List[dict]
    ) -> Optional[SchemaMetadata]:
        fields = []
        schema_metadata = None
        for field in columns:
            # Datasource fields

            if field.get(tableau_constant.NAME) is None:
                logger.warning(
                    f"Skipping field {field[tableau_constant.ID]} from schema since its name is none"
                )
                continue
            nativeDataType = field.get(
                tableau_constant.REMOTE_TYPE, tableau_constant.UNKNOWN
            )
            TypeClass = FIELD_TYPE_MAPPING.get(nativeDataType, NullTypeClass)
            schema_field = SchemaField(
                fieldPath=field[tableau_constant.NAME],
                type=SchemaFieldDataType(type=TypeClass()),
                nativeDataType=nativeDataType,
                description=field.get(tableau_constant.DESCRIPTION, ""),
            )
            fields.append(schema_field)

        schema_metadata = SchemaMetadata(
            schemaName="test",
            platform=f"urn:li:dataPlatform:{self.platform}",
            version=0,
            fields=fields,
            hash="",
            platformSchema=OtherSchema(rawSchema=""),
        )
        return schema_metadata

    def _get_published_datasource_project_luid(self, ds):
        if (
            ds.get(tableau_constant.LUID)
            and ds[tableau_constant.LUID] in self.datasource_project_map.keys()
            and self.datasource_project_map[ds[tableau_constant.LUID]]
            in self.tableau_project_registry
        ):
            return self.datasource_project_map[ds[tableau_constant.LUID]]

        logger.debug(
            f"published datasource {ds.get(tableau_constant.NAME)} project_luid not found"
        )

        return None

    def _get_workbook_project_luid(self, wb):
        if wb.get(tableau_constant.LUID) and self.workbook_project_map.get(
            wb[tableau_constant.LUID]
        ):
            return self.workbook_project_map[wb[tableau_constant.LUID]]

        logger.debug(f"workbook {wb.get(tableau_constant.NAME)} project_luid not found")

        return None

    def _get_embedded_datasource_project_luid(self, ds):
        if ds.get(tableau_constant.WORKBOOK):
            project_luid: Optional[str] = self._get_workbook_project_luid(
                ds.get(tableau_constant.WORKBOOK)
            )
            if project_luid and project_luid in self.tableau_project_registry:
                return project_luid

        logger.debug(
            f"embedded datasource {ds.get(tableau_constant.NAME)} project_luid not found"
        )

        return None

    def _get_datasource_project_luid(self, ds):
        # Only published and embedded data-sources are supported
        ds_type: Optional[str] = ds.get(tableau_constant.TYPE_NAME)
        if ds_type not in (
            tableau_constant.PUBLISHED_DATA_SOURCE,
            tableau_constant.EMBEDDED_DATA_SOURCE,
        ):
            logger.debug(
                f"datasource {ds.get(tableau_constant.NAME)} type {ds.get(tableau_constant.TYPE_NAME)} is "
                f"unsupported"
            )
            return None

        func_selector: Any = {
            tableau_constant.PUBLISHED_DATA_SOURCE: self._get_published_datasource_project_luid,
            tableau_constant.EMBEDDED_DATA_SOURCE: self._get_embedded_datasource_project_luid,
        }

        return func_selector[ds_type](ds)

    @staticmethod
    def _get_datasource_project_name(ds: dict) -> Optional[str]:
        if ds.get(
            tableau_constant.TYPE_NAME
        ) == tableau_constant.EMBEDDED_DATA_SOURCE and ds.get(
            tableau_constant.WORKBOOK
        ):
            return ds[tableau_constant.WORKBOOK].get(tableau_constant.PROJECT_NAME)
        if ds.get(tableau_constant.TYPE_NAME) == tableau_constant.PUBLISHED_DATA_SOURCE:
            return ds.get(tableau_constant.PROJECT_NAME)
        return None

    def _get_project_browse_path_name(self, ds):
        if self.config.extract_project_hierarchy is False:
            # backward compatibility. Just return the name of datasource project
            return self._get_datasource_project_name(ds)

        # form path as per nested project structure
        project_luid = self._get_datasource_project_luid(ds)
        if project_luid is None:
            datasource_name: str = ds.get(tableau_constant.NAME)
            logger.warning(
                f"Could not load project hierarchy for datasource {datasource_name}. Please check permissions."
            )
            logger.debug(f"datasource = {ds}")
            return None

        return self._project_luid_to_browse_path_name(project_luid=project_luid)

    def _create_lineage_to_upstream_tables(
        self, csql_urn: str, tables: List[dict], datasource: dict
    ) -> Iterable[MetadataWorkUnit]:
        # This adds an edge to upstream DatabaseTables using `upstreamTables`
        upstream_tables, _ = self.get_upstream_tables(
            tables,
            datasource.get(tableau_constant.NAME),
            self._get_project_browse_path_name(datasource),
            is_custom_sql=True,
        )

        if upstream_tables:
            upstream_lineage = UpstreamLineage(upstreams=upstream_tables)
            yield self.get_metadata_change_proposal(
                csql_urn,
                aspect_name=tableau_constant.UPSTREAM_LINEAGE,
                aspect=upstream_lineage,
            )

    def _create_lineage_from_unsupported_csql(
        self, csql_urn: str, csql: dict
    ) -> Iterable[MetadataWorkUnit]:
        database = csql.get(tableau_constant.DATABASE) or {}
        if (
            csql.get(tableau_constant.IS_UNSUPPORTED_CUSTOM_SQL, False)
            and tableau_constant.NAME in database
            and tableau_constant.CONNECTION_TYPE in database
        ):
            upstream_tables = []
            parser = LineageRunner(csql.get(tableau_constant.QUERY))

            for table in parser.source_tables:
                split_table = str(table).split(".")
                if len(split_table) == 2:
                    datset = make_table_urn(
                        env=self.config.env,
                        upstream_db=database.get(tableau_constant.NAME),
                        connection_type=database.get(
                            tableau_constant.CONNECTION_TYPE, ""
                        ),
                        schema=split_table[0],
                        full_name=split_table[1],
                        platform_instance_map=self.config.platform_instance_map,
                        lineage_overrides=self.config.lineage_overrides,
                    )
                    upstream_tables.append(
                        UpstreamClass(
                            type=DatasetLineageType.TRANSFORMED, dataset=datset
                        )
                    )
            upstream_lineage = UpstreamLineage(upstreams=upstream_tables)
            yield self.get_metadata_change_proposal(
                csql_urn,
                aspect_name=tableau_constant.UPSTREAM_LINEAGE,
                aspect=upstream_lineage,
            )

    def _get_schema_metadata_for_datasource(
        self, datasource_fields: List[dict]
    ) -> Optional[SchemaMetadata]:
        fields = []
        for field in datasource_fields:
            # check datasource - custom sql relations from a field being referenced
            self._track_custom_sql_ids(field)
            if field.get(tableau_constant.NAME) is None:
                logger.warning(
                    f"Skipping field {field[tableau_constant.ID]} from schema since its name is none"
                )
                continue

            schema_field = tableau_field_to_schema_field(field, self.config.ingest_tags)
            fields.append(schema_field)

        return (
            SchemaMetadata(
                schemaName="test",
                platform=f"urn:li:dataPlatform:{self.platform}",
                version=0,
                fields=fields,
                hash="",
                platformSchema=OtherSchema(rawSchema=""),
            )
            if fields
            else None
        )

    def get_metadata_change_event(
        self, snap_shot: Union["DatasetSnapshot", "DashboardSnapshot", "ChartSnapshot"]
    ) -> MetadataWorkUnit:
        mce = MetadataChangeEvent(proposedSnapshot=snap_shot)
        work_unit = MetadataWorkUnit(id=snap_shot.urn, mce=mce)
        self.report.report_workunit(work_unit)

        return work_unit

    def get_metadata_change_proposal(
        self,
        urn: str,
        aspect_name: str,
        aspect: Union["UpstreamLineage", "SubTypesClass"],
    ) -> MetadataWorkUnit:
        mcp = MetadataChangeProposalWrapper(
            entityType=tableau_constant.DATASET,
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=urn,
            aspectName=aspect_name,
            aspect=aspect,
        )
        mcp_workunit = MetadataWorkUnit(
            id=f"tableau-{mcp.entityUrn}-{mcp.aspectName}",
            mcp=mcp,
            treat_errors_as_warnings=True,
        )
        self.report.report_workunit(mcp_workunit)
        return mcp_workunit

    def emit_datasource(
        self,
        datasource: dict,
        workbook: Optional[dict] = None,
        is_embedded_ds: bool = False,
    ) -> Iterable[MetadataWorkUnit]:
        datasource_info = workbook
        if not is_embedded_ds:
            datasource_info = datasource

        browse_path = self._get_project_browse_path_name(datasource)
        logger.debug(
            f"datasource {datasource.get(tableau_constant.NAME)} browse-path {browse_path}"
        )
        datasource_id = datasource[tableau_constant.ID]
        datasource_urn = builder.make_dataset_urn_with_platform_instance(
            self.platform, datasource_id, self.config.platform_instance, self.config.env
        )
        if datasource_id not in self.datasource_ids_being_used:
            self.datasource_ids_being_used.append(datasource_id)

        dataset_snapshot = DatasetSnapshot(
            urn=datasource_urn,
            aspects=[self.get_data_platform_instance()],
        )

        # Browse path

        if (
            browse_path
            and is_embedded_ds
            and workbook
            and workbook.get(tableau_constant.NAME)
        ):
            browse_path = f"{browse_path}/{workbook[tableau_constant.NAME].replace('/', REPLACE_SLASH_CHAR)}"

        if browse_path:
            browse_paths = BrowsePathsClass(
                paths=[f"/{self.config.env.lower()}/{self.platform}/{browse_path}"]
            )
            dataset_snapshot.aspects.append(browse_paths)

        # Ownership
        owner = (
            self._get_ownership(
                datasource_info.get(tableau_constant.OWNER, {}).get(
                    tableau_constant.USERNAME, ""
                )
            )
            if datasource_info
            else None
        )
        if owner is not None:
            dataset_snapshot.aspects.append(owner)

        # Dataset properties
        dataset_props = DatasetPropertiesClass(
            name=datasource.get(tableau_constant.NAME),
            description=datasource.get(tableau_constant.DESCRIPTION),
            customProperties={
                tableau_constant.HAS_EXTRACTS: str(
                    datasource.get(tableau_constant.HAS_EXTRACTS, "")
                ),
                tableau_constant.EXTRACT_LAST_REFRESH_TIME: datasource.get(
                    tableau_constant.EXTRACT_LAST_REFRESH_TIME, ""
                )
                or "",
                tableau_constant.EXTRACT_LAST_INCREMENTAL_UPDATE_TIME: datasource.get(
                    tableau_constant.EXTRACT_LAST_INCREMENTAL_UPDATE_TIME, ""
                )
                or "",
                tableau_constant.EXTRACT_LAST_UPDATE_TIME: datasource.get(
                    tableau_constant.EXTRACT_LAST_UPDATE_TIME, ""
                )
                or "",
                tableau_constant.TYPE: datasource.get(tableau_constant.TYPE_NAME, ""),
            },
        )
        dataset_snapshot.aspects.append(dataset_props)

        # Upstream Tables
        if datasource.get(tableau_constant.UPSTREAM_TABLES) or datasource.get(
            tableau_constant.UPSTREAM_DATA_SOURCES
        ):
            # datasource -> db table relations
            (
                upstream_tables,
                fine_grained_lineages,
            ) = self._create_upstream_table_lineage(
                datasource, browse_path, is_embedded_ds=is_embedded_ds
            )

            if upstream_tables:
                upstream_lineage = UpstreamLineage(
                    upstreams=upstream_tables,
                    fineGrainedLineages=sorted(
                        fine_grained_lineages,
                        key=lambda x: (x.downstreams, x.upstreams),
                    )
                    or None,
                )
                yield self.get_metadata_change_proposal(
                    datasource_urn,
                    aspect_name=tableau_constant.UPSTREAM_LINEAGE,
                    aspect=upstream_lineage,
                )

        # Datasource Fields
        schema_metadata = self._get_schema_metadata_for_datasource(
            datasource.get(tableau_constant.FIELDS, [])
        )
        if schema_metadata is not None:
            dataset_snapshot.aspects.append(schema_metadata)

        yield self.get_metadata_change_event(dataset_snapshot)
        yield self.get_metadata_change_proposal(
            dataset_snapshot.urn,
            aspect_name=tableau_constant.SUB_TYPES,
            aspect=SubTypesClass(
                typeNames=(
                    ["Embedded Data Source"]
                    if is_embedded_ds
                    else ["Published Data Source"]
                )
            ),
        )

        if (
            is_embedded_ds and workbook is not None
        ):  # It is embedded then parent is container is workbook
            workunits = add_entity_to_container(
                self.gen_workbook_key(workbook),
                tableau_constant.DATASET,
                dataset_snapshot.urn,
            )
            for wu in workunits:
                self.report.report_workunit(wu)
                yield wu
        elif (
            datasource.get(tableau_constant.LUID)
            and datasource[tableau_constant.LUID] in self.datasource_project_map.keys()
        ):  # It is published datasource and hence parent container is project
            workunits = add_entity_to_container(
                self.gen_project_key(
                    self.datasource_project_map[datasource[tableau_constant.LUID]]
                ),
                tableau_constant.DATASET,
                dataset_snapshot.urn,
            )
            for wu in workunits:
                self.report.report_workunit(wu)
                yield wu
        else:
            logger.warning(
                f"Parent container not set for datasource {datasource[tableau_constant.ID]}"
            )

    def emit_published_datasources(self) -> Iterable[MetadataWorkUnit]:
        datasource_filter = f"{tableau_constant.ID_WITH_IN}: {json.dumps(self.datasource_ids_being_used)}"

        for datasource in self.get_connection_objects(
            published_datasource_graphql_query,
            tableau_constant.PUBLISHED_DATA_SOURCES_CONNECTION,
            datasource_filter,
        ):
            yield from self.emit_datasource(datasource)

    def emit_upstream_tables(self) -> Iterable[MetadataWorkUnit]:
        for (
            table_urn,
            (columns, browse_path, is_embedded),
        ) in self.upstream_tables.items():
            if not is_embedded and not self.config.ingest_tables_external:
                logger.debug(
                    f"Skipping external table {table_urn} as ingest_tables_external is set to False"
                )
                continue

            dataset_snapshot = DatasetSnapshot(
                urn=table_urn,
                aspects=[],
            )
            if browse_path:
                # Browse path
                browse_paths = BrowsePathsClass(
                    paths=[f"/{self.config.env.lower()}/{self.platform}/{browse_path}"]
                )
                dataset_snapshot.aspects.append(browse_paths)
            else:
                logger.debug(f"Browse path not set for table {table_urn}")
            schema_metadata = None
            if columns:
                fields = []
                for field in columns:
                    if field.get(tableau_constant.NAME) is None:
                        logger.warning(
                            f"Skipping field {field[tableau_constant.ID]} from schema since its name is none"
                        )
                        continue
                    nativeDataType = field.get(
                        tableau_constant.REMOTE_TYPE, tableau_constant.UNKNOWN
                    )
                    TypeClass = FIELD_TYPE_MAPPING.get(nativeDataType, NullTypeClass)

                    schema_field = SchemaField(
                        fieldPath=field[tableau_constant.NAME],
                        type=SchemaFieldDataType(type=TypeClass()),
                        description="",
                        nativeDataType=nativeDataType,
                    )

                    fields.append(schema_field)

                schema_metadata = SchemaMetadata(
                    schemaName="test",
                    platform=f"urn:li:dataPlatform:{self.platform}",
                    version=0,
                    fields=fields,
                    hash="",
                    platformSchema=OtherSchema(rawSchema=""),
                )
            if schema_metadata is not None:
                dataset_snapshot.aspects.append(schema_metadata)

            yield self.get_metadata_change_event(dataset_snapshot)

    def get_sheetwise_upstream_datasources(self, sheet: dict) -> set:
        sheet_upstream_datasources = set()

        for field in sheet.get(tableau_constant.DATA_SOURCE_FIELDS, ""):
            if field and field.get(tableau_constant.DATA_SOURCE):
                sheet_upstream_datasources.add(
                    field[tableau_constant.DATA_SOURCE][tableau_constant.ID]
                )

        return sheet_upstream_datasources

    @staticmethod
    def _create_datahub_chart_usage_stat(
        usage_stat: UsageStat,
    ) -> ChartUsageStatisticsClass:
        return ChartUsageStatisticsClass(
            timestampMillis=round(datetime.now().timestamp() * 1000),
            viewsCount=usage_stat.view_count,
        )

    def _get_chart_stat_wu(
        self, sheet: dict, sheet_urn: str
    ) -> Optional[MetadataWorkUnit]:
        luid: Optional[str] = sheet.get(tableau_constant.LUID)
        if luid is None:
            logger.debug(
                "stat:luid is none for sheet %s(id:%s)",
                sheet.get(tableau_constant.NAME),
                sheet.get(tableau_constant.ID),
            )
            return None
        usage_stat: Optional[UsageStat] = self.tableau_stat_registry.get(luid)
        if usage_stat is None:
            logger.debug(
                "stat:UsageStat is not available in tableau_stat_registry for sheet %s(id:%s)",
                sheet.get(tableau_constant.NAME),
                sheet.get(tableau_constant.ID),
            )
            return None

        aspect: ChartUsageStatisticsClass = self._create_datahub_chart_usage_stat(
            usage_stat
        )
        logger.debug(
            "stat: Chart usage stat work unit is created for %s(id:%s)",
            sheet.get(tableau_constant.NAME),
            sheet.get(tableau_constant.ID),
        )
        return MetadataChangeProposalWrapper(
            aspect=aspect,
            entityUrn=sheet_urn,
        ).as_workunit()

    def emit_sheets(self) -> Iterable[MetadataWorkUnit]:
        sheets_filter = f"{tableau_constant.ID_WITH_IN}: {json.dumps(self.sheet_ids)}"

        for sheet in self.get_connection_objects(
            sheet_graphql_query,
            tableau_constant.SHEETS_CONNECTION,
            sheets_filter,
        ):
            yield from self.emit_sheets_as_charts(
                sheet, sheet.get(tableau_constant.WORKBOOK)
            )

    def emit_sheets_as_charts(
        self, sheet: dict, workbook: Optional[Dict]
    ) -> Iterable[MetadataWorkUnit]:
        sheet_urn: str = builder.make_chart_urn(
            self.platform, sheet[tableau_constant.ID], self.config.platform_instance
        )
        chart_snapshot = ChartSnapshot(
            urn=sheet_urn,
            aspects=[self.get_data_platform_instance()],
        )

        creator: Optional[str] = None
        if workbook is not None and workbook.get(tableau_constant.OWNER) is not None:
            creator = workbook[tableau_constant.OWNER].get(tableau_constant.USERNAME)
        created_at = sheet.get(tableau_constant.CREATED_AT, datetime.now())
        updated_at = sheet.get(tableau_constant.UPDATED_AT, datetime.now())
        last_modified = self.get_last_modified(creator, created_at, updated_at)

        if sheet.get(tableau_constant.PATH):
            site_part = f"/site/{self.config.site}" if self.config.site else ""
            sheet_external_url = f"{self.config.connect_uri}/#{site_part}/views/{sheet.get(tableau_constant.PATH)}"
        elif (
            sheet.get(tableau_constant.CONTAINED_IN_DASHBOARDS) is not None
            and len(sheet[tableau_constant.CONTAINED_IN_DASHBOARDS]) > 0
            and sheet[tableau_constant.CONTAINED_IN_DASHBOARDS][0] is not None
        ):
            # sheet contained in dashboard
            site_part = f"/t/{self.config.site}" if self.config.site else ""
            dashboard_path = sheet[tableau_constant.CONTAINED_IN_DASHBOARDS][0].get(
                tableau_constant.PATH, ""
            )
            sheet_external_url = f"{self.config.connect_uri}{site_part}/authoring/{dashboard_path}/{sheet.get(tableau_constant.NAME, '')}"
        else:
            # hidden or viz-in-tooltip sheet
            sheet_external_url = None
        input_fields: List[InputField] = []
        if sheet.get(tableau_constant.DATA_SOURCE_FIELDS):
            self.populate_sheet_upstream_fields(sheet, input_fields)

        # datasource urn
        datasource_urn = []
        data_sources = self.get_sheetwise_upstream_datasources(sheet)

        for ds_id in data_sources:
            ds_urn = builder.make_dataset_urn_with_platform_instance(
                self.platform, ds_id, self.config.platform_instance, self.config.env
            )
            datasource_urn.append(ds_urn)
            if ds_id not in self.datasource_ids_being_used:
                self.datasource_ids_being_used.append(ds_id)

        # Chart Info
        chart_info = ChartInfoClass(
            description="",
            title=sheet.get(tableau_constant.NAME, ""),
            lastModified=last_modified,
            externalUrl=sheet_external_url,
            inputs=sorted(datasource_urn),
            customProperties={
                tableau_constant.LUID: sheet.get(tableau_constant.LUID) or ""
            },
        )
        chart_snapshot.aspects.append(chart_info)
        # chart_snapshot doesn't support the stat aspect as list element and hence need to emit MCP

        if self.config.extract_usage_stats:
            wu = self._get_chart_stat_wu(sheet, sheet_urn)
            if wu is not None:
                self.report.report_workunit(wu)
                yield wu

        project_luid: Optional[str] = self._get_workbook_project_luid(workbook)

        if (
            workbook is not None
            and project_luid
            and project_luid in self.tableau_project_registry
            and workbook.get(tableau_constant.NAME)
        ):
            browse_paths = BrowsePathsClass(
                paths=[
                    f"/{self.platform}/{self._project_luid_to_browse_path_name(project_luid)}"
                    f"/{workbook[tableau_constant.NAME].replace('/', REPLACE_SLASH_CHAR)}"
                ]
            )
            chart_snapshot.aspects.append(browse_paths)
        else:
            logger.warning(
                f"Could not set browse path for workbook {sheet[tableau_constant.ID]}. Please check permissions."
            )

        # Ownership
        owner = self._get_ownership(creator)
        if owner is not None:
            chart_snapshot.aspects.append(owner)

        #  Tags
        tag_list = sheet.get(tableau_constant.TAGS, [])
        if tag_list and self.config.ingest_tags:
            tag_list_str = [
                t.get(tableau_constant.NAME, "") for t in tag_list if t is not None
            ]
            chart_snapshot.aspects.append(
                builder.make_global_tag_aspect_with_tag_list(tag_list_str)
            )
        yield self.get_metadata_change_event(chart_snapshot)
        if sheet_external_url is not None and self.config.ingest_embed_url is True:
            yield self.new_work_unit(
                self.new_embed_aspect_mcp(
                    entity_urn=sheet_urn,
                    embed_url=sheet_external_url,
                )
            )
        if workbook is not None:
            workunits = add_entity_to_container(
                self.gen_workbook_key(workbook),
                tableau_constant.CHART,
                chart_snapshot.urn,
            )

        for wu in workunits:
            self.report.report_workunit(wu)
            yield wu

        if input_fields:
            wu = MetadataChangeProposalWrapper(
                entityUrn=sheet_urn,
                aspect=InputFields(
                    fields=sorted(input_fields, key=lambda x: x.schemaFieldUrn)
                ),
            ).as_workunit()
            self.report.report_workunit(wu)
            yield wu

    def _project_luid_to_browse_path_name(self, project_luid: str) -> str:
        assert project_luid
        project: TableauProject = self.tableau_project_registry[project_luid]
        normalised_path: List[str] = [
            p.replace("/", REPLACE_SLASH_CHAR) for p in project.path
        ]
        return "/".join(normalised_path)

    def _get_project_path(self, project: TableauProject) -> str:
        return self.config.project_path_separator.join(project.path)

    def populate_sheet_upstream_fields(
        self, sheet: dict, input_fields: List[InputField]
    ) -> None:
        for field in sheet.get(tableau_constant.DATA_SOURCE_FIELDS):  # type: ignore
            if not field:
                continue
            name = field.get(tableau_constant.NAME)
            upstream_ds_id = (
                field.get(tableau_constant.DATA_SOURCE)[tableau_constant.ID]
                if field.get(tableau_constant.DATA_SOURCE)
                else None
            )
            if name and upstream_ds_id:
                input_fields.append(
                    InputField(
                        schemaFieldUrn=builder.make_schema_field_urn(
                            parent_urn=builder.make_dataset_urn_with_platform_instance(
                                self.platform,
                                upstream_ds_id,
                                self.config.platform_instance,
                                self.config.env,
                            ),
                            field_path=name,
                        ),
                        schemaField=tableau_field_to_schema_field(
                            field, self.config.ingest_tags
                        ),
                    )
                )

    def emit_workbook_as_container(self, workbook: Dict) -> Iterable[MetadataWorkUnit]:
        workbook_container_key = self.gen_workbook_key(workbook)
        creator = workbook.get(tableau_constant.OWNER, {}).get(
            tableau_constant.USERNAME
        )

        owner_urn = (
            builder.make_user_urn(creator)
            if (creator and self.config.ingest_owner)
            else None
        )

        site_part = f"/site/{self.config.site}" if self.config.site else ""
        workbook_uri = workbook.get("uri", "")
        workbook_part = (
            workbook_uri[workbook_uri.index("/workbooks/") :]
            if workbook.get("uri")
            else None
        )
        workbook_external_url = (
            f"{self.config.connect_uri}/#{site_part}{workbook_part}"
            if workbook_part
            else None
        )

        tag_list = workbook.get(tableau_constant.TAGS, [])
        tag_list_str = (
            [t.get(tableau_constant.NAME, "") for t in tag_list if t is not None]
            if (tag_list and self.config.ingest_tags)
            else None
        )
        parent_key = None
        project_luid: Optional[str] = self._get_workbook_project_luid(workbook)
        if project_luid and project_luid in self.tableau_project_registry.keys():
            parent_key = self.gen_project_key(project_luid)
        else:
            workbook_id: Optional[str] = workbook.get(tableau_constant.ID)
            workbook_name: Optional[str] = workbook.get(tableau_constant.NAME)
            logger.warning(
                f"Could not load project hierarchy for workbook {workbook_name}({workbook_id}). Please check permissions."
            )

        container_workunits = gen_containers(
            container_key=workbook_container_key,
            name=workbook.get(tableau_constant.NAME, ""),
            parent_container_key=parent_key,
            description=workbook.get(tableau_constant.DESCRIPTION),
            sub_types=[BIContainerSubTypes.TABLEAU_WORKBOOK],
            owner_urn=owner_urn,
            external_url=workbook_external_url,
            tags=tag_list_str,
        )

        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def gen_workbook_key(self, workbook: Dict) -> WorkbookKey:
        return WorkbookKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            workbook_id=workbook[tableau_constant.ID],
        )

    def gen_project_key(self, project_luid):
        return ProjectKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            project_id=project_luid,
        )

    @staticmethod
    def _create_datahub_dashboard_usage_stat(
        usage_stat: UsageStat,
    ) -> DashboardUsageStatisticsClass:
        return DashboardUsageStatisticsClass(
            timestampMillis=round(datetime.now().timestamp() * 1000),
            # favoritesCount=looker_dashboard.favorite_count,  It is available in REST API response,
            # however not exposed by tableau python library
            viewsCount=usage_stat.view_count,
            # lastViewedAt=looker_dashboard.last_viewed_at, Not available
        )

    def _get_dashboard_stat_wu(
        self, dashboard: dict, dashboard_urn: str
    ) -> Optional[MetadataWorkUnit]:
        luid: Optional[str] = dashboard.get(tableau_constant.LUID)
        if luid is None:
            logger.debug(
                "stat:luid is none for dashboard %s(id:%s)",
                dashboard.get(tableau_constant.NAME),
                dashboard.get(tableau_constant.ID),
            )
            return None
        usage_stat: Optional[UsageStat] = self.tableau_stat_registry.get(luid)
        if usage_stat is None:
            logger.debug(
                "stat:UsageStat is not available in tableau_stat_registry for dashboard %s(id:%s)",
                dashboard.get(tableau_constant.NAME),
                dashboard.get(tableau_constant.ID),
            )
            return None

        aspect: DashboardUsageStatisticsClass = (
            self._create_datahub_dashboard_usage_stat(usage_stat)
        )
        logger.debug(
            "stat: Dashboard usage stat is created for %s(id:%s)",
            dashboard.get(tableau_constant.NAME),
            dashboard.get(tableau_constant.ID),
        )

        return MetadataChangeProposalWrapper(
            aspect=aspect,
            entityUrn=dashboard_urn,
        ).as_workunit()

    @staticmethod
    def new_embed_aspect_mcp(
        entity_urn: str, embed_url: str
    ) -> MetadataChangeProposalWrapper:
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=EmbedClass(renderUrl=embed_url),
        )

    def new_work_unit(self, mcp: MetadataChangeProposalWrapper) -> MetadataWorkUnit:
        return MetadataWorkUnit(
            id="{PLATFORM}-{ENTITY_URN}-{ASPECT_NAME}".format(
                PLATFORM=self.platform,
                ENTITY_URN=mcp.entityUrn,
                ASPECT_NAME=mcp.aspectName,
            ),
            mcp=mcp,
        )

    def emit_dashboards(self) -> Iterable[MetadataWorkUnit]:
        dashboards_filter = (
            f"{tableau_constant.ID_WITH_IN}: {json.dumps(self.dashboard_ids)}"
        )

        for dashboard in self.get_connection_objects(
            dashboard_graphql_query,
            tableau_constant.DASHBOARDS_CONNECTION,
            dashboards_filter,
        ):
            yield from self.emit_dashboard(
                dashboard, dashboard.get(tableau_constant.WORKBOOK)
            )

    def emit_dashboard(
        self, dashboard: dict, workbook: Optional[Dict]
    ) -> Iterable[MetadataWorkUnit]:
        dashboard_urn: str = builder.make_dashboard_urn(
            self.platform, dashboard[tableau_constant.ID], self.config.platform_instance
        )
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[self.get_data_platform_instance()],
        )

        creator: Optional[str] = None
        if workbook is not None and workbook.get(tableau_constant.OWNER) is not None:
            creator = workbook[tableau_constant.OWNER].get(tableau_constant.USERNAME)
        created_at = dashboard.get(tableau_constant.CREATED_AT, datetime.now())
        updated_at = dashboard.get(tableau_constant.UPDATED_AT, datetime.now())
        last_modified = self.get_last_modified(creator, created_at, updated_at)

        site_part = f"/site/{self.config.site}" if self.config.site else ""
        dashboard_external_url = f"{self.config.connect_uri}/#{site_part}/views/{dashboard.get(tableau_constant.PATH, '')}"
        title = (
            dashboard[tableau_constant.NAME].replace("/", REPLACE_SLASH_CHAR)
            if dashboard.get(tableau_constant.NAME)
            else ""
        )
        chart_urns = [
            builder.make_chart_urn(
                self.platform,
                sheet.get(tableau_constant.ID),
                self.config.platform_instance,
            )
            for sheet in dashboard.get(tableau_constant.SHEETS, [])
        ]
        dashboard_info_class = DashboardInfoClass(
            description="",
            title=title,
            charts=chart_urns,
            lastModified=last_modified,
            dashboardUrl=dashboard_external_url,
            customProperties={
                tableau_constant.LUID: dashboard.get(tableau_constant.LUID) or ""
            },
        )
        dashboard_snapshot.aspects.append(dashboard_info_class)

        tag_list = dashboard.get(tableau_constant.TAGS, [])
        if tag_list and self.config.ingest_tags:
            tag_list_str = [
                t.get(tableau_constant.NAME, "") for t in tag_list if t is not None
            ]
            dashboard_snapshot.aspects.append(
                builder.make_global_tag_aspect_with_tag_list(tag_list_str)
            )

        if self.config.extract_usage_stats:
            # dashboard_snapshot doesn't support the stat aspect as list element and hence need to emit MetadataWorkUnit
            wu = self._get_dashboard_stat_wu(dashboard, dashboard_urn)
            if wu is not None:
                self.report.report_workunit(wu)
                yield wu

        project_luid: Optional[str] = self._get_workbook_project_luid(workbook)
        if (
            workbook is not None
            and project_luid
            and project_luid in self.tableau_project_registry
            and workbook.get(tableau_constant.NAME)
        ):
            browse_paths = BrowsePathsClass(
                paths=[
                    f"/{self.platform}/{self._project_luid_to_browse_path_name(project_luid)}"
                    f"/{workbook[tableau_constant.NAME].replace('/', REPLACE_SLASH_CHAR)}"
                ]
            )
            dashboard_snapshot.aspects.append(browse_paths)
        elif (
            workbook is not None
            and workbook.get(tableau_constant.PROJECT_NAME)
            and workbook.get(tableau_constant.NAME)
        ):
            # browse path
            browse_paths = BrowsePathsClass(
                paths=[
                    f"/{self.platform}/{workbook[tableau_constant.PROJECT_NAME].replace('/', REPLACE_SLASH_CHAR)}"
                    f"/{workbook[tableau_constant.NAME].replace('/', REPLACE_SLASH_CHAR)}"
                ]
            )
            dashboard_snapshot.aspects.append(browse_paths)
        else:
            logger.warning(
                f"Could not set browse path for dashboard {dashboard[tableau_constant.ID]}. Please check permissions."
            )

        # Ownership
        owner = self._get_ownership(creator)
        if owner is not None:
            dashboard_snapshot.aspects.append(owner)

        yield self.get_metadata_change_event(dashboard_snapshot)
        # Yield embed MCP
        if self.config.ingest_embed_url is True:
            yield self.new_work_unit(
                self.new_embed_aspect_mcp(
                    entity_urn=dashboard_urn,
                    embed_url=dashboard_external_url,
                )
            )

        if workbook is not None:
            workunits = add_entity_to_container(
                self.gen_workbook_key(workbook),
                tableau_constant.DASHBOARD,
                dashboard_snapshot.urn,
            )
        for wu in workunits:
            self.report.report_workunit(wu)
            yield wu

    def emit_embedded_datasources(self) -> Iterable[MetadataWorkUnit]:
        datasource_filter = f"{tableau_constant.ID_WITH_IN}: {json.dumps(self.embedded_datasource_ids_being_used)}"

        for datasource in self.get_connection_objects(
            embedded_datasource_graphql_query,
            tableau_constant.EMBEDDED_DATA_SOURCES_CONNECTION,
            datasource_filter,
        ):
            yield from self.emit_datasource(
                datasource,
                datasource.get(tableau_constant.WORKBOOK),
                is_embedded_ds=True,
            )

    @lru_cache(maxsize=None)
    def _get_schema(self, schema_provided: str, database: str, fullName: str) -> str:
        # For some databases, the schema attribute in tableau api does not return
        # correct schema name for the table. For more information, see
        # https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_model.html#schema_attribute.
        # Hence we extract schema from fullName whenever fullName is available
        schema = self._extract_schema_from_fullName(fullName) if fullName else ""
        if not schema:
            schema = schema_provided
        elif schema != schema_provided:
            logger.debug(
                "Correcting schema, provided {0}, corrected {1}".format(
                    schema_provided, schema
                )
            )

        if not schema and database in self.config.default_schema_map:
            schema = self.config.default_schema_map[database]

        return schema

    @lru_cache(maxsize=None)
    def _extract_schema_from_fullName(self, fullName: str) -> str:
        # fullName is observed to be in format [schemaName].[tableName]
        # OR simply tableName OR [tableName]
        if fullName.startswith("[") and "].[" in fullName:
            return fullName[1 : fullName.index("]")]
        return ""

    @lru_cache(maxsize=None)
    def get_last_modified(
        self, creator: Optional[str], created_at: bytes, updated_at: bytes
    ) -> ChangeAuditStamps:
        last_modified = ChangeAuditStamps()
        if creator:
            modified_actor = builder.make_user_urn(creator)
            created_ts = int(dp.parse(created_at).timestamp() * 1000)
            modified_ts = int(dp.parse(updated_at).timestamp() * 1000)
            last_modified = ChangeAuditStamps(
                created=AuditStamp(time=created_ts, actor=modified_actor),
                lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
            )
        return last_modified

    @lru_cache(maxsize=None)
    def _get_ownership(self, user: str) -> Optional[OwnershipClass]:
        if self.config.ingest_owner and user:
            owner_urn = builder.make_user_urn(user)
            ownership: OwnershipClass = OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=owner_urn,
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                ]
            )
            return ownership

        return None

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = TableauConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        return auto_stale_entity_removal(
            self.stale_entity_removal_handler,
            auto_status_aspect(self.get_workunits_internal()),
        )

    def emit_project_containers(self) -> Iterable[MetadataWorkUnit]:
        for _id, project in self.tableau_project_registry.items():
            project_workunits = gen_containers(
                container_key=self.gen_project_key(_id),
                name=project.name,
                description=project.description,
                sub_types=[tableau_constant.PROJECT],
                parent_container_key=self.gen_project_key(project.parent_id)
                if project.parent_id
                else None,
            )
            for wu in project_workunits:
                self.report.report_workunit(wu)
                yield wu

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        if self.server is None or not self.server.is_signed_in():
            return
        try:
            # Initialise the dictionary to later look-up for chart and dashboard stat
            if self.config.extract_usage_stats:
                self._populate_usage_stat_registry()

            self._populate_projects_registry()
            yield from self.emit_project_containers()
            yield from self.emit_workbooks()
            if self.sheet_ids:
                yield from self.emit_sheets()
            if self.dashboard_ids:
                yield from self.emit_dashboards()
            if self.embedded_datasource_ids_being_used:
                yield from self.emit_embedded_datasources()
            if self.datasource_ids_being_used:
                yield from self.emit_published_datasources()
            if self.custom_sql_ids_being_used:
                yield from self.emit_custom_sql_datasources()
            yield from self.emit_upstream_tables()
        except MetadataQueryException as md_exception:
            self.report.report_failure(
                key="tableau-metadata",
                reason=f"Unable to retrieve metadata from tableau. Information: {str(md_exception)}",
            )

    def get_report(self) -> StaleEntityRemovalSourceReport:
        return self.report
