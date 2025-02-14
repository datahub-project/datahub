import json
import logging
import re
import time
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field as dataclass_field
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)
from urllib.parse import quote, urlparse

import dateutil.parser as dp
import tableauserverclient as TSC
from pydantic import root_validator, validator
from pydantic.fields import Field
from requests.adapters import HTTPAdapter
from tableauserverclient import (
    GroupItem,
    PermissionsRule,
    PersonalAccessTokenAuth,
    Server,
    ServerResponseError,
    SiteItem,
    TableauAuth,
)
from tableauserverclient.server.endpoint.exceptions import (
    InternalServerError,
    NonXMLResponseError,
)
from urllib3 import Retry

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    ConfigurationError,
)
from datahub.configuration.source_common import (
    DatasetLineageProviderConfigBase,
    DatasetSourceConfigMixin,
)
from datahub.configuration.validate_field_deprecation import pydantic_field_deprecated
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    ContainerKey,
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
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    StructuredLogLevel,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.tableau import tableau_constant as c
from datahub.ingestion.source.tableau.tableau_common import (
    FIELD_TYPE_MAPPING,
    MetadataQueryException,
    TableauLineageOverrides,
    TableauUpstreamReference,
    clean_query,
    custom_sql_graphql_query,
    dashboard_graphql_query,
    database_servers_graphql_query,
    database_tables_graphql_query,
    datasource_upstream_fields_graphql_query,
    embedded_datasource_graphql_query,
    get_filter_pages,
    get_overridden_info,
    get_unique_custom_sql,
    make_filter,
    make_fine_grained_lineage_class,
    make_upstream_class,
    optimize_query_filter,
    published_datasource_graphql_query,
    query_metadata_cursor_based_pagination,
    sheet_graphql_query,
    tableau_field_to_schema_field,
    workbook_graphql_query,
)
from datahub.ingestion.source.tableau.tableau_server_wrapper import UserInfo
from datahub.ingestion.source.tableau.tableau_validation import check_user_role
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
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
    ViewPropertiesClass,
)
from datahub.sql_parsing.sql_parsing_result_utils import (
    transform_parsing_result_to_in_tables_schemas,
)
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    SqlParsingResult,
    create_lineage_sql_parsed_result,
)
from datahub.utilities import config_clean
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.stats_collections import TopKDict
from datahub.utilities.urns.dataset_urn import DatasetUrn

DEFAULT_PAGE_SIZE = 10

try:
    # On earlier versions of the tableauserverclient, the NonXMLResponseError
    # was thrown when reauthentication was necessary. We'll keep both exceptions
    # around for now, but can remove this in the future.
    from tableauserverclient.server.endpoint.exceptions import (  # type: ignore
        NotSignedInError,
    )

    REAUTHENTICATE_ERRORS: Tuple[Type[Exception], ...] = (
        NotSignedInError,
        NonXMLResponseError,
    )
except ImportError:
    REAUTHENTICATE_ERRORS = (NonXMLResponseError,)

RETRIABLE_ERROR_CODES = [
    408,  # Request Timeout
    429,  # Too Many Requests
    500,  # Internal Server Error
    502,  # Bad Gateway
    503,  # Service Unavailable
    504,  # Gateway Timeout
]

# From experience, this expiry time typically ranges from 50 minutes
# to 2 hours but might as well be configurable. We will allow upto
# 10 minutes of such expiry time
REGULAR_AUTH_EXPIRY_PERIOD = timedelta(minutes=10)

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

    max_retries: int = Field(3, description="Number of retries for failed requests.")
    ssl_verify: Union[bool, str] = Field(
        default=True,
        description="Whether to verify SSL certificates. If using self-signed certificates, set to false or provide the path to the .pem certificate bundle.",
    )

    session_trust_env: bool = Field(
        False,
        description="Configures the trust_env property in the requests session. If set to false (default value) it will bypass proxy settings. See https://requests.readthedocs.io/en/latest/api/#requests.Session.trust_env for more information.",
    )

    extract_column_level_lineage: bool = Field(
        True,
        description="When enabled, extracts column-level lineage from Tableau Datasources",
    )

    @validator("connect_uri")
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)

    def get_tableau_auth(
        self, site: str
    ) -> Union[TableauAuth, PersonalAccessTokenAuth]:
        # https://tableau.github.io/server-client-python/docs/api-ref#authentication
        authentication: Union[TableauAuth, PersonalAccessTokenAuth]
        if self.username and self.password:
            authentication = TableauAuth(
                username=self.username,
                password=self.password,
                site_id=site,
            )
        elif self.token_name and self.token_value:
            authentication = PersonalAccessTokenAuth(
                self.token_name, self.token_value, site
            )
        else:
            raise ConfigurationError(
                "Tableau Source: Either username/password or token_name/token_value must be set"
            )
        return authentication

    def make_tableau_client(self, site: str) -> Server:
        authentication: Union[TableauAuth, PersonalAccessTokenAuth] = (
            self.get_tableau_auth(site)
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

            server._session.trust_env = self.session_trust_env

            # Setup request retries.
            adapter = HTTPAdapter(
                max_retries=Retry(
                    total=self.max_retries,
                    backoff_factor=1,
                    status_forcelist=RETRIABLE_ERROR_CODES,
                )
            )
            server._session.mount("http://", adapter)
            server._session.mount("https://", adapter)

            server.auth.sign_in(authentication)
            return server
        except ServerResponseError as e:
            message = f"Unable to login (invalid/expired credentials or missing permissions): {str(e)}"
            if isinstance(authentication, PersonalAccessTokenAuth):
                # Docs on token expiry in Tableau:
                # https://help.tableau.com/current/server/en-us/security_personal_access_tokens.htm#token-expiry
                message = f"Error authenticating with Tableau. Note that Tableau personal access tokens expire if not used for 15 days or if over 1 year old: {str(e)}"
            raise ValueError(message) from e
        except Exception as e:
            raise ValueError(
                f"Unable to login (check your Tableau connection and credentials): {str(e)}"
            ) from e


class PermissionIngestionConfig(ConfigModel):
    enable_workbooks: bool = Field(
        default=True,
        description="Whether or not to enable group permission ingestion for workbooks. "
        "Default: True",
    )

    group_name_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filter for Tableau group names when ingesting group permissions. "
        "For example, you could filter for groups that include the term 'Consumer' in their name by adding '^.*Consumer$' to the allow list."
        "By default, all groups will be ingested. "
        "You can both allow and deny groups based on their name using their name, or a Regex pattern. "
        "Deny patterns always take precedence over allow patterns. ",
    )


class TableauPageSizeConfig(ConfigModel):
    """
    Configuration for setting page sizes for different Tableau metadata objects.

    Some considerations:
    - All have default values, so no setting is mandatory.
    - In general, with the `effective_` methods, if not specifically set fine-grained metrics fallback to `page_size`
    or correlate with `page_size`.

    Measuring the impact of changing these values can be done by looking at the
    `num_(filter_|paginated_)?queries_by_connection_type` metrics in the report.
    """

    page_size: int = Field(
        default=DEFAULT_PAGE_SIZE,
        description="[advanced] Number of metadata objects (e.g. CustomSQLTable, PublishedDatasource, etc) to query at a time using the Tableau API.",
    )

    database_server_page_size: Optional[int] = Field(
        default=None,
        description="[advanced] Number of database servers to query at a time using the Tableau API; fallbacks to `page_size` if not set.",
    )

    @property
    def effective_database_server_page_size(self) -> int:
        return self.database_server_page_size or self.page_size

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
    workbook_page_size: Optional[int] = Field(
        default=1,
        description="[advanced] Number of workbooks to query at a time using the Tableau API; defaults to `1` and fallbacks to `page_size` if not set.",
    )

    @property
    def effective_workbook_page_size(self) -> int:
        return self.workbook_page_size or self.page_size

    sheet_page_size: Optional[int] = Field(
        default=None,
        description="[advanced] Number of sheets to query at a time using the Tableau API; fallbacks to `page_size` if not set.",
    )

    @property
    def effective_sheet_page_size(self) -> int:
        return self.sheet_page_size or self.page_size

    dashboard_page_size: Optional[int] = Field(
        default=None,
        description="[advanced] Number of dashboards to query at a time using the Tableau API; fallbacks to `page_size` if not set.",
    )

    @property
    def effective_dashboard_page_size(self) -> int:
        return self.dashboard_page_size or self.page_size

    embedded_datasource_page_size: Optional[int] = Field(
        default=None,
        description="[advanced] Number of embedded datasources to query at a time using the Tableau API; fallbacks to `page_size` if not set.",
    )

    @property
    def effective_embedded_datasource_page_size(self) -> int:
        return self.embedded_datasource_page_size or self.page_size

    # Since the field upstream query was separated from the embedded datasource queries into an independent query,
    # the number of queries increased significantly and so the execution time.
    # To increase the batching and so reduce the number of queries, we can increase the page size for that
    # particular case.
    #
    # That's why unless specifically set, we will effectively use 10 times the page size as the default page size.
    embedded_datasource_field_upstream_page_size: Optional[int] = Field(
        default=None,
        description="[advanced] Number of upstream fields to query at a time for embedded datasources using the Tableau API; fallbacks to `page_size` * 10 if not set.",
    )

    @property
    def effective_embedded_datasource_field_upstream_page_size(self) -> int:
        return self.embedded_datasource_field_upstream_page_size or self.page_size * 10

    published_datasource_page_size: Optional[int] = Field(
        default=None,
        description="[advanced] Number of published datasources to query at a time using the Tableau API; fallbacks to `page_size` if not set.",
    )

    @property
    def effective_published_datasource_page_size(self) -> int:
        return self.published_datasource_page_size or self.page_size

    published_datasource_field_upstream_page_size: Optional[int] = Field(
        default=None,
        description="[advanced] Number of upstream fields to query at a time for published datasources using the Tableau API; fallbacks to `page_size` * 10 if not set.",
    )

    @property
    def effective_published_datasource_field_upstream_page_size(self) -> int:
        return self.published_datasource_field_upstream_page_size or self.page_size * 10

    custom_sql_table_page_size: Optional[int] = Field(
        default=None,
        description="[advanced] Number of custom sql datasources to query at a time using the Tableau API; fallbacks to `page_size` if not set.",
    )

    @property
    def effective_custom_sql_table_page_size(self) -> int:
        return self.custom_sql_table_page_size or self.page_size

    database_table_page_size: Optional[int] = Field(
        default=None,
        description="[advanced] Number of database tables to query at a time using the Tableau API; fallbacks to `page_size` if not set.",
    )

    @property
    def effective_database_table_page_size(self) -> int:
        return self.database_table_page_size or self.page_size


class TableauConfig(
    DatasetLineageProviderConfigBase,
    StatefulIngestionConfigBase,
    DatasetSourceConfigMixin,
    TableauConnectionConfig,
    TableauPageSizeConfig,
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
        description="[deprecated] Use project_path_pattern instead. Filter for specific Tableau projects. For example, use 'My Project' to ingest a root-level Project with name 'My Project', or 'My Project/Nested Project' to ingest a nested Project with name 'Nested Project'. "
        "By default, all Projects nested inside a matching Project will be included in ingestion. "
        "You can both allow and deny projects based on their name using their name, or a Regex pattern. "
        "Deny patterns always take precedence over allow patterns. "
        "By default, all projects will be ingested.",
    )
    _deprecate_projects_pattern = pydantic_field_deprecated("project_pattern")

    project_path_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filters Tableau projects by their full path. For instance, 'My Project/Nested Project' targets a specific nested project named 'Nested Project'."
        " This is also useful when you need to exclude all nested projects under a particular project."
        " You can allow or deny projects by specifying their path or a regular expression pattern."
        " Deny patterns always override allow patterns."
        " By default, all projects are ingested.",
    )

    project_path_separator: str = Field(
        default="/",
        description="The separator used for the project_path_pattern field between project names. By default, we use a slash. "
        "You can change this if your Tableau projects contain slashes in their names, and you'd like to filter by project.",
    )

    default_schema_map: Dict[str, str] = Field(
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

    env: str = Field(
        default=builder.DEFAULT_ENV,
        description="Environment to use in namespace when constructing URNs.",
    )

    lineage_overrides: Optional[TableauLineageOverrides] = Field(
        default=None,
        description="Mappings to change generated dataset urns. Use only if you really know what you are doing.",
    )

    database_hostname_to_platform_instance_map: Optional[Dict[str, str]] = Field(
        default=None,
        description="Mappings to change platform instance in generated dataset urns based on database. Use only if you really know what you are doing.",
    )

    extract_usage_stats: bool = Field(
        default=False,
        description="[experimental] Extract usage statistics for dashboards and charts.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description=""
    )

    ingest_embed_url: Optional[bool] = Field(
        default=False,
        description="Ingest a URL to render an embedded Preview of assets within Tableau.",
    )

    ingest_external_links_for_dashboards: Optional[bool] = Field(
        default=True,
        description="Ingest a URL to link out to from dashboards.",
    )

    ingest_external_links_for_charts: Optional[bool] = Field(
        default=True,
        description="Ingest a URL to link out to from charts.",
    )

    extract_project_hierarchy: bool = Field(
        default=True,
        description="Whether to extract entire project hierarchy for nested projects.",
    )

    extract_lineage_from_unsupported_custom_sql_queries: bool = Field(
        default=False,
        description="[Experimental] Whether to extract lineage from unsupported custom sql queries using SQL parsing",
    )

    force_extraction_of_lineage_from_custom_sql_queries: bool = Field(
        default=False,
        description="[Experimental] Force extraction of lineage from custom sql queries using SQL parsing, ignoring Tableau metadata",
    )

    sql_parsing_disable_schema_awareness: bool = Field(
        default=False,
        description="[Experimental] Ignore pre ingested tables schemas during parsing of SQL queries "
        "(allows to workaround ingestion errors when pre ingested schema and queries are out of sync)",
    )

    ingest_multiple_sites: bool = Field(
        False,
        description="When enabled, ingests multiple sites the user has access to. If the user doesn't have access to the default site, specify an initial site to query in the site property. By default all sites the user has access to will be ingested. You can filter sites with the site_name_pattern property. This flag is currently only supported for Tableau Server. Tableau Cloud is not supported.",
    )

    site_name_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filter for specific Tableau sites. "
        "By default, all sites will be included in the ingestion. "
        "You can both allow and deny sites based on their name using their name, or a Regex pattern. "
        "Deny patterns always take precedence over allow patterns. "
        "This property is currently only supported for Tableau Server. Tableau Cloud is not supported. ",
    )

    add_site_container: bool = Field(
        False,
        description="When enabled, sites are added as containers and therefore visible in the folder structure within Datahub.",
    )

    permission_ingestion: Optional[PermissionIngestionConfig] = Field(
        default=None,
        description="Configuration settings for ingesting Tableau groups and their capabilities as custom properties.",
    )

    ingest_hidden_assets: bool = Field(
        True,
        description="When enabled, hidden views and dashboards are ingested into Datahub. "
        "If a dashboard or view is hidden in Tableau the luid is blank. Default of this config field is True.",
    )

    tags_for_hidden_assets: List[str] = Field(
        default=[],
        description="Tags to be added to hidden dashboards and views. If a dashboard or view is hidden in Tableau the luid is blank. "
        "This can only be used with ingest_tags enabled as it will overwrite tags entered from the UI.",
    )

    _fetch_size = pydantic_removed_field(
        "fetch_size",
    )

    # pre = True because we want to take some decision before pydantic initialize the configuration to default values
    @root_validator(pre=True)
    def projects_backward_compatibility(cls, values: Dict) -> Dict:
        projects = values.get("projects")
        project_pattern = values.get("project_pattern")
        project_path_pattern = values.get("project_path_pattern")
        if project_pattern is None and project_path_pattern is None and projects:
            logger.warning(
                "projects is deprecated, please use project_path_pattern instead."
            )
            logger.info("Initializing project_pattern from projects")
            values["project_pattern"] = AllowDenyPattern(
                allow=[f"^{prj}$" for prj in projects]
            )
        elif (project_pattern or project_path_pattern) and projects:
            raise ValueError(
                "projects is deprecated. Please use project_path_pattern only."
            )
        elif project_path_pattern and project_pattern:
            raise ValueError(
                "project_pattern is deprecated. Please use project_path_pattern only."
            )

        return values

    @root_validator()
    def validate_config_values(cls, values: Dict) -> Dict:
        tags_for_hidden_assets = values.get("tags_for_hidden_assets")
        ingest_tags = values.get("ingest_tags")
        if (
            not ingest_tags
            and tags_for_hidden_assets
            and len(tags_for_hidden_assets) > 0
        ):
            raise ValueError(
                "tags_for_hidden_assets is only allowed with ingest_tags enabled. Be aware that this will overwrite tags entered from the UI."
            )
        return values


class WorkbookKey(ContainerKey):
    workbook_id: str


class ProjectKey(ContainerKey):
    project_id: str


class SiteKey(ContainerKey):
    site_id: str


@dataclass
class UsageStat:
    view_count: int


@dataclass
class TableauProject:
    id: str
    name: str
    description: Optional[str]
    parent_id: Optional[str]
    parent_name: Optional[str]  # Name of a parent project
    path: List[str]


@dataclass
class DatabaseTable:
    """
    Corresponds to unique database table identified by DataHub.

    It is possible for logically table to be represented by different instances of [databaseTable]
    (https://help.tableau.com/current/api/metadata_api/en-us/reference/databasetable.doc.html)
    if Tableau can not identify them as same table (e.g. in Custom SQLs). Assuming that, all such
    instances will generate same URN in DataHub, this class captures group all databaseTable instances,
    that the URN represents - all browse paths and id of most informative (with columns) instance.

    """

    urn: str
    id: Optional[str] = (
        None  # is not None only for tables that came from Tableau metadata
    )
    num_cols: Optional[int] = None

    paths: Optional[Set[str]] = (
        None  # maintains all browse paths encountered for this table
    )

    parsed_columns: Optional[Set[str]] = (
        None  # maintains all columns encountered for this table during parsing SQL queries
    )

    def update_table(
        self,
        id: Optional[str] = None,
        num_tbl_cols: Optional[int] = None,
        path: Optional[str] = None,
        parsed_columns: Optional[Set[str]] = None,
    ) -> None:
        if path:
            if self.paths:
                self.paths.add(path)
            else:
                self.paths = {path}

        # the new instance of table has columns information, prefer its id.
        if not self.num_cols and num_tbl_cols:
            self.id = id
            self.num_cols = num_tbl_cols

        if parsed_columns:
            if self.parsed_columns:
                self.parsed_columns.update(parsed_columns)
            else:
                self.parsed_columns = parsed_columns


@dataclass
class SiteIdContentUrl:
    site_id: str
    site_content_url: str


@dataclass
class TableauSourceReport(
    StaleEntityRemovalSourceReport,
    IngestionStageReport,
):
    get_all_datasources_query_failed: bool = False
    num_get_datasource_query_failures: int = 0
    num_datasource_field_skipped_no_name: int = 0
    num_csql_field_skipped_no_name: int = 0
    num_table_field_skipped_no_name: int = 0
    # timers
    extract_usage_stats_timer: Dict[str, float] = dataclass_field(
        default_factory=TopKDict
    )
    fetch_groups_timer: Dict[str, float] = dataclass_field(default_factory=TopKDict)
    populate_database_server_hostname_map_timer: Dict[str, float] = dataclass_field(
        default_factory=TopKDict
    )
    populate_projects_registry_timer: Dict[str, float] = dataclass_field(
        default_factory=TopKDict
    )
    emit_workbooks_timer: Dict[str, float] = dataclass_field(default_factory=TopKDict)
    emit_sheets_timer: Dict[str, float] = dataclass_field(default_factory=TopKDict)
    emit_dashboards_timer: Dict[str, float] = dataclass_field(default_factory=TopKDict)
    emit_embedded_datasources_timer: Dict[str, float] = dataclass_field(
        default_factory=TopKDict
    )
    emit_published_datasources_timer: Dict[str, float] = dataclass_field(
        default_factory=TopKDict
    )
    emit_custom_sql_datasources_timer: Dict[str, float] = dataclass_field(
        default_factory=TopKDict
    )
    emit_upstream_tables_timer: Dict[str, float] = dataclass_field(
        default_factory=TopKDict
    )
    # lineage
    num_tables_with_upstream_lineage: int = 0
    num_upstream_table_lineage: int = 0
    num_upstream_fine_grained_lineage: int = 0
    num_upstream_table_skipped_no_name: int = 0
    num_upstream_table_skipped_no_columns: int = 0
    num_upstream_table_failed_generate_reference: int = 0
    num_upstream_table_lineage_failed_parse_sql: int = 0
    num_upstream_fine_grained_lineage_failed_parse_sql: int = 0
    num_hidden_assets_skipped: int = 0
    logged_in_user: LossyList[UserInfo] = dataclass_field(default_factory=LossyList)

    last_authenticated_at: Optional[datetime] = None

    num_expected_tableau_metadata_queries: int = 0
    num_actual_tableau_metadata_queries: int = 0
    tableau_server_error_stats: Dict[str, int] = dataclass_field(
        default_factory=(lambda: defaultdict(int))
    )

    # Counters for tracking the number of queries made to get_connection_objects method
    # by connection type (static and short set of keys):
    # - num_queries_by_connection_type: total number of queries
    # - num_filter_queries_by_connection_type: number of paginated queries due to splitting query filters
    # - num_paginated_queries_by_connection_type: total number of queries due to Tableau pagination
    # These counters are useful to understand the impact of changing the page size.

    num_queries_by_connection_type: Dict[str, int] = dataclass_field(
        default_factory=(lambda: defaultdict(int))
    )
    num_filter_queries_by_connection_type: Dict[str, int] = dataclass_field(
        default_factory=(lambda: defaultdict(int))
    )
    num_paginated_queries_by_connection_type: Dict[str, int] = dataclass_field(
        default_factory=(lambda: defaultdict(int))
    )


def report_user_role(report: TableauSourceReport, server: Server) -> None:
    title: str = "Insufficient Permissions"
    message: str = "The user must have the `Site Administrator Explorer` role to perform metadata ingestion."
    try:
        # TableauSiteSource instance is per site, so each time we need to find-out user detail
        # the site-role might be different on another site
        logged_in_user: UserInfo = UserInfo.from_server(server=server)

        if not logged_in_user.has_site_administrator_explorer_privileges():
            report.warning(
                title=title,
                message=message,
                context=f"user-name={logged_in_user.user_name}, role={logged_in_user.site_role}, site_id={logged_in_user.site_id}",
            )

        report.logged_in_user.append(logged_in_user)

    except Exception as e:
        report.warning(
            title=title,
            message="Failed to verify the user's role. The user must have `Site Administrator Explorer` role.",
            context=f"{e}",
            exc=e,
        )


@platform_name("Tableau")
@config_class(TableauConfig)
@support_status(SupportStatus.CERTIFIED)
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
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, configure using `extract_column_level_lineage`",
)
class TableauSource(StatefulIngestionSourceBase, TestableSource):
    platform = "tableau"

    def __hash__(self):
        return id(self)

    def __init__(
        self,
        config: TableauConfig,
        ctx: PipelineContext,
    ):
        super().__init__(config, ctx)
        self.config: TableauConfig = config
        self.report: TableauSourceReport = TableauSourceReport()
        self.server: Optional[Server] = None
        self._authenticate(self.config.site)

    def _authenticate(self, site_content_url: str) -> None:
        try:
            logger.info(f"Authenticated to Tableau site: '{site_content_url}'")
            self.server = self.config.make_tableau_client(site_content_url)
            self.report.last_authenticated_at = datetime.now(timezone.utc)
            report_user_role(report=self.report, server=self.server)
        # Note that we're not catching ConfigurationError, since we want that to throw.
        except ValueError as e:
            self.report.failure(
                title="Tableau Login Error",
                message="Failed to authenticate with Tableau.",
                exc=e,
            )

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            source_config = TableauConfig.parse_obj_allow_extras(config_dict)

            server = source_config.make_tableau_client(source_config.site)

            test_report.basic_connectivity = CapabilityReport(capable=True)

            test_report.capability_report = check_user_role(
                logged_in_user=UserInfo.from_server(server=server)
            )

        except Exception as e:
            logger.warning(f"{e}", exc_info=e)
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    def get_report(self) -> TableauSourceReport:
        return self.report

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        if self.server is None or not self.server.is_signed_in():
            return
        try:
            if self.config.ingest_multiple_sites:
                for site in list(TSC.Pager(self.server.sites)):
                    if (
                        site.state != "Active"
                        or not self.config.site_name_pattern.allowed(site.name)
                    ):
                        logger.info(
                            f"Skip site '{site.name}' as it's excluded in site_name_pattern or inactive."
                        )
                        continue
                    self.server.auth.switch_site(site)
                    site_source = TableauSiteSource(
                        config=self.config,
                        ctx=self.ctx,
                        site=site,
                        report=self.report,
                        server=self.server,
                        platform=self.platform,
                    )
                    logger.info(f"Ingesting assets of site '{site.content_url}'.")
                    yield from site_source.ingest_tableau_site()
            else:
                site = None
                with self.report.report_exc(
                    title="Unable to fetch site details. Site hierarchy may be incomplete and external urls may be missing.",
                    message="This usually indicates missing permissions. Ensure that you have all necessary permissions.",
                    level=StructuredLogLevel.WARN,
                ):
                    site = self.server.sites.get_by_id(self.server.site_id)

                site_source = TableauSiteSource(
                    config=self.config,
                    ctx=self.ctx,
                    site=(
                        site
                        if site
                        else SiteIdContentUrl(
                            site_id=self.server.site_id,
                            site_content_url=self.config.site,
                        )
                    ),
                    report=self.report,
                    server=self.server,
                    platform=self.platform,
                )
                yield from site_source.ingest_tableau_site()

        except MetadataQueryException as md_exception:
            self.report.failure(
                title="Failed to Retrieve Tableau Metadata",
                message="Unable to retrieve metadata from tableau.",
                context=str(md_exception),
                exc=md_exception,
            )

    def close(self) -> None:
        try:
            if self.server is not None:
                self.server.auth.sign_out()
        except Exception as ex:
            logger.warning(
                "During graceful closing of Tableau source a sign-out call was tried but ended up with"
                " an Exception (%s). Continuing closing of the source",
                ex,
            )
            self.server = None
        super().close()


class TableauSiteSource:
    def __init__(
        self,
        config: TableauConfig,
        ctx: PipelineContext,
        site: Union[SiteItem, SiteIdContentUrl],
        report: TableauSourceReport,
        server: Server,
        platform: str,
    ):
        self.config: TableauConfig = config
        self.report = report
        self.server: Server = server
        self.ctx: PipelineContext = ctx
        self.platform = platform

        self.site: Optional[SiteItem] = None
        if isinstance(site, SiteItem):
            self.site = site
            assert site.id is not None, "Site ID is required"
            self.site_id = site.id
            self.site_content_url = site.content_url
        elif isinstance(site, SiteIdContentUrl):
            self.site = None
            self.site_id = site.site_id
            self.site_content_url = site.site_content_url
        else:
            raise AssertionError("site or site id+content_url pair is required")

        self.database_tables: Dict[str, DatabaseTable] = {}
        self.tableau_stat_registry: Dict[str, UsageStat] = {}
        self.tableau_project_registry: Dict[str, TableauProject] = {}
        self.workbook_project_map: Dict[str, str] = {}
        self.datasource_project_map: Dict[str, str] = {}

        self.group_map: Dict[str, GroupItem] = {}

        # This map keeps track of the database server connection hostnames.
        self.database_server_hostname_map: Dict[str, str] = {}
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

        report_user_role(report=report, server=server)

    @property
    def no_env_browse_prefix(self) -> str:
        # Prefix to use with browse path (v1)
        # This is for charts and dashboards.

        platform_with_instance = (
            f"{self.platform}/{self.config.platform_instance}"
            if self.config.platform_instance
            else self.platform
        )
        return f"/{platform_with_instance}{self.site_name_browse_path}"

    @property
    def site_name_browse_path(self) -> str:
        site_name_prefix = (
            self.site.name if self.site and self.config.add_site_container else ""
        )
        return f"/{site_name_prefix}" if site_name_prefix else ""

    @property
    def dataset_browse_prefix(self) -> str:
        # datasets also have the env in the browse path
        return f"/{self.config.env.lower()}{self.no_env_browse_prefix}"

    def _re_authenticate(self) -> None:
        logger.info(f"Re-authenticating to Tableau site '{self.site_content_url}'")
        # Sign-in again may not be enough because Tableau sometimes caches invalid sessions
        # so we need to recreate the Tableau Server object
        self.server = self.config.make_tableau_client(self.site_content_url)
        self.report.last_authenticated_at = datetime.now(timezone.utc)

    def _populate_usage_stat_registry(self) -> None:
        if self.server is None:
            return

        view: TSC.ViewItem
        for view in TSC.Pager(self.server.views, usage=True):
            if not view.id:
                continue
            self.tableau_stat_registry[view.id] = UsageStat(view_count=view.total_views)
        logger.info(f"Got Tableau stats for {len(self.tableau_stat_registry)} assets")
        logger.debug("Tableau stats %s", self.tableau_stat_registry)

    def _populate_database_server_hostname_map(self) -> None:
        def maybe_parse_hostname():
            # If the connection string is a URL instead of a hostname, parse it
            # and extract the hostname, otherwise just return the connection string.
            parsed_host_name = urlparse(server_connection).hostname
            if parsed_host_name:
                return parsed_host_name
            return server_connection

        for database_server in self.get_connection_objects(
            query=database_servers_graphql_query,
            connection_type=c.DATABASE_SERVERS_CONNECTION,
            page_size=self.config.effective_database_server_page_size,
        ):
            database_server_id = database_server.get(c.ID)
            server_connection = database_server.get(c.HOST_NAME)
            host_name = maybe_parse_hostname()
            if host_name:
                self.database_server_hostname_map[str(database_server_id)] = host_name

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
                    parent_name=None,
                    description=project.description,
                    path=[],
                )
            # Set parent project name
            for _project_id, project in all_project_map.items():
                if project.parent_id is None:
                    continue

                if project.parent_id in all_project_map:
                    project.parent_name = all_project_map[project.parent_id].name
                else:
                    self.report.warning(
                        title="Incomplete project hierarchy",
                        message="Project details missing. Child projects will be ingested without reference to their parent project. We generally need Site Administrator Explorer permissions to extract the complete project hierarchy.",
                        context=f"Missing {project.parent_id}, referenced by {project.id} {project.project_name}",
                    )
                    project.parent_id = None

            # Post-condition
            assert all(
                [
                    ((project.parent_id is None) == (project.parent_name is None))
                    and (
                        project.parent_id is None
                        or project.parent_id in all_project_map
                    )
                    for project in all_project_map.values()
                ]
            ), "Parent project id and name should be consistent"

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
        is_allowed: bool = (
            self.config.project_pattern.allowed(project.name)
            or self.config.project_pattern.allowed(self._get_project_path(project))
        ) and self.config.project_path_pattern.allowed(self._get_project_path(project))
        if is_allowed is False:
            logger.info(
                f"Project ({project.name}) is not allowed as per project_pattern or project_path_pattern"
            )
        return is_allowed

    def _is_denied_project(self, project: TableauProject) -> bool:
        """
        Why use an explicit denial check instead of the `AllowDenyPattern.allowed` method?

        Consider a scenario where a Tableau site contains four projects: A, B, C, and D, with the following hierarchical relationship:

        - **A**
          - **B** (Child of A)
          - **C** (Child of A)
        - **D**

        In this setup:

        - `project_pattern` is configured with `allow: ["A"]` and `deny: ["B"]`.
        - `extract_project_hierarchy` is set to `True`.

        The goal is to extract assets from project A and its children while explicitly denying the child project B.

        If we rely solely on the `project_pattern.allowed()` method, project C's assets will not be ingested.
        This happens because project C is not explicitly included in the `allow` list, nor is it part of the `deny` list.
        However, since `extract_project_hierarchy` is enabled, project C should ideally be included in the ingestion process unless explicitly denied.

        To address this, the function explicitly checks the deny regex to ensure that project C’s assets are ingested if it is not specifically denied in the deny list. This approach ensures that the hierarchy is respected while adhering to the configured allow/deny rules.
        """

        # Either project_pattern or project_path_pattern is set in a recipe
        # TableauConfig.projects_backward_compatibility ensures that at least one of these properties is configured.

        return self.config.project_pattern.denied(
            project.name
        ) or self.config.project_path_pattern.denied(self._get_project_path(project))

    def _init_tableau_project_registry(self, all_project_map: dict) -> None:
        list_of_skip_projects: List[TableauProject] = []
        projects_to_ingest = {}
        for project in all_project_map.values():
            # Skip project if it is not allowed
            logger.debug(f"Evaluating project pattern for {project.name}")
            if self._is_allowed_project(project) is False:
                list_of_skip_projects.append(project)
                logger.debug(f"Project {project.name} is skipped")
                continue
            logger.debug(f"Project {project.name} is added in project registry")
            projects_to_ingest[project.id] = project

        if self.config.extract_project_hierarchy is False:
            logger.debug(
                "Skipping project hierarchy processing as configuration extract_project_hierarchy is "
                "disabled"
            )
        else:
            logger.debug(
                "Reevaluating projects as extract_project_hierarchy is enabled"
            )

            for project in list_of_skip_projects:
                if (
                    project.parent_id in projects_to_ingest
                    and not self._is_denied_project(project)
                ):
                    logger.debug(
                        f"Project {project.name} is added in project registry as it's a child project and not explicitly denied in `deny` list"
                    )
                    projects_to_ingest[project.id] = project

        # We rely on automatic browse paths (v2) when creating containers. That's why we need to sort the projects here.
        # Otherwise, nested projects will not have the correct browse paths if not created in correct order / hierarchy.
        self.tableau_project_registry = OrderedDict(
            sorted(projects_to_ingest.items(), key=lambda item: len(item[1].path))
        )

    def _init_datasource_registry(self) -> None:
        if self.server is None:
            return

        try:
            for ds in TSC.Pager(self.server.datasources):
                if ds.project_id not in self.tableau_project_registry:
                    logger.debug(
                        f"project id ({ds.project_id}) of datasource {ds.name} is not present in project "
                        f"registry"
                    )
                    continue
                self.datasource_project_map[ds.id] = ds.project_id
        except Exception as e:
            self.report.get_all_datasources_query_failed = True
            self.report.warning(
                title="Unexpected Query Error",
                message="Get all datasources query failed due to error",
                exc=e,
            )

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

    def _populate_projects_registry(self) -> None:
        if self.server is None:
            logger.warning("server is None. Can not initialize the project registry")
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

    def get_data_platform_instance(self) -> DataPlatformInstanceClass:
        return DataPlatformInstanceClass(
            platform=builder.make_data_platform_urn(self.platform),
            instance=(
                builder.make_dataplatform_instance_urn(
                    self.platform, self.config.platform_instance
                )
                if self.config.platform_instance
                else None
            ),
        )

    def _is_hidden_view(self, dashboard_or_view: Dict) -> bool:
        # LUID is blank if the view is hidden in the workbook.
        # More info here: https://help.tableau.com/current/api/metadata_api/en-us/reference/view.doc.html
        return not dashboard_or_view.get(c.LUID)

    def get_connection_object_page(
        self,
        query: str,
        connection_type: str,
        query_filter: str,
        current_cursor: Optional[str],
        fetch_size: int,
        retry_on_auth_error: bool = True,
        retries_remaining: Optional[int] = None,
    ) -> Tuple[dict, Optional[str], int]:
        """
        `current_cursor:` Tableau Server executes the query and returns
            a set of records based on the specified fetch_size. It also provides a cursor that
            indicates the current position within the result set. In the next API call,
            you pass this cursor to retrieve the next batch of records,
            with each batch containing up to fetch_size records.
            Initial value is None.

        `fetch_size:` The number of records to retrieve from Tableau
            Server in a single API call, starting from the current cursor position on Tableau Server.
        """
        retries_remaining = retries_remaining or self.config.max_retries

        logger.debug(
            f"Query {connection_type} to get {fetch_size} objects with cursor {current_cursor}"
            f" and filter {query_filter}"
        )
        try:
            assert self.server is not None
            self.report.num_actual_tableau_metadata_queries += 1
            query_data = query_metadata_cursor_based_pagination(
                server=self.server,
                main_query=query,
                connection_name=connection_type,
                first=fetch_size,
                after=current_cursor,
                qry_filter=query_filter,
            )

        except REAUTHENTICATE_ERRORS as e:
            self.report.tableau_server_error_stats[e.__class__.__name__] += 1
            if not retry_on_auth_error or retries_remaining <= 0:
                raise

            # We have been getting some irregular authorization errors like below well before the expected expiry time
            # - within few seconds of initial authentication . We'll retry without re-auth for such cases.
            # <class 'tableauserverclient.server.endpoint.exceptions.NonXMLResponseError'>:
            # b'{"timestamp":"xxx","status":401,"error":"Unauthorized","path":"/relationship-service-war/graphql"}'
            if self.report.last_authenticated_at and (
                datetime.now(timezone.utc) - self.report.last_authenticated_at
                > REGULAR_AUTH_EXPIRY_PERIOD
            ):
                # If ingestion has been running for over 2 hours, the Tableau
                # temporary credentials will expire. If this happens, this exception
                # will be thrown, and we need to re-authenticate and retry.
                self._re_authenticate()

            return self.get_connection_object_page(
                query=query,
                connection_type=connection_type,
                query_filter=query_filter,
                fetch_size=fetch_size,
                current_cursor=current_cursor,
                retry_on_auth_error=True,
                retries_remaining=retries_remaining - 1,
            )

        except InternalServerError as ise:
            self.report.tableau_server_error_stats[InternalServerError.__name__] += 1
            # In some cases Tableau Server returns 504 error, which is a timeout error, so it worths to retry.
            # Extended with other retryable errors.
            if ise.code in RETRIABLE_ERROR_CODES:
                if retries_remaining <= 0:
                    raise ise
                logger.info(f"Retrying query due to error {ise.code}")
                return self.get_connection_object_page(
                    query=query,
                    connection_type=connection_type,
                    query_filter=query_filter,
                    fetch_size=fetch_size,
                    current_cursor=current_cursor,
                    retry_on_auth_error=True,
                    retries_remaining=retries_remaining - 1,
                )
            else:
                raise ise

        except OSError:
            self.report.tableau_server_error_stats[OSError.__name__] += 1
            # In tableauseverclient 0.26 (which was yanked and released in 0.28 on 2023-10-04),
            # the request logic was changed to use threads.
            # https://github.com/tableau/server-client-python/commit/307d8a20a30f32c1ce615cca7c6a78b9b9bff081
            # I'm not exactly sure why, but since then, we now occasionally see
            # `OSError: Response is not a http response?` for some requests. This
            # retry logic is basically a bandaid for that.
            if retries_remaining <= 0:
                raise
            return self.get_connection_object_page(
                query=query,
                connection_type=connection_type,
                query_filter=query_filter,
                fetch_size=fetch_size,
                current_cursor=current_cursor,
                retry_on_auth_error=True,
                retries_remaining=retries_remaining - 1,
            )

        if c.ERRORS in query_data:
            errors = query_data[c.ERRORS]
            if all(
                # The format of the error messages is highly unpredictable, so we have to
                # be extra defensive with our parsing.
                error and (error.get(c.EXTENSIONS) or {}).get(c.SEVERITY) == c.WARNING
                for error in errors
            ):
                # filter out PERMISSIONS_MODE_SWITCHED to report error in human-readable format
                other_errors = []
                permission_mode_errors = []
                node_limit_errors = []
                for error in errors:
                    if (
                        error.get("extensions")
                        and error["extensions"].get("code")
                        == "PERMISSIONS_MODE_SWITCHED"
                    ):
                        permission_mode_errors.append(error)
                    elif (
                        error.get("extensions")
                        and error["extensions"].get("code") == "NODE_LIMIT_EXCEEDED"
                    ):
                        node_limit_errors.append(error)
                    else:
                        other_errors.append(error)

                if other_errors:
                    self.report.warning(
                        message=f"Received error fetching Query Connection {connection_type}",
                        context=f"Errors: {other_errors}",
                    )

                if permission_mode_errors:
                    self.report.warning(
                        title="Derived Permission Error",
                        message="Turn on your derived permissions. See for details "
                        "https://community.tableau.com/s/question/0D54T00000QnjHbSAJ/how-to-fix-the"
                        "-permissionsmodeswitched-error",
                        context=f"{permission_mode_errors}",
                    )

                if node_limit_errors:
                    self.report.warning(
                        title="Tableau Data Exceed Predefined Limit",
                        message="The numbers of record in result set exceeds a predefined limit. Increase the tableau "
                        "configuration metadata query node limit to higher value. Refer "
                        "https://help.tableau.com/current/server/en-us/"
                        "cli_configuration-set_tsm.htm#metadata_nodelimit",
                        context=f"""{{
                                "errors": {node_limit_errors},
                                "connection_type": {connection_type},
                                "query_filter": {query_filter},
                                "query": {query},
                        }}""",
                    )
            else:
                # As of Tableau Server 2024.2, the metadata API sporadically returns a 30-second
                # timeout error.
                # It doesn't reliably happen, so retrying a couple of times makes sense.
                if all(
                    error.get("message")
                    == "Execution canceled because timeout of 30000 millis was reached"
                    for error in errors
                ):
                    # If it was only a timeout error, we can retry.
                    if retries_remaining <= 0:
                        raise

                    # This is a pretty dumb backoff mechanism, but it's good enough for now.
                    backoff_time = min(
                        (self.config.max_retries - retries_remaining + 1) ** 2, 60
                    )
                    logger.info(
                        f"Query {connection_type} received a 30 second timeout error - will retry in {backoff_time} seconds. "
                        f"Retries remaining: {retries_remaining}"
                    )
                    time.sleep(backoff_time)
                    return self.get_connection_object_page(
                        query=query,
                        connection_type=connection_type,
                        query_filter=query_filter,
                        fetch_size=fetch_size,
                        current_cursor=current_cursor,
                        retry_on_auth_error=True,
                        retries_remaining=retries_remaining,
                    )
                raise RuntimeError(f"Query {connection_type} error: {errors}")

        connection_object = query_data.get(c.DATA, {}).get(connection_type, {})

        has_next_page = connection_object.get(c.PAGE_INFO, {}).get(
            c.HAS_NEXT_PAGE, False
        )

        next_cursor = connection_object.get(c.PAGE_INFO, {}).get(
            "endCursor",
            None,
        )

        return connection_object, next_cursor, has_next_page

    def get_connection_objects(
        self,
        query: str,
        connection_type: str,
        page_size: int,
        query_filter: dict = {},
    ) -> Iterable[dict]:
        query_filter = optimize_query_filter(query_filter)

        # Calls the get_connection_object_page function to get the objects,
        # and automatically handles pagination.

        filter_pages = get_filter_pages(query_filter, page_size)
        self.report.num_queries_by_connection_type[connection_type] += 1
        self.report.num_filter_queries_by_connection_type[connection_type] += len(
            filter_pages
        )

        for filter_page in filter_pages:
            has_next_page = 1
            current_cursor: Optional[str] = None
            while has_next_page:
                filter_: str = make_filter(filter_page)

                self.report.num_paginated_queries_by_connection_type[
                    connection_type
                ] += 1

                self.report.num_expected_tableau_metadata_queries += 1
                (
                    connection_objects,
                    current_cursor,
                    has_next_page,
                ) = self.get_connection_object_page(
                    query=query,
                    connection_type=connection_type,
                    query_filter=filter_,
                    current_cursor=current_cursor,
                    # `filter_page` contains metadata object IDs (e.g., Project IDs, Field IDs, Sheet IDs, etc.).
                    # The number of IDs is always less than or equal to page_size.
                    # If the IDs are primary keys, the number of metadata objects to load matches the number of records to return.
                    # In our case, mostly, the IDs are primary key, therefore, fetch_size is set equal to page_size.
                    fetch_size=page_size,
                )

                yield from connection_objects.get(c.NODES) or []

    def emit_workbooks(self) -> Iterable[MetadataWorkUnit]:
        if self.tableau_project_registry:
            project_names: List[str] = [
                project.name for project in self.tableau_project_registry.values()
            ]
            projects = {c.PROJECT_NAME_WITH_IN: project_names}

            for workbook in self.get_connection_objects(
                query=workbook_graphql_query,
                connection_type=c.WORKBOOKS_CONNECTION,
                query_filter=projects,
                page_size=self.config.effective_workbook_page_size,
            ):
                # This check is needed as we are using projectNameWithin which return project as per project name so if
                # user want to ingest only nested project C from A->B->C then tableau might return more than one Project
                # if multiple project has name C. Ideal solution is to use projectLuidWithin to avoid duplicate project,
                # however Tableau supports projectLuidWithin in Tableau Cloud June 2022 / Server 2022.3 and later.
                project_luid: Optional[str] = self._get_workbook_project_luid(workbook)
                if project_luid not in self.tableau_project_registry.keys():
                    wrk_name: Optional[str] = workbook.get(c.NAME)
                    wrk_id: Optional[str] = workbook.get(c.ID)
                    prj_name: Optional[str] = workbook.get(c.PROJECT_NAME)

                    self.report.warning(
                        title="Skipping Missing Workbook",
                        message="Skipping workbook as its project is not present in project registry",
                        context=f"workbook={wrk_name}({wrk_id}), project={prj_name}({project_luid})",
                    )
                    continue

                yield from self.emit_workbook_as_container(workbook)

                for sheet in workbook.get(c.SHEETS, []):
                    self.sheet_ids.append(sheet[c.ID])

                for dashboard in workbook.get(c.DASHBOARDS, []):
                    self.dashboard_ids.append(dashboard[c.ID])

                for ds in workbook.get(c.EMBEDDED_DATA_SOURCES, []):
                    self.embedded_datasource_ids_being_used.append(ds[c.ID])

    def _track_custom_sql_ids(self, field: dict) -> None:
        # Tableau shows custom sql datasource as a table in ColumnField's upstreamColumns.
        for column in field.get(c.UPSTREAM_COLUMNS, []):
            table_id = (
                column.get(c.TABLE, {}).get(c.ID)
                if column.get(c.TABLE)
                and column[c.TABLE][c.TYPE_NAME] == c.CUSTOM_SQL_TABLE
                else None
            )

            if table_id is not None and table_id not in self.custom_sql_ids_being_used:
                self.custom_sql_ids_being_used.append(table_id)

    def _create_upstream_table_lineage(
        self,
        datasource: dict,
        browse_path: Optional[str],
        is_embedded_ds: bool = False,
    ) -> Tuple[List[Upstream], List[FineGrainedLineage]]:
        upstream_tables: List[Upstream] = []
        fine_grained_lineages: List[FineGrainedLineage] = []
        table_id_to_urn = {}

        upstream_datasources = self.get_upstream_datasources(datasource)
        upstream_tables.extend(upstream_datasources)

        # When tableau workbook connects to published datasource, it creates an embedded
        # datasource inside workbook that connects to published datasource. Both embedded
        # and published datasource have same upstreamTables in this case.
        if upstream_tables and is_embedded_ds:
            logger.debug(
                f"Embedded datasource {datasource.get(c.ID)} has upstreamDatasources.\
                Setting only upstreamDatasources lineage. The upstreamTables lineage \
                    will be set via upstream published datasource."
            )
        else:
            # This adds an edge to upstream DatabaseTables using `upstreamTables`
            upstreams, id_to_urn = self.get_upstream_tables(
                datasource.get(c.UPSTREAM_TABLES, []),
                datasource.get(c.NAME),
                browse_path,
                is_custom_sql=False,
            )
            upstream_tables.extend(upstreams)
            table_id_to_urn.update(id_to_urn)

            # This adds an edge to upstream CustomSQLTables using `fields`.`upstreamColumns`.`table`
            csql_upstreams, csql_id_to_urn = self.get_upstream_csql_tables(
                datasource.get(c.FIELDS) or [],
            )
            upstream_tables.extend(csql_upstreams)
            table_id_to_urn.update(csql_id_to_urn)

        logger.debug(
            f"A total of {len(upstream_tables)} upstream table edges found for datasource {datasource[c.ID]}"
        )

        datasource_urn = builder.make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=datasource[c.ID],
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        if not upstream_tables:
            # Tableau's metadata graphql API sometimes returns an empty list for upstreamTables
            # for embedded datasources. However, the upstreamColumns field often includes information.
            # This attempts to populate upstream table information from the upstreamColumns field.
            table_id_to_urn = {
                column[c.TABLE][c.ID]: builder.make_dataset_urn_with_platform_instance(
                    self.platform,
                    column[c.TABLE][c.ID],
                    self.config.platform_instance,
                    self.config.env,
                )
                for field in datasource.get(c.FIELDS, [])
                for column in field.get(c.UPSTREAM_COLUMNS, [])
                if column.get(c.TABLE, {}).get(c.TYPE_NAME) == c.CUSTOM_SQL_TABLE
                and column.get(c.TABLE, {}).get(c.ID)
            }
            fine_grained_lineages = self.get_upstream_columns_of_fields_in_datasource(
                datasource, datasource_urn, table_id_to_urn
            )
            upstream_tables = [
                Upstream(dataset=table_urn, type=DatasetLineageType.TRANSFORMED)
                for table_urn in table_id_to_urn.values()
            ]

        if datasource.get(c.FIELDS):
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
                    f"A total of {len(fine_grained_lineages)} upstream column edges found for datasource {datasource[c.ID]}"
                )

        return upstream_tables, fine_grained_lineages

    def get_upstream_datasources(self, datasource: dict) -> List[Upstream]:
        upstream_tables = []
        for ds in datasource.get(c.UPSTREAM_DATA_SOURCES, []):
            if ds[c.ID] not in self.datasource_ids_being_used:
                self.datasource_ids_being_used.append(ds[c.ID])

            upstream_ds_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=ds[c.ID],
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )
            upstream_table = Upstream(
                dataset=upstream_ds_urn,
                type=DatasetLineageType.TRANSFORMED,
            )
            upstream_tables.append(upstream_table)
        return upstream_tables

    def get_upstream_csql_tables(
        self, fields: List[dict]
    ) -> Tuple[List[Upstream], Dict[str, str]]:
        upstream_csql_urns = set()
        csql_id_to_urn = {}

        for field in fields:
            if not field.get(c.UPSTREAM_COLUMNS):
                continue
            for upstream_col in field[c.UPSTREAM_COLUMNS]:
                if (
                    upstream_col
                    and upstream_col.get(c.TABLE)
                    and upstream_col.get(c.TABLE)[c.TYPE_NAME] == c.CUSTOM_SQL_TABLE
                ):
                    upstream_table_id = upstream_col.get(c.TABLE)[c.ID]

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

    def get_upstream_tables(
        self,
        tables: List[dict],
        datasource_name: Optional[str],
        browse_path: Optional[str],
        is_custom_sql: bool,
    ) -> Tuple[List[Upstream], Dict[str, str]]:
        upstream_tables = []
        # Same table urn can be used when setting fine grained lineage,
        table_id_to_urn: Dict[str, str] = {}
        for table in tables:
            # skip upstream tables when there is no column info when retrieving datasource
            # Lineage and Schema details for these will be taken care in self.emit_custom_sql_datasources()
            num_tbl_cols: Optional[int] = table.get(c.COLUMNS_CONNECTION) and table[
                c.COLUMNS_CONNECTION
            ].get("totalCount")
            if not is_custom_sql and not num_tbl_cols:
                self.report.num_upstream_table_skipped_no_columns += 1
                logger.warning(
                    f"Skipping upstream table with id {table[c.ID]}, no columns: {table}"
                )
                continue
            elif table[c.NAME] is None:
                self.report.num_upstream_table_skipped_no_name += 1
                logger.warning(
                    f"Skipping upstream table {table[c.ID]} from lineage since its name is none: {table}"
                )
                continue

            try:
                ref = TableauUpstreamReference.create(
                    table, default_schema_map=self.config.default_schema_map
                )
            except Exception as e:
                self.report.num_upstream_table_failed_generate_reference += 1
                self.report.warning(
                    title="Potentially Missing Lineage Issue",
                    message="Failed to generate upstream reference",
                    exc=e,
                    context=f"table={table}",
                )
                continue

            table_urn = ref.make_dataset_urn(
                self.config.env,
                self.config.platform_instance_map,
                self.config.lineage_overrides,
                self.config.database_hostname_to_platform_instance_map,
                self.database_server_hostname_map,
            )
            table_id_to_urn[table[c.ID]] = table_urn

            upstream_table = Upstream(
                dataset=table_urn,
                type=DatasetLineageType.TRANSFORMED,
            )
            upstream_tables.append(upstream_table)

            table_path: Optional[str] = None
            if browse_path and datasource_name:
                table_path = f"{browse_path}/{datasource_name}"

            if table_urn not in self.database_tables:
                self.database_tables[table_urn] = DatabaseTable(
                    urn=table_urn,
                    id=table[c.ID],
                    num_cols=num_tbl_cols,
                    paths={table_path} if table_path else set(),
                )
            else:
                self.database_tables[table_urn].update_table(
                    table[c.ID], num_tbl_cols, table_path
                )

        return upstream_tables, table_id_to_urn

    def get_upstream_columns_of_fields_in_datasource(
        self,
        datasource: dict,
        datasource_urn: str,
        table_id_to_urn: Dict[str, str],
    ) -> List[FineGrainedLineage]:
        fine_grained_lineages = []
        for field in datasource.get(c.FIELDS) or []:
            field_name = field.get(c.NAME)
            # upstreamColumns lineage will be set via upstreamFields.
            # such as for CalculatedField
            if (
                not field_name
                or not field.get(c.UPSTREAM_COLUMNS)
                or field.get(c.UPSTREAM_FIELDS)
            ):
                continue
            input_columns = []
            for upstream_col in field.get(c.UPSTREAM_COLUMNS):
                if not upstream_col:
                    continue
                name = upstream_col.get(c.NAME)
                upstream_table_id = (
                    upstream_col.get(c.TABLE)[c.ID]
                    if upstream_col.get(c.TABLE)
                    else None
                )
                if (
                    name
                    and upstream_table_id
                    and upstream_table_id in table_id_to_urn.keys()
                ):
                    parent_dataset_urn = table_id_to_urn[upstream_table_id]
                    if (
                        self.is_snowflake_urn(parent_dataset_urn)
                        and not self.config.ingest_tables_external
                    ):
                        # This is required for column level lineage to work correctly as
                        # DataHub Snowflake source lowercases all field names in the schema.
                        #
                        # It should not be done if snowflake tables are not pre ingested but
                        # parsed from SQL queries or ingested from Tableau metadata (in this case
                        # it just breaks case sensitive table level linage)
                        name = name.lower()
                    input_columns.append(
                        builder.make_schema_field_urn(
                            parent_urn=parent_dataset_urn,
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

    def is_snowflake_urn(self, urn: str) -> bool:
        return (
            DatasetUrn.from_string(urn).get_data_platform_urn().platform_name
            == "snowflake"
        )

    def get_upstream_fields_of_field_in_datasource(
        self, datasource: dict, datasource_urn: str
    ) -> List[FineGrainedLineage]:
        fine_grained_lineages = []
        for field in datasource.get(c.FIELDS) or []:
            field_name = field.get(c.NAME)
            # It is observed that upstreamFields gives one-hop field
            # lineage, and not multi-hop field lineage
            # This behavior is as desired in our case.
            if not field_name or not field.get(c.UPSTREAM_FIELDS):
                continue
            input_fields = []
            for upstream_field in field.get(c.UPSTREAM_FIELDS):
                if not upstream_field:
                    continue
                name = upstream_field.get(c.NAME)
                upstream_ds_id = (
                    upstream_field.get(c.DATA_SOURCE)[c.ID]
                    if upstream_field.get(c.DATA_SOURCE)
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

    def get_upstream_fields_from_custom_sql(
        self, datasource: dict, datasource_urn: str
    ) -> List[FineGrainedLineage]:
        parsed_result = self.parse_custom_sql(
            datasource=datasource,
            datasource_urn=datasource_urn,
            env=self.config.env,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            func_overridden_info=None,  # Here we don't want to override any information from configuration
        )

        if parsed_result is None or parsed_result.debug_info.error:
            return []

        cll: List[ColumnLineageInfo] = (
            parsed_result.column_lineage
            if parsed_result.column_lineage is not None
            else []
        )

        fine_grained_lineages: List[FineGrainedLineage] = []
        for cll_info in cll:
            downstream = (
                [
                    builder.make_schema_field_urn(
                        datasource_urn, cll_info.downstream.column
                    )
                ]
                if cll_info.downstream is not None
                and cll_info.downstream.column is not None
                else []
            )
            upstreams = [
                builder.make_schema_field_urn(column_ref.table, column_ref.column)
                for column_ref in cll_info.upstreams
            ]
            fine_grained_lineages.append(
                FineGrainedLineage(
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=downstream,
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=upstreams,
                )
            )

        return fine_grained_lineages

    def get_transform_operation(self, field: dict) -> str:
        field_type = field[c.TYPE_NAME]
        if field_type in (
            c.DATA_SOURCE_FIELD,
            c.COLUMN_FIELD,
        ):
            op = c.IDENTITY  # How to specify exact same
        elif field_type == c.CALCULATED_FIELD:
            op = field_type
            if field.get(c.FORMULA):
                op += f"formula: {field.get(c.FORMULA)}"
        else:
            op = field_type  # BinField, CombinedField, etc

        return op

    def emit_custom_sql_datasources(self) -> Iterable[MetadataWorkUnit]:
        custom_sql_filter = {c.ID_WITH_IN: self.custom_sql_ids_being_used}

        custom_sql_connection = list(
            self.get_connection_objects(
                query=custom_sql_graphql_query,
                connection_type=c.CUSTOM_SQL_TABLE_CONNECTION,
                query_filter=custom_sql_filter,
                page_size=self.config.effective_custom_sql_table_page_size,
            )
        )

        unique_custom_sql = get_unique_custom_sql(custom_sql_connection)

        for csql in unique_custom_sql:
            csql_id: str = csql[c.ID]
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
            columns: List[Dict[Any, Any]] = []
            if len(csql[c.DATA_SOURCES]) > 0:
                # CustomSQLTable id owned by exactly one tableau data source
                logger.debug(
                    f"Number of datasources referencing CustomSQLTable: {len(csql[c.DATA_SOURCES])}"
                )

                datasource = csql[c.DATA_SOURCES][0]
                datasource_name = datasource.get(c.NAME)
                if datasource.get(
                    c.TYPE_NAME
                ) == c.EMBEDDED_DATA_SOURCE and datasource.get(c.WORKBOOK):
                    workbook = datasource.get(c.WORKBOOK)
                    datasource_name = (
                        f"{workbook.get(c.NAME)}/{datasource_name}"
                        if datasource_name and workbook.get(c.NAME)
                        else None
                    )
                    logger.debug(
                        f"Adding datasource {datasource_name}({datasource.get('id')}) to workbook container"
                    )
                    yield from add_entity_to_container(
                        self.gen_workbook_key(workbook[c.ID]),
                        c.DATASET,
                        dataset_snapshot.urn,
                    )
                else:
                    project_luid = self._get_datasource_project_luid(datasource)
                    if project_luid:
                        logger.debug(
                            f"Adding datasource {datasource_name}({datasource.get('id')}) to project {project_luid} container"
                        )
                        # TODO: Technically, we should have another layer of hierarchy with the datasource name here.
                        # Same with the workbook name above. However, in practice most projects/workbooks have a single
                        # datasource, so the extra nesting just gets in the way.
                        yield from add_entity_to_container(
                            self.gen_project_key(project_luid),
                            c.DATASET,
                            dataset_snapshot.urn,
                        )
                    else:
                        logger.debug(
                            f"Datasource {datasource_name}({datasource.get('id')}) project_luid not found"
                        )

                project = self._get_project_browse_path_name(datasource)

                # if condition is needed as graphQL return "columns": None
                columns = (
                    cast(List[Dict[Any, Any]], csql.get(c.COLUMNS))
                    if c.COLUMNS in csql and csql.get(c.COLUMNS) is not None
                    else []
                )

                # The Tableau SQL parser much worse than our sqlglot based parser,
                # so relying on metadata parsed by Tableau from SQL queries can be
                # less accurate. This option allows us to ignore Tableau's parser and
                # only use our own.
                if self.config.force_extraction_of_lineage_from_custom_sql_queries:
                    logger.debug("Extracting TLL & CLL from custom sql (forced)")
                    yield from self._create_lineage_from_unsupported_csql(
                        csql_urn, csql, columns
                    )
                else:
                    tables = csql.get(c.TABLES, [])

                    if tables:
                        # lineage from custom sql -> datasets/tables #
                        yield from self._create_lineage_to_upstream_tables(
                            csql_urn, tables, datasource
                        )
                    elif (
                        self.config.extract_lineage_from_unsupported_custom_sql_queries
                    ):
                        logger.debug("Extracting TLL & CLL from custom sql")
                        # custom sql tables may contain unsupported sql, causing incomplete lineage
                        # we extract the lineage from the raw queries
                        yield from self._create_lineage_from_unsupported_csql(
                            csql_urn, csql, columns
                        )

            #  Schema Metadata
            schema_metadata = self.get_schema_metadata_for_custom_sql(columns)
            if schema_metadata is not None:
                dataset_snapshot.aspects.append(schema_metadata)

            # Browse path
            if project and datasource_name:
                browse_paths = BrowsePathsClass(
                    paths=[f"{self.dataset_browse_prefix}/{project}/{datasource_name}"]
                )
                dataset_snapshot.aspects.append(browse_paths)
            else:
                logger.debug(f"Browse path not set for Custom SQL table {csql_id}")
                logger.warning(
                    f"Skipping Custom SQL table {csql_id} due to filtered downstream"
                )
                continue

            dataset_properties = DatasetPropertiesClass(
                name=csql.get(c.NAME),
                description=csql.get(c.DESCRIPTION),
            )

            dataset_snapshot.aspects.append(dataset_properties)

            if csql.get(c.QUERY):
                view_properties = ViewPropertiesClass(
                    materialized=False,
                    viewLanguage=c.SQL,
                    viewLogic=clean_query(csql[c.QUERY]),
                )
                dataset_snapshot.aspects.append(view_properties)

            yield self.get_metadata_change_event(dataset_snapshot)
            yield self.get_metadata_change_proposal(
                dataset_snapshot.urn,
                aspect_name=c.SUB_TYPES,
                aspect=SubTypesClass(typeNames=[DatasetSubTypes.VIEW, c.CUSTOM_SQL]),
            )

    def get_schema_metadata_for_custom_sql(
        self, columns: List[dict]
    ) -> Optional[SchemaMetadata]:
        fields = []
        schema_metadata = None
        for field in columns:
            # Datasource fields

            if field.get(c.NAME) is None:
                self.report.num_csql_field_skipped_no_name += 1
                logger.warning(
                    f"Skipping field {field[c.ID]} from schema since its name is none"
                )
                continue
            nativeDataType = field.get(c.REMOTE_TYPE, c.UNKNOWN)
            TypeClass = FIELD_TYPE_MAPPING.get(nativeDataType, NullTypeClass)
            schema_field = SchemaField(
                fieldPath=field[c.NAME],
                type=SchemaFieldDataType(type=TypeClass()),
                nativeDataType=nativeDataType,
                description=field.get(c.DESCRIPTION),
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

    def _get_published_datasource_project_luid(self, ds: dict) -> Optional[str]:
        # This is fallback in case "get all datasources" query fails for some reason.
        # It is possible due to https://github.com/tableau/server-client-python/issues/1210
        if (
            ds.get(c.LUID)
            and ds[c.LUID] not in self.datasource_project_map.keys()
            and self.report.get_all_datasources_query_failed
        ):
            logger.debug(
                f"published datasource {ds.get(c.NAME)} project_luid not found."
                f" Running get datasource query for {ds[c.LUID]}"
            )
            # Query and update self.datasource_project_map with luid
            self._query_published_datasource_for_project_luid(ds[c.LUID])

        if (
            ds.get(c.LUID)
            and ds[c.LUID] in self.datasource_project_map.keys()
            and self.datasource_project_map[ds[c.LUID]] in self.tableau_project_registry
        ):
            return self.datasource_project_map[ds[c.LUID]]

        logger.debug(f"published datasource {ds.get(c.NAME)} project_luid not found")

        return None

    def _query_published_datasource_for_project_luid(self, ds_luid: str) -> None:
        if self.server is None:
            return

        try:
            ds_result = self.server.datasources.get_by_id(ds_luid)
            if ds_result.project_id not in self.tableau_project_registry:
                logger.debug(
                    f"project id ({ds_result.project_id}) of datasource {ds_result.name} is not present in project "
                    f"registry"
                )
            else:
                self.datasource_project_map[ds_result.id] = ds_result.project_id
        except Exception as e:
            self.report.num_get_datasource_query_failures += 1
            self.report.warning(
                title="Unexpected Query Error",
                message="Failed to get datasource details",
                exc=e,
                context=f"ds_luid={ds_luid}",
            )

    def _get_workbook_project_luid(self, wb: dict) -> Optional[str]:
        if wb.get(c.LUID) and self.workbook_project_map.get(wb[c.LUID]):
            return self.workbook_project_map[wb[c.LUID]]

        logger.debug(f"workbook {wb.get(c.NAME)} project_luid not found")

        return None

    def _get_embedded_datasource_project_luid(self, ds: dict) -> Optional[str]:
        if ds.get(c.WORKBOOK):
            project_luid: Optional[str] = self._get_workbook_project_luid(
                ds[c.WORKBOOK]
            )

            if project_luid and project_luid in self.tableau_project_registry:
                return project_luid

        logger.debug(f"embedded datasource {ds.get(c.NAME)} project_luid not found")

        return None

    def _get_datasource_project_luid(self, ds: dict) -> Optional[str]:
        # Only published and embedded data-sources are supported
        ds_type: Optional[str] = ds.get(c.TYPE_NAME)
        if ds_type not in (
            c.PUBLISHED_DATA_SOURCE,
            c.EMBEDDED_DATA_SOURCE,
        ):
            logger.debug(
                f"datasource {ds.get(c.NAME)} type {ds.get(c.TYPE_NAME)} is unsupported"
            )
            return None

        func_selector: Any = {
            c.PUBLISHED_DATA_SOURCE: self._get_published_datasource_project_luid,
            c.EMBEDDED_DATA_SOURCE: self._get_embedded_datasource_project_luid,
        }

        return func_selector[ds_type](ds)

    @staticmethod
    def _get_datasource_project_name(ds: dict) -> Optional[str]:
        if ds.get(c.TYPE_NAME) == c.EMBEDDED_DATA_SOURCE and ds.get(c.WORKBOOK):
            return ds[c.WORKBOOK].get(c.PROJECT_NAME)
        if ds.get(c.TYPE_NAME) == c.PUBLISHED_DATA_SOURCE:
            return ds.get(c.PROJECT_NAME)
        return None

    def _get_project_browse_path_name(self, ds: dict) -> Optional[str]:
        if self.config.extract_project_hierarchy is False:
            # backward compatibility. Just return the name of datasource project
            return self._get_datasource_project_name(ds)

        # form path as per nested project structure
        project_luid = self._get_datasource_project_luid(ds)
        if project_luid is None:
            logger.warning(
                f"Could not load project hierarchy for datasource {ds.get(c.NAME)}. Please check permissions."
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
            datasource.get(c.NAME) or "",
            self._get_project_browse_path_name(datasource),
            is_custom_sql=True,
        )

        if upstream_tables:
            upstream_lineage = UpstreamLineage(upstreams=upstream_tables)
            yield self.get_metadata_change_proposal(
                csql_urn,
                aspect_name=c.UPSTREAM_LINEAGE,
                aspect=upstream_lineage,
            )
            self.report.num_tables_with_upstream_lineage += 1
            self.report.num_upstream_table_lineage += len(upstream_tables)

    @staticmethod
    def _clean_tableau_query_parameters(query: str) -> str:
        if not query:
            return query

        #
        # It replaces all following occurrences by 1
        # which is enough to fix syntax of SQL query
        # and make sqlglot parser happy:
        #
        #   <[Parameters].[SomeParameterName]>
        #   <Parameters.[SomeParameterName]>
        #   <[Parameters].SomeParameterName>
        #   <[Parameters].SomeParameter Name>
        #   <Parameters.SomeParameterName>
        #
        # After, it unescapes (Tableau escapes it)
        #   >> to >
        #   << to <
        #
        return (
            re.sub(r"\<\[?[Pp]arameters\]?\.(\[[^\]]+\]|[^\>]+)\>", "1", query)
            .replace("<<", "<")
            .replace(">>", ">")
            .replace("\n\n", "\n")
        )

    def parse_custom_sql(
        self,
        datasource: dict,
        datasource_urn: str,
        platform: str,
        env: str,
        platform_instance: Optional[str],
        func_overridden_info: Optional[
            Callable[
                [
                    str,
                    Optional[str],
                    Optional[str],
                    Optional[Dict[str, str]],
                    Optional[TableauLineageOverrides],
                    Optional[Dict[str, str]],
                    Optional[Dict[str, str]],
                ],
                Tuple[Optional[str], Optional[str], str, str],
            ]
        ],
    ) -> Optional["SqlParsingResult"]:
        database_field = datasource.get(c.DATABASE) or {}
        database_id: Optional[str] = database_field.get(c.ID)
        database_name: Optional[str] = database_field.get(c.NAME) or c.UNKNOWN.lower()
        database_connection_type: Optional[str] = database_field.get(
            c.CONNECTION_TYPE
        ) or datasource.get(c.CONNECTION_TYPE)

        if (
            datasource.get(c.IS_UNSUPPORTED_CUSTOM_SQL) in (None, False)
            and not self.config.force_extraction_of_lineage_from_custom_sql_queries
        ):
            logger.debug(f"datasource {datasource_urn} is not created from custom sql")
            return None

        if database_connection_type is None:
            logger.debug(
                f"database information is missing from datasource {datasource_urn}"
            )
            return None

        query = datasource.get(c.QUERY)
        if query is None:
            logger.debug(
                f"raw sql query is not available for datasource {datasource_urn}"
            )
            return None
        query = self._clean_tableau_query_parameters(query)

        logger.debug(f"Parsing sql={query}")

        upstream_db = database_name

        if func_overridden_info is not None:
            # Override the information as per configuration
            upstream_db, platform_instance, platform, _ = func_overridden_info(
                database_connection_type,
                database_name,
                database_id,
                self.config.platform_instance_map,
                self.config.lineage_overrides,
                self.config.database_hostname_to_platform_instance_map,
                self.database_server_hostname_map,
            )

        logger.debug(
            f"Overridden info upstream_db={upstream_db}, platform_instance={platform_instance}, platform={platform}"
        )

        parsed_result = create_lineage_sql_parsed_result(
            query=query,
            default_db=upstream_db,
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            graph=self.ctx.graph,
            schema_aware=not self.config.sql_parsing_disable_schema_awareness,
        )

        assert parsed_result is not None

        if parsed_result.debug_info.table_error:
            logger.warning(
                f"Failed to extract table lineage from datasource {datasource_urn}: {parsed_result.debug_info.table_error}"
            )
            self.report.num_upstream_table_lineage_failed_parse_sql += 1
        elif parsed_result.debug_info.column_error:
            logger.warning(
                f"Failed to extract column level lineage from datasource {datasource_urn}: {parsed_result.debug_info.column_error}"
            )
            self.report.num_upstream_fine_grained_lineage_failed_parse_sql += 1

        return parsed_result

    def _enrich_database_tables_with_parsed_schemas(
        self, parsing_result: SqlParsingResult
    ) -> None:
        in_tables_schemas: Dict[str, Set[str]] = (
            transform_parsing_result_to_in_tables_schemas(parsing_result)
        )

        if not in_tables_schemas:
            logger.info("Unable to extract table schema from parsing result")
            return

        for table_urn, columns in in_tables_schemas.items():
            if table_urn in self.database_tables:
                self.database_tables[table_urn].update_table(
                    table_urn, parsed_columns=columns
                )
            else:
                self.database_tables[table_urn] = DatabaseTable(
                    urn=table_urn, parsed_columns=columns
                )

    def _create_lineage_from_unsupported_csql(
        self, csql_urn: str, csql: dict, out_columns: List[Dict[Any, Any]]
    ) -> Iterable[MetadataWorkUnit]:
        parsed_result = self.parse_custom_sql(
            datasource=csql,
            datasource_urn=csql_urn,
            env=self.config.env,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            func_overridden_info=get_overridden_info,
        )
        logger.debug(
            f"_create_lineage_from_unsupported_csql parsed_result = {parsed_result}"
        )

        if parsed_result is None:
            return

        self._enrich_database_tables_with_parsed_schemas(parsed_result)

        upstream_tables = make_upstream_class(parsed_result)

        logger.debug(f"Upstream tables = {upstream_tables}")

        fine_grained_lineages: List[FineGrainedLineage] = []
        if self.config.extract_column_level_lineage:
            logger.debug("Extracting CLL from custom sql")
            fine_grained_lineages = make_fine_grained_lineage_class(
                parsed_result, csql_urn, out_columns
            )

        upstream_lineage = UpstreamLineage(
            upstreams=upstream_tables,
            fineGrainedLineages=fine_grained_lineages,
        )
        yield self.get_metadata_change_proposal(
            csql_urn,
            aspect_name=c.UPSTREAM_LINEAGE,
            aspect=upstream_lineage,
        )
        self.report.num_tables_with_upstream_lineage += 1
        self.report.num_upstream_table_lineage += len(upstream_tables)
        self.report.num_upstream_fine_grained_lineage += len(fine_grained_lineages)

    def _get_schema_metadata_for_datasource(
        self, datasource_fields: List[dict]
    ) -> Optional[SchemaMetadata]:
        fields = []
        for field in datasource_fields:
            # check datasource - custom sql relations from a field being referenced
            self._track_custom_sql_ids(field)
            if field.get(c.NAME) is None:
                self.report.num_upstream_table_skipped_no_name += 1
                logger.warning(
                    f"Skipping field {field[c.ID]} from schema since its name is none"
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
        return MetadataWorkUnit(id=snap_shot.urn, mce=mce)

    def get_metadata_change_proposal(
        self,
        urn: str,
        aspect_name: str,
        aspect: Union["UpstreamLineage", "SubTypesClass"],
    ) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityType=c.DATASET,
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=urn,
            aspectName=aspect_name,
            aspect=aspect,
        ).as_workunit()

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
        if (
            not is_embedded_ds
            and self._get_published_datasource_project_luid(datasource) is None
        ):
            logger.warning(
                f"Skip ingesting published datasource {datasource.get(c.NAME)} because of filtered project"
            )
            return

        logger.debug(f"datasource {datasource.get(c.NAME)} browse-path {browse_path}")
        datasource_id = datasource[c.ID]
        datasource_urn = builder.make_dataset_urn_with_platform_instance(
            self.platform, datasource_id, self.config.platform_instance, self.config.env
        )
        if datasource_id not in self.datasource_ids_being_used:
            self.datasource_ids_being_used.append(datasource_id)

        dataset_snapshot = DatasetSnapshot(
            urn=datasource_urn,
            aspects=[self.get_data_platform_instance()],
        )

        # Tags
        if datasource_info and self.config.ingest_tags:
            tags = self.get_tags(datasource_info)
            dataset_snapshot.aspects.append(
                builder.make_global_tag_aspect_with_tag_list(tags)
            )

        # Browse path
        if browse_path and is_embedded_ds and workbook and workbook.get(c.NAME):
            browse_path = (
                f"{browse_path}/{workbook[c.NAME].replace('/', REPLACE_SLASH_CHAR)}"
            )

        if browse_path:
            browse_paths = BrowsePathsClass(
                paths=[f"{self.dataset_browse_prefix}/{browse_path}"]
            )
            dataset_snapshot.aspects.append(browse_paths)

        # Ownership
        owner = (
            self._get_ownership(datasource_info[c.OWNER][c.USERNAME])
            if datasource_info
            and datasource_info.get(c.OWNER)
            and datasource_info[c.OWNER].get(c.USERNAME)
            else None
        )
        if owner is not None:
            dataset_snapshot.aspects.append(owner)

        # Dataset properties
        dataset_props = DatasetPropertiesClass(
            name=datasource.get(c.NAME),
            description=datasource.get(c.DESCRIPTION),
            customProperties=self.get_custom_props_from_dict(
                datasource,
                [
                    c.HAS_EXTRACTS,
                    c.EXTRACT_LAST_REFRESH_TIME,
                    c.EXTRACT_LAST_INCREMENTAL_UPDATE_TIME,
                    c.EXTRACT_LAST_UPDATE_TIME,
                ],
            ),
        )
        dataset_snapshot.aspects.append(dataset_props)

        # Upstream Tables
        if (
            datasource.get(c.UPSTREAM_TABLES)
            or datasource.get(c.UPSTREAM_DATA_SOURCES)
            or datasource.get(c.FIELDS)
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
                    aspect_name=c.UPSTREAM_LINEAGE,
                    aspect=upstream_lineage,
                )
                self.report.num_tables_with_upstream_lineage += 1
                self.report.num_upstream_table_lineage += len(upstream_tables)
                self.report.num_upstream_fine_grained_lineage += len(
                    fine_grained_lineages
                )

        # Datasource Fields
        schema_metadata = self._get_schema_metadata_for_datasource(
            datasource.get(c.FIELDS, [])
        )
        if schema_metadata is not None:
            dataset_snapshot.aspects.append(schema_metadata)

        yield self.get_metadata_change_event(dataset_snapshot)
        yield self.get_metadata_change_proposal(
            dataset_snapshot.urn,
            aspect_name=c.SUB_TYPES,
            aspect=SubTypesClass(
                typeNames=(
                    ["Embedded Data Source"]
                    if is_embedded_ds
                    else ["Published Data Source"]
                )
            ),
        )

        container_key = self._get_datasource_container_key(
            datasource, workbook, is_embedded_ds
        )
        if container_key is not None:
            yield from add_entity_to_container(
                container_key,
                c.DATASET,
                dataset_snapshot.urn,
            )

    def get_custom_props_from_dict(self, obj: dict, keys: List[str]) -> Optional[dict]:
        return {key: str(obj[key]) for key in keys if obj.get(key)} or None

    def _get_datasource_container_key(
        self, datasource: dict, workbook: Optional[dict], is_embedded_ds: bool
    ) -> Optional[ContainerKey]:
        container_key: Optional[ContainerKey] = None
        if is_embedded_ds:  # It is embedded then parent is container is workbook
            if workbook is not None:
                container_key = self.gen_workbook_key(workbook[c.ID])
            else:
                logger.warning(
                    f"Parent container not set for embedded datasource {datasource[c.ID]}"
                )
        else:
            parent_project_luid = self._get_published_datasource_project_luid(
                datasource
            )
            # It is published datasource and hence parent container is project
            if parent_project_luid is not None:
                container_key = self.gen_project_key(parent_project_luid)
            else:
                logger.warning(
                    f"Parent container not set for published datasource {datasource[c.ID]}"
                )

        return container_key

    def update_datasource_for_field_upstream(
        self,
        datasource: dict,
        field_upstream_query: str,
        page_size: int,
    ) -> dict:
        # Collect field ids to fetch field upstreams
        field_ids: List[str] = []
        for field in datasource.get(c.FIELDS, []):
            if field.get(c.ID):
                field_ids.append(field.get(c.ID))

        # Fetch field upstreams and arrange them in map
        field_vs_upstream: Dict[str, dict] = {}
        for field_upstream in self.get_connection_objects(
            query=field_upstream_query,
            connection_type=c.FIELDS_CONNECTION,
            query_filter={c.ID_WITH_IN: field_ids},
            page_size=page_size,
        ):
            if field_upstream.get(c.ID):
                field_id = field_upstream[c.ID]
                # delete the field id, we don't need it for further processing
                del field_upstream[c.ID]
                field_vs_upstream[field_id] = field_upstream

        # update datasource's field for its upstream
        for field_dict in datasource.get(c.FIELDS, []):
            field_upstream_dict: Optional[dict] = field_vs_upstream.get(
                field_dict.get(c.ID)
            )
            if field_upstream_dict:
                # Add upstream fields to field
                field_dict.update(field_upstream_dict)

        return datasource

    def emit_published_datasources(self) -> Iterable[MetadataWorkUnit]:
        datasource_filter = {c.ID_WITH_IN: self.datasource_ids_being_used}

        for datasource in self.get_connection_objects(
            query=published_datasource_graphql_query,
            connection_type=c.PUBLISHED_DATA_SOURCES_CONNECTION,
            query_filter=datasource_filter,
            page_size=self.config.effective_published_datasource_page_size,
        ):
            datasource = self.update_datasource_for_field_upstream(
                datasource=datasource,
                field_upstream_query=datasource_upstream_fields_graphql_query,
                page_size=self.config.effective_published_datasource_field_upstream_page_size,
            )

            yield from self.emit_datasource(datasource)

    def emit_upstream_tables(self) -> Iterable[MetadataWorkUnit]:
        tableau_database_table_id_to_urn_map: Dict[str, str] = dict()
        for urn, tbl in self.database_tables.items():
            # only tables that came from Tableau metadata have id
            if tbl.id:
                tableau_database_table_id_to_urn_map[tbl.id] = urn

        tables_filter = {
            c.ID_WITH_IN: list(tableau_database_table_id_to_urn_map.keys())
        }

        # Emitting tables that came from Tableau metadata
        for tableau_table in self.get_connection_objects(
            query=database_tables_graphql_query,
            connection_type=c.DATABASE_TABLES_CONNECTION,
            query_filter=tables_filter,
            page_size=self.config.effective_database_table_page_size,
        ):
            if tableau_database_table_id_to_urn_map.get(tableau_table[c.ID]) is None:
                logger.warning(
                    f"Skipping table {tableau_table[c.ID]} due to filtered out published datasource"
                )
                continue
            database_table = self.database_tables[
                tableau_database_table_id_to_urn_map[tableau_table[c.ID]]
            ]
            tableau_columns = tableau_table.get(c.COLUMNS, [])
            is_embedded = tableau_table.get(c.IS_EMBEDDED) or False
            if not is_embedded and not self.config.ingest_tables_external:
                logger.debug(
                    f"Skipping external table {database_table.urn} as ingest_tables_external is set to False"
                )
                continue

            yield from self.emit_table(database_table, tableau_columns)

        # Emitting tables that were purely parsed from SQL queries
        for database_table in self.database_tables.values():
            # Only tables purely parsed from SQL queries don't have ID
            if database_table.id:
                logger.debug(
                    f"Skipping external table {database_table.urn} should have already been ingested from Tableau metadata"
                )
                continue

            if not self.config.ingest_tables_external:
                logger.debug(
                    f"Skipping external table {database_table.urn} as ingest_tables_external is set to False"
                )
                continue

            yield from self.emit_table(database_table, None)

    def emit_table(
        self,
        database_table: DatabaseTable,
        tableau_columns: Optional[List[Dict[str, Any]]],
    ) -> Iterable[MetadataWorkUnit]:
        logger.debug(
            f"Emitting external table {database_table} tableau_columns {tableau_columns}"
        )
        dataset_urn = DatasetUrn.from_string(database_table.urn)
        dataset_snapshot = DatasetSnapshot(
            urn=str(dataset_urn),
            aspects=[],
        )
        if database_table.paths:
            # Browse path
            browse_paths = BrowsePathsClass(
                paths=[
                    f"{self.dataset_browse_prefix}/{path}"
                    for path in sorted(database_table.paths, key=lambda p: (len(p), p))
                ]
            )
            dataset_snapshot.aspects.append(browse_paths)
        else:
            logger.debug(f"Browse path not set for table {database_table.urn}")
            return

        schema_metadata = self.get_schema_metadata_for_table(
            tableau_columns, database_table.parsed_columns
        )
        if schema_metadata is not None:
            dataset_snapshot.aspects.append(schema_metadata)

        if not dataset_snapshot.aspects:
            # This should only happen with ingest_tables_external enabled.
            logger.warning(
                f"Urn {database_table.urn} has no real aspects, adding a key aspect to ensure materialization"
            )
            dataset_snapshot.aspects.append(dataset_urn.to_key_aspect())

        yield self.get_metadata_change_event(dataset_snapshot)

    def get_schema_metadata_for_table(
        self,
        tableau_columns: Optional[List[Dict[str, Any]]],
        parsed_columns: Optional[Set[str]] = None,
    ) -> Optional[SchemaMetadata]:
        schema_metadata: Optional[SchemaMetadata] = None

        fields = []

        if tableau_columns:
            for field in tableau_columns:
                if field.get(c.NAME) is None:
                    self.report.num_table_field_skipped_no_name += 1
                    logger.warning(
                        f"Skipping field {field[c.ID]} from schema since its name is none"
                    )
                    continue
                nativeDataType = field.get(c.REMOTE_TYPE, c.UNKNOWN)
                TypeClass = FIELD_TYPE_MAPPING.get(nativeDataType, NullTypeClass)

                schema_field = SchemaField(
                    fieldPath=field[c.NAME],
                    type=SchemaFieldDataType(type=TypeClass()),
                    description=field.get(c.DESCRIPTION),
                    nativeDataType=nativeDataType,
                )

                fields.append(schema_field)

        if parsed_columns:
            remaining_columns = (
                parsed_columns.difference(map(lambda x: x.get(c.NAME), tableau_columns))
                if tableau_columns
                else parsed_columns
            )
            remaining_schema_fields = [
                SchemaField(
                    fieldPath=col,
                    type=SchemaFieldDataType(type=NullTypeClass()),
                    description="",
                    nativeDataType=c.UNKNOWN,
                )
                for col in remaining_columns
            ]
            fields.extend(remaining_schema_fields)

        if fields:
            schema_metadata = SchemaMetadata(
                schemaName="test",
                platform=f"urn:li:dataPlatform:{self.platform}",
                version=0,
                fields=fields,
                hash="",
                platformSchema=OtherSchema(rawSchema=""),
            )

        # TODO: optionally add logic that will lookup current table schema from DataHub
        # and merge it together with what was inferred during current run, it allows incrementally
        # ingest different Tableau projects sharing the same tables

        return schema_metadata

    def get_sheetwise_upstream_datasources(self, sheet: dict) -> set:
        sheet_upstream_datasources = set()

        for field in sheet.get(c.DATA_SOURCE_FIELDS) or []:
            if field and field.get(c.DATA_SOURCE):
                sheet_upstream_datasources.add(field[c.DATA_SOURCE][c.ID])

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
        luid: Optional[str] = sheet.get(c.LUID)
        if luid is None:
            logger.debug(
                "stat:luid is none for sheet %s(id:%s)",
                sheet.get(c.NAME),
                sheet.get(c.ID),
            )
            return None
        usage_stat: Optional[UsageStat] = self.tableau_stat_registry.get(luid)
        if usage_stat is None:
            logger.debug(
                "stat:UsageStat is not available in tableau_stat_registry for sheet %s(id:%s)",
                sheet.get(c.NAME),
                sheet.get(c.ID),
            )
            return None

        aspect: ChartUsageStatisticsClass = self._create_datahub_chart_usage_stat(
            usage_stat
        )
        logger.debug(
            "stat: Chart usage stat work unit is created for %s(id:%s)",
            sheet.get(c.NAME),
            sheet.get(c.ID),
        )
        return MetadataChangeProposalWrapper(
            aspect=aspect,
            entityUrn=sheet_urn,
        ).as_workunit()

    def emit_sheets(self) -> Iterable[MetadataWorkUnit]:
        sheets_filter = {c.ID_WITH_IN: self.sheet_ids}

        for sheet in self.get_connection_objects(
            query=sheet_graphql_query,
            connection_type=c.SHEETS_CONNECTION,
            query_filter=sheets_filter,
            page_size=self.config.effective_sheet_page_size,
        ):
            if self.config.ingest_hidden_assets or not self._is_hidden_view(sheet):
                yield from self.emit_sheets_as_charts(sheet, sheet.get(c.WORKBOOK))
            else:
                self.report.num_hidden_assets_skipped += 1
                logger.debug(
                    f"Skip view {sheet.get(c.ID)} because it's hidden (luid is blank)."
                )

    def emit_sheets_as_charts(
        self, sheet: dict, workbook: Optional[Dict]
    ) -> Iterable[MetadataWorkUnit]:
        sheet_urn: str = builder.make_chart_urn(
            self.platform, sheet[c.ID], self.config.platform_instance
        )
        chart_snapshot = ChartSnapshot(
            urn=sheet_urn,
            aspects=[self.get_data_platform_instance()],
        )

        creator: Optional[str] = None
        if workbook is not None and workbook.get(c.OWNER) is not None:
            creator = workbook[c.OWNER].get(c.USERNAME)
        created_at = sheet.get(c.CREATED_AT, datetime.now())
        updated_at = sheet.get(c.UPDATED_AT, datetime.now())
        last_modified = self.get_last_modified(creator, created_at, updated_at)

        if sheet.get(c.PATH):
            site_part = (
                f"/site/{self.site_content_url}" if self.site_content_url else ""
            )
            sheet_external_url = (
                f"{self.config.connect_uri}/#{site_part}/views/{sheet.get(c.PATH)}"
            )
        elif (
            sheet.get(c.CONTAINED_IN_DASHBOARDS) is not None
            and len(sheet[c.CONTAINED_IN_DASHBOARDS]) > 0
            and sheet[c.CONTAINED_IN_DASHBOARDS][0] is not None
            and sheet[c.CONTAINED_IN_DASHBOARDS][0].get(c.PATH)
        ):
            # sheet contained in dashboard
            site_part = f"/t/{self.site_content_url}" if self.site_content_url else ""
            dashboard_path = sheet[c.CONTAINED_IN_DASHBOARDS][0][c.PATH]
            sheet_external_url = f"{self.config.connect_uri}{site_part}/authoring/{dashboard_path}/{quote(sheet.get(c.NAME, ''), safe='')}"
        else:
            # hidden or viz-in-tooltip sheet
            sheet_external_url = None
        input_fields: List[InputField] = []
        if sheet.get(c.DATA_SOURCE_FIELDS):
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
            title=sheet.get(c.NAME) or "",
            lastModified=last_modified,
            externalUrl=(
                sheet_external_url
                if self.config.ingest_external_links_for_charts
                else None
            ),
            inputs=sorted(datasource_urn),
            customProperties=self.get_custom_props_from_dict(sheet, [c.LUID]),
        )
        chart_snapshot.aspects.append(chart_info)
        # chart_snapshot doesn't support the stat aspect as list element and hence need to emit MCP

        if self.config.extract_usage_stats:
            wu = self._get_chart_stat_wu(sheet, sheet_urn)
            if wu is not None:
                yield wu

        browse_paths = self.get_browse_paths_aspect(workbook)
        if browse_paths:
            chart_snapshot.aspects.append(browse_paths)
        else:
            logger.warning(
                f"Could not set browse path for workbook {sheet[c.ID]}. Please check permissions."
            )

        # Ownership
        owner = self._get_ownership(creator)
        if owner is not None:
            chart_snapshot.aspects.append(owner)

        #  Tags
        if self.config.ingest_tags:
            tags = self.get_tags(sheet)
            if len(self.config.tags_for_hidden_assets) > 0 and self._is_hidden_view(
                sheet
            ):
                tags.extend(self.config.tags_for_hidden_assets)

            chart_snapshot.aspects.append(
                builder.make_global_tag_aspect_with_tag_list(tags)
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
            yield from add_entity_to_container(
                self.gen_workbook_key(workbook[c.ID]), c.CHART, chart_snapshot.urn
            )

        if input_fields:
            yield MetadataChangeProposalWrapper(
                entityUrn=sheet_urn,
                aspect=InputFields(
                    fields=sorted(input_fields, key=lambda x: x.schemaFieldUrn)
                ),
            ).as_workunit()

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
        for field in sheet.get(c.DATA_SOURCE_FIELDS):  # type: ignore
            if not field:
                continue
            name = field.get(c.NAME)
            upstream_ds_id = (
                field.get(c.DATA_SOURCE)[c.ID] if field.get(c.DATA_SOURCE) else None
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
        workbook_container_key = self.gen_workbook_key(workbook[c.ID])
        creator = workbook.get(c.OWNER, {}).get(c.USERNAME)

        owner_urn = (
            builder.make_user_urn(creator)
            if (creator and self.config.ingest_owner)
            else None
        )

        site_part = f"/site/{self.site_content_url}" if self.site_content_url else ""
        workbook_uri = workbook.get("uri")
        workbook_part = (
            workbook_uri[workbook_uri.index("/workbooks/") :] if workbook_uri else None
        )
        workbook_external_url = (
            f"{self.config.connect_uri}/#{site_part}{workbook_part}"
            if workbook_part
            else None
        )

        tags = self.get_tags(workbook) if self.config.ingest_tags else None

        parent_key = None
        project_luid: Optional[str] = self._get_workbook_project_luid(workbook)
        if project_luid and project_luid in self.tableau_project_registry.keys():
            parent_key = self.gen_project_key(project_luid)
        else:
            workbook_id: Optional[str] = workbook.get(c.ID)
            workbook_name: Optional[str] = workbook.get(c.NAME)
            logger.warning(
                f"Could not load project hierarchy for workbook {workbook_name}({workbook_id}). Please check permissions."
            )

        custom_props = None
        if (
            self.config.permission_ingestion
            and self.config.permission_ingestion.enable_workbooks
        ):
            logger.debug(f"Ingest access roles of workbook-id='{workbook.get(c.LUID)}'")
            workbook_instance = self.server.workbooks.get_by_id(workbook.get(c.LUID))
            self.server.workbooks.populate_permissions(workbook_instance)
            custom_props = self._create_workbook_properties(
                workbook_instance.permissions
            )

        yield from gen_containers(
            container_key=workbook_container_key,
            name=workbook.get(c.NAME) or "",
            parent_container_key=parent_key,
            description=workbook.get(c.DESCRIPTION),
            sub_types=[BIContainerSubTypes.TABLEAU_WORKBOOK],
            owner_urn=owner_urn,
            external_url=workbook_external_url,
            extra_properties=custom_props,
            tags=tags,
        )

    def gen_workbook_key(self, workbook_id: str) -> WorkbookKey:
        return WorkbookKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            workbook_id=workbook_id,
        )

    def gen_project_key(self, project_luid: str) -> ProjectKey:
        return ProjectKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            project_id=project_luid,
        )

    def gen_site_key(self, site_id: str) -> SiteKey:
        return SiteKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            site_id=site_id,
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
        luid: Optional[str] = dashboard.get(c.LUID)
        if luid is None:
            logger.debug(
                "stat:luid is none for dashboard %s(id:%s)",
                dashboard.get(c.NAME),
                dashboard.get(c.ID),
            )
            return None
        usage_stat: Optional[UsageStat] = self.tableau_stat_registry.get(luid)
        if usage_stat is None:
            logger.debug(
                "stat:UsageStat is not available in tableau_stat_registry for dashboard %s(id:%s)",
                dashboard.get(c.NAME),
                dashboard.get(c.ID),
            )
            return None

        aspect: DashboardUsageStatisticsClass = (
            self._create_datahub_dashboard_usage_stat(usage_stat)
        )
        logger.debug(
            "stat: Dashboard usage stat is created for %s(id:%s)",
            dashboard.get(c.NAME),
            dashboard.get(c.ID),
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
        dashboards_filter = {c.ID_WITH_IN: self.dashboard_ids}

        for dashboard in self.get_connection_objects(
            query=dashboard_graphql_query,
            connection_type=c.DASHBOARDS_CONNECTION,
            query_filter=dashboards_filter,
            page_size=self.config.effective_dashboard_page_size,
        ):
            if self.config.ingest_hidden_assets or not self._is_hidden_view(dashboard):
                yield from self.emit_dashboard(dashboard, dashboard.get(c.WORKBOOK))
            else:
                self.report.num_hidden_assets_skipped += 1
                logger.debug(
                    f"Skip dashboard {dashboard.get(c.ID)} because it's hidden (luid is blank)."
                )

    def get_tags(self, obj: dict) -> List[str]:
        tag_list = obj.get(c.TAGS, [])
        if tag_list:
            tag_list_str = [
                t[c.NAME] for t in tag_list if t is not None and t.get(c.NAME)
            ]

            return tag_list_str
        return []

    def emit_dashboard(
        self, dashboard: dict, workbook: Optional[Dict]
    ) -> Iterable[MetadataWorkUnit]:
        dashboard_urn: str = builder.make_dashboard_urn(
            self.platform, dashboard[c.ID], self.config.platform_instance
        )
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[self.get_data_platform_instance()],
        )

        creator: Optional[str] = None
        if workbook is not None and workbook.get(c.OWNER) is not None:
            creator = workbook[c.OWNER].get(c.USERNAME)
        created_at = dashboard.get(c.CREATED_AT, datetime.now())
        updated_at = dashboard.get(c.UPDATED_AT, datetime.now())
        last_modified = self.get_last_modified(creator, created_at, updated_at)

        site_part = f"/site/{self.site_content_url}" if self.site_content_url else ""
        dashboard_external_url = (
            f"{self.config.connect_uri}/#{site_part}/views/{dashboard.get(c.PATH, '')}"
        )
        title = (
            dashboard[c.NAME].replace("/", REPLACE_SLASH_CHAR)
            if dashboard.get(c.NAME)
            else ""
        )
        chart_urns = [
            builder.make_chart_urn(
                self.platform,
                sheet.get(c.ID),
                self.config.platform_instance,
            )
            for sheet in dashboard.get(c.SHEETS, [])
        ]
        dashboard_info_class = DashboardInfoClass(
            description="",
            title=title,
            charts=chart_urns,
            lastModified=last_modified,
            dashboardUrl=(
                dashboard_external_url
                if self.config.ingest_external_links_for_dashboards
                else None
            ),
            customProperties=self.get_custom_props_from_dict(dashboard, [c.LUID]),
        )
        dashboard_snapshot.aspects.append(dashboard_info_class)

        if self.config.ingest_tags:
            tags = self.get_tags(dashboard)
            if len(self.config.tags_for_hidden_assets) > 0 and self._is_hidden_view(
                dashboard
            ):
                tags.extend(self.config.tags_for_hidden_assets)

            dashboard_snapshot.aspects.append(
                builder.make_global_tag_aspect_with_tag_list(tags)
            )

        if self.config.extract_usage_stats:
            # dashboard_snapshot doesn't support the stat aspect as list element and hence need to emit MetadataWorkUnit
            wu = self._get_dashboard_stat_wu(dashboard, dashboard_urn)
            if wu is not None:
                yield wu

        browse_paths = self.get_browse_paths_aspect(workbook)
        if browse_paths:
            dashboard_snapshot.aspects.append(browse_paths)
        else:
            logger.warning(
                f"Could not set browse path for dashboard {dashboard[c.ID]}. Please check permissions."
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
            yield from add_entity_to_container(
                self.gen_workbook_key(workbook[c.ID]),
                c.DASHBOARD,
                dashboard_snapshot.urn,
            )

    def get_browse_paths_aspect(
        self, workbook: Optional[Dict]
    ) -> Optional[BrowsePathsClass]:
        browse_paths: Optional[BrowsePathsClass] = None
        if workbook and workbook.get(c.NAME):
            project_luid: Optional[str] = self._get_workbook_project_luid(workbook)
            if project_luid in self.tableau_project_registry:
                browse_paths = BrowsePathsClass(
                    paths=[
                        f"{self.no_env_browse_prefix}/{self._project_luid_to_browse_path_name(project_luid)}"
                        f"/{workbook[c.NAME].replace('/', REPLACE_SLASH_CHAR)}"
                    ]
                )

            elif workbook.get(c.PROJECT_NAME):
                # browse path
                browse_paths = BrowsePathsClass(
                    paths=[
                        f"{self.no_env_browse_prefix}/{workbook[c.PROJECT_NAME].replace('/', REPLACE_SLASH_CHAR)}"
                        f"/{workbook[c.NAME].replace('/', REPLACE_SLASH_CHAR)}"
                    ]
                )

        return browse_paths

    def emit_embedded_datasources(self) -> Iterable[MetadataWorkUnit]:
        datasource_filter = {c.ID_WITH_IN: self.embedded_datasource_ids_being_used}

        for datasource in self.get_connection_objects(
            query=embedded_datasource_graphql_query,
            connection_type=c.EMBEDDED_DATA_SOURCES_CONNECTION,
            query_filter=datasource_filter,
            page_size=self.config.effective_embedded_datasource_page_size,
        ):
            datasource = self.update_datasource_for_field_upstream(
                datasource=datasource,
                field_upstream_query=datasource_upstream_fields_graphql_query,
                page_size=self.config.effective_embedded_datasource_field_upstream_page_size,
            )
            yield from self.emit_datasource(
                datasource,
                datasource.get(c.WORKBOOK),
                is_embedded_ds=True,
            )

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

    def emit_project_containers(self) -> Iterable[MetadataWorkUnit]:
        generated_project_keys: Set[str] = set()

        def emit_project_in_topological_order(
            project_: TableauProject,
        ) -> Iterable[MetadataWorkUnit]:
            """
            The source_helpers.py file includes a function called auto_browse_path_v2 that automatically generates
            BrowsePathsV2. This auto-generation relies on the correct sequencing of MetadataWorkUnit containers.
            The emit_project_in_topological_order function ensures that parent containers are emitted before their child
            containers.
            This is a recursive function.
            """
            project_key: ProjectKey = self.gen_project_key(project_.id)

            if project_key.guid() in generated_project_keys:
                return

            generated_project_keys.add(project_key.guid())

            parent_project_key: Optional[Union[ProjectKey, SiteKey]] = (
                None  # It is going
            )
            # to be used as a parent container key for the current tableau project

            if project_.parent_id is not None:
                # Go to the parent project as we need to generate container first for parent
                parent_project_key = self.gen_project_key(project_.parent_id)

                parent_tableau_project: Optional[TableauProject] = (
                    self.tableau_project_registry.get(project_.parent_id)
                )

                if (
                    parent_tableau_project is None
                ):  # It is not in project registry because of project_pattern
                    assert project_.parent_name, (
                        f"project {project_.name} should not be null"
                    )
                    parent_tableau_project = TableauProject(
                        id=project_.parent_id,
                        name=project_.parent_name,
                        description=None,
                        parent_id=None,
                        parent_name=None,
                        path=[],
                    )

                yield from emit_project_in_topological_order(parent_tableau_project)

            else:
                # This is a root Tableau project since the parent_project_id is None.
                # For a root project, either the site is the parent, or the platform is the default parent.
                if self.config.add_site_container:
                    # The site containers have already been generated by emit_site_container, so we
                    # don't need to emit them again here.
                    parent_project_key = self.gen_site_key(self.site_id)

            yield from gen_containers(
                container_key=project_key,
                name=project_.name,
                description=project_.description,
                sub_types=[c.PROJECT],
                parent_container_key=parent_project_key,
            )

        for project in self.tableau_project_registry.values():
            logger.debug(
                f"project {project.name} and it's parent {project.parent_name} and parent id {project.parent_id}"
            )
            yield from emit_project_in_topological_order(project)

    def emit_site_container(self):
        if not self.site:
            logger.warning("Can not ingest site container. No site information found.")
            return

        yield from gen_containers(
            container_key=self.gen_site_key(self.site_id),
            name=self.site.name or "Default",
            sub_types=[c.SITE],
        )

    def _fetch_groups(self):
        for group in TSC.Pager(self.server.groups):
            self.group_map[group.id] = group

    def _get_allowed_capabilities(self, capabilities: Dict[str, str]) -> List[str]:
        if not self.config.permission_ingestion:
            return []

        allowed_capabilities = [
            key for key, value in capabilities.items() if value == "Allow"
        ]
        return allowed_capabilities

    def _create_workbook_properties(
        self, permissions: List[PermissionsRule]
    ) -> Optional[Dict[str, str]]:
        if not self.config.permission_ingestion:
            return None

        groups = []
        for rule in permissions:
            if rule.grantee.tag_name == "group":
                group = self.group_map.get(rule.grantee.id)
                if not group or not group.name:
                    logger.debug(f"Group {rule.grantee.id} not found in group map.")
                    continue
                if not self.config.permission_ingestion.group_name_pattern.allowed(
                    group.name
                ):
                    logger.info(
                        f"Skip permission '{group.name}' as it's excluded in group_name_pattern."
                    )
                    continue

                capabilities = self._get_allowed_capabilities(rule.capabilities)
                groups.append({"group": group.name, "capabilities": capabilities})

        return {"permissions": json.dumps(groups)} if len(groups) > 0 else None

    def ingest_tableau_site(self):
        with self.report.new_stage(
            f"Ingesting Tableau Site: {self.site_id} {self.site_content_url}"
        ):
            # Initialise the dictionary to later look-up for chart and dashboard stat
            if self.config.extract_usage_stats:
                with PerfTimer() as timer:
                    self._populate_usage_stat_registry()
                    self.report.extract_usage_stats_timer[self.site_content_url] = (
                        timer.elapsed_seconds(digits=2)
                    )

            if self.config.permission_ingestion:
                with PerfTimer() as timer:
                    self._fetch_groups()
                    self.report.fetch_groups_timer[self.site_content_url] = (
                        timer.elapsed_seconds(digits=2)
                    )

            # Populate the map of database names and database hostnames to be used later to map
            # databases to platform instances.
            if self.config.database_hostname_to_platform_instance_map:
                with PerfTimer() as timer:
                    self._populate_database_server_hostname_map()
                    self.report.populate_database_server_hostname_map_timer[
                        self.site_content_url
                    ] = timer.elapsed_seconds(digits=2)

            with PerfTimer() as timer:
                self._populate_projects_registry()
                self.report.populate_projects_registry_timer[self.site_content_url] = (
                    timer.elapsed_seconds(digits=2)
                )

            if self.config.add_site_container:
                yield from self.emit_site_container()
            yield from self.emit_project_containers()

            with PerfTimer() as timer:
                yield from self.emit_workbooks()
                self.report.emit_workbooks_timer[self.site_content_url] = (
                    timer.elapsed_seconds(digits=2)
                )

            if self.sheet_ids:
                with PerfTimer() as timer:
                    yield from self.emit_sheets()
                    self.report.emit_sheets_timer[self.site_content_url] = (
                        timer.elapsed_seconds(digits=2)
                    )

            if self.dashboard_ids:
                with PerfTimer() as timer:
                    yield from self.emit_dashboards()
                    self.report.emit_dashboards_timer[self.site_content_url] = (
                        timer.elapsed_seconds(digits=2)
                    )

            if self.embedded_datasource_ids_being_used:
                with PerfTimer() as timer:
                    yield from self.emit_embedded_datasources()
                    self.report.emit_embedded_datasources_timer[
                        self.site_content_url
                    ] = timer.elapsed_seconds(digits=2)

            if self.datasource_ids_being_used:
                with PerfTimer() as timer:
                    yield from self.emit_published_datasources()
                    self.report.emit_published_datasources_timer[
                        self.site_content_url
                    ] = timer.elapsed_seconds(digits=2)

            if self.custom_sql_ids_being_used:
                with PerfTimer() as timer:
                    yield from self.emit_custom_sql_datasources()
                    self.report.emit_custom_sql_datasources_timer[
                        self.site_content_url
                    ] = timer.elapsed_seconds(digits=2)

            if self.database_tables:
                with PerfTimer() as timer:
                    yield from self.emit_upstream_tables()
                    self.report.emit_upstream_tables_timer[self.site_content_url] = (
                        timer.elapsed_seconds(digits=2)
                    )
