"""
Power BI Models Module

This module contains all Pydantic BaseModel and dataclass definitions used across
the Power BI connector. Models are organized by their functional area:
- Configuration models
- REST API wrapper data classes
- PBIX parsing models
- DAX parsing models
- Lineage builder models
- Visualization builder models
"""

import dataclasses
import logging
from dataclasses import dataclass, field as dataclass_field
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Set, Union

import pydantic
from pydantic import BaseModel, Field, field_validator, model_validator

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern, ConfigModel, HiddenFromDocs
from datahub.configuration.source_common import DatasetSourceConfigMixin, PlatformDetail
from datahub.configuration.validate_field_deprecation import pydantic_field_deprecated
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.incremental_lineage_helper import (
    IncrementalLineageConfigMixin,
)
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    UpstreamClass,
)
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    ChartInfoClass,
    DateTypeClass,
    InputFieldClass,
    NullTypeClass,
    NumberTypeClass,
    StringTypeClass,
)
from datahub.utilities.global_warning_util import add_global_warning
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)


# =============================================================================
# Constants and Configuration Models
# =============================================================================


class Constant:
    """
    keys used in powerbi plugin
    """

    PBIAccessToken = "PBIAccessToken"
    DASHBOARD_LIST = "DASHBOARD_LIST"
    TILE_LIST = "TILE_LIST"
    REPORT_LIST = "REPORT_LIST"
    PAGE_BY_REPORT = "PAGE_BY_REPORT"
    DATASET_GET = "DATASET_GET"
    DATASET_LIST = "DATASET_LIST"
    WORKSPACE_MODIFIED_LIST = "WORKSPACE_MODIFIED_LIST"
    REPORT_GET = "REPORT_GET"
    DATASOURCE_GET = "DATASOURCE_GET"
    TILE_GET = "TILE_GET"
    ENTITY_USER_LIST = "ENTITY_USER_LIST"
    SCAN_CREATE = "SCAN_CREATE"
    SCAN_GET = "SCAN_GET"
    SCAN_RESULT_GET = "SCAN_RESULT_GET"
    Authorization = "Authorization"
    WORKSPACE_ID = "workspaceId"
    DASHBOARD_ID = "powerbi.linkedin.com/dashboards/{}"
    DATASET_EXECUTE_QUERIES = "DATASET_EXECUTE_QUERIES_POST"
    GET_WORKSPACE_APP = "GET_WORKSPACE_APP"
    DATASET_ID = "datasetId"
    REPORT_ID = "reportId"
    SCAN_ID = "ScanId"
    CHART_URN = "ChartURN"
    CHART = "chart"
    CORP_USER = "corpuser"
    CORP_USER_INFO = "corpUserInfo"
    CORP_USER_KEY = "corpUserKey"
    CHART_INFO = "chartInfo"
    GLOBAL_TAGS = "globalTags"
    STATUS = "status"
    CHART_ID = "powerbi.linkedin.com/charts/{}"
    CHART_KEY = "chartKey"
    COLUMN_TYPE = "columnType"
    DATA_TYPE = "dataType"
    DASHBOARD = "dashboard"
    DASHBOARDS = "dashboards"
    DASHBOARD_KEY = "dashboardKey"
    DESCRIPTION = "description"
    OWNERSHIP = "ownership"
    BROWSERPATH = "browsePaths"
    DASHBOARD_INFO = "dashboardInfo"
    DATAPLATFORM_INSTANCE = "dataPlatformInstance"
    DATASET = "dataset"
    DATASETS = "datasets"
    DATASET_KEY = "datasetKey"
    DATASET_PROPERTIES = "datasetProperties"
    VIEW_PROPERTIES = "viewProperties"
    SCHEMA_METADATA = "schemaMetadata"
    SUBTYPES = "subTypes"
    VALUE = "value"
    ENTITY = "ENTITY"
    ID = "id"
    HTTP_RESPONSE_TEXT = "HttpResponseText"
    HTTP_RESPONSE_STATUS_CODE = "HttpResponseStatusCode"
    NAME = "name"
    DISPLAY_NAME = "displayName"
    CURRENT_VALUE = "currentValue"
    ORDER = "order"
    IDENTIFIER = "identifier"
    EMAIL_ADDRESS = "emailAddress"
    PRINCIPAL_TYPE = "principalType"
    DATASET_USER_ACCESS_RIGHT = "datasetUserAccessRight"
    REPORT_USER_ACCESS_RIGHT = "reportUserAccessRight"
    DASHBOARD_USER_ACCESS_RIGHT = "dashboardUserAccessRight"
    GROUP_USER_ACCESS_RIGHT = "groupUserAccessRight"
    GRAPH_ID = "graphId"
    WORKSPACES = "workspaces"
    TITLE = "title"
    EMBED_URL = "embedUrl"
    ACCESS_TOKEN = "access_token"
    ACCESS_TOKEN_EXPIRY = "expires_in"
    IS_READ_ONLY = "isReadOnly"
    WEB_URL = "webUrl"
    ODATA_COUNT = "@odata.count"
    DESCRIPTION = "description"
    REPORT = "report"
    REPORTS = "reports"
    CREATED_FROM = "createdFrom"
    SUCCEEDED = "SUCCEEDED"
    ENDORSEMENT = "endorsement"
    ENDORSEMENT_DETAIL = "endorsementDetails"
    TABLES = "tables"
    EXPRESSION = "expression"
    SOURCE = "source"
    SCHEMA_METADATA = "schemaMetadata"
    PLATFORM_NAME = "powerbi"
    REPORT_TYPE_NAME = BIAssetSubTypes.REPORT
    CHART_COUNT = "chartCount"
    WORKSPACE_NAME = "workspaceName"
    DATASET_WEB_URL = "datasetWebUrl"
    TYPE = "type"
    REPORT_TYPE = "reportType"
    LAST_UPDATE = "lastUpdate"
    APP_ID = "appId"
    REPORTS = "reports"
    ORIGINAL_REPORT_OBJECT_ID = "originalReportObjectId"
    APP_SUB_TYPE = "App"
    STATE = "state"
    ACTIVE = "Active"
    SQL_PARSING_FAILURE = "SQL Parsing Failure"
    M_QUERY_NULL = '"null"'
    REPORT_WEB_URL = "reportWebUrl"


@dataclass
class DataPlatformPair:
    datahub_data_platform_name: str
    powerbi_data_platform_name: str


@dataclass
class PowerBIPlatformDetail:
    data_platform_pair: DataPlatformPair
    data_platform_server: str


class SupportedDataPlatform(Enum):
    POSTGRES_SQL = DataPlatformPair(
        powerbi_data_platform_name="PostgreSQL", datahub_data_platform_name="postgres"
    )

    ORACLE = DataPlatformPair(
        powerbi_data_platform_name="Oracle", datahub_data_platform_name="oracle"
    )

    SNOWFLAKE = DataPlatformPair(
        powerbi_data_platform_name="Snowflake", datahub_data_platform_name="snowflake"
    )

    MS_SQL = DataPlatformPair(
        powerbi_data_platform_name="Sql", datahub_data_platform_name="mssql"
    )

    GOOGLE_BIGQUERY = DataPlatformPair(
        powerbi_data_platform_name="GoogleBigQuery",
        datahub_data_platform_name="bigquery",
    )

    AMAZON_REDSHIFT = DataPlatformPair(
        powerbi_data_platform_name="AmazonRedshift",
        datahub_data_platform_name="redshift",
    )

    DATABRICKS_SQL = DataPlatformPair(
        powerbi_data_platform_name="Databricks", datahub_data_platform_name="databricks"
    )

    DatabricksMultiCloud_SQL = DataPlatformPair(
        powerbi_data_platform_name="DatabricksMultiCloud",
        datahub_data_platform_name="databricks",
    )

    MYSQL = DataPlatformPair(
        powerbi_data_platform_name="MySQL",
        datahub_data_platform_name="mysql",
    )

    ODBC = DataPlatformPair(
        powerbi_data_platform_name="Odbc",
        datahub_data_platform_name="odbc",
    )


@dataclass
class PowerBiDashboardSourceReport(StaleEntityRemovalSourceReport):
    all_workspace_count: int = 0
    filtered_workspace_names: LossyList[str] = dataclass_field(
        default_factory=LossyList
    )
    filtered_workspace_types: LossyList[str] = dataclass_field(
        default_factory=LossyList
    )

    dashboards_scanned: int = 0
    charts_scanned: int = 0
    filtered_dashboards: LossyList[str] = dataclass_field(default_factory=LossyList)
    filtered_charts: LossyList[str] = dataclass_field(default_factory=LossyList)

    m_query_parse_timer: PerfTimer = dataclass_field(default_factory=PerfTimer)
    m_query_parse_attempts: int = 0
    m_query_parse_successes: int = 0
    m_query_parse_timeouts: int = 0
    m_query_parse_validation_errors: int = 0
    m_query_parse_unexpected_character_errors: int = 0
    m_query_parse_unknown_errors: int = 0
    m_query_resolver_errors: int = 0
    m_query_resolver_no_lineage: int = 0
    m_query_resolver_successes: int = 0

    pbix_export_attempts: int = 0
    pbix_export_successes: int = 0
    pbix_export_failures: int = 0
    dax_parse_attempts: int = 0
    dax_parse_successes: int = 0
    dax_parse_failures: int = 0

    def report_dashboards_scanned(self, count: int = 1) -> None:
        self.dashboards_scanned += count

    def report_charts_scanned(self, count: int = 1) -> None:
        self.charts_scanned += count

    def report_dashboards_dropped(self, model: str) -> None:
        self.filtered_dashboards.append(model)

    def report_charts_dropped(self, view: str) -> None:
        self.filtered_charts.append(view)


class DataBricksPlatformDetail(PlatformDetail):
    """
    metastore is an additional field used in Databricks connector to generate the dataset urn
    """

    metastore: str = pydantic.Field(
        description="Databricks Unity Catalog metastore name.",
    )


class OwnershipMapping(ConfigModel):
    create_corp_user: bool = pydantic.Field(
        default=True, description="Whether ingest PowerBI user as Datahub Corpuser"
    )
    use_powerbi_email: bool = pydantic.Field(
        default=True,
        description="Use PowerBI User email to ingest as corpuser, default is powerbi user identifier",
    )
    remove_email_suffix: bool = pydantic.Field(
        default=False,
        description="Remove PowerBI User email suffix for example, @acryl.io",
    )
    dataset_configured_by_as_owner: bool = pydantic.Field(
        default=False,
        description="Take PBI dataset configuredBy as dataset owner if exist",
    )
    owner_criteria: Optional[List[str]] = pydantic.Field(
        default=None,
        description="Need to have certain authority to qualify as owner for example ['ReadWriteReshareExplore','Owner','Admin']",
    )


class PowerBiProfilingConfig(ConfigModel):
    enabled: bool = pydantic.Field(
        default=False,
        description="Whether profiling of PowerBI datasets should be done",
    )


class PowerBiDashboardSourceConfig(
    StatefulIngestionConfigBase, DatasetSourceConfigMixin, IncrementalLineageConfigMixin
):
    platform_name: HiddenFromDocs[str] = pydantic.Field(default=Constant.PLATFORM_NAME)

    platform_urn: HiddenFromDocs[str] = pydantic.Field(
        default=builder.make_data_platform_urn(platform=Constant.PLATFORM_NAME),
    )

    tenant_id: str = pydantic.Field(description="PowerBI tenant identifier")
    workspace_id: HiddenFromDocs[Optional[str]] = pydantic.Field(
        default=None,
        description="[deprecated] Use workspace_id_pattern instead",
    )
    workspace_id_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter PowerBI workspaces in ingestion by ID."
        " By default all IDs are allowed unless they are filtered by name using 'workspace_name_pattern'."
        " Note: This field works in conjunction with 'workspace_type_filter' and both must be considered when filtering workspaces.",
    )
    workspace_name_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter PowerBI workspaces in ingestion by name."
        " By default all names are allowed unless they are filtered by ID using 'workspace_id_pattern'."
        " Note: This field works in conjunction with 'workspace_type_filter' and both must be considered when filtering workspaces.",
    )

    dataset_type_mapping: HiddenFromDocs[
        Union[Dict[str, str], Dict[str, PlatformDetail]]
    ] = pydantic.Field(
        default_factory=lambda: {},
        description="[deprecated] Use server_to_platform_instance instead. Mapping of PowerBI datasource type to "
        "DataHub supported datasources."
        "You can configured platform instance for dataset lineage. "
        "See Quickstart Recipe for mapping",
    )
    server_to_platform_instance: Dict[
        str, Union[PlatformDetail, DataBricksPlatformDetail]
    ] = pydantic.Field(
        default={},
        description="A mapping of PowerBI datasource's server i.e host[:port] to Data platform instance."
        " :port is optional and only needed if your datasource server is running on non-standard port. "
        "For Google BigQuery the datasource's server is google bigquery project name. "
        "For Databricks Unity Catalog the datasource's server is workspace FQDN.",
    )
    dsn_to_platform_name: Dict[str, str] = pydantic.Field(
        default={},
        description="A mapping of ODBC DSN to DataHub data platform name. "
        "For example with an ODBC connection string 'DSN=database' where the database type "
        "is 'PostgreSQL' you would configure the mapping as 'database: postgres'.",
    )
    dsn_to_database_schema: Dict[str, str] = pydantic.Field(
        default={},
        description="A mapping of ODBC DSN to database names with optional schema names "
        "(some database platforms such a MySQL use the table name pattern 'database.table', "
        "while others use the pattern 'database.schema.table'). "
        "This mapping is used in conjunction with ODBC SQL query parsing. "
        "If SQL queries used with ODBC do not reference fully qualified tables names, "
        "then you should configure mappings for your DSNs. "
        "For example with an ODBC connection string 'DSN=database' where the database "
        "is 'prod' you would configure the mapping as 'database: prod'. "
        "If the database is 'prod' and the schema is 'data' then mapping would be 'database: prod.data'.",
    )
    _dataset_type_mapping = pydantic_field_deprecated(
        "dataset_type_mapping",
        message="dataset_type_mapping is deprecated, use server_to_platform_instance instead",
    )
    client_id: str = pydantic.Field(description="Azure app client identifier")
    client_secret: str = pydantic.Field(description="Azure app client secret")
    scan_timeout: int = pydantic.Field(
        default=60, description="timeout for PowerBI metadata scanning"
    )
    scan_batch_size: int = pydantic.Field(
        default=1,
        gt=0,
        le=100,
        description="batch size for sending workspace_ids to PBI, 100 is the limit",
    )
    workspace_id_as_urn_part: bool = pydantic.Field(
        default=False,
        description="It is recommended to set this to True only if you have legacy workspaces based on Office 365 groups, as those workspaces can have identical names. "
        "To maintain backward compatibility, this is set to False which uses workspace name",
    )
    extract_ownership: bool = pydantic.Field(
        default=False,
        description="Whether ownership should be ingested. Admin API access is required if this setting is enabled. "
        "Note that enabling this may overwrite owners that you've added inside DataHub's web application.",
    )
    extract_reports: bool = pydantic.Field(
        default=True, description="Whether reports should be ingested"
    )
    ownership: OwnershipMapping = pydantic.Field(
        default=OwnershipMapping(),
        description="Configure how is ownership ingested",
    )
    modified_since: Optional[str] = pydantic.Field(
        default=None,
        description="Get only recently modified workspaces based on modified_since datetime '2023-02-10T00:00:00.0000000Z', excludeInActiveWorkspaces limit to last 30 days",
    )
    extract_dashboards: bool = pydantic.Field(
        default=True,
        description="Whether to ingest PBI Dashboard and Tiles as Datahub Dashboard and Chart",
    )
    extract_dataset_schema: bool = pydantic.Field(
        default=True,
        description="Whether to ingest PBI Dataset Table columns and measures."
        " Note: this setting must be `true` for schema extraction and column lineage to be enabled.",
    )
    extract_lineage: bool = pydantic.Field(
        default=True,
        description="Whether lineage should be ingested between X and Y. Admin API access is required if this setting is enabled",
    )
    extract_dataset_to_report_lineage: bool = pydantic.Field(
        default=True,
        description="Whether to extract lineage between datasets and reports (shown as datasetEdges in DashboardInfo). "
        "If disabled, reports will not show direct lineage to their source datasets.",
    )
    extract_endorsements_to_tags: bool = pydantic.Field(
        default=False,
        description="Whether to extract endorsements to tags, note that this may overwrite existing tags. "
        "Admin API access is required if this setting is enabled.",
    )
    filter_dataset_endorsements: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Filter and ingest datasets which are 'Certified' or 'Promoted' endorsement. If both are added, dataset which are 'Certified' or 'Promoted' will be ingested . Default setting allows all dataset to be ingested",
    )
    extract_workspaces_to_containers: bool = pydantic.Field(
        default=True, description="Extract workspaces to DataHub containers"
    )
    extract_datasets_to_containers: bool = pydantic.Field(
        default=False,
        description="PBI tables will be grouped under a Datahub Container, the container reflect a PBI Dataset",
    )
    native_query_parsing: bool = pydantic.Field(
        default=True,
        description="Whether PowerBI native query should be parsed to extract lineage",
    )

    convert_urns_to_lowercase: bool = pydantic.Field(
        default=False,
        description="Whether to convert the PowerBI assets urns to lowercase",
    )

    convert_lineage_urns_to_lowercase: bool = pydantic.Field(
        default=True,
        description="Whether to convert the urns of ingested lineage dataset to lowercase",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = pydantic.Field(
        default=None, description="PowerBI Stateful Ingestion Config."
    )
    admin_apis_only: bool = pydantic.Field(
        default=False,
        description="Retrieve metadata using PowerBI Admin API only. If this is enabled, then Report Pages will not "
        "be extracted. Admin API access is required if this setting is enabled",
    )
    extract_independent_datasets: bool = pydantic.Field(
        default=False,
        description="Whether to extract datasets not used in any PowerBI visualization",
    )

    platform_instance: Optional[str] = pydantic.Field(
        default=None,
        description="The instance of the platform that all assets produced by this recipe belong to",
    )

    enable_advance_lineage_sql_construct: bool = pydantic.Field(
        default=True,
        description="Whether to enable advance native sql construct for parsing like join, sub-queries. "
        "along this flag , the native_query_parsing should be enabled. "
        "By default convert_lineage_urns_to_lowercase is enabled, in-case if you have disabled it in previous "
        "ingestion execution then it may break lineage"
        "as this option generates the upstream datasets URN in lowercase.",
    )

    extract_column_level_lineage: bool = pydantic.Field(
        default=False,
        description="Whether to extract column level lineage. "
        "Works only if configs `native_query_parsing`, `enable_advance_lineage_sql_construct` & `extract_lineage` are "
        "enabled."
        "Works for M-Query where native SQL is used for transformation.",
    )

    profile_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter tables for profiling during ingestion. Note that only tables "
        "allowed by the `table_pattern` will be considered. Matched format is 'workspacename.datasetname.tablename'",
    )
    profiling: PowerBiProfilingConfig = PowerBiProfilingConfig()

    patch_metadata: bool = pydantic.Field(
        default=True,
        description="Patch dashboard metadata",
    )

    workspace_type_filter: List[
        Literal[
            "Workspace", "PersonalGroup", "Personal", "AdminWorkspace", "AdminInsights"
        ]
    ] = pydantic.Field(
        default=["Workspace"],
        description="Ingest the metadata of the workspace where the workspace type corresponds to the specified workspace_type_filter."
        " Note: This field works in conjunction with 'workspace_id_pattern'. Both must be matched for a workspace to be processed.",
    )

    include_workspace_name_in_dataset_urn: bool = pydantic.Field(
        default=False,
        description="It is recommended to set this to true, as it helps prevent the overwriting of datasets."
        "Read section #11560 at https://docs.datahub.com/docs/how/updating-datahub/ before enabling this option."
        "To maintain backward compatibility, this is set to False.",
    )

    extract_app: bool = pydantic.Field(
        default=False,
        description="Whether to ingest workspace app. Requires DataHub server 0.14.2+.",
    )

    m_query_parse_timeout: int = pydantic.Field(
        default=70,
        description="Timeout for PowerBI M-query parsing in seconds. Table-level lineage is determined by analyzing the M-query expression. "
        "Increase this value if you encounter the 'M-Query Parsing Timeout' message in the connector report.",
    )

    metadata_api_timeout: HiddenFromDocs[int] = pydantic.Field(
        default=30,
        description="timeout in seconds for Metadata Rest Api.",
    )

    extract_from_pbix_file: bool = pydantic.Field(
        default=False,
        description="Extract report and dataset metadata from .pbix files using ExportTo API. "
        "When enabled, report metadata (pages, visualizations, column-level lineage) will be extracted "
        "from downloaded .pbix files instead of using REST API. Falls back to skipping report if download fails.",
    )

    extract_reports_as_containers: bool = pydantic.Field(
        default=False,
        description="Extract Power BI Reports as Container entities (like Tableau Workbooks) instead of Dashboard entities. "
        "When enabled, the entity hierarchy becomes: Report (Container) -> Pages (Dashboards) -> Visualizations (Charts). "
        "When disabled (default), the legacy structure is used: Report (Dashboard) -> Pages (Charts). "
        "This option requires extract_from_pbix_file=True to extract individual visualizations and may need "
        "additional API permissions for PBIX export. Default is False for backward compatibility.",
    )

    @model_validator(mode="after")
    def validate_report_container_requirements(self) -> "PowerBiDashboardSourceConfig":
        if self.extract_reports_as_containers and not self.extract_from_pbix_file:
            raise ValueError(
                "extract_reports_as_containers=True requires extract_from_pbix_file=True to extract individual visualizations. "
                "Either set extract_from_pbix_file=True or use extract_reports_as_containers=False (default)."
            )
        return self

    @model_validator(mode="after")
    def validate_extract_column_level_lineage(self) -> "PowerBiDashboardSourceConfig":
        flags = [
            "native_query_parsing",
            "enable_advance_lineage_sql_construct",
            "extract_lineage",
            "extract_dataset_schema",
        ]

        if self.extract_column_level_lineage is False:
            return self

        logger.debug(f"Validating additional flags: {flags}")

        is_flag_enabled: bool = True
        for flag in flags:
            if not getattr(self, flag, True):
                is_flag_enabled = False

        if not is_flag_enabled:
            raise ValueError(f"Enable all these flags in recipe: {flags} ")

        return self

    @field_validator("dataset_type_mapping", mode="after")
    @classmethod
    def map_data_platform(cls, value):
        if "PostgreSql" in value:
            platform_name = value["PostgreSql"]
            del value["PostgreSql"]
            value["PostgreSQL"] = platform_name

        return value

    @model_validator(mode="after")
    def workspace_id_backward_compatibility(self) -> "PowerBiDashboardSourceConfig":
        if (
            self.workspace_id_pattern == AllowDenyPattern.allow_all()
            and self.workspace_id
        ):
            logger.warning(
                "workspace_id_pattern is not set but workspace_id is set, setting workspace_id as "
                "workspace_id_pattern. workspace_id will be deprecated, please use workspace_id_pattern instead."
            )
            self.workspace_id_pattern = AllowDenyPattern(
                allow=[f"^{self.workspace_id}$"]
            )
        elif (
            self.workspace_id_pattern != AllowDenyPattern.allow_all()
            and self.workspace_id
        ):
            logger.warning(
                "workspace_id will be ignored in favour of workspace_id_pattern. workspace_id will be deprecated, "
                "please use workspace_id_pattern only."
            )
            self.workspace_id = None
        return self

    @model_validator(mode="before")
    @classmethod
    def raise_error_for_dataset_type_mapping(cls, values: Dict) -> Dict:
        if (
            values.get("dataset_type_mapping") is not None
            and values.get("server_to_platform_instance") is not None
        ):
            raise ValueError(
                "dataset_type_mapping is deprecated. Use server_to_platform_instance only."
            )

        return values

    @model_validator(mode="after")
    def validate_extract_dataset_schema(self) -> "PowerBiDashboardSourceConfig":
        if self.extract_dataset_schema is False:
            add_global_warning(
                "Please use `extract_dataset_schema: true`, otherwise dataset schema extraction will be skipped."
            )
        return self

    @model_validator(mode="after")
    def validate_dsn_to_database_schema(self) -> "PowerBiDashboardSourceConfig":
        if self.dsn_to_database_schema is not None:
            dsn_mapping = self.dsn_to_database_schema
            if not isinstance(dsn_mapping, dict):
                raise ValueError("dsn_to_database_schema must contain key-value pairs")

            for _key, value in dsn_mapping.items():
                if not isinstance(value, str):
                    raise ValueError(
                        "dsn_to_database_schema mapping values must be strings"
                    )
                parts = value.split(".")
                if len(parts) != 1 and len(parts) != 2:
                    raise ValueError(
                        f"dsn_to_database_schema invalid mapping value: {value}"
                    )

        return self


# =============================================================================
# REST API Wrapper Data Classes
# =============================================================================

FIELD_TYPE_MAPPING: Dict[
    str,
    Union[
        BooleanTypeClass, DateTypeClass, NullTypeClass, NumberTypeClass, StringTypeClass
    ],
] = {
    "Int64": NumberTypeClass(),
    "Double": NumberTypeClass(),
    "Boolean": BooleanTypeClass(),
    "Datetime": DateTypeClass(),
    "DateTime": DateTypeClass(),
    "String": StringTypeClass(),
    "Decimal": NumberTypeClass(),
    "Null": NullTypeClass(),
}


class WorkspaceKey(ContainerKey):
    workspace: str


class ReportKey(ContainerKey):
    report: str


class DatasetKey(ContainerKey):
    dataset: str


@dataclass
class AppDashboard:
    id: str
    original_dashboard_id: str


@dataclass
class AppReport:
    id: str
    original_report_id: str


@dataclass
class App:
    id: str
    name: str
    description: Optional[str]
    last_update: Optional[str]
    dashboards: List["AppDashboard"]
    reports: List["AppReport"]

    def get_urn_part(self):
        return App.get_urn_part_by_id(self.id)

    @staticmethod
    def get_urn_part_by_id(id_: str) -> str:
        return f"apps.{id_}"


@dataclass
class Workspace:
    id: str
    name: str
    type: str
    dashboards: Dict[str, "Dashboard"]
    reports: Dict[str, "Report"]
    datasets: Dict[str, "PowerBIDataset"]
    report_endorsements: Dict[str, List[str]]
    dashboard_endorsements: Dict[str, List[str]]
    scan_result: dict
    independent_datasets: Dict[str, "PowerBIDataset"]
    app: Optional["App"]

    def get_urn_part(self, workspace_id_as_urn_part: Optional[bool] = False) -> str:
        return self.id if workspace_id_as_urn_part else self.name

    def get_workspace_key(
        self,
        platform_name: str,
        platform_instance: Optional[str] = None,
        workspace_id_as_urn_part: Optional[bool] = False,
    ) -> ContainerKey:
        return WorkspaceKey(
            workspace=self.get_urn_part(workspace_id_as_urn_part),
            platform=platform_name,
            instance=platform_instance,
        )

    def format_name_for_logger(self) -> str:
        return f"{self.name} ({self.id})"


@dataclass
class DataSource:
    id: str
    type: str
    raw_connection_detail: Dict

    def __members(self):
        return (self.id,)

    def __eq__(self, instance):
        return (
            isinstance(instance, DataSource)
            and self.__members() == instance.__members()
        )

    def __hash__(self):
        return hash(self.__members())


@dataclass
class MeasureProfile:
    min: Optional[str] = None
    max: Optional[str] = None
    unique_count: Optional[int] = None
    sample_values: Optional[List[str]] = None


@dataclass
class Column:
    name: str
    dataType: str
    isHidden: bool
    datahubDataType: Union[
        BooleanTypeClass, DateTypeClass, NullTypeClass, NumberTypeClass, StringTypeClass
    ]
    columnType: Optional[str] = None
    expression: Optional[str] = None
    description: Optional[str] = None
    measure_profile: Optional[MeasureProfile] = None


@dataclass
class Measure:
    name: str
    expression: str
    isHidden: bool
    dataType: str = "measure"
    datahubDataType: Union[
        BooleanTypeClass, DateTypeClass, NullTypeClass, NumberTypeClass, StringTypeClass
    ] = dataclasses.field(default_factory=NullTypeClass)
    description: Optional[str] = None
    measure_profile: Optional[MeasureProfile] = None


@dataclass
class Table:
    name: str
    full_name: str
    expression: Optional[str] = None
    columns: Optional[List[Column]] = None
    measures: Optional[List[Measure]] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    dataset: Optional["PowerBIDataset"] = None


@dataclass
class PowerBIDataset:
    id: str
    name: Optional[str]
    description: str
    webUrl: Optional[str]
    workspace_id: str
    workspace_name: str
    parameters: Dict[str, str]
    tables: List["Table"]
    tags: List[str]
    configuredBy: Optional[str] = None
    # PBIX-extracted metadata (optional, populated when extracted from PBIX)
    hierarchies: Optional[List["PBIXHierarchy"]] = None
    roles: Optional[List["PBIXRole"]] = None
    dataSources: Optional[List["PBIXDataSource"]] = None
    expressions: Optional[List["PBIXExpression"]] = None
    relationships: Optional[List[Union["PBIXRelationship", Dict[str, Any]]]] = None
    bookmarks: Optional[List[Dict[str, Any]]] = None
    interactions: Optional[Dict[str, Any]] = None

    def get_urn_part(self):
        return f"datasets.{self.id}"

    def __members(self):
        return (self.id,)

    def __eq__(self, instance):
        return (
            isinstance(instance, PowerBIDataset)
            and self.__members() == instance.__members()
        )

    def __hash__(self):
        return hash(self.__members())

    def get_dataset_key(self, platform_name: str) -> ContainerKey:
        return DatasetKey(
            dataset=self.id,
            platform=platform_name,
        )


@dataclass
class Page:
    id: str
    displayName: str
    name: str
    order: int

    def get_urn_part(self):
        return f"pages.{self.id}"


@dataclass
class User:
    id: str
    displayName: str
    emailAddress: str
    graphId: str
    principalType: str
    datasetUserAccessRight: Optional[str] = None
    reportUserAccessRight: Optional[str] = None
    dashboardUserAccessRight: Optional[str] = None
    groupUserAccessRight: Optional[str] = None

    def get_urn_part(self, use_email: bool, remove_email_suffix: bool) -> str:
        if use_email and self.emailAddress:
            if remove_email_suffix:
                return self.emailAddress.split("@")[0]
            else:
                return self.emailAddress
        return f"users.{self.id}"

    def __members(self):
        return (self.id,)

    def __eq__(self, instance):
        return isinstance(instance, User) and self.__members() == instance.__members()

    def __hash__(self):
        return hash(self.__members())


class ReportType(Enum):
    PaginatedReport = "PaginatedReport"
    PowerBIReport = "Report"


@dataclass
class Report:
    id: str
    name: str
    type: ReportType
    webUrl: Optional[str]
    embedUrl: str
    description: str
    dataset_id: Optional[str]
    dataset: Optional["PowerBIDataset"]
    pages: List["Page"]
    users: List["User"]
    tags: List[str]
    pbix_metadata: Optional[Union["PBIXExtractedMetadata", Dict[str, Any]]] = None

    def get_urn_part(self):
        return Report.get_urn_part_by_id(self.id)

    @staticmethod
    def get_urn_part_by_id(id_: str) -> str:
        return f"reports.{id_}"


@dataclass
class Tile:
    class CreatedFrom(Enum):
        REPORT = "Report"
        DATASET = "Dataset"
        VISUALIZATION = "Visualization"
        UNKNOWN = "UNKNOWN"

    id: str
    title: str
    embedUrl: str
    dataset_id: Optional[str]
    report_id: Optional[str]
    createdFrom: CreatedFrom
    dataset: Optional["PowerBIDataset"]
    report: Optional[Report]

    def get_urn_part(self):
        return f"charts.{self.id}"


@dataclass
class Dashboard:
    id: str
    displayName: str
    description: str
    embedUrl: str
    isReadOnly: Any
    workspace_id: str
    workspace_name: str
    tiles: List["Tile"]
    users: List["User"]
    tags: List[str]
    webUrl: Optional[str]

    def get_urn_part(self):
        return Dashboard.get_urn_part_by_id(self.id)

    @staticmethod
    def get_urn_part_by_id(id_: str) -> str:
        return f"dashboards.{id_}"

    def __members(self):
        return (self.id,)

    def __eq__(self, instance):
        return (
            isinstance(instance, Dashboard) and self.__members() == instance.__members()
        )

    def __hash__(self):
        return hash(self.__members())


# =============================================================================
# PBIX Parsing Models
# =============================================================================


class PBIXColumn(BaseModel):
    name: str
    dataType: str = "Unknown"
    formatString: Optional[str] = None
    isHidden: bool = False
    isNullable: bool = True
    sourceColumn: Optional[str] = None
    summarizeBy: Optional[str] = None
    expression: Optional[str] = None
    description: Optional[str] = Field(
        default=None, description="Column description/annotation"
    )
    displayFolder: Optional[str] = Field(
        default=None, description="Display folder for organization"
    )


class PBIXMeasure(BaseModel):
    name: str
    expression: str = ""
    formatString: Optional[str] = None
    isHidden: bool = False
    description: Optional[str] = Field(
        default=None, description="Measure description/annotation"
    )
    displayFolder: Optional[str] = Field(
        default=None, description="Display folder for organization"
    )


class PBIXPartition(BaseModel):
    name: str = "Partition"
    mode: str = "Import"
    source: Dict[str, Any] = Field(default_factory=dict)


class PBIXTable(BaseModel):
    name: str
    columns: List[PBIXColumn] = Field(default_factory=list)
    measures: List[PBIXMeasure] = Field(default_factory=list)
    partitions: List[PBIXPartition] = Field(default_factory=list)
    hierarchies: List["PBIXHierarchy"] = Field(default_factory=list)
    isHidden: bool = False
    description: Optional[str] = Field(
        default=None, description="Table description/annotation"
    )


class PBIXRelationship(BaseModel):
    name: str = ""
    fromTable: str
    fromColumn: str
    toTable: str
    toColumn: str
    crossFilteringBehavior: str = "oneDirection"
    isActive: bool = True
    securityFilteringBehavior: Optional[str] = Field(
        default=None,
        description="Security filtering behavior (e.g., 'oneDirection', 'bothDirections', 'none')",
    )
    fromCardinality: Optional[str] = Field(
        default=None, description="Cardinality on the from side (e.g., 'one', 'many')"
    )
    toCardinality: Optional[str] = Field(
        default=None, description="Cardinality on the to side (e.g., 'one', 'many')"
    )
    relyOnReferentialIntegrity: bool = Field(
        default=False, description="Whether to rely on referential integrity"
    )
    joinOnDateBehavior: Optional[str] = Field(
        default=None, description="Join behavior for date columns"
    )


class PBIXDataModel(BaseModel):
    tables: List[PBIXTable] = Field(default_factory=list)
    relationships: List[PBIXRelationship] = Field(default_factory=list)
    roles: List["PBIXRole"] = Field(default_factory=list)
    dataSources: List["PBIXDataSource"] = Field(default_factory=list)
    expressions: List["PBIXExpression"] = Field(default_factory=list)
    cultures: str = "en-US"
    version: str = "Unknown"


class PBIXDataModelRoot(BaseModel):
    model: Dict[str, Any] = Field(default_factory=dict)


class PBIXAggregationExpression(BaseModel):
    Column: Optional[Dict[str, Any]] = None
    Property: Optional[Dict[str, Any]] = None


class PBIXAggregation(BaseModel):
    Function: int = 0
    Expression: PBIXAggregationExpression = Field(
        default_factory=PBIXAggregationExpression
    )


class PBIXSelectItem(BaseModel):
    Aggregation: Optional[PBIXAggregation] = None
    Measure: Optional[Dict[str, Any]] = None
    Column: Optional[Dict[str, Any]] = None
    HierarchyLevel: Optional[Dict[str, Any]] = None
    Name: Optional[str] = None
    NativeReferenceName: Optional[str] = None


class PBIXBookmark(BaseModel):
    name: str
    displayName: Optional[str] = None
    id: Optional[str] = None
    explorationState: Dict[str, Any] = Field(default_factory=dict)
    options: Dict[str, Any] = Field(default_factory=dict)
    children: List[str] = Field(default_factory=list)


class PBIXParameter(BaseModel):
    name: str
    value: Optional[Union[str, int, float, bool]] = None
    type: Optional[str] = None
    expression: Optional[str] = None
    kind: Optional[str] = None


class PBIXDataTransformSelect(BaseModel):
    queryName: Optional[str] = None
    displayName: Optional[str] = None
    roles: Dict[str, bool] = Field(default_factory=dict)
    type: Dict[str, Any] = Field(default_factory=dict)
    format: Optional[str] = None


class VisualPosition(BaseModel):
    x: float = 0.0
    y: float = 0.0
    z: int = 0
    width: float = 0.0
    height: float = 0.0


class VisualColumnInfo(BaseModel):
    name: str
    nativeReferenceName: str = ""
    table: str = ""
    column: str = ""
    displayName: str = ""
    roles: Dict[str, bool] = Field(default_factory=dict)
    dataType: Optional[str] = None
    aggregation: Optional[str] = None
    type: str = "column"


class VisualMeasureInfo(BaseModel):
    name: str
    nativeReferenceName: str = ""
    entity: str = ""
    property: str = ""
    displayName: str = ""
    roles: Dict[str, bool] = Field(default_factory=dict)
    dataType: Optional[str] = None
    format: Optional[str] = None


class DataRole(BaseModel):
    queryRef: str
    active: bool = False


class VisualInfo(BaseModel):
    id: Optional[int] = None  # Integer ID in PBIX files
    position: VisualPosition = Field(default_factory=VisualPosition)
    visualType: str = "Unknown"
    columns: List[VisualColumnInfo] = Field(default_factory=list)
    measures: List[VisualMeasureInfo] = Field(default_factory=list)
    dataRoles: Dict[str, List[DataRole]] = Field(default_factory=dict)
    sectionName: Optional[str] = None
    sectionId: Optional[int] = None  # Integer ID in PBIX files


class SectionInfo(BaseModel):
    name: str
    displayName: str
    id: Optional[int] = None  # Integer ID in PBIX files
    width: float = 0.0
    height: float = 0.0
    visualContainers: List[VisualInfo] = Field(default_factory=list)
    filters: Union[List[Any], str] = Field(
        default_factory=list
    )  # Can be list or JSON string


class LayoutParsed(BaseModel):
    sections: List[SectionInfo] = Field(default_factory=list)
    themes: List[Any] = Field(default_factory=list)
    defaultLayout: Dict[str, Any] = Field(default_factory=dict)
    visualizations: List[VisualInfo] = Field(default_factory=list)
    bookmarks: List[Dict[str, Any]] = Field(default_factory=list)
    interactions: Dict[str, Any] = Field(default_factory=dict)


class ColumnLineageEntry(BaseModel):
    visualizationColumn: str
    displayName: str = ""
    sourceTable: str = ""
    sourceColumn: str = ""
    dataRole: List[str] = Field(default_factory=list)
    aggregation: Optional[str] = None
    columnType: str = "column"
    dataType: str = "String"  # Data type of the column for schema metadata
    page: str = ""
    pageId: Optional[int] = None  # Integer ID in PBIX files
    visualization: str = ""
    visualizationId: Optional[int] = None  # Integer ID in PBIX files
    lineagePath: List[str] = Field(default_factory=list)
    sourceColumnDetails: Optional[Dict[str, Any]] = None


class MeasureLineageEntry(BaseModel):
    visualizationMeasure: str
    displayName: str = ""
    sourceEntity: str = ""
    measureName: str = ""
    dataRole: List[str] = Field(default_factory=list)
    format: Optional[str] = None
    page: str = ""
    pageId: Optional[int] = None  # Integer ID in PBIX files
    visualization: str = ""
    visualizationId: Optional[int] = None  # Integer ID in PBIX files
    lineagePath: List[str] = Field(default_factory=list)
    sourceMeasureDetails: Optional[Dict[str, Any]] = None


class VisualizationLineage(BaseModel):
    visualizationId: Optional[int] = None  # Integer ID in PBIX files
    visualizationType: str = "Unknown"
    sectionName: Optional[str] = None
    sectionId: Optional[int] = None  # Integer ID in PBIX files
    columns: List[ColumnLineageEntry] = Field(default_factory=list)
    measures: List[MeasureLineageEntry] = Field(default_factory=list)


class PBIXVisualConfig(BaseModel):
    singleVisual: Dict[str, Any] = Field(default_factory=dict)


class PBIXVisualContainer(BaseModel):
    id: Optional[int] = None  # Integer ID in PBIX files
    x: Union[int, float] = 0  # Can be float for precise positioning
    y: Union[int, float] = 0
    z: int = 0
    width: Union[int, float] = 0
    height: Union[int, float] = 0
    config: Union[str, Dict[str, Any]] = "{}"
    query: Union[str, Dict[str, Any]] = "{}"
    dataTransforms: Optional[Union[str, Dict[str, Any]]] = None


class PBIXSection(BaseModel):
    name: str = ""
    displayName: str = ""
    id: Optional[Union[str, int]] = None  # Can be string or integer
    width: int = 0
    height: int = 0
    visualContainers: List[Dict[str, Any]] = Field(default_factory=list)
    filters: Union[List[Any], str] = Field(
        default_factory=list
    )  # Can be list or JSON string


class PBIXLayout(BaseModel):
    sections: List[PBIXSection] = Field(default_factory=list)
    themes: List[Any] = Field(default_factory=list)
    defaultLayout: Dict[str, Any] = Field(default_factory=dict)
    bookmarks: List[Dict[str, Any]] = Field(default_factory=list)
    config: Optional[Union[str, Dict[str, Any]]] = None


class ColumnReference(BaseModel):
    name: str = Field(description="Full column reference name")
    table: str = Field(default="", description="Source table name")
    column: str = Field(default="", description="Column name")
    display_name: str = Field(
        default="", description="Display name shown in the visual"
    )
    roles: Dict[str, bool] = Field(
        default_factory=dict, description="Data roles this column fills"
    )


class MeasureReference(BaseModel):
    name: str = Field(description="Full measure reference name")
    entity: str = Field(default="", description="Table/entity containing the measure")
    property: str = Field(default="", description="Measure property name")
    display_name: str = Field(
        default="", description="Display name shown in the visual"
    )
    roles: Dict[str, bool] = Field(
        default_factory=dict, description="Data roles this measure fills"
    )


class VisualizationMetadataPBIX(BaseModel):
    id: str = Field(description="Unique identifier for the visualization")
    visual_type: str = Field(
        description="Type of visual (e.g., 'barChart', 'table', 'pieChart')"
    )
    position_x: float = Field(default=0.0, description="X coordinate of visual")
    position_y: float = Field(default=0.0, description="Y coordinate of visual")
    width: float = Field(default=0.0, description="Width of visual")
    height: float = Field(default=0.0, description="Height of visual")
    z_index: int = Field(default=0, description="Z-index (layering) of visual")
    columns: List[ColumnReference] = Field(
        default_factory=list, description="Column references used"
    )
    measures: List[MeasureReference] = Field(
        default_factory=list, description="Measure references used"
    )
    filters: List[str] = Field(default_factory=list, description="Visual-level filters")
    title: Optional[str] = Field(default=None, description="Visual title if set")
    config_raw: Optional[Dict] = Field(
        default=None, description="Raw visual configuration"
    )


class BookmarkMetadata(BaseModel):
    name: str = Field(description="Internal name of the bookmark")
    display_name: Optional[str] = Field(
        default=None, description="User-facing display name"
    )
    id: str = Field(description="Unique identifier for the bookmark")
    exploration_state: Dict = Field(
        default_factory=dict, description="Captured state (filters, slicers, etc.)"
    )
    options: Dict = Field(
        default_factory=dict, description="Bookmark options and settings"
    )
    children: List[str] = Field(default_factory=list, description="Child bookmark IDs")


class ParameterMetadata(BaseModel):
    name: str = Field(description="Parameter name")
    parameter_type: Literal["what_if", "query", "m_query", "field"] = Field(
        description="Type of parameter"
    )
    value: Optional[Union[str, int, float, bool]] = Field(
        default=None, description="Current parameter value"
    )
    expression: Optional[str] = Field(
        default=None, description="M-Query or DAX expression defining the parameter"
    )


class VisualInteraction(BaseModel):
    visual_id: int = Field(description="ID of the visual - integer in PBIX files")
    section_id: int = Field(
        description="Page/section ID containing the visual - integer in PBIX files"
    )
    interactions: Dict = Field(
        default_factory=dict, description="Interaction configuration with other visuals"
    )


class PageMetadata(BaseModel):
    name: str = Field(description="Internal page name")
    display_name: str = Field(description="User-facing page display name")
    id: str = Field(description="Unique page identifier")
    width: float = Field(default=0.0, description="Page width")
    height: float = Field(default=0.0, description="Page height")
    visualizations: List[VisualizationMetadataPBIX] = Field(
        default_factory=list, description="Visualizations on this page"
    )
    filters: List[str] = Field(default_factory=list, description="Page-level filters")


class PBIXMetadata(BaseModel):
    file_info: Dict[str, Union[str, int]] = Field(
        description="File information (name, size, etc.)"
    )
    version: Optional[str] = Field(default=None, description="Power BI file version")
    pages: List[PageMetadata] = Field(default_factory=list, description="Report pages")
    bookmarks: List[BookmarkMetadata] = Field(
        default_factory=list, description="Report bookmarks"
    )
    parameters: List[ParameterMetadata] = Field(
        default_factory=list, description="Report parameters"
    )
    interactions: List[VisualInteraction] = Field(
        default_factory=list, description="Visual interaction settings"
    )
    data_model_raw: Optional[Dict] = Field(
        default=None, description="Raw data model JSON"
    )
    layout_raw: Optional[Dict] = Field(default=None, description="Raw layout JSON")


# =============================================================================
# DAX Parser Models
# =============================================================================


class TableColumnReference(BaseModel):
    """Represents a table.column reference in DAX."""

    table_name: str = Field(description="Name of the table being referenced")
    column_name: str = Field(
        description="Name of the column being referenced, empty string for table-only references"
    )

    class Config:
        frozen = True


class MeasureDependency(BaseModel):
    """Represents a measure dependency (measure referencing another measure)."""

    measure_name: str = Field(description="Name of the measure being referenced")
    source_table: Optional[str] = Field(
        default=None, description="Table containing the measure, if known"
    )


class DAXParameter(BaseModel):
    """Represents a parameter or placeholder in a DAX expression."""

    name: str = Field(description="Parameter name")
    parameter_type: Literal[
        "field_parameter", "what_if", "query_parameter", "slicer_value"
    ] = Field(description="Type of parameter")
    table_name: Optional[str] = Field(
        default=None, description="Associated table if applicable"
    )
    default_value: Optional[str] = Field(
        default=None, description="Default value if specified"
    )
    possible_values: List[str] = Field(
        default_factory=list,
        description="All possible values this parameter can take",
    )


class FilterReference(BaseModel):
    """Represents a table/column reference within a filter expression."""

    table: str = Field(description="Table name")
    column: str = Field(description="Column name")


class CalculateExpression(BaseModel):
    """Represents a CALCULATE or CALCULATETABLE expression with context."""

    function: Literal["CALCULATE", "CALCULATETABLE"] = Field(
        description="The function name"
    )
    nesting_depth: int = Field(
        description="How deeply nested this CALCULATE is (0 = top level)"
    )
    expression: str = Field(description="The expression being calculated")
    filters: List[str] = Field(
        default_factory=list, description="List of filter expressions"
    )
    filter_references: List[FilterReference] = Field(
        default_factory=list,
        description="Table/column references found in filter expressions",
    )
    full_content: str = Field(
        description="Full content of the CALCULATE call (truncated if too long)"
    )


class FilterContextModifier(BaseModel):
    """Represents a filter context modification function."""

    function: Literal[
        "ALL",
        "ALLEXCEPT",
        "ALLSELECTED",
        "FILTER",
        "REMOVEFILTERS",
        "KEEPFILTERS",
        "USERELATIONSHIP",
    ] = Field(description="The filter modifier function name")
    modifier_type: Literal[
        "remove_filter", "apply_filter", "keep_filter", "modify_relationship"
    ] = Field(description="Category of filter modification")
    arguments: Optional[str] = Field(
        default=None, description="Function arguments as string"
    )
    references: List[FilterReference] = Field(
        default_factory=list, description="Table/column references in the modifier"
    )
    table_expression: Optional[str] = Field(
        default=None, description="Table expression (for FILTER function)"
    )
    filter_expression: Optional[str] = Field(
        default=None, description="Filter predicate (for FILTER function)"
    )
    column1: Optional[str] = Field(
        default=None, description="First column in relationship"
    )
    column2: Optional[str] = Field(
        default=None, description="Second column in relationship"
    )


class DAXParseResult(BaseModel):
    """Result of parsing a DAX expression."""

    table_column_references: List[TableColumnReference] = Field(
        default_factory=list,
        description="List of table.column references found in the expression",
    )
    measure_references: List[str] = Field(
        default_factory=list,
        description="List of measure names referenced in the expression",
    )
    table_references: List[str] = Field(
        default_factory=list,
        description="List of table names referenced without specific columns",
    )
    variables: Dict[str, str] = Field(
        default_factory=dict,
        description="VAR declarations found in the expression",
    )
    parameters: List[DAXParameter] = Field(
        default_factory=list,
        description="Parameters and placeholders found in the expression",
    )
    calculate_expressions: List[CalculateExpression] = Field(
        default_factory=list,
        description="Nested CALCULATE expressions with their filter contexts",
    )
    filter_context_modifiers: List[FilterContextModifier] = Field(
        default_factory=list,
        description="Filter context modifiers",
    )

    @property
    def all_tables(self) -> Set[str]:
        """Get all unique table names referenced."""
        tables = set(self.table_references)
        tables.update(
            ref.table_name for ref in self.table_column_references if ref.table_name
        )
        return tables


class DirectColumnMapping(BaseModel):
    """Represents a direct column mapping in SUMMARIZE."""

    source_table: str = Field(description="Source table name")
    source_column: str = Field(description="Source column name")
    target_column: str = Field(description="Target column name in the result")


class CalculatedColumnMapping(BaseModel):
    """Represents a calculated column mapping in SUMMARIZE."""

    target_column: str = Field(description="Target column name")
    expression: str = Field(description="DAX expression for the calculated column")


class SummarizeParseResult(BaseModel):
    """Result of parsing a SUMMARIZE DAX expression."""

    source_table: str = Field(description="The source table being summarized")
    direct_mappings: List[DirectColumnMapping] = Field(
        default_factory=list, description="Direct column mappings from source to target"
    )
    calculated_mappings: List[CalculatedColumnMapping] = Field(
        default_factory=list, description="Calculated column mappings with expressions"
    )


# =============================================================================
# Lineage Builder Models
# =============================================================================


class TableExpressionLineageResult(BaseModel):
    """Result of extracting lineage from a table expression."""

    upstream_tables: List[UpstreamClass] = Field(
        default_factory=list, description="Upstream table dependencies"
    )
    column_edges: List["ColumnLineageEdge"] = Field(
        default_factory=list, description="Column-level lineage edges"
    )

    class Config:
        arbitrary_types_allowed = True


class MeasureLineageResult(BaseModel):
    """Result of extracting lineage from measures."""

    measure_edges: List["MeasureLineageEdge"] = Field(
        default_factory=list, description="Measure lineage edges"
    )
    upstream_tables: List[UpstreamClass] = Field(
        default_factory=list, description="Additional upstream tables discovered"
    )

    class Config:
        arbitrary_types_allowed = True


class TableReference(BaseModel):
    """A reference to a table in lineage extraction."""

    table_name: str = Field(description="Name of the table")
    table_urn: str = Field(description="DataHub URN for the table")
    is_dax_table: bool = Field(
        default=False, description="Whether this is a DAX calculated table"
    )


class ColumnLineageEdge(BaseModel):
    """Represents a column-level lineage edge."""

    source_table_urn: str = Field(description="URN of the source table")
    source_column: str = Field(description="Name of the source column")
    target_table_urn: str = Field(description="URN of the target table")
    target_column: str = Field(description="Name of the target column")
    transform_operation: str = Field(description="Type of transformation applied")


class MeasureLineageEdge(BaseModel):
    """Represents measure-level lineage."""

    source_urn: str = Field(description="URN of the source (can be column or measure)")
    source_name: str = Field(description="Name of the source")
    target_measure_urn: str = Field(description="URN of the target measure")
    target_measure_name: str = Field(description="Name of the target measure")
    transform_operation: str = Field(description="Type of transformation")
    is_measure_to_measure: bool = Field(
        default=False,
        description="True if source is a measure, False if source is a column",
    )


class LineageExtractionResult(BaseModel):
    """Result of extracting lineage from a Power BI table."""

    table_urn: str = Field(description="URN of the table being processed")
    table_name: str = Field(description="Name of the table")
    upstream_tables: List[UpstreamClass] = Field(
        default_factory=list, description="Table-level upstream lineage"
    )
    column_lineage_edges: List[ColumnLineageEdge] = Field(
        default_factory=list, description="Column-level lineage edges"
    )
    measure_lineage_edges: List[MeasureLineageEdge] = Field(
        default_factory=list, description="Measure lineage edges"
    )
    warnings: List[str] = Field(
        default_factory=list, description="Warnings encountered during extraction"
    )

    def to_fine_grained_lineage(self) -> List[FineGrainedLineage]:
        """Convert edges to DataHub FineGrainedLineage objects."""
        result: List[FineGrainedLineage] = []

        for edge in self.column_lineage_edges:
            result.append(
                FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    upstreams=[
                        builder.make_schema_field_urn(
                            edge.source_table_urn, edge.source_column
                        )
                    ],
                    downstreams=[
                        builder.make_schema_field_urn(
                            edge.target_table_urn, edge.target_column
                        )
                    ],
                    transformOperation=edge.transform_operation,
                )
            )

        measure_edge: MeasureLineageEdge
        for measure_edge in self.measure_lineage_edges:
            result.append(
                FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    upstreams=[measure_edge.source_urn],
                    downstreams=[
                        builder.make_schema_field_urn(
                            measure_edge.target_measure_urn,
                            measure_edge.target_measure_name,
                        )
                    ],
                    transformOperation=measure_edge.transform_operation,
                )
            )

        return result

    class Config:
        arbitrary_types_allowed = True


# =============================================================================
# Visualization Builder Models
# =============================================================================


class VisualizationColumn(BaseModel):
    """Represents a column used in a visualization."""

    source_table: str = Field(description="Name of the source table")
    source_column: str = Field(description="Name of the source column")
    data_type: str = Field(default="String", description="Data type of the column")
    display_name: Optional[str] = Field(
        default=None, description="Display name in visual"
    )


class VisualizationMeasure(BaseModel):
    """Represents a measure used in a visualization."""

    source_entity: str = Field(description="Entity/table containing the measure")
    measure_name: str = Field(description="Name of the measure")
    expression: Optional[str] = Field(
        default=None, description="DAX expression if available"
    )


class VisualizationMetadata(BaseModel):
    """Complete metadata for a Power BI visualization."""

    visualization_id: str = Field(description="Unique ID of the visualization")
    visualization_type: str = Field(default="visual", description="Type of visual")
    page_name: str = Field(description="Name of the page containing this visual")
    page_id: str = Field(description="ID of the page")
    columns: List[VisualizationColumn] = Field(
        default_factory=list, description="Columns used in the visualization"
    )
    measures: List[VisualizationMeasure] = Field(
        default_factory=list, description="Measures used in the visualization"
    )

    def get_all_source_tables(self) -> Set[str]:
        """Get all unique source table names."""
        tables: Set[str] = set()
        for col in self.columns:
            tables.add(col.source_table)
        for measure in self.measures:
            tables.add(measure.source_entity)
        return tables


class ChartWithLineage(BaseModel):
    """Represents a chart entity with its lineage information."""

    chart_urn: str = Field(description="URN of the chart")
    chart_info: ChartInfoClass = Field(description="Chart information aspect")
    input_fields: List[InputFieldClass] = Field(
        default_factory=list, description="Column-level lineage (InputFields)"
    )
    dataset_urns: Set[str] = Field(
        default_factory=set, description="URNs of datasets this chart uses"
    )

    class Config:
        arbitrary_types_allowed = True


# =============================================================================
# Hierarchy and RLS Models
# =============================================================================


class PBIXHierarchyLevel(BaseModel):
    """Represents a level within a hierarchy."""

    name: str = Field(description="Level name")
    ordinal: int = Field(description="Order of this level in the hierarchy (0-based)")
    column: str = Field(description="Column name this level is based on")
    lineageTag: Optional[str] = Field(
        default=None, description="Lineage tag for tracking"
    )


class PBIXHierarchy(BaseModel):
    """Represents a hierarchy in a Power BI table."""

    name: str = Field(description="Hierarchy name")
    levels: List[PBIXHierarchyLevel] = Field(
        default_factory=list, description="Levels in this hierarchy"
    )
    lineageTag: Optional[str] = Field(
        default=None, description="Lineage tag for tracking"
    )
    isHidden: bool = Field(default=False, description="Whether hierarchy is hidden")


class PBIXRolePermission(BaseModel):
    """Represents a row-level security role permission."""

    table: str = Field(description="Table name this permission applies to")
    filterExpression: str = Field(description="DAX filter expression for RLS")
    lineageTag: Optional[str] = Field(
        default=None, description="Lineage tag for tracking"
    )


class PBIXRole(BaseModel):
    """Represents a row-level security (RLS) role."""

    name: str = Field(description="Role name")
    modelPermission: str = Field(
        default="read", description="Permission level (e.g., 'read')"
    )
    tablePermissions: List[PBIXRolePermission] = Field(
        default_factory=list, description="Table-specific RLS permissions"
    )


class PBIXDataSource(BaseModel):
    """Represents a data source in the DataModel."""

    name: str = Field(description="Data source name")
    connectionString: Optional[str] = Field(
        default=None, description="Connection string (may be encrypted)"
    )
    type: Optional[str] = Field(default=None, description="Data source type")
    provider: Optional[str] = Field(default=None, description="Data provider")


class PBIXExpression(BaseModel):
    """Represents an M-Query expression in the DataModel."""

    name: str = Field(description="Expression name")
    kind: Optional[str] = Field(default=None, description="Expression kind")
    expression: Optional[str] = Field(
        default=None, description="M-Query expression text"
    )
    lineageTag: Optional[str] = Field(
        default=None, description="Lineage tag for tracking"
    )


# =============================================================================
# PBIX Parser Result Models (Strongly-typed return values)
# =============================================================================


class PBIXFileInfo(BaseModel):
    """File information from PBIX extraction."""

    filename: str = Field(description="Name of the PBIX file")
    size_bytes: int = Field(description="Size of the file in bytes")


class PBIXExtractResult(BaseModel):
    """Result of extracting and parsing a .pbix ZIP archive."""

    file_info: PBIXFileInfo = Field(description="File metadata")
    version: Optional[str] = Field(default=None, description="Power BI file version")
    metadata: Optional[Union[Dict[str, Any], str]] = Field(
        default=None, description="Metadata content (may be JSON or text)"
    )
    data_model: Optional[Dict[str, Any]] = Field(
        default=None, description="Raw data model JSON"
    )
    layout: Optional[Dict[str, Any]] = Field(
        default=None, description="Raw layout JSON"
    )
    file_list: List[str] = Field(
        default_factory=list, description="List of files in the archive"
    )


class PBIXTableParsed(BaseModel):
    """Parsed table from data model."""

    name: str = Field(description="Table name")
    columns: List[Dict[str, Any]] = Field(
        default_factory=list, description="Column definitions"
    )
    measures: List[Dict[str, Any]] = Field(
        default_factory=list, description="Measure definitions"
    )
    partitions: List[Dict[str, Any]] = Field(
        default_factory=list, description="Partition definitions"
    )
    isHidden: bool = Field(default=False, description="Whether table is hidden")


class PBIXDataModelParsed(BaseModel):
    """Parsed data model with validated structures."""

    tables: List[PBIXTableParsed] = Field(
        default_factory=list, description="Parsed tables"
    )
    relationships: List[Dict[str, Any]] = Field(
        default_factory=list, description="Table relationships"
    )
    hierarchies: List[PBIXHierarchy] = Field(
        default_factory=list, description="All hierarchies across tables"
    )
    roles: List[PBIXRole] = Field(
        default_factory=list, description="Row-level security roles"
    )
    dataSources: List[PBIXDataSource] = Field(
        default_factory=list, description="Data source connections"
    )
    expressions: List[PBIXExpression] = Field(
        default_factory=list, description="M-Query expressions"
    )
    cultures: str = Field(default="en-US", description="Culture/locale setting")
    version: str = Field(default="Unknown", description="Data model version")
    validated_model: Optional[PBIXDataModel] = Field(
        default=None, description="Validated Pydantic model for type-safe access"
    )

    class Config:
        arbitrary_types_allowed = True


class VisualizationUsage(BaseModel):
    """Represents how a visualization uses a column or measure."""

    visualization_id: Optional[int] = Field(
        description="Visualization ID - integer in PBIX files"
    )
    visualization_type: str = Field(description="Type of visualization")
    page: Optional[str] = Field(default=None, description="Page name")
    page_id: Optional[int] = Field(
        default=None, description="Page ID - integer in PBIX files"
    )
    data_role: List[str] = Field(
        default_factory=list,
        description="Data roles this field fills in the visualization",
    )


class PBIXColumnMapping(BaseModel):
    """Mapping of a column to its visualization usage."""

    table: str = Field(description="Source table name")
    column: str = Field(description="Column name")
    used_in_visualizations: List[VisualizationUsage] = Field(
        default_factory=list, description="List of visualizations using this column"
    )


class PBIXMeasureMapping(BaseModel):
    """Mapping of a measure to its visualization usage."""

    entity: str = Field(description="Entity/table name")
    measure: str = Field(description="Measure name")
    used_in_visualizations: List[VisualizationUsage] = Field(
        default_factory=list, description="List of visualizations using this measure"
    )


class VisualizationReference(BaseModel):
    """Simple reference to a visualization."""

    id: Optional[int] = Field(description="Visualization ID - integer in PBIX files")
    type: str = Field(description="Visualization type")
    page: Optional[str] = Field(default=None, description="Page name")
    page_id: Optional[int] = Field(
        default=None, description="Page ID - integer in PBIX files"
    )


class PBIXTableUsage(BaseModel):
    """Usage statistics for a table across visualizations."""

    columns: List[str] = Field(
        default_factory=list, description="Columns used from this table"
    )
    visualizations: List[VisualizationReference] = Field(
        default_factory=list, description="Visualizations using this table"
    )


class ColumnUsageOnPage(BaseModel):
    """Represents how a column is used on a page."""

    table: str = Field(description="Table name")
    column: str = Field(description="Column name")
    visualizations: List[VisualizationUsage] = Field(
        default_factory=list, description="Visualizations using this column"
    )


class MeasureUsageOnPage(BaseModel):
    """Represents how a measure is used on a page."""

    entity: str = Field(description="Entity/table name")
    measure: str = Field(description="Measure name")
    visualizations: List[VisualizationUsage] = Field(
        default_factory=list, description="Visualizations using this measure"
    )


class PBIXPageLineage(BaseModel):
    """Lineage information for a single page."""

    page_id: Optional[int] = Field(
        default=None, description="Page identifier - integer in PBIX files"
    )
    page_name: str = Field(description="Page name")
    columns: Dict[str, ColumnUsageOnPage] = Field(
        default_factory=dict, description="Columns used on this page"
    )
    measures: Dict[str, MeasureUsageOnPage] = Field(
        default_factory=dict, description="Measures used on this page"
    )
    tables: List[str] = Field(
        default_factory=list, description="Tables referenced on this page"
    )
    visualizations: List[VisualizationReference] = Field(
        default_factory=list, description="Visualizations on this page"
    )


class PageUsageInfo(BaseModel):
    """Information about which page uses a column/measure."""

    page: str = Field(description="Page name")
    page_id: Optional[int] = Field(
        default=None, description="Page ID - integer in PBIX files"
    )
    visualization_count: int = Field(
        description="Number of visualizations on this page using this field"
    )


class ColumnPageMapping(BaseModel):
    """Mapping of a column to pages that use it."""

    table: str = Field(description="Table name")
    column: str = Field(description="Column name")
    used_in_pages: List[PageUsageInfo] = Field(
        default_factory=list, description="Pages using this column"
    )


class MeasurePageMapping(BaseModel):
    """Mapping of a measure to pages that use it."""

    entity: str = Field(description="Entity/table name")
    measure: str = Field(description="Measure name")
    used_in_pages: List[PageUsageInfo] = Field(
        default_factory=list, description="Pages using this measure"
    )


class PBIXLineageResult(BaseModel):
    """Complete lineage information extracted from PBIX."""

    visualization_lineage: List[VisualizationLineage] = Field(
        default_factory=list, description="Per-visualization lineage details"
    )
    column_mapping: Dict[str, PBIXColumnMapping] = Field(
        default_factory=dict, description="Column to visualization mappings"
    )
    measure_mapping: Dict[str, PBIXMeasureMapping] = Field(
        default_factory=dict, description="Measure to visualization mappings"
    )
    table_usage: Dict[str, PBIXTableUsage] = Field(
        default_factory=dict, description="Table usage statistics"
    )
    page_lineage: Dict[str, PBIXPageLineage] = Field(
        default_factory=dict, description="Per-page lineage information"
    )
    page_mapping: Dict[str, Union[ColumnPageMapping, MeasurePageMapping]] = Field(
        default_factory=dict, description="Column/measure to page mappings"
    )


class PBIXLayoutParsedEnhanced(BaseModel):
    """Enhanced parsed layout with full type safety."""

    sections: List[SectionInfo] = Field(
        default_factory=list, description="Report pages/sections"
    )
    themes: List[Any] = Field(default_factory=list, description="Visual themes")
    default_layout: Dict[str, Any] = Field(
        default_factory=dict, description="Default layout settings"
    )
    visualizations: List[VisualInfo] = Field(
        default_factory=list, description="All visualizations across pages"
    )
    bookmarks: List[Dict[str, Any]] = Field(
        default_factory=list, description="Report bookmarks"
    )
    interactions: Dict[str, Any] = Field(
        default_factory=dict, description="Visual interaction settings"
    )

    class Config:
        arbitrary_types_allowed = True


class PBIXExtractedMetadata(BaseModel):
    """Complete metadata extracted from a PBIX file."""

    file_info: PBIXFileInfo = Field(description="File information")
    version: Optional[str] = Field(default=None, description="Power BI file version")
    metadata: Optional[Union[Dict[str, Any], str]] = Field(
        default=None, description="Raw metadata content"
    )
    file_list: List[str] = Field(
        default_factory=list, description="Files in the archive"
    )
    data_model_parsed: Optional[PBIXDataModelParsed] = Field(
        default=None, description="Parsed data model"
    )
    data_model_raw: Optional[Dict[str, Any]] = Field(
        default=None, description="Raw data model JSON"
    )
    layout_parsed: Optional[PBIXLayoutParsedEnhanced] = Field(
        default=None, description="Parsed layout"
    )
    layout_raw: Optional[Dict[str, Any]] = Field(
        default=None, description="Raw layout JSON"
    )
    parameters: List[Dict[str, Any]] = Field(
        default_factory=list, description="Report parameters"
    )
    lineage: Optional[PBIXLineageResult] = Field(
        default=None, description="Extracted lineage information"
    )

    class Config:
        arbitrary_types_allowed = True
