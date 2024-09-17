import logging
from dataclasses import dataclass, field as dataclass_field
from enum import Enum
from typing import Dict, List, Optional, Union

import pydantic
from pydantic import validator
from pydantic.class_validators import root_validator

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import DEFAULT_ENV, DatasetSourceConfigMixin
from datahub.configuration.validate_field_deprecation import pydantic_field_deprecated
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)

logger = logging.getLogger(__name__)


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

    DATABRICK_SQL = DataPlatformPair(
        powerbi_data_platform_name="Databricks", datahub_data_platform_name="databricks"
    )


@dataclass
class PowerBiDashboardSourceReport(StaleEntityRemovalSourceReport):
    dashboards_scanned: int = 0
    charts_scanned: int = 0
    filtered_dashboards: List[str] = dataclass_field(default_factory=list)
    filtered_charts: List[str] = dataclass_field(default_factory=list)
    number_of_workspaces: int = 0

    def report_dashboards_scanned(self, count: int = 1) -> None:
        self.dashboards_scanned += count

    def report_charts_scanned(self, count: int = 1) -> None:
        self.charts_scanned += count

    def report_dashboards_dropped(self, model: str) -> None:
        self.filtered_dashboards.append(model)

    def report_charts_dropped(self, view: str) -> None:
        self.filtered_charts.append(view)

    def report_number_of_workspaces(self, number_of_workspaces: int) -> None:
        self.number_of_workspaces = number_of_workspaces


def default_for_dataset_type_mapping() -> Dict[str, str]:
    dict_: dict = {}
    for item in SupportedDataPlatform:
        dict_[
            item.value.powerbi_data_platform_name
        ] = item.value.datahub_data_platform_name

    return dict_


class PlatformDetail(ConfigModel):
    platform_instance: Optional[str] = pydantic.Field(
        default=None,
        description="DataHub platform instance name. To generate correct urn for upstream dataset, this should match "
        "with platform instance name used in ingestion "
        "recipe of other datahub sources.",
    )
    env: str = pydantic.Field(
        default=DEFAULT_ENV,
        description="The environment that all assets produced by DataHub platform ingestion source belong to",
    )


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
        default=False,
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
    StatefulIngestionConfigBase, DatasetSourceConfigMixin
):
    platform_name: str = pydantic.Field(
        default=Constant.PLATFORM_NAME, hidden_from_docs=True
    )

    platform_urn: str = pydantic.Field(
        default=builder.make_data_platform_urn(platform=Constant.PLATFORM_NAME),
        hidden_from_docs=True,
    )

    # Organization Identifier
    tenant_id: str = pydantic.Field(description="PowerBI tenant identifier")
    # PowerBi workspace identifier
    workspace_id: Optional[str] = pydantic.Field(
        default=None,
        description="[deprecated] Use workspace_id_pattern instead",
        hidden_from_docs=True,
    )
    # PowerBi workspace identifier
    workspace_id_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter PowerBI workspaces in ingestion",
    )

    # Dataset type mapping PowerBI support many type of data-sources. Here user need to define what type of PowerBI
    # DataSource need to be mapped to corresponding DataHub Platform DataSource. For example PowerBI `Snowflake` is
    # mapped to DataHub `snowflake` PowerBI `PostgreSQL` is mapped to DataHub `postgres` and so on.
    dataset_type_mapping: Union[
        Dict[str, str], Dict[str, PlatformDetail]
    ] = pydantic.Field(
        default_factory=default_for_dataset_type_mapping,
        description="[deprecated] Use server_to_platform_instance instead. Mapping of PowerBI datasource type to "
        "DataHub supported datasources."
        "You can configured platform instance for dataset lineage. "
        "See Quickstart Recipe for mapping",
        hidden_from_docs=True,
    )
    # PowerBI datasource's server to platform instance mapping
    server_to_platform_instance: Dict[
        str, Union[PlatformDetail, DataBricksPlatformDetail]
    ] = pydantic.Field(
        default={},
        description="A mapping of PowerBI datasource's server i.e host[:port] to Data platform instance."
        " :port is optional and only needed if your datasource server is running on non-standard port. "
        "For Google BigQuery the datasource's server is google bigquery project name. "
        "For Databricks Unity Catalog the datasource's server is workspace FQDN.",
    )
    # deprecated warning
    _dataset_type_mapping = pydantic_field_deprecated(
        "dataset_type_mapping",
        message="dataset_type_mapping is deprecated, use server_to_platform_instance instead",
    )
    # Azure app client identifier
    client_id: str = pydantic.Field(description="Azure app client identifier")
    # Azure app client secret
    client_secret: str = pydantic.Field(description="Azure app client secret")
    # timeout for meta-data scanning
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
        description="Highly recommend changing this to True, as you can have the same workspace name"
        "To maintain backward compatability, this is set to False which uses workspace name",
    )
    # Enable/Disable extracting ownership information of Dashboard
    extract_ownership: bool = pydantic.Field(
        default=False,
        description="Whether ownership should be ingested. Admin API access is required if this setting is enabled. "
        "Note that enabling this may overwrite owners that you've added inside DataHub's web application.",
    )
    # Enable/Disable extracting report information
    extract_reports: bool = pydantic.Field(
        default=True, description="Whether reports should be ingested"
    )
    # Configure ingestion of ownership
    ownership: OwnershipMapping = pydantic.Field(
        default=OwnershipMapping(),
        description="Configure how is ownership ingested",
    )
    modified_since: Optional[str] = pydantic.Field(
        default=None,
        description="Get only recently modified workspaces based on modified_since datetime '2023-02-10T00:00:00.0000000Z', excludePersonalWorkspaces and excludeInActiveWorkspaces limit to last 30 days",
    )
    extract_dashboards: bool = pydantic.Field(
        default=True,
        description="Whether to ingest PBI Dashboard and Tiles as Datahub Dashboard and Chart",
    )
    # Enable/Disable extracting dataset schema
    extract_dataset_schema: bool = pydantic.Field(
        default=False,
        description="Whether to ingest PBI Dataset Table columns and measures",
    )
    # Enable/Disable extracting lineage information of PowerBI Dataset
    extract_lineage: bool = pydantic.Field(
        default=True,
        description="Whether lineage should be ingested between X and Y. Admin API access is required if this setting is enabled",
    )
    # Enable/Disable extracting endorsements to tags. Please notice this may overwrite
    # any existing tags defined to those entities
    extract_endorsements_to_tags: bool = pydantic.Field(
        default=False,
        description="Whether to extract endorsements to tags, note that this may overwrite existing tags. Admin API "
        "access is required is this setting is enabled",
    )
    filter_dataset_endorsements: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Filter and ingest datasets which are 'Certified' or 'Promoted' endorsement. If both are added, dataset which are 'Certified' or 'Promoted' will be ingested . Default setting allows all dataset to be ingested",
    )
    # Enable/Disable extracting workspace information to DataHub containers
    extract_workspaces_to_containers: bool = pydantic.Field(
        default=True, description="Extract workspaces to DataHub containers"
    )
    # Enable/Disable grouping PBI dataset tables into Datahub container (PBI Dataset)
    extract_datasets_to_containers: bool = pydantic.Field(
        default=False,
        description="PBI tables will be grouped under a Datahub Container, the container reflect a PBI Dataset",
    )
    # Enable/Disable extracting lineage information from PowerBI Native query
    native_query_parsing: bool = pydantic.Field(
        default=True,
        description="Whether PowerBI native query should be parsed to extract lineage",
    )

    # convert PowerBI dataset URN to lower-case
    convert_urns_to_lowercase: bool = pydantic.Field(
        default=False,
        description="Whether to convert the PowerBI assets urns to lowercase",
    )

    # convert lineage dataset's urns to lowercase
    convert_lineage_urns_to_lowercase: bool = pydantic.Field(
        default=True,
        description="Whether to convert the urns of ingested lineage dataset to lowercase",
    )
    # Configuration for stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = pydantic.Field(
        default=None, description="PowerBI Stateful Ingestion Config."
    )
    # Retrieve PowerBI Metadata using Admin API only
    admin_apis_only: bool = pydantic.Field(
        default=False,
        description="Retrieve metadata using PowerBI Admin API only. If this is enabled, then Report Pages will not "
        "be extracted. Admin API access is required if this setting is enabled",
    )
    # Extract independent datasets
    extract_independent_datasets: bool = pydantic.Field(
        default=False,
        description="Whether to extract datasets not used in any PowerBI visualization",
    )

    platform_instance: Optional[str] = pydantic.Field(
        default=None,
        description="The instance of the platform that all assets produced by this recipe belong to",
    )

    # Enable advance sql construct
    enable_advance_lineage_sql_construct: bool = pydantic.Field(
        default=True,
        description="Whether to enable advance native sql construct for parsing like join, sub-queries. "
        "along this flag , the native_query_parsing should be enabled. "
        "By default convert_lineage_urns_to_lowercase is enabled, in-case if you have disabled it in previous "
        "ingestion execution then it may break lineage"
        "as this option generates the upstream datasets URN in lowercase.",
    )

    # Enable CLL extraction
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

    @root_validator(skip_on_failure=True)
    def validate_extract_column_level_lineage(cls, values: Dict) -> Dict:
        flags = [
            "native_query_parsing",
            "enable_advance_lineage_sql_construct",
            "extract_lineage",
        ]

        if (
            "extract_column_level_lineage" in values
            and values["extract_column_level_lineage"] is False
        ):
            # Flag is not set. skip validation
            return values

        logger.debug(f"Validating additional flags: {flags}")

        is_flag_enabled: bool = True
        for flag in flags:
            if flag not in values or values[flag] is False:
                is_flag_enabled = False

        if not is_flag_enabled:
            raise ValueError(f"Enable all these flags in recipe: {flags} ")

        return values

    @validator("dataset_type_mapping")
    @classmethod
    def map_data_platform(cls, value):
        # For backward compatibility convert input PostgreSql to PostgreSQL
        # PostgreSQL is name of the data-platform in M-Query
        if "PostgreSql" in value.keys():
            platform_name = value["PostgreSql"]
            del value["PostgreSql"]
            value["PostgreSQL"] = platform_name

        return value

    @root_validator(skip_on_failure=True)
    def workspace_id_backward_compatibility(cls, values: Dict) -> Dict:
        workspace_id = values.get("workspace_id")
        workspace_id_pattern = values.get("workspace_id_pattern")

        if workspace_id_pattern == AllowDenyPattern.allow_all() and workspace_id:
            logger.warning(
                "workspace_id_pattern is not set but workspace_id is set, setting workspace_id as "
                "workspace_id_pattern. workspace_id will be deprecated, please use workspace_id_pattern instead."
            )
            values["workspace_id_pattern"] = AllowDenyPattern(
                allow=[f"^{workspace_id}$"]
            )
        elif workspace_id_pattern != AllowDenyPattern.allow_all() and workspace_id:
            logger.warning(
                "workspace_id will be ignored in favour of workspace_id_pattern. workspace_id will be deprecated, "
                "please use workspace_id_pattern only."
            )
            values.pop("workspace_id")
        return values

    @root_validator(pre=True)
    def raise_error_for_dataset_type_mapping(cls, values: Dict) -> Dict:
        if (
            values.get("dataset_type_mapping") is not None
            and values.get("server_to_platform_instance") is not None
        ):
            raise ValueError(
                "dataset_type_mapping is deprecated. Use server_to_platform_instance only."
            )

        return values
