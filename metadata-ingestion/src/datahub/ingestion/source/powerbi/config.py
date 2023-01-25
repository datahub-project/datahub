import logging
from dataclasses import dataclass, field as dataclass_field
from typing import Dict, List, Optional, Union

import pydantic
from pydantic import validator
from pydantic.class_validators import root_validator

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DEFAULT_ENV, EnvBasedSourceConfigBase
from datahub.ingestion.api.source import SourceReport

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
    REPORT_GET = "REPORT_GET"
    DATASOURCE_GET = "DATASOURCE_GET"
    TILE_GET = "TILE_GET"
    ENTITY_USER_LIST = "ENTITY_USER_LIST"
    SCAN_CREATE = "SCAN_CREATE"
    SCAN_GET = "SCAN_GET"
    SCAN_RESULT_GET = "SCAN_RESULT_GET"
    Authorization = "Authorization"
    WorkspaceId = "WorkspaceId"
    DashboardId = "DashboardId"
    DatasetId = "DatasetId"
    ReportId = "ReportId"
    SCAN_ID = "ScanId"
    Dataset_URN = "DatasetURN"
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
    DASHBOARD_ID = "powerbi.linkedin.com/dashboards/{}"
    DASHBOARD = "dashboard"
    DASHBOARD_KEY = "dashboardKey"
    OWNERSHIP = "ownership"
    BROWSERPATH = "browsePaths"
    DASHBOARD_INFO = "dashboardInfo"
    DATAPLATFORM_INSTANCE = "dataPlatformInstance"
    DATASET = "dataset"
    DATASET_ID = "powerbi.linkedin.com/datasets/{}"
    DATASET_KEY = "datasetKey"
    DATASET_PROPERTIES = "datasetProperties"
    VALUE = "value"
    ENTITY = "ENTITY"
    ID = "ID"
    HTTP_RESPONSE_TEXT = "HttpResponseText"
    HTTP_RESPONSE_STATUS_CODE = "HttpResponseStatusCode"


@dataclass
class PowerBiDashboardSourceReport(SourceReport):
    dashboards_scanned: int = 0
    charts_scanned: int = 0
    filtered_dashboards: List[str] = dataclass_field(default_factory=list)
    filtered_charts: List[str] = dataclass_field(default_factory=list)

    def report_dashboards_scanned(self, count: int = 1) -> None:
        self.dashboards_scanned += count

    def report_charts_scanned(self, count: int = 1) -> None:
        self.charts_scanned += count

    def report_dashboards_dropped(self, model: str) -> None:
        self.filtered_dashboards.append(model)

    def report_charts_dropped(self, view: str) -> None:
        self.filtered_charts.append(view)


@dataclass
class PlatformDetail:
    platform_instance: Optional[str] = pydantic.Field(
        default=None,
        description="DataHub platform instance name. It should be same as you have used in ingestion receipe of DataHub platform ingestion source of particular platform",
    )
    env: str = pydantic.Field(
        default=DEFAULT_ENV,
        description="The environment that all assets produced by DataHub platform ingestion source belong to",
    )


class PowerBiAPIConfig(EnvBasedSourceConfigBase):
    # Organisation Identifier
    tenant_id: str = pydantic.Field(description="PowerBI tenant identifier")
    # PowerBi workspace identifier
    workspace_id: Optional[str] = pydantic.Field(
        description="[deprecated] Use workspace_id_pattern instead", default=None
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
        description="Mapping of PowerBI datasource type to DataHub supported data-sources. See Quickstart Recipe for mapping"
    )
    # Azure app client identifier
    client_id: str = pydantic.Field(description="Azure app client identifier")
    # Azure app client secret
    client_secret: str = pydantic.Field(description="Azure app client secret")
    # timeout for meta-data scanning
    scan_timeout: int = pydantic.Field(
        default=60, description="timeout for PowerBI metadata scanning"
    )
    # Enable/Disable extracting ownership information of Dashboard
    extract_ownership: bool = pydantic.Field(
        default=True, description="Whether ownership should be ingested"
    )
    # Enable/Disable extracting report information
    extract_reports: bool = pydantic.Field(
        default=True, description="Whether reports should be ingested"
    )
    # Enable/Disable extracting lineage information of PowerBI Dataset
    extract_lineage: bool = pydantic.Field(
        default=True, description="Whether lineage should be ingested"
    )
    # Enable/Disable extracting endorsements to tags. Please notice this may overwrite
    # any existing tags defined to those entitiies
    extract_endorsements_to_tags: bool = pydantic.Field(
        default=False,
        description="Whether to extract endorsements to tags, note that this may overwrite existing tags",
    )
    # Enable/Disable extracting workspace information to DataHub containers
    extract_workspaces_to_containers: bool = pydantic.Field(
        default=True, description="Extract workspaces to DataHub containers"
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

    @root_validator(pre=False)
    def workspace_id_backward_compatibility(cls, values: Dict) -> Dict:
        workspace_id = values.get("workspace_id")
        workspace_id_pattern = values.get("workspace_id_pattern")

        if workspace_id_pattern == AllowDenyPattern.allow_all() and workspace_id:
            logger.warning(
                "workspace_id_pattern is not set but workspace_id is set, setting workspace_id as workspace_id_pattern. workspace_id will be deprecated, please use workspace_id_pattern instead."
            )
            values["workspace_id_pattern"] = AllowDenyPattern(
                allow=[f"^{workspace_id}$"]
            )
        elif workspace_id_pattern != AllowDenyPattern.allow_all() and workspace_id:
            logger.warning(
                "workspace_id will be ignored in favour of workspace_id_pattern. workspace_id will be deprecated, please use workspace_id_pattern only."
            )
            values.pop("workspace_id")
        return values


class PowerBiDashboardSourceConfig(PowerBiAPIConfig):
    platform_name: str = "powerbi"
    platform_urn: str = builder.make_data_platform_urn(platform=platform_name)
