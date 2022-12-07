import pydantic
import datahub.emitter.mce_builder as builder

from dataclasses import field as dataclass_field
from typing import List

from dataclasses import  dataclass
from datahub.configuration.source_common import EnvBasedSourceConfigBase, DEFAULT_ENV
from typing import Dict, Union
from datahub.ingestion.api.source import SourceReport


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
    platform: str = pydantic.Field(description="DataHub platform name. Example postgres or oracle or snowflake")
    platform_instance: str = pydantic.Field(default=None, description="DataHub platform instance name. It should be same as you have used in ingestion receipe of DataHub platform ingestion source")
    env: str = pydantic.Field(
        default=DEFAULT_ENV,
        description="The environment that all assets produced by DataHub platform ingestion source belong to",
    )


class PowerBiAPIConfig(EnvBasedSourceConfigBase):
    # Organisation Identifier
    tenant_id: str = pydantic.Field(description="PowerBI tenant identifier")
    # PowerBi workspace identifier
    workspace_id: str = pydantic.Field(description="PowerBI workspace identifier")
    # Dataset type mapping PowerBI support many type of data-sources. Here user need to define what type of PowerBI
    # DataSource need to be mapped to corresponding DataHub Platform DataSource. For example PowerBI `Snowflake` is
    # mapped to DataHub `snowflake` PowerBI `PostgreSQL` is mapped to DataHub `postgres` and so on.
    dataset_type_mapping: Union[Dict[str, str], Dict[str, PlatformDetail]] = pydantic.Field(
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


class PowerBiDashboardSourceConfig(PowerBiAPIConfig):
    platform_name: str = "powerbi"
    platform_urn: str = builder.make_data_platform_urn(platform=platform_name)
