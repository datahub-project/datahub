#########################################################
#
# Meta Data Ingestion From the PowerBI Report Server
#
#########################################################
import logging
from dataclasses import dataclass, field as dataclass_field
from typing import Any, Dict, Iterable, List, Optional

import pydantic
import requests
from requests.exceptions import ConnectionError
from requests_ntlm import HttpNtlmAuth

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import EnvConfigMixin
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.powerbi_report_server.constants import (
    API_ENDPOINTS,
    Constant,
)
from datahub.ingestion.source.powerbi_report_server.report_server_domain import (
    CorpUser,
    LinkedReport,
    Owner,
    OwnershipData,
    PowerBiReport,
    Report,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import ChangeAuditStamps
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    ChangeTypeClass,
    CorpUserInfoClass,
    CorpUserKeyClass,
    DashboardInfoClass,
    DashboardKeyClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
)
from datahub.utilities.dedup_list import deduplicate_list

LOGGER = logging.getLogger(__name__)


class PowerBiReportServerAPIConfig(EnvConfigMixin):
    username: str = pydantic.Field(description="Windows account username")
    password: str = pydantic.Field(description="Windows account password")
    workstation_name: str = pydantic.Field(
        default="localhost", description="Workstation name"
    )
    host_port: str = pydantic.Field(description="Power BI Report Server host URL")
    server_alias: str = pydantic.Field(
        default="", description="Alias for Power BI Report Server host URL"
    )
    graphql_url: Optional[str] = pydantic.Field(
        default=None, description="[deprecated] Not used"
    )
    report_virtual_directory_name: str = pydantic.Field(
        description="Report Virtual Directory URL name"
    )
    report_server_virtual_directory_name: str = pydantic.Field(
        description="Report Server Virtual Directory URL name"
    )
    extract_ownership: bool = pydantic.Field(
        default=True, description="Whether ownership should be ingested"
    )
    ownership_type: str = pydantic.Field(
        default=OwnershipTypeClass.NONE,
        description="Ownership type of owner",
    )

    @property
    def get_base_api_http_url(self):
        return "http://{}/{}/api/v2.0".format(
            self.host_port, self.report_virtual_directory_name
        )

    @property
    def get_base_api_https_url(self):
        return "https://{}/{}/api/v2.0".format(
            self.host_port, self.report_virtual_directory_name
        )

    @property
    def get_base_url(self):
        return "http://{}/{}/".format(
            self.host_port, self.report_virtual_directory_name
        )

    @property
    def host(self):
        return self.server_alias or self.host_port.split(":")[0]


class PowerBiReportServerDashboardSourceConfig(PowerBiReportServerAPIConfig):
    platform_name: str = "powerbi"
    platform_urn: str = builder.make_data_platform_urn(platform=platform_name)
    report_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    chart_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


def log_http_error(e: BaseException, message: str) -> Any:
    LOGGER.warning(message)

    if isinstance(e, requests.exceptions.HTTPError):
        LOGGER.warning(f"HTTP status-code = {e.response.status_code}")

    LOGGER.debug(msg=message, exc_info=e)

    return e


def get_response_dict(response: requests.Response, error_message: str) -> dict:

    result_dict: dict = {}
    try:
        response.raise_for_status()
        result_dict = response.json()
    except BaseException as e:
        log_http_error(e=e, message=error_message)

    return result_dict


class PowerBiReportServerAPI:
    # API endpoints of PowerBI Report Server to fetch reports, datasets

    def __init__(self, config: PowerBiReportServerAPIConfig) -> None:
        self.__config: PowerBiReportServerAPIConfig = config
        self.__auth: HttpNtlmAuth = HttpNtlmAuth(
            f"{self.__config.workstation_name}\\{self.__config.username}",
            self.__config.password,
        )

    @property
    def get_auth_credentials(self):
        return self.__auth

    def requests_get(self, url_http: str, url_https: str, content_type: str) -> Any:
        try:
            LOGGER.info(f"Request to Report URL={url_https}")
            response = requests.get(
                url=url_https,
                auth=self.get_auth_credentials,
                verify=True,
            )
        except ConnectionError:
            LOGGER.info(f"Request to Report URL={url_http}")
            response = requests.get(
                url=url_http,
                auth=self.get_auth_credentials,
            )

        error_message: str = (
            f"Failed to fetch {content_type} Report from powerbi-report-server"
        )

        return get_response_dict(
            response=response,
            error_message=error_message,
        )

    def get_all_reports(self) -> List[Any]:
        """
        Fetch all Reports from PowerBI Report Server
        """
        report_types_mapping: Dict[str, Any] = {
            Constant.REPORTS: Report,
            Constant.LINKED_REPORTS: LinkedReport,
            Constant.POWERBI_REPORTS: PowerBiReport,
        }

        reports: List[Any] = []
        for report_type in report_types_mapping.keys():
            report_get_endpoint: str = API_ENDPOINTS[report_type]
            # Replace place holders
            report_get_endpoint_http = report_get_endpoint.format(
                PBIRS_BASE_URL=self.__config.get_base_api_http_url,
            )
            report_get_endpoint_https = report_get_endpoint.format(
                PBIRS_BASE_URL=self.__config.get_base_api_https_url,
            )

            response_dict = self.requests_get(
                url_http=report_get_endpoint_http,
                url_https=report_get_endpoint_https,
                content_type=report_type,
            )

            if response_dict.get("value"):
                reports.extend(
                    report_types_mapping[report_type].parse_obj(report)
                    for report in response_dict.get("value")
                )

        return reports


class Mapper:
    """
    Transfrom PowerBI Report Server concept Report to DataHub concept Dashboard
    """

    class EquableMetadataWorkUnit(MetadataWorkUnit):
        """
        We can add EquableMetadataWorkUnit to set.
        This will avoid passing same MetadataWorkUnit to DataHub Ingestion framework.
        """

        def __eq__(self, instance):
            return self.id == self.id

        def __hash__(self):
            return id(self.id)

    def __init__(self, config: PowerBiReportServerDashboardSourceConfig):
        self.__config = config

    @staticmethod
    def new_mcp(
        entity_type,
        entity_urn,
        aspect_name,
        aspect,
        change_type=ChangeTypeClass.UPSERT,
    ):
        """
        Create MCP
        """
        return MetadataChangeProposalWrapper(
            entityType=entity_type,
            changeType=change_type,
            entityUrn=entity_urn,
            aspectName=aspect_name,
            aspect=aspect,
        )

    def __to_work_unit(
        self, mcp: MetadataChangeProposalWrapper
    ) -> EquableMetadataWorkUnit:
        return Mapper.EquableMetadataWorkUnit(
            id="{PLATFORM}-{ENTITY_URN}-{ASPECT_NAME}".format(
                PLATFORM=self.__config.platform_name,
                ENTITY_URN=mcp.entityUrn,
                ASPECT_NAME=mcp.aspectName,
            ),
            mcp=mcp,
        )

    @staticmethod
    def to_urn_set(mcps: List[MetadataChangeProposalWrapper]) -> List[str]:
        return deduplicate_list(
            [
                mcp.entityUrn
                for mcp in mcps
                if mcp is not None and mcp.entityUrn is not None
            ]
        )

    def to_ownership_set(
        self,
        mcps: List[MetadataChangeProposalWrapper],
        existing_owners: List[OwnerClass],
    ) -> List[Owner]:
        ownership = [
            Owner(owner=owner.owner, type=owner.type) for owner in existing_owners
        ]
        for mcp in mcps:
            if mcp is not None and mcp.entityUrn is not None:
                ownership.append(
                    Owner(owner=mcp.entityUrn, type=self.__config.ownership_type)
                )
        return deduplicate_list(ownership)

    def __to_datahub_dashboard(
        self,
        report: Report,
        chart_mcps: List[MetadataChangeProposalWrapper],
        user_mcps: List[MetadataChangeProposalWrapper],
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBI Report Server report to Datahub Dashboard
        """
        dashboard_urn = builder.make_dashboard_urn(
            self.__config.platform_name, report.get_urn_part()
        )

        chart_urn_list: List[str] = self.to_urn_set(chart_mcps)
        user_urn_list: List[Owner] = self.to_ownership_set(
            mcps=user_mcps, existing_owners=report.user_info.existing_owners
        )

        def custom_properties(
            _report: Report,
        ) -> dict:
            return {
                "workspaceName": "PowerBI Report Server",
                "workspaceId": self.__config.host_port,
                "createdBy": report.created_by,
                "createdDate": str(report.created_date),
                "modifiedBy": report.modified_by or "",
                "modifiedDate": str(report.modified_date) or str(report.created_date),
                "dataSource": (
                    str([report.connection_string for report in _report.data_sources])
                    if _report.data_sources
                    else ""
                ),
            }

        # DashboardInfo mcp
        dashboard_info_cls = DashboardInfoClass(
            description=report.description or "",
            title=report.name or "",
            charts=chart_urn_list,
            lastModified=ChangeAuditStamps(),
            dashboardUrl=report.get_web_url(self.__config.get_base_url),
            customProperties={**custom_properties(report)},
        )

        info_mcp = self.new_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.DASHBOARD_INFO,
            aspect=dashboard_info_cls,
        )

        # removed status mcp
        removed_status_mcp = self.new_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.STATUS,
            aspect=StatusClass(removed=False),
        )

        # dashboardKey mcp
        dashboard_key_cls = DashboardKeyClass(
            dashboardTool=self.__config.platform_name,
            dashboardId=Constant.DASHBOARD_ID.format(report.id),
        )

        # Dashboard key
        dashboard_key_mcp = self.new_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.DASHBOARD_KEY,
            aspect=dashboard_key_cls,
        )

        # Dashboard Ownership
        owners = [
            OwnerClass(owner=user.owner, type=user.type) for user in user_urn_list
        ]
        ownership = OwnershipClass(owners=owners)
        # Dashboard owner MCP
        owner_mcp = self.new_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.OWNERSHIP,
            aspect=ownership,
        )

        # Dashboard browsePaths
        browse_path = BrowsePathsClass(
            paths=[
                report.get_browse_path(
                    "powerbi_report_server",
                    self.__config.host,
                    self.__config.env,
                    self.__config.report_virtual_directory_name,
                )
            ]
        )
        browse_path_mcp = self.new_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.BROWSERPATH,
            aspect=browse_path,
        )

        return [
            browse_path_mcp,
            info_mcp,
            removed_status_mcp,
            dashboard_key_mcp,
            owner_mcp,
        ]

    def to_datahub_user(self, user: CorpUser) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBI Report Server user to datahub user
        """
        user_mcps = []
        if user:
            LOGGER.info(f"Converting user {user.username} to datahub's user")

            # Create an URN for User
            user_urn = builder.make_user_urn(user.get_urn_part())

            user_info_instance = CorpUserInfoClass(
                displayName=user.properties.display_name,
                email=user.properties.email,
                title=user.properties.title,
                active=True,
            )

            info_mcp = self.new_mcp(
                entity_type=Constant.CORP_USER,
                entity_urn=user_urn,
                aspect_name=Constant.CORP_USER_INFO,
                aspect=user_info_instance,
            )
            user_mcps.append(info_mcp)

            # removed status mcp
            status_mcp = self.new_mcp(
                entity_type=Constant.CORP_USER,
                entity_urn=user_urn,
                aspect_name=Constant.STATUS,
                aspect=StatusClass(removed=False),
            )
            user_mcps.append(status_mcp)
            user_key = CorpUserKeyClass(username=user.username)

            user_key_mcp = self.new_mcp(
                entity_type=Constant.CORP_USER,
                entity_urn=user_urn,
                aspect_name=Constant.CORP_USER_KEY,
                aspect=user_key,
            )
            user_mcps.append(user_key_mcp)

        return user_mcps

    def to_datahub_work_units(self, report: Report) -> List[EquableMetadataWorkUnit]:
        mcps = []
        user_mcps = []
        LOGGER.info(f"Converting Dashboard={report.name} to DataHub Dashboard")
        # Convert user to CorpUser
        user_info = report.user_info.owner_to_add
        if user_info:
            user_mcps = self.to_datahub_user(user_info)
        # Convert tiles to charts
        ds_mcps: List[Any]
        chart_mcps: List[Any]
        # ds_mcps = self.__to_datahub_dataset(report)
        chart_mcps, ds_mcps = [], []  # self.to_datahub_chart(dashboard.tiles)
        # Lets convert Dashboard to DataHub Dashboard
        dashboard_mcps = self.__to_datahub_dashboard(report, chart_mcps, user_mcps)

        # Now add MCPs in sequence
        mcps.extend(ds_mcps)
        mcps.extend(user_mcps)
        mcps.extend(chart_mcps)
        mcps.extend(dashboard_mcps)

        # Convert MCP to work_units
        work_units = map(self.__to_work_unit, mcps)
        # Return set of work_unit
        return deduplicate_list([wu for wu in work_units if wu is not None])


@dataclass
class PowerBiReportServerDashboardSourceReport(SourceReport):
    scanned_report: int = 0
    filtered_reports: List[str] = dataclass_field(default_factory=list)

    def report_scanned(self, count: int = 1) -> None:
        self.scanned_report += count

    def report_dropped(self, view: str) -> None:
        self.filtered_reports.append(view)


@platform_name("PowerBI")
@config_class(PowerBiReportServerDashboardSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.OWNERSHIP, "Enabled by default")
class PowerBiReportServerDashboardSource(Source):
    """
    Use this plugin to connect to [PowerBI Report Server](https://powerbi.microsoft.com/en-us/report-server/).
    It extracts the following:

    Metadata that can be ingested:
       - report name
       - report description
       - ownership(can add existing users in DataHub as owners)
       - transfer folders structure to DataHub as it is in Report Server
       - webUrl to report in Report Server

    Due to limits of PBIRS REST API, it's impossible to ingest next data for now:
       - tiles info
       - datasource of report
       - dataset of report

    Next types of report can be ingested:
       - PowerBI report(.pbix)
       - Paginated report(.rdl)
       - Linked report
    """

    source_config: PowerBiReportServerDashboardSourceConfig
    report: PowerBiReportServerDashboardSourceReport
    accessed_dashboards: int = 0

    def __init__(
        self, config: PowerBiReportServerDashboardSourceConfig, ctx: PipelineContext
    ):
        super().__init__(ctx)
        self.source_config = config
        self.report = PowerBiReportServerDashboardSourceReport()
        self.auth = PowerBiReportServerAPI(self.source_config).get_auth_credentials
        self.powerbi_client = PowerBiReportServerAPI(self.source_config)
        self.mapper = Mapper(config)

    @classmethod
    def create(cls, config_dict, ctx):
        config = PowerBiReportServerDashboardSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Datahub Ingestion framework invoke this method
        """
        LOGGER.info("PowerBI Report Server plugin execution is started")

        # Fetch PowerBI Report Server reports for given url
        reports = self.powerbi_client.get_all_reports()

        for report in reports:
            try:
                report.user_info = self.get_user_info(report)
            except pydantic.ValidationError as e:
                message = "Error ({}) occurred while loading User {}(id={})".format(
                    e, report.name, report.id
                )
                LOGGER.exception(message, e)
                self.report.report_warning(report.id, message)
            finally:
                # Increase Dashboard and tiles count in report
                self.report.report_scanned(count=1)
            # Convert PowerBi Report Server Dashboard and child entities
            # to Datahub work unit to ingest into Datahub
            yield from self.mapper.to_datahub_work_units(report)

    def get_user_info(self, report: Any) -> OwnershipData:
        existing_ownership: List[OwnerClass] = []
        if not self.source_config.extract_ownership:
            return OwnershipData(existing_owners=[], owner_to_add=None)
        dashboard_urn = builder.make_dashboard_urn(
            self.source_config.platform_name, report.get_urn_part()
        )
        user_urn = builder.make_user_urn(report.display_name)

        assert self.ctx.graph
        ownership = self.ctx.graph.get_ownership(entity_urn=dashboard_urn)
        if ownership:
            existing_ownership = ownership.owners
        if self.ctx.graph.get_aspect_v2(
            entity_urn=user_urn, aspect="corpUserInfo", aspect_type=CorpUserInfoClass
        ):
            existing_ownership.append(
                OwnerClass(owner=user_urn, type=self.source_config.ownership_type)
            )
            return OwnershipData(existing_owners=existing_ownership)
        user_data = dict(
            urn=user_urn,
            type=Constant.CORP_USER,
            username=report.display_name,
            properties=dict(active=True, displayName=report.display_name, email=""),
        )
        owner_to_add = CorpUser(**user_data)
        return OwnershipData(
            existing_owners=existing_ownership, owner_to_add=owner_to_add
        )

    def get_report(self) -> SourceReport:
        return self.report
