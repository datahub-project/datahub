#########################################################
#
# Meta Data Ingestion From the Power BI Report Server
#
#########################################################
import logging
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Dict, Iterable, List, Optional, Set

import requests
from orderedset import OrderedSet
from pydantic import ValidationError
from pydantic.fields import Field
from requests_ntlm import HttpNtlmAuth

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import EnvBasedSourceConfigBase
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

from .constants import API_ENDPOINTS, Constant
from .graphql_domain import CorpUser
from .report_server_domain import (
    LinkedReport,
    MobileReport,
    PowerBiReport,
    Report,
    SystemPolicies,
)

LOGGER = logging.getLogger(__name__)


class PowerBiReportServerAPIConfig(EnvBasedSourceConfigBase):
    username: str = Field(description="Windows account username")
    password: str = Field(description="Windows account password")
    workstation_name: str = Field(default="localhost", description="Workstation name")
    host_port: str = Field(description="Power BI Report Server host URL")
    server_alias: str = Field(
        default="", description="Alias for Power BI Report Server host URL"
    )
    graphql_url: str = Field(description="GraphQL API URL")
    report_virtual_directory_name: str = Field(
        description="Report Virtual Directory URL name"
    )
    report_server_virtual_directory_name: str = Field(
        description="Report Server Virtual Directory URL name"
    )
    dataset_type_mapping: Dict[str, str] = Field(
        default={},
        description="Mapping of Power BI DataSource type to Datahub DataSet.",
    )
    scan_timeout: int = Field(
        default=60,
        description="time in seconds to wait for Power BI metadata scan result.",
    )

    @property
    def get_base_api_url(self):
        return "http://{}/{}/api/v2.0/".format(
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


class PowerBiReportServerAPI:
    # API endpoints of PowerBi Report Server to fetch reports, datasets

    def __init__(self, config: PowerBiReportServerAPIConfig) -> None:
        self.__config: PowerBiReportServerAPIConfig = config
        self.__auth: HttpNtlmAuth = HttpNtlmAuth(
            "{}\\{}".format(self.__config.workstation_name, self.__config.username),
            self.__config.password,
        )

    def get_auth_credentials(self):
        return self.__auth

    def get_users_policies(self) -> List[SystemPolicies]:
        """
        Get User policy by Power Bi Report Server System
        """
        user_list_endpoint: str = API_ENDPOINTS[Constant.SYSTEM_POLICIES]
        # Replace place holders
        user_list_endpoint = user_list_endpoint.format(
            PBIRS_BASE_URL=self.__config.get_base_api_url
        )
        # Hit PowerBi Report Server
        LOGGER.info("Request to URL={}".format(user_list_endpoint))
        response = requests.get(
            url=user_list_endpoint, auth=self.get_auth_credentials()
        )

        # Check if we got response from PowerBi Report Server
        if response.status_code != 200:
            LOGGER.warning(
                "Failed to fetch User list from power-bi for, http_status={}, message={}".format(
                    response.status_code, response.text
                )
            )
            raise ConnectionError("Failed to fetch the User list from the power-bi")

        users_dict: List[Any] = response.json()[Constant.VALUE]
        # Iterate through response and create a list of PowerBiReportServerAPI.Dashboard
        users: List[SystemPolicies] = [
            SystemPolicies.parse_obj(instance) for instance in users_dict
        ]
        return users

    def get_user_policies(self, user_name: str) -> Optional[SystemPolicies]:
        users_policies = self.get_users_policies()
        for user_policy in users_policies:
            if user_policy.group_user_name == user_name:
                return user_policy
        return None

    def get_report(self, report_id: str) -> Optional[Report]:
        """
        Fetch the .rdl Report from PowerBiReportServer for the given Report Id
        """
        if report_id is None:
            LOGGER.info("Input value is None")
            LOGGER.info("{}={}".format(Constant.ReportId, report_id))
            return None

        report_get_endpoint: str = API_ENDPOINTS[Constant.REPORT]
        # Replace place holders
        report_get_endpoint = report_get_endpoint.format(
            PBIRS_BASE_URL=self.__config.get_base_api_url,
            REPORT_ID=report_id,
        )
        # Hit PowerBiReportServer
        LOGGER.info("Request to Report URL={}".format(report_get_endpoint))
        response = requests.get(
            url=report_get_endpoint,
            auth=self.get_auth_credentials(),
        )

        # Check if we got response from PowerBi Report Server
        if response.status_code != 200:
            message: str = "Failed to fetch Report from power-bi-report-server for"
            LOGGER.warning(message)
            LOGGER.warning("{}={}".format(Constant.ReportId, report_id))
            raise ConnectionError(message)

        response_dict = response.json()

        return Report.parse_obj(response_dict)

    def get_powerbi_report(self, report_id: str) -> Optional[PowerBiReport]:
        """
        Fetch the .pbix Report from PowerBiReportServer for the given Report Id
        """
        if report_id is None:
            LOGGER.info("Input value is None")
            LOGGER.info("{}={}".format(Constant.ReportId, report_id))
            return None

        powerbi_report_get_endpoint: str = API_ENDPOINTS[Constant.POWERBI_REPORT]
        # Replace place holders
        powerbi_report_get_endpoint = powerbi_report_get_endpoint.format(
            PBIRS_BASE_URL=self.__config.get_base_api_url,
            POWERBI_REPORT_ID=report_id,
        )
        # Hit PowerBiReportServer
        LOGGER.info("Request to Report URL={}".format(powerbi_report_get_endpoint))
        response = requests.get(
            url=powerbi_report_get_endpoint,
            auth=self.get_auth_credentials(),
        )

        # Check if we got response from PowerBi Report Server
        if response.status_code != 200:
            message: str = "Failed to fetch Report from power-bi-report-server for"
            LOGGER.warning(message)
            LOGGER.warning("{}={}".format(Constant.ReportId, report_id))
            raise ConnectionError(message)

        response_dict = response.json()
        return PowerBiReport.parse_obj(response_dict)

    def get_linked_report(self, report_id: str) -> Optional[LinkedReport]:
        """
        Fetch the Mobile Report from PowerBiReportServer for the given Report Id
        """
        if report_id is None:
            LOGGER.info("Input value is None")
            LOGGER.info("{}={}".format(Constant.ReportId, report_id))
            return None

        linked_report_get_endpoint: str = API_ENDPOINTS[Constant.LINKED_REPORT]
        # Replace place holders
        linked_report_get_endpoint = linked_report_get_endpoint.format(
            PBIRS_BASE_URL=self.__config.get_base_api_url,
            LINKED_REPORT_ID=report_id,
        )
        # Hit PowerBiReportServer
        LOGGER.info("Request to Report URL={}".format(linked_report_get_endpoint))
        response = requests.get(
            url=linked_report_get_endpoint,
            auth=self.get_auth_credentials(),
        )

        # Check if we got response from PowerBi Report Server
        if response.status_code != 200:
            message: str = "Failed to fetch Report from power-bi-report-server for"
            LOGGER.warning(message)
            LOGGER.warning("{}={}".format(Constant.ReportId, report_id))
            raise ConnectionError(message)

        response_dict = response.json()

        return LinkedReport.parse_obj(response_dict)

    def get_mobile_report(self, report_id: str) -> Optional[MobileReport]:
        """
        Fetch the Mobile Report from PowerBiReportServer for the given Report Id
        """
        if report_id is None:
            LOGGER.info("Input value is None")
            LOGGER.info("{}={}".format(Constant.ReportId, report_id))
            return None

        mobile_report_get_endpoint: str = API_ENDPOINTS[Constant.MOBILE_REPORT]
        # Replace place holders
        mobile_report_get_endpoint = mobile_report_get_endpoint.format(
            PBIRS_BASE_URL=self.__config.get_base_api_url,
            MOBILE_REPORT_ID=report_id,
        )
        # Hit PowerBi ReportServer
        LOGGER.info("Request to Report URL={}".format(mobile_report_get_endpoint))
        response = requests.get(
            url=mobile_report_get_endpoint,
            auth=self.get_auth_credentials(),
        )

        # Check if we got response from PowerBi Report Server
        if response.status_code != 200:
            message: str = "Failed to fetch Report from power-bi-report-server for"
            LOGGER.warning(message)
            LOGGER.warning("{}={}".format(Constant.ReportId, report_id))
            raise ConnectionError(message)

        response_dict = response.json()

        return MobileReport.parse_obj(response_dict)

    def get_all_reports(self) -> List[Any]:
        """
        Fetch all Reports from PowerBiReportServer
        """
        report_types_mapping: Dict[str, Any] = {
            Constant.REPORTS: Report,
            Constant.MOBILE_REPORTS: MobileReport,
            Constant.LINKED_REPORTS: LinkedReport,
            Constant.POWERBI_REPORTS: PowerBiReport,
        }

        reports: List[Any] = []
        for report_type in report_types_mapping.keys():

            report_get_endpoint: str = API_ENDPOINTS[report_type]
            # Replace place holders
            report_get_endpoint = report_get_endpoint.format(
                PBIRS_BASE_URL=self.__config.get_base_api_url,
            )
            # Hit PowerBi ReportServer
            LOGGER.info("Request to Report URL={}".format(report_get_endpoint))
            response = requests.get(
                url=report_get_endpoint,
                auth=self.get_auth_credentials(),
            )

            # Check if we got response from PowerBi Report Server
            if response.status_code != 200:
                message: str = "Failed to fetch Report from power-bi-report-server for"
                LOGGER.warning(message)
                LOGGER.warning("{}={}".format(Constant.ReportId, report_type))
                raise ValueError(message)

            response_dict = response.json()["value"]
            if response_dict:
                reports.extend(
                    report_types_mapping[report_type].parse_obj(report)
                    for report in response_dict
                )

        return reports


class UserDao:
    def __init__(self, config: PowerBiReportServerDashboardSourceConfig):
        self.__config = config

    def _run_query(self, query) -> Dict[str, Any]:
        request = requests.post(url=self.__config.graphql_url, json={"query": query})
        if request.status_code == 200:
            return request.json()
        else:
            raise Exception(
                "Query failed to run by returning code of {}. {}".format(
                    request.status_code, query
                )
            )

    def _get_owner_by_name(self, user_name: str) -> Dict[str, Any]:
        get_owner = f"""{{
        search(input: {{ type: CORP_USER, query: "{user_name}"}}){{
            searchResults{{
                entity{{
                     urn
                     type
                     ...on CorpUser {{
                        username
                        urn
                        type
                        properties {{
                            active
                            displayName
                            email
                            title
                        }}
                    }}
                }}
              matchedFields {{
                name
                value
                }}
            }}
        }}
    }}
    """

        result = self._run_query(get_owner)
        return result

    @staticmethod
    def _filter_values(response: Dict[str, Any], mask: str) -> Optional[Dict[str, Any]]:
        users = response["data"]["search"]["searchResults"]
        for user in users:
            if user["matchedFields"][0]["value"] == mask:
                return user["entity"]
        return None

    def get_owner_by_name(self, user_name: str) -> Optional[CorpUser]:
        response = self._get_owner_by_name(user_name=user_name)
        filtered_response = self._filter_values(response=response, mask=user_name)
        if filtered_response:
            return CorpUser.parse_obj(filtered_response)

        user_data = dict(
            urn=f"urn:li:corpuser:{user_name}",
            type=Constant.CORP_USER,
            username=user_name,
            properties=dict(active=True, displayName=user_name, email=""),
        )
        return CorpUser.parse_obj(user_data)


class Mapper:
    """
    Transfrom PowerBi Report Server concept Report to DataHub concept Dashboard
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
        return list(
            OrderedSet(
                [
                    mcp.entityUrn
                    for mcp in mcps
                    if mcp is not None and mcp.entityUrn is not None
                ]
            )
        )

    def __to_datahub_dashboard(
        self,
        report: Report,
        chart_mcps: List[MetadataChangeProposalWrapper],
        user_mcps: List[MetadataChangeProposalWrapper],
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi Report Server report to Datahub Dashboard
        """

        dashboard_urn = builder.make_dashboard_urn(
            self.__config.platform_name, report.get_urn_part()
        )

        chart_urn_list: List[str] = self.to_urn_set(chart_mcps)
        user_urn_list: List[str] = self.to_urn_set(user_mcps)

        def custom_properties(
            _report: Report,
        ) -> dict:
            return {
                "workspaceName": "PowerBI Report Server",
                "workspaceId": self.__config.host_port,
                "dataSource": str(
                    [report.connection_string for report in _report.data_sources]
                )
                if _report.data_sources
                else "",
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
            OwnerClass(owner=user_urn, type=OwnershipTypeClass.BUSINESS_OWNER)
            for user_urn in user_urn_list
            if user_urn is not None
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
            paths=[report.get_browse_path("powerbi_report_server", self.__config.host)]
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
        Map PowerBi ReportServer user to datahub user
        """
        LOGGER.info("Converting user {} to datahub's user".format(user.username))

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

        # removed status mcp
        status_mcp = self.new_mcp(
            entity_type=Constant.CORP_USER,
            entity_urn=user_urn,
            aspect_name=Constant.STATUS,
            aspect=StatusClass(removed=False),
        )

        user_key = CorpUserKeyClass(username=user.username)

        user_key_mcp = self.new_mcp(
            entity_type=Constant.CORP_USER,
            entity_urn=user_urn,
            aspect_name=Constant.CORP_USER_KEY,
            aspect=user_key,
        )

        return [info_mcp, status_mcp, user_key_mcp]

    def to_datahub_work_units(self, report: Report) -> Set[EquableMetadataWorkUnit]:
        mcps = []

        LOGGER.info("Converting Dashboard={} to DataHub Dashboard".format(report.name))
        # Convert user to CorpUser
        user_mcps = self.to_datahub_user(report.user_info)
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
        return OrderedSet([wu for wu in work_units if wu is not None])


@dataclass
class PowerBiReportServerDashboardSourceReport(SourceReport):
    scanned_report: int = 0
    filtered_reports: List[str] = dataclass_field(default_factory=list)

    def report_scanned(self, count: int = 1) -> None:
        self.scanned_report += count

    def report_dropped(self, view: str) -> None:
        self.filtered_reports.append(view)


@platform_name("PowerBIReportServer")
@config_class(PowerBiReportServerDashboardSourceConfig)
@support_status(SupportStatus.UNKNOWN)
@capability(SourceCapability.OWNERSHIP, "Enabled by default")
class PowerBiReportServerDashboardSource(Source):
    """
        This plugin extracts the following:

    - Power BI Dashboards, tiles, datasets
    - Names, descriptions and URLs of Dashboard and tile
    - Owners of Dashboards

    ## Configuration Notes

    See the
    1.  [Microsoft AD App Creation doc]
        (https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal)
        for the steps to create a app client ID and secret.
    2.  Login to Power BI as Admin and from `Tenant settings` allow below permissions.
        - Allow service principles to use Power BI APIs
        - Allow service principals to use read-only Power BI admin APIs
        - Enhance admin APIs responses with detailed metadata
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
        self.auth = PowerBiReportServerAPI(self.source_config).get_auth_credentials()
        self.powerbi_client = PowerBiReportServerAPI(self.source_config)
        self.mapper = Mapper(config)
        self.user_dao = UserDao(config)

    @classmethod
    def create(cls, config_dict, ctx):
        config = PowerBiReportServerDashboardSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Datahub Ingestion framework invoke this method
        """
        LOGGER.info("PowerBiReportServer plugin execution is started")

        # Fetch PowerBiReportServer reports for given url
        # workspace = self.powerbi_client.get_workspace(self.source_config.workspace_id)
        reports = self.powerbi_client.get_all_reports()

        for report in reports:
            try:
                report.user_info = self.user_dao.get_owner_by_name(
                    user_name=report.display_name
                )
            except ValidationError as e:
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
            workunits = self.mapper.to_datahub_work_units(report)
            for workunit in workunits:
                # Add workunit to report
                self.report.report_workunit(workunit)
                # Return workunit to Datahub Ingestion framework
                yield workunit

    def get_report(self) -> SourceReport:
        return self.report

    def close(self):
        pass
