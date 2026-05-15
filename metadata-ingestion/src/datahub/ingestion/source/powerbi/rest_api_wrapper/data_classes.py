import dataclasses
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.source.powerbi.config import Constant
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    StringTypeClass,
)

logger = logging.getLogger(__name__)

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
    type: str  # This is used as a subtype of the Container entity.
    dashboards: Dict[str, "Dashboard"]  # key = dashboard id
    reports: Dict[str, "Report"]  # key = report id
    datasets: Dict[str, "PowerBIDataset"]  # key = dataset id
    report_endorsements: Dict[str, List[str]]  # key = report id
    dashboard_endorsements: Dict[str, List[str]]  # key = dashboard id
    scan_result: dict
    independent_datasets: Dict[str, "PowerBIDataset"]  # key = dataset id
    app: Optional["App"]
    # Built locally from the environment base URL + workspace id; the
    # getGroupsAsAdmin payload does not include a webUrl for workspaces.
    # None when the workspace type is not directly addressable in the
    # PowerBI UI by id (e.g. PersonalGroup, which is only reachable via
    # the /groups/me alias by its owner).
    webUrl: Optional[str] = None
    # Fabric artifacts (Lakehouse, Warehouse, SQLAnalyticsEndpoint) for DirectLake lineage
    fabric_artifacts: Dict[str, "FabricArtifact"] = dataclasses.field(
        default_factory=dict
    )  # key = artifact id

    def get_urn_part(self, workspace_id_as_urn_part: Optional[bool] = False) -> str:
        # shouldn't use workspace name, as they can be the same?
        return self.id if workspace_id_as_urn_part else self.name

    def get_workspace_key(
        self,
        platform_name: str,
        platform_instance: Optional[str] = None,
        env: Optional[str] = None,
        workspace_id_as_urn_part: Optional[bool] = False,
    ) -> ContainerKey:
        return WorkspaceKey(
            workspace=self.get_urn_part(workspace_id_as_urn_part),
            platform=platform_name,
            instance=platform_instance,
            env=env,
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
class FabricArtifact:
    """Represents a Microsoft Fabric artifact (Lakehouse, Warehouse, SQLAnalyticsEndpoint).

    Used for DirectLake lineage extraction to identify upstream OneLake tables.
    """

    id: str
    name: str
    artifact_type: Literal[
        "Lakehouse", "Warehouse", "SQLAnalyticsEndpoint"
    ]  # casing really matters here
    workspace_id: str
    # Raw relation ids from API (relations[].dependentOnArtifactId); used to resolve physical_item_ids.
    relation_dependent_ids: Optional[List[str]] = None
    # For SQLAnalyticsEndpoint: Lakehouse/Warehouse ids to use in lineage URNs (so they match OneLake connector).
    # None or empty for Lakehouse/Warehouse; lineage then uses id.
    physical_item_ids: Optional[List[str]] = None


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

    # Pointer to the parent dataset.
    dataset: Optional["PowerBIDataset"] = None

    # DirectLake support fields
    storage_mode: Optional[str] = None  # e.g., "DirectLake", "Import", "DirectQuery"
    source_schema: Optional[str] = None  # From source[].schemaName (e.g., "dbo")
    source_expression: Optional[str] = (
        None  # From source[].expression (upstream table name)
    )


@dataclass
class PowerBIDataset:
    id: str
    name: Optional[str]
    description: str
    webUrl: Optional[str]
    workspace_id: str
    workspace_name: str
    parameters: Dict[str, str]

    # Table in datasets
    tables: List["Table"]
    tags: List[str]
    configuredBy: Optional[str] = None

    # DirectLake support: artifact ID this dataset depends on (Lakehouse/Warehouse)
    # From relations[].dependentOnArtifactId
    dependent_on_artifact_id: Optional[str] = None

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

    def get_dataset_key(
        self,
        platform_name: str,
        platform_instance: Optional[str] = None,
        env: Optional[str] = None,
    ) -> ContainerKey:
        return DatasetKey(
            dataset=self.id,
            platform=platform_name,
            instance=platform_instance,
            env=env,
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
    # Users with principalType=App do not have emailAddress.
    emailAddress: Optional[str]
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
    embedUrl: Optional[str]
    description: str
    dataset_id: Optional[str]  # dataset_id is coming from REST API response
    dataset: Optional[
        "PowerBIDataset"
    ]  # This the dataclass later initialise by powerbi_api.py
    pages: List["Page"]
    users: List["User"]
    tags: List[str]

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
    title: Optional[str]
    embedUrl: Optional[str]
    dataset_id: Optional[str]
    report_id: Optional[str]
    createdFrom: CreatedFrom

    # In a first pass, `dataset_id` and/or `report_id` are filled in.
    # In a subsequent pass, the objects are populated.
    dataset: Optional["PowerBIDataset"]
    report: Optional[Report]

    def get_urn_part(self):
        return f"charts.{self.id}"


@dataclass
class Dashboard:
    id: str
    displayName: str
    description: str
    embedUrl: Optional[str]
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


def new_powerbi_dataset(workspace: Workspace, raw_instance: dict) -> PowerBIDataset:
    # Extract dependent artifact ID from relations (for DirectLake lineage)
    dependent_on_artifact_id = None
    relations = raw_instance.get(Constant.RELATIONS, [])
    for relation in relations:
        if relation.get(Constant.DEPENDENT_ON_ARTIFACT_ID):
            dependent_on_artifact_id = relation[Constant.DEPENDENT_ON_ARTIFACT_ID]
            break

    # Prefer the dataset-level webUrl from the scan result; fall back to a
    # workspace-rooted URL when the scan omits it. workspace.webUrl is None
    # for non-addressable workspaces (personal / legacy personal), so the
    # final branch leaves webUrl unset rather than emit a dead UI link.
    if raw_instance.get("webUrl") is not None:
        web_url = f"{raw_instance.get('webUrl')}/details"
    elif workspace.webUrl:
        web_url = f"{workspace.webUrl}/datasets/{raw_instance['id']}/details"
    else:
        web_url = None

    return PowerBIDataset(
        id=raw_instance["id"],
        name=raw_instance.get("name"),
        description=raw_instance.get("description") or "",
        webUrl=web_url,
        workspace_id=workspace.id,
        workspace_name=workspace.name,
        parameters={},
        tables=[],
        tags=[],
        configuredBy=raw_instance.get("configuredBy"),
        dependent_on_artifact_id=dependent_on_artifact_id,
    )


def new_powerbi_dashboards(
    workspace: Workspace, raw_instances: List[Dict[str, Any]]
) -> List[Dashboard]:
    def build_dashboard(raw_instance: Dict[str, Any]) -> Optional[Dashboard]:
        dashboard_id = raw_instance.get(Constant.ID)
        display_name = raw_instance.get(Constant.DISPLAY_NAME)
        if not dashboard_id or not display_name:
            logger.warning(
                "Skipping dashboard with missing id or displayName (id=%r)",
                dashboard_id,
            )
            return None

        web_url_val = raw_instance.get(Constant.WEB_URL)
        if web_url_val:
            web_url = web_url_val
        elif workspace.webUrl:
            web_url = f"{workspace.webUrl}/dashboards/{dashboard_id}"
        else:
            web_url = None

        return Dashboard(
            id=dashboard_id,
            isReadOnly=raw_instance.get(Constant.IS_READ_ONLY),
            displayName=display_name,
            description=raw_instance.get(Constant.DESCRIPTION, ""),
            embedUrl=raw_instance.get(Constant.EMBED_URL),
            webUrl=web_url,
            workspace_id=workspace.id,
            workspace_name=workspace.name,
            tiles=new_powerbi_tiles(raw_instance.get(Constant.TILES) or []),
            users=[
                user
                for user_instance in (raw_instance.get(Constant.USERS) or [])
                if (user := new_powerbi_user(user_instance)) is not None
            ],
            tags=[],  # filled from workspace.dashboard_endorsements in fill_regular_metadata_detail()
        )

    return [
        dashboard
        for raw_instance in raw_instances
        if raw_instance is not None
        # As we add dashboards to the App, Power BI starts providing duplicate dashboard information,
        # where the duplicate includes an AppId, while the original dashboard does not.
        and Constant.APP_ID not in raw_instance
        if (dashboard := build_dashboard(raw_instance)) is not None
    ]


def new_powerbi_tiles(raw_instances: List[Dict[str, Any]]) -> List[Tile]:
    result = []
    for raw_instance in raw_instances:
        if raw_instance is None:
            continue
        tile_id = raw_instance.get(Constant.ID)
        if not tile_id:
            logger.warning(
                "Skipping tile with missing id: raw=%r",
                {
                    k: raw_instance.get(k)
                    for k in (Constant.TITLE, Constant.DATASET_ID, Constant.REPORT_ID)
                },
            )
            continue
        result.append(
            Tile(
                id=tile_id,
                title=raw_instance.get(Constant.TITLE),
                embedUrl=raw_instance.get(Constant.EMBED_URL),
                dataset_id=raw_instance.get(Constant.DATASET_ID),
                report_id=raw_instance.get(Constant.REPORT_ID),
                dataset=None,
                report=None,
                createdFrom=(
                    # In the past we considered that only one of the two report_id or dataset_id would be present
                    # but we have seen cases where both are present. If both are present, we prioritize the report.
                    Tile.CreatedFrom.REPORT
                    if raw_instance.get(Constant.REPORT_ID)
                    else Tile.CreatedFrom.DATASET
                    if raw_instance.get(Constant.DATASET_ID)
                    else Tile.CreatedFrom.VISUALIZATION
                ),
            )
        )
    return result


def new_powerbi_reports(
    workspace: Workspace, raw_instances: List[Dict[str, Any]]
) -> List[Report]:
    def build_report(raw_instance: Dict[str, Any]) -> Optional[Report]:
        report_id = raw_instance.get(Constant.ID)
        report_name = raw_instance.get(Constant.NAME)
        if not report_id or not report_name:
            logger.warning("Skipping report with missing id or name (id=%r)", report_id)
            return None

        report_type_raw = raw_instance.get(Constant.REPORT_TYPE)
        if not isinstance(report_type_raw, str):
            logger.warning(
                "Skipping report with missing or non-string reportType=%r (id=%s)",
                report_type_raw,
                report_id,
            )
            return None
        try:
            report_type = ReportType[report_type_raw]
        except KeyError:
            logger.warning(
                "Skipping report with unknown reportType=%r (id=%s)",
                report_type_raw,
                report_id,
            )
            return None

        web_url_val = raw_instance.get(Constant.WEB_URL)
        if web_url_val:
            web_url = web_url_val
        elif workspace.webUrl:
            if report_type is ReportType.PaginatedReport:
                web_url = f"{workspace.webUrl}/rdlreports/{report_id}"
            else:
                web_url = f"{workspace.webUrl}/reports/{report_id}"
        else:
            web_url = None

        return Report(
            id=report_id,
            name=report_name,
            type=report_type,
            webUrl=web_url,
            embedUrl=raw_instance.get(Constant.EMBED_URL),
            description=raw_instance.get(Constant.DESCRIPTION) or "",
            pages=[],  # populated in PowerBiAPI.get_reports() via get_pages_by_report()
            dataset_id=raw_instance.get(Constant.DATASET_ID),
            users=[
                user
                for user_instance in (raw_instance.get(Constant.USERS) or [])
                if (user := new_powerbi_user(user_instance)) is not None
            ],
            tags=[],  # filled by fill_tags() in get_reports() from workspace.report_endorsements
            dataset=None,  # It will come from dataset_registry defined in powerbi_api.py
        )

    return [
        report
        for raw_instance in raw_instances
        # As we add reports to the App, Power BI starts providing duplicate report information,
        # where the duplicate includes an AppId, while the original report does not.
        if raw_instance is not None and Constant.APP_ID not in raw_instance
        if (report := build_report(raw_instance)) is not None
    ]


def new_powerbi_user(raw_instance: Dict[str, Any]) -> Optional[User]:
    if not raw_instance:
        return None
    required = (
        Constant.IDENTIFIER,
        Constant.DISPLAY_NAME,
        Constant.GRAPH_ID,
        Constant.PRINCIPAL_TYPE,
    )
    # Reject fields that are absent, None, or empty string — all would produce malformed URNs.
    missing = [k for k in required if raw_instance.get(k) in (None, "")]
    if missing:
        logger.warning(
            "Skipping malformed user entry missing required fields: %s", missing
        )
        return None
    return User(
        id=raw_instance[Constant.IDENTIFIER],
        displayName=raw_instance[Constant.DISPLAY_NAME],
        emailAddress=raw_instance.get(Constant.EMAIL_ADDRESS),
        graphId=raw_instance[Constant.GRAPH_ID],
        principalType=raw_instance[Constant.PRINCIPAL_TYPE],
        datasetUserAccessRight=raw_instance.get(Constant.DATASET_USER_ACCESS_RIGHT),
        reportUserAccessRight=raw_instance.get(Constant.REPORT_USER_ACCESS_RIGHT),
        dashboardUserAccessRight=raw_instance.get(Constant.DASHBOARD_USER_ACCESS_RIGHT),
        groupUserAccessRight=raw_instance.get(Constant.GROUP_USER_ACCESS_RIGHT),
    )
