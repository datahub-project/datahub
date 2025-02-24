import dataclasses
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from datahub.emitter.mcp_builder import ContainerKey
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    StringTypeClass,
)

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

    def get_urn_part(self, workspace_id_as_urn_part: Optional[bool] = False) -> str:
        # shouldn't use workspace name, as they can be the same?
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

    # Pointer to the parent dataset.
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

    # Table in datasets
    tables: List["Table"]
    tags: List[str]
    configuredBy: Optional[str] = None

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
    title: str
    embedUrl: str
    dataset: Optional["PowerBIDataset"]
    dataset_id: Optional[str]
    report: Optional[Report]
    createdFrom: CreatedFrom

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


def new_powerbi_dataset(workspace: Workspace, raw_instance: dict) -> PowerBIDataset:
    return PowerBIDataset(
        id=raw_instance["id"],
        name=raw_instance.get("name"),
        description=raw_instance.get("description", ""),
        webUrl=(
            "{}/details".format(raw_instance.get("webUrl"))
            if raw_instance.get("webUrl") is not None
            else None
        ),
        workspace_id=workspace.id,
        workspace_name=workspace.name,
        parameters={},
        tables=[],
        tags=[],
        configuredBy=raw_instance.get("configuredBy"),
    )
