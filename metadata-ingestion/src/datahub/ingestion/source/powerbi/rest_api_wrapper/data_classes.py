from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from datahub.emitter.mcp_builder import PlatformKey


class WorkspaceKey(PlatformKey):
    workspace: str


@dataclass
class Workspace:
    id: str
    name: str
    dashboards: List["Dashboard"]
    reports: List["Report"]
    datasets: Dict[str, "PowerBIDataset"]
    report_endorsements: Dict[str, List[str]]
    dashboard_endorsements: Dict[str, List[str]]
    scan_result: dict

    def get_urn_part(self):
        return self.name

    def get_workspace_key(self, platform_name: str) -> PlatformKey:
        return WorkspaceKey(
            workspace=self.get_urn_part(),
            platform=platform_name,
        )


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
class Table:
    name: str
    full_name: str
    expression: Optional[str]


@dataclass
class PowerBIDataset:
    id: str
    name: Optional[str]
    webUrl: Optional[str]
    workspace_id: str
    # Table in datasets
    tables: List["Table"]
    tags: List[str]

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

    def get_urn_part(self):
        return f"users.{self.id}"

    def __members(self):
        return (self.id,)

    def __eq__(self, instance):
        return isinstance(instance, User) and self.__members() == instance.__members()

    def __hash__(self):
        return hash(self.__members())


@dataclass
class Report:
    id: str
    name: str
    webUrl: Optional[str]
    embedUrl: str
    description: str
    dataset: Optional["PowerBIDataset"]
    pages: List["Page"]
    users: List["User"]
    tags: List[str]

    def get_urn_part(self):
        return f"reports.{self.id}"


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
    embedUrl: str
    webUrl: Optional[str]
    isReadOnly: Any
    workspace_id: str
    workspace_name: str
    tiles: List["Tile"]
    users: List["User"]
    tags: List[str]

    def get_urn_part(self):
        return f"dashboards.{self.id}"

    def __members(self):
        return (self.id,)

    def __eq__(self, instance):
        return (
            isinstance(instance, Dashboard) and self.__members() == instance.__members()
        )

    def __hash__(self):
        return hash(self.__members())


def new_powerbi_dataset(workspace_id: str, raw_instance: dict) -> PowerBIDataset:
    return PowerBIDataset(
        id=raw_instance["id"],
        name=raw_instance.get("name"),
        webUrl="{}/details".format(raw_instance.get("webUrl"))
        if raw_instance.get("webUrl") is not None
        else None,
        workspace_id=workspace_id,
        tables=[],
        tags=[],
    )
