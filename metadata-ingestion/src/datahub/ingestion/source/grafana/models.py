from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from datahub.emitter.mcp_builder import ContainerKey


@dataclass
class Panel:
    """Represents a Grafana dashboard panel"""

    id: str
    title: str
    description: str = ""
    type: str = ""
    targets: List[Dict] = field(default_factory=list)
    datasource: Optional[Dict] = None
    field_config: Dict = field(default_factory=dict)
    transformations: List[Dict] = field(default_factory=list)

    def __post_init__(self):
        if self.targets is None:
            self.targets = []

        if self.field_config is None:
            self.field_config = {}

        if self.transformations is None:
            self.transformations = []

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Panel":
        return cls(
            id=str(data.get("id", "")),
            title=data.get("title", ""),
            description=data.get("description", ""),
            type=data.get("type", ""),
            targets=data.get("targets", []),
            datasource=data.get("datasource"),
            field_config=data.get("fieldConfig", {}),
            transformations=data.get("transformations", []),
        )


@dataclass
class Dashboard:
    """Represents a Grafana dashboard"""

    uid: str
    title: str
    description: str
    version: str
    panels: List[Panel]
    tags: List[str]
    timezone: str = ""
    refresh: Optional[str] = None
    schema_version: str = ""
    folder_id: Optional[str] = None
    created_by: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Dashboard":
        dashboard_data = data.get("dashboard", {})

        # Extract all panels including nested ones
        panels = []
        for panel_data in dashboard_data.get("panels", []):
            if panel_data.get("type") == "row" and "panels" in panel_data:
                # Add panels nested in rows
                panels.extend(
                    [
                        Panel.from_dict(p)
                        for p in panel_data["panels"]
                        if p.get("type") != "row"
                    ]
                )
            elif panel_data.get("type") != "row":
                # Add regular panels
                panels.append(Panel.from_dict(panel_data))

        return cls(
            uid=dashboard_data.get("uid", ""),
            title=dashboard_data.get("title", ""),
            description=dashboard_data.get("description", ""),
            version=str(dashboard_data.get("version", "")),
            panels=panels,  # Use our processed panels list
            tags=dashboard_data.get("tags", []),
            timezone=dashboard_data.get("timezone", ""),
            refresh=dashboard_data.get("refresh"),
            schema_version=str(dashboard_data.get("schemaVersion", "")),
            folder_id=data.get("meta", {}).get("folderId"),
            created_by=dashboard_data.get("createdBy"),
        )


@dataclass
class Folder:
    """Represents a Grafana folder"""

    id: str
    title: str
    description: str = ""

    @classmethod
    def from_dict(cls, data: Dict) -> "Folder":
        return cls(
            id=data["id"],
            title=data["title"],
            description=data.get("description", ""),
        )


class FolderKey(ContainerKey):
    folder_id: str


class DashboardContainerKey(ContainerKey):
    dashboard_id: str
