from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from datahub.emitter.mcp_builder import ContainerKey


class Panel(BaseModel):
    """Represents a Grafana dashboard panel."""

    id: str
    title: str
    description: str = ""
    type: str = ""
    targets: List[Dict[str, Any]] = Field(default_factory=list)
    datasource: Optional[Dict[str, Any]] = None
    field_config: Dict[str, Any] = Field(default_factory=dict, alias="fieldConfig")
    transformations: List[Dict[str, Any]] = Field(default_factory=list)


class Dashboard(BaseModel):
    """Represents a Grafana dashboard."""

    uid: str
    title: str
    description: str
    version: str
    panels: List[Panel]
    tags: List[str]
    timezone: str = ""
    refresh: Optional[str] = None
    schema_version: str = Field(default="", alias="schemaVersion")
    folder_id: Optional[str] = Field(default=None, alias="meta.folderId")
    created_by: Optional[str] = None

    @staticmethod
    def extract_panels(panels_data: List[Dict[str, Any]]) -> List[Panel]:
        """Extract panels, including nested ones."""
        panels: List[Panel] = []
        for panel_data in panels_data:
            if panel_data.get("type") == "row" and "panels" in panel_data:
                panels.extend(
                    Panel.parse_obj(p)
                    for p in panel_data["panels"]
                    if p.get("type") != "row"
                )
            elif panel_data.get("type") != "row":
                panels.append(Panel.parse_obj(panel_data))
        return panels

    @classmethod
    def parse_obj(cls, data: Dict[str, Any]) -> "Dashboard":
        """Custom parsing to handle nested panel extraction."""
        dashboard_data = data.get("dashboard", {})
        panels = cls.extract_panels(dashboard_data.get("panels", []))

        # Extract meta.folderId from nested structure
        meta = dashboard_data.get("meta", {})
        folder_id = meta.get("folderId")

        # Create dashboard data without meta to avoid conflicts
        dashboard_dict = {**dashboard_data, "panels": panels, "folder_id": folder_id}
        if "meta" in dashboard_dict:
            del dashboard_dict["meta"]

        return super().parse_obj(dashboard_dict)


class Folder(BaseModel):
    """Represents a Grafana folder."""

    id: str
    title: str
    description: str = ""


class FolderKey(ContainerKey):
    """Key for identifying a Grafana folder."""

    folder_id: str


class DashboardContainerKey(ContainerKey):
    """Key for identifying a Grafana dashboard."""

    dashboard_id: str
