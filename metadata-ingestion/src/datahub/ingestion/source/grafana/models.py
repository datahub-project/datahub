"""Grafana data models for DataHub ingestion.

References:
- Grafana HTTP API: https://grafana.com/docs/grafana/latest/developers/http_api/
- Dashboard API: https://grafana.com/docs/grafana/latest/developers/http_api/dashboard/
- Folder API: https://grafana.com/docs/grafana/latest/developers/http_api/folder/
- Search API: https://grafana.com/docs/grafana/latest/developers/http_api/other/#search-api
- Dashboard JSON structure: https://grafana.com/docs/grafana/latest/dashboards/build-dashboards/view-dashboard-json-model/
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from datahub.configuration.pydantic_migration_helpers import PYDANTIC_VERSION_2
from datahub.emitter.mcp_builder import ContainerKey

# Grafana-specific type definitions for better type safety
GrafanaQueryTarget = Dict[
    str, Any
]  # Query targets: refId, expr/query, datasource, hide, etc.
GrafanaFieldConfig = Dict[
    str, Any
]  # Field config: defaults, overrides, display settings
GrafanaTransformation = Dict[str, Any]  # Transformations: id, options


class DatasourceRef(BaseModel):
    """Reference to a Grafana datasource."""

    type: Optional[str] = None  # Datasource type (prometheus, mysql, postgres, etc.)
    uid: Optional[str] = None  # Datasource unique identifier
    name: Optional[str] = None  # Datasource display name


class Panel(BaseModel):
    """Represents a Grafana dashboard panel."""

    id: str
    title: str
    description: str = ""
    type: Optional[str]
    # Query targets - each contains refId (A,B,C...), query/expr, datasource ref, etc.
    query_targets: List[GrafanaQueryTarget] = Field(
        default_factory=list, alias="targets"
    )
    # Datasource reference - contains type, uid, name
    datasource_ref: Optional[DatasourceRef] = Field(default=None, alias="datasource")
    # Field configuration - display settings, defaults, overrides
    field_config: GrafanaFieldConfig = Field(default_factory=dict, alias="fieldConfig")
    # Data transformations - each contains id and transformation-specific options
    transformations: List[GrafanaTransformation] = Field(default_factory=list)


class Dashboard(BaseModel):
    """Represents a Grafana dashboard."""

    uid: str
    title: str
    description: str = ""
    version: Optional[str]
    panels: List[Panel]
    tags: List[str]
    timezone: Optional[str]
    refresh: Optional[str] = None
    schema_version: Optional[str] = Field(default=None, alias="schemaVersion")
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
    description: Optional[str] = ""

    if PYDANTIC_VERSION_2:
        from pydantic import ConfigDict

        model_config = ConfigDict(coerce_numbers_to_str=True)  # type: ignore


class FolderKey(ContainerKey):
    """Key for identifying a Grafana folder."""

    folder_id: str


class DashboardContainerKey(ContainerKey):
    """Key for identifying a Grafana dashboard."""

    dashboard_id: str
    folder_id: Optional[str] = None  # Reference to parent folder
