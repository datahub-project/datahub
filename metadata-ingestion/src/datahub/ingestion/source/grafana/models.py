"""Grafana data models for DataHub ingestion.

References:
- Grafana HTTP API: https://grafana.com/docs/grafana/latest/developers/http_api/
- Dashboard API: https://grafana.com/docs/grafana/latest/developers/http_api/dashboard/
- Folder API: https://grafana.com/docs/grafana/latest/developers/http_api/folder/
- Search API: https://grafana.com/docs/grafana/latest/developers/http_api/other/#search-api
- Dashboard JSON structure: https://grafana.com/docs/grafana/latest/dashboards/build-dashboards/view-dashboard-json-model/
"""

import logging
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from datahub.emitter.mcp_builder import ContainerKey

logger = logging.getLogger(__name__)

GrafanaQueryTarget = Dict[str, Any]
GrafanaFieldConfig = Dict[str, Any]
GrafanaTransformation = Dict[str, Any]


class _GrafanaBaseModel(BaseModel):
    model_config = ConfigDict(
        coerce_numbers_to_str=True,
        populate_by_name=True,
        extra="ignore",
    )


class DatasourceRef(_GrafanaBaseModel):
    """Reference to a Grafana datasource."""

    type: Optional[str] = None
    uid: Optional[str] = None
    name: Optional[str] = None


class Panel(_GrafanaBaseModel):
    """Represents a Grafana dashboard panel."""

    id: str
    title: Optional[str] = (
        None  # Optional: text panels in Grafana v11+ don't have titles
    )
    description: str = ""
    type: Optional[str] = None
    query_targets: List[GrafanaQueryTarget] = Field(
        default_factory=list, alias="targets"
    )
    datasource_ref: Optional[DatasourceRef] = Field(default=None, alias="datasource")
    field_config: GrafanaFieldConfig = Field(default_factory=dict, alias="fieldConfig")
    transformations: List[GrafanaTransformation] = Field(default_factory=list)


class Dashboard(_GrafanaBaseModel):
    """Represents a Grafana dashboard."""

    uid: str
    title: str
    description: str = ""
    version: Optional[str] = None
    panels: List[Panel]
    tags: List[str] = Field(default_factory=list)
    timezone: Optional[str] = None
    refresh: Optional[str] = None
    schema_version: Optional[str] = Field(default=None, alias="schemaVersion")
    folder_id: Optional[str] = None
    created_by: Optional[str] = None

    @staticmethod
    def extract_panels(
        panels_data: List[Dict[str, Any]], skip_text_panels: bool = False
    ) -> List[Panel]:
        """Extract panels, including nested ones, skipping invalid panels."""
        panels: List[Panel] = []
        for panel_data in panels_data:
            if panel_data.get("type") == "row" and "panels" in panel_data:
                for p in panel_data["panels"]:
                    if p.get("type") != "row":
                        if skip_text_panels and p.get("type") == "text":
                            continue
                        try:
                            panels.append(Panel.model_validate(p))
                        except Exception as e:
                            logger.warning(
                                f"Error parsing panel (id={p.get('id')}, type={p.get('type')}): {e}. Skipping this panel."
                            )
            elif panel_data.get("type") != "row":
                if skip_text_panels and panel_data.get("type") == "text":
                    continue
                try:
                    panels.append(Panel.model_validate(panel_data))
                except Exception as e:
                    logger.warning(
                        f"Error parsing panel (id={panel_data.get('id')}, type={panel_data.get('type')}): {e}. Skipping this panel."
                    )
        return panels

    @field_validator("refresh", mode="before")
    @classmethod
    def convert_refresh_to_string(cls, v: Any) -> Optional[str]:
        """Convert boolean refresh values to strings for compatibility."""
        if isinstance(v, bool):
            return str(v)
        return v

    @model_validator(mode="before")
    @classmethod
    def extract_dashboard_data(cls, data: Any) -> Dict[str, Any]:
        """Extract dashboard data from nested structure and process panels."""
        if isinstance(data, dict):
            dashboard_data = data.get("dashboard", data)

            _panel_data = dashboard_data.get("panels", [])
            panels = []

            skip_text_panels = dashboard_data.get("_skip_text_panels", False)

            if _panel_data and all(isinstance(p, dict) for p in _panel_data):
                try:
                    panels = cls.extract_panels(_panel_data, skip_text_panels)
                except Exception as e:
                    logger.warning(f"Error extracting panels from dashboard: {e}")
            else:
                panels = _panel_data

            meta = dashboard_data.get("meta", {})
            folder_id = meta.get("folderId") if meta else None

            result = {**dashboard_data, "panels": panels}
            if folder_id is not None:
                result["folder_id"] = folder_id

            result.pop("meta", None)
            result.pop("_skip_text_panels", None)

            return result

        return data


class Folder(_GrafanaBaseModel):
    """Represents a Grafana folder."""

    id: str
    title: str
    description: Optional[str] = ""


class FolderKey(ContainerKey):
    """Key for identifying a Grafana folder."""

    folder_id: str


class DashboardContainerKey(ContainerKey):
    """Key for identifying a Grafana dashboard."""

    dashboard_id: str
    folder_id: Optional[str] = None
