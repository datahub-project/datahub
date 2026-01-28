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

# Type aliases for Grafana data structures
GrafanaQueryTarget = Dict[str, Any]
GrafanaFieldConfig = Dict[str, Any]  # Never None, always a dict (possibly empty)
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


class QueryInfo(_GrafanaBaseModel):
    """Represents a query extracted from a Grafana panel."""

    query: str
    language: str

    @field_validator("query")
    @classmethod
    def validate_query_not_empty(cls, v: str) -> str:
        """Ensure query is not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError("Query cannot be empty or whitespace-only")
        return v.strip()

    @field_validator("language")
    @classmethod
    def validate_language_not_empty(cls, v: str) -> str:
        """Ensure language is not empty or whitespace-only."""
        if not v or not v.strip():
            raise ValueError("Language cannot be empty or whitespace-only")
        return v.strip()


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

    @property
    def safe_field_config(self) -> GrafanaFieldConfig:
        """Get field_config, guaranteed to be a dict (never None)."""
        return self.field_config or {}

    @property
    def safe_query_targets(self) -> List[GrafanaQueryTarget]:
        """Get query_targets, guaranteed to be a list (never None)."""
        return self.query_targets or []

    @property
    def safe_transformations(self) -> List[GrafanaTransformation]:
        """Get transformations, guaranteed to be a list (never None)."""
        return self.transformations or []

    @staticmethod
    def _ensure_dict_field(
        data: Dict[str, Any], field_name: str, default: Dict[str, Any]
    ) -> None:
        """Ensure a field is a dict, converting None/invalid types to the default dict."""
        value = data.get(field_name)
        if value is None or not isinstance(value, dict):
            data[field_name] = default
        # If value exists and is already a dict, leave it as is

    @staticmethod
    def _ensure_list_field(
        data: Dict[str, Any], field_name: str, default: List[Any]
    ) -> None:
        """Ensure a field is a list, converting None/invalid types to the default list."""
        value = data.get(field_name)
        if value is None or not isinstance(value, list):
            data[field_name] = default
        # If value exists and is already a list, leave it as is

    @staticmethod
    def _normalize_id_field(data: Dict[str, Any]) -> None:
        """Convert integer ID to string and generate fallback ID if missing."""
        if "id" not in data or data["id"] is None:
            # Generate a deterministic fallback ID based on multiple panel properties
            panel_type = data.get("type", "unknown")
            title = data.get("title", "untitled")

            # Include additional properties for uniqueness
            grid_pos = data.get("gridPos", {})
            x = grid_pos.get("x", 0)
            y = grid_pos.get("y", 0)
            w = grid_pos.get("w", 0)
            h = grid_pos.get("h", 0)

            # Create a deterministic identifier from multiple properties
            # This ensures uniqueness even for panels with identical type/title
            identifier_parts = [
                panel_type,
                title,
                str(x),
                str(y),
                str(w),
                str(h),  # Grid position for uniqueness
            ]
            identifier_string = "_".join(identifier_parts)

            # Use hash for consistent ID generation across runs
            fallback_id = f"{panel_type}_{abs(hash(identifier_string)) % 100000}"
            data["id"] = fallback_id
        elif isinstance(data["id"], int):
            data["id"] = str(data["id"])

    @model_validator(mode="before")
    @classmethod
    def ensure_panel_defaults(cls, data: Any) -> Dict[str, Any]:
        """Set defaults for optional fields and normalize data types."""
        if isinstance(data, dict):
            result = dict(data)

            # Set basic defaults - handle None values
            if result.get("description") is None:
                result["description"] = ""

            # Handle datasource field - convert invalid types to None
            datasource = result.get("datasource")
            if isinstance(datasource, str):
                # Handle template variables like '$datasource' or other string values
                result["datasource"] = None
            elif datasource is not None and not isinstance(datasource, dict):
                # Handle any other invalid types
                result["datasource"] = None

            # Ensure complex fields are never None
            cls._ensure_list_field(result, "targets", [])
            cls._ensure_list_field(result, "transformations", [])
            cls._ensure_dict_field(result, "fieldConfig", {})

            # Normalize data types
            cls._normalize_id_field(result)

            return result
        return data


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
            if panel_data.get("type") == "row" and panel_data.get("panels"):
                for p in panel_data.get("panels", []):
                    if p and p.get("type") != "row":
                        if skip_text_panels and p.get("type") == "text":
                            continue
                        try:
                            panels.append(Panel.model_validate(p))
                        except Exception as e:
                            logger.debug(
                                f"Error parsing nested panel (id={p.get('id')}, type={p.get('type')}): {e}. "
                                f"Panel data: {p}. Skipping this panel."
                            )
                            logger.warning(
                                f"Skipping panel with validation errors (id={p.get('id')}, type={p.get('type')}): "
                                f"Missing or invalid fields. Enable debug logging for details."
                            )
            elif panel_data.get("type") != "row":
                if skip_text_panels and panel_data.get("type") == "text":
                    continue
                try:
                    panels.append(Panel.model_validate(panel_data))
                except Exception as e:
                    logger.debug(
                        f"Error parsing panel (id={panel_data.get('id')}, type={panel_data.get('type')}): {e}. "
                        f"Panel data: {panel_data}. Skipping this panel."
                    )
                    logger.warning(
                        f"Skipping panel with validation errors (id={panel_data.get('id')}, type={panel_data.get('type')}): "
                        f"Missing or invalid fields. Enable debug logging for details."
                    )
        return panels

    @staticmethod
    def _set_dashboard_defaults(result: Dict[str, Any]) -> None:
        """Set default values for optional dashboard fields."""
        result.setdefault("tags", [])
        result.setdefault("description", "")
        result.setdefault("version", None)
        result.setdefault("timezone", None)
        result.setdefault("refresh", None)

    @staticmethod
    def _cleanup_dashboard_metadata(result: Dict[str, Any]) -> None:
        """Remove internal metadata fields from dashboard data."""
        result.pop("meta", None)
        result.pop("_skip_text_panels", None)

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

            result = {**dashboard_data, "panels": panels}

            if (meta := data.get("meta")) is not None:
                # We only want to set folder_id and created_by from meta if we get it
                # from json and not when creating Dashboard(uid=...) like in tests.
                result["folder_id"] = meta.get("folderId")
                result["created_by"] = meta.get("createdBy")

            cls._set_dashboard_defaults(result)
            cls._cleanup_dashboard_metadata(result)

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
