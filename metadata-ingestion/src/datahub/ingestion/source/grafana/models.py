"""Grafana data models for DataHub ingestion.

References:
- Grafana HTTP API: https://grafana.com/docs/grafana/latest/developers/http_api/
- Dashboard API: https://grafana.com/docs/grafana/latest/developers/http_api/dashboard/
- Folder API: https://grafana.com/docs/grafana/latest/developers/http_api/folder/
- Search API: https://grafana.com/docs/grafana/latest/developers/http_api/other/#search-api
- Dashboard JSON structure: https://grafana.com/docs/grafana/latest/dashboards/build-dashboards/view-dashboard-json-model/
"""

import logging
from dataclasses import dataclass, field
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
    # Panels dropped while parsing, as "id=..., type=..." descriptions. The
    # model has no report, so the source reports them.
    skipped_panels: List[str] = Field(default_factory=list)

    @staticmethod
    def extract_panels(
        panels_data: List[object], skip_text_panels: bool = False
    ) -> "PanelExtraction":
        """Extract panels, including those nested in rows.

        A panel that cannot be read is skipped on its own and described in
        ``PanelExtraction.skipped``; the other panels are still extracted.
        """
        extraction = PanelExtraction()
        for panel_data in panels_data:
            if isinstance(panel_data, Panel):  # Dashboard(panels=[Panel(...)])
                extraction.panels.append(panel_data)
                continue
            if not isinstance(panel_data, dict):
                extraction.skipped.append(_describe_entry(panel_data))
                continue
            if panel_data.get("type") != "row":
                Dashboard._extract_panel(panel_data, skip_text_panels, extraction)
                continue
            nested = panel_data.get("panels") or []
            if not isinstance(nested, list):
                extraction.skipped.append(
                    f"{_describe_panel(panel_data)} (its panels are not a list)"
                )
                continue
            for nested_panel in nested:
                if not isinstance(nested_panel, dict):
                    extraction.skipped.append(_describe_entry(nested_panel))
                elif nested_panel.get("type") != "row":
                    Dashboard._extract_panel(nested_panel, skip_text_panels, extraction)
        return extraction

    @staticmethod
    def _extract_panel(
        panel_data: Dict[str, Any],
        skip_text_panels: bool,
        extraction: "PanelExtraction",
    ) -> None:
        if skip_text_panels and panel_data.get("type") == "text":
            return
        try:
            extraction.panels.append(Panel.model_validate(panel_data))
        except Exception as e:
            description = _describe_panel(panel_data)
            logger.debug(f"Skipping panel ({description}): {e}")
            extraction.skipped.append(description)

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
            # pydantic only turns ValueError into a ValidationError; an
            # AttributeError or TypeError from here would escape get_dashboard
            # and stop ingestion.
            if not isinstance(dashboard_data, dict):
                raise ValueError("dashboard is not an object")

            _panel_data = dashboard_data.get("panels", [])
            if _panel_data is None:
                _panel_data = []
            if not isinstance(_panel_data, list):
                raise ValueError("panels is not a list")
            # The API client sets the flag on the response it fetched, which
            # wraps the dashboard; tests may set it on the dashboard itself.
            skip_text_panels = bool(
                data.get("_skip_text_panels") or dashboard_data.get("_skip_text_panels")
            )
            extraction = cls.extract_panels(_panel_data, skip_text_panels)

            result = {
                **dashboard_data,
                "panels": extraction.panels,
                "skipped_panels": extraction.skipped,
            }

            if isinstance(meta := data.get("meta"), dict):
                # We only want to set folder_id and created_by from meta if we get it
                # from json and not when creating Dashboard(uid=...) like in tests.
                result["folder_id"] = meta.get("folderId")
                result["created_by"] = meta.get("createdBy")

            cls._set_dashboard_defaults(result)
            cls._cleanup_dashboard_metadata(result)

            return result

        return data


@dataclass
class PanelExtraction:
    """Panels read from a dashboard, and descriptions of the ones skipped."""

    panels: List[Panel] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)


def _describe_panel(panel_data: Dict[str, Any]) -> str:
    return f"id={panel_data.get('id')}, type={panel_data.get('type')}"


def _describe_entry(entry: object) -> str:
    return f"entry that is not a panel: {entry!r}"


class DashboardSearchHit(_GrafanaBaseModel):
    """One result of /api/search. Only uid is required to fetch the dashboard;
    basic mode also needs title and url."""

    uid: str = Field(min_length=1)
    title: Optional[str] = None
    url: Optional[str] = None
    id: Optional[str] = None
    uri: Optional[str] = None
    type: Optional[str] = None
    folder_id: Optional[str] = Field(default=None, alias="folderId")
    folder_uid: Optional[str] = Field(default=None, alias="folderUid")
    folder_title: Optional[str] = Field(default=None, alias="folderTitle")


class Folder(_GrafanaBaseModel):
    """Represents a Grafana folder."""

    id: str
    title: str
    uid: Optional[str] = None
    description: Optional[str] = ""

    @model_validator(mode="before")
    @classmethod
    def default_title(cls, data: Any) -> Any:
        """Name a folder without a title after its uid or id.

        Only a folder with no id is unusable. Skipping one for a missing title
        would fail runs that remove stale metadata, and folder_pattern cannot
        exclude a folder that never parsed.
        """
        if isinstance(data, dict) and not data.get("title"):
            fallback = data.get("uid") or data.get("id")
            if fallback is not None:
                return {**data, "title": str(fallback)}
        return data


class FolderKey(ContainerKey):
    """Key for identifying a Grafana folder."""

    folder_id: str


class DashboardContainerKey(ContainerKey):
    """Key for identifying a Grafana dashboard."""

    dashboard_id: str
    folder_id: Optional[str] = None


@dataclass
class DashboardListing:
    """The dashboards fetched, and how many search results were skipped
    because they or their dashboard could not be read."""

    dashboards: List[Dashboard] = field(default_factory=list)
    skipped: int = 0


@dataclass
class FolderListing:
    """The folders /api/folders returned, and how many could not be parsed."""

    folders: List[Folder] = field(default_factory=list)
    unparseable: int = 0
