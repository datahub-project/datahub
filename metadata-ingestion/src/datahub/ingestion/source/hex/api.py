import functools
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import requests
import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from requests.adapters import HTTPAdapter
from typing_extensions import assert_never
from urllib3.util.retry import Retry

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.hex.constants import (
    CONNECTION_TYPE_DEFAULTS,
    HEX_API_BASE_URL_DEFAULT,
    HEX_API_PAGE_SIZE_DEFAULT,
)
from datahub.ingestion.source.hex.model import (
    Analytics,
    Category,
    Collection,
    Component,
    Owner,
    Project,
    RunRecord,
    Status,
)
from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)

_F = TypeVar("_F", bound=Callable)


@dataclass
class HexApiConnection:
    """A row from Hex's GET /v1/data-connections, with per-type defaults
    already extracted.

    `default_database` / `default_schema` slot into `sqlglot_lineage`'s
    `default_db` / `default_schema` parameters so unqualified `FROM table`
    refs in SQL cells resolve to the canonical warehouse URN. Either may
    be None when Hex doesn't expose the value (e.g. databricks jdbcUrl,
    nullable Snowflake schema) — the user can override via
    `connection_platform_map` in the recipe.
    """

    name: str
    type: str
    default_database: Optional[str] = None
    default_schema: Optional[str] = None


def _extract_connection_defaults(
    hex_type: str, details: dict
) -> Tuple[Optional[str], Optional[str]]:
    """Return (default_database, default_schema) for a Hex connection type.

    Unknown types return (None, None) — defaults come from user override
    via `connection_platform_map` only.
    """
    spec = CONNECTION_TYPE_DEFAULTS.get(hex_type.lower())
    if spec is None:
        return None, None
    database_key, schema_key, schema_fallback = spec
    db = details.get(database_key) if database_key else None
    schema = details.get(schema_key) if schema_key else None
    if schema is None and schema_fallback is not None and db is not None:
        schema = schema_fallback
    return db, schema


def _api_call(name: str) -> Callable[[_F], _F]:
    """Decorator that increments self.report.api_calls[name] once per call.

    For paginated endpoints, the method body calls self._track_page(name) once
    per inner HTTP request to populate api_calls[<name>.pages] alongside the
    outer-invocation count.
    """

    def decorator(func: _F) -> _F:
        @functools.wraps(func)
        def wrapper(self: "HexApi", *args: Any, **kwargs: Any) -> Any:
            self.report.api_calls[name] = self.report.api_calls.get(name, 0) + 1
            self.report.api_total_calls += 1
            return func(self, *args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator


# The following models were Claude-generated from Hex API OpenAPI definition https://static.hex.site/openapi.json
# To be exclusively used internally for the deserialization of the API response
# Model is incomplete and fields may have not been mapped if not used in the ingestion


class HexApiAppViewStats(BaseModel):
    """App view analytics data model."""

    all_time: Optional[int] = Field(default=None, alias="allTime")
    last_seven_days: Optional[int] = Field(default=None, alias="lastSevenDays")
    last_fourteen_days: Optional[int] = Field(default=None, alias="lastFourteenDays")
    last_thirty_days: Optional[int] = Field(default=None, alias="lastThirtyDays")


class HexApiProjectAnalytics(BaseModel):
    """Analytics data model for projects."""

    app_views: Optional[HexApiAppViewStats] = Field(default=None, alias="appViews")
    last_viewed_at: Optional[datetime] = Field(default=None, alias="lastViewedAt")
    published_results_updated_at: Optional[datetime] = Field(
        default=None, alias="publishedResultsUpdatedAt"
    )

    @field_validator("last_viewed_at", "published_results_updated_at", mode="before")
    @classmethod
    def parse_datetime(cls, value: Union[str, datetime, None]) -> Optional[datetime]:
        if isinstance(value, str):
            return _parse_iso(value)
        return value


class HexApiProjectStatus(BaseModel):
    """Project status model."""

    name: str


class HexApiCategory(BaseModel):
    """Category model."""

    name: str
    description: Optional[str] = None


class HexApiReviews(BaseModel):
    """Reviews configuration model."""

    required: bool


class HexApiUser(BaseModel):
    """User model."""

    email: str


class HexApiUserAccess(BaseModel):
    """User access model."""

    user: HexApiUser


class HexApiCollectionData(BaseModel):
    """Collection data model."""

    name: str


class HexApiCollectionAccess(BaseModel):
    """Collection access model."""

    collection: HexApiCollectionData


class HexApiWeeklySchedule(BaseModel):
    """Weekly schedule model."""

    day_of_week: str = Field(alias="dayOfWeek")
    hour: int
    minute: int
    timezone: str


class HexApiSchedule(BaseModel):
    """Schedule model."""

    cadence: str
    enabled: bool
    hourly: Optional[dict] = None
    daily: Optional[dict] = None
    weekly: Optional[HexApiWeeklySchedule] = None
    monthly: Optional[dict] = None
    custom: Optional[dict] = None


class HexApiSharing(BaseModel):
    """Sharing configuration model."""

    users: List[HexApiUserAccess] = Field(default_factory=list)
    collections: List[HexApiCollectionAccess] = Field(default_factory=list)
    groups: List[dict] = Field(default_factory=list)

    model_config = ConfigDict(extra="ignore")  # Allow extra fields in the JSON


class HexApiItemType(StrEnum):
    """Item type enum."""

    PROJECT = "PROJECT"
    COMPONENT = "COMPONENT"


class HexApiProjectApiResource(BaseModel):
    """Base model for Hex items (projects and components) from the API."""

    id: str
    title: str
    description: Optional[str] = None
    type: HexApiItemType
    creator: Optional[HexApiUser] = None
    owner: Optional[HexApiUser] = None
    status: Optional[HexApiProjectStatus] = None
    categories: List[HexApiCategory] = Field(default_factory=list)
    reviews: Optional[HexApiReviews] = None
    analytics: Optional[HexApiProjectAnalytics] = None
    last_edited_at: Optional[datetime] = Field(default=None, alias="lastEditedAt")
    last_published_at: Optional[datetime] = Field(default=None, alias="lastPublishedAt")
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")
    archived_at: Optional[datetime] = Field(default=None, alias="archivedAt")
    trashed_at: Optional[datetime] = Field(default=None, alias="trashedAt")
    schedules: List[HexApiSchedule] = Field(default_factory=list)
    sharing: Optional[HexApiSharing] = None

    model_config = ConfigDict(extra="ignore")  # Allow extra fields in the JSON

    @field_validator(
        "created_at",
        "last_edited_at",
        "last_published_at",
        "archived_at",
        "trashed_at",
        mode="before",
    )
    @classmethod
    def parse_datetime(cls, value: Union[str, datetime, None]) -> Optional[datetime]:
        if isinstance(value, str):
            return _parse_iso(value)
        return value


class HexApiPageCursors(BaseModel):
    """Pagination cursor model."""

    after: Optional[str] = None
    before: Optional[str] = None


class HexApiProjectsListResponse(BaseModel):
    """Response model for the list projects API."""

    values: List[HexApiProjectApiResource]
    pagination: Optional[HexApiPageCursors] = None

    model_config = ConfigDict(extra="ignore")  # Allow extra fields in the JSON


@dataclass
class HexApiReport(SourceReport):
    # Per-endpoint call counts — populated automatically by @_api_call decorators.
    # Keys match the name passed to @_api_call on each HexApi method.
    api_calls: Dict[str, int] = field(default_factory=dict)
    # Total HTTP requests across all endpoints (sum of api_calls values).
    api_total_calls: int = 0
    # Items returned by paginated list endpoints.
    api_projects_items: int = 0


class _RateLimiter:
    """Sliding-window rate limiter.

    Tracks call timestamps in a deque. Before each call, removes timestamps
    older than `period` seconds and sleeps if `max_calls` are still in the
    window. Thread-safe via a mutex.
    """

    def __init__(self, max_calls: int, period: float) -> None:
        self._max_calls = max_calls
        self._period = period
        self._calls: Deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            # Drop timestamps that have aged out of the window
            while self._calls and self._calls[0] <= now - self._period:
                self._calls.popleft()

            if len(self._calls) >= self._max_calls:
                # Sleep until the oldest call expires
                sleep_for = self._period - (now - self._calls[0])
                if sleep_for > 0:
                    logger.debug(
                        "Hex API rate limit reached (%d/%ds). Sleeping %.2fs.",
                        self._max_calls,
                        self._period,
                        sleep_for,
                    )
                    time.sleep(sleep_for)
                now = time.monotonic()
                while self._calls and self._calls[0] <= now - self._period:
                    self._calls.popleft()

            self._calls.append(time.monotonic())


class HexApi:
    """https://learn.hex.tech/docs/api/api-reference"""

    def __init__(
        self,
        token: str,
        report: HexApiReport,
        base_url: str = HEX_API_BASE_URL_DEFAULT,
        page_size: int = HEX_API_PAGE_SIZE_DEFAULT,
    ):
        self.token = token
        self.base_url = base_url
        self.report = report
        self.page_size = page_size
        self.session = self._create_retry_session()
        # Tri-state cache for queriedTables tier availability, learned from
        # the first call's response. 403 → permanently False so subsequent
        # entities skip the API hop entirely. Per-entity failures (unpublished,
        # network errors) do NOT flip this flag.
        self._queried_tables_tier_available: Optional[bool] = None
        # Proactive rate limiter: stay under Hex's 60 req/min limit.
        # Patching session.request means every get/post/etc. is throttled
        # transparently — no call-site changes needed.
        _limiter = _RateLimiter(max_calls=57, period=60.0)
        _orig_request = self.session.request

        def _rate_limited_request(
            method: str, url: str, **kwargs: Any
        ) -> requests.Response:
            _limiter.acquire()
            return _orig_request(method, url, **kwargs)

        self.session.request = _rate_limited_request  # type: ignore[assignment]

    def _list_projects_url(self) -> str:
        return f"{self.base_url}/projects"

    def _auth_header(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def _track_page(self, name: str) -> None:
        """Bump the per-page counter for a paginated endpoint."""
        pages_key = f"{name}.pages"
        self.report.api_calls[pages_key] = self.report.api_calls.get(pages_key, 0) + 1
        self.report.api_total_calls += 1

    def _create_retry_session(self) -> requests.Session:
        """Create a requests session with retry logic for rate limiting.

        Hex API rate limit: 60 requests per minute
        https://learn.hex.tech/docs/api/api-overview#kernel-and-rate-limits
        """
        session = requests.Session()

        # Configure retry strategy for 429 (Too Many Requests) with exponential backoff
        retry_strategy = Retry(
            total=5,  # Maximum number of retries
            status_forcelist=[429],  # Only retry on 429 status code
            backoff_factor=2,  # Exponential backoff: 2, 4, 8, 16, 32 seconds
            raise_on_status=True,  # Raise exception after max retries
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    @_api_call("get_workspace_id")
    def fetch_workspace_id(self) -> Optional[str]:
        """Fetch the workspace (org) ID from /users/me.

        Hex's web URLs require the workspace UUID, not the human-readable name,
        so we derive it from /users/me which returns it under `org.id` for both
        Personal Access Tokens and Workspace Tokens.

        Returns None if the endpoint errors or the field is missing — callers
        should treat that as "no externalUrl/chartUrl" rather than failing.
        """
        try:
            resp = self.session.get(
                url=f"{self.base_url}/users/me",
                headers=self._auth_header(),
                timeout=15,
            )
            resp.raise_for_status()
            return resp.json().get("org", {}).get("id") or None
        except Exception as e:
            self.report.warning(
                title="Could not auto-discover Hex workspace_id",
                message=("External URLs (externalUrl/chartUrl) will be omitted. "),
                exc=e,
            )
            return None

    @_api_call("list_projects")
    def fetch_projects(
        self,
        include_components: bool = True,
        include_archived: bool = False,
        include_trashed: bool = False,
    ) -> Generator[Union[Project, Component], None, None]:
        """Fetch all projects and components.

        https://learn.hex.tech/docs/api/api-reference#operation/ListProjects
        """
        params: Dict[str, Any] = {
            "includeComponents": include_components,
            "includeArchived": include_archived,
            "includeTrashed": include_trashed,
            "includeSharing": True,
            "limit": self.page_size,
            "after": None,
            "before": None,
            "sortBy": "LAST_EDITED_AT",
            "sortDirection": "DESC",
        }
        yield from self._fetch_projects_page(params)
        while params["after"]:
            yield from self._fetch_projects_page(params)

    @_api_call("list_projects.pages")
    def _fetch_projects_page(
        self, params: Dict[str, Any]
    ) -> Generator[Union[Project, Component], None, None]:
        """Yields items from one page."""
        logger.debug(f"Fetching projects page with params: {params}")
        try:
            response = self.session.get(
                url=self._list_projects_url(),
                headers=self._auth_header(),
                params=params,
                timeout=30,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            self.report.failure(
                title="Listing Projects and Components API request error",
                message="Error fetching Projects and Components and halting metadata ingestion",
                context=str(params),
                exc=e,
            )
            # Clear cursor so the outer pagination loop terminates instead of
            # retrying the same failing page forever (Retry only auto-retries 429s).
            params["after"] = None
            return

        try:
            api_response = HexApiProjectsListResponse.model_validate(response.json())
        except ValidationError as e:
            self.report.failure(
                title="Listing Projects and Components API response parsing error",
                message="Error parsing API response and halting metadata ingestion",
                context=str(response.json()),
                exc=e,
            )
            params["after"] = None
            return

        logger.info(f"Fetched {len(api_response.values)} items")
        params["after"] = (
            api_response.pagination.after if api_response.pagination else None
        )

        self.report.api_projects_items += len(api_response.values)

        for item in api_response.values:
            try:
                yield self._map_data_from_model(item)
            except Exception as e:
                self.report.warning(
                    title="Incomplete metadata",
                    message="Incomplete metadata because of error mapping item",
                    context=str(item),
                    exc=e,
                )

    def _map_data_from_model(
        self, hex_item: HexApiProjectApiResource
    ) -> Union[Project, Component]:
        """
        Maps a HexApi pydantic model parsed from the API to our domain model
        """

        # Map status
        status = Status(name=hex_item.status.name) if hex_item.status else None

        # Map categories
        categories = []
        if hex_item.categories:
            categories = [
                Category(name=cat.name, description=cat.description)
                for cat in hex_item.categories
            ]

        # Map collections
        collections = []
        if hex_item.sharing and hex_item.sharing.collections:
            collections = [
                Collection(name=col.collection.name)
                for col in hex_item.sharing.collections
            ]

        # Map creator and owner
        creator = Owner(email=hex_item.creator.email) if hex_item.creator else None
        owner = Owner(email=hex_item.owner.email) if hex_item.owner else None

        # Map analytics
        analytics = None
        if hex_item.analytics and hex_item.analytics.app_views:
            analytics = Analytics(
                appviews_all_time=hex_item.analytics.app_views.all_time,
                appviews_last_7_days=hex_item.analytics.app_views.last_seven_days,
                appviews_last_14_days=hex_item.analytics.app_views.last_fourteen_days,
                appviews_last_30_days=hex_item.analytics.app_views.last_thirty_days,
                last_viewed_at=hex_item.analytics.last_viewed_at,
            )

        # Create the appropriate domain model based on type
        if hex_item.type == HexApiItemType.PROJECT:
            return Project(
                id=hex_item.id,
                title=hex_item.title,
                description=hex_item.description,
                created_at=hex_item.created_at,
                last_edited_at=hex_item.last_edited_at,
                last_published_at=hex_item.last_published_at,
                status=status,
                categories=categories,
                collections=collections,
                creator=creator,
                owner=owner,
                analytics=analytics,
            )
        elif hex_item.type == HexApiItemType.COMPONENT:
            return Component(
                id=hex_item.id,
                title=hex_item.title,
                description=hex_item.description,
                created_at=hex_item.created_at,
                last_edited_at=hex_item.last_edited_at,
                last_published_at=hex_item.last_published_at,
                status=status,
                categories=categories,
                collections=collections,
                creator=creator,
                owner=owner,
                analytics=analytics,
            )
        else:
            assert_never(hex_item.type)

    @_api_call("list_connections")
    def fetch_connections(self) -> Dict[str, "HexApiConnection"]:
        """
        Return {connection_id: HexApiConnection} for all data connections.

        Used to resolve dataConnectionId references in cells to DataHub platform names
        and to display human-readable connection labels in context documents.

        Per-type defaults (e.g. BigQuery `projectId`, Snowflake `database`) are
        extracted from `connectionDetails.<type>` so unqualified SQL refs can be
        resolved to canonical 3-part warehouse URNs downstream.
        """
        try:
            resp = self.session.get(
                url=f"{self.base_url}/data-connections",
                headers=self._auth_header(),
                timeout=30,
            )
            resp.raise_for_status()
            result: Dict[str, HexApiConnection] = {}
            for conn in resp.json().get("values", []):
                hex_type = conn.get("type", "")
                details = (conn.get("connectionDetails") or {}).get(hex_type) or {}
                default_database, default_schema = _extract_connection_defaults(
                    hex_type, details
                )
                result[conn["id"]] = HexApiConnection(
                    name=conn.get("name", ""),
                    type=hex_type,
                    default_database=default_database,
                    default_schema=default_schema,
                )
            return result
        except Exception as e:
            self.report.warning(
                title="Failed to fetch data connections",
                message="Lineage will be incomplete: connection IDs cannot be resolved to platforms.",
                exc=e,
            )
            return {}

    @_api_call("get_project")
    def fetch_single_project(
        self, project_id: str
    ) -> Optional[Union[Project, Component]]:
        """Fetch metadata for a single project or component by ID.

        Used to resolve unknown component imports on-demand during streaming
        ingestion — avoids waiting for the component to appear in the listing.
        """
        try:
            resp = self.session.get(
                url=f"{self.base_url}/projects/{project_id}",
                headers=self._auth_header(),
                timeout=30,
            )
            resp.raise_for_status()
            item = HexApiProjectApiResource.model_validate(resp.json())
            return self._map_data_from_model(item)
        except Exception as e:
            self.report.warning(
                title="Failed to fetch single project",
                message="Could not fetch project by ID",
                context=f"project_id={project_id}",
                exc=e,
            )
            return None

    @_api_call("list_cells")
    def fetch_cells(self, project_id: str) -> List[dict]:
        """
        Return all cells for a project via paginated REST calls.

        Response shape per cell:
          {id, staticId, cellType, label, dataConnectionId,
           contents: {sqlCell: {source}, markdownCell: {source}, codeCell: null},
           projectId}
        """
        cells: List[dict] = []
        after: Optional[str] = None
        while True:
            params: Dict[str, Any] = {
                "projectId": project_id,
                "limit": self.page_size,
            }
            if after:
                params["after"] = after
            try:
                resp = self.session.get(
                    url=f"{self.base_url}/cells",
                    headers=self._auth_header(),
                    params=params,
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
                self._track_page("list_cells")
            except Exception as e:
                self.report.warning(
                    title="Failed to fetch cells",
                    message="Cells API call failed",
                    context=f"project_id={project_id}",
                    exc=e,
                )
                break
            cells.extend(data.get("values", []))
            after = (data.get("pagination") or {}).get("after")
            if not after:
                break
        return cells

    @staticmethod
    def _make_normalized_cell(
        cell_id: str,
        cell_type: str,
        label: Optional[str],
        data_connection_id: Optional[str] = None,
        sql_source: Optional[str] = None,
        markdown_source: Optional[str] = None,
    ) -> dict:
        return {
            "staticId": cell_id,
            "id": cell_id,
            "cellType": cell_type,
            "label": label,
            "dataConnectionId": data_connection_id,
            "contents": {
                "sqlCell": {"source": sql_source} if sql_source is not None else None,
                "codeCell": None,
                "markdownCell": (
                    {"source": markdown_source} if markdown_source is not None else None
                ),
            },
        }

    @staticmethod
    def _normalize_export_cells(
        yaml_cells: List[dict],
    ) -> "tuple[List[dict], List[str]]":
        """
        Flatten and normalise export YAML cells to the /v1/cells API shape.

        The export YAML is hierarchical: COLLAPSIBLE containers hold nested cells
        in config.cells. This method recursively flattens them so callers receive
        a flat list identical in structure to what /v1/cells returns.

        Returns (normalized_cells, component_ids).
        """
        result: List[dict] = []
        component_ids: List[str] = []

        for cell in yaml_cells:
            ct = cell.get("cellType", "")
            cfg = cell.get("config", {}) or {}
            cell_id = cell.get("cellId", "")
            label = cell.get("cellLabel")

            if ct == "SQL":
                result.append(
                    HexApi._make_normalized_cell(
                        cell_id,
                        ct,
                        label,
                        data_connection_id=cfg.get("dataConnectionId"),
                        sql_source=cfg.get("source", ""),
                    )
                )
            elif ct == "MARKDOWN":
                result.append(
                    HexApi._make_normalized_cell(
                        cell_id,
                        ct,
                        label,
                        markdown_source=cfg.get("source", ""),
                    )
                )
            elif ct == "COLLAPSIBLE":
                # Emit a stub so _parse_cells captures the section name, then
                # recurse into the nested children.
                result.append(HexApi._make_normalized_cell(cell_id, ct, label))
                nested, nested_comp_ids = HexApi._normalize_export_cells(
                    cfg.get("cells", [])
                )
                result.extend(nested)
                component_ids.extend(nested_comp_ids)
            elif ct == "EXPLORE":
                result.append(HexApi._make_normalized_cell(cell_id, ct, label))
            elif ct == "COMPONENT_IMPORT":
                comp_id = (cfg.get("component") or {}).get("id")
                if comp_id:
                    component_ids.append(comp_id)
                result.append(HexApi._make_normalized_cell(cell_id, ct, label))

        return result, component_ids

    @_api_call("export_project")
    def fetch_project_export(self, project_id: str) -> "tuple[List[dict], List[str]]":
        """
        POST /api/v1/projects/export returns the full project YAML.

        Unlike the cells API this endpoint:
        - Exposes config.component.id in COMPONENT_IMPORT cells (cells API strips it)
        - Contains SQL cells with source in config.source (not contents.sqlCell.source)
        - Organises cells hierarchically inside COLLAPSIBLE containers
        - Works with tokens that lack the 'Read projects' cells scope

        Returns (all_normalized_cells, component_ids) where:
          all_normalized_cells — flat list of all cells in /v1/cells API shape,
                                 recursively extracted from COLLAPSIBLE containers.
          component_ids        — component project IDs imported by this project.
        """
        try:
            resp = self.session.post(
                url=f"{self.base_url}/projects/export",
                headers=self._auth_header(),
                json={"projectId": project_id, "version": "draft"},
                timeout=30,
            )
            resp.raise_for_status()
            content_yaml = resp.json().get("content", "")
            content = yaml.safe_load(content_yaml) or {}
        except Exception as e:
            self.report.warning(
                title="Failed to fetch project export",
                message="Export API call failed",
                context=f"project_id={project_id}",
                exc=e,
            )
            return [], []

        return HexApi._normalize_export_cells(content.get("cells", []))

    @_api_call("queried_tables")
    def fetch_queried_tables(self, hex_item_id: str) -> Optional[List[dict]]:
        """Return Hex's pre-resolved warehouse tables for a project or
        component, or None if unavailable."""
        if self._queried_tables_tier_available is False:
            return None  # workspace already known to be non-ENTERPRISE

        try:
            resp = self.session.get(
                url=f"{self.base_url}/projects/{hex_item_id}/queriedTables",
                headers=self._auth_header(),
                timeout=30,
            )
            if resp.status_code == 403:
                self._queried_tables_tier_available = False
                self.report.warning(
                    title="queriedTables unavailable on this workspace",
                    message=(
                        "The /projects/{id}/queriedTables endpoint requires a Hex "
                        "Enterprise workspace. Lineage will fall back to SQL parsing "
                        "of cell sources. Set use_queried_tables_lineage: false to "
                        "silence this warning."
                    ),
                    context=f"hex_item_id={hex_item_id}",
                )
                return None
            resp.raise_for_status()
            self._queried_tables_tier_available = True
            return resp.json().get("values", [])
        except Exception as e:
            self.report.warning(
                title="Failed to fetch queriedTables",
                message="queriedTables call failed; falling back to cell-based lineage",
                context=f"hex_item_id={hex_item_id}",
                exc=e,
            )
            return None

    @_api_call("run_list")
    def fetch_latest_run(self, project_id: str) -> Optional[RunRecord]:
        """Return the most recent run for a project, or None if unavailable."""
        try:
            resp = self.session.get(
                url=f"{self.base_url}/projects/{project_id}/runs",
                headers=self._auth_header(),
                params={"limit": 1},
                timeout=30,
            )
            resp.raise_for_status()
            runs = resp.json().get("runs", [])
            if not runs:
                return None
            r = runs[0]
            start_time = _parse_iso(r.get("startTime"))
            if not start_time:
                return None
            elapsed_ms = r.get("elapsedTime")
            return RunRecord(
                run_id=r.get("runId", ""),
                status=r.get("status", "UNKNOWN"),
                start_time=start_time,
                # API returns milliseconds; convert to seconds
                elapsed_seconds=elapsed_ms / 1000.0 if elapsed_ms else None,
            )
        except Exception as e:
            self.report.warning(
                title="Failed to fetch run history",
                message="Run history call failed",
                context=f"project_id={project_id}",
                exc=e,
            )
            return None


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
