import functools
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Deque, Dict, Generator, List, Optional, TypeVar, Union

import requests
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from requests.adapters import HTTPAdapter
from typing_extensions import assert_never
from urllib3.util.retry import Retry

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.hex.constants import (
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


def _api_call(name: str, paginated: bool = False) -> Callable[[_F], _F]:
    """
    Decorator that tracks Hex API calls on self.report.

    For non-paginated methods: increments api_calls[name] once per call.
    For paginated methods (paginated=True): increments api_calls[name] once
    per outer invocation AND injects a _track_page() helper onto self that
    the method body calls once per inner HTTP request. This produces:

      api_calls["list_cells"]       # outer invocations (one per project)
      api_calls["list_cells.pages"] # actual HTTP page requests
    """

    def decorator(func: _F) -> _F:
        pages_key = f"{name}.pages"

        @functools.wraps(func)
        def wrapper(self: "HexApi", *args: Any, **kwargs: Any) -> Any:
            self.report.api_calls[name] = self.report.api_calls.get(name, 0) + 1
            self.report.api_total_calls += 1
            if paginated:
                # Inject a page-tracking helper the method body can call.
                def _track_page() -> None:
                    self.report.api_calls[pages_key] = (
                        self.report.api_calls.get(pages_key, 0) + 1
                    )
                    self.report.api_total_calls += 1

                self._track_page = _track_page  # type: ignore[attr-defined]
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
    def parse_datetime(cls, value):
        if value is None:
            return None
        if isinstance(value, str):
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                tzinfo=timezone.utc
            )
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
    hourly: Optional[Any] = None
    daily: Optional[Any] = None
    weekly: Optional[HexApiWeeklySchedule] = None
    monthly: Optional[Any] = None
    custom: Optional[Any] = None


class HexApiSharing(BaseModel):
    """Sharing configuration model."""

    users: Optional[List[HexApiUserAccess]] = []
    collections: Optional[List[HexApiCollectionAccess]] = []
    groups: Optional[List[Any]] = []

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
    categories: Optional[List[HexApiCategory]] = []
    reviews: Optional[HexApiReviews] = None
    analytics: Optional[HexApiProjectAnalytics] = None
    last_edited_at: Optional[datetime] = Field(default=None, alias="lastEditedAt")
    last_published_at: Optional[datetime] = Field(default=None, alias="lastPublishedAt")
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")
    archived_at: Optional[datetime] = Field(default=None, alias="archivedAt")
    trashed_at: Optional[datetime] = Field(default=None, alias="trashedAt")
    schedules: Optional[List[HexApiSchedule]] = []
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
    def parse_datetime(cls, value):
        if value is None:
            return None
        if isinstance(value, str):
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                tzinfo=timezone.utc
            )
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
    window. Thread-safe via a reentrant lock.
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
        # Callable attribute, not a method, so @_api_call(paginated=True) can
        # override it via self._track_page = ... without a [method-assign] error.
        self._track_page: Callable[[], None] = lambda: None
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

    def _list_projects_url(self):
        return f"{self.base_url}/projects"

    def _auth_header(self):
        return {"Authorization": f"Bearer {self.token}"}

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

    @_api_call("list_projects")
    def fetch_projects(
        self,
        include_components: bool = True,
        include_archived: bool = False,
        include_trashed: bool = False,
        stop_before_ms: Optional[int] = None,
    ) -> Generator[Union[Project, Component], None, None]:
        """Fetch all projects and components, sorted newest-first.

        Sorting by LAST_EDITED_AT DESC enables early termination on incremental
        runs: once a page's last item is older than stop_before_ms (the previous
        checkpoint timestamp) everything after it is guaranteed unchanged and
        pagination stops. This reduces listing from O(total) to O(changed) per
        incremental run.

        https://learn.hex.tech/docs/api/api-reference#operation/ListProjects
        """
        params = {
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
        yield from self._fetch_projects_page(params, stop_before_ms)
        while params["after"]:
            yield from self._fetch_projects_page(params, stop_before_ms)

    @_api_call("list_projects.pages")
    def _fetch_projects_page(
        self, params: Dict[str, Any], stop_before_ms: Optional[int] = None
    ) -> Generator[Union[Project, Component], None, None]:
        """Yields items from one page. Sets params["after"]=None on early termination."""
        logger.debug(f"Fetching projects page with params: {params}")
        try:
            response = self.session.get(
                url=self._list_projects_url(),
                headers=self._auth_header(),
                params=params,
                timeout=30,
            )
            response.raise_for_status()

            api_response = HexApiProjectsListResponse.model_validate(response.json())
            logger.info(f"Fetched {len(api_response.values)} items")
            params["after"] = (
                api_response.pagination.after if api_response.pagination else None
            )

            self.report.api_projects_items += len(api_response.values)

            for item in api_response.values:
                try:
                    ret = self._map_data_from_model(item)
                    yield ret
                    # Early termination: results are sorted LAST_EDITED_AT DESC so
                    # once we see an item older than the checkpoint everything after
                    # it is also older — stop paginating.
                    if stop_before_ms and item.last_edited_at:
                        if int(item.last_edited_at.timestamp() * 1000) < stop_before_ms:
                            logger.info(
                                "Incremental: reached projects older than checkpoint "
                                "— stopping pagination early."
                            )
                            params["after"] = None  # prevent outer loop continuing
                            return
                except Exception as e:
                    self.report.warning(
                        title="Incomplete metadata",
                        message="Incomplete metadata because of error mapping item",
                        context=str(item),
                        exc=e,
                    )
        except ValidationError as e:
            self.report.failure(
                title="Listing Projects and Components API response parsing error",
                message="Error parsing API response and halting metadata ingestion",
                context=str(response.json()),
                exc=e,
            )
        except (requests.RequestException, Exception) as e:
            self.report.failure(
                title="Listing Projects and Components API request error",
                message="Error fetching Projects and Components and halting metadata ingestion",
                context=str(params),
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

    # ------------------------------------------------------------------
    # Data connections  (/v1/data-connections)
    # ------------------------------------------------------------------

    @_api_call("list_connections")
    def fetch_connections(self) -> Dict[str, Any]:
        """
        Return {connection_id: (name, connection_type)} for all data connections.

        Used to resolve dataConnectionId references in cells to DataHub platform names
        and to display human-readable connection labels in context documents.
        """
        try:
            resp = self.session.get(
                url=f"{self.base_url}/data-connections",
                headers=self._auth_header(),
                timeout=30,
            )
            resp.raise_for_status()
            result: Dict[str, Any] = {}
            for conn in resp.json().get("values", []):
                result[conn["id"]] = (conn.get("name", ""), conn.get("type", ""))
            return result
        except Exception as e:
            self.report.warning(
                title="Failed to fetch data connections",
                message="Connection type resolution will fall back to default platform",
                exc=e,
            )
            return {}

    # ------------------------------------------------------------------
    # Cells  (all tiers — /v1/cells?projectId=)
    # ------------------------------------------------------------------

    @_api_call("list_cells", paginated=True)
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
                self._track_page()  # counts list_cells.pages
            except Exception as e:
                self.report.warning(
                    title="Failed to fetch cells",
                    message=f"Cells API call failed for project {project_id}",
                    exc=e,
                )
                break
            cells.extend(data.get("values", []))
            after = (data.get("pagination") or {}).get("after")
            if not after:
                break
        return cells

    # ------------------------------------------------------------------
    # Project export  (/v1/projects/export — reveals COMPONENT_IMPORT ids)
    # ------------------------------------------------------------------

    @_api_call("export_project")
    def fetch_project_export(self, project_id: str) -> "tuple[List[dict], List[str]]":
        """
        POST /api/v1/projects/export returns the full project YAML.

        This is the only API that exposes which component IDs a project imports
        (the cells API strips config.component.id from COMPONENT_IMPORT cells).

        Returns (native_sql_cells, component_ids) where:
          native_sql_cells — raw cell dicts for SQL cells defined in the project
                             itself (not inlined from components), in the same
                             shape as /v1/cells, usable by _extract_sql_cells().
          component_ids    — list of component project IDs imported by this project.
        """
        import yaml

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
                message=f"Export API call failed for project {project_id}; component linkage unavailable",
                exc=e,
            )
            return [], []

        native_sql_cells: List[dict] = []
        component_ids: List[str] = []

        for cell in content.get("cells", []):
            ct = cell.get("cellType", "")
            if ct == "SQL":
                # Normalise YAML cell shape to match the /v1/cells shape
                native_sql_cells.append(
                    {
                        "staticId": cell.get("cellId", ""),
                        "id": cell.get("cellId", ""),
                        "cellType": "SQL",
                        "label": cell.get("cellLabel"),
                        "dataConnectionId": cell.get("dataConnectionId"),
                        "contents": {
                            "sqlCell": {"source": cell.get("source", "")},
                            "codeCell": None,
                            "markdownCell": None,
                        },
                    }
                )
            elif ct == "COMPONENT_IMPORT":
                comp_id = cell.get("config", {}).get("component", {}).get("id")
                if comp_id:
                    component_ids.append(comp_id)

        return native_sql_cells, component_ids

    # ------------------------------------------------------------------
    # Queried tables  (ENTERPRISE tier — /v1/projects/{id}/queriedTables)
    # ------------------------------------------------------------------

    @_api_call("queried_tables")
    def fetch_queried_tables(self, project_id: str) -> Optional[List[dict]]:
        """
        Return [{dataConnectionId, dataConnectionName, tableName}] for all
        warehouse tables touched by this project, or None on 403 (non-ENTERPRISE).

        This is the most accurate lineage source — Hex's own pre-resolved table
        list requiring no SQL parsing on the DataHub side.
        """
        try:
            resp = self.session.get(
                url=f"{self.base_url}/projects/{project_id}/queriedTables",
                headers=self._auth_header(),
                timeout=30,
            )
            if resp.status_code == 403:
                return None  # non-ENTERPRISE workspace
            resp.raise_for_status()
            return resp.json().get("values", [])
        except Exception as e:
            self.report.warning(
                title="Failed to fetch queriedTables",
                message=f"queriedTables call failed for {project_id}; falling back to cell-based lineage",
                exc=e,
            )
            return None

    # ------------------------------------------------------------------
    # Run history  (/v1/projects/{id}/runs)
    # ------------------------------------------------------------------

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
                message=f"Run history call failed for {project_id}",
                exc=e,
            )
            return None


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
