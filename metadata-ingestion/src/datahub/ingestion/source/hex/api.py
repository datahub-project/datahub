import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional, Union

import requests
from pydantic import BaseModel, Field, ValidationError, validator
from typing_extensions import assert_never

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
    Status,
)
from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)

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

    @validator("last_viewed_at", "published_results_updated_at", pre=True)
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

    class Config:
        extra = "ignore"  # Allow extra fields in the JSON


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

    class Config:
        extra = "ignore"  # Allow extra fields in the JSON

    @validator(
        "created_at",
        "last_edited_at",
        "last_published_at",
        "archived_at",
        "trashed_at",
        pre=True,
    )
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

    class Config:
        extra = "ignore"  # Allow extra fields in the JSON


@dataclass
class HexApiReport(SourceReport):
    fetch_projects_page_calls: int = 0
    fetch_projects_page_items: int = 0


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

    def _list_projects_url(self):
        return f"{self.base_url}/projects"

    def _auth_header(self):
        return {"Authorization": f"Bearer {self.token}"}

    def fetch_projects(
        self,
        include_components: bool = True,
        include_archived: bool = False,
        include_trashed: bool = False,
    ) -> Generator[Union[Project, Component], None, None]:
        """Fetch all projects and components

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
            "sortBy": "CREATED_AT",
            "sortDirection": "ASC",
        }
        yield from self._fetch_projects_page(params)

        while params["after"]:
            yield from self._fetch_projects_page(params)

    def _fetch_projects_page(
        self, params: Dict[str, Any]
    ) -> Generator[Union[Project, Component], None, None]:
        logger.debug(f"Fetching projects page with params: {params}")
        self.report.fetch_projects_page_calls += 1
        try:
            response = requests.get(
                url=self._list_projects_url(),
                headers=self._auth_header(),
                params=params,
                timeout=30,
            )
            response.raise_for_status()

            api_response = HexApiProjectsListResponse.parse_obj(response.json())
            logger.info(f"Fetched {len(api_response.values)} items")
            params["after"] = (
                api_response.pagination.after if api_response.pagination else None
            )

            self.report.fetch_projects_page_items += len(api_response.values)

            for item in api_response.values:
                try:
                    ret = self._map_data_from_model(item)
                    yield ret
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
                status=status,
                categories=categories,
                collections=collections,
                creator=creator,
                owner=owner,
                analytics=analytics,
            )
        else:
            assert_never(hex_item.type)
