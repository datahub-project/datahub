import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Generator, Union

import requests

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

logger = logging.getLogger(__name__)


@dataclass
class HexAPIReport:
    fetch_projects_page_calls: int = 0
    fetch_projects_page_items: int = 0


class HexAPI:
    """https://learn.hex.tech/docs/api/api-reference"""

    def __init__(
        self,
        token: str,
        report: HexAPIReport,
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
        response = requests.get(
            url=self._list_projects_url(),
            headers=self._auth_header(),
            params=params,
        )
        response.raise_for_status()
        data = response.json()
        logger.debug(f"Fetched data: {data}")
        values = data.get("values", [])
        params["after"] = data.get("pagination", {}).get("after")

        self.report.fetch_projects_page_items += len(values)
        for value in values:
            yield self._map_data(data=value)

    def _map_data(self, data: Dict[str, Any]) -> Union[Project, Component]:
        description = data.get("description")
        created_at = (
            self._parse_datetime(data["createdAt"]) if data.get("createdAt") else None
        )
        last_edited_at = (
            self._parse_datetime(data["lastEditedAt"])
            if data.get("lastEditedAt")
            else None
        )
        status = Status(name=data["status"]["name"]) if data.get("status") else None
        categories = [
            Category(
                name=cat["name"],
                description=cat.get("description"),
            )
            for cat in data.get("categories", [])
        ]
        collections = [
            Collection(
                name=col["collection"]["name"],
            )
            for col in data.get("sharing", {}).get("collections", [])
        ]
        creator = Owner(email=data["creator"]["email"]) if data.get("creator") else None
        owner = Owner(email=data["owner"]["email"]) if data.get("owner") else None
        analytics = (
            Analytics(
                appviews_all_time=data["analytics"]["appViews"].get("allTime"),
                appviews_last_7_days=data["analytics"]["appViews"].get("lastSevenDays"),
                appviews_last_14_days=data["analytics"]["appViews"].get(
                    "lastFourteenDays"
                ),
                appviews_last_30_days=data["analytics"]["appViews"].get(
                    "lastThirtyDays"
                ),
                last_viewed_at=self._parse_datetime(data["analytics"]["lastViewedAt"])
                if data["analytics"].get("lastViewedAt")
                else None,
            )
            if data.get("analytics", {}).get("appViews")
            else None
        )

        if data["type"] == "PROJECT":
            return Project(
                id=data["id"],
                title=data["title"],
                description=description,
                created_at=created_at,
                last_edited_at=last_edited_at,
                status=status,
                categories=categories,
                collections=collections,
                creator=creator,
                owner=owner,
                analytics=analytics,
                _raw_data=data,
            )
        elif data["type"] == "COMPONENT":
            return Component(
                id=data["id"],
                title=data["title"],
                description=description,
                created_at=created_at,
                last_edited_at=last_edited_at,
                status=status,
                categories=categories,
                collections=collections,
                creator=creator,
                owner=owner,
                analytics=analytics,
                _raw_data=data,
            )
        else:
            raise ValueError(f"Unknown type: {data['type']}")

    def _parse_datetime(self, dt_str: str) -> datetime:
        return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc
        )
